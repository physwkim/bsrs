//! `TiledSink` — push Documents to a Tiled HTTP catalog via `tiled-rs`.
//!
//! ## Scope
//!
//! Tiled's full bluesky writer (`bluesky.callbacks.tiled_writer.TiledWriter`)
//! is ~800 lines of Python that handles run-router fan-out, schema
//! normalization, asset registration, batched writes, and recovery via
//! backup directories. bsrs delegates that orchestration to either:
//! - the **Python relay** path: `ZmqDocumentSink` → `RemoteDispatcher` →
//!   `TiledWriter` (doc 08 D19), OR
//! - **direct via `tiled-rs`** (this file): minimal RunStart →
//!   container register, RunStop → metadata patch. All other Document
//!   types are dropped — additional structure-family registrations
//!   (`array`, `table`, `stream_resource`, `stream_datum`) live in a
//!   future `TiledFullSink` that wraps the same `Context`.
//!
//! Using `tiled-rs` here (rather than raw reqwest) buys us:
//! - automatic auth (api_key / OAuth) via `Context`
//! - CSRF + cache-invalidation handling
//! - typed `ClientError` surface mapped to `BsrsError::Backend`
//! - profile / `from_uri` integration so the sink composes with the
//!   rest of the bsrs + tiled-rs ecosystem.

use crate::core::error::{BsrsError, Result};
use crate::engine::DocumentSink;
use crate::event_model::Document;
use async_trait::async_trait;
use serde_json::json;
use tiled_rs::client::{Context, ContextOptions};
use tokio::sync::Mutex;
use url::Url;

use crate::callbacks::doc_name::document_name;

/// Minimal Tiled HTTP catalog sink, backed by `tiled_rs::client::Context`.
pub struct TiledSink {
    /// Tiled HTTP context (auth + cache + csrf).
    ctx: Context,
    /// Container path under which runs are written
    /// (e.g. `bluesky` for `/api/v1/register/bluesky/<run_uid>`).
    container: String,
    /// Set of run UIDs whose container has been registered this session.
    runs_started: Mutex<std::collections::HashSet<String>>,
}

impl TiledSink {
    /// Build with a base URL and container path. Reads `TILED_API_KEY`
    /// from the environment if no key is supplied via [`with_api_key`].
    pub fn new(base_url: impl Into<String>, container: impl Into<String>) -> Result<Self> {
        let api_key = std::env::var("TILED_API_KEY").ok();
        let mut opts = ContextOptions::default();
        if let Some(k) = api_key {
            opts = opts.api_key(k);
        }
        let (ctx, _warnings) = Context::from_uri_with_options(&base_url.into(), opts)
            .map_err(|e| BsrsError::Backend(format!("tiled-rs init: {e}")))?;
        Ok(Self {
            ctx,
            container: container.into().trim_matches('/').to_string(),
            runs_started: Mutex::new(Default::default()),
        })
    }

    /// Override the API key after construction (otherwise read from env).
    pub fn with_api_key(self, key: impl Into<String>) -> Self {
        // Best-effort: tokio block_in_place would be needed if we were
        // inside an async context. For typical setup-time use the caller
        // is on a sync path — `set_api_key` is async, so route through
        // the bsrs runtime.
        let key = key.into();
        let ctx = self.ctx.clone();
        crate::core::runtime::bsrs_runtime().block_on(async move {
            ctx.set_api_key(Some(key)).await;
        });
        self
    }

    /// Borrow the underlying `tiled_rs::client::Context` — useful for
    /// applications that want to extend the sink with extra HTTP
    /// calls against the same auth/cache state.
    pub fn context(&self) -> &Context {
        &self.ctx
    }

    /// Build a `<api_uri>/<rel>` URL from a relative path like
    /// `register/bluesky` or `metadata/bluesky/<uid>`.
    fn url(&self, rel: &str) -> Result<Url> {
        let api = self.ctx.api_uri();
        // api_uri() ends with a `/`, so a relative join works without
        // dropping path components.
        let abs = api
            .join(rel.trim_start_matches('/'))
            .map_err(|e| BsrsError::Backend(format!("tiled url join: {e}")))?;
        Ok(abs)
    }

    /// Register a new run container the first time we see its RunStart.
    async fn ensure_run_registered(&self, run_uid: &str, start: &serde_json::Value) -> Result<()> {
        let mut seen = self.runs_started.lock().await;
        if seen.contains(run_uid) {
            return Ok(());
        }
        seen.insert(run_uid.to_string());
        drop(seen);
        let url = self.url(&format!("register/{}", self.container))?;
        let body = json!({
            "structure_family": "container",
            "metadata": {"start": start},
            "specs": [{"name": "BlueskyRun", "version": "1.0"}],
            "key": run_uid,
        });
        self.ctx
            .post_json(&url, &body)
            .await
            .map_err(|e| BsrsError::Backend(format!("tiled register {url}: {e}")))?;
        Ok(())
    }

    /// Patch the run container's metadata when RunStop arrives.
    async fn patch_run_stop(&self, run_uid: &str, stop: &serde_json::Value) -> Result<()> {
        let url = self.url(&format!("metadata/{}/{}", self.container, run_uid))?;
        let body = json!({"metadata": {"stop": stop}});
        self.ctx
            .patch_json(&url, &body)
            .await
            .map_err(|e| BsrsError::Backend(format!("tiled stop patch {url}: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl DocumentSink for TiledSink {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        // Coarse routing: Start opens a container; Stop patches it;
        // everything else is dropped under the run path.
        match doc {
            Document::Start(s) => {
                let value = serde_json::to_value(s)?;
                self.ensure_run_registered(&s.uid, &value).await?;
            }
            Document::Stop(s) => {
                let value = serde_json::to_value(s)?;
                // Best-effort PATCH; if the server doesn't support it
                // we log and continue rather than fail the run.
                if let Err(e) = self.patch_run_stop(&s.run_start, &value).await {
                    tracing::warn!(target: "bsrs.tiled", "stop patch: {e}");
                }
            }
            other => {
                let name = document_name(other);
                tracing::trace!(
                    target: "bsrs.tiled",
                    "drop {name} doc — full TiledWriter compat lives in either \
                     a Python relay (ZmqDocumentSink → TiledWriter) or a future \
                     direct TiledFullSink; see crate-level docs"
                );
            }
        }
        Ok(())
    }
}
