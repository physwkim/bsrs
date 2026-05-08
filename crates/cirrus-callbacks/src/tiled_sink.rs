//! `TiledSink` — push Documents to a Tiled HTTP catalog.
//!
//! ## Scope
//!
//! Tiled's full bluesky writer (`bluesky.callbacks.tiled_writer.TiledWriter`)
//! is ~800 lines of Python that handles run-router fan-out, schema
//! normalization, asset registration, batched writes, and recovery via
//! backup directories. Replicating it in Rust is a separate project.
//!
//! `TiledSink` here is the **minimal** surface: per-document POSTs to the
//! Tiled REST `register` endpoint, suitable for production-light setups
//! where the Tiled server runs the bluesky writer plugin OR for ingestion
//! into a fresh catalog.
//!
//! ## Recommended deployment
//!
//! For full TiledWriter behavior, prefer **emitting documents over
//! [`crate::ZmqDocumentSink`]** and running a small Python relay that
//! subscribes via `bluesky.callbacks.zmq.RemoteDispatcher` and forwards
//! into `TiledWriter`. This is the same pattern bluesky-queueserver uses
//! and keeps cirrus out of the Tiled-protocol detail.

use async_trait::async_trait;
use cirrus_core::error::{CirrusError, Result};
use cirrus_engine::DocumentSink;
use cirrus_event_model::Document;
use reqwest::Client;
use serde_json::json;
use tokio::sync::Mutex;

use crate::doc_name::document_name;

/// Minimal Tiled HTTP catalog sink.
pub struct TiledSink {
    /// Base URL of the Tiled server (e.g. `http://localhost:8000`).
    base_url: String,
    /// Container path under which runs are written
    /// (e.g. `bluesky` for `/api/v1/register/bluesky/<run_uid>`).
    container: String,
    /// HTTP client (rustls).
    client: Client,
    /// API key for `Authorization: Apikey ...` header.
    api_key: Option<String>,
    /// In-flight run state — first time we see a run we POST a register call.
    runs_started: Mutex<std::collections::HashSet<String>>,
}

impl TiledSink {
    /// Build with a base URL, container path, and optional API key.
    ///
    /// Reads `TILED_API_KEY` from the environment if `api_key` is `None`.
    pub fn new(base_url: impl Into<String>, container: impl Into<String>) -> Result<Self> {
        let client = Client::builder()
            .build()
            .map_err(|e| CirrusError::Backend(format!("reqwest build: {e}")))?;
        let api_key = std::env::var("TILED_API_KEY").ok();
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            container: container.into().trim_matches('/').to_string(),
            client,
            api_key,
            runs_started: Mutex::new(Default::default()),
        })
    }

    /// Override the API key (otherwise read from env).
    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    fn auth_header(&self) -> Option<String> {
        self.api_key.as_ref().map(|k| format!("Apikey {k}"))
    }

    async fn post_json(&self, path: &str, body: serde_json::Value) -> Result<()> {
        let url = format!("{}/{}", self.base_url, path.trim_start_matches('/'));
        let mut req = self.client.post(&url).json(&body);
        if let Some(auth) = self.auth_header() {
            req = req.header("Authorization", auth);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| CirrusError::Backend(format!("tiled POST {url}: {e}")))?;
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CirrusError::Backend(format!(
                "tiled POST {url} → {code}: {body}"
            )));
        }
        Ok(())
    }

    /// Register a new run container the first time we see its RunStart.
    async fn ensure_run_registered(&self, run_uid: &str, start: &serde_json::Value) -> Result<()> {
        let mut seen = self.runs_started.lock().await;
        if seen.contains(run_uid) {
            return Ok(());
        }
        seen.insert(run_uid.to_string());
        drop(seen);
        let path = format!("api/v1/register/{}", self.container);
        let body = json!({
            "structure_family": "container",
            "metadata": {"start": start},
            "specs": [{"name": "BlueskyRun", "version": "1.0"}],
            "key": run_uid,
        });
        self.post_json(&path, body).await
    }
}

#[async_trait]
impl DocumentSink for TiledSink {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        // Coarse routing: Start opens a container; Stop patches it; everything
        // else is dropped under the run path.
        match doc {
            Document::Start(s) => {
                let value = serde_json::to_value(s)?;
                self.ensure_run_registered(&s.uid, &value).await?;
            }
            Document::Stop(s) => {
                let path = format!("api/v1/metadata/{}/{}", self.container, s.run_start);
                let body = json!({"metadata": {"stop": serde_json::to_value(s)?}});
                // Best-effort PATCH; if the server doesn't support it we log.
                if let Err(e) = self.post_json(&path, body).await {
                    tracing::warn!(target: "cirrus.tiled", "stop patch: {e}");
                }
            }
            other => {
                let name = document_name(other);
                tracing::trace!(
                    target: "cirrus.tiled",
                    "drop {name} doc — full TiledWriter compat lives in Python relay; \
                     see crate-level docs"
                );
            }
        }
        Ok(())
    }
}
