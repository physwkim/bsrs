//! `StandardDetector<C, W>` — composition of `DetectorControl` + `DetectorWriter`.

use async_trait::async_trait;
use cirrus_core::error::Result;
use cirrus_core::msg::{
    CollectableObj, FlyableObj, NamedObj, ReadableObj, StageableObj, TriggerableObj,
};
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::Status;
use cirrus_event_model::DataKey;
use cirrus_protocols_async::{
    DetectorControl, DetectorWriter, Flyable, Stageable, StreamAsset, Triggerable,
    WritesStreamAssets,
};
use futures::stream::{BoxStream, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Re-export so users get `TriggerInfo` / `DetectorTrigger` straight from
/// cirrus-devices.
pub use cirrus_protocols_async::{DetectorTrigger, TriggerInfo};

/// A detector composed of an arming half (`DetectorControl`) and a writing half
/// (`DetectorWriter`). Implements all eight bluesky protocols by delegation.
pub struct StandardDetector<C, W>
where
    C: DetectorControl,
    W: DetectorWriter,
{
    name: String,
    control: C,
    writer: W,
    // K1: any background tasks owned by start/arm should be tracked here.
    // For M3 we don't spawn any directly.
    cached_target: AtomicU64,
}

impl<C, W> StandardDetector<C, W>
where
    C: DetectorControl,
    W: DetectorWriter,
{
    /// Build a `StandardDetector`.
    pub fn new(name: impl Into<String>, control: C, writer: W) -> Self {
        Self {
            name: name.into(),
            control,
            writer,
            cached_target: AtomicU64::new(0),
        }
    }

    /// Reference the inner writer (for plan code that needs it).
    pub fn writer(&self) -> &W {
        &self.writer
    }

    /// Reference the inner control (for plan code that needs it).
    pub fn control(&self) -> &C {
        &self.control
    }

    /// Stable name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl<C, W> Stageable for StandardDetector<C, W>
where
    C: DetectorControl,
    W: DetectorWriter,
{
    fn name(&self) -> &str {
        &self.name
    }
    async fn stage(&self) -> Result<()> {
        // Open writer with multiplier=1 by default; plans can call configure()
        // to change this.
        self.writer.open(1).await?;
        Ok(())
    }
    async fn unstage(&self) -> Result<()> {
        self.control.disarm().await?;
        self.writer.close().await
    }
}

#[async_trait]
impl<C, W> Triggerable for StandardDetector<C, W>
where
    C: DetectorControl,
    W: DetectorWriter,
{
    fn name(&self) -> &str {
        &self.name
    }
    async fn trigger(&self) -> Status {
        // For step scans: arm → wait_for_idle, then return done.
        let arm = self.control.arm().await;
        match arm.await {
            Ok(()) => match self.control.wait_for_idle().await {
                Ok(()) => Status::done(),
                Err(e) => Status::fail(cirrus_core::status::StatusError::Failed(format!(
                    "wait_for_idle: {e}"
                ))),
            },
            Err(e) => Status::fail(e),
        }
    }
}

#[async_trait]
impl<C, W> Flyable for StandardDetector<C, W>
where
    C: DetectorControl,
    W: DetectorWriter,
{
    fn name(&self) -> &str {
        &self.name
    }
    async fn kickoff(&self) -> Status {
        self.control.arm().await
    }
    async fn complete(&self) -> Status {
        // Wait for indices_written to reach cached_target, or until disarm.
        let target = self.cached_target.load(Ordering::SeqCst);
        let mut rx = self.writer.observe_indices_written();
        let fut = async {
            while *rx.borrow_and_update() < target {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        };
        fut.await;
        match self.control.wait_for_idle().await {
            Ok(()) => Status::done(),
            Err(e) => Status::fail(cirrus_core::status::StatusError::Failed(format!(
                "wait_for_idle: {e}"
            ))),
        }
    }
}

#[async_trait]
impl<C, W> WritesStreamAssets for StandardDetector<C, W>
where
    C: DetectorControl,
    W: DetectorWriter,
{
    fn name(&self) -> &str {
        &self.name
    }
    async fn get_index(&self) -> Result<u64> {
        Ok(self.writer.indices_written().await)
    }
    fn collect_asset_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset> {
        self.writer.collect_stream_docs(up_to, descriptor)
    }
}

/// Helper to expose the writer's `data_keys` after `open`.
pub async fn open_writer<W: DetectorWriter>(
    w: &W,
    multiplier: u32,
) -> Result<HashMap<String, DataKey>> {
    w.open(multiplier).await
}

// -- bridges from StandardDetector to engine `*Obj` traits --------------

#[async_trait]
impl<C, W> NamedObj for StandardDetector<C, W>
where
    C: DetectorControl + Send + Sync + 'static,
    W: DetectorWriter + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "StandardDetector",
            "control": std::any::type_name::<C>(),
            "writer": std::any::type_name::<W>(),
        })
    }
}

#[async_trait]
impl<C, W> StageableObj for StandardDetector<C, W>
where
    C: DetectorControl + Send + Sync + 'static,
    W: DetectorWriter + Send + Sync + 'static,
{
    async fn stage_dyn(&self) -> Result<()> {
        Stageable::stage(self).await
    }
    async fn unstage_dyn(&self) -> Result<()> {
        Stageable::unstage(self).await
    }
}

#[async_trait]
impl<C, W> TriggerableObj for StandardDetector<C, W>
where
    C: DetectorControl + Send + Sync + 'static,
    W: DetectorWriter + Send + Sync + 'static,
{
    async fn trigger_dyn(&self) -> Status {
        Triggerable::trigger(self).await
    }
}

#[async_trait]
impl<C, W> FlyableObj for StandardDetector<C, W>
where
    C: DetectorControl + Send + Sync + 'static,
    W: DetectorWriter + Send + Sync + 'static,
{
    async fn kickoff_dyn(&self) -> Status {
        Flyable::kickoff(self).await
    }
    async fn complete_dyn(&self) -> Status {
        Flyable::complete(self).await
    }
}

/// `Collectable` impl for `StandardDetector` — translates the writer's
/// `collect_stream_docs` into engine-visible `(stream, data, ts)` rows.
#[async_trait]
impl<C, W> CollectableObj for StandardDetector<C, W>
where
    C: DetectorControl + Send + Sync + 'static,
    W: DetectorWriter + Send + Sync + 'static,
{
    async fn describe_collect_dyn(&self) -> Result<HashMap<String, HashMap<String, DataKey>>> {
        let dks = WritesStreamAssets::name(self).to_string();
        let _ = dks; // unused
        let dk = self.writer.open(1).await?;
        let mut out = HashMap::new();
        out.insert(self.name.clone(), dk);
        Ok(out)
    }

    async fn collect_dyn(
        &self,
    ) -> Result<Vec<(String, HashMap<String, Value>, HashMap<String, f64>)>> {
        // Emit one summary event with the current index. The writer's
        // StreamResource/StreamDatum are drained separately via
        // `collect_stream_docs_dyn` (driven by the engine, which supplies the
        // composed descriptor UID) — draining them here too would
        // double-consume the writer's `last_emitted` and silently drop the
        // stream docs.
        let up_to = WritesStreamAssets::get_index(self).await?;
        let mut data = HashMap::new();
        data.insert(format!("{}_index", self.name), Value::from(up_to));
        let mut ts = HashMap::new();
        ts.insert(format!("{}_index", self.name), 0.0);
        Ok(vec![(self.name.clone(), data, ts)])
    }

    async fn collect_stream_docs_dyn(
        &self,
        descriptor: &str,
    ) -> Result<Vec<cirrus_event_model::Document>> {
        let up_to = WritesStreamAssets::get_index(self).await?;
        let docs = WritesStreamAssets::collect_asset_docs(self, up_to, descriptor)
            .map(|asset| match asset {
                StreamAsset::Resource(r) => cirrus_event_model::Document::StreamResource(r),
                StreamAsset::Datum(d) => cirrus_event_model::Document::StreamDatum(d),
            })
            .collect::<Vec<_>>()
            .await;
        Ok(docs)
    }
}

#[async_trait]
impl<C, W> ReadableObj for StandardDetector<C, W>
where
    C: DetectorControl + Send + Sync + 'static,
    W: DetectorWriter + Send + Sync + 'static,
{
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        let mut out = HashMap::new();
        let idx = WritesStreamAssets::get_index(self).await?;
        out.insert(
            format!("{}_index", self.name),
            ReadingValue {
                value: Value::from(idx),
                timestamp: 0.0,
                alarm_severity: None,
                message: None,
            },
        );
        Ok(out)
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        self.writer.open(1).await
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![format!("{}_index", self.name)])
    }
}
