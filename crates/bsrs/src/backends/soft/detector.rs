//! Soft detector — fake counts on every trigger; soft writer emits in-memory frames.

use crate::core::error::{BsrsError, Result};
use crate::core::msg::{NamedObj, ReadableObj};
use crate::core::reading::ReadingValue;
use crate::core::status::Status;
use crate::devices::StandardDetector;
use crate::event_model::{DataKey, Dtype};
use crate::protocols_async::{
    AsyncReadable, DetectorControl, DetectorTrigger, DetectorWriter, StreamAsset, TriggerInfo,
};
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

/// Fake-counts detector implementing `AsyncReadable` directly (step scans).
pub struct SoftDetector {
    name: String,
    counts: AtomicU64,
}

impl SoftDetector {
    /// Build with an initial counter.
    pub fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            counts: AtomicU64::new(0),
        })
    }

    /// Bump the counter.
    pub fn tick(&self) {
        self.counts.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl NamedObj for SoftDetector {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "SoftDetector",
            "counts": self.counts.load(Ordering::SeqCst),
            "data_key": format!("{}_counts", self.name),
            "connected": true,
        })
    }
}

#[async_trait]
impl AsyncReadable for SoftDetector {
    fn name(&self) -> &str {
        &self.name
    }
    async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        let v = self.counts.load(Ordering::SeqCst);
        let mut out = HashMap::new();
        out.insert(
            format!("{}_counts", self.name),
            ReadingValue {
                value: serde_json::Value::Number(v.into()),
                timestamp: now_ts(),
                alarm_severity: None,
                message: None,
            },
        );
        Ok(out)
    }
    async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        let mut out = HashMap::new();
        out.insert(
            format!("{}_counts", self.name),
            DataKey {
                source: format!("soft://{}/counts", self.name),
                dtype: Dtype::Integer,
                shape: vec![],
                dtype_numpy: Some("<i8".into()),
                external: None,
                units: Some("counts".into()),
                precision: None,
                object_name: Some(self.name.clone()),
                dims: None,
                limits: None,
                choices: None,
            },
        );
        Ok(out)
    }
}

#[async_trait]
impl ReadableObj for SoftDetector {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        AsyncReadable::read(self).await
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        AsyncReadable::describe(self).await
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![format!("{}_counts", self.name)])
    }
}

// -- StandardDetector parts --------------------------------------------------

/// Soft `DetectorControl` — every `arm` increments an internal counter that
/// also drives the writer's index (when paired in `SoftDetector`).
pub struct SoftDetectorControl {
    deadtime: Duration,
    arm_count: Arc<AtomicU64>,
    target: Arc<AtomicU64>,
    index_tx: Arc<watch::Sender<u64>>,
}

impl SoftDetectorControl {
    /// Build with a fixed deadtime.
    pub fn new(deadtime: Duration) -> Self {
        let (tx, _rx) = watch::channel(0u64);
        Self {
            deadtime,
            arm_count: Arc::new(AtomicU64::new(0)),
            target: Arc::new(AtomicU64::new(1)),
            index_tx: Arc::new(tx),
        }
    }

    /// Shared handle the writer uses to know how many frames have been "armed".
    pub fn arm_count(&self) -> Arc<AtomicU64> {
        self.arm_count.clone()
    }

    /// Shared target frame count.
    pub fn target(&self) -> Arc<AtomicU64> {
        self.target.clone()
    }

    /// Subscribe to the index watch channel driven by `arm()`.
    pub fn subscribe_index(&self) -> watch::Receiver<u64> {
        self.index_tx.subscribe()
    }
}

#[async_trait]
impl DetectorControl for SoftDetectorControl {
    fn deadtime(&self, _exposure: Option<Duration>) -> Duration {
        self.deadtime
    }
    async fn prepare(&self, info: TriggerInfo) -> Result<()> {
        // The soft detector self-times its exposures on arm — it has no
        // external input, so only internal triggering is supported (mirrors
        // ophyd-async logic raising NotImplementedError for unsupported modes).
        if info.trigger != DetectorTrigger::Internal {
            return Err(BsrsError::Backend(format!(
                "soft detector supports only DetectorTrigger::Internal, got {:?}",
                info.trigger
            )));
        }
        self.target
            .store(info.number_of_events as u64, Ordering::SeqCst);
        Ok(())
    }
    async fn arm(&self) -> Status {
        let new = self.arm_count.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.index_tx.send(new);
        Status::done()
    }
    async fn wait_for_idle(&self) -> Result<()> {
        // soft detector is instantly idle
        Ok(())
    }
    async fn disarm(&self) -> Result<()> {
        Ok(())
    }
}

/// Soft `DetectorWriter` — keeps a vec of frame timestamps in memory and emits
/// `StreamResource` + `StreamDatum` documents.
pub struct SoftDetectorWriter {
    name: String,
    indices_rx: watch::Receiver<u64>,
    counter: Arc<AtomicU64>,
    /// Tracks whether we already emitted the StreamResource.
    resource_emitted: std::sync::Mutex<Option<String>>,
    /// Index of last emitted StreamDatum (frames `[0, last_emitted)`).
    last_emitted: AtomicU64,
    /// Mimetype label (for tests).
    mimetype: String,
    /// URI label (for tests).
    uri: String,
    /// Compose handle. Set by `bind_to_run` before use.
    compose: tokio::sync::Mutex<Option<Arc<crate::event_model::compose::RunBundle>>>,
}

impl SoftDetectorWriter {
    /// Build with a counter handle and a watch receiver driven by the paired
    /// `SoftDetectorControl` (obtained via `SoftDetectorControl::subscribe_index`).
    pub fn new(
        name: impl Into<String>,
        counter: Arc<AtomicU64>,
        indices_rx: watch::Receiver<u64>,
    ) -> Self {
        Self {
            name: name.into(),
            indices_rx,
            counter,
            resource_emitted: std::sync::Mutex::new(None),
            last_emitted: AtomicU64::new(0),
            mimetype: "application/x-bsrs-soft-frames".into(),
            uri: "memory://soft-frames".into(),
            compose: tokio::sync::Mutex::new(None),
        }
    }

    /// Bind to a run's compose handle so emitted documents reference the
    /// correct run UID.
    pub async fn bind_to_run(&self, compose: Arc<crate::event_model::compose::RunBundle>) {
        *self.compose.lock().await = Some(compose);
    }
}

#[async_trait]
impl DetectorWriter for SoftDetectorWriter {
    async fn open(&self, _multiplier: u32) -> Result<HashMap<String, DataKey>> {
        let mut out = HashMap::new();
        out.insert(
            format!("{}_image", self.name),
            DataKey {
                source: format!("soft://{}/image", self.name),
                dtype: Dtype::Number,
                shape: vec![Some(1)],
                dtype_numpy: Some("<f4".into()),
                external: Some("STREAM:".into()),
                units: None,
                precision: None,
                object_name: Some(self.name.clone()),
                dims: Some(vec!["pixel".into()]),
                limits: None,
                choices: None,
            },
        );
        Ok(out)
    }
    fn observe_indices_written(&self) -> watch::Receiver<u64> {
        self.indices_rx.clone()
    }
    async fn indices_written(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
    fn collect_stream_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset> {
        let mut docs: Vec<StreamAsset> = Vec::new();
        // Resource (only once)
        let resource_uid = {
            let mut guard = self.resource_emitted.lock().unwrap();
            if let Some(u) = guard.clone() {
                u
            } else {
                let new_uid = uuid::Uuid::new_v4().to_string();
                *guard = Some(new_uid.clone());
                let resource = crate::event_model::StreamResource {
                    uid: new_uid.clone(),
                    data_key: format!("{}_image", self.name),
                    mimetype: self.mimetype.clone(),
                    uri: self.uri.clone(),
                    parameters: Default::default(),
                    run_start: None,
                };
                docs.push(StreamAsset::Resource(resource));
                new_uid
            }
        };
        // Datum
        let last = self.last_emitted.load(Ordering::SeqCst);
        if up_to > last {
            let datum = crate::event_model::StreamDatum {
                uid: uuid::Uuid::new_v4().to_string(),
                stream_resource: resource_uid,
                descriptor: descriptor.to_string(),
                indices: crate::event_model::StreamRange {
                    start: last,
                    stop: up_to,
                },
                seq_nums: crate::event_model::StreamRange {
                    start: last + 1,
                    stop: up_to + 1,
                },
            };
            self.last_emitted.store(up_to, Ordering::SeqCst);
            docs.push(StreamAsset::Datum(datum));
        }
        stream::iter(docs).boxed()
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Convenience constructor for a `StandardDetector` backed by soft control + writer.
pub fn soft_detector(
    name: impl Into<String>,
) -> StandardDetector<SoftDetectorControl, SoftDetectorWriter> {
    let control = SoftDetectorControl::new(Duration::from_micros(0));
    let counter = control.arm_count();
    let indices_rx = control.subscribe_index();
    let name: String = name.into();
    let writer = SoftDetectorWriter::new(name.clone(), counter, indices_rx);
    StandardDetector::new(name, control, writer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_info_defaults_to_internal() {
        // DB-01: TriggerInfo carries a trigger mode, defaulting to Internal
        // (matches ophyd-async `DetectorTrigger.INTERNAL`).
        assert_eq!(TriggerInfo::default().trigger, DetectorTrigger::Internal);
    }

    #[tokio::test]
    async fn soft_prepare_accepts_internal_and_sets_target() {
        let control = SoftDetectorControl::new(Duration::from_micros(0));
        let info = TriggerInfo {
            number_of_events: 7,
            ..Default::default()
        };
        DetectorControl::prepare(&control, info).await.unwrap();
        assert_eq!(control.target().load(Ordering::SeqCst), 7);
    }

    #[tokio::test]
    async fn soft_prepare_rejects_external_trigger() {
        // The soft detector is internal-only; external modes are unsupported.
        let control = SoftDetectorControl::new(Duration::from_micros(0));
        let info = TriggerInfo {
            trigger: DetectorTrigger::ExternalEdge,
            ..Default::default()
        };
        let err = DetectorControl::prepare(&control, info).await.unwrap_err();
        assert!(format!("{err}").contains("Internal"), "err: {err}");
    }
}
