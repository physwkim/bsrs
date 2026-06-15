//! `StandardDetector<C, W>` — composition of `DetectorControl` + `DetectorWriter`.

use async_trait::async_trait;
use cirrus_core::error::{CirrusError, Result};
use cirrus_core::msg::{
    CollectableObj, FlyableObj, NamedObj, ReadableObj, StageableObj, TriggerableObj,
};
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::Status;
use cirrus_event_model::DataKey;
use cirrus_protocols_async::{
    DetectorControl, DetectorWriter, Flyable, Preparable, Stageable, StreamAsset, Triggerable,
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
    // TriggerInfo stored by Preparable::prepare(); kickoff() reads it to
    // configure the hardware and compute the absolute target index.
    // Defaults to TriggerInfo::default() so a bare kickoff() without an
    // explicit prepare() still acquires one frame (internal trigger).
    cached_trigger_info: std::sync::Mutex<TriggerInfo>,
    // Absolute writer index that complete() waits for. Set in kickoff()
    // from the current writer baseline + number_of_collections().
    cached_target: AtomicU64,
    // DataKeys captured when the writer is opened at `stage()`. `describe`
    // reads from this cache rather than re-opening the writer mid-acquisition
    // (DB-04); `None` until staged.
    opened: std::sync::Mutex<Option<HashMap<String, DataKey>>>,
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
            cached_trigger_info: std::sync::Mutex::new(TriggerInfo::default()),
            cached_target: AtomicU64::new(0),
            opened: std::sync::Mutex::new(None),
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

    /// Read the DataKeys cached when the writer was opened at `stage()`.
    /// Errors with `CirrusError::State` if the detector has not been staged
    /// (the writer was never opened), so `describe` never re-opens it (DB-04).
    fn cached_data_keys(&self) -> Result<HashMap<String, DataKey>> {
        self.opened.lock().unwrap().clone().ok_or_else(|| {
            CirrusError::State(format!(
                "{}: describe before stage (writer not opened)",
                self.name
            ))
        })
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
        // A detector left armed by a previous (possibly aborted) scan must be
        // returned to idle before a new scan begins, or it can free-run into
        // the next acquisition. Mirrors ophyd-async `StandardDetector.stage()`
        // calling `_disarm_and_stop(on_unstage=False)`.
        self.control.disarm().await?;
        // Ready the writer for this scan with multiplier=1 by default; plans
        // can call configure() to change this. Cache the DataKeys the writer
        // reports so describe() is a pure read and never re-opens the writer
        // mid-acquisition (DB-04). (Relocating this open() into prepare() is
        // DB-15's scope.)
        let data_keys = self.writer.open(1).await?;
        *self.opened.lock().unwrap() = Some(data_keys);
        Ok(())
    }
    async fn unstage(&self) -> Result<()> {
        self.control.disarm().await?;
        self.writer.close().await?;
        // Drop the cached DataKeys so a subsequent re-stage re-opens the writer.
        *self.opened.lock().unwrap() = None;
        Ok(())
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
        let info = self.cached_trigger_info.lock().unwrap().clone();
        if let Err(e) = self.control.prepare(info.clone()).await {
            return Status::fail(cirrus_core::status::StatusError::Failed(format!(
                "prepare: {e}"
            )));
        }
        // Capture the write baseline so complete() waits for exactly
        // number_of_collections NEW frames, not an absolute total.
        let baseline = self.writer.indices_written().await;
        self.cached_target.store(
            baseline + info.number_of_collections() as u64,
            Ordering::SeqCst,
        );
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

/// Store `TriggerInfo` so the next `kickoff()` configures the hardware and
/// waits for the correct number of frames. Mirrors Python
/// `StandardDetector.prepare(trigger_info)` which stores `_prepare_ctx`.
///
/// Hardware setup (`DetectorControl::prepare`) is deferred to `kickoff()` so
/// the trigger-info can be updated multiple times before arming without
/// issuing redundant hardware transactions.
#[async_trait]
impl<C, W> Preparable<TriggerInfo> for StandardDetector<C, W>
where
    C: DetectorControl,
    W: DetectorWriter,
{
    fn name(&self) -> &str {
        &self.name
    }
    async fn prepare(&self, info: TriggerInfo) -> cirrus_core::status::Status {
        *self.cached_trigger_info.lock().unwrap() = info;
        cirrus_core::status::Status::done()
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
        // Read the DataKeys cached when the writer was opened at stage(); never
        // re-open the writer here (DB-04) — that would re-emit a StreamResource
        // with a new uid and disturb the in-progress frame counter.
        let data_keys = self.cached_data_keys()?;
        let mut out = HashMap::new();
        out.insert(self.name.clone(), data_keys);
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
        // Read cached DataKeys, never re-open the writer (DB-04).
        self.cached_data_keys()
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![format!("{}_index", self.name)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cirrus_protocols_async::Preparable;
    use futures::stream;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;

    /// Counts how many times `disarm()` fired so a test can assert that
    /// `stage()` returns the detector to idle (DB-03).
    struct RecordingControl {
        disarms: Arc<AtomicU64>,
    }

    #[async_trait]
    impl DetectorControl for RecordingControl {
        fn deadtime(&self, _exposure: Option<Duration>) -> Duration {
            Duration::ZERO
        }
        async fn prepare(&self, _info: TriggerInfo) -> Result<()> {
            Ok(())
        }
        async fn arm(&self) -> Status {
            Status::done()
        }
        async fn wait_for_idle(&self) -> Result<()> {
            Ok(())
        }
        async fn disarm(&self) -> Result<()> {
            self.disarms.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Minimal writer that counts `open()` calls.
    struct CountingWriter {
        opens: Arc<AtomicU64>,
        rx: watch::Receiver<u64>,
    }

    #[async_trait]
    impl DetectorWriter for CountingWriter {
        async fn open(&self, _multiplier: u32) -> Result<HashMap<String, DataKey>> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(HashMap::new())
        }
        fn observe_indices_written(&self) -> watch::Receiver<u64> {
            self.rx.clone()
        }
        async fn indices_written(&self) -> u64 {
            0
        }
        fn collect_stream_docs(
            &self,
            _up_to: u64,
            _descriptor: &str,
        ) -> BoxStream<'_, StreamAsset> {
            stream::iter(Vec::new()).boxed()
        }
        async fn close(&self) -> Result<()> {
            Ok(())
        }
    }

    #[allow(clippy::type_complexity)]
    fn detector() -> (
        StandardDetector<RecordingControl, CountingWriter>,
        Arc<AtomicU64>,
        Arc<AtomicU64>,
    ) {
        let disarms = Arc::new(AtomicU64::new(0));
        let opens = Arc::new(AtomicU64::new(0));
        // The receiver keeps serving its last value after the sender drops;
        // this test never observes index changes, so the tx is discarded.
        let (_, rx) = watch::channel(0u64);
        let control = RecordingControl {
            disarms: disarms.clone(),
        };
        let writer = CountingWriter {
            opens: opens.clone(),
            rx,
        };
        (
            StandardDetector::new("det", control, writer),
            disarms,
            opens,
        )
    }

    #[tokio::test]
    async fn stage_disarms_before_readying_writer() {
        // DB-03: a detector left armed by a previous scan must be disarmed at
        // stage time. Before the fix `stage()` never called `disarm()`, so a
        // detector armed by a prior (aborted) scan free-ran into the next one.
        let (det, disarms, opens) = detector();
        Stageable::stage(&det).await.unwrap();
        assert_eq!(
            disarms.load(Ordering::SeqCst),
            1,
            "stage() must disarm exactly once"
        );
        assert_eq!(
            opens.load(Ordering::SeqCst),
            1,
            "stage() still readies the writer"
        );
    }

    #[tokio::test]
    async fn describe_reads_cache_without_reopening_writer() {
        // DB-04: describe must read the DataKeys cached at stage(), never
        // re-open the writer (which would re-emit a StreamResource with a new
        // uid and disturb the in-progress frame counter).
        let (det, _disarms, opens) = detector();

        // Before stage: describe is a device-state error, with no open().
        assert!(CollectableObj::describe_collect_dyn(&det).await.is_err());
        assert!(ReadableObj::describe_dyn(&det).await.is_err());
        assert_eq!(
            opens.load(Ordering::SeqCst),
            0,
            "describe before stage must not open the writer"
        );

        Stageable::stage(&det).await.unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 1, "stage opens exactly once");

        // Repeated describe calls read the cache; the open count stays at 1.
        CollectableObj::describe_collect_dyn(&det).await.unwrap();
        ReadableObj::describe_dyn(&det).await.unwrap();
        CollectableObj::describe_collect_dyn(&det).await.unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            1,
            "describe must not re-open the writer (DB-04)"
        );

        // After unstage the cache is cleared, so describe errors again.
        Stageable::unstage(&det).await.unwrap();
        assert!(ReadableObj::describe_dyn(&det).await.is_err());
    }

    /// A writer whose index is driven externally by a `watch::Sender<u64>`.
    struct ControlledWriter {
        rx: watch::Receiver<u64>,
    }

    #[async_trait]
    impl DetectorWriter for ControlledWriter {
        async fn open(&self, _multiplier: u32) -> Result<HashMap<String, DataKey>> {
            Ok(HashMap::new())
        }
        fn observe_indices_written(&self) -> watch::Receiver<u64> {
            self.rx.clone()
        }
        async fn indices_written(&self) -> u64 {
            *self.rx.borrow()
        }
        fn collect_stream_docs(
            &self,
            _up_to: u64,
            _descriptor: &str,
        ) -> BoxStream<'_, StreamAsset> {
            stream::iter(Vec::new()).boxed()
        }
        async fn close(&self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn complete_blocks_until_indices_written_reaches_target() {
        // Regression: complete() used to read cached_target == 0 (never set)
        // and return immediately regardless of how many frames were requested.
        // After the fix, kickoff() sets cached_target = baseline +
        // number_of_collections, and complete() genuinely blocks until the
        // writer reaches that count.
        let (tx, rx) = watch::channel(0u64);
        let disarms = Arc::new(AtomicU64::new(0));
        let det = Arc::new(StandardDetector::new(
            "d",
            RecordingControl {
                disarms: disarms.clone(),
            },
            ControlledWriter { rx },
        ));

        // Prepare for 3 frames, then stage and kickoff.
        Preparable::<TriggerInfo>::prepare(
            &*det,
            TriggerInfo {
                number_of_events: 3,
                ..Default::default()
            },
        )
        .await;
        Stageable::stage(&*det).await.unwrap();
        let _kickoff = Flyable::kickoff(&*det).await;

        // complete() must block — no frames yet.
        let det_c = det.clone();
        let complete = tokio::spawn(async move { Flyable::complete(&*det_c).await.success() });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !complete.is_finished(),
            "complete() must block when frames have not yet arrived (was target==0 bug)"
        );

        // Deliver 3 frames; complete() must unblock.
        tx.send(3).unwrap();

        let succeeded = tokio::time::timeout(Duration::from_secs(1), complete)
            .await
            .expect("complete() timed out after frames were delivered")
            .expect("task panicked");
        assert!(succeeded, "complete() should succeed after frames arrive");
    }
}
