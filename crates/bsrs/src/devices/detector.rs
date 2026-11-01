//! `StandardDetector<C, W>` — composition of `DetectorControl` + `DetectorWriter`.

use crate::core::error::{BsrsError, Result};
use crate::core::msg::{
    CollectableObj, ConfigurableObj, FlyableObj, NamedObj, ReadableObj, StageableObj,
    TriggerableObj,
};
use crate::core::reading::ReadingValue;
use crate::core::status::Status;
use crate::event_model::DataKey;
use crate::protocols_async::{
    DetectorControl, DetectorWriter, Flyable, Preparable, Stageable, StreamAsset, Triggerable,
    WritesStreamAssets,
};
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Re-export so users get `TriggerInfo` / `DetectorTrigger` straight from
/// bsrs-devices.
pub use crate::protocols_async::{DetectorTrigger, TriggerInfo};

/// ophyd-async `_PrepareCtx`: what the last prepare since stage set up.
#[derive(Clone)]
struct PrepareCtx {
    trigger_info: TriggerInfo,
    // Writer index at that prepare (refreshed by trigger); the prepared
    // collections end at this + `trigger_info.number_of_collections()`.
    collections_written: u64,
}

/// ophyd-async `_KickoffCtx`: what the last kickoff asked `complete` to wait
/// for.
#[derive(Clone, Copy)]
struct KickoffCtx {
    collections_written: u64,
    collections_requested: u64,
    // Only the kickoff that reaches the prepared end waits for idle.
    is_last_kickoff: bool,
}

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
    // `None` after stage, as in ophyd-async.
    prepared: std::sync::Mutex<Option<PrepareCtx>>,
    kicked_off: std::sync::Mutex<Option<KickoffCtx>>,
    // ophyd-async `events_to_kickoff`: `number_of_events` at prepare, 0 at
    // stage; a plan may lower it to split the prepared events over kickoffs.
    events_to_kickoff: AtomicU64,
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
            prepared: std::sync::Mutex::new(None),
            kicked_off: std::sync::Mutex::new(None),
            events_to_kickoff: AtomicU64::new(0),
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

    /// Events the next `kickoff` acquires (ophyd-async `events_to_kickoff`).
    pub fn events_to_kickoff(&self) -> u64 {
        self.events_to_kickoff.load(Ordering::SeqCst)
    }

    /// Set the events the next `kickoff` acquires; a kickoff past the
    /// prepared events fails.
    pub fn set_events_to_kickoff(&self, events: u64) {
        self.events_to_kickoff.store(events, Ordering::SeqCst);
    }

    /// Read the DataKeys cached when the writer was opened at `stage()`.
    /// Errors with `BsrsError::State` if the detector has not been staged
    /// (the writer was never opened), so `describe` never re-opens it (DB-04).
    fn cached_data_keys(&self) -> Result<HashMap<String, DataKey>> {
        self.opened.lock().unwrap().clone().ok_or_else(|| {
            BsrsError::State(format!(
                "{}: describe before stage (writer not opened)",
                self.name
            ))
        })
    }

    /// ophyd-async `StandardDetector.prepare`: set the hardware up for `info`,
    /// then record it as this scan's prepare.
    async fn prepare_for(&self, info: TriggerInfo) -> Result<PrepareCtx> {
        self.control.prepare(info.clone()).await?;
        let ctx = self.update_prepare_context(info).await;
        self.set_events_to_kickoff(ctx.trigger_info.number_of_events.into());
        Ok(ctx)
    }

    /// ophyd-async `_update_prepare_context`: record `info` with the writer
    /// index now as its baseline.
    async fn update_prepare_context(&self, trigger_info: TriggerInfo) -> PrepareCtx {
        let ctx = PrepareCtx {
            trigger_info,
            collections_written: self.writer.indices_written().await,
        };
        *self.prepared.lock().unwrap() = Some(ctx.clone());
        ctx
    }

    /// The prepare since stage, or an implicit `TriggerInfo::default()` one
    /// (ophyd-async `trigger`; `kickoff` raises there instead).
    async fn prepared_or_default(&self) -> Result<PrepareCtx> {
        let prepared = self.prepared.lock().unwrap().clone();
        match prepared {
            Some(ctx) => Ok(ctx),
            None => self.prepare_for(TriggerInfo::default()).await,
        }
    }

    /// ophyd-async `StandardDetector.trigger`: one event of the prepare since
    /// stage, or of an implicit `TriggerInfo()` one.
    async fn trigger_one(&self) -> Result<()> {
        let prepared = self.prepared.lock().unwrap().clone();
        match prepared {
            None => {
                self.prepare_for(TriggerInfo::default()).await?;
            }
            Some(ctx) if ctx.trigger_info.number_of_events != 1 => {
                return Err(BsrsError::Plan(format!(
                    "{}: trigger() is not supported for multiple events, the detector \
                     was prepared with number_of_events={}.",
                    self.name, ctx.trigger_info.number_of_events
                )));
            }
            Some(ctx) => {
                self.update_prepare_context(ctx.trigger_info).await;
            }
        }
        self.control.arm().await.await?;
        self.control.wait_for_idle().await
    }

    /// ophyd-async `StandardDetector.kickoff` up to starting the acquisition:
    /// `events_to_kickoff` events from the index now, within the prepare.
    async fn kickoff_ctx(&self) -> Result<KickoffCtx> {
        let ctx = self.prepared_or_default().await?;
        let collections_written = self.writer.indices_written().await;
        let collections_requested =
            self.events_to_kickoff() * u64::from(ctx.trigger_info.collections_per_event);
        let last_requested = collections_written + collections_requested;
        let last_expected =
            ctx.collections_written + u64::from(ctx.trigger_info.number_of_collections());
        if last_requested > last_expected {
            return Err(BsrsError::State(format!(
                "{}: Kickoff requested {collections_written}:{last_requested}, but \
                 detector was only prepared up to {last_expected}",
                self.name
            )));
        }
        let kickoff = KickoffCtx {
            collections_written,
            collections_requested,
            is_last_kickoff: last_requested == last_expected,
        };
        *self.kicked_off.lock().unwrap() = Some(kickoff);
        Ok(kickoff)
    }

    /// Drain the writer's buffered `StreamResource`/`StreamDatum` for frames up
    /// to the current write index, mapped to `Document`s and each `StreamDatum`
    /// stamped with `descriptor`. Shared by the fly path
    /// ([`CollectableObj::collect_stream_docs_dyn`]) and the step path
    /// ([`ReadableObj::collect_asset_docs_dyn`]); a given scan drives exactly
    /// one of them, so the writer's `last_emitted` cursor is never
    /// double-consumed.
    ///
    /// [`CollectableObj::collect_stream_docs_dyn`]: crate::core::msg::CollectableObj::collect_stream_docs_dyn
    /// [`ReadableObj::collect_asset_docs_dyn`]: crate::core::msg::ReadableObj::collect_asset_docs_dyn
    async fn drain_asset_documents(
        &self,
        descriptor: &str,
    ) -> Result<Vec<crate::event_model::Document>> {
        let up_to = WritesStreamAssets::get_index(self).await?;
        let docs = WritesStreamAssets::collect_asset_docs(self, up_to, descriptor)
            .map(|asset| match asset {
                StreamAsset::Resource(r) => crate::event_model::Document::StreamResource(r),
                StreamAsset::Datum(d) => crate::event_model::Document::StreamDatum(d),
            })
            .collect::<Vec<_>>()
            .await;
        Ok(docs)
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
        // ophyd-async `stage` drops the prepare and kickoff contexts: they
        // belong to the scan they were made for.
        *self.prepared.lock().unwrap() = None;
        *self.kicked_off.lock().unwrap() = None;
        self.set_events_to_kickoff(0);
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
        match self.trigger_one().await {
            Ok(()) => Status::done(),
            Err(e) => Status::fail(crate::core::status::StatusError::Failed(format!(
                "trigger: {e}"
            ))),
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
        // The hardware was set up by prepare, as in ophyd-async.
        if let Err(e) = self.kickoff_ctx().await {
            return Status::fail(crate::core::status::StatusError::Failed(format!(
                "kickoff: {e}"
            )));
        }
        self.control.arm().await
    }
    async fn complete(&self) -> Status {
        let Some(kickoff) = *self.kicked_off.lock().unwrap() else {
            return Status::fail(crate::core::status::StatusError::Failed(format!(
                "{}: Kickoff not called",
                self.name
            )));
        };
        let target = kickoff.collections_written + kickoff.collections_requested;
        let mut rx = self.writer.observe_indices_written();
        let fut = async {
            while *rx.borrow_and_update() < target {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        };
        fut.await;
        if !kickoff.is_last_kickoff {
            return Status::done();
        }
        match self.control.wait_for_idle().await {
            Ok(()) => Status::done(),
            Err(e) => Status::fail(crate::core::status::StatusError::Failed(format!(
                "wait_for_idle: {e}"
            ))),
        }
    }
}

/// Set the hardware up for `TriggerInfo` and keep it for the scan's
/// `trigger`/`kickoff`, as ophyd-async `StandardDetector.prepare` does.
#[async_trait]
impl<C, W> Preparable<TriggerInfo> for StandardDetector<C, W>
where
    C: DetectorControl,
    W: DetectorWriter,
{
    fn name(&self) -> &str {
        &self.name
    }
    async fn prepare(&self, info: TriggerInfo) -> crate::core::status::Status {
        match self.prepare_for(info).await {
            Ok(_) => Status::done(),
            Err(e) => Status::fail(crate::core::status::StatusError::Failed(format!(
                "prepare: {e}"
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
    ) -> Result<Vec<crate::event_model::Document>> {
        self.drain_asset_documents(descriptor).await
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        ReadableObj::hint_fields(self)
    }
    fn as_configurable(&self) -> Option<&dyn ConfigurableObj> {
        Some(self)
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
    fn as_stageable(self: std::sync::Arc<Self>) -> Option<std::sync::Arc<dyn StageableObj>> {
        Some(self)
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![format!("{}_index", self.name)])
    }
    fn writes_external_assets(&self) -> bool {
        true
    }
    async fn collect_asset_docs_dyn(
        &self,
        descriptor: &str,
    ) -> Result<Vec<crate::event_model::Document>> {
        self.drain_asset_documents(descriptor).await
    }
    fn as_configurable(&self) -> Option<&dyn ConfigurableObj> {
        Some(self)
    }
}

/// Configuration = the control half's slow-changing acquisition parameters
/// (ophyd-async `AreaDetector` adds `driver.acquire_time`/`acquire_period`
/// as CONFIG_SIGNALs; controls without config parameters contribute none).
#[async_trait]
impl<C, W> ConfigurableObj for StandardDetector<C, W>
where
    C: DetectorControl + Send + Sync + 'static,
    W: DetectorWriter + Send + Sync + 'static,
{
    async fn read_configuration_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        self.control.read_configuration(&self.name).await
    }
    async fn describe_configuration_dyn(&self) -> Result<HashMap<String, DataKey>> {
        self.control.describe_configuration(&self.name).await
    }
    async fn configure_dyn(&self, _args: crate::core::ConfigureArgs) -> Result<()> {
        // Acquisition parameters are set through `prepare(TriggerInfo)`, not
        // a generic configure — same as ophyd-async, whose detectors expose
        // config signals read-only on the descriptor path.
        Err(BsrsError::State(format!(
            "{}: StandardDetector is configured via prepare(TriggerInfo), not configure()",
            self.name
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols_async::Preparable;
    use futures::stream;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;

    /// Counts how many times `disarm()` fired so a test can assert that
    /// `stage()` returns the detector to idle (DB-03).
    struct RecordingControl {
        disarms: Arc<AtomicU64>,
        idles: Arc<AtomicU64>,
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
            self.idles.fetch_add(1, Ordering::SeqCst);
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
            idles: Arc::default(),
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
        // Regression: complete() used to wait for a target that was never set
        // and returned immediately; it must block until the writer reaches
        // the kickoff baseline + the requested collections.
        let (tx, rx) = watch::channel(0u64);
        let disarms = Arc::new(AtomicU64::new(0));
        let det = Arc::new(StandardDetector::new(
            "d",
            RecordingControl {
                disarms: disarms.clone(),
                idles: Arc::default(),
            },
            ControlledWriter { rx },
        ));

        // Stage, prepare for 3 frames, then kickoff.
        Stageable::stage(&*det).await.unwrap();
        Preparable::<TriggerInfo>::prepare(
            &*det,
            TriggerInfo {
                number_of_events: 3,
                ..Default::default()
            },
        )
        .await;
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

    /// A detector whose index `tx` drives, and its count of `wait_for_idle`.
    fn controlled() -> (
        StandardDetector<RecordingControl, ControlledWriter>,
        watch::Sender<u64>,
        Arc<AtomicU64>,
    ) {
        let (tx, rx) = watch::channel(0u64);
        let idles = Arc::new(AtomicU64::new(0));
        let control = RecordingControl {
            disarms: Arc::default(),
            idles: idles.clone(),
        };
        (
            StandardDetector::new("det", control, ControlledWriter { rx }),
            tx,
            idles,
        )
    }

    async fn prepare_events(det: &StandardDetector<RecordingControl, ControlledWriter>, n: u32) {
        let info = TriggerInfo {
            number_of_events: n,
            ..Default::default()
        };
        Preparable::<TriggerInfo>::prepare(det, info)
            .await
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn kickoff_respects_prepare_bounds() {
        // ophyd-async test_standard_detector.py:543.
        let (det, tx, _) = controlled();
        Stageable::stage(&det).await.unwrap();
        prepare_events(&det, 5).await;
        det.set_events_to_kickoff(3);
        Flyable::kickoff(&det).await.await.unwrap();
        tx.send(3).unwrap();
        det.set_events_to_kickoff(2);
        Flyable::kickoff(&det).await.await.unwrap();
        tx.send(5).unwrap();
        det.set_events_to_kickoff(1);
        let err = Flyable::kickoff(&det).await.await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Kickoff requested 5:6, but detector was only prepared up to 5"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn complete_waits_for_idle_only_after_the_last_kickoff() {
        // ophyd-async `kickoff` sets `is_last_kickoff`; `complete` passes it
        // as `wait_for_idle`.
        let (det, tx, idles) = controlled();
        Stageable::stage(&det).await.unwrap();
        prepare_events(&det, 5).await;
        det.set_events_to_kickoff(3);
        Flyable::kickoff(&det).await.await.unwrap();
        tx.send(3).unwrap();
        Flyable::complete(&det).await.await.unwrap();
        assert_eq!(idles.load(Ordering::SeqCst), 0, "3 of 5 events");
        det.set_events_to_kickoff(2);
        Flyable::kickoff(&det).await.await.unwrap();
        tx.send(5).unwrap();
        Flyable::complete(&det).await.await.unwrap();
        assert_eq!(idles.load(Ordering::SeqCst), 1, "5 of 5 events");
    }

    #[tokio::test]
    async fn stage_resets_the_prepare_and_kickoff() {
        // ophyd-async test_standard_detector.py:574.
        let (det, _tx, _) = controlled();
        Stageable::stage(&det).await.unwrap();
        prepare_events(&det, 5).await;
        det.set_events_to_kickoff(3);
        Flyable::kickoff(&det).await.await.unwrap();
        Stageable::stage(&det).await.unwrap();
        assert_eq!(det.events_to_kickoff(), 0);
        let err = Flyable::complete(&det).await.await.unwrap_err();
        assert!(err.to_string().contains("Kickoff not called"), "{err}");
    }
}
