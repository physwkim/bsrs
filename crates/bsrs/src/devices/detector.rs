//! `StandardDetector<C>` — composition of `DetectorControl` and its
//! [`DataLogic`]s.

use crate::core::error::{BsrsError, Result};
use crate::core::msg::{
    CollectableObj, ConfigurableObj, FlyableObj, NamedObj, ReadableObj, StageableObj,
    TriggerableObj,
};
use crate::core::reading::ReadingValue;
use crate::core::status::Status;
use crate::event_model::DataKey;
use crate::protocols_async::{
    DetectorControl, DetectorWriter, Flyable, Preparable, SingleDataLogic, Stageable, StreamAsset,
    Triggerable, WritesStreamAssets,
};
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Re-export so users get `TriggerInfo` / `DetectorTrigger` straight from
/// bsrs-devices.
pub use crate::protocols_async::{DetectorTrigger, TriggerInfo};

/// ophyd-async `DetectorDataLogic`, by the provider its prepare makes.
#[derive(Clone)]
pub enum DataLogic {
    /// `prepare_unbounded`: a writer, for step and fly scans.
    Unbounded(Arc<dyn DetectorWriter>),
    /// `prepare_single`: data read with the event, only for a prepare of
    /// one collection.
    Single(Arc<dyn SingleDataLogic>),
}

impl DataLogic {
    fn hinted_fields(&self) -> Vec<String> {
        match self {
            Self::Unbounded(w) => w.hinted_fields(),
            Self::Single(s) => s.hinted_fields(),
        }
    }

    async fn stop(&self) -> Result<()> {
        match self {
            Self::Unbounded(w) => w.close().await,
            Self::Single(s) => s.stop().await,
        }
    }
}

/// ophyd-async `_PrepareCtx`: what the last prepare since stage set up.
#[derive(Clone)]
struct PrepareCtx {
    trigger_info: TriggerInfo,
    // Collections the writers had written at that prepare (refreshed by
    // trigger); the prepared ones end at this +
    // `trigger_info.number_of_collections()`.
    collections_written: u64,
    // What the writers, opened for `trigger_info.collections_per_event`,
    // reported; `describe` reads these, never re-opening a writer.
    data_keys: HashMap<String, DataKey>,
    // The single data logics set up: none unless prepared for one
    // collection.
    singles: Vec<Arc<dyn SingleDataLogic>>,
}

/// ophyd-async `_KickoffCtx`: what the last kickoff asked `complete` to wait
/// for.
#[derive(Clone, Copy)]
struct KickoffCtx {
    collections_written: u64,
    collections_requested: u64,
    exposure_timeout: std::time::Duration,
    // Only the kickoff that reaches the prepared end waits for idle.
    is_last_kickoff: bool,
}

/// A detector composed of an arming half (`DetectorControl`) and the data
/// logics that write and read its data. Implements all eight bluesky
/// protocols by delegation.
pub struct StandardDetector<C>
where
    C: DetectorControl,
{
    name: String,
    control: C,
    // ophyd-async `_data_logics`, in the order added.
    data_logics: Vec<DataLogic>,
    // `None` after stage, as in ophyd-async.
    prepared: std::sync::Mutex<Option<PrepareCtx>>,
    kicked_off: std::sync::Mutex<Option<KickoffCtx>>,
    // ophyd-async `events_to_kickoff`: `number_of_events` at prepare, 0 at
    // stage; a plan may lower it to split the prepared events over kickoffs.
    events_to_kickoff: AtomicU64,
}

impl<C> StandardDetector<C>
where
    C: DetectorControl,
{
    /// Build a `StandardDetector` whose data `writer` writes.
    pub fn new(name: impl Into<String>, control: C, writer: impl DetectorWriter + 'static) -> Self {
        Self::with_data_logics(name, control, vec![DataLogic::Unbounded(Arc::new(writer))])
    }

    /// Build a `StandardDetector` over `data_logics`, which may be none.
    pub fn with_data_logics(
        name: impl Into<String>,
        control: C,
        data_logics: Vec<DataLogic>,
    ) -> Self {
        Self {
            name: name.into(),
            control,
            data_logics,
            prepared: std::sync::Mutex::new(None),
            kicked_off: std::sync::Mutex::new(None),
            events_to_kickoff: AtomicU64::new(0),
        }
    }

    /// Add a data logic (ophyd-async `add_detector_logics`).
    pub fn add_data_logic(&mut self, logic: DataLogic) {
        self.data_logics.push(logic);
    }

    /// Connect every writer.
    pub async fn connect_writers(&self, timeout: std::time::Duration) -> Result<()> {
        futures::future::try_join_all(self.writers().map(|w| w.connect(timeout))).await?;
        Ok(())
    }

    fn writers(&self) -> impl Iterator<Item = &Arc<dyn DetectorWriter>> {
        self.data_logics.iter().filter_map(|l| match l {
            DataLogic::Unbounded(w) => Some(w),
            DataLogic::Single(_) => None,
        })
    }

    /// ophyd-async `_get_collections_written` with `_all_the_same`: what
    /// every writer has written, which must agree; 0 with no writer.
    async fn collections_written(&self) -> Result<u64> {
        let written: BTreeSet<u64> =
            futures::future::join_all(self.writers().map(|w| w.indices_written()))
                .await
                .into_iter()
                .collect();
        match written.len() {
            0 | 1 => Ok(written.first().copied().unwrap_or(0)),
            _ => Err(BsrsError::State(format!(
                "{}: Detectors have written different numbers of collections: {written:?}",
                self.name
            ))),
        }
    }

    /// `_get_collections_written` with `reducer=min`.
    async fn min_collections_written(&self) -> u64 {
        futures::future::join_all(self.writers().map(|w| w.indices_written()))
            .await
            .into_iter()
            .min()
            .unwrap_or(0)
    }

    /// ophyd-async `DetectorDataLogic.stop` of every data logic, each run
    /// whether another fails.
    async fn stop_data_logics(&self) -> Result<()> {
        futures::future::join_all(self.data_logics.iter().map(DataLogic::stop))
            .await
            .into_iter()
            .collect()
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

    /// The prepare since stage; ophyd-async raises `{not_run}` without one.
    fn prepare_ctx(&self, not_run: &str) -> Result<PrepareCtx> {
        self.prepared
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| BsrsError::State(format!("{}: {not_run}", self.name)))
    }

    /// ophyd-async `StandardDetector.prepare`: set the hardware up for `info`,
    /// then record it as this scan's prepare.
    async fn prepare_for(&self, info: TriggerInfo) -> Result<PrepareCtx> {
        self.control.prepare(info.clone()).await?;
        let ctx = self.update_prepare_context(info).await?;
        self.set_events_to_kickoff(ctx.trigger_info.number_of_events.into());
        Ok(ctx)
    }

    /// ophyd-async `_update_prepare_context`: prepare the data logics for
    /// `collections_per_event` (kept while that is unchanged, since it shapes
    /// the DataKeys) and record `trigger_info` with the collections written
    /// now as its baseline.
    async fn update_prepare_context(&self, trigger_info: TriggerInfo) -> Result<PrepareCtx> {
        // Taken, not cloned: a failed re-open leaves no prepare behind.
        let previous = self.prepared.lock().unwrap().take();
        let (data_keys, singles) = match previous {
            Some(p)
                if p.trigger_info.collections_per_event == trigger_info.collections_per_event =>
            {
                (p.data_keys, p.singles)
            }
            previous => {
                if previous.is_some() {
                    self.stop_data_logics().await?;
                }
                self.prepare_data_logics(&trigger_info).await?
            }
        };
        let ctx = PrepareCtx {
            trigger_info,
            collections_written: self.collections_written().await?,
            data_keys,
            singles,
        };
        *self.prepared.lock().unwrap() = Some(ctx.clone());
        Ok(ctx)
    }

    /// Open each writer for `info.collections_per_event` and set up each
    /// single data logic if `info` is for one collection, as ophyd-async's
    /// `prepare_unbounded`/`prepare_single`; the writers' DataKeys and the
    /// single data logics set up.
    async fn prepare_data_logics(
        &self,
        info: &TriggerInfo,
    ) -> Result<(HashMap<String, DataKey>, Vec<Arc<dyn SingleDataLogic>>)> {
        let collections = info.number_of_collections();
        let mut opens = Vec::new();
        let mut singles = Vec::new();
        for logic in &self.data_logics {
            match logic {
                DataLogic::Unbounded(w) => opens.push(w.open(info.collections_per_event)),
                DataLogic::Single(s) if collections == 1 => singles.push(s.clone()),
                DataLogic::Single(_) => tracing::warn!(
                    "{}: a single data logic only supports a single collection, but the \
                     detector was prepared for {collections} collections.",
                    self.name
                ),
            }
        }
        let (keys, _) = futures::future::try_join(
            futures::future::try_join_all(opens),
            futures::future::try_join_all(singles.iter().map(|s| s.prepare_single())),
        )
        .await?;
        Ok((keys.into_iter().flatten().collect(), singles))
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
        let ctx = match prepared {
            None => self.prepare_for(TriggerInfo::default()).await?,
            Some(ctx) if ctx.trigger_info.number_of_events != 1 => {
                return Err(BsrsError::Plan(format!(
                    "{}: trigger() is not supported for multiple events, the detector \
                     was prepared with number_of_events={}.",
                    self.name, ctx.trigger_info.number_of_events
                )));
            }
            Some(ctx) => self.update_prepare_context(ctx.trigger_info).await?,
        };
        self.control.arm().await.await?;
        self.wait_for_index(
            ctx.collections_written,
            ctx.trigger_info.collections_per_event.into(),
            ctx.trigger_info.effective_exposure_timeout(),
            true,
        )
        .await
    }

    /// ophyd-async `_wait_for_index`: wait until every writer has written
    /// `requested` collections past `initial`, each update of an index
    /// within `exposure_timeout`, then for the hardware to go idle if asked.
    async fn wait_for_index(
        &self,
        initial: u64,
        requested: u64,
        exposure_timeout: std::time::Duration,
        wait_for_idle: bool,
    ) -> Result<()> {
        let target = initial + requested;
        let mut rxs: Vec<_> = self
            .writers()
            .map(|w| w.observe_indices_written())
            .collect();
        while let Some(written) = rxs.iter_mut().map(|rx| *rx.borrow_and_update()).min() {
            if written >= target {
                break;
            }
            let changed =
                futures::future::select_all(rxs.iter_mut().map(|rx| Box::pin(rx.changed())));
            match tokio::time::timeout(exposure_timeout, changed).await {
                Ok((Ok(()), _, _)) => {}
                Ok((Err(_), _, _)) => {
                    return Err(BsrsError::State(format!(
                        "{}: writer closed at index {written} of {target}",
                        self.name
                    )))
                }
                Err(_) => return Err(BsrsError::Timeout(exposure_timeout)),
            }
        }
        if wait_for_idle {
            self.control.wait_for_idle().await?;
        }
        Ok(())
    }

    /// ophyd-async `StandardDetector.kickoff` up to starting the acquisition:
    /// `events_to_kickoff` events from the index now, within the prepare.
    async fn kickoff_ctx(&self) -> Result<KickoffCtx> {
        let ctx = self.prepared_or_default().await?;
        if self.writers().next().is_none() {
            return Err(BsrsError::State(format!(
                "Detector {} is not streamable, so cannot kickoff",
                self.name
            )));
        }
        let collections_written = self.collections_written().await?;
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
            exposure_timeout: ctx.trigger_info.effective_exposure_timeout(),
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
impl<C> Stageable for StandardDetector<C>
where
    C: DetectorControl,
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
        // ophyd-async `stage` stops the data logics and drops the prepare
        // and kickoff contexts; the next prepare sets the data logics up.
        *self.prepared.lock().unwrap() = None;
        *self.kicked_off.lock().unwrap() = None;
        self.set_events_to_kickoff(0);
        self.stop_data_logics().await
    }
    async fn unstage(&self) -> Result<()> {
        self.control.disarm().await?;
        self.stop_data_logics().await
    }
}

#[async_trait]
impl<C> Triggerable for StandardDetector<C>
where
    C: DetectorControl,
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
impl<C> Flyable for StandardDetector<C>
where
    C: DetectorControl,
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
        let waited = self
            .wait_for_index(
                kickoff.collections_written,
                kickoff.collections_requested,
                kickoff.exposure_timeout,
                kickoff.is_last_kickoff,
            )
            .await;
        match waited {
            Ok(()) => Status::done(),
            Err(e) => Status::fail(crate::core::status::StatusError::Failed(format!(
                "complete: {e}"
            ))),
        }
    }
}

/// Set the hardware up for `TriggerInfo` and keep it for the scan's
/// `trigger`/`kickoff`, as ophyd-async `StandardDetector.prepare` does.
#[async_trait]
impl<C> Preparable<TriggerInfo> for StandardDetector<C>
where
    C: DetectorControl,
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
impl<C> WritesStreamAssets for StandardDetector<C>
where
    C: DetectorControl,
{
    fn name(&self) -> &str {
        &self.name
    }
    /// ophyd-async `get_index`: whole events every writer has written,
    /// `min(collections) // collections_per_event`.
    async fn get_index(&self) -> Result<u64> {
        let ctx = self.prepare_ctx("Prepare not called")?;
        let per_event = u64::from(ctx.trigger_info.collections_per_event.max(1));
        Ok(self.min_collections_written().await / per_event)
    }
    fn collect_asset_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset> {
        let descriptor = descriptor.to_string();
        futures::stream::iter(self.writers())
            .flat_map(move |w| w.collect_stream_docs(up_to, &descriptor))
            .boxed()
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
impl<C> NamedObj for StandardDetector<C>
where
    C: DetectorControl + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "StandardDetector",
            "control": std::any::type_name::<C>(),
            "data_logics": self.data_logics.len(),
        })
    }
}

#[async_trait]
impl<C> StageableObj for StandardDetector<C>
where
    C: DetectorControl + Send + Sync + 'static,
{
    async fn stage_dyn(&self) -> Result<()> {
        Stageable::stage(self).await
    }
    async fn unstage_dyn(&self) -> Result<()> {
        Stageable::unstage(self).await
    }
}

#[async_trait]
impl<C> TriggerableObj for StandardDetector<C>
where
    C: DetectorControl + Send + Sync + 'static,
{
    async fn trigger_dyn(&self) -> Status {
        Triggerable::trigger(self).await
    }
}

#[async_trait]
impl<C> FlyableObj for StandardDetector<C>
where
    C: DetectorControl + Send + Sync + 'static,
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
impl<C> CollectableObj for StandardDetector<C>
where
    C: DetectorControl + Send + Sync + 'static,
{
    async fn describe_collect_dyn(&self) -> Result<HashMap<String, HashMap<String, DataKey>>> {
        // The DataKeys the prepare opened the writer with; never re-open the
        // writer here (DB-04) — that would re-emit a StreamResource with a
        // new uid and disturb the in-progress frame counter.
        let data_keys = self.prepare_ctx("Prepare not run")?.data_keys;
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
impl<C> ReadableObj for StandardDetector<C>
where
    C: DetectorControl + Send + Sync + 'static,
{
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        // ophyd-async `read` returns only readable data providers; the
        // writers' data reaches the event through StreamDatum.
        let ctx = self.prepare_ctx("Prepare not called")?;
        let reads = futures::future::try_join_all(ctx.singles.iter().map(|s| s.read())).await?;
        Ok(reads.into_iter().flatten().collect())
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        // The writers' DataKeys are the prepare's, never re-opening a writer
        // (DB-04).
        let ctx = self.prepare_ctx("Prepare not run")?;
        let keys = futures::future::try_join_all(ctx.singles.iter().map(|s| s.describe())).await?;
        Ok(keys.into_iter().flatten().chain(ctx.data_keys).collect())
    }
    fn as_stageable(self: std::sync::Arc<Self>) -> Option<std::sync::Arc<dyn StageableObj>> {
        Some(self)
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(
            self.data_logics
                .iter()
                .flat_map(DataLogic::hinted_fields)
                .collect(),
        )
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
impl<C> ConfigurableObj for StandardDetector<C>
where
    C: DetectorControl + Send + Sync + 'static,
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

    /// The multiplier of every `open()` and the count of `close()`.
    #[derive(Clone, Default)]
    struct WriterLog {
        opens: Arc<std::sync::Mutex<Vec<u32>>>,
        closes: Arc<AtomicU64>,
    }

    impl WriterLog {
        fn opens(&self) -> Vec<u32> {
            self.opens.lock().unwrap().clone()
        }
        fn closes(&self) -> u64 {
            self.closes.load(Ordering::SeqCst)
        }
    }

    /// Minimal writer that logs `open()`/`close()`; its one DataKey's shape
    /// is the multiplier.
    struct CountingWriter {
        log: WriterLog,
        rx: watch::Receiver<u64>,
    }

    #[async_trait]
    impl DetectorWriter for CountingWriter {
        async fn open(&self, multiplier: u32) -> Result<HashMap<String, DataKey>> {
            self.log.opens.lock().unwrap().push(multiplier);
            let key = DataKey {
                source: "test://det".into(),
                dtype: crate::event_model::Dtype::Array,
                shape: vec![Some(multiplier.into())],
                dtype_numpy: None,
                external: Some("STREAM:".into()),
                units: None,
                precision: None,
                object_name: None,
                dims: None,
                limits: None,
                choices: None,
            };
            Ok(HashMap::from([("det_image".to_string(), key)]))
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
            self.log.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn detector() -> (
        StandardDetector<RecordingControl>,
        Arc<AtomicU64>,
        WriterLog,
    ) {
        let disarms = Arc::new(AtomicU64::new(0));
        let log = WriterLog::default();
        // The receiver keeps serving its last value after the sender drops;
        // this test never observes index changes, so the tx is discarded.
        let (_, rx) = watch::channel(0u64);
        let control = RecordingControl {
            disarms: disarms.clone(),
            idles: Arc::default(),
        };
        let writer = CountingWriter {
            log: log.clone(),
            rx,
        };
        (StandardDetector::new("det", control, writer), disarms, log)
    }

    async fn prepare_per_event(det: &impl Preparable<TriggerInfo>, events: u32, per_event: u32) {
        let info = TriggerInfo {
            number_of_events: events,
            collections_per_event: per_event,
            ..Default::default()
        };
        Preparable::<TriggerInfo>::prepare(det, info)
            .await
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stage_disarms_and_stops_the_writer() {
        // DB-03: a detector left armed by a previous scan must be disarmed at
        // stage time. Before the fix `stage()` never called `disarm()`, so a
        // detector armed by a prior (aborted) scan free-ran into the next one.
        // ophyd-async `stage` also stops the data logic.
        let (det, disarms, log) = detector();
        Stageable::stage(&det).await.unwrap();
        assert_eq!(
            disarms.load(Ordering::SeqCst),
            1,
            "stage() must disarm exactly once"
        );
        assert_eq!(log.closes(), 1, "stage() stops the writer");
    }

    #[tokio::test]
    async fn describe_reads_the_prepare_without_reopening_writer() {
        // DB-04: describe must read the DataKeys the prepare opened the
        // writer with, never re-open it (which would re-emit a StreamResource
        // with a new uid and disturb the in-progress frame counter).
        let (det, _disarms, log) = detector();
        Stageable::stage(&det).await.unwrap();

        // ophyd-async `describe` raises "Prepare not run" before a prepare.
        let err = ReadableObj::describe_dyn(&det).await.unwrap_err();
        assert!(err.to_string().contains("Prepare not run"), "{err}");
        assert!(CollectableObj::describe_collect_dyn(&det).await.is_err());
        assert!(log.opens().is_empty(), "describe must not open the writer");

        prepare_per_event(&det, 1, 1).await;
        CollectableObj::describe_collect_dyn(&det).await.unwrap();
        ReadableObj::describe_dyn(&det).await.unwrap();
        CollectableObj::describe_collect_dyn(&det).await.unwrap();
        assert_eq!(
            log.opens(),
            [1],
            "describe must not re-open the writer (DB-04)"
        );
    }

    #[tokio::test]
    async fn prepare_opens_the_writer_for_its_collections_per_event() {
        // ophyd-async `_update_prepare_context`: providers are made at
        // prepare, shaped by collections_per_event, and remade only when it
        // changes.
        let (det, _disarms, log) = detector();
        Stageable::stage(&det).await.unwrap();
        assert!(log.opens().is_empty(), "stage opens nothing");

        prepare_per_event(&det, 2, 2).await;
        assert_eq!(log.opens(), [2]);
        let keys = ReadableObj::describe_dyn(&det).await.unwrap();
        assert_eq!(keys["det_image"].shape, [Some(2)]);

        prepare_per_event(&det, 3, 2).await;
        assert_eq!(log.opens(), [2], "same collections_per_event reuses it");

        let closes = log.closes();
        prepare_per_event(&det, 1, 1).await;
        assert_eq!(log.opens(), [2, 1]);
        assert_eq!(log.closes(), closes + 1, "stopped before re-opening");
    }

    #[tokio::test]
    async fn get_index_counts_whole_events() {
        // ophyd-async `get_index`: collections // collections_per_event.
        let (det, tx, _) = controlled();
        Stageable::stage(&det).await.unwrap();
        let err = WritesStreamAssets::get_index(&det).await.unwrap_err();
        assert!(err.to_string().contains("Prepare not called"), "{err}");
        prepare_per_event(&det, 2, 2).await;
        tx.send(3).unwrap();
        assert_eq!(WritesStreamAssets::get_index(&det).await.unwrap(), 1);
        tx.send(4).unwrap();
        assert_eq!(WritesStreamAssets::get_index(&det).await.unwrap(), 2);
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
        StandardDetector<RecordingControl>,
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

    async fn prepare_events(det: &StandardDetector<RecordingControl>, n: u32) {
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

    #[tokio::test]
    async fn trigger_times_out_when_the_index_does_not_advance() {
        // ophyd-async test_use_cases.py:191: each index update must arrive
        // within exposure_timeout.
        let (det, _tx, idles) = controlled();
        Stageable::stage(&det).await.unwrap();
        let info = TriggerInfo {
            exposure_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        };
        Preparable::<TriggerInfo>::prepare(&det, info)
            .await
            .await
            .unwrap();
        let err = Triggerable::trigger(&det).await.await.unwrap_err();
        assert!(err.to_string().contains("timeout after 50ms"), "{err}");
        assert_eq!(idles.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn trigger_waits_for_the_index_then_idle() {
        let (det, tx, idles) = controlled();
        Stageable::stage(&det).await.unwrap();
        let sent = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            tx.send(1).unwrap();
            tx
        });
        Triggerable::trigger(&det).await.await.unwrap();
        assert_eq!(idles.load(Ordering::SeqCst), 1);
        drop(sent.await.unwrap());
    }

    #[tokio::test]
    async fn complete_fails_when_the_writer_closes_short() {
        let (det, tx, idles) = controlled();
        Stageable::stage(&det).await.unwrap();
        prepare_events(&det, 3).await;
        Flyable::kickoff(&det).await.await.unwrap();
        tx.send(1).unwrap();
        drop(tx);
        let err = Flyable::complete(&det).await.await.unwrap_err();
        assert!(
            err.to_string().contains("writer closed at index 1 of 3"),
            "{err}"
        );
        assert_eq!(idles.load(Ordering::SeqCst), 0);
    }

    fn data_key(source: &str, dtype: crate::event_model::Dtype, shape: Vec<u64>) -> DataKey {
        DataKey {
            source: source.into(),
            dtype,
            shape: shape.into_iter().map(Some).collect(),
            dtype_numpy: None,
            external: None,
            units: None,
            precision: None,
            object_name: None,
            dims: None,
            limits: None,
            choices: None,
        }
    }

    /// ophyd-async test `ReadableOnlyDataLogic`: reads `foo-value` as 42.
    #[derive(Default)]
    struct ReadableOnly {
        prepares: AtomicU64,
        stops: AtomicU64,
    }

    #[async_trait]
    impl SingleDataLogic for ReadableOnly {
        async fn prepare_single(&self) -> Result<()> {
            self.prepares.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn describe(&self) -> Result<HashMap<String, DataKey>> {
            let key = data_key(
                "soft://foo-value",
                crate::event_model::Dtype::Integer,
                vec![],
            );
            Ok(HashMap::from([("foo-value".to_string(), key)]))
        }
        async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
            let reading = ReadingValue::new(42.into(), 0.0);
            Ok(HashMap::from([("foo-value".to_string(), reading)]))
        }
        fn hinted_fields(&self) -> Vec<String> {
            vec!["foo-value".into()]
        }
        async fn stop(&self) -> Result<()> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// ophyd-async test `StreamableOnlyDataLogic`: the DataKey `key`, an
    /// index `rx` drives, and a `StreamResource` for `key` on every drain.
    struct StreamableOnly {
        key: &'static str,
        rx: watch::Receiver<u64>,
    }

    #[async_trait]
    impl DetectorWriter for StreamableOnly {
        async fn open(&self, multiplier: u32) -> Result<HashMap<String, DataKey>> {
            let mut key = data_key(
                "test://h5",
                crate::event_model::Dtype::Array,
                vec![multiplier.into(), 10, 15],
            );
            key.external = Some("STREAM:".into());
            Ok(HashMap::from([(self.key.to_string(), key)]))
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
            let resource = crate::event_model::StreamResource {
                uid: self.key.into(),
                data_key: self.key.into(),
                mimetype: "application/x-hdf5".into(),
                uri: "test://h5".into(),
                parameters: HashMap::new(),
                run_start: None,
            };
            stream::iter([StreamAsset::Resource(resource)]).boxed()
        }
        async fn close(&self) -> Result<()> {
            Ok(())
        }
        fn hinted_fields(&self) -> Vec<String> {
            vec![self.key.into()]
        }
    }

    fn streamable(key: &'static str) -> (DataLogic, watch::Sender<u64>) {
        let (tx, rx) = watch::channel(0u64);
        (
            DataLogic::Unbounded(Arc::new(StreamableOnly { key, rx })),
            tx,
        )
    }

    /// A detector over `logics`, and its count of `wait_for_idle`.
    fn with_logics(
        name: &str,
        logics: Vec<DataLogic>,
    ) -> (StandardDetector<RecordingControl>, Arc<AtomicU64>) {
        let idles = Arc::new(AtomicU64::new(0));
        let control = RecordingControl {
            disarms: Arc::default(),
            idles: idles.clone(),
        };
        (
            StandardDetector::with_data_logics(name, control, logics),
            idles,
        )
    }

    #[tokio::test]
    async fn hints_join_every_data_logic_in_order() {
        // ophyd-async test_standard_detector.py:602.
        let (writer, _tx) = streamable("bar");
        let single = DataLogic::Single(Arc::new(ReadableOnly::default()));
        let (det, _) = with_logics("bar", vec![single, writer]);
        assert_eq!(
            ReadableObj::hint_fields(&det).unwrap(),
            ["foo-value", "bar"]
        );
    }

    #[tokio::test]
    async fn trigger_reads_a_single_data_logic_without_a_writer() {
        // ophyd-async test_standard_detector.py:752: with no writer, trigger
        // waits only for idle and read returns the single logic's readings.
        let single = Arc::new(ReadableOnly::default());
        let (det, idles) = with_logics("det", vec![DataLogic::Single(single.clone())]);
        Stageable::stage(&det).await.unwrap();
        assert_eq!(single.stops.load(Ordering::SeqCst), 1, "stage stops it");
        let err = ReadableObj::read_dyn(&det).await.unwrap_err();
        assert!(err.to_string().contains("Prepare not called"), "{err}");

        Triggerable::trigger(&det).await.await.unwrap();
        assert_eq!(single.prepares.load(Ordering::SeqCst), 1);
        assert_eq!(idles.load(Ordering::SeqCst), 1);
        let reading = ReadableObj::read_dyn(&det).await.unwrap();
        assert_eq!(reading["foo-value"].value, 42);
    }

    #[tokio::test]
    async fn single_data_logic_is_not_prepared_for_several_collections() {
        // ophyd-async test_standard_detector.py:868: it is skipped with a
        // warning, so the prepare reads and describes nothing.
        let single = Arc::new(ReadableOnly::default());
        let (det, _) = with_logics("det", vec![DataLogic::Single(single.clone())]);
        prepare_events(&det, 5).await;
        assert_eq!(single.prepares.load(Ordering::SeqCst), 0);
        assert!(ReadableObj::describe_dyn(&det).await.unwrap().is_empty());
        assert!(ReadableObj::read_dyn(&det).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn kickoff_without_a_writer_is_refused() {
        // ophyd-async test_standard_detector.py:657.
        let single = DataLogic::Single(Arc::new(ReadableOnly::default()));
        let (det, _) = with_logics("foo", vec![single]);
        prepare_events(&det, 1).await;
        Triggerable::trigger(&det).await.await.unwrap();
        prepare_events(&det, 5).await;
        let err = Flyable::kickoff(&det).await.await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Detector foo is not streamable, so cannot kickoff"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn describe_joins_single_and_writer_keys_and_collect_only_writers() {
        // ophyd-async test_standard_detector.py:945.
        let (writer, _tx) = streamable("det");
        let single = DataLogic::Single(Arc::new(ReadableOnly::default()));
        let (det, _) = with_logics("det", vec![single, writer]);
        prepare_events(&det, 1).await;
        let keys = ReadableObj::describe_dyn(&det).await.unwrap();
        let mut names: Vec<_> = keys.keys().cloned().collect();
        names.sort();
        assert_eq!(names, ["det", "foo-value"]);
        assert_eq!(keys["det"].shape, [Some(1), Some(10), Some(15)]);
        assert_eq!(keys["foo-value"].source, "soft://foo-value");
        let collect = CollectableObj::describe_collect_dyn(&det).await.unwrap();
        assert_eq!(collect["det"].keys().collect::<Vec<_>>(), ["det"]);
        prepare_events(&det, 3).await;
        Flyable::kickoff(&det).await.await.unwrap();
    }

    #[tokio::test]
    async fn writers_that_disagree_on_collections_refuse_kickoff() {
        // ophyd-async test_standard_detector.py:922; get_index takes the
        // least of them.
        let (w1, tx1) = streamable("roi1");
        let (w2, tx2) = streamable("roi2");
        let (det, _) = with_logics("det", vec![w1, w2]);
        prepare_events(&det, 5).await;
        tx1.send(3).unwrap();
        tx2.send(5).unwrap();
        assert_eq!(WritesStreamAssets::get_index(&det).await.unwrap(), 3);
        let err = Flyable::kickoff(&det).await.await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Detectors have written different numbers of collections: {3, 5}"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn trigger_waits_for_every_writer_and_assets_chain_in_order() {
        // ophyd-async `_wait_for_index` waits on the least index;
        // `collect_asset_docs` yields each writer's documents in turn.
        let (w1, tx1) = streamable("roi1");
        let (w2, tx2) = streamable("roi2");
        let (det, idles) = with_logics("det", vec![w1, w2]);
        let det = Arc::new(det);
        prepare_events(&det, 1).await;
        let triggered = tokio::spawn({
            let det = det.clone();
            async move { Triggerable::trigger(&*det).await.await }
        });
        // After trigger takes its baseline, which the writers must agree on.
        tokio::time::sleep(Duration::from_millis(20)).await;
        tx1.send(1).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!triggered.is_finished(), "roi2 has written nothing yet");
        tx2.send(1).unwrap();
        tokio::time::timeout(Duration::from_secs(1), triggered)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(idles.load(Ordering::SeqCst), 1);
        let keys: Vec<_> = det
            .collect_asset_docs(1, "d")
            .map(|asset| match asset {
                StreamAsset::Resource(r) => r.data_key,
                StreamAsset::Datum(d) => d.stream_resource,
            })
            .collect()
            .await;
        assert_eq!(keys, ["roi1", "roi2"]);
    }
}
