//! The RunEngine: consumes a `Plan`, dispatches `Msg`, emits `Document`s.
//!
//! M4 surface:
//!
//! - `pause(defer)` / `resume()` / `abort(reason)` / `halt(reason)` — engine
//!   control. Pause clears the run permit; resume notifies waiters and replays
//!   the rewind cache (since the last `Checkpoint`).
//! - `Checkpoint` / `ClearCheckpoint` Msg — define rewindable regions. Cache
//!   `Msg`s tagged `is_cacheable()` between a Checkpoint and the next
//!   ClearCheckpoint (or end of run).
//! - `InstallSuspender` / `RemoveSuspender` Msg — register objects whose
//!   `watch()` future resolves on the resume condition (e.g. a shutter PV).
//! - SIGINT 3-tap — first ctrl-c → `pause(false)`, second → `abort`, third →
//!   `halt`. Installed via `install_signal_handler()`; off by default so the
//!   engine plays nicely with hosts that own SIGINT.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cirrus_core::error::{CirrusError, Result};
use cirrus_core::msg::{Msg, RunMetadata};
use cirrus_core::plan::{Plan, PlanItem};
use cirrus_core::status::{Status, StatusError};
use cirrus_event_model::compose::RunBundle;
use cirrus_event_model::Document;
use futures::StreamExt;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::bundler::RunBundler;
use crate::sink::DocumentSink;
use crate::suspender::{Suspender, SuspenderHandle};

/// Final state of a finished run.
#[derive(Debug, Clone)]
pub struct RunResult {
    /// Run start UID, if a run was opened.
    pub run_uid: Option<String>,
    /// Final exit status (`success` / `abort` / `fail` / `halt` / `no-run`).
    pub exit_status: String,
}

/// Pending status group bookkeeping.
#[derive(Default)]
struct WaitGroup {
    members: Vec<Status>,
}

/// Optional pre/post-suspend plan injection (placeholder for M4+).
pub type SuspendCallback = Box<dyn FnOnce() -> Plan + Send + Sync>;

/// The RunEngine.
pub struct RunEngine {
    sinks: Vec<Arc<dyn DocumentSink>>,
    cancel: CancellationToken,
    permit: Arc<Notify>,
    is_paused: Arc<AtomicBool>,
    deferred_pause: AtomicBool,
    is_aborting: AtomicBool,
    is_halting: AtomicBool,
    sigint_count: AtomicU8,
    suspender_count: AtomicU64,
    state: Mutex<EngineState>,
    /// `true` if `install_signal_handler()` has run.
    signal_installed: AtomicBool,
}

#[derive(Default)]
struct EngineState {
    bundler: Option<RunBundler>,
    groups: HashMap<String, WaitGroup>,
    staged: Vec<Arc<dyn cirrus_core::msg::StageableObj>>,
    monitors: Vec<(String, Arc<dyn cirrus_core::msg::MonitorableObj>)>,
    msg_cache: VecDeque<Msg>,
    replay_queue: VecDeque<Msg>,
    rewindable: bool,
    suspenders: HashMap<u64, SuspenderHandle>,
}

impl RunEngine {
    /// Construct a fresh RunEngine with the given sinks.
    pub fn new(sinks: Vec<Arc<dyn DocumentSink>>) -> Self {
        Self {
            sinks,
            cancel: CancellationToken::new(),
            permit: Arc::new(Notify::new()),
            is_paused: Arc::new(AtomicBool::new(false)),
            deferred_pause: AtomicBool::new(false),
            is_aborting: AtomicBool::new(false),
            is_halting: AtomicBool::new(false),
            sigint_count: AtomicU8::new(0),
            suspender_count: AtomicU64::new(0),
            state: Mutex::new(EngineState::default()),
            signal_installed: AtomicBool::new(false),
        }
    }

    /// Async entry point — drive a plan to completion.
    pub async fn run_async(&self, plan: Plan) -> Result<RunResult> {
        let outcome = self.run_loop(plan).await;
        // Cleanup: unstage anything still staged; drop suspenders.
        let mut state = self.state.lock().await;
        let staged = std::mem::take(&mut state.staged);
        let _ = std::mem::take(&mut state.suspenders); // Drop aborts watchers
        drop(state);
        for s in staged {
            let _ = s.unstage_dyn().await;
        }
        outcome
    }

    /// Sync entry point — drive a plan via the cirrus runtime.
    /// Must not be called from inside an async task.
    pub fn run_blocking(&self, plan: Plan) -> Result<RunResult> {
        cirrus_core::runtime::block_on(self.run_async(plan))
    }

    /// External: request a pause. If `defer = true`, the pause takes effect at
    /// the next `Checkpoint`; otherwise immediately at the top of the message
    /// loop.
    pub fn pause(&self, defer: bool) {
        if defer {
            self.deferred_pause.store(true, Ordering::SeqCst);
        } else {
            self.is_paused.store(true, Ordering::SeqCst);
        }
    }

    /// External: resume a paused engine. Replays the rewind cache before
    /// pulling the next plan message.
    pub fn resume(&self) {
        self.is_paused.store(false, Ordering::SeqCst);
        self.permit.notify_waiters();
    }

    /// External: abort the run. Closes the open run with `exit_status="abort"`.
    pub fn abort(&self, _reason: impl Into<String>) {
        self.is_aborting.store(true, Ordering::SeqCst);
        self.is_paused.store(false, Ordering::SeqCst);
        self.cancel.cancel();
        self.permit.notify_waiters();
    }

    /// External: halt — like abort but skips run-level cleanup.
    pub fn halt(&self, _reason: impl Into<String>) {
        self.is_halting.store(true, Ordering::SeqCst);
        self.is_aborting.store(true, Ordering::SeqCst);
        self.is_paused.store(false, Ordering::SeqCst);
        self.cancel.cancel();
        self.permit.notify_waiters();
    }

    /// Whether a pause is currently in effect.
    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::SeqCst)
    }

    /// Install a SIGINT handler implementing bluesky's 3-tap pattern:
    /// 1st = `pause(false)`, 2nd = `abort`, 3rd = `halt`.
    pub fn install_signal_handler(self: &Arc<Self>) {
        if self
            .signal_installed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                if tokio::signal::ctrl_c().await.is_err() {
                    return;
                }
                let n = me.sigint_count.fetch_add(1, Ordering::SeqCst) + 1;
                match n {
                    1 => {
                        eprintln!("\n[cirrus] ctrl-c — pausing (tap again to abort)");
                        me.pause(false);
                    }
                    2 => {
                        eprintln!("[cirrus] ctrl-c (2) — aborting (tap again to halt)");
                        me.abort("user abort");
                    }
                    _ => {
                        eprintln!("[cirrus] ctrl-c (3+) — halting");
                        me.halt("user halt");
                        return;
                    }
                }
            }
        });
    }

    // -- main loop ----------------------------------------------------------

    async fn run_loop(&self, plan: Plan) -> Result<RunResult> {
        let plan = Mutex::new(plan);
        let mut run_uid: Option<String> = None;
        let mut exit_status = String::from("no-run");

        loop {
            let msg = match self.next_msg(&plan).await {
                Some(m) => m,
                None => {
                    if self.is_halting.load(Ordering::SeqCst) {
                        exit_status = "halt".into();
                    } else if self.is_aborting.load(Ordering::SeqCst) {
                        exit_status = "abort".into();
                    }
                    break;
                }
            };
            if self.is_halting.load(Ordering::SeqCst) {
                exit_status = "halt".into();
                break;
            }
            if self.is_aborting.load(Ordering::SeqCst) {
                exit_status = "abort".into();
                break;
            }
            tracing::debug!("RE msg: {:?}", &msg);
            match self.handle(msg).await {
                Ok(Some(uid)) => run_uid = Some(uid),
                Ok(None) => {}
                Err(e) => {
                    tracing::error!("plan error: {e}");
                    exit_status = "fail".into();
                    self.close_run_if_open("fail", Some(format!("{e}"))).await?;
                    return Ok(RunResult {
                        run_uid,
                        exit_status,
                    });
                }
            }
        }

        // On abort/halt: close the open run with the right status.
        if exit_status == "abort" || exit_status == "halt" {
            let reason = if exit_status == "halt" {
                None
            } else {
                Some("user-requested abort".into())
            };
            self.close_run_if_open(&exit_status, reason).await?;
            return Ok(RunResult {
                run_uid,
                exit_status,
            });
        }

        // Normal exit: close any open run as success.
        let still_open = self.state.lock().await.bundler.is_some();
        if still_open {
            self.close_run_if_open("success", None).await?;
            exit_status = "success".into();
        } else if run_uid.is_some() && exit_status == "no-run" {
            exit_status = "success".into();
        }

        Ok(RunResult {
            run_uid,
            exit_status,
        })
    }

    /// Pull the next message: handle pause gating, replay queue, then plan.
    async fn next_msg(&self, plan: &Mutex<Plan>) -> Option<Msg> {
        loop {
            // Pause gate
            while self.is_paused.load(Ordering::SeqCst) && !self.is_aborting.load(Ordering::SeqCst)
            {
                self.on_pause_enter().await;
                self.permit.notified().await;
                self.on_resume().await;
            }
            if self.is_aborting.load(Ordering::SeqCst) {
                return None;
            }
            // Replay queue first
            {
                let mut state = self.state.lock().await;
                if let Some(m) = state.replay_queue.pop_front() {
                    return Some(m);
                }
            }
            // Plan stream
            let item = {
                let mut p = plan.lock().await;
                p.next().await
            };
            let item = item?;
            let m = match item {
                PlanItem::Bare(m) => m,
                _ => continue,
            };
            // Cache if rewindable
            {
                let mut state = self.state.lock().await;
                if state.rewindable && m.is_cacheable() {
                    state.msg_cache.push_back(m.clone());
                }
            }
            return Some(m);
        }
    }

    async fn on_pause_enter(&self) {
        // Suspend monitors (drop them; they'll be re-installed on resume by
        // re-issuing Monitor messages from the cache, if any).
        let mut state = self.state.lock().await;
        state.monitors.clear();
    }

    async fn on_resume(&self) {
        // Move msg_cache → replay_queue so the engine replays from the last
        // checkpoint.
        let mut state = self.state.lock().await;
        let cache = std::mem::take(&mut state.msg_cache);
        state.replay_queue.extend(cache);
    }

    // -- handler ------------------------------------------------------------

    async fn handle(&self, msg: Msg) -> Result<Option<String>> {
        match msg {
            Msg::OpenRun(meta) => {
                let uid = self.open_run(meta).await?;
                return Ok(Some(uid));
            }
            Msg::CloseRun {
                exit_status,
                reason,
            } => {
                self.close_run_if_open(&exit_status, reason).await?;
            }
            Msg::Create { stream_name } => {
                self.state
                    .lock()
                    .await
                    .bundler
                    .as_mut()
                    .ok_or_else(|| CirrusError::Plan("Create with no open run".into()))?
                    .create(stream_name)?;
            }
            Msg::Save => {
                let docs = {
                    let mut state = self.state.lock().await;
                    state
                        .bundler
                        .as_mut()
                        .ok_or_else(|| CirrusError::Plan("Save with no open run".into()))?
                        .save()?
                };
                for d in docs {
                    self.broadcast(&d).await?;
                }
            }
            Msg::Drop => {
                self.state
                    .lock()
                    .await
                    .bundler
                    .as_mut()
                    .ok_or_else(|| CirrusError::Plan("Drop with no open run".into()))?
                    .drop_bundle()?;
            }
            Msg::DeclareStream {
                stream_name,
                data_keys,
            } => {
                let descriptor = {
                    let mut state = self.state.lock().await;
                    state
                        .bundler
                        .as_mut()
                        .ok_or_else(|| CirrusError::Plan("DeclareStream with no open run".into()))?
                        .declare_stream(stream_name, data_keys)?
                };
                self.broadcast(&Document::Descriptor(descriptor)).await?;
            }
            Msg::Read(obj) => {
                let readings = obj.read_dyn().await?;
                let data_keys = obj.describe_dyn().await?;
                let object_name = Some(obj.name().to_string());
                let hint_fields = obj.hint_fields();
                let mut state = self.state.lock().await;
                state
                    .bundler
                    .as_mut()
                    .ok_or_else(|| CirrusError::Plan("Read with no open run".into()))?
                    .add_readings(readings, data_keys, object_name, hint_fields)?;
            }
            Msg::Set { obj, value, group } => {
                let status = obj.set_dyn(value).await;
                self.handle_status(status, group).await?;
            }
            Msg::Trigger { obj, group } => {
                let status = obj.trigger_dyn().await;
                self.handle_status(status, group).await?;
            }
            Msg::Stage(obj) => {
                obj.stage_dyn().await?;
                self.state.lock().await.staged.push(obj);
            }
            Msg::Unstage(obj) => {
                obj.unstage_dyn().await?;
                let mut state = self.state.lock().await;
                state
                    .staged
                    .retain(|o| !Arc::ptr_eq(&(o.clone() as Arc<_>), &(obj.clone() as Arc<_>)));
            }
            Msg::Kickoff { obj, group } => {
                let status = obj.kickoff_dyn().await;
                self.handle_status(status, group).await?;
            }
            Msg::Complete { obj, group } => {
                let status = obj.complete_dyn().await;
                self.handle_status(status, group).await?;
            }
            Msg::Collect { obj, stream_name } => {
                let descs = obj.describe_collect_dyn().await?;
                let new_descriptors: Vec<cirrus_event_model::EventDescriptor> = {
                    let mut state = self.state.lock().await;
                    let bundler = state
                        .bundler
                        .as_mut()
                        .ok_or_else(|| CirrusError::Plan("Collect with no open run".into()))?;
                    let mut out = Vec::new();
                    for (name, dks) in &descs {
                        if bundler.descriptor_uid(name).is_none() {
                            out.push(bundler.declare_stream(name.clone(), dks.clone())?);
                        }
                    }
                    out
                };
                for descriptor in new_descriptors {
                    self.broadcast(&Document::Descriptor(descriptor)).await?;
                }
                let events = obj.collect_dyn().await?;
                for (name, data, timestamps) in events {
                    let stream = stream_name.clone().unwrap_or(name);
                    let ev = {
                        let state = self.state.lock().await;
                        let bundler = state.bundler.as_ref().unwrap();
                        bundler
                            .compose()
                            .event(&stream, data, timestamps)
                            .ok_or_else(|| CirrusError::Plan("event for unknown stream".into()))?
                    };
                    self.broadcast(&Document::Event(ev)).await?;
                }
            }
            Msg::Monitor { obj, name } => {
                let _sub = obj.subscribe_dyn().await?;
                let stream = name.unwrap_or_else(|| "primary".into());
                self.state.lock().await.monitors.push((stream, obj));
            }
            Msg::Unmonitor(obj) => {
                let mut state = self.state.lock().await;
                state.monitors.retain(|(_, o)| {
                    !Arc::ptr_eq(&(o.clone() as Arc<_>), &(obj.clone() as Arc<_>))
                });
            }
            Msg::Wait {
                group,
                error_on_timeout,
                timeout,
            } => {
                self.wait_group(&group, error_on_timeout, timeout).await?;
            }
            Msg::Sleep(d) => {
                tokio::select! {
                    _ = tokio::time::sleep(d) => {}
                    _ = self.cancel.cancelled() => {
                        return Err(CirrusError::Cancelled);
                    }
                }
            }
            Msg::Checkpoint => {
                let mut state = self.state.lock().await;
                // Clear cache up to this point — the rewindable region restarts.
                state.msg_cache.clear();
                state.rewindable = true;
                drop(state);
                // If a deferred_pause is queued, apply it now.
                if self.deferred_pause.swap(false, Ordering::SeqCst) {
                    self.is_paused.store(true, Ordering::SeqCst);
                }
            }
            Msg::ClearCheckpoint => {
                let mut state = self.state.lock().await;
                state.rewindable = false;
                state.msg_cache.clear();
            }
            Msg::Pause { defer } => {
                self.pause(defer);
            }
            Msg::Resume => {
                self.resume();
            }
            Msg::Rewindable(b) => {
                self.state.lock().await.rewindable = b;
            }
            Msg::InstallSuspender { id, suspender } => {
                self.install_suspender(id, suspender).await?;
            }
            Msg::RemoveSuspender { id } => {
                self.state.lock().await.suspenders.remove(&id);
            }
            Msg::Configure { obj, args } => {
                obj.configure_dyn(args).await?;
            }
            Msg::Custom { name, .. } => {
                tracing::warn!("ignoring unknown custom Msg: {name}");
            }
            Msg::Null => {}
            _ => {
                tracing::warn!("ignoring unhandled Msg variant");
            }
        }
        Ok(None)
    }

    /// Allocate a fresh suspender id.
    pub fn next_suspender_id(&self) -> u64 {
        self.suspender_count.fetch_add(1, Ordering::SeqCst)
    }

    async fn install_suspender(&self, id: u64, susp: Arc<dyn Any + Send + Sync>) -> Result<()> {
        // Recover the typed handle. The plan-side Msg carried `Arc<dyn Any>`
        // wrapping an `Arc<dyn Suspender>`.
        let typed: Arc<dyn Suspender> = susp
            .downcast::<Arc<dyn Suspender>>()
            .map(|a| (*a).clone())
            .map_err(|_| {
                CirrusError::Plan("InstallSuspender payload was not Arc<dyn Suspender>".into())
            })?;

        let permit = self.permit.clone();
        let paused = self.is_paused.clone();
        let suspender_clone = typed.clone();
        let handle = tokio::spawn(async move {
            loop {
                let fut = suspender_clone.watch();
                fut.await;
                paused.store(false, Ordering::SeqCst);
                permit.notify_waiters();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let registration = SuspenderHandle::new(id, typed, handle);
        self.state.lock().await.suspenders.insert(id, registration);
        Ok(())
    }

    async fn open_run(&self, meta: RunMetadata) -> Result<String> {
        let mut start_doc = RunBundle::start(meta.scan_id, None);
        for (k, v) in meta.extra {
            start_doc.extra.insert(k, v);
        }
        if let Some(plan_name) = meta.plan_name {
            start_doc
                .extra
                .insert("plan_name".into(), serde_json::Value::String(plan_name));
        }
        let bundle = Arc::new(RunBundle::open(&start_doc));
        let uid = start_doc.uid.clone();
        self.broadcast(&Document::Start(start_doc)).await?;
        let mut state = self.state.lock().await;
        if state.bundler.is_some() {
            return Err(CirrusError::Plan(
                "OpenRun while a previous run is still open".into(),
            ));
        }
        state.bundler = Some(RunBundler::new(bundle));
        Ok(uid)
    }

    async fn close_run_if_open(&self, exit_status: &str, reason: Option<String>) -> Result<()> {
        let stop_doc = {
            let mut state = self.state.lock().await;
            state
                .bundler
                .take()
                .map(|bundler| bundler.compose().stop(exit_status, reason))
        };
        if let Some(stop) = stop_doc {
            self.broadcast(&Document::Stop(stop)).await?;
        }
        Ok(())
    }

    async fn broadcast(&self, doc: &Document) -> Result<()> {
        for s in &self.sinks {
            let _ = s.dispatch(doc).await;
        }
        Ok(())
    }

    async fn handle_status(&self, status: Status, group: Option<String>) -> Result<()> {
        match group {
            Some(g) => {
                self.state
                    .lock()
                    .await
                    .groups
                    .entry(g)
                    .or_default()
                    .members
                    .push(status);
                Ok(())
            }
            None => match status.await {
                Ok(()) => Ok(()),
                Err(StatusError::Cancelled) => Err(CirrusError::Cancelled),
                Err(StatusError::Timeout) => Err(CirrusError::Timeout(Duration::from_secs(0))),
                Err(StatusError::Failed(s)) => Err(CirrusError::Backend(s)),
            },
        }
    }

    async fn wait_group(
        &self,
        group: &str,
        error_on_timeout: bool,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let members = {
            let mut state = self.state.lock().await;
            state
                .groups
                .remove(group)
                .map(|g| g.members)
                .unwrap_or_default()
        };
        if members.is_empty() {
            return Ok(());
        }
        let fut = async {
            for s in members {
                if let Err(e) = s.await {
                    if error_on_timeout {
                        return Err(match e {
                            StatusError::Cancelled => CirrusError::Cancelled,
                            StatusError::Timeout => CirrusError::Timeout(Duration::from_secs(0)),
                            StatusError::Failed(s) => CirrusError::Backend(s),
                        });
                    }
                }
            }
            Ok(())
        };
        match timeout {
            Some(d) => match tokio::time::timeout(d, fut).await {
                Ok(r) => r,
                Err(_) => {
                    if error_on_timeout {
                        Err(CirrusError::Timeout(d))
                    } else {
                        Ok(())
                    }
                }
            },
            None => fut.await,
        }
    }
}

impl Default for RunEngine {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}
