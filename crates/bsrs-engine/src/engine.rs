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
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use bsrs_core::error::{BsrsError, Result};
use bsrs_core::msg::{Msg, RunMetadata};
use bsrs_core::plan::{Plan, PlanItem};
use bsrs_core::status::{Status, StatusError};
use bsrs_event_model::compose::RunBundle;
use bsrs_event_model::{DocFilter, Document};
use futures::future::BoxFuture;
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::bundler::RunBundler;
use crate::sink::DocumentSink;
use crate::suspender::{Suspender, SuspenderHandle};

/// Mint a process-unique stream name for a `monitor` Msg that carries no
/// explicit `name`. Mirrors bluesky's bundler, which defaults the monitor
/// stream name to `short_uid("monitor")` (`bundlers.py:469`) — a fresh unique
/// label, **not** the device name. Defaulting to `obj.name()` collides with any
/// stream already declared under that same name (via `create`/`declare_stream`):
/// `start_monitor` would then reuse that stream's first-wins descriptor and emit
/// the monitor's events against its (differently-keyed) schema. A unique
/// `monitor-N` label cannot collide with a device name, so a name-less monitor
/// always gets its own descriptor.
fn default_monitor_stream_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!("monitor-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// State the engine reports via [`RunEngine::state`]. Mirrors bluesky's
/// `RunEngine.state` enum (idle / running / paused / aborting / halting).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EngineRunState {
    /// Not in a `run_async` call.
    Idle,
    /// Inside `run_async`, the loop is processing messages.
    Running,
    /// Inside `run_async`, the loop is blocked at a pause gate.
    Paused,
    /// `abort()` has been requested; the loop is closing the run.
    Aborting,
    /// `halt()` has been requested; the loop is short-circuiting cleanup.
    Halting,
}

/// Document callback signature for [`RunEngine::subscribe`].
///
/// Callbacks are invoked synchronously in `broadcast` order (after static
/// `sinks`). They must be quick — slow callbacks back the engine up.
pub type DocumentCallback = Arc<dyn Fn(&Document) + Send + Sync + 'static>;

/// Stable identifier returned by [`RunEngine::subscribe`].
pub type SubscriptionId = u64;

/// Custom-command handler signature. The engine downcasts the
/// `Msg::Custom` payload itself before invoking, so handlers receive a
/// type they know how to interpret.
pub type CustomCommandHandler = Arc<
    dyn for<'a> Fn(&'a (dyn Any + Send + Sync)) -> BoxFuture<'a, Result<()>>
        + Send
        + Sync
        + 'static,
>;

/// `RunMetadata` validator hook signature. Called once per `OpenRun` with
/// the merged metadata (`md` + plan-supplied extras). Return `Err` to
/// reject the run.
pub type MdValidator = Arc<dyn Fn(&HashMap<String, Value>) -> Result<()> + Send + Sync + 'static>;

/// `RunMetadata` normalizer hook signature. Called once per `OpenRun`
/// after the validator; returns the (possibly-modified) metadata that
/// is finally written into the RunStart document. Mirrors bluesky's
/// `md_normalizer`.
pub type MdNormalizer =
    Arc<dyn Fn(HashMap<String, Value>) -> Result<HashMap<String, Value>> + Send + Sync + 'static>;

/// `scan_id_source` hook signature. Called on each `OpenRun` (when no
/// `scan_id` is supplied via the Msg) to produce the next scan id.
/// Mirrors bluesky's `scan_id_source(md) -> int`.
pub type ScanIdSource = Arc<dyn Fn(&HashMap<String, Value>) -> Result<u64> + Send + Sync + 'static>;

/// Plan-wrapper signature. Each registered preprocessor is applied to
/// the plan in registration order at `run_async` entry. Mirrors
/// bluesky's `RE.preprocessors` list.
pub type Preprocessor = Arc<dyn Fn(Plan) -> Plan + Send + Sync + 'static>;

/// `before_plan` / `after_plan` hook signature. Synchronous; called from
/// inside `run_async` *outside* the message loop.
pub type PlanHook = Arc<dyn Fn() + Send + Sync + 'static>;

/// `msg_hook` signature. Synchronous; called on every `Msg` in the run loop
/// *before* it is dispatched. Mirrors bluesky's `RE.msg_hook`
/// (`run_engine.py:1645`) — the primary tool for plan introspection, logging,
/// and test capture.
pub type MsgHook = Arc<dyn Fn(&Msg) + Send + Sync + 'static>;

/// Snapshot delivered to a [`CheckpointHook`] on every `Msg::Checkpoint`
/// and on every `CloseRun` (with [`exit_status`](Self::exit_status) set).
/// Lets callers persist enough state to know "the engine reached a
/// safe point at time T inside run R" — or "run R finished cleanly
/// with status S" — without coupling to the engine's internal types.
///
/// Crash-recovery flow: a `Checkpoint` snapshot for run-uid R that is
/// **not** followed in the audit log by a `CloseRun` snapshot for the
/// same R means R was abandoned (daemon went down mid-run). See
/// `bsrs_cli::checkpoint_store::JsonlCheckpointStore::unfinished_run`.
#[derive(Clone, Debug)]
pub struct CheckpointSnapshot {
    /// Wall-clock UTC nanoseconds since the unix epoch.
    pub timestamp_ns: u64,
    /// `RunStart.uid` of the currently open run, or `None` if no run
    /// is open (between runs).
    pub run_uid: Option<String>,
    /// `None` for `Msg::Checkpoint` snapshots emitted mid-run.
    /// `Some(status)` for a snapshot fired right after `CloseRun`
    /// emitted its RunStop document (`success` / `abort` / `fail` /
    /// `halt`).
    pub exit_status: Option<String>,
}

/// Hook invoked synchronously on every `Msg::Checkpoint`. Implementations
/// must be quick — the engine awaits the call. Use it to persist
/// crash-recovery info (write a JSONL line to disk, ping a watchdog,
/// etc.); for heavier work spawn a tokio task and return immediately.
pub type CheckpointHook = Arc<dyn Fn(CheckpointSnapshot) + Send + Sync + 'static>;

/// Handler used by [`RunEngine`] to satisfy `Msg::Input`. Receives the
/// prompt and returns the user's response. Mirrors bluesky's
/// `_input` which routes through `AsyncInput`.
pub type InputHandler =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<String>> + Send + Sync + 'static>;

/// Side-channel result from the most recently-processed `Msg`. Producers
/// that yield `Msg`s (Lua coroutines, future async-fn plans) can poll
/// `RunEngine::take_msg_result` after each yield to see what the engine
/// did with the Msg.
///
/// `MsgResult` reflects the *engine's* observable effect — it is not a
/// promise that the operation has fully completed. For grouped
/// Set/Trigger/Kickoff/Complete, the `Status` is added to the named
/// wait group; the result reports that group name. For ungrouped
/// (synchronous) variants, the engine has already awaited completion
/// before writing the result.
#[derive(Debug, Clone)]
pub enum MsgResult {
    /// No useful result for this Msg kind.
    None,
    /// `OpenRun` produced a fresh run-start UID.
    OpenRun {
        /// Run-start UID.
        uid: String,
    },
    /// `Set` / `Trigger` / `Kickoff` / `Complete` issued a Status that
    /// was added to the given wait group. Plans pair this with a
    /// matching `Msg::Wait { group }`.
    Status {
        /// Wait group the Status was added to.
        group: String,
    },
    /// `Read` produced a reading per signal. Same shape as the engine
    /// stored into the bundler.
    Reading {
        /// `field_name` → `ReadingValue`.
        data: HashMap<String, bsrs_core::reading::ReadingValue>,
    },
    /// `Locate` produced a setpoint + readback pair.
    Location {
        /// Where the device was last requested to move.
        setpoint: f64,
        /// Where the device currently is.
        readback: f64,
    },
    /// `CloseRun` finished. Engine reports the exit status it just
    /// emitted in the RunStop document.
    CloseRun {
        /// `success` / `abort` / `fail` / etc.
        exit_status: String,
    },
    /// `Msg::Input` produced a string from the configured handler.
    Input {
        /// The user's response.
        text: String,
    },
    /// `Msg::ReClass` — the engine identifies itself.
    EngineClass {
        /// Stable identifier — `"bsrs.RunEngine"`.
        name: &'static str,
    },
    /// `Msg::Subscribe` returned an id; pair with `Msg::Unsubscribe`
    /// to remove early. Otherwise the engine drops it at run end.
    SubscriptionId {
        /// Stable subscription id.
        id: SubscriptionId,
    },
}

/// Final state of a finished run.
#[derive(Debug, Clone)]
pub struct RunResult {
    /// Run start UID, if a run was opened.
    pub run_uid: Option<String>,
    /// Final exit status (`success` / `abort` / `fail` / `halt` / `no-run`).
    pub exit_status: String,
}

/// Per-call options for [`RunEngine::run_async_with`]. Mirrors
/// bluesky's `RE(plan, subs, **md)` extras.
#[derive(Default)]
pub struct RunOptions {
    /// Per-call metadata; merged into every `OpenRun` for this run.
    /// Bluesky parity: `_metadata_per_call`.
    pub md: HashMap<String, Value>,
    /// Temporary subscribers — installed before the plan starts and
    /// removed at run end. Bluesky parity: positional `subs` arg to
    /// `RE.__call__`.
    pub subs: Vec<DocumentCallback>,
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
    /// Per-run cancellation token. Replaced at every `run_async` entry so
    /// a stale `abort` / `stop` from a previous run doesn't immediately
    /// tear down the new one. Borrowed via `cancel_token()`.
    cancel: StdMutex<CancellationToken>,
    permit: Arc<Notify>,
    is_paused: Arc<AtomicBool>,
    /// Fired on every `is_paused: false → true` transition (immediate
    /// `pause`, `suspend_until_with`, or a deferred pause applied at a
    /// checkpoint). Lets installed-`Suspender` watchers park until the
    /// engine is actually paused instead of busy-looping a resume —
    /// distinct from `permit` (which wakes the *plan* loop on resume).
    pause_notify: Arc<Notify>,
    is_running: AtomicBool,
    deferred_pause: AtomicBool,
    is_aborting: AtomicBool,
    is_halting: AtomicBool,
    is_stopping: AtomicBool,
    /// Caller-supplied reason for an interrupt (`abort`/`halt`), surfaced on
    /// the `RunStop` document of the closed run. Single owner of the interrupt
    /// reason: the interrupt entry points write it, `run_loop` reads it when
    /// closing the run, and `run_async` clears it so a previous run's reason
    /// cannot leak into this one. Mirrors bluesky's `RunEngine._reason`
    /// (run_engine.py:1336 set in `_abort_coro`, read at :1765 and passed to
    /// `close_run` :1792, reset at `_run` start :1497).
    interrupt_reason: StdMutex<String>,
    sigint_count: AtomicU8,
    suspender_count: AtomicU64,
    sub_counter: AtomicU64,
    state: Mutex<EngineState>,
    /// Persistent metadata, merged into every `OpenRun`. Mirrors
    /// `bluesky.run_engine.RunEngine.md`.
    md: StdMutex<HashMap<String, Value>>,
    /// Auto-incrementing scan_id, bumped when a run does not supply one.
    /// Bluesky stores this inside `md["scan_id"]`; bsrs mirrors that
    /// behavior — every successful `OpenRun` sets `md["scan_id"] = id+1`.
    scan_id: AtomicU64,
    /// Dynamic Document subscribers. Inserted/removed via
    /// `subscribe` / `unsubscribe`. Wrapped in `Arc` so spawned tasks
    /// (monitor pumps) can re-read the live list on each tick.
    subscribers: Arc<StdMutex<Vec<(SubscriptionId, DocFilter, DocumentCallback)>>>,
    /// Custom command handlers — `RunEngine::register_command`.
    commands: StdMutex<HashMap<String, CustomCommandHandler>>,
    /// Optional metadata validator.
    md_validator: StdMutex<Option<MdValidator>>,
    /// Optional metadata normalizer.
    md_normalizer: StdMutex<Option<MdNormalizer>>,
    /// Optional scan_id source override.
    scan_id_source: StdMutex<Option<ScanIdSource>>,
    /// Plan preprocessors applied in order at `run_async` entry.
    preprocessors: StdMutex<Vec<Preprocessor>>,
    /// Optional pre-plan hook.
    before_plan: StdMutex<Option<PlanHook>>,
    /// Optional post-plan hook.
    after_plan: StdMutex<Option<PlanHook>>,
    /// Optional per-`Msg` hook, called before each message is dispatched.
    msg_hook: StdMutex<Option<MsgHook>>,
    /// Optional whole-plan timeout. If set and exceeded, the loop fails
    /// with `BsrsError::Timeout`. Mirrors bluesky's
    /// `loop_until_completion_timeout`.
    loop_timeout: StdMutex<Option<Duration>>,
    /// Optional handler for `Msg::Input`. `None` = inputs fail.
    input_handler: StdMutex<Option<InputHandler>>,
    /// Per-call metadata supplied via `run_async_with`. Cleared at
    /// run end. Mirrors bluesky's `_metadata_per_call`.
    per_call_md: StdMutex<HashMap<String, Value>>,
    /// Subscription ids staged by `run_async_with` *before*
    /// `run_async` clears engine state. `run_async` migrates these
    /// into `state.temp_subscribers` after its reset.
    staged_temp_subs: StdMutex<Vec<SubscriptionId>>,
    /// Side-channel for the most recently-processed `Msg`'s result.
    /// Producers (Lua coroutine bridge, future async-fn plans) poll
    /// `take_msg_result` between Msg yields.
    last_msg_result: StdMutex<MsgResult>,
    /// `true` if `install_signal_handler()` has run.
    signal_installed: AtomicBool,
    /// When `true`, the engine emits an `Event` document to a special
    /// `"interruptions"` stream on each pause / resume / suspend.
    /// Mirrors bluesky's `record_interruptions`. The stream is
    /// declared on `OpenRun` (only when the flag is on at that
    /// moment). Off by default.
    record_interruptions: AtomicBool,
    /// Optional callback fired on every `Msg::Checkpoint`. Used for
    /// crash-recovery persistence — the daemon installs a hook that
    /// appends a JSONL line so a post-restart audit can answer
    /// "where was the engine at last shutdown?".
    checkpoint_hook: StdMutex<Option<CheckpointHook>>,
}

#[derive(Default)]
struct EngineState {
    bundler: Option<RunBundler>,
    groups: HashMap<String, WaitGroup>,
    staged: Vec<Arc<dyn bsrs_core::msg::StageableObj>>,
    /// Live monitor pumps, keyed by the monitored object's name (`obj.name()`)
    /// — the identity `Msg::Unmonitor(obj)` carries — independent of the
    /// descriptor stream name. The `MonitorTask` drops the `Subscription` (RAII
    /// unsubscribe) and aborts the pump on `Drop`. Inserted by `Msg::Monitor`,
    /// removed by `Msg::Unmonitor`.
    monitor_tasks: HashMap<String, MonitorTask>,
    /// Active monitor registrations, keyed by `obj.name()` like `monitor_tasks`
    /// — the persistent record (obj + stream) that outlives the pump. Kept
    /// across a pause so resume can re-install the pump; cleared by
    /// `Msg::Unmonitor` and at run close. bluesky's `_monitor_params`.
    monitored: HashMap<String, MonitorSpec>,
    /// Movables touched by `Msg::Set` during this run, keyed by name
    /// for dedup. Engine walks this on pause / cleanup and calls
    /// `MovableObj::stop_on_pause(success=true)`. Mirrors bluesky's
    /// `_movable_objs_touched`.
    movable_objs_touched: HashMap<String, Arc<dyn bsrs_core::msg::MovableObj>>,
    /// Flyers touched by `Msg::Kickoff` during this run, same role
    /// as `movable_objs_touched`.
    flyable_objs_touched: HashMap<String, Arc<dyn bsrs_core::msg::FlyableObj>>,
    /// Devices that opted into pause/resume hooks via
    /// `Msg::RegisterPausable` or `RunEngine::register_pausable`.
    /// Walked on every pause-enter and resume.
    pausables: HashMap<String, Arc<dyn bsrs_core::msg::PausableObj>>,
    /// Subscription ids added during this run (via `Msg::Subscribe`
    /// or the positional `subs` arg on `run_async_with`). Mirror of
    /// bluesky's `_temp_callback_ids` — entries are removed
    /// automatically when the run ends.
    temp_subscribers: Vec<SubscriptionId>,
    msg_cache: VecDeque<Msg>,
    replay_queue: VecDeque<Msg>,
    rewindable: bool,
    suspenders: HashMap<u64, SuspenderHandle>,
}

/// One live monitor pump. Drops abort the pump task and (transitively)
/// the held `Subscription`, releasing the backend slot (rule **K1**+**K2**).
struct MonitorTask {
    abort: tokio::task::AbortHandle,
}

impl Drop for MonitorTask {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// The persistent record of one active monitor — the monitored object and its
/// resolved stream name — kept independently of the live `MonitorTask` pump.
/// The bsrs equivalent of bluesky's `_monitor_params` (bundlers.py:500): it
/// survives a pause (which drops the pump but not the registration) so the
/// monitor can be re-installed on resume (`restore_monitors`, :665).
#[derive(Clone)]
struct MonitorSpec {
    obj: Arc<dyn bsrs_core::msg::MonitorableObj>,
    stream: String,
}

impl RunEngine {
    /// Construct a fresh RunEngine with the given sinks.
    pub fn new(sinks: Vec<Arc<dyn DocumentSink>>) -> Self {
        Self {
            sinks,
            cancel: StdMutex::new(CancellationToken::new()),
            permit: Arc::new(Notify::new()),
            is_paused: Arc::new(AtomicBool::new(false)),
            pause_notify: Arc::new(Notify::new()),
            is_running: AtomicBool::new(false),
            deferred_pause: AtomicBool::new(false),
            is_aborting: AtomicBool::new(false),
            is_halting: AtomicBool::new(false),
            is_stopping: AtomicBool::new(false),
            interrupt_reason: StdMutex::new(String::new()),
            sigint_count: AtomicU8::new(0),
            suspender_count: AtomicU64::new(0),
            sub_counter: AtomicU64::new(0),
            state: Mutex::new(EngineState::default()),
            md: StdMutex::new(HashMap::new()),
            scan_id: AtomicU64::new(0),
            subscribers: Arc::new(StdMutex::new(Vec::new())),
            commands: StdMutex::new(HashMap::new()),
            md_validator: StdMutex::new(None),
            md_normalizer: StdMutex::new(None),
            scan_id_source: StdMutex::new(None),
            preprocessors: StdMutex::new(Vec::new()),
            before_plan: StdMutex::new(None),
            after_plan: StdMutex::new(None),
            msg_hook: StdMutex::new(None),
            loop_timeout: StdMutex::new(None),
            input_handler: StdMutex::new(None),
            per_call_md: StdMutex::new(HashMap::new()),
            staged_temp_subs: StdMutex::new(Vec::new()),
            last_msg_result: StdMutex::new(MsgResult::None),
            signal_installed: AtomicBool::new(false),
            record_interruptions: AtomicBool::new(false),
            checkpoint_hook: StdMutex::new(None),
        }
    }

    /// Install a callback fired on every `Msg::Checkpoint`. The hook
    /// is synchronous — keep it light. Subsequent calls overwrite.
    /// Pass `None`-equivalent (an empty closure) to disable.
    pub fn set_checkpoint_hook(&self, hook: CheckpointHook) {
        *self.checkpoint_hook.lock().unwrap() = Some(hook);
    }

    /// Toggle interruption recording. When enabled, every subsequent
    /// `OpenRun` declares an `"interruptions"` stream and the engine
    /// emits an Event to it on pause / resume / suspend. Mirrors
    /// bluesky's `RE.record_interruptions = True/False`.
    pub fn set_record_interruptions(&self, on: bool) {
        self.record_interruptions.store(on, Ordering::SeqCst);
    }

    /// Whether interruption recording is enabled.
    pub fn record_interruptions_enabled(&self) -> bool {
        self.record_interruptions.load(Ordering::SeqCst)
    }

    /// Take and clear the most recent `Msg` result side channel. Returns
    /// `MsgResult::None` if nothing was written since the last take.
    pub fn take_msg_result(&self) -> MsgResult {
        std::mem::replace(&mut *self.last_msg_result.lock().unwrap(), MsgResult::None)
    }

    /// Async entry point with per-call options. Mirrors bluesky's
    /// `RE(plan, subs, **md)`. The supplied `md` is merged into every
    /// `OpenRun` for this run only; the `subs` are installed before
    /// the plan starts and auto-removed at run end.
    pub async fn run_async_with(&self, plan: Plan, opts: RunOptions) -> Result<RunResult> {
        // Stage per-call md and temp subs before run_async resets state.
        *self.per_call_md.lock().unwrap() = opts.md;
        let mut staged_ids = Vec::new();
        for cb in opts.subs {
            staged_ids.push(self.subscribe(cb));
        }
        // Splice the staged ids into temp_subscribers so the run-end
        // cleanup removes them. We need an owned guard because
        // run_async clears state at the top — push *after* its
        // pre-flight, via a one-shot stash.
        *self.staged_temp_subs.lock().unwrap() = staged_ids;
        self.run_async(plan).await
    }

    /// Async entry point — drive a plan to completion.
    pub async fn run_async(&self, plan: Plan) -> Result<RunResult> {
        // before_plan hook — runs before is_running flips on, so it sees
        // EngineRunState::Idle.
        if let Some(h) = self.before_plan.lock().unwrap().clone() {
            h();
        }
        self.is_running.store(true, Ordering::SeqCst);
        // Reset abort/halt/stop flags from a previous (terminated) run so
        // `RunEngine` is reusable across plans.
        self.is_aborting.store(false, Ordering::SeqCst);
        self.is_halting.store(false, Ordering::SeqCst);
        self.is_stopping.store(false, Ordering::SeqCst);
        // Clear any interrupt reason left by a previous run so it cannot leak
        // into this run's RunStop (bluesky run_engine.py:1497).
        self.interrupt_reason.lock().unwrap().clear();
        self.is_paused.store(false, Ordering::SeqCst);
        // Reset SIGINT 3-tap counter — a previous session's taps must
        // not put a fresh run into the abort/halt path on the very
        // first ctrl-c.
        self.sigint_count.store(0, Ordering::SeqCst);
        // Renew the cancel token so a previous abort/stop's cancel state
        // doesn't immediately tear down this run.
        *self.cancel.lock().unwrap() = CancellationToken::new();
        // Migrate any temp subs staged by `run_async_with` into the
        // engine state register so cleanup picks them up.
        let staged = std::mem::take(&mut *self.staged_temp_subs.lock().unwrap());
        if !staged.is_empty() {
            let mut state = self.state.lock().await;
            state.temp_subscribers.extend(staged);
        }
        // Apply registered preprocessors in order — each wraps the
        // plan into a new Plan whose Msgs are filtered/extended.
        let plan = {
            let pps = self.preprocessors.lock().unwrap().clone();
            let mut p = plan;
            for pp in pps {
                p = pp(p);
            }
            p
        };
        let timeout = *self.loop_timeout.lock().unwrap();
        let outcome = match timeout {
            Some(d) => match tokio::time::timeout(d, self.run_loop(plan)).await {
                Ok(r) => r,
                Err(_) => {
                    self.cancel.lock().unwrap().cancel();
                    Err(BsrsError::Timeout(d))
                }
            },
            None => self.run_loop(plan).await,
        };
        // Cleanup: stop touched movables / flyers, unstage anything
        // still staged, drop suspenders. Mirrors bluesky's `_run`
        // exit chain (`_stop_movable_objects` then `unstage`).
        let mut state = self.state.lock().await;
        let staged = std::mem::take(&mut state.staged);
        let movables = std::mem::take(&mut state.movable_objs_touched);
        let flyables = std::mem::take(&mut state.flyable_objs_touched);
        let temp_subs = std::mem::take(&mut state.temp_subscribers);
        let _ = std::mem::take(&mut state.pausables);
        let _ = std::mem::take(&mut state.suspenders); // Drop aborts watchers
        let _ = std::mem::take(&mut state.monitor_tasks); // K1: monitor pumps
        let _ = std::mem::take(&mut state.monitored); // K1: monitor registry — run over, no resume
        drop(state);
        // Bluesky `_temp_callback_ids` parity: subscribers added via
        // `Msg::Subscribe` or run_async_with's `subs` arg are removed
        // at run end so they don't leak across plans.
        for id in temp_subs {
            self.unsubscribe(id);
        }
        for (_name, m) in movables {
            if let Err(e) = m.stop_on_pause(true).await {
                tracing::warn!("stop_on_pause failed for movable {}: {e}", m.name());
            }
        }
        for (_name, fly) in flyables {
            if let Err(e) = fly.stop_on_pause(true).await {
                tracing::warn!("stop_on_pause failed for flyer {}: {e}", fly.name());
            }
        }
        for s in staged {
            let _ = s.unstage_dyn().await;
        }
        self.is_running.store(false, Ordering::SeqCst);
        // Clear per-call md so a subsequent `run_async` (without
        // `run_async_with`) sees an empty per-call register.
        self.per_call_md.lock().unwrap().clear();
        if let Some(h) = self.after_plan.lock().unwrap().clone() {
            h();
        }
        outcome
    }

    // -- query / setters ----------------------------------------------------

    /// UID of the currently-open run, if any. Useful for plans that
    /// want to capture the run UID after issuing `Msg::OpenRun` (the
    /// Lua coroutine bridge surfaces this as the `coroutine.yield`
    /// return value for `msg.open_run`).
    pub async fn current_run_uid(&self) -> Option<String> {
        self.state
            .lock()
            .await
            .bundler
            .as_ref()
            .map(|b| b.start_uid.clone())
    }

    /// Current engine run-state. Bluesky's `RE.state`.
    pub fn state(&self) -> EngineRunState {
        if self.is_halting.load(Ordering::SeqCst) {
            return EngineRunState::Halting;
        }
        if self.is_aborting.load(Ordering::SeqCst) {
            return EngineRunState::Aborting;
        }
        if self.is_paused.load(Ordering::SeqCst) {
            return EngineRunState::Paused;
        }
        if self.is_running.load(Ordering::SeqCst) {
            return EngineRunState::Running;
        }
        EngineRunState::Idle
    }

    /// Read the persistent metadata dict (`bluesky.RE.md`). Cheap clone.
    pub fn md(&self) -> HashMap<String, Value> {
        self.md.lock().unwrap().clone()
    }

    /// Set a single metadata key.
    pub fn md_set(&self, key: impl Into<String>, value: Value) {
        self.md.lock().unwrap().insert(key.into(), value);
    }

    /// Remove a metadata key.
    pub fn md_remove(&self, key: &str) {
        self.md.lock().unwrap().remove(key);
    }

    /// Replace the entire metadata dict (use with care).
    pub fn md_replace(&self, md: HashMap<String, Value>) {
        *self.md.lock().unwrap() = md;
    }

    /// Subscribe a Document callback for *every* document type. Returns a
    /// [`SubscriptionId`]; pair with `unsubscribe(id)` to remove.
    pub fn subscribe(&self, cb: DocumentCallback) -> SubscriptionId {
        self.subscribe_filtered(DocFilter::All, cb)
    }

    /// Subscribe a Document callback restricted to one document type
    /// (bluesky `RE.subscribe(func, name)`). Only documents matching
    /// `filter` reach `cb`; `DocFilter::All` is equivalent to
    /// [`subscribe`](Self::subscribe).
    pub fn subscribe_filtered(&self, filter: DocFilter, cb: DocumentCallback) -> SubscriptionId {
        let id = self.sub_counter.fetch_add(1, Ordering::SeqCst) + 1;
        self.subscribers.lock().unwrap().push((id, filter, cb));
        id
    }

    /// Fan a document out to every dynamic subscriber whose filter
    /// matches. The single owner of subscriber filtering — both
    /// [`broadcast`](Self::broadcast) and the spawned monitor pump route
    /// through here so the filter rule lives in exactly one place. The
    /// callback Arcs are cloned out from under the lock so user code never
    /// runs while the lock is held.
    fn dispatch_subscribers(
        subs: &StdMutex<Vec<(SubscriptionId, DocFilter, DocumentCallback)>>,
        doc: &Document,
    ) {
        let matched: Vec<DocumentCallback> = subs
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, filter, _)| filter.matches(doc))
            .map(|(_, _, cb)| cb.clone())
            .collect();
        for cb in matched {
            cb(doc);
        }
    }

    /// Remove a subscriber by id. No-op if the id is unknown.
    pub fn unsubscribe(&self, id: SubscriptionId) {
        self.subscribers
            .lock()
            .unwrap()
            .retain(|(i, _, _)| *i != id);
    }

    /// Forward an externally-supplied Document through the engine's
    /// broadcast path — same fan-out as internally-emitted Documents
    /// (static sinks + dynamic subscribers). Used by
    /// `ZmqDocumentSource` and other Document-plane bridges to inject
    /// a remote run into the local engine's subscriber chain without
    /// being inside a plan.
    pub async fn inject_document(&self, doc: &Document) -> Result<()> {
        self.broadcast(doc).await
    }

    /// Register a custom command handler. Plans yielding
    /// `Msg::Custom { name, payload }` route to the handler whose name
    /// matches; the payload is passed as `&dyn Any`.
    pub fn register_command(&self, name: impl Into<String>, handler: CustomCommandHandler) {
        self.commands.lock().unwrap().insert(name.into(), handler);
    }

    /// Remove a custom command handler.
    pub fn unregister_command(&self, name: &str) {
        self.commands.lock().unwrap().remove(name);
    }

    /// Install a metadata validator. Called once per `OpenRun` *after*
    /// `md` is merged with the plan's `RunMetadata.extra`. Return `Err`
    /// to reject the run.
    pub fn set_md_validator(&self, v: Option<MdValidator>) {
        *self.md_validator.lock().unwrap() = v;
    }

    /// Install a metadata normalizer. Runs after the validator on the
    /// merged metadata; the returned dict is what lands in the
    /// `RunStart` document.
    pub fn set_md_normalizer(&self, n: Option<MdNormalizer>) {
        *self.md_normalizer.lock().unwrap() = n;
    }

    /// Install a `scan_id_source` callback. If set, every `OpenRun`
    /// without a caller-supplied `scan_id` consults this source
    /// instead of the auto-increment counter.
    pub fn set_scan_id_source(&self, s: Option<ScanIdSource>) {
        *self.scan_id_source.lock().unwrap() = s;
    }

    /// Append a plan preprocessor. Applied in registration order at
    /// every `run_async` entry, just before the message loop begins.
    pub fn add_preprocessor(&self, p: Preprocessor) {
        self.preprocessors.lock().unwrap().push(p);
    }

    /// Drop all registered preprocessors.
    pub fn clear_preprocessors(&self) {
        self.preprocessors.lock().unwrap().clear();
    }

    /// Hook fired before each `run_async`, *before* the engine flips into
    /// `Running` state.
    pub fn set_before_plan(&self, h: Option<PlanHook>) {
        *self.before_plan.lock().unwrap() = h;
    }

    /// Hook fired after each `run_async`, after cleanup.
    pub fn set_after_plan(&self, h: Option<PlanHook>) {
        *self.after_plan.lock().unwrap() = h;
    }

    /// Install a `msg_hook` called with every `Msg` just before it is
    /// dispatched (bluesky `RE.msg_hook`). Pass `None` to clear it.
    pub fn set_msg_hook(&self, h: Option<MsgHook>) {
        *self.msg_hook.lock().unwrap() = h;
    }

    /// Set an overall plan timeout (bluesky `loop_until_completion_timeout`).
    /// `None` = no timeout (default).
    pub fn set_loop_timeout(&self, t: Option<Duration>) {
        *self.loop_timeout.lock().unwrap() = t;
    }

    /// Install a handler that satisfies `Msg::Input`. `None` clears
    /// the handler — subsequent `Msg::Input` will fail with
    /// `BsrsError::Plan`.
    pub fn set_input_handler(&self, h: Option<InputHandler>) {
        *self.input_handler.lock().unwrap() = h;
    }

    /// Register a Pausable device. Equivalent to yielding
    /// `Msg::RegisterPausable(obj)` from a plan; useful when the
    /// device is set up before the run begins (e.g. by a host
    /// application or plan preprocessor).
    pub async fn register_pausable(&self, obj: Arc<dyn bsrs_core::msg::PausableObj>) {
        self.state
            .lock()
            .await
            .pausables
            .insert(obj.name().to_string(), obj);
    }

    /// Remove a previously-registered Pausable device.
    pub async fn unregister_pausable(&self, name: &str) {
        self.state.lock().await.pausables.remove(name);
    }

    /// Pause the engine and auto-resume when `fut` resolves. Mirrors
    /// bluesky's `RE.request_suspend(fut, …)`.
    ///
    /// Spawns a background task that awaits `fut`; when it resolves,
    /// the engine is resumed. The engine is paused immediately. If
    /// the engine is already paused, this still installs the
    /// auto-resume task — the next resume will fire when `fut`
    /// resolves.
    pub fn suspend_until(self: &Arc<Self>, fut: BoxFuture<'static, ()>) {
        self.suspend_until_with(fut, None);
    }

    /// Like [`Self::suspend_until`] but records `justification` (default
    /// `"suspended"`) into the interruptions stream when recording
    /// is enabled. Mirrors bluesky's `request_suspend(fut, …,
    /// justification=…)`.
    pub fn suspend_until_with(
        self: &Arc<Self>,
        fut: BoxFuture<'static, ()>,
        justification: Option<String>,
    ) {
        self.mark_paused();
        let me = Arc::downgrade(self);
        let label = justification.unwrap_or_else(|| "suspended".into());
        tokio::spawn(async move {
            // Record the suspend at the start so it lands before any
            // resume event from a fast-resolving future.
            if let Some(me) = me.upgrade() {
                me.record_interruption(&label).await;
            }
            fut.await;
            if let Some(me) = me.upgrade() {
                me.resume();
            }
        });
    }

    /// Synonym for [`Self::pause`]. Mirrors bluesky's `RE.request_pause`.
    pub fn request_pause(&self, defer: bool) {
        self.pause(defer);
    }

    /// External nudge: ask the engine to pause. The engine pauses at
    /// the next opportunity; pair with a `Suspender` (via
    /// `Msg::InstallSuspender`) or call `suspend_until(fut)` if you
    /// want auto-resume on a condition. Mirrors bluesky's
    /// `request_suspend` for the no-future case (which pauses, not
    /// aborts).
    pub fn request_suspend(&self, _reason: impl Into<String>) {
        self.pause(false);
    }

    /// Sync entry point — drive a plan via the bsrs runtime.
    /// Must not be called from inside an async task.
    pub fn run_blocking(&self, plan: Plan) -> Result<RunResult> {
        bsrs_core::runtime::block_on(self.run_async(plan))
    }

    /// Single owner of the `is_paused: false → true` transition. Stores the
    /// flag and signals `pause_notify` so installed-`Suspender` watchers can
    /// re-arm on the pause edge. Every site that pauses the engine routes
    /// through here so no watcher misses a suspension.
    fn mark_paused(&self) {
        self.is_paused.store(true, Ordering::SeqCst);
        self.pause_notify.notify_waiters();
    }

    /// External: request a pause. If `defer = true`, the pause takes effect at
    /// the next `Checkpoint`; otherwise immediately at the top of the message
    /// loop.
    pub fn pause(&self, defer: bool) {
        if defer {
            self.deferred_pause.store(true, Ordering::SeqCst);
        } else {
            self.mark_paused();
        }
    }

    /// External: resume a paused engine. Replays the rewind cache before
    /// pulling the next plan message.
    pub fn resume(&self) {
        self.is_paused.store(false, Ordering::SeqCst);
        self.permit.notify_waiters();
    }

    /// External: abort the run. Closes the open run with `exit_status="abort"`.
    /// The `reason` is threaded onto the closing `RunStop` document, matching
    /// bluesky's `RE.abort(reason)` (run_engine.py:1336).
    pub fn abort(&self, reason: impl Into<String>) {
        *self.interrupt_reason.lock().unwrap() = reason.into();
        self.is_aborting.store(true, Ordering::SeqCst);
        self.is_paused.store(false, Ordering::SeqCst);
        self.cancel.lock().unwrap().cancel();
        self.permit.notify_waiters();
    }

    /// External: halt — like abort but skips run-level cleanup. The `reason` is
    /// threaded onto the closing `RunStop` document.
    pub fn halt(&self, reason: impl Into<String>) {
        *self.interrupt_reason.lock().unwrap() = reason.into();
        self.is_halting.store(true, Ordering::SeqCst);
        self.is_aborting.store(true, Ordering::SeqCst);
        self.is_paused.store(false, Ordering::SeqCst);
        self.cancel.lock().unwrap().cancel();
        self.permit.notify_waiters();
    }

    /// External: graceful stop — like abort, but the run closes with
    /// `exit_status="success"`. Mirrors bluesky's `RE.stop`.
    pub fn stop(&self) {
        self.is_stopping.store(true, Ordering::SeqCst);
        self.is_aborting.store(true, Ordering::SeqCst);
        self.is_paused.store(false, Ordering::SeqCst);
        self.cancel.lock().unwrap().cancel();
        self.permit.notify_waiters();
    }

    /// The caller-supplied interrupt reason as a `RunStop` `reason` field: an
    /// empty reason maps to `None` (bluesky's default `_reason = ""` surfaces as
    /// no reason), a non-empty one to `Some`. Read by `run_loop` when an
    /// `abort`/`halt`/`stop` closes the run.
    fn stop_reason(&self) -> Option<String> {
        let r = self.interrupt_reason.lock().unwrap();
        if r.is_empty() {
            None
        } else {
            Some(r.clone())
        }
    }

    /// Whether a pause is currently in effect.
    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::SeqCst)
    }

    /// Install a SIGINT handler implementing bluesky's 3-tap pattern:
    /// 1st = `pause(false)`, 2nd = `abort`, 3rd = `halt`.
    ///
    /// The watcher captures `Weak<Self>` and exits when the engine drops.
    /// Holding a strong `Arc<Self>` would create a reference cycle that
    /// pins the engine forever — bad in environments (e.g. bsrs-qs)
    /// that recreate the engine across `environment_open/close`.
    pub fn install_signal_handler(self: &Arc<Self>) {
        if self
            .signal_installed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                if tokio::signal::ctrl_c().await.is_err() {
                    return;
                }
                let Some(me) = weak.upgrade() else { return };
                let n = me.sigint_count.fetch_add(1, Ordering::SeqCst) + 1;
                match n {
                    1 => {
                        eprintln!("\n[bsrs] ctrl-c — pausing (tap again to abort)");
                        me.pause(false);
                    }
                    2 => {
                        eprintln!("[bsrs] ctrl-c (2) — aborting (tap again to halt)");
                        me.abort("user abort");
                    }
                    _ => {
                        eprintln!("[bsrs] ctrl-c (3+) — halting");
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

        let resolve_exit = |this: &Self, current: &mut String| {
            if this.is_halting.load(Ordering::SeqCst) {
                *current = "halt".into();
            } else if this.is_stopping.load(Ordering::SeqCst) {
                *current = "success".into();
            } else if this.is_aborting.load(Ordering::SeqCst) {
                *current = "abort".into();
            }
        };

        loop {
            let msg = match self.next_msg(&plan).await {
                Some(m) => m,
                None => {
                    resolve_exit(self, &mut exit_status);
                    break;
                }
            };
            if self.is_halting.load(Ordering::SeqCst) {
                exit_status = "halt".into();
                break;
            }
            if self.is_stopping.load(Ordering::SeqCst) {
                exit_status = "success".into();
                break;
            }
            if self.is_aborting.load(Ordering::SeqCst) {
                exit_status = "abort".into();
                break;
            }
            tracing::debug!("RE msg: {:?}", &msg);
            // msg_hook sees every Msg before dispatch (bluesky run_engine.py:1645).
            if let Some(h) = self.msg_hook.lock().unwrap().clone() {
                h(&msg);
            }
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

        // Close the open run with the right status. `stop` and the natural-end
        // case both close as "success".
        if exit_status == "abort" || exit_status == "halt" {
            // Reason comes from the caller (`abort(reason)` / `halt(reason)`),
            // not a hardcoded string — bluesky threads `_reason` onto the stop
            // document (run_engine.py:1792).
            let reason = self.stop_reason();
            self.close_run_if_open(&exit_status, reason).await?;
            return Ok(RunResult {
                run_uid,
                exit_status,
            });
        }
        if exit_status == "success" && self.is_stopping.load(Ordering::SeqCst) {
            // `stop()` sets no reason, so this resolves to `None` (bluesky's
            // default `_reason = ""`), not a hardcoded "user-requested stop".
            self.close_run_if_open("success", self.stop_reason())
                .await?;
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
        // Snapshot touched objects under the lock, then drop the lock
        // before awaiting their stop / pause hooks so a slow backend
        // can't hold the engine state locked.
        let (movables, flyables, pausables) = {
            let mut state = self.state.lock().await;
            // Suspend monitors — drop the live pumps (releasing the backend
            // subscriptions) but keep the `monitored` registrations, so
            // `on_resume` re-installs them. Mirrors bluesky `suspend_monitors`
            // (clear_sub but keep `_monitor_params`, bundlers.py:661-663).
            state.monitor_tasks.clear();
            let movables: Vec<_> = state.movable_objs_touched.values().cloned().collect();
            let flyables: Vec<_> = state.flyable_objs_touched.values().cloned().collect();
            let pausables: Vec<_> = state.pausables.values().cloned().collect();
            (movables, flyables, pausables)
        };
        // Per doc 03: pause "Calls Stoppable::stop(success=true) on
        // all set/kickoff'd objects". `stop_on_pause` defaults to a
        // no-op for non-stoppable devices.
        for m in movables {
            if let Err(e) = m.stop_on_pause(true).await {
                tracing::warn!(
                    "stop_on_pause failed on pause for movable {}: {e}",
                    m.name()
                );
            }
        }
        for fly in flyables {
            if let Err(e) = fly.stop_on_pause(true).await {
                tracing::warn!(
                    "stop_on_pause failed on pause for flyer {}: {e}",
                    fly.name()
                );
            }
        }
        // Mirror bluesky `_run`: after the stop walk, notify Pausable
        // devices so they can quiesce internal state.
        for p in pausables {
            if let Err(e) = p.pause_dyn().await {
                tracing::warn!("pause_dyn failed for {}: {e}", p.name());
            }
        }
        // Bluesky parity: record_interruption("pause") on every pause
        // entry. No-op when recording is off or no run is open.
        self.record_interruption("pause").await;
    }

    async fn on_resume(&self) {
        // Snapshot pausables under the lock; release before awaiting
        // user code.
        let pausables: Vec<_> = {
            let mut state = self.state.lock().await;
            // Move msg_cache → replay_queue so the engine replays
            // from the last checkpoint.
            let cache = std::mem::take(&mut state.msg_cache);
            // Roll back bundler checkpoint state before replaying — mirrors
            // bluesky `_rewind` calling `RunBundler.rewind` only when the cache
            // is non-empty (run_engine.py:1043-1048). Cancels a bundle left
            // open by a pause that landed mid-event so the replayed `Create`
            // does not collide with it.
            if !cache.is_empty() {
                if let Some(b) = state.bundler.as_mut() {
                    b.rewind();
                }
            }
            state.replay_queue.extend(cache);
            state.pausables.values().cloned().collect()
        };
        for p in pausables {
            if let Err(e) = p.resume_dyn().await {
                tracing::warn!("resume_dyn failed for {}: {e}", p.name());
            }
        }
        // Re-install the monitors suspended on pause. Mirrors bluesky
        // `restore_monitors` (re-subscribe from the kept `_monitor_params`,
        // bundlers.py:665-666). `start_monitor` is idempotent on the descriptor
        // (the stream was already declared), so this re-subscribes the device
        // and respawns the pump without re-emitting the Descriptor.
        let specs: Vec<MonitorSpec> = {
            let state = self.state.lock().await;
            state.monitored.values().cloned().collect()
        };
        for spec in specs {
            if let Err(e) = self.start_monitor(spec.stream, spec.obj.clone()).await {
                tracing::warn!("restore monitor failed for {}: {e}", spec.obj.name());
            }
        }
        self.record_interruption("resume").await;
    }

    // -- handler ------------------------------------------------------------

    /// Engine-level mirror of bluesky's `_reset_checkpoint_state_meth`
    /// (run_engine.py:2461-2467). Two effects, in lockstep:
    ///
    /// 1. Drop the rewind cache so a subsequent resume cannot replay messages
    ///    issued *before* this point.
    /// 2. Snapshot each open run's per-stream sequence counters as the rewind
    ///    rollback target (`RunBundler::reset_checkpoint_state`), so a `save`
    ///    replayed from this checkpoint re-emits the same `seq_num` instead of
    ///    advancing it.
    ///
    /// The single owner of "the rewindable region restarts here" — invoked by
    /// the `Checkpoint` and `Rewindable` messages and by every commit-point
    /// message whose side effect must not be straddled by a rewind:
    /// stage/unstage, monitor/unmonitor, subscribe/unsubscribe (bluesky resets
    /// at each: run_engine.py:2047/2065/2556/2580/2629/2650). `ClearCheckpoint`
    /// is the one reset that *clears* the rollback target rather than taking a
    /// snapshot, so it takes its own path.
    fn reset_checkpoint_state(state: &mut EngineState) {
        state.msg_cache.clear();
        if let Some(b) = state.bundler.as_mut() {
            b.reset_checkpoint_state();
        }
    }

    async fn handle(&self, msg: Msg) -> Result<Option<String>> {
        match msg {
            Msg::OpenRun(meta) => {
                let uid = self.open_run(meta).await?;
                *self.last_msg_result.lock().unwrap() = MsgResult::OpenRun { uid: uid.clone() };
                return Ok(Some(uid));
            }
            Msg::CloseRun {
                exit_status,
                reason,
            } => {
                // bluesky's _close_run raises IllegalMessageSequence when no run
                // is open (run_engine.py:1902-1905). close_run_if_open is
                // intentionally lenient — it is the internal run-end cleanup path
                // (run_loop) — so the strict check belongs on the explicit
                // message path here.
                if self.state.lock().await.bundler.is_none() {
                    return Err(BsrsError::Plan("CloseRun without an open run".into()));
                }
                self.close_run_if_open(&exit_status, reason).await?;
                *self.last_msg_result.lock().unwrap() = MsgResult::CloseRun {
                    exit_status: exit_status.clone(),
                };
            }
            Msg::Create { stream_name } => {
                self.state
                    .lock()
                    .await
                    .bundler
                    .as_mut()
                    .ok_or_else(|| BsrsError::Plan("Create with no open run".into()))?
                    .create(stream_name)?;
            }
            Msg::Save => {
                let docs = {
                    let mut state = self.state.lock().await;
                    state
                        .bundler
                        .as_mut()
                        .ok_or_else(|| BsrsError::Plan("Save with no open run".into()))?
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
                    .ok_or_else(|| BsrsError::Plan("Drop with no open run".into()))?
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
                        .ok_or_else(|| BsrsError::Plan("DeclareStream with no open run".into()))?
                        .declare_stream(stream_name, data_keys)?
                };
                self.broadcast(&Document::Descriptor(descriptor)).await?;
            }
            Msg::Read(obj) => {
                let readings = obj.read_dyn().await?;
                let result_snapshot = readings.clone();
                // Bundle the reading only when a run is open. bluesky `_read`
                // reads the object and returns its value unconditionally,
                // folding it into the event bundle ONLY if the run_key has an
                // open bundler (run_engine.py:1993-1997). A read with no open
                // run is valid ad-hoc inspection: read and surface the value,
                // do not bundle, do not error. (Contrast Create/DeclareStream,
                // which DO raise without a run: run_engine.py:1942/1968.)
                let bundler_present = {
                    let state = self.state.lock().await;
                    state.bundler.is_some()
                };
                if bundler_present {
                    // describe() only matters for the bundle's descriptor, so
                    // compute it (awaiting before re-locking) only when bundling.
                    let data_keys = obj.describe_dyn().await?;
                    let object_name = Some(obj.name().to_string());
                    let hint_fields = obj.hint_fields();
                    let mut state = self.state.lock().await;
                    if let Some(bundler) = state.bundler.as_mut() {
                        bundler.add_readings(readings, data_keys, object_name, hint_fields)?;
                    }
                }
                // Surface the reading even when there's no open run; the
                // coroutine bridge can use it for ad-hoc inspection.
                *self.last_msg_result.lock().unwrap() = MsgResult::Reading {
                    data: result_snapshot,
                };
            }
            Msg::Locate(obj) => {
                let loc = obj.locate_dyn().await?;
                *self.last_msg_result.lock().unwrap() = MsgResult::Location {
                    setpoint: loc.setpoint,
                    readback: loc.readback,
                };
            }
            Msg::Set { obj, value, group } => {
                // Track for pause / cleanup before issuing the move so
                // a status that fails or never resolves still leaves
                // the obj in our touched register.
                self.state
                    .lock()
                    .await
                    .movable_objs_touched
                    .insert(obj.name().to_string(), obj.clone());
                let status = obj.set_dyn(value).await;
                if let Some(g) = group.clone() {
                    *self.last_msg_result.lock().unwrap() = MsgResult::Status { group: g };
                }
                self.handle_status(status, group).await?;
            }
            Msg::Trigger { obj, group } => {
                let status = obj.trigger_dyn().await;
                if let Some(g) = group.clone() {
                    *self.last_msg_result.lock().unwrap() = MsgResult::Status { group: g };
                }
                self.handle_status(status, group).await?;
            }
            Msg::Stage(obj) => {
                obj.stage_dyn().await?;
                let mut state = self.state.lock().await;
                // bluesky `_staged` is a set (run_engine.py:509, 2555): a device
                // is tracked at most once, so the run-end unstage walk — and any
                // explicit `Unstage` — balances each stage with exactly one
                // unstage. A bare `Vec` push would record a redundant stage of
                // the same device twice and unstage it twice at cleanup. Match
                // the identity test used by the `Unstage` retain below so add and
                // remove agree.
                if !state
                    .staged
                    .iter()
                    .any(|o| Arc::ptr_eq(&(o.clone() as Arc<_>), &(obj.clone() as Arc<_>)))
                {
                    state.staged.push(obj);
                }
                Self::reset_checkpoint_state(&mut state);
            }
            Msg::Unstage(obj) => {
                obj.unstage_dyn().await?;
                let mut state = self.state.lock().await;
                state
                    .staged
                    .retain(|o| !Arc::ptr_eq(&(o.clone() as Arc<_>), &(obj.clone() as Arc<_>)));
                Self::reset_checkpoint_state(&mut state);
            }
            Msg::Stop { obj, success } => {
                obj.stop_dyn(success).await?;
            }
            Msg::Kickoff { obj, group } => {
                // A kickoff requires an open run: the flyer's data is collected
                // into the current run, so kicking off the hardware with no run
                // to land in is an illegal message sequence. bluesky's
                // `_kickoff` rejects this *before* calling `obj.kickoff()`
                // (run_engine.py:2143-2147); reject here before the flyer is
                // registered or started so no hardware begins flying.
                {
                    let mut state = self.state.lock().await;
                    if state.bundler.is_none() {
                        return Err(BsrsError::Plan("Kickoff sent but no run is open".into()));
                    }
                    state
                        .flyable_objs_touched
                        .insert(obj.name().to_string(), obj.clone());
                }
                let status = obj.kickoff_dyn().await;
                if let Some(g) = group.clone() {
                    *self.last_msg_result.lock().unwrap() = MsgResult::Status { group: g };
                }
                self.handle_status(status, group).await?;
            }
            Msg::Complete { obj, group } => {
                let status = obj.complete_dyn().await;
                if let Some(g) = group.clone() {
                    *self.last_msg_result.lock().unwrap() = MsgResult::Status { group: g };
                }
                self.handle_status(status, group).await?;
            }
            Msg::Collect { obj, stream_name } => {
                // Open-run precondition, checked before the device is described.
                // bluesky's _collect rejects a collect with no open run at the top
                // (run_engine.py:2201-2206) *before* current_run.collect() runs
                // describe_collect. bsrs called describe_collect_dyn before its
                // bundler check below, so a collect with no open run did a wasted
                // device describe round-trip before erroring. Gate it here,
                // mirroring the Read path which describes only when bundling. The
                // later `ok_or_else` checks stay as defense for the bundler being
                // cleared mid-await (a pause landing while collect_dyn is awaiting).
                {
                    let state = self.state.lock().await;
                    if state.bundler.is_none() {
                        return Err(BsrsError::Plan(
                            "A 'collect' message was sent but no run is open".into(),
                        ));
                    }
                }
                let descs = obj.describe_collect_dyn().await?;
                let new_descriptors: Vec<bsrs_event_model::EventDescriptor> = {
                    let mut state = self.state.lock().await;
                    let bundler = state
                        .bundler
                        .as_mut()
                        .ok_or_else(|| BsrsError::Plan("Collect with no open run".into()))?;
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
                // Emit the writer's StreamResource/StreamDatum, stamped with the
                // collect stream's just-composed EventDescriptor UID, so stream
                // data links back to its descriptor (CBEM-13). StandardDetector
                // collects into a single stream; pass that stream's descriptor.
                let collect_descriptor = {
                    let state = self.state.lock().await;
                    let bundler = state.bundler.as_ref().ok_or_else(|| {
                        BsrsError::Plan("Collect lost open run before stream docs".into())
                    })?;
                    descs.keys().next().and_then(|s| bundler.descriptor_uid(s))
                };
                if let Some(descriptor_uid) = collect_descriptor {
                    for doc in obj.collect_stream_docs_dyn(&descriptor_uid).await? {
                        self.broadcast(&doc).await?;
                    }
                }
                let events = obj.collect_dyn().await?;
                for (name, data, timestamps) in events {
                    let stream = stream_name.clone().unwrap_or(name);
                    let ev = {
                        let state = self.state.lock().await;
                        let bundler = state.bundler.as_ref().ok_or_else(|| {
                            BsrsError::Plan(
                                "Collect lost open run mid-process (bundler cleared while \
                                 collect_dyn was awaiting)"
                                    .into(),
                            )
                        })?;
                        bundler
                            .compose()
                            .event(&stream, data, timestamps)
                            .ok_or_else(|| BsrsError::Plan("event for unknown stream".into()))?
                    };
                    self.broadcast(&Document::Event(ev)).await?;
                }
            }
            Msg::Monitor { obj, name } => {
                {
                    let state = self.state.lock().await;
                    // Open-run precondition, checked before any device describe.
                    // bluesky's _monitor rejects a monitor with no open run at the
                    // top (run_engine.py:2040-2044) *before* current_run.monitor()
                    // runs describe/subscribe. bsrs's start_monitor calls
                    // describe_dyn before its own bundler check, so a monitor with
                    // no open run did a wasted device describe round-trip before
                    // erroring. Gate it here, mirroring the Read path which
                    // describes only when a bundler is present. start_monitor keeps
                    // its internal check as defense for the resume re-install path.
                    if state.bundler.is_none() {
                        return Err(BsrsError::Plan(
                            "A 'monitor' message was sent but no run is open".into(),
                        ));
                    }
                    // Reject a second monitor of an already-monitored object.
                    // bluesky's bundler raises IllegalMessageSequence ("...which is
                    // already monitored", bundlers.py:470-471) *before* subscribing
                    // or emitting a descriptor. Without this guard a double-monitor
                    // silently re-subscribes, emits another Descriptor (when a
                    // different stream name is given), and overwrites monitor_tasks
                    // — aborting the first pump. The guard is on the explicit
                    // message path only; the resume re-install (restore_monitors)
                    // calls start_monitor directly from the kept `monitored`
                    // registry and is unaffected (same split as the lenient
                    // CloseRun cleanup).
                    if state.monitored.contains_key(obj.name()) {
                        return Err(BsrsError::Plan(format!(
                            "A 'monitor' message was sent for {} which is already monitored",
                            obj.name()
                        )));
                    }
                }
                let stream = name.unwrap_or_else(default_monitor_stream_name);
                self.start_monitor(stream.clone(), obj.clone()).await?;
                let mut state = self.state.lock().await;
                // Record the registration so the monitor survives a pause: the
                // pump (monitor_tasks) is dropped on pause, this spec is not, and
                // resume re-installs the pump from it. bluesky `_monitor_params`.
                state
                    .monitored
                    .insert(obj.name().to_string(), MonitorSpec { obj, stream });
                Self::reset_checkpoint_state(&mut state);
            }
            Msg::Unmonitor(obj) => {
                // monitor_tasks is keyed by the monitored object's name (set in
                // start_monitor), so remove the entry whose key == obj.name().
                // MonitorTask::drop aborts the pump and drops the Subscription.
                let mut state = self.state.lock().await;
                // Reject an 'unmonitor' for an object that is not being monitored.
                // bluesky's bundler raises IllegalMessageSequence ("Cannot
                // 'unmonitor' {obj}; it is not being monitored.", bundlers.py:544-545)
                // before touching any subscription. Without this guard a stray
                // 'unmonitor' — never monitored, or already unmonitored — is a
                // silent no-op. Symmetric to the Msg::Monitor double-monitor guard
                // above; `monitored` is the registry, so membership there is the
                // precondition. The bulk teardown on pause/close (monitor_tasks
                // .clear / monitored.clear) is intentionally lenient and unaffected
                // — this guard is on the explicit per-object message path only.
                if !state.monitored.contains_key(obj.name()) {
                    return Err(BsrsError::Plan(format!(
                        "Cannot 'unmonitor' {}; it is not being monitored",
                        obj.name()
                    )));
                }
                state
                    .monitor_tasks
                    .retain(|obj_name, _| obj_name != obj.name());
                // Drop the registration too, so a later resume does not
                // re-install a monitor the plan explicitly removed.
                state.monitored.remove(obj.name());
                Self::reset_checkpoint_state(&mut state);
            }
            Msg::Wait {
                group,
                error_on_timeout,
                timeout,
            } => {
                self.wait_group(&group, error_on_timeout, timeout).await?;
            }
            Msg::Sleep(d) => {
                let token = self.cancel.lock().unwrap().clone();
                tokio::select! {
                    _ = tokio::time::sleep(d) => {}
                    _ = token.cancelled() => {
                        return Err(BsrsError::Cancelled);
                    }
                }
            }
            Msg::Checkpoint => {
                let mut state = self.state.lock().await;
                // A checkpoint between `create` and `save` is illegal: rewinding
                // to a point inside an open event bundle cannot be done cleanly.
                // bluesky rejects it with IllegalMessageSequence
                // (run_engine.py:2444-2446); mirror that here.
                if state.bundler.as_ref().is_some_and(|b| b.is_bundling()) {
                    return Err(BsrsError::Plan(
                        "Cannot 'checkpoint' after 'create' and before 'save'".into(),
                    ));
                }
                // Clear cache up to this point — the rewindable region restarts.
                Self::reset_checkpoint_state(&mut state);
                state.rewindable = true;
                let run_uid = state.bundler.as_ref().map(|b| b.start_uid.clone());
                drop(state);
                // Crash-recovery hook: persist the snapshot so post-
                // restart auditing can pinpoint where the engine
                // left off. Fired *after* msg_cache is cleared so
                // the cleared state is the durable one.
                if let Some(hook) = self.checkpoint_hook.lock().unwrap().clone() {
                    let snap = CheckpointSnapshot {
                        timestamp_ns: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_nanos() as u64)
                            .unwrap_or_default(),
                        run_uid,
                        exit_status: None,
                    };
                    hook(snap);
                }
                // If a deferred_pause is queued, apply it now.
                if self.deferred_pause.swap(false, Ordering::SeqCst) {
                    self.mark_paused();
                }
            }
            Msg::ClearCheckpoint => {
                let mut state = self.state.lock().await;
                state.rewindable = false;
                // Drop the rewind cache AND the seq-counter rollback target: the
                // checkpoint region is gone, so there is nothing to roll back
                // to. Mirrors bluesky `_clear_checkpoint` (msg_cache=None +
                // RunBundler.clear_checkpoint, run_engine.py:2472-2483). Unlike
                // the Checkpoint / lifecycle resets, this CLEARS the snapshot
                // rather than taking one.
                state.msg_cache.clear();
                if let Some(b) = state.bundler.as_mut() {
                    b.clear_checkpoint();
                }
            }
            Msg::Pause { defer } => {
                self.pause(defer);
            }
            Msg::Resume => {
                self.resume();
            }
            Msg::Rewindable(b) => {
                // Mirror bluesky's `rewindable` setter (run_engine.py:694-699):
                // a *change* in the rewindable flag resets checkpoint state, so
                // the resume boundary moves to this toggle rather than replaying
                // work from before it. In bsrs, "reset checkpoint state" is
                // clearing `msg_cache` — the same reset the `Checkpoint` /
                // `ClearCheckpoint` handlers perform. Without it, wrapping a
                // non-safe-to-rewind bundle in `rewindable_wrapper(_, false)`
                // would leave the pre-toggle cache live, and a pause inside the
                // region would replay already-completed messages on resume.
                let mut state = self.state.lock().await;
                if state.rewindable != b {
                    state.rewindable = b;
                    Self::reset_checkpoint_state(&mut state);
                }
            }
            Msg::InstallSuspender { id, suspender } => {
                self.install_suspender(id, suspender).await?;
            }
            Msg::RemoveSuspender { id } => {
                self.state.lock().await.suspenders.remove(&id);
            }
            Msg::RegisterPausable(obj) => {
                self.state
                    .lock()
                    .await
                    .pausables
                    .insert(obj.name().to_string(), obj);
            }
            Msg::UnregisterPausable(obj) => {
                self.state.lock().await.pausables.remove(obj.name());
            }
            Msg::Input { prompt } => {
                let handler = self.input_handler.lock().unwrap().clone();
                let h = handler.ok_or_else(|| {
                    BsrsError::Plan("Msg::Input issued but no input handler installed".into())
                })?;
                let text = h(prompt).await?;
                *self.last_msg_result.lock().unwrap() = MsgResult::Input { text };
            }
            Msg::ReClass => {
                *self.last_msg_result.lock().unwrap() = MsgResult::EngineClass {
                    name: "bsrs.RunEngine",
                };
            }
            Msg::Subscribe { cb, filter } => {
                let id = self.subscribe_filtered(filter, cb);
                {
                    let mut state = self.state.lock().await;
                    state.temp_subscribers.push(id);
                    Self::reset_checkpoint_state(&mut state);
                }
                *self.last_msg_result.lock().unwrap() = MsgResult::SubscriptionId { id };
            }
            Msg::Unsubscribe(id) => {
                self.unsubscribe(id);
                let mut state = self.state.lock().await;
                state.temp_subscribers.retain(|i| *i != id);
                Self::reset_checkpoint_state(&mut state);
            }
            Msg::Configure { obj, args } => {
                // A configure issued between `create` and `save` would change the
                // object's configuration after readings were already folded into
                // the open bundle, desyncing the descriptor from its events.
                // bluesky rejects it with IllegalMessageSequence
                // (run_engine.py:2515-2517); mirror that. Same bundling-guard
                // family as the `checkpoint`-in-bundle rejection.
                {
                    let state = self.state.lock().await;
                    if state.bundler.as_ref().is_some_and(|b| b.is_bundling()) {
                        return Err(BsrsError::Plan(
                            "Cannot configure after 'create' but before 'save'".into(),
                        ));
                    }
                }
                obj.configure_dyn(args).await?;
            }
            Msg::Prepare { obj, value, group } => {
                let status = obj.prepare_dyn(value).await;
                if let Some(g) = group.clone() {
                    *self.last_msg_result.lock().unwrap() = MsgResult::Status { group: g };
                }
                self.handle_status(status, group).await?;
            }
            Msg::WaitFor { factories, timeout } => {
                let token = self.cancel.lock().unwrap().clone();
                // Start every awaitable up front so they make progress
                // concurrently, mirroring bluesky's
                // `[asyncio.ensure_future(f()) for f in futs]` followed by
                // `asyncio.wait(futs, ...)` (run_engine.py:1828-1829). Awaiting
                // the factories in sequence would defer each future's creation
                // until the previous one resolved — wrong for independent or
                // event-like conditions (a later factory could miss an event
                // that fired before it was started) and turning the `timeout`
                // into a sum-of-waits instead of a single concurrent bound.
                // `try_join_all` preserves the prior `f().await?` contract of
                // returning the first error (bsrs propagates via `Result`
                // rather than bluesky's exception-ignoring ALL_COMPLETED).
                let futs: Vec<_> = factories.iter().map(|f| f()).collect();
                let inner = futures::future::try_join_all(futs);
                match timeout {
                    Some(d) => tokio::select! {
                        r = tokio::time::timeout(d, inner) => match r {
                            Ok(r) => {
                                r?;
                            }
                            Err(_) => return Err(BsrsError::Timeout(d)),
                        },
                        _ = token.cancelled() => return Err(BsrsError::Cancelled),
                    },
                    None => tokio::select! {
                        r = inner => {
                            r?;
                        }
                        _ = token.cancelled() => return Err(BsrsError::Cancelled),
                    },
                }
            }
            Msg::Custom { name, payload } => {
                let handler = self.commands.lock().unwrap().get(name).cloned();
                match handler {
                    Some(h) => {
                        h(payload.as_ref()).await?;
                    }
                    None => {
                        return Err(BsrsError::Plan(format!("unknown custom command: {name}")));
                    }
                }
            }
            Msg::Publish(doc) => {
                self.broadcast(doc.as_ref()).await?;
            }
            Msg::Null => {}
            Msg::Fail(reason) => {
                return Err(BsrsError::Plan(reason));
            }
            _ => {
                tracing::warn!("ignoring unhandled Msg variant");
            }
        }
        Ok(None)
    }

    async fn start_monitor(
        &self,
        stream: String,
        obj: Arc<dyn bsrs_core::msg::MonitorableObj>,
    ) -> Result<()> {
        // Step 1: declare the descriptor for this stream from the device's
        // own describe_dyn (MonitorableObj : ReadableObj).
        let data_keys = obj.describe_dyn().await?;
        let (descriptor, bundle) = {
            let mut state = self.state.lock().await;
            let bundler = state
                .bundler
                .as_mut()
                .ok_or_else(|| BsrsError::Plan("Monitor with no open run".into()))?;
            let descriptor = if bundler.descriptor_uid(&stream).is_some() {
                None
            } else {
                Some(bundler.declare_stream(stream.clone(), data_keys.clone())?)
            };
            (descriptor, bundler.bundle())
        };
        if let Some(d) = descriptor {
            self.broadcast(&Document::Descriptor(d)).await?;
        }

        // Step 2: subscribe + spawn a pump that emits one Event per rx tick.
        let mut sub = obj.subscribe_dyn().await?;
        let stream_for_task = stream.clone();
        let obj_name = obj.name().to_string();
        let sinks = self.sinks.clone();
        let subs_arc = self.subscribers.clone();

        let handle = tokio::spawn(async move {
            loop {
                let reading = {
                    let r = sub.rx_mut();
                    if r.changed().await.is_err() {
                        return;
                    }
                    r.borrow_and_update().clone()
                };
                let mut data = HashMap::new();
                let mut timestamps = HashMap::new();
                data.insert(obj_name.clone(), reading.value);
                timestamps.insert(obj_name.clone(), reading.timestamp);
                let ev = match bundle.event(&stream_for_task, data, timestamps) {
                    Some(ev) => ev,
                    None => continue,
                };
                let doc = Document::Event(ev);
                for s in &sinks {
                    let _ = s.dispatch(&doc).await;
                }
                RunEngine::dispatch_subscribers(&subs_arc, &doc);
            }
        });
        let abort = handle.abort_handle();
        // Key the pump by the monitored object's identity — that is what
        // Msg::Unmonitor(obj) carries — NOT by the descriptor stream name.
        // Keying by `stream` leaked the pump whenever a custom monitor name was
        // used: Unmonitor matched the key against obj.name() and never found it.
        self.state
            .lock()
            .await
            .monitor_tasks
            .insert(obj.name().to_string(), MonitorTask { abort });
        Ok(())
    }

    /// Allocate a fresh suspender id.
    pub fn next_suspender_id(&self) -> u64 {
        self.suspender_count.fetch_add(1, Ordering::SeqCst)
    }

    /// Emit an Event to the special `"interruptions"` stream. No-op
    /// when recording is off, when there is no open run, or when the
    /// `OpenRun` happened *before* recording was turned on (the
    /// stream is declared at OpenRun time only).
    async fn record_interruption(&self, content: &str) {
        if !self.record_interruptions.load(Ordering::SeqCst) {
            return;
        }
        let bundle = {
            let state = self.state.lock().await;
            let bundler = match state.bundler.as_ref() {
                Some(b) => b,
                None => return,
            };
            if bundler.descriptor_uid("interruptions").is_none() {
                return;
            }
            bundler.bundle()
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let mut data = HashMap::new();
        data.insert("interruption".to_string(), Value::String(content.into()));
        let mut timestamps = HashMap::new();
        timestamps.insert("interruption".to_string(), now);
        if let Some(ev) = bundle.event("interruptions", data, timestamps) {
            let _ = self.broadcast(&Document::Event(ev)).await;
        }
    }

    /// Install a `Suspender` at the engine level (outside any
    /// active plan). The suspender's monitor task starts immediately
    /// and is removed via [`Msg::RemoveSuspender`]. Use
    /// [`Self::next_suspender_id`] to allocate `id`.
    pub async fn install_suspender(&self, id: u64, susp: Arc<dyn Any + Send + Sync>) -> Result<()> {
        // Recover the typed handle. The plan-side Msg carried `Arc<dyn Any>`
        // wrapping an `Arc<dyn Suspender>`.
        let typed: Arc<dyn Suspender> = susp
            .downcast::<Arc<dyn Suspender>>()
            .map(|a| (*a).clone())
            .map_err(|_| {
                BsrsError::Plan("InstallSuspender payload was not Arc<dyn Suspender>".into())
            })?;

        let permit = self.permit.clone();
        let paused = self.is_paused.clone();
        let pause_notify = self.pause_notify.clone();
        let suspender_clone = typed.clone();
        let handle = tokio::spawn(async move {
            loop {
                // Park until the engine is actually paused. The watcher exists
                // to lift a *suspension*; it must never clear `is_paused` on a
                // running engine (that would override an unrelated pause and
                // make the engine un-pausable for as long as this suspender's
                // condition stays clear). Arm the notification *before*
                // re-checking the flag so a pause landing between check and
                // await is never lost.
                loop {
                    let notified = pause_notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if paused.load(Ordering::SeqCst) {
                        break;
                    }
                    notified.await;
                }
                // Paused: wait for this suspender's condition to clear.
                suspender_clone.watch().await;
                // Resume — but only if the engine is still paused. If it was
                // already resumed/aborted/halted, do nothing and re-arm; this
                // never force-resumes a running engine.
                if paused.swap(false, Ordering::SeqCst) {
                    permit.notify_waiters();
                }
            }
        });
        let registration = SuspenderHandle::new(id, typed, handle);
        self.state.lock().await.suspenders.insert(id, registration);
        Ok(())
    }

    /// Uninstall every engine-level suspender. Mirrors bluesky's
    /// `RunEngine.clear_suspenders`: each suspender installed via
    /// [`Msg::InstallSuspender`] is removed and its monitor task aborted
    /// (dropping the [`SuspenderHandle`] aborts the watcher). A no-op when
    /// none are installed. Per-id removal is [`Msg::RemoveSuspender`].
    pub async fn clear_suspenders(&self) {
        self.state.lock().await.suspenders.clear();
    }

    async fn open_run(&self, meta: RunMetadata) -> Result<String> {
        // Reject a second open_run *before* any side effect — bluesky's
        // `run_key in self._run_bundlers` check precedes scan_id resolution and
        // document emission (run_engine.py:1849-1851). Doing it here means a
        // rejected re-open neither advances the scan_id counter nor broadcasts
        // a spurious RunStart.
        if self.state.lock().await.bundler.is_some() {
            return Err(BsrsError::Plan(
                "OpenRun while a previous run is still open".into(),
            ));
        }
        // Combine metadata in order of *decreasing* precedence, mirroring
        // bluesky's ChainMap (run_engine.py:1861-1870):
        //   per-call (run_async_with) > per-run (OpenRun extra)
        //     > computed {plan_name} > persistent (RE.md).
        // Build bottom-up — insert the lowest-precedence source first so the
        // highest-precedence source (per-call) is written last and wins.
        let mut merged: HashMap<String, Value> = {
            let mut m = self.md.lock().unwrap().clone(); // persistent (lowest)
                                                         // Computed plan_name overrides persistent but is overridden by the
                                                         // per-run / per-call layers below. bluesky places its computed
                                                         // {plan_type, plan_name} above self.md and below msg.kwargs and
                                                         // _metadata_per_call, so this is an `insert` (overwrite persistent),
                                                         // not the previous `or_insert` (which left it lowest-precedence).
            if let Some(ref pn) = meta.plan_name {
                m.insert("plan_name".into(), Value::String(pn.clone()));
            }
            // Per-run extras (the OpenRun Msg's kwargs) override the computed
            // level.
            for (k, v) in &meta.extra {
                m.insert(k.clone(), v.clone());
            }
            // Per-call md (`run_async_with`) is bluesky's `_metadata_per_call`,
            // the highest-precedence ChainMap layer — the operator's
            // invocation-time md wins over what the plan baked into OpenRun.
            let per_call = self.per_call_md.lock().unwrap().clone();
            for (k, v) in per_call {
                m.insert(k, v);
            }
            m
        };
        // Resolve scan_id: caller-supplied via Msg wins; else
        // scan_id_source if installed; else auto-increment counter.
        let scan_id = match meta.scan_id {
            Some(s) => {
                self.scan_id.store(s, Ordering::SeqCst);
                Some(s)
            }
            None => {
                let src = self.scan_id_source.lock().unwrap().clone();
                match src {
                    Some(s) => Some(s(&merged)?),
                    None => Some(self.scan_id.fetch_add(1, Ordering::SeqCst) + 1),
                }
            }
        };
        if let Some(scan_id) = scan_id {
            merged
                .entry("scan_id".into())
                .or_insert(Value::from(scan_id));
            // Persist the resolved scan_id back into RE.md so a custom
            // `scan_id_source` reading `md["scan_id"]` sees the last-used value
            // on the next run, and external persisters of RE.md observe the
            // current counter (bluesky `run_engine.py:1855`:
            // `self.md["scan_id"] = scan_id_source(self.md)`).
            self.md
                .lock()
                .unwrap()
                .insert("scan_id".into(), Value::from(scan_id));
        }
        let mut start_doc = RunBundle::start(scan_id, None);
        // Validator hook.
        if let Some(v) = self.md_validator.lock().unwrap().clone() {
            v(&merged)?;
        }
        // Normalizer hook — runs after validator.
        if let Some(n) = self.md_normalizer.lock().unwrap().clone() {
            merged = n(merged)?;
        }
        for (k, v) in merged {
            start_doc.extra.insert(k, v);
        }
        let bundle = Arc::new(RunBundle::open(&start_doc));
        let uid = start_doc.uid.clone();
        self.broadcast(&Document::Start(start_doc)).await?;
        let interruptions_descriptor = {
            let mut state = self.state.lock().await;
            let mut bundler = RunBundler::new(bundle);
            // Declare the interruptions stream upfront when recording
            // is on at OpenRun. Bluesky declares it inside the Bundler
            // open_run path; same effect here.
            let descriptor = if self.record_interruptions.load(Ordering::SeqCst) {
                let mut keys = HashMap::new();
                keys.insert(
                    "interruption".into(),
                    bsrs_event_model::DataKey {
                        source: "RunEngine".into(),
                        dtype: bsrs_event_model::Dtype::String,
                        shape: vec![],
                        dtype_numpy: None,
                        external: None,
                        units: None,
                        precision: None,
                        object_name: None,
                        dims: None,
                        limits: None,
                        choices: None,
                    },
                );
                Some(bundler.declare_stream("interruptions".into(), keys)?)
            } else {
                None
            };
            state.bundler = Some(bundler);
            descriptor
        };
        if let Some(d) = interruptions_descriptor {
            self.broadcast(&Document::Descriptor(d)).await?;
        }
        Ok(uid)
    }

    async fn close_run_if_open(&self, exit_status: &str, reason: Option<String>) -> Result<()> {
        // The RunStop document's `exit_status` is constrained by the event-model
        // schema to `success` | `abort` | `fail`. The engine's run-result status
        // additionally uses `halt` (and `no-run`) for its own reporting —
        // `RunResult.exit_status`, which bsrs-qs consumes — but those must
        // never reach a *document*. bluesky's `halt()` marks the close as
        // `abort` (run_engine.py:1442-1450), so normalize `halt` → `abort` here.
        // This is the sole owner that emits a RunStop, so normalizing once keeps
        // the broadcast document and the crash-recovery audit snapshot consistent
        // and schema-valid. (`no-run` cannot reach here: it means no run was
        // open, so there is no bundler to close.)
        let exit_status = if exit_status == "halt" {
            "abort"
        } else {
            exit_status
        };
        let stop_doc = {
            let mut state = self.state.lock().await;
            let stop = state
                .bundler
                .take()
                .map(|bundler| bundler.compose().stop(exit_status, reason));
            if stop.is_some() {
                // Tear down any monitors still active when the run closes —
                // bluesky's close_run clears each remaining `_monitor_params`
                // subscription (bundlers.py:246-248). A `Msg::Monitor` not
                // explicitly `Unmonitor`'d is unsubscribed per-run, not leaked
                // into the next run, where its pump would otherwise keep
                // composing Events against this now-closed bundle.
                // `MonitorTask::drop` aborts the pump and drops its Subscription.
                state.monitor_tasks.clear();
                // Drop the registrations too — a closed run must not restore its
                // monitors on a later resume.
                state.monitored.clear();
            }
            stop
        };
        if let Some(stop) = stop_doc {
            let run_uid = stop.run_start.clone();
            self.broadcast(&Document::Stop(stop)).await?;
            // Crash-recovery audit: fire the checkpoint hook with
            // `exit_status` set so a downstream JSONL store can
            // pair the prior Checkpoint(s) for this run_uid with a
            // clean close. The pairing is what
            // `JsonlCheckpointStore::unfinished_run` uses to detect
            // abandoned runs after a daemon restart.
            if let Some(hook) = self.checkpoint_hook.lock().unwrap().clone() {
                let snap = CheckpointSnapshot {
                    timestamp_ns: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or_default(),
                    run_uid: Some(run_uid),
                    exit_status: Some(exit_status.to_string()),
                };
                hook(snap);
            }
        }
        Ok(())
    }

    async fn broadcast(&self, doc: &Document) -> Result<()> {
        for s in &self.sinks {
            let _ = s.dispatch(doc).await;
        }
        // Dynamic subscribers — filtered + fanned out by the single owner.
        // Each callback is invoked synchronously; lossless w.r.t. order, but
        // slow callbacks back the engine up. (Use a buffering callback if you
        // need decoupling.)
        Self::dispatch_subscribers(&self.subscribers, doc);
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
                Err(StatusError::Cancelled) => Err(BsrsError::Cancelled),
                Err(StatusError::Timeout) => Err(BsrsError::Timeout(Duration::from_secs(0))),
                Err(StatusError::Failed(s)) => Err(BsrsError::Backend(s)),
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
        // Await *clones* of the members so the originals survive a move-on
        // timeout and can be restored to the group below. A clone shares the
        // same status state, so awaiting it observes the same completion.
        let waited = members.clone();
        let fut = async move {
            // Await every member *concurrently*, returning as soon as the first
            // one fails — bluesky `_wait` runs the group through asyncio
            // `FIRST_EXCEPTION` (run_engine.py:2311-2324), so a status that fails
            // fast short-circuits the wait even while an earlier-issued status in
            // the same group is still pending. A sequential await would block on
            // the earlier status and not observe the failure until it resolved
            // (and would hang indefinitely if it never did). The error still
            // propagates regardless of `error_on_timeout`; only the group-level
            // wait timeout below is gated by it, matching bluesky where
            // `error_on_timeout` suppresses only `WaitForTimeoutError`
            // (run_engine.py:2341-2346) while a `FailedStatus` always raises
            // (:2384).
            futures::future::try_join_all(waited)
                .await
                .map(|_| ())
                .map_err(|e| match e {
                    StatusError::Cancelled => BsrsError::Cancelled,
                    StatusError::Timeout => BsrsError::Timeout(Duration::from_secs(0)),
                    StatusError::Failed(s) => BsrsError::Backend(s),
                })
        };
        match timeout {
            Some(d) => match tokio::time::timeout(d, fut).await {
                Ok(r) => r,
                Err(_) => {
                    if error_on_timeout {
                        Err(BsrsError::Timeout(d))
                    } else {
                        // Move-on timeout (`error_on_timeout=false`): execution
                        // continues, so restore the group's members for a later
                        // `wait` on the same group to re-await the still-pending
                        // ones. bluesky puts the futures back on
                        // `WaitForTimeoutError` before returning
                        // (run_engine.py:2342-2344); a member that already
                        // completed is harmless to re-await (it resolves at once).
                        // Reached only on a genuine timeout, never on a member
                        // failure (that takes the `Ok(r)` arm above and aborts).
                        {
                            let mut state = self.state.lock().await;
                            state
                                .groups
                                .entry(group.to_string())
                                .or_default()
                                .members
                                .extend(members);
                        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// K1 regression: `install_signal_handler` must capture `Weak<Self>`,
    /// not `Arc<Self>`. Otherwise the watcher pins the engine forever and
    /// every `RunEngine::new(...)` leaks across `environment_open/close`.
    #[tokio::test]
    async fn install_signal_handler_does_not_pin_arc() {
        let re = Arc::new(RunEngine::new(Vec::new()));
        let before = Arc::strong_count(&re);
        re.install_signal_handler();
        // Let the spawn schedule and observe the Weak.
        tokio::task::yield_now().await;
        let after = Arc::strong_count(&re);
        assert_eq!(
            before, after,
            "signal handler must not increment Arc<RunEngine> strong count"
        );
    }

    /// A `wait` on a group must surface a member's failure as soon as it
    /// happens (bluesky FIRST_EXCEPTION), not after an earlier-issued member
    /// resolves. The group here holds a never-completing status *first* and a
    /// failed status *second*: concurrent waiting returns the failure promptly,
    /// while a sequential await would block on the pending status forever.
    #[tokio::test]
    async fn wait_group_short_circuits_on_first_failing_member() {
        let re = RunEngine::new(Vec::new());
        // member[0]: pending forever — keep its setter alive, never resolve it.
        let (pending, _keep) = Status::new();
        // member[1]: already failed.
        let failed = Status::fail(StatusError::Failed("boom".into()));
        {
            let mut state = re.state.lock().await;
            state.groups.insert(
                "g".into(),
                WaitGroup {
                    members: vec![pending, failed],
                },
            );
        }
        // 500ms is a generous upper bound: concurrently the failure resolves
        // immediately; a sequential await would block on the pending member
        // until this timeout elapses and never observe the failure.
        let result =
            tokio::time::timeout(Duration::from_millis(500), re.wait_group("g", true, None)).await;
        match result {
            Ok(Err(BsrsError::Backend(msg))) => assert_eq!(msg, "boom"),
            other => panic!("expected a prompt Backend(\"boom\") failure, got {other:?}"),
        }
    }

    /// A move-on wait (`error_on_timeout=false`) that times out must restore
    /// the group's still-pending members so a later `wait` on the same group
    /// re-awaits them, mirroring bluesky putting the futures back on
    /// `WaitForTimeoutError` (run_engine.py:2342-2344). Without restoration the
    /// later wait finds an empty group and returns immediately, silently
    /// dropping the unfinished operation.
    #[tokio::test]
    async fn wait_move_on_restores_pending_members_to_group() {
        let re = RunEngine::new(Vec::new());
        // A single member that never completes — keep its setter alive.
        let (pending, _keep) = Status::new();
        {
            let mut state = re.state.lock().await;
            state.groups.insert(
                "g".into(),
                WaitGroup {
                    members: vec![pending],
                },
            );
        }
        // error_on_timeout=false: wait up to 50ms, then move on without failing.
        let r = re
            .wait_group("g", false, Some(Duration::from_millis(50)))
            .await;
        assert!(
            r.is_ok(),
            "error_on_timeout=false must not fail the run on timeout, got {r:?}"
        );
        let restored = {
            let state = re.state.lock().await;
            state.groups.get("g").map(|g| g.members.len())
        };
        assert_eq!(
            restored,
            Some(1),
            "the still-pending member must be restored to the group on move-on"
        );
    }
}
