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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use crate::core::error::{BsrsError, Interrupt, Result};
use crate::core::msg::{Msg, MsgResult, RunMetadata, SubscriptionId, Thrown};
use crate::core::plan::{Plan, PlanItem};
use crate::core::status::{Status, StatusError};
use crate::event_model::compose::RunBundle;
use crate::event_model::{DocFilter, Document};
use futures::future::BoxFuture;
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::engine::bundler::{RunBundler, StreamObject};
use crate::engine::sink::DocumentSink;
use crate::engine::suspender::{Suspender, SuspenderHandle};

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

/// Channel a plan's [`PlanItem::Respond`] carries so the engine can hand the
/// [`MsgResult`] back into the plan. `None` for a plain `Bare` message.
type PlanResponder = Option<tokio::sync::oneshot::Sender<MsgResult>>;

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

/// Final state of a finished run — bsrs's analogue of bluesky's
/// `RunEngineResult` (run_engine.py:92). bluesky's `plan_result` (the value the
/// plan generator returns via `StopIteration`) has no bsrs equivalent: bsrs
/// plans are `async_stream`s that yield `Msg`s and return `()`, so there is no
/// plan return value to carry — that field is intentionally absent.
///
/// Not `Clone`: [`exception`](Self::exception) holds a [`BsrsError`], which is
/// not `Clone` (it wraps non-`Clone` sources such as `serde_json::Error`).
#[derive(Debug)]
pub struct RunResult {
    /// Every `RunStart` UID opened during this call, in the order the runs
    /// opened. Empty if the plan opened no run. bluesky
    /// `RunEngineResult.run_start_uids`.
    pub run_uids: Vec<String>,
    /// Final exit status (`success` / `abort` / `fail` / `halt` / `no-run`).
    pub exit_status: String,
    /// `true` unless the plan ran to a clean completion — i.e. it was stopped,
    /// aborted, halted, or failed. bluesky `RunEngineResult.interrupted`.
    pub interrupted: bool,
    /// Text reason for an abort/halt/stop, or the failure's error message;
    /// empty when the plan completed cleanly. bluesky `RunEngineResult.reason`.
    pub reason: String,
    /// The error that failed the run, if any — `Some` only on the `fail` path,
    /// `None` on success/abort/halt. bluesky `RunEngineResult.exception`.
    pub exception: Option<BsrsError>,
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

pub use crate::core::suspender::SuspendCallback;

/// The engine's control plane: the flags and wakeups a `pause`, `resume` or
/// suspension request touches, behind one owner the engine shares (as an
/// `Arc`) with the tasks that make such requests — an installed suspender's
/// watcher, a suspension's release — so none of them needs the engine itself.
/// Every `is_paused: false → true` transition goes through
/// [`Self::mark_paused`], every `true → false` through [`Self::wake`].
struct RunControl {
    /// The engine has claimed a run: `run_async` is in progress, paused or not.
    is_running: AtomicBool,
    is_paused: AtomicBool,
    /// Wakes the run loop parked in the pause gate (bluesky `_run_permit`).
    permit: Notify,
    /// Fired on every `is_paused: false → true` transition (immediate `pause`,
    /// a suspension, or a deferred pause applied at a checkpoint) — the pause
    /// edge, distinct from `permit` (which wakes the *plan* loop on resume).
    pause_notify: Notify,
    /// Per-run cancellation token. Renewed at every `run_async` entry so a
    /// stale `abort` / `stop` from a previous run doesn't immediately tear
    /// down the new one, and whenever the run loop acknowledges the pause or
    /// interrupt request that cancelled it.
    cancel: StdMutex<CancellationToken>,
    /// Count of pause/suspend requests, the pause-side twin of the engine's
    /// `interrupt_seq`: the run loop remembers the count as of the request it
    /// last acted on, so a handler unparked by the request's token
    /// cancellation is recognised as pausing rather than failing, and so a
    /// `resume` that lands before the loop reaches the pause gate cannot make
    /// the gate skip the request (the rewind on the way out is what replays
    /// the interrupted message). Bumped only by `mark_paused`.
    pause_seq: AtomicU64,
    /// The suspension behind the pause request being made: stored before
    /// `mark_paused` and taken by the pause gate as it enters, so its
    /// justification is what the gate records and its plans are what the gate
    /// runs. A second suspension requested while the run is already parked
    /// replaces an untaken one — see `RunEngine::install_suspender`.
    pending_suspend: StdMutex<Option<Suspension>>,
    /// Suspensions requested and not yet released. A release wakes the engine
    /// only when it is the last one — a beam dump closes the safety shutter
    /// too, and the scan must wait for both — and never over a manual pause.
    holds: AtomicUsize,
    /// A pause the user asked for (`pause`, SIGINT, `Msg::Pause`) is lifted
    /// only by `resume`, never by a suspension's release: in bluesky, Ctrl-C
    /// during a suspension cancels its `wait_for` (run_engine.py:1710-1716)
    /// and the suspender's event then resumes nothing.
    manual_pause: AtomicBool,
    /// Bumped by `reset_for_run`; a release carrying an older generation
    /// belongs to a suspension of a previous run and is ignored.
    run_gen: AtomicU64,
}

/// What a suspension runs around its wait and how it is recorded — bluesky
/// `request_suspend`'s `pre_plan`, `post_plan` and `justification`.
struct Suspension {
    justification: String,
    pre_plan: Option<SuspendCallback>,
    post_plan: Option<SuspendCallback>,
}

impl RunControl {
    fn new() -> Self {
        Self {
            is_running: AtomicBool::new(false),
            is_paused: AtomicBool::new(false),
            permit: Notify::new(),
            pause_notify: Notify::new(),
            cancel: StdMutex::new(CancellationToken::new()),
            pause_seq: AtomicU64::new(0),
            pending_suspend: StdMutex::new(None),
            holds: AtomicUsize::new(0),
            manual_pause: AtomicBool::new(false),
            run_gen: AtomicU64::new(0),
        }
    }

    /// Reset for a new run: nothing a previous run's pause, suspension or
    /// interrupt left behind may reach this one.
    fn reset_for_run(&self) {
        self.run_gen.fetch_add(1, Ordering::SeqCst);
        self.is_paused.store(false, Ordering::SeqCst);
        self.manual_pause.store(false, Ordering::SeqCst);
        self.holds.store(0, Ordering::SeqCst);
        *self.pending_suspend.lock().unwrap() = None;
        *self.cancel.lock().unwrap() = CancellationToken::new();
    }

    /// Single owner of the `is_paused: false → true` transition. Stores the
    /// flag, counts the request under the token lock and cancels the run's
    /// token so a handler parked in `Wait`/`Sleep`/`WaitFor` (or an inline
    /// status await) returns at once — bluesky's `_request_pause_coro` and
    /// `_request_suspend` both cancel the `_run` task (run_engine.py:856,
    /// :1253), which is what lets `stop_on_pause` reach a motor while it is
    /// still moving. Then signals `pause_notify` for whoever watches the pause
    /// edge.
    fn mark_paused(&self) {
        self.is_paused.store(true, Ordering::SeqCst);
        {
            let token = self.cancel.lock().unwrap();
            self.pause_seq.fetch_add(1, Ordering::SeqCst);
            token.cancel();
        }
        self.pause_notify.notify_waiters();
    }

    /// A pause the user asked for: lifted only by [`Self::resume`].
    fn pause(&self) {
        self.manual_pause.store(true, Ordering::SeqCst);
        self.mark_paused();
    }

    /// The user's resume: lifts the pause whatever requested it.
    fn resume(&self) {
        self.manual_pause.store(false, Ordering::SeqCst);
        self.wake();
    }

    /// Single owner of the `is_paused: true → false` transition: clears the
    /// flag and wakes the run loop parked in the pause gate.
    fn wake(&self) {
        self.is_paused.store(false, Ordering::SeqCst);
        self.permit.notify_waiters();
    }

    /// Suspend the run until `fut` resolves (bluesky `request_suspend`). The
    /// suspension is one hold on the engine; `fut` resolving releases it, and
    /// the run wakes once no hold and no manual pause remains.
    fn request_suspend(self: &Arc<Self>, fut: BoxFuture<'static, ()>, suspension: Suspension) {
        // Store the suspension BEFORE `mark_paused` flips `is_paused`: the
        // pause gate takes it the moment it observes the pause.
        *self.pending_suspend.lock().unwrap() = Some(suspension);
        self.holds.fetch_add(1, Ordering::SeqCst);
        let gen = self.run_gen.load(Ordering::SeqCst);
        self.mark_paused();
        let me = Arc::downgrade(self);
        tokio::spawn(async move {
            fut.await;
            if let Some(me) = me.upgrade() {
                me.release(gen);
            }
        });
    }

    /// One suspension requested in run generation `gen` has lifted.
    fn release(&self, gen: u64) {
        if gen != self.run_gen.load(Ordering::SeqCst) {
            return;
        }
        let before = self
            .holds
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |h| {
                Some(h.saturating_sub(1))
            })
            .unwrap_or(0);
        if before <= 1 && !self.manual_pause.load(Ordering::SeqCst) {
            self.wake();
        }
    }
}

/// The RunEngine.
pub struct RunEngine {
    sinks: Vec<Arc<dyn DocumentSink>>,
    /// The control plane (`pause`/`resume`/suspend flags, wakeups, the run
    /// token), shared with the tasks that pause the engine.
    ctl: Arc<RunControl>,
    deferred_pause: AtomicBool,
    is_aborting: AtomicBool,
    is_halting: AtomicBool,
    is_stopping: AtomicBool,
    /// Count of `stop`/`abort`/`halt` requests. The run loop remembers the
    /// count as of the request it last acted on, so it can tell a new request
    /// from the one already unwinding the plan — a second `abort` during
    /// cleanup is a second interrupt. Bumped only by `request_interrupt`.
    interrupt_seq: AtomicU64,
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
    /// Open runs keyed by run key (`None` = the single default run). The bsrs
    /// analogue of bluesky's `_run_bundlers: dict[run_key, RunBundler]`
    /// (run_engine.py:504): several runs can be open at once, each owning its
    /// own bundler, monitors, uncollected flyers and asset cache (see
    /// [`RunSlot`]). A `Msg::InRun { run, .. }` routes a bundler-touching verb
    /// to `Some(run)`; an unwrapped verb targets `None`. Empty = no run open.
    runs: HashMap<Option<String>, RunSlot>,
    groups: HashMap<String, WaitGroup>,
    staged: Vec<Arc<dyn crate::core::msg::StageableObj>>,
    /// Movables touched by `Msg::Set` during this run, keyed by name
    /// for dedup. Engine walks this on pause / cleanup and calls
    /// `MovableObj::stop_on_pause(success=true)`. Mirrors bluesky's
    /// `_movable_objs_touched`.
    movable_objs_touched: HashMap<String, Arc<dyn crate::core::msg::MovableObj>>,
    /// Flyers touched by `Msg::Kickoff` during this run, same role
    /// as `movable_objs_touched`.
    flyable_objs_touched: HashMap<String, Arc<dyn crate::core::msg::FlyableObj>>,
    /// Devices that opted into pause/resume hooks via
    /// `Msg::RegisterPausable` or `RunEngine::register_pausable`.
    /// Walked on every pause-enter and resume.
    pausables: HashMap<String, Arc<dyn crate::core::msg::PausableObj>>,
    /// Subscription ids added during this run (via `Msg::Subscribe`
    /// or the positional `subs` arg on `run_async_with`). Mirror of
    /// bluesky's `_temp_callback_ids` — entries are removed
    /// automatically when the run ends.
    temp_subscribers: Vec<SubscriptionId>,
    /// Active `contingency_wrapper` sinks, innermost last (a LIFO stack).
    /// While non-empty, a message error or a `stop`/`abort` request is thrown
    /// into the top sink and the run keeps going, rather than ending — so the
    /// wrapper that pushed it can run its `except`/`finally` recovery and
    /// re-raise via `Msg::Raise`. Pushed by `Msg::PushContingency`, popped by
    /// `Msg::PopContingency`; the equivalent of the exception propagating to
    /// the nearest enclosing generator `try` in bluesky.
    contingency_stack: Vec<crate::core::msg::ContingencySink>,
    msg_cache: VecDeque<Msg>,
    replay_queue: VecDeque<Msg>,
    rewindable: bool,
    suspenders: HashMap<u64, SuspenderHandle>,
}

/// All state owned by one open run, keyed in [`EngineState::runs`] by run key.
/// The bsrs analogue of one bluesky `RunBundler` plus the per-run engine-side
/// registries that in bsrs live beside the bundler. Grouping them here is what
/// makes multiple runs coexist: routing a `Msg::InRun { run, .. }` selects one
/// `RunSlot`, so its documents, monitors, flyers and assets never bleed into
/// another open run. Created in [`RunEngine::open_run`]; removed in
/// [`RunEngine::close_run_if_open`].
struct RunSlot {
    /// The run's document composer (RunStart/Descriptor/Event/RunStop, sequence
    /// counters, config caches). Bluesky's `RunBundler`.
    bundler: RunBundler,
    /// Read objects in this run's current event bundle that also write external
    /// assets ([`ReadableObj::writes_external_assets`]). Populated on `Msg::Read`
    /// while bundling; drained on `Msg::Save` (their `collect_asset_docs_dyn`
    /// emits `StreamResource`/`StreamDatum` stamped with the bundle's
    /// descriptor); cleared when the bundle ends (`Save`/`Drop`/`rewind`) or a
    /// new one begins (`Create`). Bluesky's per-bundle `_asset_docs_cache`
    /// (bundlers.py:158), reset on `create`.
    ///
    /// [`ReadableObj::writes_external_assets`]: crate::core::msg::ReadableObj::writes_external_assets
    bundle_asset_objs: Vec<Arc<dyn crate::core::msg::ReadableObj>>,
    /// Live monitor pumps for this run, keyed by the monitored object's name
    /// (`obj.name()`) — the identity `Msg::Unmonitor(obj)` carries — independent
    /// of the descriptor stream name. The `MonitorTask` drops the `Subscription`
    /// (RAII unsubscribe) and aborts the pump on `Drop`. Inserted by
    /// `Msg::Monitor`, removed by `Msg::Unmonitor`.
    monitor_tasks: HashMap<String, MonitorTask>,
    /// Active monitor registrations for this run, keyed by `obj.name()` like
    /// `monitor_tasks` — the persistent record (obj + stream) that outlives the
    /// pump. Kept across a pause so resume can re-install the pump; cleared by
    /// `Msg::Unmonitor` and at run close. bluesky's `_monitor_params`.
    monitored: HashMap<String, MonitorSpec>,
    /// Flyers kicked off into this run but not yet collected — the collectable
    /// view of every `Msg::Kickoff` object that exposes one
    /// ([`FlyableObj::as_collectable`]), keyed by name. An entry is removed when
    /// the object is collected (`Msg::Collect`). At run finalize the engine
    /// drains whatever remains so a flyer aborted between kickoff and collect
    /// still lands its buffered data before RunStop. Mirrors bluesky's
    /// `_uncollected` set + `backstop_collect` (bundlers.py:172, 1190).
    uncollected: HashMap<String, Arc<dyn crate::core::msg::CollectableObj>>,
}

impl RunSlot {
    /// A fresh slot wrapping `bundler`, with empty per-run registries.
    fn new(bundler: RunBundler) -> Self {
        Self {
            bundler,
            bundle_asset_objs: Vec::new(),
            monitor_tasks: HashMap::new(),
            monitored: HashMap::new(),
            uncollected: HashMap::new(),
        }
    }
}

impl EngineState {
    /// The [`RunSlot`] for `run` (`None` = default run), if that run is open.
    fn run(&self, run: &Option<String>) -> Option<&RunSlot> {
        self.runs.get(run)
    }

    /// Mutable [`RunSlot`] for `run`, if open.
    fn run_mut(&mut self, run: &Option<String>) -> Option<&mut RunSlot> {
        self.runs.get_mut(run)
    }

    /// This run's bundler, if the run is open.
    fn bundler(&self, run: &Option<String>) -> Option<&RunBundler> {
        self.runs.get(run).map(|s| &s.bundler)
    }

    /// This run's bundler mutably, if open.
    fn bundler_mut(&mut self, run: &Option<String>) -> Option<&mut RunBundler> {
        self.runs.get_mut(run).map(|s| &mut s.bundler)
    }

    /// Whether `run` is currently open.
    fn run_open(&self, run: &Option<String>) -> bool {
        self.runs.contains_key(run)
    }

    /// Whether any run at all is open.
    fn any_run_open(&self) -> bool {
        !self.runs.is_empty()
    }

    /// Whether any open run has an event bundle in flight (between `create` and
    /// `save`). bluesky rejects `checkpoint`/`configure` when any bundler is
    /// bundling (run_engine.py:2437, 2515).
    fn any_bundling(&self) -> bool {
        self.runs.values().any(|s| s.bundler.is_bundling())
    }
}

/// One live monitor pump. Drops abort the pump task and (transitively)
/// the held `Subscription`, releasing the backend slot (rule **K1**+**K2**).
///
/// `Msg::Unmonitor` goes through [`MonitorTask::stop`] instead of the abort:
/// an update that reached the subscription while the object was monitored
/// but that the pump has not consumed yet (a starved worker, a set issued
/// just before the unmonitor) is still emitted, then the pump exits. bluesky
/// cannot lose that update because its monitor callback is synchronous.
struct MonitorTask {
    stop: Arc<tokio::sync::Notify>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl MonitorTask {
    /// Ask the pump to drain its pending update and exit, then wait for it.
    async fn stop(mut self) {
        self.stop.notify_one();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for MonitorTask {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

/// The persistent record of one active monitor — the monitored object and its
/// resolved stream name — kept independently of the live `MonitorTask` pump.
/// The bsrs equivalent of bluesky's `_monitor_params` (bundlers.py:500): it
/// survives a pause (which drops the pump but not the registration) so the
/// monitor can be re-installed on resume (`restore_monitors`, :665).
#[derive(Clone)]
struct MonitorSpec {
    obj: Arc<dyn crate::core::msg::MonitorableObj>,
    stream: String,
}

/// Read one object's configuration into the descriptor's per-object
/// [`Configuration`] shape — values/timestamps from `read_configuration`,
/// keys from `describe_configuration`. bluesky `_StreamCache.cache_read_config`
/// + `_cache_describe_config` (bundlers.py:109-130).
async fn read_object_configuration(
    obj: &dyn crate::core::msg::ConfigurableObj,
) -> Result<crate::event_model::Configuration, BsrsError> {
    let (readings, data_keys) = tokio::join!(
        obj.read_configuration_dyn(),
        obj.describe_configuration_dyn()
    );
    let (readings, data_keys) = (readings?, data_keys?);
    let mut data = HashMap::new();
    let mut timestamps = HashMap::new();
    for (k, r) in readings {
        data.insert(k.clone(), r.value);
        timestamps.insert(k, r.timestamp);
    }
    Ok(crate::event_model::Configuration {
        data,
        data_keys,
        timestamps,
    })
}

/// Validate + stamp one drain of external-asset documents, the engine-owned
/// half of every `StreamResource`/`StreamDatum` emission. Port of bluesky
/// `_pack_external_assets` + `_pack_seq_nums_into_stream_datum`
/// (bundlers.py:830-940):
///
/// - `StreamResource`: run_start stamped (single owner: the engine); a uid
///   emitted twice in one run is an error; its `data_key` must be one of the
///   descriptor's external (`STREAM:`) keys and is recorded in the run's
///   `resource_data_keys` registry.
/// - `StreamDatum`: must reference a registered resource uid; must arrive
///   with `seq_nums {0, 0}` (the sequence counter belongs to the run, never
///   the writer); every datum in one drain must span the same indices width
///   (detectors in a stream advance together); `seq_nums` is filled with
///   `[next_seq, next_seq + width)`.
/// - After the drain: if any datum arrived, every external data key of the
///   descriptor must have received one — a writer silently dropping one of
///   its datasets desyncs the stream.
///
/// Returns the shared indices width (0 when the drain holds no datums).
fn pack_external_assets(
    docs: &mut [Document],
    run_start: &str,
    next_seq: u64,
    external_data_keys: &[String],
    resource_data_keys: &mut HashMap<String, String>,
) -> Result<u64, BsrsError> {
    let mut width: Option<u64> = None;
    let mut data_keys_received: std::collections::HashSet<String> = Default::default();
    for doc in docs.iter_mut() {
        match doc {
            Document::StreamResource(r) => {
                if r.run_start.is_none() {
                    r.run_start = Some(run_start.to_string());
                }
                if resource_data_keys.contains_key(&r.uid) {
                    return Err(BsrsError::Plan(format!(
                        "Received `stream_resource` with uid {} twice",
                        r.uid
                    )));
                }
                if !external_data_keys.contains(&r.data_key) {
                    return Err(BsrsError::Plan(format!(
                        "Received a `stream_resource` with data_key {} that is \
                         not in the descriptor 'STREAM:' data_keys \
                         {external_data_keys:?}",
                        r.data_key
                    )));
                }
                resource_data_keys.insert(r.uid.clone(), r.data_key.clone());
            }
            Document::StreamDatum(d) => {
                if d.seq_nums.start != 0 || d.seq_nums.stop != 0 {
                    return Err(BsrsError::Plan(format!(
                        "StreamDatum {} arrived with pre-filled seq_nums \
                         [{}, {}); the run engine owns the sequence counter and \
                         writers must emit seq_nums {{0, 0}}",
                        d.uid, d.seq_nums.start, d.seq_nums.stop
                    )));
                }
                let Some(data_key) = resource_data_keys.get(&d.stream_resource) else {
                    return Err(BsrsError::Plan(format!(
                        "Received a `stream_datum` referring to an unknown \
                         stream resource {}",
                        d.stream_resource
                    )));
                };
                data_keys_received.insert(data_key.clone());
                let w = d.indices.stop.saturating_sub(d.indices.start);
                match width {
                    None => width = Some(w),
                    Some(prev) if prev != w => {
                        return Err(BsrsError::Plan(format!(
                            "StreamDatum {} spans {w} indices but another datum \
                             in the same drain spans {prev}; every detector in a \
                             stream must advance by the same number of indices",
                            d.uid
                        )));
                    }
                    Some(_) => {}
                }
                d.seq_nums = crate::event_model::StreamRange {
                    start: next_seq,
                    stop: next_seq + w,
                };
            }
            _ => {}
        }
    }
    if !data_keys_received.is_empty()
        && (external_data_keys.len() != data_keys_received.len()
            || !external_data_keys
                .iter()
                .all(|k| data_keys_received.contains(k)))
    {
        return Err(BsrsError::Plan(format!(
            "Received `stream_datum` for the data keys {data_keys_received:?}, \
             but the descriptor's 'STREAM:' data keys are \
             {external_data_keys:?}; every external data key must receive a \
             `stream_datum` in the same drain"
        )));
    }
    Ok(width.unwrap_or(0))
}

/// An interrupt request the run loop has not acted on yet.
enum InterruptRequest {
    /// `halt`: drop the plan, no cleanup message runs.
    Halt,
    /// `stop`/`abort`: throw the interrupt into the plan's innermost
    /// contingency region so its cleanup runs.
    Throw(Interrupt),
}

impl RunEngine {
    /// Construct a fresh RunEngine with the given sinks.
    pub fn new(sinks: Vec<Arc<dyn DocumentSink>>) -> Self {
        Self {
            sinks,
            ctl: Arc::new(RunControl::new()),
            deferred_pause: AtomicBool::new(false),
            is_aborting: AtomicBool::new(false),
            is_halting: AtomicBool::new(false),
            is_stopping: AtomicBool::new(false),
            interrupt_seq: AtomicU64::new(0),
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
        // Enforce the single-plan invariant by construction: at most one plan
        // runs on a RunEngine at a time. `compare_exchange(false → true)` both
        // claims the engine and rejects a *concurrent* `run_async` — e.g. a
        // local console `RE:run` firing while the qs queue worker is mid-plan
        // (or vice versa), which would otherwise corrupt the single shared run
        // loop (pause gate, permit, replay queue, `runs` map). The claim is
        // released at run end (`is_running.store(false)` below); there is no
        // early return between claim and release, so the flag never leaks. A
        // rejection here has zero side effects: the `before_plan` hook and every
        // state reset are skipped, so a rejected caller leaves the in-flight
        // run untouched.
        if self
            .ctl
            .is_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(BsrsError::State(
                "a plan is already running on this RunEngine".into(),
            ));
        }
        // before_plan hook — fires only once the engine has claimed the run, so
        // it never fires for a rejected concurrent call. `state()` reports
        // `Running` during the hook.
        if let Some(h) = self.before_plan.lock().unwrap().clone() {
            h();
        }
        // Reset abort/halt/stop flags from a previous (terminated) run so
        // `RunEngine` is reusable across plans.
        self.is_aborting.store(false, Ordering::SeqCst);
        self.is_halting.store(false, Ordering::SeqCst);
        self.is_stopping.store(false, Ordering::SeqCst);
        // Interrupt requests up to here belonged to the previous run; one that
        // arrives from now on (even during the suspender gate below) is this
        // run's to act on.
        let thrown_seq = self.interrupt_seq.load(Ordering::SeqCst);
        // Clear any interrupt reason left by a previous run so it cannot leak
        // into this run's RunStop (bluesky run_engine.py:1497).
        self.interrupt_reason.lock().unwrap().clear();
        // Pause, suspension and token state of a previous run stop here.
        self.ctl.reset_for_run();
        let pause_seen = self.ctl.pause_seq.load(Ordering::SeqCst);
        // A deferred pause that never reached a Checkpoint before its run
        // ended must not carry over and pause this run's first Checkpoint.
        self.deferred_pause.store(false, Ordering::SeqCst);
        // Reset SIGINT 3-tap counter — a previous session's taps must
        // not put a fresh run into the abort/halt path on the very
        // first ctrl-c.
        self.sigint_count.store(0, Ordering::SeqCst);
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
        // ENG-12: honor suspenders that are already tripped at plan start.
        // bluesky's `__call__` collects every currently-tripped suspender's
        // clear-future and prepends a `wait_for` before the plan, so a scan
        // never runs its first point while a condition (e.g. beam down) is bad
        // (run_engine.py:933-967). Gather the tripped futures from the
        // installed suspenders, log the justifications, and wait for all to
        // clear before entering the run loop. A non-tripped suspender returns
        // `None`, so a clear engine starts immediately. This runs *outside* the
        // loop_timeout below because, like bluesky, waiting for beam is
        // intentionally unbounded.
        let tripped: Vec<(String, BoxFuture<'static, ()>)> = {
            let state = self.state.lock().await;
            state
                .suspenders
                .values()
                .filter_map(|h| h.inner.tripped().map(|f| (h.inner.name().to_string(), f)))
                .collect()
        };
        if !tripped.is_empty() {
            let names: Vec<&str> = tripped.iter().map(|(n, _)| n.as_str()).collect();
            tracing::warn!(
                "at least one suspender is tripped; waiting to start: {}",
                names.join(", ")
            );
            futures::future::join_all(tripped.into_iter().map(|(_, f)| f)).await;
        }

        let timeout = *self.loop_timeout.lock().unwrap();
        let outcome = match timeout {
            Some(d) => {
                match tokio::time::timeout(d, self.run_loop(plan, thrown_seq, pause_seen)).await {
                    Ok(r) => r,
                    Err(_) => {
                        self.ctl.cancel.lock().unwrap().cancel();
                        Err(BsrsError::Timeout(d))
                    }
                }
            }
            None => self.run_loop(plan, thrown_seq, pause_seen).await,
        };
        // Cleanup: stop touched movables / flyers, unstage anything
        // still staged. Mirrors bluesky's `_run`
        // exit chain (`_stop_movable_objects` then `unstage`).
        let mut state = self.state.lock().await;
        let staged = std::mem::take(&mut state.staged);
        let movables = std::mem::take(&mut state.movable_objs_touched);
        let flyables = std::mem::take(&mut state.flyable_objs_touched);
        // Drop every run's slot. `drain_and_close` already closed all runs at
        // finalize (draining each `uncollected` via `backstop_collect` before its
        // RunStop and removing the slot), so this normally takes an empty map; it
        // is the defensive backstop for a slot that survived — dropping it aborts
        // that run's monitor pumps (K1: `monitor_tasks`), drops its monitor
        // registry (`monitored` — run over, no resume) and any un-drained
        // `uncollected`, so nothing leaks into the next run.
        let _ = std::mem::take(&mut state.runs);
        // A run that ends with a contingency region still on the stack (e.g. an
        // abort tore the wrapper down before its PopContingency) must not leak
        // it into the next run.
        let _ = std::mem::take(&mut state.contingency_stack);
        let temp_subs = std::mem::take(&mut state.temp_subscribers);
        let _ = std::mem::take(&mut state.pausables);
        // Suspenders stay installed across runs, as bluesky's `_suspenders`
        // set does: only `remove_suspender` / `clear_suspenders` touch it.
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
        self.ctl.is_running.store(false, Ordering::SeqCst);
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
        let state = self.state.lock().await;
        // The default (unkeyed) run is what a plain `Msg::OpenRun` opens and what
        // the Lua bridge's `msg.open_run` return surfaces; fall back to any open
        // run so a keyed-only plan still gets a UID.
        state
            .bundler(&None)
            .or_else(|| state.runs.values().next().map(|s| &s.bundler))
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
        if self.ctl.is_paused.load(Ordering::SeqCst) {
            return EngineRunState::Paused;
        }
        if self.ctl.is_running.load(Ordering::SeqCst) {
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
    pub async fn register_pausable(&self, obj: Arc<dyn crate::core::msg::PausableObj>) {
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
    pub fn suspend_until(&self, fut: BoxFuture<'static, ()>) {
        self.suspend_until_with(fut, None);
    }

    /// Like [`Self::suspend_until`] but records `justification` (default
    /// `"suspended"`) into the interruptions stream when recording
    /// is enabled. Mirrors bluesky's `request_suspend(fut, …,
    /// justification=…)`.
    pub fn suspend_until_with(&self, fut: BoxFuture<'static, ()>, justification: Option<String>) {
        self.suspend_until_with_plans(fut, justification, None, None);
    }

    /// Like [`Self::suspend_until_with`] but injects `pre_plan` on suspension
    /// (after motors stop, before the wait) and `post_plan` on resume (after
    /// Pausable devices re-notify, before the rewind replay). Mirrors bluesky's
    /// `request_suspend(fut, pre_plan=…, post_plan=…)` (`run_engine.py:1199`):
    /// close a shutter before parking, re-open it on resume. Each factory is a
    /// [`SuspendCallback`]; its plan's messages run through the same handlers as
    /// the main plan (real device motion + document emission), *not* as opaque
    /// side-effects. `None` for either leaves that phase unchanged.
    pub fn suspend_until_with_plans(
        &self,
        fut: BoxFuture<'static, ()>,
        justification: Option<String>,
        pre_plan: Option<SuspendCallback>,
        post_plan: Option<SuspendCallback>,
    ) {
        self.ctl.request_suspend(
            fut,
            Suspension {
                justification: justification.unwrap_or_else(|| "suspended".into()),
                pre_plan,
                post_plan,
            },
        );
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
        crate::core::runtime::block_on(self.run_async(plan))
    }

    /// External: request a pause. If `defer = true`, the pause takes effect at
    /// the next `Checkpoint`; otherwise immediately, interrupting the message
    /// in flight. The interrupted message is not lost: the rewind on resume
    /// replays from the last checkpoint.
    pub fn pause(&self, defer: bool) {
        if defer {
            self.deferred_pause.store(true, Ordering::SeqCst);
        } else {
            self.ctl.pause();
        }
    }

    /// External: resume a paused engine. Replays the rewind cache before
    /// pulling the next plan message.
    pub fn resume(&self) {
        self.ctl.resume();
    }

    /// External: abort the run. Closes the open run with `exit_status="abort"`.
    /// The `reason` is threaded onto the closing `RunStop` document, matching
    /// bluesky's `RE.abort(reason)` (run_engine.py:1336).
    pub fn abort(&self, reason: impl Into<String>) {
        *self.interrupt_reason.lock().unwrap() = reason.into();
        self.is_aborting.store(true, Ordering::SeqCst);
        // An abort supersedes a stop still unwinding (bluesky's `_state` simply
        // becomes "aborting"); a stop never downgrades an abort.
        self.is_stopping.store(false, Ordering::SeqCst);
        self.request_interrupt();
    }

    /// External: halt — like abort but skips run-level cleanup. The `reason` is
    /// threaded onto the closing `RunStop` document.
    pub fn halt(&self, reason: impl Into<String>) {
        *self.interrupt_reason.lock().unwrap() = reason.into();
        self.is_halting.store(true, Ordering::SeqCst);
        self.is_aborting.store(true, Ordering::SeqCst);
        self.request_interrupt();
    }

    /// External: graceful stop — like abort, but the run closes with
    /// `exit_status="success"`. Mirrors bluesky's `RE.stop`.
    pub fn stop(&self) {
        self.is_stopping.store(true, Ordering::SeqCst);
        self.is_aborting.store(true, Ordering::SeqCst);
        self.request_interrupt();
    }

    /// Single owner of "an interrupt was requested", called by `stop`/`abort`/
    /// `halt` once their flags are set. Wakes the run loop wherever it is
    /// parked: clears the pause so a paused loop wakes through `permit`, and
    /// cancels the run's token so a handler racing it (`Sleep`, `WaitFor`)
    /// returns. The request is counted under the token lock, so the loop never
    /// observes the cancelled token without the count that explains it.
    fn request_interrupt(&self) {
        {
            let token = self.ctl.cancel.lock().unwrap();
            self.interrupt_seq.fetch_add(1, Ordering::SeqCst);
            token.cancel();
        }
        self.ctl.wake();
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
        self.ctl.is_paused.load(Ordering::SeqCst)
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

    /// Collect one collectable object into the open run: declare any new
    /// stream(s), emit their stream-asset docs, then emit the per-collect
    /// events. Sole owner of the collect path — driven by `Msg::Collect` and by
    /// [`Self::backstop_collect`] at finalize. On success the object is removed
    /// from the uncollected set so the finalize backstop does not re-drain it.
    async fn collect_object(
        &self,
        run_key: &Option<String>,
        obj: Arc<dyn crate::core::msg::CollectableObj>,
        stream_name: Option<String>,
    ) -> Result<()> {
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
            if !state.run_open(run_key) {
                return Err(BsrsError::Plan(
                    "A 'collect' message was sent but no run is open".into(),
                ));
            }
        }
        let descs = obj.describe_collect_dyn().await?;
        // The collect object's configuration, for any descriptor
        // declared below — bluesky's collect path runs
        // `ensure_cached(obj)` + `_prepare_stream`, which folds the
        // object's config into the descriptor (bundlers.py:814-819).
        let config = self
            .ensure_object_configuration(run_key, obj.name(), obj.as_configurable())
            .await?;
        let new_descriptors: Vec<crate::event_model::EventDescriptor> = {
            let mut state = self.state.lock().await;
            let bundler = state
                .bundler_mut(run_key)
                .ok_or_else(|| BsrsError::Plan("Collect with no open run".into()))?;
            let mut out = Vec::new();
            for (name, dks) in &descs {
                if bundler.descriptor_uid(name).is_none() {
                    let collected = StreamObject {
                        object: Some(obj.name().to_string()),
                        data_keys: dks.clone(),
                        hint_fields: None,
                        configuration: config.clone(),
                    };
                    out.push(bundler.declare_stream(name.clone(), vec![collected]));
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
        let (collect_stream, collect_descriptor) = {
            let state = self.state.lock().await;
            let bundler = state.bundler(run_key).ok_or_else(|| {
                BsrsError::Plan("Collect lost open run before stream docs".into())
            })?;
            let stream = descs.keys().next().cloned();
            (
                stream.clone(),
                stream.and_then(|s| bundler.descriptor_uid(&s)),
            )
        };
        if let Some(descriptor_uid) = collect_descriptor {
            let mut asset_docs = obj.collect_stream_docs_dyn(&descriptor_uid).await?;
            if !asset_docs.is_empty() {
                // Validate + stamp the drain, fill the datums'
                // seq_nums from the stream's counter, and advance it
                // by the indices width: no per-frame Event exists on
                // this path to advance it ("we do it ourselves",
                // bluesky bundlers.py:1180). The summary index event
                // composed below then lands after the datum span,
                // keeping one monotonic sequence axis.
                let stream = collect_stream
                    .as_deref()
                    .expect("descriptor_uid implies a collect stream name");
                let mut state = self.state.lock().await;
                let bundler = state.bundler_mut(run_key).ok_or_else(|| {
                    BsrsError::Plan("Collect lost open run before stream docs".into())
                })?;
                let next_seq = bundler.compose().peek_next_seq(stream).ok_or_else(|| {
                    BsrsError::Plan(format!(
                        "Collect stream `{stream}` has a descriptor but \
                         no sequence counter"
                    ))
                })?;
                let external_keys = bundler
                    .compose()
                    .external_data_keys(stream)
                    .unwrap_or_default();
                let run_start = bundler.start_uid.clone();
                let width = pack_external_assets(
                    &mut asset_docs,
                    &run_start,
                    next_seq,
                    &external_keys,
                    bundler.stream_resource_data_keys_mut(),
                )?;
                if width > 0 {
                    bundler.compose().advance_seq(stream, width);
                }
                drop(state);
                for doc in asset_docs {
                    self.broadcast(&doc).await?;
                }
            }
        }
        let events = obj.collect_dyn().await?;
        for (name, data, timestamps) in events {
            let stream = stream_name.clone().unwrap_or(name);
            let ev = {
                let state = self.state.lock().await;
                let bundler = state.bundler(run_key).ok_or_else(|| {
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
        // Collected: drop it from this run's uncollected set so the finalize
        // backstop will not drain it a second time. bluesky
        // `_uncollected.discard(obj)` (bundlers.py:1090).
        if let Some(slot) = self.state.lock().await.run_mut(run_key) {
            slot.uncollected.remove(obj.name());
        }
        Ok(())
    }

    /// Before a run closes, drain any flyer that was kicked off but never
    /// explicitly collected, so its buffered data lands in the run rather than
    /// being lost. Mirrors bluesky's `RunBundler.backstop_collect`
    /// (bundlers.py:1190), called from the `_run` finally on every exit path
    /// (run_engine.py:1777). Runs only while the run is still open, and swallows
    /// per-object errors — "some might not support partial collection".
    async fn backstop_collect(&self) {
        // Every open run's uncollected flyers, paired with the run key so each
        // drains into its own bundler. bluesky iterates `_run_bundlers`
        // (run_engine.py:1777).
        let pending: Vec<(Option<String>, Arc<dyn crate::core::msg::CollectableObj>)> = {
            let state = self.state.lock().await;
            state
                .runs
                .iter()
                .flat_map(|(run, slot)| {
                    slot.uncollected
                        .values()
                        .cloned()
                        .map(move |obj| (run.clone(), obj))
                })
                .collect()
        };
        for (run, obj) in pending {
            let name = obj.name().to_string();
            if let Err(e) = self.collect_object(&run, obj, None).await {
                tracing::warn!("backstop collect failed for flyer {name}: {e}");
            }
        }
    }

    /// Drain uncollected flyers, then close *every* open run with
    /// `status`/`reason`. The single finalize path for `run_loop`: bluesky runs
    /// `backstop_collect` immediately before closing each run in its `_run`
    /// finally (run_engine.py:1777, 1789), so every run — success or abort —
    /// emits its flyer data before the RunStop that ends it. Runs left open by a
    /// plan that never issued `CloseRun` are all closed here.
    async fn drain_and_close(&self, status: &str, reason: Option<String>) -> Result<()> {
        self.backstop_collect().await;
        let open_keys: Vec<Option<String>> = self.state.lock().await.runs.keys().cloned().collect();
        for key in open_keys {
            self.close_run_if_open(&key, status, reason.clone()).await?;
        }
        Ok(())
    }

    /// Assemble the [`RunResult`], deriving `interrupted` and `reason` from the
    /// engine's terminal state. `exception` is `Some` only on the failure path;
    /// its message then supplies `reason`, otherwise the caller-set abort/halt/
    /// stop reason ([`stop_reason`](Self::stop_reason)) does.
    fn build_result(
        &self,
        run_uids: Vec<String>,
        exit_status: String,
        exception: Option<BsrsError>,
    ) -> RunResult {
        // Interrupted = anything but a clean run to completion. bluesky sets
        // `_interrupted` on pause/stop/abort/halt/fail; a natural end leaves it
        // false. (Pause returns before a result is built, so it is not tested
        // here.)
        let interrupted = exit_status == "fail"
            || self.is_halting.load(Ordering::SeqCst)
            || self.is_stopping.load(Ordering::SeqCst)
            || self.is_aborting.load(Ordering::SeqCst);
        let reason = match &exception {
            Some(e) => e.to_string(),
            None => self.stop_reason().unwrap_or_default(),
        };
        RunResult {
            run_uids,
            exit_status,
            interrupted,
            reason,
            exception,
        }
    }

    async fn run_loop(
        &self,
        plan: Plan,
        mut thrown_seq: u64,
        mut pause_seen: u64,
    ) -> Result<RunResult> {
        let plan = Mutex::new(plan);
        // Every RunStart UID opened during this call, in open order (bluesky
        // accumulates `_run_start_uids` in `_open_run`); `handle` returns a UID
        // exactly once per `Msg::OpenRun`, so no de-duplication is needed.
        let mut run_uids: Vec<String> = Vec::new();

        loop {
            self.pause_gate(thrown_seq, &mut pause_seen).await;
            // A `stop`/`abort`/`halt` requested since the last message. bluesky's
            // `_run` throws `RequestStop`/`RequestAbort` into the plan so the
            // `finally`/`except` blocks of its wrappers issue their cleanup
            // messages (run_engine.py:1730-1740, 1586-1600), and drops the plan
            // on `halt` (`PlanHalt` is a `GeneratorExit`: no cleanup may yield).
            // Here the request is thrown into the innermost contingency region;
            // a plan with none has nothing to catch it and is dropped.
            match self.take_interrupt(&mut thrown_seq) {
                Some(InterruptRequest::Halt) => {
                    return self.end_interrupted(run_uids, "halt").await;
                }
                Some(InterruptRequest::Throw(kind)) => {
                    let thrown = Thrown::Interrupt(kind, self.stop_reason());
                    if !self.throw_into_plan(thrown).await {
                        return self.end_interrupted(run_uids, kind.exit_status()).await;
                    }
                    continue;
                }
                None => {}
            }
            let Some((msg, responder)) = self.pull_msg(&plan).await else {
                break;
            };
            tracing::debug!("RE msg: {:?}", &msg);
            // msg_hook sees every Msg before dispatch (bluesky run_engine.py:1645).
            if let Some(h) = self.msg_hook.lock().unwrap().clone() {
                h(&msg);
            }
            match self.handle(msg).await {
                Ok(Some(uid)) => run_uids.push(uid),
                Ok(None) => {}
                Err(e) => {
                    // What the plan sees at its `yield`. A handler cancelled while
                    // an interrupt request is outstanding is that request arriving
                    // (`stop`/`abort`/`halt` cancel the token to unpark an in-flight
                    // `Sleep`/`Wait`/`WaitFor`), not a plan failure; one cancelled
                    // by a pause request is the pause landing mid-message —
                    // bluesky's `_run` bounces to the top of its loop on a
                    // `CancelledError` in the "pausing" state (run_engine.py:
                    // 1710-1716) — and the pause gate takes it from here.
                    // `Cancelled` with no request outstanding (a device cancelling
                    // its own status) stays a failure. `Interrupted` is a
                    // `Msg::Raise` of an interrupt a wrapper finished unwinding: it
                    // keeps propagating outward.
                    let thrown = match &e {
                        BsrsError::Cancelled => match self.take_interrupt(&mut thrown_seq) {
                            None if self.pause_pending(pause_seen) => continue,
                            None => Thrown::Error(e.to_string()),
                            Some(InterruptRequest::Halt) => {
                                return self.end_interrupted(run_uids, "halt").await;
                            }
                            Some(InterruptRequest::Throw(kind)) => {
                                Thrown::Interrupt(kind, self.stop_reason())
                            }
                        },
                        BsrsError::Interrupted(kind) => {
                            Thrown::Interrupt(*kind, self.stop_reason())
                        }
                        _ => Thrown::Error(e.to_string()),
                    };
                    if self.throw_into_plan(thrown.clone()).await {
                        tracing::debug!("thrown into the plan: {thrown:?}");
                        continue;
                    }
                    // Nothing in the plan catches it: the run ends the way the
                    // value leaving bluesky's plan stack ends `_run` — an
                    // interrupt with its exit status, an error as `fail`.
                    return match thrown {
                        Thrown::Interrupt(kind, _) => {
                            self.end_interrupted(run_uids, kind.exit_status()).await
                        }
                        Thrown::Error(text) => {
                            tracing::error!("plan error: {text}");
                            self.drain_and_close("fail", Some(text)).await?;
                            // Move the error itself into the result so callers can
                            // match on its variant (bluesky's
                            // `RunEngineResult.exception`).
                            Ok(self.build_result(run_uids, "fail".into(), Some(e)))
                        }
                    };
                }
            }
            // Hand the engine's result back to a `Respond`-issuing plan so it can
            // branch on it inline (e.g. `collect_while_completing` looping on the
            // `Wait` done-flag). Reached only after a successful `handle`; on
            // error the sender is dropped and the plan observes `Err(RecvError)`.
            // A dropped receiver (plan already advanced) is equally fine to drop.
            if let Some(tx) = responder {
                let _ = tx.send(self.take_msg_result());
            }
        }

        // The plan stream ended — bluesky's `StopIteration`, "success" even
        // after an interrupt a wrapper chose to swallow: close any open run.
        let still_open = self.state.lock().await.any_run_open();
        let exit_status = if still_open {
            self.drain_and_close("success", None).await?;
            "success"
        } else if run_uids.is_empty() {
            "no-run"
        } else {
            "success"
        };
        Ok(self.build_result(run_uids, exit_status.into(), None))
    }

    /// End the run on a `stop`/`abort`/`halt` nothing in the plan caught: close
    /// every open run with the interrupt's exit status and the caller's reason
    /// (`abort(reason)`, threaded onto the RunStop as bluesky's `_reason`,
    /// run_engine.py:1792).
    async fn end_interrupted(&self, run_uids: Vec<String>, exit_status: &str) -> Result<RunResult> {
        self.drain_and_close(exit_status, self.stop_reason())
            .await?;
        Ok(self.build_result(run_uids, exit_status.to_string(), None))
    }

    /// Consume the interrupt request outstanding since `thrown_seq`, if any,
    /// and renew the cancel token: the request cancelled the token to unpark
    /// the handler it interrupted, and the cleanup messages the plan issues in
    /// response must not inherit that cancellation. A later request cancels the
    /// new token and counts again, so a second `abort` during cleanup is a
    /// second interrupt — as bluesky throws a second `RequestAbort` into the
    /// `finally` block.
    fn take_interrupt(&self, thrown_seq: &mut u64) -> Option<InterruptRequest> {
        let mut token = self.ctl.cancel.lock().unwrap();
        let seq = self.interrupt_seq.load(Ordering::SeqCst);
        if seq == *thrown_seq {
            return None;
        }
        *thrown_seq = seq;
        *token = CancellationToken::new();
        Some(if self.is_halting.load(Ordering::SeqCst) {
            InterruptRequest::Halt
        } else if self.is_stopping.load(Ordering::SeqCst) {
            InterruptRequest::Throw(Interrupt::Stop)
        } else {
            InterruptRequest::Throw(Interrupt::Abort)
        })
    }

    /// Is an interrupt request outstanding that the loop has not acted on?
    fn interrupt_pending(&self, thrown_seq: u64) -> bool {
        self.interrupt_seq.load(Ordering::SeqCst) != thrown_seq
    }

    /// Is a pause request outstanding that the pause gate has not acted on?
    fn pause_pending(&self, pause_seen: u64) -> bool {
        self.ctl.pause_seq.load(Ordering::SeqCst) != pause_seen
    }

    /// Consume the pause requests outstanding since `pause_seen` and renew the
    /// cancel token they cancelled, so the messages handled next — a
    /// suspender's `pre_plan`/`post_plan`, the replay after resume — do not
    /// inherit the cancellation that unparked the interrupted handler. A pause
    /// requested after this cancels the new token and counts again.
    fn ack_pause(&self, pause_seen: &mut u64) {
        let mut token = self.ctl.cancel.lock().unwrap();
        let seq = self.ctl.pause_seq.load(Ordering::SeqCst);
        if seq != *pause_seen {
            *pause_seen = seq;
            *token = CancellationToken::new();
        }
    }

    /// Throw `thrown` into the plan: hand it to the innermost contingency
    /// region, whose `contingency_wrapper` reads it right after the message it
    /// forwarded and runs its `except`/`finally` plans. A rewind replay in
    /// progress dies with it — bluesky throws into the replay generator first,
    /// which has no handler (run_engine.py:1586-1600). Returns `false` when no
    /// region is active: the plan has nothing to catch it and the caller ends
    /// the run.
    async fn throw_into_plan(&self, thrown: Thrown) -> bool {
        let mut state = self.state.lock().await;
        let Some(sink) = state.contingency_stack.last() else {
            return false;
        };
        *sink.lock().unwrap() = Some(thrown);
        state.replay_queue.clear();
        true
    }

    /// Park the run loop on a pause request. Entering: `on_pause_enter` stops
    /// the touched movables/flyers and quiesces Pausables, then the suspender's
    /// `pre_plan` runs. Woken by `resume`: `on_resume` arms the rewind and
    /// resumes Pausables, then the `post_plan` runs. Woken by a
    /// `stop`/`abort`/`halt` request instead: nothing is rewound or resumed —
    /// the caller throws the request into the plan — and only the monitors
    /// suspended on pause are restored, as bluesky's `_run` restores them after
    /// its permit wait whatever woke it (run_engine.py:1536-1538) while
    /// `_rewind` and `Pausable.resume` belong to `resume()` alone
    /// (run_engine.py:994-1016).
    ///
    /// The gate is entered for every request counted since the last pass, not
    /// for the `is_paused` flag alone: the request may have unparked a handler
    /// mid-message, and only the rewind on the way out replays that message —
    /// a `resume` that already cleared the flag must still pass through here.
    async fn pause_gate(&self, thrown_seq: u64, pause_seen: &mut u64) {
        while self.pause_pending(*pause_seen) {
            // Arm the resume notification BEFORE the (possibly slow)
            // pause-enter + pre_plan work. `permit` is a bare `Notify` whose
            // `notify_waiters` drops the wakeup if no waiter is registered yet,
            // so a suspend future that resolves and calls `resume` mid-pre_plan
            // would otherwise be lost and the engine would hang. `enable()`
            // registers the waiter up front; the `is_paused` re-check below
            // closes the tiny window between the loop condition and `enable()`.
            let resumed = self.ctl.permit.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();

            // Take the request and renew the token it cancelled before any
            // message is handled on its behalf (the stop walk's devices, the
            // pre_plan).
            self.ack_pause(pause_seen);
            // The suspension behind the request, if it was one: its
            // justification is what the interruptions stream records — bluesky's
            // `_start_suspender` records `justification or "suspended"`
            // (run_engine.py:1263) where `_request_pause_coro` records "pause" —
            // and its plans bracket the wait.
            let suspension = self.ctl.pending_suspend.lock().unwrap().take();
            let interruption = suspension
                .as_ref()
                .map_or("pause", |s| s.justification.as_str());
            self.on_pause_enter(interruption).await;
            // pre_plan: run after the motor-stop / Pausable walk and before the
            // suspend wait, driven through the same handlers as the main plan.
            if let Some(pre) = suspension.as_ref().and_then(|s| s.pre_plan.clone()) {
                self.run_injected_plan(pre()).await;
            }
            // Wait for resume — unless it already fired during pause-enter or
            // pre_plan (captured by the armed `resumed`, or observed here as a
            // cleared `is_paused` when the notify landed before `enable()`).
            if self.ctl.is_paused.load(Ordering::SeqCst) {
                resumed.await;
            }
            // A pause requested while already parked is absorbed here rather
            // than re-entering the gate after this resume: bluesky rejects
            // `request_pause` in the paused state (run_engine.py:840-841).
            self.ack_pause(pause_seen);
            if self.interrupt_pending(thrown_seq) {
                self.restore_monitors().await;
                return;
            }
            self.on_resume().await;
            // post_plan: run after Pausable resume + monitor restore and before
            // the rewind replay drains, mirroring bluesky's post_plan.
            if let Some(post) = suspension.as_ref().and_then(|s| s.post_plan.clone()) {
                self.run_injected_plan(post()).await;
            }
        }
    }

    /// The next message to handle: the rewind replay first, then the plan
    /// stream (caching what a later rewind may replay). `None` once the plan
    /// stream has ended.
    async fn pull_msg(&self, plan: &Mutex<Plan>) -> Option<(Msg, PlanResponder)> {
        {
            let mut state = self.state.lock().await;
            if let Some(m) = state.replay_queue.pop_front() {
                return Some((m, None));
            }
        }
        let item = {
            let mut p = plan.lock().await;
            p.next().await
        };
        let m = match item? {
            PlanItem::Bare(m) => m,
            // A `Respond` item carries a oneshot the engine fulfills after
            // handling. It is never cached for rewind: the sender can't be
            // cloned, and a resumed replay would have no channel to answer.
            PlanItem::Respond(m, tx) => return Some((m, Some(tx))),
        };
        {
            let mut state = self.state.lock().await;
            if state.rewindable && m.is_cacheable() {
                state.msg_cache.push_back(m.clone());
            }
        }
        Some((m, None))
    }

    /// Drive an injected suspender plan (`pre_plan` / `post_plan`) through the
    /// engine handlers, exactly as the main plan is driven — so its `Set`/`Wait`
    /// move real devices and any documents are emitted — but WITHOUT rewind
    /// caching (these messages are not part of the rewindable main-plan region,
    /// so a later resume must not replay them) and WITHOUT re-entering the pause
    /// gate (the caller is already inside it). A failing message is logged and
    /// ends the injection; it does not abort the run. Mirrors bluesky running
    /// pre/post_plan messages through the same `_run` loop.
    async fn run_injected_plan(&self, mut plan: Plan) {
        while !self.is_aborting.load(Ordering::SeqCst) {
            // Injected plans may also issue `Respond` items; carry the sender so
            // an inline-result plan (e.g. a suspender that waits with a done-flag)
            // works here too, mirroring the main loop's fulfillment.
            let (m, responder) = match plan.next().await {
                Some(PlanItem::Bare(m)) => (m, None),
                Some(PlanItem::Respond(m, tx)) => (m, Some(tx)),
                None => break,
            };
            if let Err(e) = self.handle(m).await {
                tracing::warn!("suspender injected plan message failed: {e}");
                break;
            }
            if let Some(tx) = responder {
                let _ = tx.send(self.take_msg_result());
            }
        }
    }

    async fn on_pause_enter(&self, interruption: &str) {
        // Snapshot touched objects under the lock, then drop the lock
        // before awaiting their stop / pause hooks so a slow backend
        // can't hold the engine state locked.
        let (movables, flyables, pausables) = {
            let mut state = self.state.lock().await;
            // Suspend monitors on every open run — drop the live pumps
            // (releasing the backend subscriptions) but keep the `monitored`
            // registrations, so `on_resume` re-installs them. Mirrors bluesky
            // `suspend_monitors` (clear_sub but keep `_monitor_params`,
            // bundlers.py:661-663).
            for slot in state.runs.values_mut() {
                slot.monitor_tasks.clear();
            }
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
        // Bluesky parity: every pause entry is recorded — "pause" for a
        // pause, the justification for a suspension. No-op when recording is
        // off or no run is open.
        self.record_interruption(interruption).await;
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
                // Roll back EVERY open run — a pause mid-event in any run leaves
                // a bundle the replayed `Create` would collide with.
                for slot in state.runs.values_mut() {
                    slot.bundler.rewind();
                    // The cancelled bundle's external-asset reads are replayed
                    // from the checkpoint (which precedes the bundle's `Create`),
                    // so drop the stale tracking to match the bundler rewind.
                    slot.bundle_asset_objs.clear();
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
        self.restore_monitors().await;
        self.record_interruption("resume").await;
    }

    /// Re-install the monitors suspended on pause, per run. Mirrors bluesky
    /// `restore_monitors` (re-subscribe from the kept `_monitor_params`,
    /// bundlers.py:665-666). `start_monitor` is idempotent on the descriptor
    /// (the stream was already declared), so this re-subscribes the device
    /// and respawns the pump without re-emitting the Descriptor.
    async fn restore_monitors(&self) {
        let specs: Vec<(Option<String>, MonitorSpec)> = {
            let state = self.state.lock().await;
            state
                .runs
                .iter()
                .flat_map(|(run, slot)| {
                    slot.monitored
                        .values()
                        .cloned()
                        .map(move |spec| (run.clone(), spec))
                })
                .collect()
        };
        for (run, spec) in specs {
            if let Err(e) = self
                .start_monitor(&run, spec.stream, spec.obj.clone())
                .await
            {
                tracing::warn!("restore monitor failed for {}: {e}", spec.obj.name());
            }
        }
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
        // Snapshot every open run's sequence counters as the rewind rollback
        // target — bluesky iterates `_run_bundlers.values()` (run_engine.py:2465).
        for slot in state.runs.values_mut() {
            slot.bundler.reset_checkpoint_state();
        }
    }

    async fn handle(&self, msg: Msg) -> Result<Option<String>> {
        // Extract the run key once (bluesky's `msg.run`): `Msg::InRun { run, .. }`
        // routes its inner bundler verb to `Some(run)`; every other message
        // targets the default run `None`. `InRun` is not nestable — a wrapped
        // `InRun` is a plan bug, rejected here rather than silently unwrapped.
        let (run_key, msg): (Option<String>, Msg) = match msg {
            Msg::InRun { run, inner } => {
                if matches!(*inner, Msg::InRun { .. }) {
                    return Err(BsrsError::Plan("InRun cannot be nested".into()));
                }
                (Some(run), *inner)
            }
            other => (None, other),
        };
        match msg {
            Msg::InRun { .. } => unreachable!("InRun unwrapped above"),
            Msg::OpenRun(meta) => {
                let uid = self.open_run(&run_key, meta).await?;
                *self.last_msg_result.lock().unwrap() = MsgResult::OpenRun { uid: uid.clone() };
                return Ok(Some(uid));
            }
            Msg::CloseRun {
                exit_status,
                reason,
            } => {
                // bluesky's _close_run raises IllegalMessageSequence when the
                // keyed run is not open (run_engine.py:1902-1905).
                // close_run_if_open is intentionally lenient — it is the internal
                // run-end cleanup path (run_loop) — so the strict check belongs on
                // the explicit message path here.
                if !self.state.lock().await.run_open(&run_key) {
                    return Err(BsrsError::Plan("CloseRun without an open run".into()));
                }
                self.close_run_if_open(&run_key, &exit_status, reason)
                    .await?;
                *self.last_msg_result.lock().unwrap() = MsgResult::CloseRun {
                    exit_status: exit_status.clone(),
                };
            }
            Msg::Create { stream_name } => {
                let mut state = self.state.lock().await;
                let slot = state
                    .run_mut(&run_key)
                    .ok_or_else(|| BsrsError::Plan("Create with no open run".into()))?;
                slot.bundler.create(stream_name)?;
                // A fresh bundle starts with no external-asset reads (bluesky
                // resets `_asset_docs_cache` on create, bundlers.py:388).
                slot.bundle_asset_objs.clear();
            }
            Msg::Save => {
                // Compose the bundle's Descriptor (first event only) + Event,
                // and capture the stream name + the asset-writing read objects
                // to drain — all before `save` consumes the open bundle.
                let (docs, stream_name, asset_objs) = {
                    let mut state = self.state.lock().await;
                    let slot = state
                        .run_mut(&run_key)
                        .ok_or_else(|| BsrsError::Plan("Save with no open run".into()))?;
                    let stream_name = slot.bundler.open_stream_name();
                    let docs = slot.bundler.save()?;
                    let asset_objs = std::mem::take(&mut slot.bundle_asset_objs);
                    (docs, stream_name, asset_objs)
                };
                // Drain external-asset docs (`StreamResource`/`StreamDatum`)
                // from the read objects that write them, stamped with this
                // stream's just-composed EventDescriptor UID — the step-mode
                // analogue of the Collect path, and a port of bluesky
                // `_pack_external_assets` on save (bundlers.py:610).
                let descriptor_uid = {
                    let state = self.state.lock().await;
                    match (state.bundler(&run_key), stream_name.as_ref()) {
                        (Some(b), Some(s)) => b.descriptor_uid(s),
                        _ => None,
                    }
                };
                let mut assets = Vec::new();
                if let Some(uid) = &descriptor_uid {
                    for obj in &asset_objs {
                        assets.extend(obj.collect_asset_docs_dyn(uid).await?);
                    }
                }
                if !assets.is_empty() {
                    // Validate + stamp the drain (run_start, resource/datum
                    // cross-checks) and fill the datums' seq_nums from the
                    // Event this save just composed: the datums accompany
                    // that event, so they cover [event_seq, event_seq +
                    // width) — bluesky packs with the stream counter at the
                    // same point (bundlers.py:830). The event itself advanced
                    // the counter; no extra advance here.
                    let event_seq = docs.iter().find_map(|d| match d {
                        Document::Event(ev) => Some(ev.seq_num),
                        _ => None,
                    });
                    if event_seq.is_none()
                        && assets.iter().any(|a| matches!(a, Document::StreamDatum(_)))
                    {
                        return Err(BsrsError::Plan(
                            "Save drained StreamDatum documents but composed no \
                             Event to anchor their seq_nums"
                                .into(),
                        ));
                    }
                    let mut state = self.state.lock().await;
                    let bundler = state.bundler_mut(&run_key).ok_or_else(|| {
                        BsrsError::Plan("Save lost open run before stream docs".into())
                    })?;
                    let external_keys = stream_name
                        .as_deref()
                        .and_then(|s| bundler.compose().external_data_keys(s))
                        .unwrap_or_default();
                    let run_start = bundler.start_uid.clone();
                    pack_external_assets(
                        &mut assets,
                        &run_start,
                        event_seq.unwrap_or(0),
                        &external_keys,
                        bundler.stream_resource_data_keys_mut(),
                    )?;
                }
                // Emit order: Descriptor(s) → StreamResource/StreamDatum →
                // Event(s), so a consumer sees the descriptor before the stream
                // docs that reference it, and the stream docs before the event
                // they accompany.
                for d in &docs {
                    if matches!(d, Document::Descriptor(_)) {
                        self.broadcast(d).await?;
                    }
                }
                for a in &assets {
                    self.broadcast(a).await?;
                }
                for d in &docs {
                    if !matches!(d, Document::Descriptor(_)) {
                        self.broadcast(d).await?;
                    }
                }
            }
            Msg::Drop => {
                let mut state = self.state.lock().await;
                let slot = state
                    .run_mut(&run_key)
                    .ok_or_else(|| BsrsError::Plan("Drop with no open run".into()))?;
                slot.bundler.drop_bundle()?;
                // The discarded bundle's external-asset reads go with it.
                slot.bundle_asset_objs.clear();
            }
            Msg::DeclareStream {
                stream_name,
                data_keys,
            } => {
                // `Msg::DeclareStream` carries raw data keys, not objects
                // (deviation from bluesky, whose declare_stream takes the
                // collect objects), so there is nothing to read configuration
                // or hints from; `object_keys` comes from the keys' own
                // `object_name` annotations. The object-driven paths
                // (Read/save, Collect, Monitor) fill everything.
                let descriptor = {
                    let mut state = self.state.lock().await;
                    state
                        .bundler_mut(&run_key)
                        .ok_or_else(|| BsrsError::Plan("DeclareStream with no open run".into()))?
                        .declare_stream(stream_name, StreamObject::from_data_keys(data_keys))
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
                    state.run_open(&run_key)
                };
                if bundler_present {
                    // describe() only matters for the bundle's descriptor, so
                    // compute it (awaiting before re-locking) only when bundling.
                    let data_keys = obj.describe_dyn().await?;
                    // The object's configuration goes into the same descriptor,
                    // keyed by object name — empty for non-configurables
                    // (bluesky `_prepare_stream`, bundlers.py:286-290).
                    let config = self
                        .ensure_object_configuration(&run_key, obj.name(), obj.as_configurable())
                        .await?;
                    let read_obj = StreamObject {
                        object: Some(obj.name().to_string()),
                        data_keys,
                        hint_fields: obj.hint_fields(),
                        configuration: config,
                    };
                    let tracks_assets = obj.writes_external_assets();
                    let mut state = self.state.lock().await;
                    if let Some(slot) = state.run_mut(&run_key) {
                        slot.bundler.add_read(read_obj, readings)?;
                        // Track asset-writing readables so the paired `Save`
                        // drains their `StreamResource`/`StreamDatum`, stamped
                        // with the bundle's descriptor (bluesky
                        // `maybe_collect_asset_docs`, bundlers.py:444).
                        if tracks_assets {
                            slot.bundle_asset_objs.push(obj.clone());
                        }
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
                    if !state.run_open(&run_key) {
                        return Err(BsrsError::Plan("Kickoff sent but no run is open".into()));
                    }
                    state
                        .flyable_objs_touched
                        .insert(obj.name().to_string(), obj.clone());
                    // Record the flyer's collectable view (if any) into THIS run so
                    // a finalize-time backstop can drain it should the run abort
                    // before the plan issues its own `Msg::Collect`. bluesky adds
                    // `msg.obj` to the keyed run's `_uncollected` (bundlers.py:707).
                    if let Some(coll) = obj.clone().as_collectable() {
                        if let Some(slot) = state.run_mut(&run_key) {
                            slot.uncollected.insert(coll.name().to_string(), coll);
                        }
                    }
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
                self.collect_object(&run_key, obj, stream_name).await?;
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
                    if !state.run_open(&run_key) {
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
                    if state
                        .run(&run_key)
                        .is_some_and(|s| s.monitored.contains_key(obj.name()))
                    {
                        return Err(BsrsError::Plan(format!(
                            "A 'monitor' message was sent for {} which is already monitored",
                            obj.name()
                        )));
                    }
                }
                let stream = name.unwrap_or_else(default_monitor_stream_name);
                self.start_monitor(&run_key, stream.clone(), obj.clone())
                    .await?;
                let mut state = self.state.lock().await;
                // Record the registration so the monitor survives a pause: the
                // pump (monitor_tasks) is dropped on pause, this spec is not, and
                // resume re-installs the pump from it. bluesky `_monitor_params`.
                if let Some(slot) = state.run_mut(&run_key) {
                    slot.monitored
                        .insert(obj.name().to_string(), MonitorSpec { obj, stream });
                }
                Self::reset_checkpoint_state(&mut state);
            }
            Msg::Unmonitor(obj) => {
                // monitor_tasks is keyed by the monitored object's name (set in
                // start_monitor), so remove the entry whose key == obj.name().
                // The pump is stopped gracefully (MonitorTask::stop) outside the
                // state lock: it emits an update it has not consumed yet, then
                // drops the Subscription.
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
                if !state
                    .run(&run_key)
                    .is_some_and(|s| s.monitored.contains_key(obj.name()))
                {
                    return Err(BsrsError::Plan(format!(
                        "Cannot 'unmonitor' {}; it is not being monitored",
                        obj.name()
                    )));
                }
                let task = state.run_mut(&run_key).and_then(|slot| {
                    // Drop the registration too, so a later resume does not
                    // re-install a monitor the plan explicitly removed.
                    slot.monitored.remove(obj.name());
                    slot.monitor_tasks.remove(obj.name())
                });
                Self::reset_checkpoint_state(&mut state);
                drop(state);
                if let Some(task) = task {
                    task.stop().await;
                }
            }
            Msg::Wait {
                group,
                error_on_timeout,
                timeout,
            } => {
                // `done` is false only on a move-on timeout with members still
                // pending; plans that `Respond` on this loop until it's true.
                let done = self.wait_group(&group, error_on_timeout, timeout).await?;
                *self.last_msg_result.lock().unwrap() = MsgResult::WaitComplete { done };
            }
            Msg::Sleep(d) => {
                let token = self.ctl.cancel.lock().unwrap().clone();
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
                if state.any_bundling() {
                    return Err(BsrsError::Plan(
                        "Cannot 'checkpoint' after 'create' and before 'save'".into(),
                    ));
                }
                // Clear cache up to this point — the rewindable region restarts
                // across every open run.
                Self::reset_checkpoint_state(&mut state);
                state.rewindable = true;
                // Crash-audit anchor: the default run's UID if open, else any
                // open run's. (The snapshot schema carries one UID; multi-run
                // per-run audit is a follow-up.)
                let run_uid = state
                    .bundler(&None)
                    .or_else(|| state.runs.values().next().map(|s| &s.bundler))
                    .map(|b| b.start_uid.clone());
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
                    self.ctl.pause();
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
                for slot in state.runs.values_mut() {
                    slot.bundler.clear_checkpoint();
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
                // The plan-side Msg carries `Arc<dyn Any>` wrapping an
                // `Arc<dyn Suspender>`.
                let typed: Arc<dyn Suspender> = suspender
                    .downcast::<Arc<dyn Suspender>>()
                    .map(|a| (*a).clone())
                    .map_err(|_| {
                        BsrsError::Plan(
                            "InstallSuspender payload was not Arc<dyn Suspender>".into(),
                        )
                    })?;
                self.register_suspender(id, typed).await;
            }
            Msg::RemoveSuspender { id } => {
                self.remove_suspender(id).await;
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
                    if state.bundler(&run_key).is_some_and(|b| b.is_bundling()) {
                        return Err(BsrsError::Plan(
                            "Cannot configure after 'create' but before 'save'".into(),
                        ));
                    }
                }
                obj.configure_dyn(args).await?;
                // Refresh the run's cached configuration (so streams declared
                // after this point carry the new values, bluesky
                // cache_read_config, bundlers.py:1209) AND re-emit the
                // descriptors of already-declared streams that include this
                // object, each a new generation carrying the fresh config with
                // the sequence counter unbroken — bluesky's configure-time
                // invalidation (bundlers.py:1213-1218). Without the re-emit,
                // events after the configure would keep referencing the old
                // descriptor and its stale configuration.
                let run_open = { self.state.lock().await.run_open(&run_key) };
                if run_open {
                    let config = read_object_configuration(obj.as_ref()).await?;
                    // Compose the new generations WITHOUT installing them, then
                    // broadcast, then install. A monitor pump composes its events
                    // against the stream's *current* descriptor on a separate
                    // task; installing only after the broadcast guarantees the new
                    // descriptor is on the wire before the pump can stamp it onto
                    // an event (descriptor-before-event holds by construction, not
                    // by a runtime lock).
                    let new_descriptors = {
                        let mut state = self.state.lock().await;
                        match state.bundler_mut(&run_key) {
                            Some(b) => {
                                b.cache_configuration(obj.name().to_string(), config.clone());
                                b.compose_reconfigure(obj.name(), config)
                            }
                            None => Vec::new(),
                        }
                    };
                    for d in &new_descriptors {
                        self.broadcast(&Document::Descriptor(d.clone())).await?;
                    }
                    if !new_descriptors.is_empty() {
                        if let Some(b) = self.state.lock().await.bundler_mut(&run_key) {
                            b.install_reconfigured(&new_descriptors);
                        }
                    }
                }
            }
            Msg::Prepare { obj, value, group } => {
                let status = obj.prepare_dyn(value).await;
                if let Some(g) = group.clone() {
                    *self.last_msg_result.lock().unwrap() = MsgResult::Status { group: g };
                }
                self.handle_status(status, group).await?;
            }
            Msg::WaitFor { factories, timeout } => {
                let token = self.ctl.cancel.lock().unwrap().clone();
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
            Msg::Raise(thrown) => {
                return Err(thrown.into());
            }
            Msg::PushContingency(sink) => {
                // Clear any stale error before arming, so this region starts
                // with an empty sink regardless of what the Arc last held.
                *sink.lock().unwrap() = None;
                self.state.lock().await.contingency_stack.push(sink);
            }
            Msg::PopContingency => {
                self.state.lock().await.contingency_stack.pop();
            }
        }
        Ok(None)
    }

    /// The run-cached configuration for object `name`, reading it through
    /// `configurable` on first use (empty when the object is not
    /// configurable). Cache lives on the `RunBundler` (run-scoped); the
    /// bluesky analogue is `ensure_cached` filling the config caches once
    /// per object (bundlers.py:93-102). Returns the configuration even if
    /// the run closed mid-read (it just isn't cached then).
    async fn ensure_object_configuration(
        &self,
        run_key: &Option<String>,
        name: &str,
        configurable: Option<&dyn crate::core::msg::ConfigurableObj>,
    ) -> Result<crate::event_model::Configuration> {
        let cached = {
            let state = self.state.lock().await;
            state
                .bundler(run_key)
                .and_then(|b| b.cached_configuration(name))
        };
        if let Some(c) = cached {
            return Ok(c);
        }
        let config = match configurable {
            Some(c) => read_object_configuration(c).await?,
            None => crate::event_model::Configuration::default(),
        };
        let mut state = self.state.lock().await;
        if let Some(b) = state.bundler_mut(run_key) {
            b.cache_configuration(name.to_string(), config.clone());
        }
        Ok(config)
    }

    async fn start_monitor(
        &self,
        run_key: &Option<String>,
        stream: String,
        obj: Arc<dyn crate::core::msg::MonitorableObj>,
    ) -> Result<()> {
        // Step 1: declare the descriptor for this stream from the device's
        // own describe_dyn (MonitorableObj : ReadableObj), with the device's
        // configuration — bluesky `_monitor` runs `ensure_cached(obj)` +
        // `_prepare_stream(name, {obj: ...})` (bundlers.py:473-475).
        let data_keys = obj.describe_dyn().await?;
        let config = self
            .ensure_object_configuration(run_key, obj.name(), obj.as_configurable())
            .await?;
        let (descriptor, bundle) = {
            let mut state = self.state.lock().await;
            let bundler = state
                .bundler_mut(run_key)
                .ok_or_else(|| BsrsError::Plan("Monitor with no open run".into()))?;
            let descriptor = if bundler.descriptor_uid(&stream).is_some() {
                None
            } else {
                let monitored = StreamObject {
                    object: Some(obj.name().to_string()),
                    data_keys: data_keys.clone(),
                    hint_fields: obj.hint_fields(),
                    configuration: config,
                };
                Some(bundler.declare_stream(stream.clone(), vec![monitored]))
            };
            (descriptor, bundler.bundle())
        };
        if let Some(d) = descriptor {
            self.broadcast(&Document::Descriptor(d)).await?;
        }

        // Step 2: subscribe + spawn a pump that emits one Event per rx tick.
        let mut sub = obj.subscribe_dyn().await?;
        // The subscription names the data key of its readings (ophyd-async
        // `subscribe_reading` delivers `{name: reading}`); the descriptor
        // declared above must carry it or the Events would not match.
        if !data_keys.contains_key(sub.key()) {
            return Err(BsrsError::Plan(format!(
                "Monitor: {} streams data key {:?}, which its describe() does not declare",
                obj.name(),
                sub.key()
            )));
        }
        let event_key = sub.key().to_string();
        let stream_for_task = stream.clone();
        let sinks = self.sinks.clone();
        let subs_arc = self.subscribers.clone();
        let stop = Arc::new(tokio::sync::Notify::new());
        let stop_for_task = stop.clone();

        let handle = tokio::spawn(async move {
            loop {
                let last = tokio::select! {
                    changed = sub.rx_mut().changed() => {
                        if changed.is_err() {
                            return;
                        }
                        false
                    }
                    _ = stop_for_task.notified() => {
                        // Emit the update this pump never consumed, if any,
                        // then exit.
                        match sub.rx_mut().has_changed() {
                            Ok(true) => true,
                            _ => return,
                        }
                    }
                };
                let reading = sub.rx_mut().borrow_and_update().clone();
                let mut data = HashMap::new();
                let mut timestamps = HashMap::new();
                data.insert(event_key.clone(), reading.value);
                timestamps.insert(event_key.clone(), reading.timestamp);
                if let Some(ev) = bundle.event(&stream_for_task, data, timestamps) {
                    let doc = Document::Event(ev);
                    for s in &sinks {
                        if let Err(e) = s.dispatch(&doc).await {
                            tracing::warn!("document sink failed for monitor event: {e}");
                        }
                    }
                    RunEngine::dispatch_subscribers(&subs_arc, &doc);
                }
                if last {
                    return;
                }
            }
        });
        // Key the pump by the monitored object's identity — that is what
        // Msg::Unmonitor(obj) carries — NOT by the descriptor stream name.
        // Keying by `stream` leaked the pump whenever a custom monitor name was
        // used: Unmonitor matched the key against obj.name() and never found it.
        // Stored on THIS run's slot so pause/close teardown is per-run.
        if let Some(slot) = self.state.lock().await.run_mut(run_key) {
            slot.monitor_tasks.insert(
                obj.name().to_string(),
                MonitorTask {
                    stop,
                    handle: Some(handle),
                },
            );
        }
        Ok(())
    }

    /// Allocate a fresh suspender id.
    pub fn next_suspender_id(&self) -> u64 {
        self.suspender_count.fetch_add(1, Ordering::SeqCst)
    }

    /// Emit an Event to the special `"interruptions"` stream of *every* open
    /// run that has one declared. No-op when recording is off; a run whose
    /// `OpenRun` happened *before* recording was turned on has no such stream
    /// (declared at OpenRun time only) and is skipped. bluesky records the
    /// interruption in each `_run_bundlers` value (run_engine.py:1585-1592).
    async fn record_interruption(&self, content: &str) {
        if !self.record_interruptions.load(Ordering::SeqCst) {
            return;
        }
        // One bundle handle per run whose interruptions stream is declared.
        let bundles = {
            let state = self.state.lock().await;
            state
                .runs
                .values()
                .filter(|slot| slot.bundler.descriptor_uid("interruptions").is_some())
                .map(|slot| slot.bundler.bundle())
                .collect::<Vec<_>>()
        };
        if bundles.is_empty() {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        for bundle in bundles {
            let mut data = HashMap::new();
            data.insert("interruption".to_string(), Value::String(content.into()));
            let mut timestamps = HashMap::new();
            timestamps.insert("interruption".to_string(), now);
            if let Some(ev) = bundle.event("interruptions", data, timestamps) {
                let _ = self.broadcast(&Document::Event(ev)).await;
            }
        }
    }

    /// Install `susp` (bluesky `RE.install_suspender`). Registered, it gates
    /// plan start while tripped (the ENG-12 gate in `run_async`), and its
    /// watcher suspends the run each time [`Suspender::trip`] resolves until
    /// [`Suspender::watch`] does, running the suspender's `pre_plan` after the
    /// touched motors stop and its `post_plan` before the rewind replays. The
    /// registration outlives the run, as bluesky's `_suspenders` set does; the
    /// returned id is what [`Self::remove_suspender`] takes, and removing the
    /// suspender releases a suspension it holds (bluesky's
    /// `SuspenderBase.remove` sets its event, `suspenders.py:74-85`).
    ///
    /// A trip while no run is in progress requests nothing — the plan-start
    /// gate covers a condition still bad when the next run starts — and one
    /// bad episode requests one suspension: the watcher re-arms only once
    /// `watch` has resolved, as bluesky requests only while no release event
    /// exists (`suspenders.py:128-140`). A second suspender tripping while the
    /// run is already suspended adds a hold the first release cannot lift, but
    /// its plans do not run and its justification is not recorded: the gate
    /// takes one suspension per pause.
    pub async fn install_suspender(&self, susp: Arc<dyn Suspender>) -> u64 {
        let id = self.next_suspender_id();
        self.register_suspender(id, susp).await;
        id
    }

    /// Registration shared by [`Self::install_suspender`] and the plan-side
    /// `Msg::InstallSuspender`, whose issuer allocated `id` itself.
    async fn register_suspender(&self, id: u64, susp: Arc<dyn Suspender>) {
        let released = CancellationToken::new();
        let watcher = {
            let susp = susp.clone();
            let ctl = self.ctl.clone();
            let released = released.clone();
            tokio::spawn(async move {
                loop {
                    susp.trip().await;
                    if ctl.is_running.load(Ordering::SeqCst) {
                        let clear = susp.watch();
                        let released = released.clone();
                        let fut: BoxFuture<'static, ()> = Box::pin(async move {
                            tokio::select! {
                                _ = clear => {}
                                _ = released.cancelled() => {}
                            }
                        });
                        ctl.request_suspend(
                            fut,
                            Suspension {
                                justification: susp.justification(),
                                pre_plan: susp.pre_plan(),
                                post_plan: susp.post_plan(),
                            },
                        );
                    }
                    susp.watch().await;
                }
            })
        };
        let registration = SuspenderHandle::new(id, susp, watcher, released);
        self.state.lock().await.suspenders.insert(id, registration);
    }

    /// Uninstall one suspender (bluesky `RE.remove_suspender`): its watcher is
    /// aborted and a suspension it holds is released.
    pub async fn remove_suspender(&self, id: u64) {
        self.state.lock().await.suspenders.remove(&id);
    }

    /// Uninstall every suspender (bluesky `RE.clear_suspenders`): each watcher
    /// is aborted and any suspension held is released. A no-op when none are
    /// installed.
    pub async fn clear_suspenders(&self) {
        self.state.lock().await.suspenders.clear();
    }

    async fn open_run(&self, run_key: &Option<String>, meta: RunMetadata) -> Result<String> {
        // Reject a second open_run *for the same run key* before any side
        // effect — bluesky's `run_key in self._run_bundlers` check precedes
        // scan_id resolution and document emission (run_engine.py:1849-1851).
        // Doing it here means a rejected re-open neither advances the scan_id
        // counter nor broadcasts a spurious RunStart. A *different* run key is
        // allowed to open concurrently — that is exactly the multi-run case.
        if self.state.lock().await.run_open(run_key) {
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
                                                         // plan_type: bluesky computes `type(self._plan).__name__`, which is
                                                         // "generator" for every generator-function plan. bsrs plans are lazy
                                                         // `Msg` streams — generators — so the faithful value is the constant
                                                         // "generator"; a plan or caller that wants a different plan_type sets
                                                         // it via the OpenRun `extra` or per-call md, both inserted after this
                                                         // and thus higher precedence.
            m.insert("plan_type".into(), Value::String("generator".into()));
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
                    crate::event_model::DataKey {
                        source: "RunEngine".into(),
                        dtype: crate::event_model::Dtype::String,
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
                // No object behind it: bluesky's interruptions descriptor is
                // composed from a bare data key (run_engine.py:1880).
                Some(
                    bundler
                        .declare_stream("interruptions".into(), StreamObject::from_data_keys(keys)),
                )
            } else {
                None
            };
            state.runs.insert(run_key.clone(), RunSlot::new(bundler));
            descriptor
        };
        if let Some(d) = interruptions_descriptor {
            self.broadcast(&Document::Descriptor(d)).await?;
        }
        Ok(uid)
    }

    async fn close_run_if_open(
        &self,
        run_key: &Option<String>,
        exit_status: &str,
        reason: Option<String>,
    ) -> Result<()> {
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
            // Remove THIS run's slot and compose its RunStop. Dropping the slot
            // tears down any monitors still active when the run closes — bluesky's
            // close_run clears each remaining `_monitor_params` subscription
            // (bundlers.py:246-248). A `Msg::Monitor` not explicitly `Unmonitor`'d
            // is unsubscribed as the slot's `monitor_tasks`/`monitored` drop with
            // it, not leaked into the next run where its pump would keep composing
            // Events against this now-closed bundle. `MonitorTask::drop` aborts the
            // pump and drops its Subscription. The slot's `uncollected` set is
            // already drained by the finalize `backstop_collect` that precedes this.
            state
                .runs
                .remove(run_key)
                .map(|slot| slot.bundler.compose().stop(exit_status, reason))
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
        // Sink errors are logged here — the fan-out is the single owner of
        // that policy (`DocumentSink::dispatch` contract: logged, not fatal
        // to the run) — so individual sinks propagate instead of catching.
        for s in &self.sinks {
            if let Err(e) = s.dispatch(doc).await {
                let name = crate::callbacks::document_name(doc);
                tracing::warn!("document sink failed for {name} doc: {e}");
            }
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
            None => {
                // The ungrouped form awaits inline, so it is parked exactly as
                // `wait_group` is and must be unparked the same way: a
                // pause/stop/abort cancels the token.
                let token = self.ctl.cancel.lock().unwrap().clone();
                let outcome = tokio::select! {
                    r = status => r,
                    _ = token.cancelled() => return Err(BsrsError::Cancelled),
                };
                match outcome {
                    Ok(()) => Ok(()),
                    Err(StatusError::Cancelled) => Err(BsrsError::Cancelled),
                    Err(StatusError::Timeout) => Err(BsrsError::Timeout(Duration::from_secs(0))),
                    Err(StatusError::Failed(s)) => Err(BsrsError::Backend(s)),
                }
            }
        }
    }

    /// Await a status group. Returns `Ok(true)` when every member completed,
    /// `Ok(false)` when a move-on timeout (`error_on_timeout=false`) elapsed with
    /// members still pending (they are restored to the group). Propagates `Err`
    /// on member failure, or on a hard timeout when `error_on_timeout` is true.
    async fn wait_group(
        &self,
        group: &str,
        error_on_timeout: bool,
        timeout: Option<Duration>,
    ) -> Result<bool> {
        let members = {
            let mut state = self.state.lock().await;
            state
                .groups
                .remove(group)
                .map(|g| g.members)
                .unwrap_or_default()
        };
        if members.is_empty() {
            return Ok(true);
        }
        // Await *clones* of the members so the originals survive a move-on
        // timeout and can be restored to the group below. A clone shares the
        // same status state, so awaiting it observes the same completion.
        let waited = members.clone();
        // A pause/stop/abort cancels the run's token to unpark this wait —
        // bluesky's `_request_pause_coro` cancels the `_run` task parked in
        // `_wait` (run_engine.py:856) — so `stop_on_pause` reaches a motor while
        // it is still moving. The members are not restored on cancellation: as
        // in bluesky, where the popped futures are simply lost, the rewind on
        // resume re-issues the messages that created them.
        let token = self.ctl.cancel.lock().unwrap().clone();
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
            let all = futures::future::try_join_all(waited);
            let joined = tokio::select! {
                r = all => r,
                _ = token.cancelled() => return Err(BsrsError::Cancelled),
            };
            joined.map(|_| ()).map_err(|e| match e {
                StatusError::Cancelled => BsrsError::Cancelled,
                StatusError::Timeout => BsrsError::Timeout(Duration::from_secs(0)),
                StatusError::Failed(s) => BsrsError::Backend(s),
            })
        };
        match timeout {
            Some(d) => match tokio::time::timeout(d, fut).await {
                Ok(r) => r.map(|_| true),
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
                        Ok(false)
                    }
                }
            },
            None => fut.await.map(|_| true),
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

    fn datum_for(uid: &str, resource: &str, indices: (u64, u64), seq_nums: (u64, u64)) -> Document {
        Document::StreamDatum(crate::event_model::StreamDatum {
            uid: uid.into(),
            stream_resource: resource.into(),
            descriptor: "desc".into(),
            indices: crate::event_model::StreamRange {
                start: indices.0,
                stop: indices.1,
            },
            seq_nums: crate::event_model::StreamRange {
                start: seq_nums.0,
                stop: seq_nums.1,
            },
        })
    }

    fn datum(uid: &str, indices: (u64, u64), seq_nums: (u64, u64)) -> Document {
        datum_for(uid, "res", indices, seq_nums)
    }

    fn resource(uid: &str, data_key: &str) -> Document {
        Document::StreamResource(crate::event_model::StreamResource {
            uid: uid.into(),
            data_key: data_key.into(),
            mimetype: "application/x-hdf5".into(),
            uri: "file://localhost/data/f.h5".into(),
            parameters: Default::default(),
            run_start: None,
        })
    }

    /// One registered resource for the datum-only tests: `res` → `img`, the
    /// descriptor's sole external key.
    fn seeded_registry() -> HashMap<String, String> {
        HashMap::from([("res".to_string(), "img".to_string())])
    }

    const EXTERNAL: [&str; 1] = ["img"];

    fn external() -> Vec<String> {
        EXTERNAL.iter().map(|s| s.to_string()).collect()
    }

    /// The engine fills `[next_seq, next_seq + width)` into every datum of a
    /// drain, each from its own indices span, reports the shared width, and
    /// stamps `run_start` on resources (writers emit `None`).
    #[test]
    fn pack_assets_fills_seq_nums_and_stamps_run_start() {
        let mut registry = HashMap::new();
        let mut docs = vec![
            resource("res", "img"),
            datum("a", (10, 13), (0, 0)),
            datum("b", (5, 8), (0, 0)),
            Document::Event(crate::event_model::Event {
                uid: "ev".into(),
                descriptor: "desc".into(),
                time: 0.0,
                seq_num: 7,
                data: Default::default(),
                timestamps: Default::default(),
                filled: Default::default(),
            }),
        ];
        let width =
            pack_external_assets(&mut docs, "run-1", 7, &external(), &mut registry).unwrap();
        assert_eq!(width, 3);
        let Document::StreamResource(r) = &docs[0] else {
            panic!("expected resource")
        };
        assert_eq!(r.run_start.as_deref(), Some("run-1"));
        assert_eq!(registry.get("res").map(String::as_str), Some("img"));
        for d in &docs[1..3] {
            let Document::StreamDatum(d) = d else {
                panic!("expected datum")
            };
            assert_eq!((d.seq_nums.start, d.seq_nums.stop), (7, 10));
        }
    }

    /// A writer that pre-fills seq_nums violates the engine's ownership of
    /// the sequence counter and is rejected, mirroring bluesky's
    /// `_pack_seq_nums_into_stream_datum` assertion (bundlers.py:830).
    #[test]
    fn pack_assets_rejects_prefilled_datum() {
        let mut docs = vec![datum("a", (0, 1), (1, 2))];
        let err = pack_external_assets(&mut docs, "run-1", 1, &external(), &mut seeded_registry())
            .unwrap_err();
        assert!(err.to_string().contains("pre-filled"), "got: {err}");
    }

    /// Two detectors in one stream must advance by the same number of
    /// indices per drain; a mismatch means their frames no longer line up
    /// with a common event sequence.
    #[test]
    fn pack_assets_rejects_mismatched_indices_width() {
        let mut docs = vec![datum("a", (0, 2), (0, 0)), datum("b", (0, 3), (0, 0))];
        let err = pack_external_assets(&mut docs, "run-1", 1, &external(), &mut seeded_registry())
            .unwrap_err();
        assert!(
            err.to_string().contains("same number of indices"),
            "got: {err}"
        );
    }

    /// A drain with no datums (resources only, or empty) is a no-op with
    /// width 0 — the Collect path must not advance the stream counter.
    #[test]
    fn pack_assets_empty_drain_reports_zero_width() {
        let mut docs: Vec<Document> = Vec::new();
        assert_eq!(
            pack_external_assets(&mut docs, "run-1", 5, &external(), &mut HashMap::new()).unwrap(),
            0
        );
    }

    /// The same `StreamResource` uid emitted twice in one run is a writer
    /// bug (bsrs writers emit each resource exactly once, on first drain).
    #[test]
    fn pack_assets_rejects_duplicate_resource_uid() {
        let mut registry = seeded_registry();
        let mut docs = vec![resource("res", "img")];
        let err =
            pack_external_assets(&mut docs, "run-1", 1, &external(), &mut registry).unwrap_err();
        assert!(err.to_string().contains("twice"), "got: {err}");
    }

    /// A resource whose data_key is not among the descriptor's `STREAM:`
    /// keys points at data no event will ever reference.
    #[test]
    fn pack_assets_rejects_resource_for_unknown_data_key() {
        let mut docs = vec![resource("res2", "not_in_descriptor")];
        let err = pack_external_assets(&mut docs, "run-1", 1, &external(), &mut HashMap::new())
            .unwrap_err();
        assert!(err.to_string().contains("STREAM:"), "got: {err}");
    }

    /// A datum referencing a resource uid never emitted this run cannot be
    /// linked to any dataset.
    #[test]
    fn pack_assets_rejects_datum_with_unknown_resource() {
        let mut docs = vec![datum_for("a", "ghost", (0, 1), (0, 0))];
        let err = pack_external_assets(&mut docs, "run-1", 1, &external(), &mut HashMap::new())
            .unwrap_err();
        assert!(err.to_string().contains("unknown"), "got: {err}");
    }

    /// If any datum arrived, every external key of the descriptor must have
    /// received one in the same drain — a writer silently dropping one of
    /// its datasets (e.g. an NDAttribute) desyncs the stream.
    #[test]
    fn pack_assets_rejects_incomplete_data_key_coverage() {
        let mut registry = seeded_registry();
        registry.insert("res_attr".to_string(), "temp".to_string());
        let ext = vec!["img".to_string(), "temp".to_string()];
        // Only `img` gets a datum; `temp` is missing.
        let mut docs = vec![datum("a", (0, 1), (0, 0))];
        let err = pack_external_assets(&mut docs, "run-1", 1, &ext, &mut registry).unwrap_err();
        assert!(
            err.to_string().contains("every external data key"),
            "got: {err}"
        );
    }

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

    /// A cancelled run token unparks a `wait` on a member that never completes:
    /// this is how a pause reaches `stop_on_pause` while the motor is still
    /// moving (bluesky's `_request_pause_coro` cancels the `_run` task inside
    /// `_wait`, run_engine.py:856).
    #[tokio::test]
    async fn wait_group_returns_cancelled_when_the_run_token_is_cancelled() {
        let re = RunEngine::new(Vec::new());
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
        let token = re.ctl.cancel.lock().unwrap().clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            token.cancel();
        });
        let result =
            tokio::time::timeout(Duration::from_millis(500), re.wait_group("g", true, None)).await;
        match result {
            Ok(Err(BsrsError::Cancelled)) => {}
            other => panic!("expected Cancelled once the token is cancelled, got {other:?}"),
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
            matches!(r, Ok(false)),
            "error_on_timeout=false must not fail the run on timeout and must \
             report not-done (Ok(false)) with members still pending, got {r:?}"
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
