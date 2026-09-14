# 03 — RunEngine

## Overview

The RunEngine consumes a `Plan` (a stream of `Msg`) and dispatches each message to the
right device method, while emitting `Document`s to subscribed sinks. It owns checkpoint
state, suspender registry, and the bundler that turns `read()` outputs into Events.

The reference implementation is bluesky `run_engine.py:1478-2510` (`_run` and the 26
`_<command>` handlers). bsrs follows that structure verbatim, with five differences
listed at the bottom of this doc.

## Two entry points

```rust
impl RunEngine {
    /// Async entry point — primary.
    pub async fn run_async(&self, plan: Plan) -> Result<RunResult>;

    /// Sync entry point — equivalent for ophyd-style scripts and the REPL.
    /// Internally calls `bsrs_runtime().block_on(self.run_async(plan))`.
    /// Must NOT be called from inside an async task.
    pub fn run_blocking(&self, plan: Plan) -> Result<RunResult>;
}
```

Plans are written the same way for both entry points — see [`04-devices.md`](04-devices.md)
for the dual API surface.

## `Msg` representation

A typed enum — closed for the 26 core commands, plus a `Custom` escape hatch:

```rust
pub enum Msg {
    OpenRun(RunMetadata),
    CloseRun { exit_status: ExitStatus, reason: String },
    Create   { stream_name: String },
    Save,
    Drop,
    DeclareStream { stream_name: String },

    Read   (Arc<dyn AsyncReadable>),
    Set    { obj: Arc<dyn AsyncMovable<f64>>, value: f64, group: Option<GroupId> },
    Trigger{ obj: Arc<dyn Triggerable>, group: Option<GroupId> },
    Locate (Arc<dyn Locatable<f64>>),
    Configure { obj: Arc<dyn AsyncConfigurable>, args: ConfigureArgs },

    Prepare  { obj: Arc<dyn Preparable>, value: PrepareValue, group: Option<GroupId> },
    Kickoff  { obj: Arc<dyn Flyable>,    group: Option<GroupId> },
    Complete { obj: Arc<dyn Flyable>,    group: Option<GroupId> },
    Collect  { obj: Arc<dyn Collectable>, stream_name: Option<String> },

    Stage   (Arc<dyn Stageable>),
    Unstage (Arc<dyn Stageable>),
    Monitor   { obj: Arc<dyn Subscribable<f64>>, name: Option<String> },
    Unmonitor (Arc<dyn Subscribable<f64>>),

    Wait    { group: GroupId, error_on_timeout: bool, timeout: Option<Duration> },
    WaitFor (Vec<BoxFuture<'static, Result<()>>>),

    Sleep      (Duration),
    Pause      { defer: bool },
    Resume,
    Checkpoint,
    ClearCheckpoint,
    Rewindable (Option<bool>),

    InstallSuspender (Arc<dyn Suspender>),
    RemoveSuspender  (SuspenderId),

    Custom { name: &'static str, payload: Box<dyn Any + Send> },
}
```

Closed enum gives compile-time exhaustiveness in the dispatch table; the `Custom` arm
preserves bluesky's `register_command` extensibility for niche use cases.

## Message loop skeleton

```rust
async fn run_loop(&mut self) -> Result<()> {
    self.permit.notified().await;                        // pause gate
    while let Some(msg) = self.plan_stack.next().await {
        self.handle_pause_or_suspend().await?;           // K8: token-driven
        self.objs_seen.insert(msg.obj_id());
        self.maybe_cache_for_rewind(&msg);
        let resp = match msg {
            Msg::Read(o)          => self.handle_read(o).await?,
            Msg::Set { obj, .. }  => self.handle_set(obj, ..).await?,
            // ... 26 handlers, one per Msg variant
            Msg::Custom { .. }    => self.dispatch_custom(msg).await?,
        };
        self.plan_stack.send_response(resp);
    }
    self.cleanup().await
}
```

## Bundler

`bsrs_engine::bundler::RunBundler` mirrors `bluesky/bundlers.py`. Holds:

- `run_uid`, `scan_id`
- `streams: HashMap<StreamName, DescriptorState>` — stream name → cached descriptor UID
  (deduplicated by hash of `data_keys`)
- `seq_num: HashMap<StreamName, u64>`
- `out: broadcast::Sender<Document>` — fan-out to sinks
- `overflow_drops: AtomicU64` — exposed as a meta PV per rule **K6**

`Create` opens a bundle. `Read` adds to it. `Save` emits Event(s). `Drop` discards.
`DeclareStream` registers an external stream (used for fly scans).

## Cleanup invariants on RunEngine drop

When the RunEngine task ends or is cancelled, **before** the run is closed:

1. All `Stoppable` devices in `objs_seen` get `stop(success=current_status).await`.
2. All staged devices get `unstage().await` (ignore errors, log them).
3. All `monitor_tokens` are dropped — RAII removes backend slots (rule **K2**).
4. All FramePipes call `stop().await`.
5. The bundler emits a `RunStop` document with the appropriate `exit_status`.

This is enforced by a master `CancellationToken` (rule **K8**): when the RunEngine is
dropped, the token is cancelled and every owned task observes the cancellation and runs
its own cleanup chain.

> **Multi-process deployments (D21).** When the frame data plane runs in a
> separate process from the RunEngine (the production pattern for high-rate
> detectors and rogue), step 4 does not apply locally — the FramePipe lives
> in the source process. The RunEngine's `RunStop` (step 5) still completes
> first; the source process then quiesces its own FramePipe on receiving the
> `RunStop` over the Document plane (D17/D18) and flushes any in-flight
> writes before acking. Cancellation crosses the process boundary via a
> ZMQ control message, not the local `CancellationToken`.

## Pause / Resume / Suspend / Halt

| Action | Trigger | Effect |
|---|---|---|
| `pause(defer=false)` | user `pause()` plan stub or first SIGINT | Clears `permit`. Suspends monitors. Calls `Stoppable::stop(success=true)` on all set/kickoff'd objects. State → `paused`. |
| `pause(defer=true)` | inside a non-rewindable region | Sets a `deferred_pause` flag; pause happens at the next `checkpoint`. |
| `resume()` | user | Restores monitors. Sets `permit`. Replays cached messages from last checkpoint. |
| `request_suspend(fut, pre_plan, post_plan, justification)` | suspender (e.g. shutter closed) | Like pause, but auto-resumes when `fut` resolves. Optionally injects pre/post plans. |
| second SIGINT | user | abort — RunStop with `exit_status: "abort"`, no replay. |
| third SIGINT | user | halt — same as abort but no `Stoppable::stop` call (panic-equivalent). |

## Suspender

```rust
#[async_trait]
pub trait Suspender: Send + Sync + 'static {
    fn name(&self) -> &str;
    /// Resolves once the condition is active (at once if it already is).
    fn trip(&self) -> BoxFuture<'static, ()>;
    /// Resolves once the condition has cleared, resume delay included.
    fn watch(&self) -> BoxFuture<'static, ()>;
    /// `Some(clear-future)` while tripped: gates plan start (ENG-12).
    fn tripped(&self) -> Option<BoxFuture<'static, ()>> { None }
    fn justification(&self) -> String;
    fn pre_plan(&self) -> Option<SuspendCallback> { None }
    fn post_plan(&self) -> Option<SuspendCallback> { None }
}
```

`RunEngine::install_suspender(Arc<dyn Suspender>) -> u64` registers one
(bluesky `RE.install_suspender`); `remove_suspender(id)` and
`clear_suspenders()` uninstall. The registration persists across runs, as
bluesky's `RE._suspenders` does. The engine's watcher loop is
`trip → request_suspend(watch) → watch → trip …`: a trip while a run is in
progress suspends it under the suspender's justification (recorded to the
`interruptions` stream), runs `pre_plan` after the touched motors are
stopped and `post_plan` before the rewind replays, and releases when
`watch` resolves. Tripped at plan start, the suspender gates the run until
it clears. Overlapping suspensions each hold the run; it resumes when the
last releases. A pause the user requested during a suspension is lifted only
by `resume()`. Uninstalling a suspender releases a suspension it holds.
`resume()` also lifts a suspension (there is no Ctrl-C-cancels-`wait_for`
equivalent); a second suspender tripping while the run is already suspended
adds a hold but its plans do not run and its justification is not recorded.

Reference impls over a `tokio::sync::watch::Receiver`, each with
`with_resume_delay` / `with_pre_plan` / `with_post_plan`:

- `SuspendBoolHigh` / `SuspendBoolLow` — a `bool` signal (e.g. shutter PV).
- `SuspendThreshold` (`ThresholdDirection::{BadIfBelow, BadIfAbove}`) and
  `SuspendOutsideBand` — an `f64` signal.
- `SuspendWhenChanged<T>` — any value leaving `expected`; one-shot unless
  `allow_resume()`.

## Differences from `bluesky/run_engine.py`

| bluesky mechanism | bsrs equivalent |
|---|---|
| `_run_permit: asyncio.Event` | `tokio::sync::Notify` + `AtomicState` |
| `stashed_exception` injected via `gen.throw()` | `mpsc::Receiver<PlanInjection>` polled by `select!` inside the plan |
| `SigintHandler` (3-tap) | `tokio::signal::ctrl_c` task driving `RunControl::{pause, abort, halt}` |
| `msg_hook` / `state_hook` callbacks | `tracing` spans + `broadcast::Sender<EngineEvent>` |
| string command + `_command_registry` dict | typed `Msg` enum + `Custom { name, payload }` |
