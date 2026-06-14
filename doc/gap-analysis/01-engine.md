# Gap Analysis 01 — RunEngine Core, Suspenders, Msg Verbs, Bundlers

**Date:** 2026-06-14  
**cirrus ref:** `crates/cirrus-engine/src/` (engine.rs, bundler.rs, suspender.rs, sink.rs)  
**bluesky ref:** `daq/bluesky/src/bluesky/run_engine.py`, `bundlers.py`, `suspenders.py`, `utils/__init__.py`

---

## Scope & Method

Read cirrus `engine.rs` (1 772 lines), `bundler.rs`, `suspender.rs` in full; read bluesky
`run_engine.py` (all `_command_registry` handlers, state machine, `_run` loop, `request_suspend`,
`_start_suspender`), `bundlers.py` (`RunBundler`, `_StreamCache`, `_pack_external_assets`,
`_collect_events`), `suspenders.py` (all classes).

cirrus is NOT a line-for-line port; it is an async-first redesign. Style divergences (Python
thread/asyncio split → Tokio task; generator stack → Rust Stream; `ChainMap` → manual merge) are
intentional and not flagged. Only semantic / protocol / completeness gaps are listed.

---

## Verb coverage (bluesky `_command_registry` → cirrus `Msg` variants)

All 30 public bluesky verbs map to a handled cirrus `Msg` arm.

| bluesky verb | cirrus `Msg` | notes |
|---|---|---|
| `create` | `Msg::Create` | ✓ |
| `save` | `Msg::Save` | ✓ |
| `drop` | `Msg::Drop` | ✓ |
| `read` | `Msg::Read` | ✓ |
| `locate` | `Msg::Locate` | ✓ |
| `set` | `Msg::Set` | ✓ |
| `trigger` | `Msg::Trigger` | ✓ |
| `sleep` | `Msg::Sleep` | ✓ |
| `wait` | `Msg::Wait` | ✓ |
| `checkpoint` | `Msg::Checkpoint` | ✓ |
| `clear_checkpoint` | `Msg::ClearCheckpoint` | ✓ |
| `rewindable` | `Msg::Rewindable` | ✓ |
| `pause` | `Msg::Pause` | ✓ |
| `prepare` | `Msg::Prepare` | ✓ |
| `kickoff` | `Msg::Kickoff` | ✓ |
| `complete` | `Msg::Complete` | ✓ |
| `collect` | `Msg::Collect` | ✓ (gaps in collect details — see ENG-07) |
| `configure` | `Msg::Configure` | ✓ (bundler not updated — see ENG-01) |
| `stage` / `unstage` | `Msg::Stage` / `Msg::Unstage` | ✓ |
| `stop` | `Msg::Stop` | ✓ |
| `subscribe` / `unsubscribe` | `Msg::Subscribe` / `Msg::Unsubscribe` | ✓ (filter gap — ENG-06) |
| `open_run` / `close_run` | `Msg::OpenRun` / `Msg::CloseRun` | ✓ (multi-run gap — ENG-04) |
| `declare_stream` | `Msg::DeclareStream` | ✓ |
| `monitor` / `unmonitor` | `Msg::Monitor` / `Msg::Unmonitor` | ✓ |
| `wait_for` | `Msg::WaitFor` | ✓ |
| `input` | `Msg::Input` | ✓ |
| `install_suspender` / `remove_suspender` | `Msg::InstallSuspender` / `Msg::RemoveSuspender` | ✓ |
| `null` | `Msg::Null` | ✓ |
| `RE_class` | `Msg::ReClass` | ✓ |

Cirrus-only verbs (no bluesky analogue): `Msg::RegisterPausable`, `Msg::UnregisterPausable`,
`Msg::Publish`, `Msg::Fail`, `Msg::Custom` (maps to `register_command` pattern).

---

## Gap Entries (ranked by priority)

---

### ENG-01 `Msg::Configure` does not invalidate/re-emit EventDescriptor (P0)

**cirrus:** `engine.rs:1329` calls `obj.configure_dyn(args)` and returns. `RunBundler` is never
notified.

**ref:** `bundlers.py:1197-1217` (`async def configure`):
```python
await self._current_stream_cache.cache_read_config(obj)
for name in list(self._descriptors):
    obj_set = self._descriptor_objs[name]
    if obj in obj_set:
        del self._descriptors[name]
        await self._prepare_stream(name, obj_set)   # re-emits descriptor with new config
```

**Gap:** After `Msg::Configure`, bluesky invalidates every EventDescriptor that includes the
configured object and immediately re-emits a new descriptor with the updated
`read_configuration()` values. Downstream consumers (tiled, analysis pipelines) rely on this to
associate parameter changes with subsequent events. Cirrus silently skips this: configuration
changes during a scan are invisible to the event stream.

**Fix sketch:** Add a `RunBundler::on_configure(&mut self, obj_name: &str)` method that (a) calls
`obj.read_configuration_dyn()` to refresh cached config, and (b) for every stream whose
descriptor includes `obj_name`, calls `bundle.descriptor(...)` again with the new config and
re-broadcasts the resulting `Document::Descriptor`. Call it from `handle(Msg::Configure {...})`
after `configure_dyn`.

**Effort:** M

---

### ENG-02 External-asset documents (Resource / Datum / StreamResource / StreamDatum) absent (P0)

**cirrus:** `bundler.rs` and `engine.rs:1181-1218` (`Msg::Collect`) call
`obj.collect_dyn()` which returns `Vec<(name, data, timestamps)>` (inline events only).
No path exists to emit Resource, Datum, StreamResource, or StreamDatum documents.
`Msg::Read` similarly calls `obj.read_dyn()` → inline values only.

**ref:** `bundlers.py:862-945` (`_pack_external_assets`): emits Resource/Datum/StreamResource/
StreamDatum with `run_start` back-filled, `seq_nums` managed for StreamDatum, and descriptor
`uid` stitched in. Called from both `save()` and `collect()`.

**Gap:** Most real detectors (Pilatus, Eiger, Lambda, any HDF5 writer) produce data on disk and
reference it through Resource+Datum (or StreamResource+StreamDatum). Without this path, cirrus
cannot record detector data for these devices — the data simply disappears.

**Fix sketch:** (1) Add an `ExternalAssetDoc` enum (`Resource`, `Datum`, `StreamResource`,
`StreamDatum`) to `cirrus_event_model`. (2) Add `collect_asset_docs_dyn()` to the `ReadableObj`
and `FlyableObj` protocols, returning an async iterator of `ExternalAssetDoc`. (3) In
`RunBundler::save()` and the collect handler, drain that iterator, backfill `run_start`, emit via
`broadcast`. This is a multi-crate change; the exact shape of the protocol traits needs design
sign-off.

**Effort:** L

---

### ENG-03 `msg_hook` not exposed (P1)

**cirrus:** `engine.rs:870` logs `tracing::debug!("RE msg: {:?}", &msg)` but does not call any
user-supplied callback. No `msg_hook` field or setter.

**ref:** `run_engine.py:490` `self.msg_hook = None`; `run_engine.py:1645-1646`:
```python
if self.msg_hook is not None:
    self.msg_hook(msg)
```
Called on every Msg before dispatch.

**Gap:** `msg_hook` is the primary developer tool for plan validation, test introspection,
logging, and simulation. Many bluesky test suites capture all Msgs via `msg_hook`.
Without it, plan debugging requires re-implementing instrumentation outside the engine.

**Fix sketch:** Add `msg_hook: StdMutex<Option<Arc<dyn Fn(&Msg) + Send + Sync>>>` to
`RunEngine`. Call it at the top of `handle()` (before the match arm) when `Some`. Expose
`set_msg_hook(f)` in the public API.

**Effort:** S

---

### ENG-04 Multi-run per call not supported (Msg.run key) (P1)

**cirrus:** `EngineState` has `bundler: Option<RunBundler>` — exactly one open run at a time.
`open_run()` (`engine.rs:1584`) errors if `state.bundler.is_some()`.

**ref:** `run_engine.py:504` `self._run_bundlers: dict[Any, RunBundler] = {}`.
`_open_run` (`run_engine.py:1851`) keys on `msg.run`; multiple run keys can coexist.
`_close_run` (`run_engine.py:1890`) deletes one key, leaving others open.

**Gap:** Plans that open interleaved runs (e.g., a fly scan that opens a "primary" run and a
"diagnostics" run simultaneously) cannot be expressed in cirrus. The bluesky `Msg.run` field
is the key discriminator; cirrus's `Msg::OpenRun` / `Msg::CloseRun` have no run-key field at all.
All routing to the right bundler is impossible.

**Fix sketch:** (1) Add `run_key: Option<String>` to `Msg::OpenRun` and `Msg::CloseRun` (and to
`Msg::Create`, `Msg::Save`, `Msg::Drop`, `Msg::DeclareStream` — anything that touches a bundler).
(2) Replace `EngineState.bundler: Option<RunBundler>` with
`bundlers: HashMap<Option<String>, RunBundler>`. (3) In each handler, look up the right bundler
by key. `None` key = the single-run default (fully backward-compatible).

**Effort:** L

---

### ENG-05 `RunResult` missing plan_result, interrupted, reason, exception; one uid only (P1)

**cirrus:** `engine.rs:203-207`:
```rust
pub struct RunResult {
    pub run_uid: Option<String>,   // ← single UID, not a list
    pub exit_status: String,
}
```

**ref:** `run_engine.py:92-116` `RunEngineResult`:
```python
@dataclass
class RunEngineResult:
    run_start_uids: tuple[str, ...]   # all uids generated in __call__
    plan_result: Any                  # generator's StopIteration value
    exit_status: str
    interrupted: bool                 # True if aborted/halted/paused
    reason: str                       # abort reason text
    exception: Exception | None
```

**Gap:** `run_uid` is a single `Option<String>` — loses the second UID when a multi-run plan
finishes. `plan_result` (the value returned by the plan generator) is silently discarded.
`interrupted`, `reason`, and `exception` are absent, so callers cannot distinguish a clean success
from an abort without re-parsing `exit_status`.

**Fix sketch:** Rename `run_uid` → `run_uids: Vec<String>`. Add `plan_result: Option<Box<dyn Any
+ Send>>` (or a simpler typed `plan_value: serde_json::Value`). Add `interrupted: bool`,
`reason: String`, `exception: Option<CirrusError>`. Accumulate all UIDs in `run_loop`
(currently only captures the last one assigned to `run_uid`).

**Effort:** M

---

### ENG-06 `Msg::Subscribe` has no document-type filter (P1)

**cirrus:** `engine.rs:59` `DocumentCallback = Arc<dyn Fn(&Document) + Send + Sync>`. All
subscribers receive every document type.

**ref:** `run_engine.py:640-671` `RE.subscribe(func, name='all')`: `name` is one of
`{'start', 'descriptor', 'event', 'stop', 'all'}`. The Dispatcher routes only matching
document types to each callback. `Msg('subscribe', None, cb, 'event')` wires `cb` to events only.

**Gap:** Plans that subscribe selectively (e.g., "call this at every RunStop to persist the scan
to a database, but ignore events") must receive all documents and filter themselves. The
high-frequency event callback and the once-per-run stop callback cannot be separated, and plan
code that relies on the `document_name` arg to `Msg::Subscribe` silently gets all types.

**Fix sketch:** Add `name: DocFilter` (enum `All | Start | Descriptor | Event | Stop`) to
`Msg::Subscribe`. Change `DocumentCallback` to receive `(doc_name: &str, doc: &Document)` or
store `(DocFilter, Arc<Fn(&Document)>)` and filter in `broadcast`. The `Fn(&Document)` shape can
stay if `broadcast` does the filter: check the enum tag against the Document variant before
calling.

**Effort:** S

---

### ENG-07 `RunBundler::configure` not called; `read_configuration`/`describe_configuration` absent from descriptors (P1)

**cirrus:** `bundler.rs:103-150` (`save()`): calls `bundle.descriptor(stream, data_keys,
pending_config, pending_hints, pending_object_keys)`. `pending_config` is of type
`HashMap<String, Configuration>` and is accumulated. But `Msg::Configure` at `engine.rs:1329`
never touches the bundler — `pending_config` is only populated if the user explicitly calls
something (there is no code path that does).

Neither `ReadableObj` nor any trait in `cirrus_core` has `read_configuration()` or
`describe_configuration()` methods.

**ref:** `bundlers.py:82-131` `_StreamCache.ensure_cached` always calls
`_cache_describe_config(obj)` and `cache_read_config(obj)` before the first save for any
object. These populate `config_values_cache`/`config_ts_cache`/`config_desc_cache`, which
are then embedded into the EventDescriptor's `configuration` field (`bundlers.py:286-295`).

**Gap:** Every EventDescriptor in cirrus has an empty `configuration: {}`. Downstream analysis
tools that rely on knowing, e.g., detector exposure time, ROI configuration, or motor backlash
settings at the time of each scan cannot find this information. This breaks scientific
reproducibility metadata.

**Fix sketch:** (1) Add `describe_configuration_dyn()` / `read_configuration_dyn()` to
`ReadableObj` (returning `HashMap<String, DataKey>` and `HashMap<String, ReadingValue>` with
defaults that return empty maps). (2) In `RunBundler::add_readings`, call both methods
and store results in a per-stream `config_desc_cache`/`config_values_cache`. (3) Pass these into
`bundle.descriptor(...)` in `save()` and `declare_stream()`.

**Effort:** M

---

### ENG-08 `seq_num` not reset on rewind (P1)

**cirrus:** `engine.rs:1010-1027` (`on_resume`): moves `msg_cache` → `replay_queue`. Does not
touch `cirrus_event_model::compose::RunBundle`'s internal sequence counter. After a rewind, the
replayed Msgs re-emit events whose `seq_num` picks up from where the interrupted count left off,
not from the checkpoint-time count.

**ref:** `bundlers.py:520-533` (`rewind()`):
```python
def rewind(self):
    self._sequence_counters.clear()
    self._sequence_counters.update(self._sequence_counters_copy)
    self.bundling = False
```
And `reset_checkpoint_state()` (`bundlers.py:651-658`) snapshots `_sequence_counters_copy` at
every checkpoint. On rewind, sequence counters are rolled back to the checkpoint snapshot.

**Gap:** A rewound event stream has duplicate `seq_num` values (events after the checkpoint get
re-emitted with continued seq_nums instead of restarted ones). Consumers that validate seq_num
monotonicity will reject the second attempt; tiled will produce duplicate sequence numbers in the
same event stream.

**Fix sketch:** Add `snapshot_sequence_counter(stream_name)` / `restore_sequence_counter(stream_name, snapshot)` to `cirrus_event_model::compose::RunBundle`. In `RunBundler`, on `Checkpoint` save a snapshot; on resume (`on_resume` before replay begins), restore the snapshot for each affected stream.

**Effort:** M

---

### ENG-09 `backstop_collect` not called on cleanup (P1)

**cirrus:** `engine.rs:485-510` (cleanup block in `run_async`): stops movables/flyables, unstages
staged devices. It does **not** call `complete()`+`collect()` on flyables still in the
`flyable_objs_touched` map.

**ref:** `bundlers.py:1190-1195`:
```python
async def backstop_collect(self):
    for obj in list(self._uncollected):
        try:
            await self.collect(Msg("collect", obj))
        except Exception:
            self.log.exception("Failed to collect %r.", obj)
```
Called during `_run` cleanup for any flyer that had `kickoff` but not `collect`. The
`_uncollected` set tracks flyers added in `kickoff()` and removed in `collect()`.

**Gap:** If a plan aborts after `Msg::Kickoff` but before `Msg::Collect`, the flyer's buffered
data is permanently lost. Bluesky makes a best-effort data rescue. cirrus silently drops it.

**Fix sketch:** Add an `uncollected: HashSet<String>` to `EngineState`. In `handle(Msg::Kickoff)`,
insert the flyer's name; in `handle(Msg::Collect)`, remove it. In the `run_async` cleanup block,
after stopping flyables, iterate `uncollected` and call `complete_dyn()` + `collect_dyn()` on
each (best-effort, log errors).

**Effort:** S

---

### ENG-10 Suspender `sleep` (resume-delay) missing (P1)

**cirrus:** `suspender.rs:86` (`SuspendBoolHigh::install`) → `spawn_bool_watcher`. The resume
future resolves as soon as the signal flips to GOOD. No delay.

**ref:** `suspenders.py:34-37` `SuspenderBase.__init__`: `sleep=0` param. `__set_event`
(`suspenders.py:170-188`):
```python
loop.call_later(sleep, ev.set)   # delay before actually setting event
```
`SuspendFloor`/`SuspendCeil` both inherit this via `_Threshold.__init__`.

**Gap:** Beamlines routinely set `sleep=5` on a beam-current suspender: "wait 5 seconds after
beam returns before resuming, to let the beam stabilize." Without this, plans resume the instant
beam flickers back and immediately fail because the beam is still unstable.

**Fix sketch:** Add `resume_delay: Option<Duration>` to `SuspendBoolHigh`, `SuspendBoolLow`, and
`SuspendThreshold`. In the resume future, after the GOOD condition is met, do
`tokio::time::sleep(delay).await` before returning. No API changes to `RunEngine` needed.

**Effort:** S

---

### ENG-11 Suspender `pre_plan` / `post_plan` injection missing (P1)

**cirrus:** `suspend_until_with` (`engine.rs:699-718`) immediately pauses + spawns a task that
calls `resume()` when `fut` resolves. No way to inject a plan before the suspend or after it.

**ref:** `run_engine.py:1199-1311` `request_suspend(fut, pre_plan=None, post_plan=None)` and
`_start_suspender`. On suspension, bluesky:
1. Stops all motors.
2. Notifies Pausable devices.
3. Runs `pre_plan` (e.g., close photon shutter).
4. Awaits `fut` (suspender condition).
5. Runs `_resume_from_suspender` (re-notify Pausable).
6. Runs `post_plan` (e.g., re-open photon shutter, reset detector).
7. Replays from last checkpoint.

**Gap:** Without `pre_plan`/`post_plan`, suspenders cannot close/re-open shutters or reset state
around a suspension. This is a hard requirement for any beamline with beam-shutting interlocks.

**Fix sketch:** Add `pre_plan: Option<Plan>` and `post_plan: Option<Plan>` to
`SuspendCallback` (already defined as a type alias in `engine.rs:229`). Extend
`suspend_until_with` to accept these; when suspension is triggered, push the pre_plan into a
priority queue, then wait on `fut`, then push the post_plan. This requires the plan stack to
become a `VecDeque<Plan>` (bluesky's `_plan_stack`). A simpler first cut: accept pre/post as
closures that return `BoxFuture<'static, ()>` and await them in the suspend task.

**Effort:** M

---

### ENG-12 Suspenders not checked for tripped state before plan start (P1)

**cirrus:** `run_async()` / `run_async_with()` starts the plan immediately. No check of
installed suspenders.

**ref:** `run_engine.py:933-967` (`__call__`):
```python
for sup in self.suspenders:
    f_lst, justification = sup.get_futures()
    if f_lst:
        futs.extend(f_lst)
        tripped_justifications.append(justification)
if tripped_justifications:
    print("At least one suspender has tripped...")
# Then prepends Msg('wait_for', None, futs) before the plan.
```

**Gap:** If beam is already off when `run_async` is called, bluesky waits for all suspenders to
clear before starting. cirrus starts immediately, the plan runs one tick, and the suspender trips
mid-plan — the scan starts and the first scan point is corrupt.

**Fix sketch:** In `run_async`, after resetting state and before entering `run_loop`, iterate
`state.suspenders.values()` and call `suspender.watch_tripped()` (a new method: return a future
that resolves immediately if good, or after the condition clears). Combine into a `wait_all` and
poll it before the plan starts. Alternatively, model this as a synthetic `Msg::WaitFor` prepended
to the plan.

**Effort:** M

---

### ENG-13 `SuspendWhenOutsideBand` and `SuspendWhenChanged` missing (P1)

**cirrus:** `suspender.rs` exports only `SuspendBoolHigh`, `SuspendBoolLow`, `SuspendThreshold`.

**ref:** `suspenders.py`:
- `SuspendWhenOutsideBand` (`line 453`): suspend when scalar leaves `(band_bottom, band_top)`;
  resume when back inside. Common for temperature controllers and beam position.
- `SuspendWhenChanged` (`line 551`): suspend when a signal deviates from `expected_value`; by
  default `allow_resume=False` (requires restart). Used for facility-mode enum PVs.

**Gap:** Both are in regular use at beamlines. Without them, users must hand-roll the logic
using `suspend_until_with`.

**Fix sketch:** Add `SuspendOutsideBand { name, rx: Receiver<f64>, band_bottom, band_top,
resume_delay }` and `SuspendWhenChanged<T: Eq + Send + Clone + 'static> { name, rx, expected,
allow_resume, resume_delay }` in `suspender.rs`, each with an `install(re: Arc<RunEngine>) ->
JoinHandle<()>` method following the existing pattern in `spawn_bool_watcher`.

**Effort:** S (M for the generic `SuspendWhenChanged<T>`)

---

### ENG-14 `scan_id` not written back to `RE.md` after each run (P1)

**cirrus:** `engine.rs:1562-1566` stores scan_id into `merged` (the per-run metadata) and
inserts it into the RunStart. The internal `AtomicU64 scan_id` counter is incremented. But
`self.md` (the persistent dict) is never updated with the new `scan_id`.

**ref:** `run_engine.py:1855`:
```python
self.md["scan_id"] = await maybe_await(self.scan_id_source(self.md))
```
`scan_id_source`'s default reads `md.get("scan_id", 0) + 1` and bluesky writes the result
back into `self.md`. Between runs, `RE.md["scan_id"]` always reflects the last used scan ID,
so custom `scan_id_source` functions can read `md["scan_id"]` to continue a sequence.

**Gap:** Custom `ScanIdSource` callbacks that read from the metadata dict receive a stale
`scan_id = 0` (or whatever was set at init). External code that persists `RE.md` between sessions
does not get the current scan counter.

**Fix sketch:** In `open_run()` (`engine.rs:1527`), after resolving `scan_id`, write it back:
```rust
self.md.lock().unwrap().insert("scan_id".into(), Value::from(scan_id));
```
Place this immediately after the counter update, before the validator/normalizer run.

**Effort:** S

---

### ENG-15 Suspender hysteresis: single threshold for suspend and resume (P2)

**cirrus:** `SuspendThreshold` (`suspender.rs:116`) has one `threshold: f64`. The same value is
compared on both the "enter bad" and "exit bad" checks.

**ref:** `suspenders.py:302-314` `_Threshold.__init__`:
```python
self._suspend_thresh = suspend_thresh
if resume_thresh is None:
    resume_thresh = suspend_thresh
self._resume_thresh = resume_thresh
```
`SuspendFloor._should_resume` checks `not self._op(value, self._resume_thresh)`, so the
thresholds can differ (e.g., suspend at < 50 mA, resume at > 60 mA).

**Gap:** Without hysteresis, the suspender thrashes if the signal oscillates around the threshold.
`SuspendFloor(sig, 50, resume_thresh=60)` is a standard beamline pattern.

**Fix sketch:** Add `resume_threshold: Option<f64>` to `SuspendThreshold`. When `None`, fall back
to `self.threshold` for both. In `spawn_threshold_watcher`, use `resume_threshold.unwrap_or(threshold)` in the "wait until GOOD" inner loop.

**Effort:** S

---

### ENG-16 `waiting_hook` missing (P2)

**cirrus:** No equivalent.

**ref:** `run_engine.py:333-341` `waiting_hook`: callable with signature
`f(status_objects)` called whenever the engine is blocking on a long-running `wait` (trigger,
set, kickoff, complete). Called with `None` when waiting ends. Used for progress bars.

**Gap:** No way to drive a progress bar or ETA display from `Msg::Wait` without subclassing or
polling from outside the engine.

**Fix sketch:** Add `waiting_hook: StdMutex<Option<Arc<dyn Fn(Option<&[Status]>) + Send + Sync>>>`.
Call it in `wait_group()` with the live `Status` slice when waiting begins, and `None` when done.

**Effort:** S

---

### ENG-17 `state_hook` missing (P2)

**cirrus:** State is exposed via `RunEngine::state()` which reads atomics at call time.
State transitions are not observable.

**ref:** `run_engine.py:328-332` `state_hook`: callable `f(new_state, old_state)` called on every
state change. `LoggingPropertyMachine.__set__` invokes it (`run_engine.py:193-194`).

**Fix sketch:** Add `state_hook: StdMutex<Option<Arc<dyn Fn(EngineRunState, EngineRunState) + Send
+ Sync>>>`. Call it in each of the places that write to `is_paused` / `is_aborting` / etc.,
computing old and new `EngineRunState` before/after the store.

**Effort:** S

---

### ENG-18 `panicked` terminal state missing (P2)

**cirrus:** Three-tap SIGINT in `install_signal_handler` halts on the 3rd tap. No irrecoverable
terminal state.

**ref:** `run_engine.py:148` `States.PANICKED`: entered if a `KeyboardInterrupt` fires inside the
event-loop thread (not on the main thread). Once panicked, `__call__` raises immediately
without letting the plan touch state. The only recovery is to restart the Python session.

**Gap:** The panic path defends against partial writes to shared state that would leave the engine
in an indeterminate condition. Without it, a sufficiently bad signal race can leave cirrus in a
state where it accepts another plan but has stale internal state.

**Fix sketch:** Add `is_panicked: AtomicBool`. In `run_async`, check at entry and return
`Err(CirrusError::Panicked)`. Add `EngineRunState::Panicked`. Set on unrecoverable internal
errors (e.g., Tokio runtime failure). Since cirrus is fully async (no cross-thread signal
injection), the risk is lower than in bluesky, so P2 is appropriate.

**Effort:** S

---

### ENG-19 `clear_suspenders()` convenience method missing (P2)

**cirrus:** `Msg::RemoveSuspender` removes by id; no bulk clear. No external method equivalent.

**ref:** `run_engine.py:1187-1197` `RE.clear_suspenders()`.

**Fix sketch:** `pub async fn clear_suspenders(&self) { self.state.lock().await.suspenders.clear(); }` — the `SuspenderHandle` Drop aborts all watcher tasks.

**Effort:** S

---

### ENG-20 `deferred_pause_requested` property not exposed (P2)

**cirrus:** `deferred_pause: AtomicBool` (`engine.rs:241`) is private.

**ref:** `run_engine.py:390-404` `RE.deferred_pause_requested` property — public read-only.

**Fix sketch:** Add `pub fn deferred_pause_requested(&self) -> bool { self.deferred_pause.load(Ordering::SeqCst) }`.

**Effort:** S

---

### ENG-21 `strict_pre_declare` / `_require_stream_declaration` mode absent (P2)

**cirrus:** `Msg::Create` does not check whether the stream was pre-declared.

**ref:** `run_engine.py:533-534` `self._require_stream_declaration = False`; `bundlers.py:399-401`:
```python
if self._strict_pre_declare:
    if self._bundle_name not in self._descriptors:
        raise IllegalMessageSequence("In strict mode you must pre-declare streams.")
```

**Fix sketch:** Add `require_stream_declaration: AtomicBool` to `RunEngine`. In
`RunBundler::create()`, check `engine.require_stream_declaration` and error if the stream was not
pre-declared.

**Effort:** S

---

### ENG-22 `plan_type` absent from `open_run` metadata (P2)

**cirrus:** `open_run()` (`engine.rs:1527`) inserts `plan_name` but not `plan_type`.

**ref:** `run_engine.py:1857-1866`:
```python
plan_type = type(self._plan).__name__
plan_name = getattr(self._plan, "__name__", "")
md = ChainMap({...,"plan_type": plan_type, "plan_name": plan_name}, ...)
```

**Fix sketch:** Add `plan_type: Option<String>` to `RunMetadata`. Plans that know their type fill
it; cirrus inserts into merged md alongside `plan_name`.

**Effort:** S

---

### ENG-23 `ignore_callback_exceptions` not supported (P2)

**cirrus:** `broadcast()` (`engine.rs:1655-1673`) calls each `DocumentCallback` synchronously.
Any panic unwinds the engine. There is no try/catch around callbacks.

**ref:** `run_engine.py:490` `self.ignore_callback_exceptions = False`; `Dispatcher.process`
collects exceptions and either swallows them (if `ignore_exceptions=True`) or warns.

**Fix sketch:** Wrap each callback invocation in `std::panic::catch_unwind`. If
`ignore_callback_exceptions` is true, log the panic and continue. If false, re-raise.

**Effort:** S

---

## Summary table

| ID | Title | Priority | Effort |
|---|---|---|---|
| ENG-01 | `Msg::Configure` does not invalidate/re-emit EventDescriptor | P0 | M |
| ENG-02 | External-asset documents (Resource/Datum/StreamResource/StreamDatum) absent | P0 | L |
| ENG-03 | `msg_hook` not exposed | P1 | S |
| ENG-04 | Multi-run per call not supported (Msg.run key) | P1 | L |
| ENG-05 | `RunResult` missing plan_result, interrupted, reason, exception; one uid only | P1 | M |
| ENG-06 | `Msg::Subscribe` has no document-type filter | P1 | S |
| ENG-07 | `read_configuration`/`describe_configuration` absent from descriptors | P1 | M |
| ENG-08 | `seq_num` not reset on rewind | P1 | M |
| ENG-09 | `backstop_collect` not called on cleanup | P1 | S |
| ENG-10 | Suspender `sleep` (resume-delay) missing | P1 | S |
| ENG-11 | Suspender `pre_plan` / `post_plan` injection missing | P1 | M |
| ENG-12 | Suspenders not checked for tripped state before plan start | P1 | M |
| ENG-13 | `SuspendWhenOutsideBand` and `SuspendWhenChanged` missing | P1 | S–M |
| ENG-14 | `scan_id` not written back to `RE.md` after each run | P1 | S |
| ENG-15 | Suspender hysteresis: single threshold for suspend and resume | P2 | S |
| ENG-16 | `waiting_hook` missing | P2 | S |
| ENG-17 | `state_hook` missing | P2 | S |
| ENG-18 | `panicked` terminal state missing | P2 | S |
| ENG-19 | `clear_suspenders()` convenience method missing | P2 | S |
| ENG-20 | `deferred_pause_requested` property not exposed | P2 | S |
| ENG-21 | `strict_pre_declare` / `_require_stream_declaration` mode absent | P2 | S |
| ENG-22 | `plan_type` absent from `open_run` metadata | P2 | S |
| ENG-23 | `ignore_callback_exceptions` not supported | P2 | S |

**Counts:** P0 = 2, P1 = 12, P2 = 9. Total = 23 gaps.

---

## What cirrus already matches (summary)

- All 30 bluesky `_command_registry` verbs have a handled `Msg` arm.
- State machine (Idle/Running/Paused/Aborting/Halting) covers all common transitions.
- Deferred pause (`defer=true` → applied at next Checkpoint) — matches bluesky.
- Checkpoint / ClearCheckpoint / rewind replay cache — correct semantics.
- SIGINT 3-tap (pause → abort → halt) — matches bluesky.
- `record_interruptions` stream (pause/resume/suspend events) — matches.
- `md_validator`, `md_normalizer`, `scan_id_source` hooks — all present.
- `preprocessors` list — present and applied in order.
- `before_plan` / `after_plan` hooks — present.
- `loop_until_completion_timeout` — present as `set_loop_timeout`.
- Per-call `md` and `subs` via `run_async_with` / `RunOptions` — matches.
- `temp_subscribers` cleanup at run end — matches `_temp_callback_ids`.
- `movable_objs_touched` stop-on-pause and stop-on-cleanup — matches.
- Pausable device hook (`pause_dyn()` / `resume_dyn()`) — present.
- Monitor RAII pump (task abort + subscription drop on Unmonitor / pause) — present.
- `register_command` / `unregister_command` — present as `register_command()`.
- `SuspendBoolHigh` / `SuspendBoolLow` / `SuspendThreshold` core logic — present.
- `suspend_until_with(fut, justification)` — present; justification recorded in interruptions stream.
- `inject_document()` for external document bridging — cirrus addition, no bluesky analogue.
- `CheckpointHook` / crash-recovery audit trail — cirrus addition.
