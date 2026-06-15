# Gap Analysis 04 — Devices & Signal Backends

**Area:** `crates/bsrs-devices/`, `crates/bsrs-backends/{soft,mock,epics-ca,epics-pva}/`  
**Ref:** `daq/ophyd-async/src/ophyd_async/core/_detector.py`, `epics/motor.py`, `sim/_motor.py`, `epics/core/_aioca.py`, `epics/core/_p4p.py`, `core/_mock_signal_backend.py`  
**Date:** 2026-06-14

---

## Summary table

| ID | Priority | Title |
|----|----------|-------|
| DB-01 | P0 | `DetectorTrigger` enum entirely absent — trigger modes indistinguishable |
| DB-02 | P0 | `TriggerInfo` missing `trigger`, `exposures_per_collection`, computed fields |
| DB-03 | P0 | `StandardDetector.stage()` does not disarm — leaves detector armed between scans |
| DB-04 | P0 | `describe_dyn`/`describe_collect_dyn` call `writer.open()` as side effect — correctness bug |
| DB-05 | P0 | CA backend: no enum (DBR_ENUM) type — areaDetector mbbo/mbbi PVs lose label |
| DB-06 | P0 | No `WatchableAsyncStatus`/`WatcherUpdate` anywhere in bsrs — live progress impossible |
| DB-07 | P1 | CA backend: no array (waveform) type beyond char-as-long-string |
| DB-08 | P1 | PVA backend: no enum (NTEnum) type — NTEnum PVs decoded wrong |
| DB-09 | P1 | PVA backend: no array (NTScalarArray) type |
| DB-10 | P1 | CA/PVA `get_reading()` always returns `alarm_severity: None` |
| DB-11 | P1 | CA `get_datakey()` omits units/precision/limits (DBR_CTRL not fetched) |
| DB-12 | P1 | Mock backend: no `set_value`, no put interception — test ergonomics broken |
| DB-13 | P1 | `SoftMotor.set()` is instant — no velocity profile, no watchable progress |
| DB-14 | P1 | No EPICS Motor device (velocity/limits/set-with-timeout/fly/subscribe) |
| DB-15 | P1 | `StandardDetector.trigger()` lacks implicit prepare and watchable progress |
| DB-16 | P1 | `StandardDetector.complete()` returns plain `Status` — no frame-progress watch |
| DB-17 | P1 | No `FlyMotorInfo` concept — fly-scan motor prepare/kickoff/complete absent |
| DB-18 | P2 | PVA `get_reading()` uses local clock — server timestamp from NTScalar body unused |
| DB-19 | P2 | No `AreaDetector` generic composite / `NDSimDetector` packaged type |
| DB-20 | P2 | `StandardDetector` architecture: no logic-composition split (TriggerLogic/ArmLogic/DataLogic) |

---

## P0 — Correctness / protocol divergence

---

### DB-01 · `DetectorTrigger` enum entirely absent

**bsrs:** `crates/bsrs-protocols-async/src/lib.rs` — `TriggerInfo` struct exists but has no `trigger` field. No equivalent of `DetectorTrigger` exists anywhere.

**ref:** `daq/ophyd-async/src/ophyd_async/core/_detector.py:50-63`
```python
class DetectorTrigger(Enum):
    INTERNAL = "INTERNAL"
    EXTERNAL_EDGE = "EXTERNAL_EDGE"
    EXTERNAL_LEVEL = "EXTERNAL_LEVEL"
```

**Gap:** There is no way to express that a detector should be externally triggered. `DetectorControl.prepare()` receives a `TriggerInfo` but cannot communicate INTERNAL vs EXTERNAL_EDGE vs EXTERNAL_LEVEL to the hardware. areaDetector trigger-mode PVs (e.g., `TriggerMode`) must be set based on this value.

**Fix sketch:** Add `pub enum DetectorTrigger { Internal, ExternalEdge, ExternalLevel }` to `bsrs-protocols-async/src/lib.rs` and a `trigger: DetectorTrigger` field to `TriggerInfo`. Update `DetectorControl::prepare` call sites in `SoftDetectorControl` and bsrs-host. Effort: **S**.

---

### DB-02 · `TriggerInfo` missing `trigger`, `exposures_per_collection`, and computed fields

**bsrs:** `crates/bsrs-protocols-async/src/lib.rs:216-236`
```rust
pub struct TriggerInfo {
    pub number: u32,        // ≈ number_of_events
    pub livetime: Option<Duration>,
    pub deadtime: Option<Duration>,
    pub multiplier: u32,    // ≈ collections_per_event
}
```

**ref:** `daq/ophyd-async/src/ophyd_async/core/_detector.py:65-113`

**Gap:** Missing:
- `trigger: DetectorTrigger` (see DB-01)
- `exposures_per_collection: u32` — number of detector trigger pulses that are averaged/combined into one frame. When > 1, the areaDetector `NumExposures` PV is set.
- `exposure_timeout: Duration` — guards `complete()` loops; currently `complete()` in `StandardDetector` has no timeout.
- Computed: `number_of_collections = number_of_events * collections_per_event`, `number_of_exposures = number_of_collections * exposures_per_collection`. Fly-scan kickoff uses `number_of_collections` to know when to stop watching the index.

Naming: bsrs uses `number` for what Python calls `number_of_events` and `multiplier` for `collections_per_event`. These should be renamed for consistency with bluesky tooling.

**Fix sketch:** Add missing fields; rename `number → number_of_events`, `multiplier → collections_per_event`; derive `number_of_collections()` and `number_of_exposures()` as methods. Effort: **S**.

---

### DB-03 · `StandardDetector.stage()` does not disarm

**bsrs:** `crates/bsrs-devices/src/detector.rs:78-87`
```rust
async fn stage(&self) -> Result<()> {
    self.writer.open(1).await?;  // opens writer with multiplier=1
    Ok(())
}
```

**ref:** `daq/ophyd-async/src/ophyd_async/core/_detector.py:432-438`
```python
async def stage(self) -> None:
    await self._disarm_and_stop(on_unstage=False)
    self._prepare_ctx = None
    self._kickoff_ctx = None
    await self.events_to_kickoff.set(0)
```

**Gap:** Python's `stage()` calls `_disarm_and_stop(on_unstage=False)` — this disarms and stops the data logic **before** the new scan starts. This ensures a detector left armed from a previous scan (e.g., after abort) is reset to idle. Bsrs `stage()` skips this entirely and directly opens the writer, which may open on top of a still-armed hardware detector. Also, `stage()` also shouldn't open the writer — that's the job of `prepare()` / data logic.

**Fix sketch:** Call `self.control.disarm().await?` before `self.writer.close().await` in `stage()`. Remove the premature `writer.open(1)` from `stage()` — the open should happen in `prepare()`. Effort: **S**.

---

### DB-04 · `describe_dyn`/`describe_collect_dyn` call `writer.open()` as side effect

**bsrs:** `crates/bsrs-devices/src/detector.rs:243` and `crates/bsrs-devices/src/detector.rs:287`
```rust
// describe_collect_dyn:
let dk = self.writer.open(1).await?;   // BUG: opens writer as side effect of describe

// describe_dyn:
self.writer.open(1).await              // BUG: same
```

**ref:** `daq/ophyd-async/src/ophyd_async/core/_detector.py:700-708`
```python
async def describe(self) -> dict[str, DataKey]:
    ctx = error_if_none(self._prepare_ctx, "Prepare not run")
    coros = [dp.make_datakeys() for dp in ctx.readable_data_providers] + ...
    return await merge_gathered_dicts(coros)
```

**Gap:** Python's `describe()` reads DataKeys from the already-prepared data providers stored in `_prepare_ctx`. It never calls `open()`. Bsrs's `describe_dyn()` and `describe_collect_dyn()` call `writer.open(multiplier=1)` every time they are called, even in the middle of an acquisition. This can:
- Reset the writer state and re-emit a `StreamResource` with a different uid.
- Conflict with an in-progress acquisition's frame counter.
- Silently override any non-1 multiplier set by `prepare()`.

**Fix sketch:** Cache the DataKeys returned by `writer.open()` when `prepare()` is called (store them in a `Mutex<Option<HashMap<String, DataKey>>>` field). Have `describe_dyn()` and `describe_collect_dyn()` read from this cache. Error if cache is empty ("prepare not called"). Effort: **S**.

---

### DB-05 · CA backend: no enum (DBR_ENUM) type

**bsrs:** `crates/bsrs-backends/epics-ca/src/real.rs` — `SignalBackend<T>` impls exist for `f64`, `i64`, `bool`, `String`. No impl for an enum type.

**ref:** `daq/ophyd-async/src/ophyd_async/epics/core/_aioca.py:158-185`
```python
class CaEnumConverter(CaConverter[str]):
    def __init__(self, supported_values: Mapping[str, str]):
        self.supported_values = supported_values
        super().__init__(str, dbr.DBR_STRING, metadata=SignalMetadata(choices=list(supported_values)))

    def value(self, value: AugmentedValue) -> str:
        return self.supported_values[str(value)]
```

**Gap:** `mbbi`/`mbbo` records (ImageMode, TriggerMode, FileWriteMode, DetectorState_RBV, etc.) are DBR_ENUM on the wire. The bsrs CA backend has no way to receive the label string; users must use `EpicsCaBackend<i64>` which returns an integer and loses validation against the choices set. The DataKey produced has no `choices` field. Plan code that tries to `put(ImageMode::Multiple)` can't express the enum value.

**Fix sketch:** Add `pub enum CaEnumKind { ByLabel, ByIndex }` and a `CaEnumBackend` wrapper (or a `SignalBackend<String>` variant with `enum_choices: Option<Vec<String>>`) that reads DBR_STRING on get/subscribe and writes DBR_STRING on put, populating `DataKey.choices`. Also add `CaEnumBackend<E>` where `E: StrictEnum` for validated puts. Effort: **M**.

---

### DB-06 · No `WatchableAsyncStatus`/`WatcherUpdate` anywhere in bsrs

**bsrs:** `bsrs-core::status::Status` is a plain future that resolves to `Ok(())|Err(StatusError)`. There is no progress-watching layer.

**ref:** `daq/ophyd-async/src/ophyd_async/core/_status.py` and `_utils.py`
```python
@dataclass
class WatcherUpdate:
    current: T; initial: T; target: T; name: str; unit: str; precision: int; time_elapsed: float
class WatchableAsyncStatus(AsyncStatusBase): ...  # yields WatcherUpdate
```

**Gap:** Motor `set()`, detector `trigger()`, and detector `complete()` all need to yield live progress updates so bluesky's RunEngine can update progress bars and decide when to check `done_status`. Without `WatcherUpdate`, scan GUIs can't show "motor at 12.3 / 20.0 mm" or "detector 4/10 frames". This is a systemic absence — it affects every device that moves or counts.

**Fix sketch:** Add `WatcherUpdate<T>` struct and `WatchableStatus` trait to `bsrs-protocols-async` (e.g., as an `AsyncGenerator`-like stream: `BoxStream<'_, WatcherUpdate<T>>` returned alongside a `Status`). Adapt `SoftMotor`, `StandardDetector.trigger`, and `StandardDetector.complete` to emit updates at key intervals. Effort: **L**.

---

## P1 — Meaningful completeness gap

---

### DB-07 · CA backend: no array/waveform type

**bsrs:** `crates/bsrs-backends/epics-ca/src/real.rs` — no `SignalBackend<Vec<T>>` or `SignalBackend<Array1D<T>>` impl.

**ref:** `daq/ophyd-async/src/ophyd_async/epics/core/_aioca.py:169-186` — `CaArrayConverter` for all waveform DBR types.

**Gap:** Numeric waveform PVs (e.g., `ArraySizeX_RBV`, custom detector waveforms) cannot be read. The char-waveform long-string path (`EpicsCaBackend::new_long_string`) exists for `String` but no `Vec<f64>` or `Vec<u8>` variant exists.

**Fix sketch:** Add `impl SignalBackend<Vec<f64>> for EpicsCaBackend<Vec<f64>>` and similar for `Vec<f32>`, `Vec<i32>`, `Vec<u8>`. The `get()` path converts `EpicsValue::FloatArray`/`DoubleArray`/etc. Effort: **M**.

---

### DB-08 · PVA backend: no enum (NTEnum) type

**bsrs:** `crates/bsrs-backends/epics-pva/src/real.rs` — no NTEnum handling.

**ref:** `daq/ophyd-async/src/ophyd_async/epics/core/_p4p.py:168-178`
```python
class PvaEnumConverter(PvaConverter[str]):
    def value(self, value: Any) -> str:
        str_value = value["value"]["choices"][value["value"]["index"]]
        ...
```

**Gap:** An NTEnum PvField has structure `{value: {index: int, choices: [str]}}`. Bsrs `pv_field_to_string()` only matches `PvField::Scalar(ScalarValue::String(_))` — NTEnum would fall through as `None` and fail. areaDetector PVs accessed via pvaSrv/QSRV publish enums as NTEnum.

**Fix sketch:** In `pv_field_to_string()`, add an arm that checks if the structure's type_id is `"epics:nt/NTEnum:1.0"` (or a `choices`+`index` substructure) and decodes via `choices[index]`. Effort: **S**.

---

### DB-09 · PVA backend: no array (NTScalarArray) type

**bsrs:** `crates/bsrs-backends/epics-pva/src/real.rs` — no `SignalBackend<Vec<T>>`.

**ref:** `daq/ophyd-async/src/ophyd_async/epics/core/_p4p.py:229-244` — full NTScalarArray converter table.

**Gap:** NTScalarArray PVs (e.g., numeric waveforms published over PVA) cannot be read at all with bsrs.

**Fix sketch:** Add `impl SignalBackend<Vec<f64>> for EpicsPvaBackend<Vec<f64>>` and similar; decode `PvField::ScalarArray(ScalarArrayValue::DoubleArray(_))` in `pv_field_to_*` helpers. Effort: **M**.

---

### DB-10 · CA and PVA `get_reading()` always return `alarm_severity: None`

**bsrs:** `crates/bsrs-backends/epics-ca/src/real.rs:370-382` and `crates/bsrs-backends/epics-pva/src/real.rs:191-203` — all `get_reading()` impls set `alarm_severity: None`.

**ref:** `daq/ophyd-async/src/ophyd_async/epics/core/_aioca.py:305-309`
```python
def _make_reading(self, value: AugmentedValue) -> Reading:
    return {"value": ..., "timestamp": ...,
            "alarm_severity": -1 if value.severity > 2 else value.severity}
```

**Gap:** Alarm state is never surfaced. Operators relying on alarm severity for interlocks or scan decisions get no information.

**Fix sketch:** For CA: fetch with `FORMAT_TIME` to get `severity` field; map EPICS alarm severity 0/1/2/3 → bsrs `alarm_severity` (0=NO_ALARM, 1=MINOR, 2=MAJOR, 3=INVALID). For PVA: extract `alarm.severity` from the NTScalar `alarm` substructure when present. Effort: **S** per backend.

---

### DB-11 · CA `get_datakey()` omits units/precision/limits

**bsrs:** `crates/bsrs-backends/epics-ca/src/real.rs:347-369` (`SignalBackend<f64>::get_datakey`) — hardcodes `units: None, precision: None, limits: None`.

**ref:** `daq/ophyd-async/src/ophyd_async/epics/core/_aioca.py:343-348`
```python
async def get_datakey(self, source: str) -> DataKey:
    value = await self._caget(self.read_pv, FORMAT_CTRL)
    metadata = _metadata_from_augmented_value(...)
    return make_datakey(..., metadata)
```

**Gap:** Python fetches `FORMAT_CTRL` on connect, which carries units, precision, and alarm/ctrl/display limit ranges. Bsrs only calls `ch.info()` (native type + count). The resulting `DataKey` in event-model will be missing `units`, `precision`, and `limits` for any numeric PV. Plans that display units in GUIs or check limit ranges fail silently.

**Fix sketch:** Add a `DBR_CTRL` get call in `get_datakey()` (or cache it at `connect()` time, following the same approach as `ensure_channel`). Decode units/precision from the response and populate `DataKey` fields. Effort: **M**.

---

### DB-12 · Mock backend: no `set_value`, no put interception

**bsrs:** `crates/bsrs-backends/mock/src/lib.rs`
```rust
pub struct MockBackend<T> { value: T }  // fixed forever
impl SignalBackend<T> for MockBackend<T> {
    async fn put(&self, _value: T, ...) -> Status { Status::done() }   // ignores value
    fn set_callback(&self, _cb: ...) -> SubToken { SubToken::noop() }  // no subscriptions
}
```

**ref:** `daq/ophyd-async/src/ophyd_async/core/_mock_signal_backend.py:26-80`
```python
class MockSignalBackend:
    def set_value(self, value): ...           # inject new value
    def set_mock_put_callback(self, cb): ...  # react to puts (e.g., instant motor move)
    put_mock: AsyncMock                        # assert put was called with specific value
```

**Gap:** Bsrs `MockBackend` can only return one fixed value for the lifetime of the test. It cannot:
- Simulate a motor responding to a setpoint write by moving the readback.
- Assert that a `put()` was called with the right value.
- Inject new values mid-test (e.g., simulate hardware changing state).

This prevents writing unit tests for any plan that involves feedback between writes and reads (i.e., most real plans). Tests must use `SoftSignalBackend` directly, which isn't the intention for the mock crate.

**Fix sketch:** Replace `MockBackend` with a struct wrapping a `SoftSignalBackend<T>` (for state), a `Mutex<Option<Box<dyn Fn(&T)>>>` (put callback), and a `Vec<T>` (put history). Expose `set_value()`, `set_put_callback()`, and `put_history()`. Effort: **M**.

---

### DB-13 · `SoftMotor.set()` is instant — no velocity profile

**bsrs:** `crates/bsrs-backends/soft/src/motor.rs:100-103`
```rust
async fn set(&self, value: f64) -> Status {
    self.backend.put(value, true, None).await   // instant move
}
```

**ref:** `daq/ophyd-async/src/ophyd_async/sim/_motor.py:109-203` — SimMotor computes trapezoidal velocity profile at 10 Hz, emits `WatcherUpdate` at each step.

**Gap:** `SoftMotor` cannot simulate time-based motion. Plans that calculate timeouts from velocity (e.g., `timeout = distance/velocity + 2*accel_time`) will get a zero timeout because `SoftMotor` has no `velocity` signal. Fly-scan `prepare()` → move-to-run-up-start sequences will appear instant and tests will not catch timing bugs.

**Fix sketch:** Add `velocity: SoftSignalBackend<f64>` and `acceleration_time: SoftSignalBackend<f64>` fields to `SoftMotor`. When `velocity == 0.0`, keep instant semantics; otherwise simulate trapezoidal profile via `tokio::time::sleep` steps, updating the readback watch channel. Emit WatcherUpdate stream (requires DB-06). Effort: **M**.

---

### DB-14 · No EPICS Motor device

**bsrs:** No `CaMotor` or `PvaMotor` struct exists.

**ref:** `daq/ophyd-async/src/ophyd_async/epics/motor.py:109-324` — `Motor` wraps PV signals:
- `user_readback` (`.RBV`), `user_setpoint` (`.VAL`)
- `velocity` (`.VELO`), `max_velocity` (`.VMAX`), `acceleration_time` (`.ACCL`)
- `motor_egu` (`.EGU`), `precision` (`.PREC`), `deadband` (`.RDBD`)
- `low_limit_travel`/`high_limit_travel`, `dial_low_limit_travel`/`dial_high_limit_travel`
- `motor_done_move` (`.DMOV`), `motor_stop` (`.STOP`, wait=False)
- `set_use_switch` (`.SET`), `offset_freeze_switch` (`.FOFF`)
- `check_motor_limit()` before moves and fly scans
- `set()` computes `timeout = |Δpos / vel| + 2*accel + DEFAULT_TIMEOUT`, subscribes readback for WatcherUpdate
- Fly scan: `prepare(FlyMotorInfo)`, `kickoff()`, `complete()`
- `subscribe_reading()` — `Subscribable<f64>` impl

**Gap:** The most commonly used motor at an EPICS beamline has no Rust implementation. Plans that call `motor.set(10.0)` cannot connect to a real EPICS motor record.

**Fix sketch:** Add `struct EpicsMotor` to `bsrs-host/src/ca_devices.rs` or a new `bsrs-host/src/motor.rs`. Wire each PV suffix to an `EpicsCaBackend<T>` signal. Implement `AsyncMovable`, `Locatable`, `Stoppable`, `AsyncReadable`, `AsyncConfigurable`, and (for fly) `Flyable`/`Preparable`. Effort: **L**.

---

### DB-15 · `StandardDetector.trigger()` lacks implicit prepare and watchable progress

**bsrs:** `crates/bsrs-devices/src/detector.rs:99-112` — calls `arm()`, `arm.await`, `wait_for_idle()` then returns `Status::done()`. No implicit prepare, no per-frame progress.

**ref:** `daq/ophyd-async/src/ophyd_async/core/_detector.py:586-644` — `trigger()` is `@WatchableAsyncStatus.wrap`; if `_prepare_ctx` is None it calls `prepare(TriggerInfo())` first; then calls `_arm_logic.arm()` and watches `collections_written_signal` for WatcherUpdate.

**Gap:** Bsrs `trigger()` requires a prior `prepare()` call (no implicit fallback). More critically it never emits progress, so bluesky's RunEngine has no feedback during long exposures. Two root issues: (a) missing prepare-ctx state machine (DB-04 is a symptom), (b) missing `WatchableAsyncStatus` (DB-06).

**Fix sketch:** After DB-04 and DB-06 are addressed: add a `prepare_ctx: Mutex<Option<PrepareCtx>>` field to `StandardDetector`; `prepare()` populates it; `trigger()` checks it and calls `prepare(TriggerInfo::default())` if None. Effort: **S** (after DB-04 and DB-06).

---

### DB-16 · `StandardDetector.complete()` returns plain `Status` — no frame-progress watch

**bsrs:** `crates/bsrs-devices/src/detector.rs:123-145` — `complete()` polls `observe_indices_written()` and then calls `wait_for_idle()`, returning `Status::done()`. No WatcherUpdate emitted.

**ref:** `daq/ophyd-async/src/ophyd_async/core/_detector.py:681-691` — `complete()` is `@WatchableAsyncStatus.wrap` and yields `WatcherUpdate(current=frames_written, target=frames_requested)`.

**Gap:** Fly scan progress ("4 / 100 frames") is invisible. Requires DB-06.

**Fix sketch:** After DB-06: wrap the polling loop in a `WatchableStatus` and yield a `WatcherUpdate` on each `collections_written` change. Effort: **S** (after DB-06).

---

### DB-17 · No `FlyMotorInfo` concept

**bsrs:** No struct for fly-scan motor parameters exists.

**ref:** `daq/ophyd-async/src/ophyd_async/core/_utils.py` (not read fully) + used in `motor.py:209-253` and `sim/_motor.py:67-76`:
```python
@dataclass
class FlyMotorInfo:
    start_position: float
    end_position: float
    velocity: float
    timeout: float
    def ramp_up_start_pos(self, acceleration_time: float) -> float: ...
    def ramp_down_end_pos(self, acceleration_time: float) -> float: ...
```

**Gap:** Fly scan motor prepare (`prepare(FlyMotorInfo)`) requires knowing start position, end position, velocity, and timeout. These must be bundled and passed from the plan. Without this, `Preparable` for motors cannot be implemented with the correct signature. This also blocks DB-14.

**Fix sketch:** Add `pub struct FlyMotorInfo { pub start_position: f64, pub end_position: f64, pub velocity: f64, pub timeout: Duration }` with `ramp_up_start_pos(accel: Duration) -> f64` and `ramp_down_end_pos(accel: Duration) -> f64` methods to `bsrs-protocols-async`. Effort: **S**.

---

## P2 — Nice to have

---

### DB-18 · PVA `get_reading()` uses local clock

**bsrs:** `crates/bsrs-backends/epics-pva/src/real.rs:191-203` — `get_reading()` calls `now_ts()`. The server returns an NTScalar body with a `timeStamp` substructure in the `pvget` response, but `pv_field_to_ts()` is only called in `set_callback()`.

**ref:** Python `CaSignalBackend._make_reading()` receives a `FORMAT_TIME` value that includes server timestamp.

**Gap:** A GET call to an NTScalar PV returns the timestamp in the body. Bsrs discards it in `get_reading()`. This matters when comparing event timestamps across multiple PVs in a reading.

**Fix sketch:** Call `pv_field_to_ts(&f).unwrap_or_else(now_ts)` in `get_reading()` instead of unconditional `now_ts()`. Effort: **S**.

---

### DB-19 · No `AreaDetector` generic composite / `NDSimDetector` packaged type

**bsrs:** `crates/bsrs-host/src/areadetector.rs` provides low-level `AreaDetectorCam` and `NdFile*` helpers, but no composited `AreaDetector<D>` generic type that wires cam + arm_logic + trigger_logic + writer into a `StandardDetector`.

**ref:** `daq/ophyd-async/src/ophyd_async/epics/adcore/_detector.py:18-57` — `AreaDetector<ADBaseIOT>` accepts a driver, arm_logic, trigger_logic, path_provider, and writer_type and calls `add_detector_logics`.

**Gap:** Users must hand-wire the cam-to-StandardDetector connection themselves. Without a reusable `AreaDetector` abstraction, each new detector type requires duplicating the wiring.

**Fix sketch:** After DB-20 (logic composition), implement `struct AreaDetector<D>` in `bsrs-host` that accepts a driver handle, an arm-logic enum, a trigger-logic enum, and a `PathProvider`, and builds a `StandardDetector`. Effort: **L** (depends on DB-20).

---

### DB-20 · `StandardDetector` architecture: no logic-composition split

**bsrs:** `StandardDetector<C, W>` is a fixed `C: DetectorControl + W: DetectorWriter` template. Logic for trigger mode selection, arm/disarm lifecycle, and data path are bundled into these two monolithic traits.

**ref:** `daq/ophyd-async/src/ophyd_async/core/_detector.py:209-400` — three separate abstractions:
- `DetectorTriggerLogic` — `prepare_internal/edge/level`, `get_deadtime`, `config_sigs`
- `DetectorArmLogic` — `arm()`, `wait_for_idle()`, `disarm(on_unstage: bool)`
- `DetectorDataLogic` — `prepare_single` vs `prepare_unbounded`, `get_hinted_fields`, `stop`

**Gap:** Without this split, a real areaDetector must pack trigger-mode PV writes, arm, wait-for-idle, and HDF-writer open/close into one `DetectorControl::prepare + arm + wait_for_idle + disarm` sequence. This conflates concerns: the areaDetector arm logic (write `Acquire=1`, poll `DetectorState_RBV=Idle`) is independent of the trigger mode (set `TriggerMode`, `NumImages`) and of the data logic (open HDF file, set path). The current design makes reuse across different detector models (SimDetector, Pilatus, Kinetics) harder.

**Fix sketch:** Introduce `DetectorArmLogic` and `DetectorDataLogic` traits in `bsrs-protocols-async`. Refactor `DetectorControl` into `DetectorTriggerLogic + DetectorArmLogic`. Update `StandardDetector` to hold `Vec<Box<dyn DetectorDataLogic>>` alongside `DetectorArmLogic` and `DetectorTriggerLogic`. Existing `SoftDetectorControl` becomes a combined `SoftTriggerLogic + SoftArmLogic`. Effort: **L**.

---

## What bsrs already matches

- **`SignalBackend` trait shape** matches `ophyd_async.core.SignalBackend` (connect/put/get_datakey/get_reading/get_value/get_setpoint/set_callback/source). P0 parity held.
- **CA `f64`, `i64`, `bool`, `String` backends** — correct wire encoding with native-type dispatch (`f64_to_wire`, `i64_to_wire`). Native-width mismatch bug previously fixed.
- **PVA `f64`, `i64`, `bool`, `String` backends** — NTScalar structure traversal and timestamp extraction correct.
- **`SoftSignalBackend`** — in-memory state, RAII subscription tokens, correct callback fan-out.
- **`StreamResource`/`StreamDatum`** emission shape in `SoftDetectorWriter` matches event-model schema.
- **`CaStringKind::Long`** (DBR_CHAR waveform) — handles areaDetector `FilePath`/`FileName` correctly.
- **`Stageable`/`Triggerable`/`Flyable`/`WritesStreamAssets`** protocol traits present and delegated in `StandardDetector`.
