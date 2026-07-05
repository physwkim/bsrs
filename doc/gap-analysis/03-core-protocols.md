# Gap Analysis 03 — Core Signal/Device + Async/Sync Protocols + Derive Macro

**bsrs scope:** `crates/bsrs-core/src/`, `crates/bsrs-devices/src/`,
`crates/bsrs-protocols-async/src/`, `crates/bsrs-protocols-sync/src/`,
`crates/bsrs-derive/src/`

**Reference:** `daq/ophyd-async/src/ophyd_async/core/`
(`_signal.py`, `_signal_backend.py`, `_soft_signal_backend.py`,
`_mock_signal_backend.py`, `_mock_signal_utils.py`, `_device.py`,
`_readable.py`, `_status.py`, `_protocol.py`, `_derived_signal*.py`)

**Date:** 2026-06-14

---

## P0 — Correctness / Protocol Divergence / Commonly-Used Feature Entirely Missing

### CP-01 · No access-role split: SignalR / SignalW / SignalRW / SignalX are all one type — **DONE**

**bsrs:** `crates/bsrs-devices/src/signal.rs:34` — `Signal<T, B>` is a
single struct with both `get()` / `read()` and `put()` / `put_no_wait()` methods.
Every signal is simultaneously readable and writable regardless of intent.

**ref:** `_signal.py:189,276,305,317` — four distinct classes enforced at the
type level:

- `SignalR(Device, AsyncReadable, AsyncStageable, Subscribable)` — read / monitor only
- `SignalW(Device, Movable)` — write only
- `SignalRW(SignalR, SignalW, Locatable)` — read + write + locate
- `SignalX(Signal)` — trigger: calls `backend.put(None)` to execute

Without this split bsrs cannot express "this PV is write-only" or "this
PV is read-only", and `SignalX` (see CP-02) is entirely absent.

**Gap:** Whole access-role taxonomy missing; any signal can be read and written.

**Fix sketch:** Introduce marker traits or type-state generics on `Signal<T,B>`
(`PhantomData<Access>` with unit types `Read`, `Write`, `ReadWrite`, `Execute`)
plus alias types `SignalR<T,B>`, `SignalW<T,B>`, `SignalRW<T,B>`, `SignalX<B>`.
Alternatively, keep the single struct but add an `Access` enum field and derive
the blanket protocol-trait impls conditionally. The `#[derive(Device)]` macro
then parses `#[signal(ro, …)]` / `#[signal(rw, …)]` / `#[signal(wo, …)]` /
`#[signal(x, …)]` and emits the correct alias.

**Effort:** M

**Resolution:** The type-level access-role split exists. `crates/bsrs/src/devices/signal.rs:28-69`
defines a sealed `access` module with unit markers `Read` / `Write` / `ReadWrite` / `Execute`
and the `Readable` / `Writable` sub-traits; `Signal<T, B, A = ReadWrite>` (signal.rs:98) gates its
method surface on `A` — `get`/`read`/`subscribe`/`stage` live in the `A: Readable` impl
(signal.rs:156-236), `put` in the `A: Writable` impl (signal.rs:240-256), so a `SignalR::put`
or `SignalW::get` fails to compile (test signal.rs:442-473). The aliases `SignalR` / `SignalW` /
`SignalRW` / `SignalX` are exported at signal.rs:73-81. (Minor unclosed item: the
`#[derive(Device)]` role-emission from `#[signal(ro/wo/x, …)]` is not wired — the derive parses
only the PV template and `kind`, and the role is carried by the field's type annotation, not the
attribute; see `crates/bsrs-derive/src/lib.rs:87-120`.)

---

### CP-02 · SignalX (executable signal) entirely absent — **DONE**

**bsrs:** No SignalX anywhere in `crates/bsrs-*`.

**ref:** `_signal.py:317-331` —

```python
class SignalX(Signal):
    @AsyncStatus.wrap
    async def trigger(self, timeout=CALCULATE_TIMEOUT):
        await _wait_for(self._connector.backend.put(None), timeout, source)
```

`SignalX` is used for EPICS process-record triggers, reset buttons, and
acquire-start actions — one of the most common device patterns.

**Gap:** No way to define an executable "push button" signal; `Triggerable` at
the device level is a different concept (it represents the whole device trigger,
not a single named PV).

**Fix sketch:** Add `SignalX<B>` (or the type-state alias from CP-01) that
implements the `Triggerable` protocol trait (from `bsrs-protocols-async`) by
calling `backend.put(default_value, false, timeout)` where `default_value` is the
zero/default value of `T`, matching `backend.put(None)` semantics.  Also requires
CP-03 (put-None semantics in the backend trait).

**Effort:** S (depends on CP-01 and CP-03)

**Resolution:** `SignalX<T, B> = Signal<T, B, Execute>` (`crates/bsrs/src/devices/signal.rs:81`).
The `Execute`-only inherent impl gives it `trigger()`, which writes the backend default via
`backend.put(None)` (signal.rs:276-291), and it implements the `Triggerable` protocol trait the
same way (signal.rs:293-305). Backed by CP-11's put-None semantics: `SoftSignalBackend::put(None)`
writes the configured initial value (`crates/bsrs/src/backends/soft/signal.rs:162-165`). Tested at
signal.rs:478-506.

---

### CP-03 · StandardReadable + StandardReadableFormat absent — **DONE**

**bsrs:** `crates/bsrs-core/src/kind.rs:6` has `Kind { Normal, Config,
Hinted, Omitted }` for document routing.  `crates/bsrs-devices/src/signal.rs:
194-212` uses `Kind::Hinted` to populate `hint_fields()`.  But there is no
`StandardReadable` type that:
- aggregates signals into read / read_configuration / stage / hints buckets
- provides `add_readables(devices, format)` / `add_children_as_readables(format)`

**ref:** `_readable.py:83-288` — `StandardReadable(Device, AsyncReadable,
AsyncConfigurable, AsyncStageable, HasHints)` with five tuple accumulator fields
(`_read_funcs`, `_read_config_funcs`, `_describe_funcs`, `_describe_config_funcs`,
`_stageables`) and `StandardReadableFormat` enum:

| Format | Contributes to |
|---|---|
| `CHILD` | read/config/stage/hints — auto-detects what the child supports |
| `CONFIG_SIGNAL` | `read_configuration` + `describe_configuration` |
| `HINTED_SIGNAL` | `read` + `describe` + `stage` + hints |
| `UNCACHED_SIGNAL` | `read` (uncached, bypasses monitor) + `describe` |
| `HINTED_UNCACHED_SIGNAL` | `read` (uncached) + `describe` + hints |

Without `StandardReadable`, every detector/device must hand-implement
`AsyncReadable`, `AsyncConfigurable`, and `AsyncStageable` by routing signals
manually.

**Gap:** Core compositional device pattern entirely absent.

**Fix sketch:** Add `StandardReadable` struct in `bsrs-devices` that holds
`Vec<Box<dyn AsyncReadable>>` / `Vec<Box<dyn AsyncConfigurable>>` / etc.
accumulators, implement `AsyncReadable` + `AsyncConfigurable` + `AsyncStageable`
on it, and add `add_readables(devices, format: StandardReadableFormat)`.  The
`#[derive(Device)]` macro can emit `add_readables` calls for fields tagged with
`kind = config | hinted | …` if the struct also derives/embeds `StandardReadable`.

**Effort:** M

**Resolution:** `StandardReadable` (`crates/bsrs/src/devices/standard_readable.rs:46-108`) holds
`read` / `config` / `stageables` / `hints` accumulators; `add_readables(child, format)`
(standard_readable.rs:75-94) routes a child to its buckets, and `add_stageable`
(standard_readable.rs:100-102) registers staging. It implements `AsyncReadable` /
`AsyncConfigurable` / `Stageable` plus the engine-facing `ReadableObj` / `ConfigurableObj` /
`StageableObj` bridges by delegating to its children (standard_readable.rs:112-241). The
`StandardReadableFormat` enum (standard_readable.rs:24-38) has all five variants
(`Child` / `ConfigSignal` / `HintedSignal` / `UncachedSignal` / `HintedUncachedSignal`). Note: the
`Uncached*` variants currently route identically to their cached counterparts (add_readables
treats `UncachedSignal` like a plain read; comment at standard_readable.rs:33-34) — the uncached
read path is not yet distinguished now that CP-08's cache exists. Tested at
standard_readable.rs:265-308.

---

### CP-04 · connect(mock=True) / MockSignalBackend / mock mode — zero testing surface — **DONE**

**bsrs:** No `MockSignalBackend`, no `SoftSignalBackend`, no `connect_all(mock=true)`,
no `set_mock_value`, no `get_mock_put`, no `callback_on_mock_put`.
`crates/bsrs-core/`, `crates/bsrs-devices/`, `crates/bsrs-protocols-async/`
contain zero mock-related code.

**ref:**
- `_mock_signal_backend.py:26-116` — `MockSignalBackend` wraps a `SoftSignalBackend`,
  tracks put calls via `AsyncMock`, exposes `put_proceeds: asyncio.Event` to block puts
- `_mock_signal_utils.py:32-157` — `set_mock_value`, `set_mock_values`, `get_mock_put`,
  `callback_on_mock_put`, `mock_puts_blocked`
- `_device.py:222-256` — `Device.connect(mock=True)` uses `SignalConnector.connect_mock`
  to swap backends

Without mock mode, unit-testing devices requires a running EPICS IOC (CA) or PVA
server for every test; the entire ophyd-async test pattern is unavailable.

**Gap:** Complete absence of the testing surface — this is a fundamental
development-workflow gap.

**Fix sketch:** (a) Add `SoftSignalBackend<T>` to `bsrs-devices`: an in-process
backend backed by `tokio::sync::watch::Sender<T>` that satisfies `SignalBackend<T>`
with no I/O.  (b) Add `MockSignalBackend<T>` wrapping `SoftSignalBackend<T>` +
an `Arc<Mutex<Vec<T>>>` put-history.  (c) Add `connect_mock(timeout)` to
`SignalBackend<T>` as a default method that swaps to `MockSignalBackend`.
(d) Add module-level test helpers mirroring `set_mock_value`, `get_mock_puts`,
`callback_on_mock_put`.

**Effort:** M

**Resolution:** Both backends exist. `SoftSignalBackend<T>`
(`crates/bsrs/src/backends/soft/signal.rs:42`, see CP-10) is the in-process store;
`MockSignalBackend<T>` (`crates/bsrs/src/backends/mock/mock_signal.rs:37`) wraps it and adds the
ophyd-async testing surface: `set_value` (= `set_mock_value`, mock_signal.rs:82), `put_calls` /
`put_count` (= `get_mock_put`, mock_signal.rs:100-107), `set_put_callback` (= `callback_on_mock_put`,
mock_signal.rs:94), `set_put_proceeds` + `mock_puts_blocked` RAII guard (mock_signal.rs:88-116),
and a `put` that records the arg, applies the callback, writes through the soft backend, and gates
completion on the proceeds flag (mock_signal.rs:161-175). There is no runtime `connect(mock=true)`
backend swap (bsrs fixes the backend type on the `Signal` at compile time); a device is built over
the mock backend directly instead. Tested mock_signal.rs:201-259. A trivial fixed-value
`MockBackend` also exists (mock/mod.rs:20-74).

---

## P1 — Meaningful Completeness Gap

### CP-05 · WatchableAsyncStatus + Watcher protocol missing — **DONE**

**bsrs:** `crates/bsrs-core/src/status.rs:44` — `Inner.progress:
watch::Sender<f64>` and `Status::watch() -> watch::Receiver<f64>`.  Only a single
scalar fraction is observable; no structured update.

**ref:** `_status.py:189-258` — `WatchableAsyncStatus` wraps an async iterator
of `WatcherUpdate[T]` (dataclass: `current, initial, target, unit, precision,
fraction, time_elapsed, time_remaining`).  `watch(watcher: Watcher)` calls the
watcher immediately with the last update and on every subsequent one.  `Watcher`
(`_protocol.py:124-138`) is the structured callback protocol used by bluesky's
`LiveTable` / `LivePlot` and the RE progress bar.

**Gap:** Moving devices cannot report structured progress (no `initial/target` for
ETA computation, no `unit/precision` for display).

**Fix sketch:** Add `WatcherUpdate<T>` struct to `bsrs-core` and a
`WatchableStatus<T>` newtype (or extend `Status`) that holds
`watch::Sender<WatcherUpdate<T>>`.  Add `StatusSetter::update_progress(WatcherUpdate<T>)`.
Also add `Watcher` as a trait or function-pointer type to `bsrs-protocols-async`.

**Effort:** S

**Resolution:** `WatcherUpdate<T = f64>` (`crates/bsrs/src/core/status.rs:49-86`) carries
`current` / `initial` / `target` / `name` / `unit` / `precision` / `fraction` / `time_elapsed` /
`time_remaining`. The `Watcher` trait (status.rs:93-96, re-exported from `protocols_async`) is the
structured-callback sink. `Status` holds a `watch::Sender<Option<WatcherUpdate>>` (status.rs:104);
`StatusSetter::update_watcher` posts an update (back-filling `time_elapsed`, status.rs:430-438) and
`Status::observe_watcher` drives a `Watcher` immediately with the last update then on every change
until completion (status.rs:340-371). Tested status.rs:570-616, 737-780.

---

### CP-06 · Device hierarchy: parent / children / set_name propagation — **DONE** (naming) · remainder **OUT-OF-SCOPE**

**bsrs:** `#[derive(Device)]` (`crates/bsrs-derive/src/lib.rs:36`) generates
`name() -> &str` returning the stored prefix string.  No `parent` field, no
`children()` iterator, no name propagation to child devices.

**ref:** `_device.py:129-282` —

- `Device.parent: Device | None` — set by `__setattr__` when a Device is assigned as a field
- `Device.children() -> Iterator[(str, Device)]` — yield named child devices
- `Device.set_name(name, child_name_separator="-")` — recursively propagates
  `{name}-{child_attr}` names so `t1x = Motor("BL:T1X")` after `set_name("t1x")`
  yields `t1x.setpoint.name == "t1x-setpoint"`, enabling bluesky's naming convention

Without name propagation, sub-devices and signals don't get stable bluesky names;
`describe()` returns keys like `"/BL:T1X:RBV"` instead of `"t1x-readback"`.

**Gap:** Every derived device must manually set names; `init_devices` / RE name
inference are impossible.

**Fix sketch:** In the `#[derive(Device)]` macro, emit `set_name(name: &str)`
that walks `#[device(...)]` and `#[signal(...)]` fields and calls
`child.set_name(&format!("{name}-{field_name}"))` on each.  Add `parent:
Option<Arc<dyn Any + Send + Sync>>` or a weak-ref field for the parent link.

**Effort:** M

**Status (PARTIAL):** Name propagation — the headline capability — is implemented. The
`#[derive(Device)]` macro emits `new_named(prefix, dev_name)`
(`crates/bsrs-derive/src/lib.rs:193-195`) that names each `#[signal]` field
`{dev_name}-{field}` (lib.rs:101-112) and recursively calls `Sub::new_named` on each `#[device]`
sub-device (lib.rs:127-132), so a motor built with `new_named("BL:T1X", "t1x")` yields
`setpoint.name == "t1x-setpoint"` — the bluesky naming convention. bsrs devices are immutable
`Arc<Self>`, so names are fixed at construction rather than mutated via a post-hoc `set_name`.
Still missing: (1) no `parent` back-link field on any device; (2) no uniform `children()` iterator
over an arbitrary `Device`'s sub-devices/signals — `children()` exists only on `DeviceVector`
(`crates/bsrs/src/devices/device.rs:165`), not on the general `Device` trait (device.rs:22-41,
which exposes only `name` / `connect_all_boxed` / `walk_signal_sources`).

**Scope (2026-07-05):** The remaining two pieces are device-model navigation API, not
document emission or qs wire protocol, and are therefore **OUT-OF-SCOPE** for the parity
effort (bsrs owns document-emission + qs-wire correctness only; consumer-side / device-model
navigation is out — same class as DB-14/15/16/17, CBEM-04/05/12). Evidence: `rg '\bparent\b'`
across the crate finds no device `parent` in document emission — only `Resource.parent` /
`StreamResource.parent` UIDs (an unrelated resource concept) and `checkpoint_store` /
`areadetector` path/XML uses; `plans/mod.rs:153` states outright "bsrs has no device
parent/child hierarchy." Descriptor / event assembly is driven by `StandardReadable`'s
explicit child buckets + `walk_signal_sources`, never a general `Device::children()`. The
document-emission-relevant capability of CP-06 — name propagation into descriptor `data_keys`
(`t1x-setpoint`) — is **DONE**; the parent/children remainder does not change any emitted
document.

---

### CP-07 · DeviceVector absent — **DONE**

**bsrs:** No DeviceVector anywhere in bsrs.

**ref:** `_device.py:285-330` — `DeviceVector(MutableMapping[int, DeviceT], Device)`
is an integer-keyed mutable mapping of child devices that participates in
`children()` iteration and therefore in `connect()`, `set_name()`, staging, etc.
Used for e.g. 8 cameras indexed 1-8 on a beamline.

**Gap:** Arrays of identical sub-devices cannot be expressed; users must hand-roll
them or use `Vec` with no Device-tree integration.

**Fix sketch:** Add `DeviceVector<T>` struct in `bsrs-devices` implementing
`IndexMap<u32, T>` (or `BTreeMap`) plus a `children(&self) -> impl Iterator<(String, &dyn ...)>`
that yields `("1", child1), ("2", child2), ...`.  Wire into `connect_all` and
`set_name` via the derive macro's CHILD arm.

**Effort:** M

**Resolution:** `DeviceVector<D>` (`crates/bsrs/src/devices/device.rs:83-181`) is a `BTreeMap<u32, D>`
of child devices with `insert` / `get` / `iter` / `values` / `Index<u32>` and a bluesky-style
`children()` yielding `("1", &child)`, `("2", &child)`, … in ascending key order (device.rs:165-167).
For `D: Device` it participates in the device tree via `connect_all(timeout)`, which connects every
child concurrently (device.rs:170-180). Tested device.rs:215-240.

---

### CP-08 · Signal caching layer (_SignalCache / staged flag / read(cached)) absent — **DONE**

**bsrs:** `crates/bsrs-devices/src/signal.rs:84` — `Signal::read()` always
calls `backend.get_reading()`.  `subscribe()` creates a fresh `watch::channel` and
callback each call.  No concept of "this signal is staged; keep its monitor alive".

**ref:** `_signal.py:116-186` — `_SignalCache` is created when the first
subscriber arrives or `stage()` is called.  It:
- Fires `backend.set_callback(self._callback)` once and demultiplexes to N `_listeners`
- Tracks `_staged: bool` separately from listener count so the cache outlives the
  last subscriber while staged
- Provides `get_reading() / get_value()` with an `asyncio.Event` guard so the
  first poll waits for at least one callback
- Enables `read(cached=None/True/False)` to choose between network round-trip and
  cached value

**Gap:** Multi-subscriber patterns share no common backend callback; every subscriber
creates a new CA/PVA monitor.  Stage semantics don't persist the subscription.

**Fix sketch:** Add `SignalCache<T>` in `bsrs-devices` wrapping the existing
`watch::channel`; extract the `subscribe()` body into it.  The `Signal` holds
`Option<Arc<SignalCache<T>>>` initialised lazily.  `stage()` increments a counter;
`unstage()` decrements and tears down if zero listeners remain.  `read()` gains a
`cached: Option<bool>` parameter.

**Effort:** M

**Resolution:** `SignalCache<T, B>` (`crates/bsrs/src/devices/signal_cache.rs:49-178`) fires
`backend.set_callback` exactly once and fans updates to N listeners over a `watch` channel; its
documented invariant is "backend monitor alive ⟺ `staged || listeners > 0`" (signal_cache.rs:40-46),
enforced by `ensure_token` / `maybe_teardown` under the state lock (signal_cache.rs:99-152). A
`Signal` lazily holds one cache (`crates/bsrs/src/devices/signal.rs:107,168-176`); `read_cached(cached)`
implements ophyd's `read(cached=)` — `Some(false)` hits the backend, `Some(true)`/`None` return the
cached value when present (signal.rs:196-207); `stage()` / `unstage()` hold and release the monitor
(signal.rs:210-218), and `subscribe()` demultiplexes the one shared monitor (signal.rs:335-343).
Tested signal.rs:511-558 and signal_cache.rs:193-272.

---

### CP-09 · observe_value / wait_for_value helpers absent — **DONE**

**bsrs:** No equivalent of `observe_value`, `observe_signals_value`, or
`wait_for_value` anywhere in bsrs.  Users can manually watch a `Subscription`
channel but have no standard combinator.

**ref:** `_signal.py:380-580` —

- `observe_value(signal, timeout, done_status, done_timeout)` — async generator
  yielding each new signal value; exits when `done_status` completes
- `observe_signals_value(*signals)` — same for N signals, yielding `(signal, value)` pairs
- `wait_for_value(signal, match, timeout)` — waits until the signal equals/satisfies `match`
- `set_and_wait_for_value / set_and_wait_for_other_value` — set + concurrent monitor pattern

These are used in virtually every detector driver (`wait_for_value(self.acquiring, 1)`).

**Gap:** Detector and motor drivers cannot poll until a condition is met without
hand-rolling the subscribe + queue + timeout loop every time.

**Fix sketch:** Add `observe_value<T>(sub: &mut Subscription, done: Option<&Status>)
-> impl Stream<Item=T>` and `wait_for_value<T>(signal, pred, timeout) -> impl Future`
as standalone async functions in `bsrs-devices` or a new `bsrs-plans` module.

**Effort:** S

**Resolution:** `crates/bsrs/src/devices/observe.rs` provides `observe_value(sub) -> impl Stream`
(observe.rs:28-45, current value first then every change), `observe_signals_value(subs)` merging N
subscriptions tagged by input index (observe.rs:63-72), and `wait_for_value(sub, predicate, timeout)`
returning the first matching reading or `BsrsError::Timeout` (observe.rs:80-113). These operate on
JSON-erased `ReadingValue`; `timeout`/`done_status` compose at the call site (tokio) rather than as
parameters. Re-exported at devices/mod.rs:16. Tested observe.rs:131-240. (The `set_and_wait_for_value`
/ `set_and_wait_for_other_value` convenience wrappers are not standalone helpers — that set+monitor
pattern is inlined in the areadetector driver, e.g. `crates/bsrs/src/host/areadetector.rs:2390`.)

---

### CP-10 · SoftSignalBackend absent — **DONE**

**bsrs:** No in-process, non-I/O `SignalBackend<T>` implementation.  Building an
internal state signal (e.g. `acquiring: Signal<bool, _>`) requires either an EPICS
backend or a full custom `SignalBackend<T>` impl.

**ref:** `_soft_signal_backend.py:117-187` — `SoftSignalBackend<T>` uses a
`Reading` dict as in-memory state, `set_value(v)` fires the registered callback
immediately, `connect()` is a no-op, `source()` returns `"soft://{name}"`.  Used
directly by device code and as the backing store for `MockSignalBackend`.

**Gap:** Internal/soft signals are verbose to implement; mock mode (CP-04) depends
on this.

**Fix sketch:** `SoftSignalBackend<T>` in `bsrs-devices`: holds
`(watch::Sender<TypedReading<T>>, Arc<Mutex<T>>)`, `set_value(v)` sends through
the channel and fires the stored callback, `connect()` → `Ok(())`.

**Effort:** S

**Resolution:** `SoftSignalBackend<T>` (`crates/bsrs/src/backends/soft/signal.rs:42-234`) stores the
value/setpoint/timestamp in-memory, `connect()` is a no-op (signal.rs:159), `put(Some(v))` writes and
fires the registered callbacks (signal.rs:162-182), `put(None)` writes the configured `initial`
(signal.rs:165), `get_reading()` returns the value's last-change timestamp rather than a fresh
read-time stamp (signal.rs:195-207), and `source()` returns `"soft://{name}"` (signal.rs:231). It
also backs `MockSignalBackend` (CP-04). Tested signal.rs:244-289.

---

### CP-11 · SignalBackend::put takes non-None T only; no put-default semantics — **DONE**

**bsrs:** `crates/bsrs-protocols-async/src/lib.rs:38` —
`async fn put(&self, value: T, wait: bool, timeout: Option<Duration>) -> Status`

**ref:** `_signal_backend.py:82` —
`async def put(self, value: SignalDatatypeT | None)` — `None` means "put the
signal's default/initial value", used by `SignalX.trigger()`.  The `wait` and
`timeout` parameters live on the _Signal_ layer (`SignalW.set(timeout=...)`), not
the backend.

**Gap (two aspects):**
1. `None` (put-default) cannot be expressed; SignalX (CP-02) requires it.
2. `wait` / `timeout` on the backend conflate transport semantics with signal
   policy — backends must know about timeout even when they shouldn't.

**Fix sketch:** Change the backend trait to `async fn put(&self, value: Option<T>)
-> Result<()>` (no `wait`/`timeout` — those are Signal-layer policy).  Give the
`Signal<T,B>` wrapper a `put(value: T, timeout: Option<Duration>)` method that
wraps the result in a `Status` and applies the timeout.  `SignalX` calls
`backend.put(None)`.

**Effort:** S (breaking change to the backend trait)

**Resolution:** The backend trait now reads
`async fn put(&self, value: Option<T>) -> Result<()>` with no `wait`/`timeout`
(`crates/bsrs/src/protocols_async/mod.rs:46`): `None` is the put-default sentinel, waiting-for-completion
is implicit, and any timeout lives on the `Signal` layer. `Signal::put` (writable roles) wraps the
result in a `Status` (`crates/bsrs/src/devices/signal.rs:250-255`); `SignalX::trigger` calls
`backend.put(None)` (signal.rs:285-290). Every backend implements the new shape (soft signal.rs:162,
mock mock_signal.rs:161, plus the CA/PVA backends). Boundary-tested at
backends/soft/signal.rs:244-260.

---

### CP-12 · HasHints trait not formalized — **DONE** (document hints) · trait formalization **OUT-OF-SCOPE**

**bsrs:** `crates/bsrs-core/src/msg.rs:562` — `ReadableObj::hint_fields() ->
Option<Vec<String>>` returns hinted field names as strings.  No `Hints` struct,
no `HasHints` protocol trait.

**ref:** `bluesky.protocols.HasHints` — `hints: Hints` property where `Hints` is
`TypedDict` with `fields: list[str]`, `dimensions: ...` etc.  Used by LiveTable,
LivePlot, and the RE's `Hints` accumulation.

**Gap:** The `hints` shape is undocumented and untyped; downstream tooling
(CLI, LiveTable equivalent) cannot rely on a stable shape.

**Fix sketch:** Add `pub struct Hints { pub fields: Vec<String> }` to `bsrs-core`
and `trait HasHints { fn hints(&self) -> Hints; }` to `bsrs-protocols-async`.
Implement it on `Signal` when `kind == Kind::Hinted` and on `StandardReadable` (CP-03).

**Effort:** S

**Status (2026-07-05):** The document-emission-relevant hints capability is **DONE**.
`ReadableObj::hint_fields()` (on `Signal` at `signal.rs:387`, `Detector` at `detector.rs:402`,
and `StandardReadable` at `standard_readable.rs:208`) is collected by the RunEngine
(`run_engine.rs:2042`) and emitted into the descriptor's per-object hints
(`bundler.rs:231` → `Descriptor.hints: Option<HashMap<String, PerObjectHint>>`,
`event_model/documents.rs:397-399`) and the `RunStart.hints` dimensions
(`documents.rs:70-72`). A formalized `HasHints` protocol trait + `Hints` struct is
device-model / consumer API surface whose named ref consumers — LiveTable, LivePlot — are
out-of-scope live callbacks (per the parity-scope rule). The emitted document content is
already correct; the trait formalization is **OUT-OF-SCOPE** for the parity effort.

---

### CP-13 · SignalMetadata helper (make_datakey / limits / choices / units) absent — **DONE**

**bsrs:** `DataKey` construction is entirely left to each backend implementation.
No shared helper ensures that `limits`, `choices`, `precision`, `units` fields are
populated consistently across backends.

**ref:** `_signal_backend.py:180-211` — `make_datakey(datatype, value, source, metadata)`
and `make_metadata(datatype, units, precision)` compute `dtype`, `dtype_numpy`,
`shape` automatically from the Rust-side type and fill in the structured metadata.
`SignalMetadata(TypedDict)` with `limits, choices, precision, units` is the
canonical vocabulary.

**Gap:** Different backends produce `DataKey`s with inconsistently populated
optional fields; no compile-time vocabulary for `limits`, `choices`, etc.

**Fix sketch:** Add `SignalMetadata { limits: Option<Limits>, choices: Option<Vec<String>>,
precision: Option<u8>, units: Option<String> }` to `bsrs-core` and a
`fn make_datakey(source: &str, dtype: Dtype, shape: Vec<usize>, meta: SignalMetadata) -> DataKey`
constructor in `bsrs-event-model` or `bsrs-devices`.

**Effort:** S

**Resolution:** `SignalMetadata { limits, choices, precision, units }`
(`crates/bsrs/src/event_model/documents.rs:313-322`) is the canonical vocabulary, and
`make_datakey(source, dtype, shape, dtype_numpy, meta)` (documents.rs:332-352) builds a `DataKey`
from the transport-known shape plus that metadata, defaulting the six non-metadata fields
(`external` / `object_name` / `dims`) in one place so backends stop re-spelling them. `Limits` /
`LimitsRange` / `RdsRange` (documents.rs:196-235) and the `choices` field on `DataKey`
(documents.rs:298-301) supply the full vocabulary. Every backend routes through `make_datakey`
(soft signal.rs:183-193, mock mod.rs:45-52, CA/PVA backends). Tested documents.rs:694-752.

---

### CP-14 · AsyncStatus cancel context-manager semantics absent — **DONE**

**bsrs:** `crates/bsrs-core/src/status.rs` — `Status` is a `Future` returning
`Result<(), StatusError>`.  There is no cancellation path and no async context manager.

**ref:** `_status.py:110-121` — `AsyncStatusBase` implements `async __aenter__ /
__aexit__` so you can write:
```python
async with motor.set(pos) as status:
    async for v in observe_value(det, done_status=status):
        ...
# motor cancelled here if the body exits before the move completes
```
This ensures no dangling tasks after a scan ends early.

**Gap:** Bsrs `Status` cannot signal cancellation to the device that generated it;
long-running operations (motors) may not stop when a scan is aborted mid-move.

**Fix sketch:** Add `cancel()` to `StatusSetter` (sets a CANCELLED state distinct
from ERROR), and implement `Drop` or an RAII guard wrapper that calls `cancel()` on
exit.  Alternatively wrap `Status` in a newtype that `impl AsyncDrop` (nightly) or
uses `Drop` with a tokio oneshot channel.

**Effort:** M

**Resolution:** `Status` gained a distinct `CANCELLED` state (`crates/bsrs/src/core/status.rs:16`) and
a shared `CancellationToken` (status.rs:110). `Status::cancel()` signals the producer and transitions a
still-pending status to `CANCELLED` — its `Future` then resolves to `Err(StatusError::Cancelled)`
(status.rs:222-234, `Future::poll` at status.rs:472-492); it is idempotent and a no-op after
completion (test status.rs:674-683). The producer observes the request via
`StatusSetter::cancelled()` / `is_cancelled()` (status.rs:407-414) to abort in-flight work.
`Status::cancel_on_drop()` returns a `CancelGuard` (status.rs:239-241, 450-470) that cancels on
scope exit — the Rust analogue of ophyd-async's `async with status:`. Tested status.rs:633-719.

---

## P2 — Nice to Have

### CP-15 · DerivedSignal / DerivedSignalBackend absent

**bsrs:** No signal that computes its value as a transformation of other signals.

**ref:** `_derived_signal.py`, `_derived_signal_backend.py` — `DerivedSignalFactory`
with a pydantic `Transform` subclass performs many-to-many signal transformations
(e.g., `energy = 12.4 / wavelength`).

**Gap:** Compound/derived quantities require a full custom Device rather than a
simple transform.

**Fix sketch:** Add a `DerivedSignalBackend<T>` that holds a set of source
`Subscription`s and a `Fn(values...) -> T` transformer.  Lower priority than the
protocol gaps above.

**Effort:** L

---

### CP-16 · SignalBackend::source lacks read/write distinction flag — **DONE**

**bsrs:** `crates/bsrs-protocols-async/src/lib.rs:50` —
`fn source(&self, name: &str) -> String`

**ref:** `_signal_backend.py:70` — `def source(self, name: str, read: bool) -> str`

PVs with separate readback/setpoint (e.g. `motor.VAL` vs `motor.RBV`) need to
report different source strings for read vs write contexts.

**Fix sketch:** Add `read: bool` parameter to `SignalBackend::source`.  Update all
backend implementations (epics-ca, epics-pva) to pass through the flag.

**Effort:** S

**Resolution:** The trait method is now `fn source(&self, name: &str, read: bool) -> String`
(`crates/bsrs/src/protocols_async/mod.rs:57-62`; `read=true` = read-back URI, `read=false` = write
URI), and every backend implements the new signature — CA (`crates/bsrs/src/backends/epics_ca/real.rs:540`
and five more sites), PVA (`crates/bsrs/src/backends/epics_pva/real.rs:454` and four more), soft
(soft/signal.rs:231), mock (mock/mock_signal.rs:191). `Signal::source()` passes `read=true`
(devices/signal.rs:149-151). The API surface the gap named is closed; the CA/PVA/soft backends here
each carry a single PV per signal so they currently ignore the flag (`_read`), while the seam for a
distinct write URI now exists.

---

### CP-17 · soft_signal_rw / soft_signal_r_and_setter convenience factories absent

**bsrs:** No factory functions for soft signals.

**ref:** `_signal.py:334-378` — one-liners that build a `SoftSignalBackend` and
wrap it in `SignalRW` or `(SignalR, setter_fn)`.  Used throughout detector code for
internal state PVs.

**Fix sketch:** Add `soft_signal_rw<T>() -> SignalRW<T, SoftSignalBackend<T>>` and
`soft_signal_r_and_setter<T>() -> (SignalR<T, SoftSignalBackend<T>>, impl Fn(T))` after
CP-10 (SoftSignalBackend).

**Effort:** S (depends on CP-10 + CP-01)

---

### CP-18 · SignalW.set() retry on TimeoutError absent

**bsrs:** `crates/bsrs-devices/src/signal.rs:73` — `Signal::put()` makes one
attempt with no retry.

**ref:** `_signal.py:293-303` — `stamina.retry_context(on=asyncio.TimeoutError,
attempts=self._attempts)` wraps the put.  `attempts` defaults to 1 but can be set
to e.g. 3 for flaky CA connections.

**Fix sketch:** Add `attempts: u32` to `SignalConfig` (default 1); wrap the backend
`put()` call in a `for _ in 0..attempts` loop that retries on `StatusError::Timeout`.

**Effort:** S

---

### CP-19 · init_devices context manager absent

**bsrs:** No parallel-connect + auto-name context manager.

**ref:** `_device.py:406-448` — `init_devices(set_name, mock, timeout)` scans
locals before/after the context block, sets names from variable names, and connects
all in parallel.

**Fix sketch:** No direct equivalent needed if `set_name()` (CP-06) is added;
users can call `connect_all(timeout)` on each top-level device manually.  A macro
`init_devices!` that wraps the block and calls `connect_all` on all declared
devices would be ergonomic but is not blocking.

**Effort:** M

---

### CP-20 · walk_rw_signals / walk_devices / walk_signal_sources absent — **PARTIAL**

**bsrs:** No device-tree traversal utilities.

**ref:** `_signal.py:706-781` — `walk_devices(device)`, `walk_rw_signals(device)`,
`walk_signal_sources(device)` and `walk_config_signals(device)` for save/restore
and configuration introspection.

**Fix sketch:** Add to `bsrs-devices` after CP-06 (`children()` iterator): a
`walk_devices(root: &dyn Device)` that traverses `children()` recursively.

**Effort:** S (depends on CP-06)

**Status (PARTIAL):** `walk_signal_sources(root)` is implemented — it collects `(dotted_path, source)`
for every signal in a device tree (`crates/bsrs/src/devices/device.rs:67-71`), backed by a
`Device::walk_signal_sources` trait method the `#[derive(Device)]` macro emits from the same field
walk as `connect_all` (device.rs:26-41; `crates/bsrs-derive/src/lib.rs:116-142, 229-235`). Tested at
devices/device.rs via the derive. Still missing: `walk_devices`, `walk_rw_signals`, and
`walk_config_signals` — none exist (these need CP-06's general `children()` iterator and CP-01's
role marker on trait objects, which are not yet available for tree traversal).

---

## What Already Matches

- `Msg` enum (`crates/bsrs-core/src/msg.rs`) is comprehensive and closely tracks
  bluesky's command set including `Prepare`, `WaitFor`, `Subscribe/Unsubscribe`,
  `InstallSuspender`, `RegisterPausable`, etc.
- `AsyncReadable`, `AsyncMovable<T>`, `Triggerable`, `Stageable`, `Flyable`,
  `AsyncConfigurable`, `Locatable<T>`, `AsyncSubscribable<T>`, `Stoppable`, `Pausable`,
  `Preparable<V>`, `Collectable`, `WritesStreamAssets`, `DetectorControl`,
  `DetectorWriter` — protocol trait coverage is excellent.
- `Status` (`bsrs-core/status.rs`) covers `done`, `success`, `exception`,
  `progress`, `add_callback`, sync `wait(timeout)` — functional parity with
  `bluesky.protocols.Status` for the common cases.
- `Location<T>` / `locate()` exist as `Locatable<T>` in `bsrs-protocols-async`
  with a concrete `Location { setpoint, readback }` struct.
- `Kind { Normal, Config, Hinted, Omitted }` covers the document-routing semantics
  of ophyd's `kind` attribute.
- `bsrs-protocols-sync` blanket-impls sync wrappers via `block_on` — matches
  ophyd's sync Device facade.
- `#[derive(Device)]` generates `new(prefix)`, `connect_all(timeout)`, and
  `name()` from annotated structs — structural equivalent of ophyd-async's
  `DeviceConnector.create_children_from_annotations`.

---

## Priority Summary

| ID | Title | Priority | Effort |
|---|---|---|---|
| ~~CP-01~~ | ~~SignalR/W/RW/X access-role split absent~~ **DONE** | P0 | M |
| ~~CP-02~~ | ~~SignalX (executable signal) absent~~ **DONE** | P0 | S |
| ~~CP-03~~ | ~~StandardReadable + StandardReadableFormat absent~~ **DONE** | P0 | M |
| ~~CP-04~~ | ~~Mock mode / MockSignalBackend absent~~ **DONE** | P0 | M |
| ~~CP-05~~ | ~~WatchableAsyncStatus + Watcher protocol~~ **DONE** | P1 | S |
| ~~CP-06~~ | ~~Device set_name/naming propagation~~ **DONE**; parent/children **OUT-OF-SCOPE** | P1 | M |
| ~~CP-07~~ | ~~DeviceVector absent~~ **DONE** | P1 | M |
| ~~CP-08~~ | ~~Signal caching layer (_SignalCache / staged)~~ **DONE** | P1 | M |
| ~~CP-09~~ | ~~observe_value / wait_for_value helpers~~ **DONE** | P1 | S |
| ~~CP-10~~ | ~~SoftSignalBackend absent~~ **DONE** | P1 | S |
| ~~CP-11~~ | ~~backend.put(None) / put-default semantics~~ **DONE** | P1 | S |
| ~~CP-12~~ | ~~HasHints: document hints emitted~~ **DONE**; trait formalization **OUT-OF-SCOPE** | P1 | S |
| ~~CP-13~~ | ~~SignalMetadata / make_datakey helper~~ **DONE** | P1 | S |
| ~~CP-14~~ | ~~AsyncStatus cancel / context-manager~~ **DONE** | P1 | M |
| CP-15 | DerivedSignal absent | P2 | L |
| ~~CP-16~~ | ~~source(read: bool) flag absent~~ **DONE** | P2 | S |
| CP-17 | soft_signal_rw factories absent | P2 | S |
| CP-18 | set() retry on timeout absent | P2 | S |
| CP-19 | init_devices context manager absent | P2 | M |
| CP-20 | walk_devices / walk_rw_signals absent **PARTIAL** | P2 | S |

**Counts:** P0 = 4, P1 = 10, P2 = 6

**Reconciliation (as of 2026-07-05):** 13 DONE; plus CP-06 and CP-12, whose
document-emission-relevant parts are DONE (name propagation into descriptor `data_keys`;
per-object hints emitted into descriptor + `RunStart`) and whose remaining device-model API
pieces (device `parent` back-link + general `Device::children()`; the `HasHints` protocol
trait) are OUT-OF-SCOPE per the parity-scope rule (consumer-side / device-model navigation).
1 PARTIAL (CP-20). 4 OPEN (CP-15, CP-17, CP-18, CP-19). Source paths in the older entries reference the pre-consolidation
crate names (`bsrs-core`, `bsrs-devices`, …); the code now lives in the single `bsrs` crate under
`crates/bsrs/src/{core,devices,protocols_async,event_model}/` plus the companion `bsrs-derive` crate.
