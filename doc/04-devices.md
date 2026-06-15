# 04 — Devices, Signals, and the Dual API

## The 11 protocol traits

A direct port of `bluesky/protocols.py:36-526`. Three are sealed (see
[`01-architecture.md`](01-architecture.md)); the remaining eight may evolve.

| Async trait (primary) | Sync trait (facade) | Purpose |
|---|---|---|
| `AsyncReadable<T>` | `Readable<T>` | `read() / describe()` |
| `AsyncMovable<T>` | `Movable<T>` | `set(value) -> Status` |
| `Triggerable` | `Triggerable` | `trigger() -> Status` |
| `Flyable` | `Flyable` | `kickoff() / complete()` |
| `Stageable` | `Stageable` | `stage() / unstage()` |
| `AsyncConfigurable<T>` | `Configurable<T>` | slow-changing fields |
| `Locatable<T>` | `Locatable<T>` | `locate() -> Location { setpoint, readback }` |
| `Subscribable<T>` | `Subscribable<T>` | `subscribe(cb) -> SubToken` |
| `Preparable<V>` | `Preparable<V>` | `prepare(v) -> Status` |
| `Collectable` | `Collectable` | `describe_collect() / collect()` |
| `WritesStreamAssets` | `WritesStreamAssets` | `collect_asset_docs() / get_index()` |

The sync trait is implemented as a blanket impl over the async trait, calling
`bsrs_runtime().block_on()`. No double maintenance.

## How the dual API looks to a user

The same `Motor` device, two ways to use it:

```rust
// async (default)
use bsrs::ophyd_async::*;

let motor = Motor::new("BL10C:m1", &cfg).await?;
let pos = motor.read().await?;
motor.set(1.0).await?;
```

```rust
// sync (ophyd-style)
use bsrs::ophyd::*;

let motor = Motor::new("BL10C:m1", &cfg)?;          // sync construct
let pos = motor.read()?;
motor.set(1.0)?;
let status = motor.set(2.0)?;
status.add_callback(|outcome| { /* fired when done */ });
status.wait(Some(Duration::from_secs(5)))?;
```

Both compile to the same `Motor` struct. The difference is **only which trait set
the user pulled into scope**.

## `Status`

`Status` is dual-purpose by construction: it implements `Future` AND has ophyd-style
sync methods.

```rust
pub struct Status {
    inner: Arc<StatusInner>,
}

struct StatusInner {
    state:     AtomicU8,                        // 0=pending, 1=success, 2=error
    error:     Mutex<Option<StatusError>>,
    notify:    tokio::sync::Notify,
    progress:  watch::Sender<f64>,              // 0.0..=1.0
    callbacks: Mutex<Vec<Box<dyn FnOnce(&StatusOutcome) + Send>>>,
}

// async side — ophyd-async style
impl Future for Status {
    type Output = Result<(), StatusError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> { ... }
}

// sync side — ophyd style
impl Status {
    pub fn done(&self) -> bool;
    pub fn success(&self) -> bool;
    pub fn exception(&self) -> Option<StatusError>;
    pub fn add_callback(&self, cb: impl FnOnce(&StatusOutcome) + Send + 'static);
    pub fn wait(&self, timeout: Option<Duration>) -> Result<(), StatusError>;
    pub fn watch(&self) -> watch::Receiver<f64>;
}
```

`add_callback` fires immediately if already done — same semantics as ophyd
`Status.add_callback`.

`watch()` exposes the progress channel as a `WatchableAsyncStatus` (ophyd-async
`_status.py:WatchableAsyncStatus`).

## Signal

The basic device building block. Generic over backend.

```rust
pub struct Signal<T, B: SignalBackend<T>> {
    backend: Arc<B>,
    name:    String,
    kind:    Kind,        // Normal | Config | Hinted | Omitted
}

// async API (ophyd-async style)
impl<T, B: SignalBackend<T>> Signal<T, B> {
    pub async fn get(&self)            -> Result<T>;
    pub async fn put(&self, v: T)      -> Status;
    pub async fn read(&self)           -> Result<HashMap<String, Reading<T>>>;
    pub async fn describe(&self)       -> Result<HashMap<String, DataKey>>;
    pub fn subscribe(&self, cb: ReadingValueCallback<T>) -> SubToken;
}

// sync API (ophyd style) — blanket on top
impl<T, B: SignalBackend<T>> SyncSignal for Signal<T, B> {
    fn get(&self)            -> Result<T>      { block_on(self.get()) }
    fn put(&self, v: T)      -> Status         { block_on(self.put(v)) }
    fn read(&self)           -> Result<...>    { block_on(self.read()) }
    fn describe(&self)       -> Result<...>    { block_on(self.describe()) }
}
```

`Kind` (`Normal`/`Config`/`Hinted`/`Omitted`) is the ophyd kind enum, used by the
RunEngine to decide whether a signal goes into `Event.data`,
`EventDescriptor.configuration`, or is hinted for plotting.

## `#[derive(Device)]` macro

Component composition follows ophyd's `Device(Component)` metaprogramming, expressed
in Rust as a derive macro.

```rust
#[derive(Device)]
pub struct Motor {
    #[signal(rw, "{prefix}.VAL")]
    pub setpoint: Signal<f64>,

    #[signal(ro, "{prefix}.RBV")]
    pub readback: Signal<f64>,

    #[signal(rw, "{prefix}.VELO", kind = config)]
    pub velocity: Signal<f64>,

    #[signal(x, "{prefix}.STOP")]
    pub stop_cmd: SignalX,

    #[signal(rw, "{prefix}.HLM", kind = config)]
    pub high_limit: Signal<f64>,

    #[signal(rw, "{prefix}.LLM", kind = config)]
    pub low_limit: Signal<f64>,
}

#[async_trait]
impl AsyncMovable<f64> for Motor {
    async fn set(&self, value: f64) -> Status {
        self.check_value(value).await?;
        let s = self.setpoint.put(value);
        // Status completes when readback - setpoint < tolerance
        s.with_completion(self.readback.subscribe_for_match(value)).await
    }
}

impl Stoppable for Motor {
    async fn stop(&self, success: bool) -> Result<()> { self.stop_cmd.execute().await }
}

impl Locatable<f64> for Motor { /* ... */ }
```

The macro generates:

- A constructor that accepts a prefix and connects all signals.
- An impl of the appropriate `Subscribable` / `Connectable` chain.
- Type-safe path access via `motor.setpoint.put(...)`.

## StandardDetector — composing Control + Writer

ophyd-async's centerpiece. Expresses 8 protocols in one type by holding two trait
objects:

```rust
pub struct StandardDetector<C: DetectorControl, W: DetectorWriter> {
    control: C,
    writer:  W,
    tasks:   AbortOnDropSet,                    // K1: cleanup on drop
    name:    String,
}

#[async_trait]
impl<C, W> Stageable        for StandardDetector<C, W> { /* delegates */ }
#[async_trait]
impl<C, W> AsyncConfigurable for StandardDetector<C, W> { /* delegates */ }
#[async_trait]
impl<C, W> AsyncReadable     for StandardDetector<C, W> { /* delegates */ }
#[async_trait]
impl<C, W> Triggerable       for StandardDetector<C, W> { /* delegates */ }
#[async_trait]
impl<C, W> Preparable        for StandardDetector<C, W> { /* delegates */ }
#[async_trait]
impl<C, W> Flyable           for StandardDetector<C, W> { /* delegates */ }
#[async_trait]
impl<C, W> Collectable       for StandardDetector<C, W> { /* delegates */ }
#[async_trait]
impl<C, W> WritesStreamAssets for StandardDetector<C, W> { /* delegates */ }
```

`DetectorControl` handles `prepare / arm / wait_for_idle / disarm`.
`DetectorWriter` handles `open / observe_indices_written / collect_stream_docs / close`
(see [`01-architecture.md`](01-architecture.md) sealed trait 3).

A `Hdf5Writer` impl is in M3. A `RogueHdf5Writer` impl is in Phase 2 — same trait, no
plan-side change.

## Connection lifecycle (rules K9, K12)

Construction follows the rogue hardening pattern (`H2: ctor-throw cleanup`):

```rust
let motor = Motor::builder("BL10C:m1")           // accumulate config
    .signal_kind(Kind::Hinted)
    .build_async()                               // not yet connected
    .connect(Duration::from_secs(2)).await?;     // K12: external I/O is the last step
```

If `connect` fails, all SubTokens accumulated during construction drop, releasing
backend slots. No partial state survives.

The sync facade does the same with `build_sync()` + a `connect()` that uses
`block_on`.
