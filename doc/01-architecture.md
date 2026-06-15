# 01 — Architecture

## 4-layer structure

```
┌──────────────────────────────────────────────────────────────┐
│ L4  Plans          : count, scan, grid_scan, fly             │
│                      Stream<Item=Msg>  ← async-stream!       │
├──────────────────────────────────────────────────────────────┤
│ L3  RunEngine      : msg loop, Bundler, Suspender,           │
│                      checkpoint, AbortOnDrop guards          │
│                      ───────────────────────────────         │
│                      Two entry points:                       │
│                        re.run_async(plan).await   (default)  │
│                        re.run_blocking(plan)                 │
├──────────────────────────────────────────────────────────────┤
│ L2  Devices        : Device tree (#[derive(Device)])         │
│   + Protocols        ──────────────────────────────────      │
│                      AsyncReadable / AsyncMovable / ...      │
│                      Readable      / Movable      / ...      │
│                      (sync = blanket impl over async core)   │
│                      ──────────────────────────────────      │
│                      StandardDetector = DetectorControl      │
│                                       + DetectorWriter       │
├──────────────────────────────────────────────────────────────┤
│ L1a  SignalBackend : 7-method async trait (sealed)           │
│       impls: epics_ca / epics_pva / soft / mock              │
│              + (P2) rogue_zmq                                │
│                                                              │
│ L1b  FramePipe     : zero-copy bulk data                     │
│       impls: PvaMonitorSource → Hdf5Sink                     │
│              + (P2) RogueDmaSource                           │
└──────────────────────────────────────────────────────────────┘
```

Key shifts from earlier drafts:

- L1a backends bind directly to `epics-rs` (`epics-ca-rs` + `epics-pva-rs`). No Python
  shim. No C library FFI.
- L2 has *two* trait families — async and sync — but only the async one carries logic.
  The sync family is a blanket `impl` over the async one through a bsrs runtime handle.

## The 3 sealed traits

These must be decided in Phase 1 and **never break afterwards**.

### Sealed trait 1 — `SignalBackend<T>` (async only)

A direct port of `ophyd_async/core/_signal_backend.py:16-59`. Seven methods.

```rust
#[async_trait]
pub trait SignalBackend<T: Send + 'static>: Send + Sync {
    async fn connect(&self, timeout: Duration) -> Result<()>;
    async fn put(&self, value: T, wait: bool, timeout: Option<Duration>) -> Status;
    async fn get_datakey(&self, source: &str) -> Result<DataKey>;
    async fn get_reading(&self) -> Result<Reading<T>>;
    async fn get_value(&self) -> Result<T>;
    async fn get_setpoint(&self) -> Result<T>;
    fn set_callback(&self, cb: Option<ReadingValueCallback<T>>) -> SubToken;
}
```

`SubToken` is a RAII handle — its `Drop` removes the slot from the backend (rule **K2**).
Backends are **always** async. The sync surface is built on top.

### Sealed trait 2 — `FrameSource` / `FrameSink`

The same interface for a PVA monitor source and (later) a rogue DMA source.
**The word `rogue` does not appear in this trait.**

```rust
pub struct Frame {
    pub payload: Bytes,        // zero-copy capable
    pub ts_ns:   u64,
    pub channel: u8,
    pub flags:   u16,
    pub seq:     u64,
}

#[async_trait]
pub trait FrameSource: Send + Sync {
    fn frames(&self) -> BoxStream<'static, Frame>;
    fn pool(&self) -> Option<&dyn FrameAllocator> { None }   // downstream-pool, optional
    async fn start(&self) -> Result<()>;
    async fn stop(&self)  -> Result<()>;
}

#[async_trait]
pub trait FrameSink: Send + Sync {
    async fn accept(&self, frame: Frame) -> Result<()>;
}
```

`FrameAllocator` is the rogue `Pool` equivalent. Phase 1 returns `None`. Phase 2's rogue
DMA source returns a real allocator that exposes a zero-copy DMA buffer pool.

### Sealed trait 3 — `DetectorWriter`

A direct port of `ophyd_async/core/_detector.py:116-148`.

```rust
#[async_trait]
pub trait DetectorWriter: Send + Sync {
    async fn open(&self, multiplier: u32) -> Result<HashMap<String, DataKey>>;
    fn observe_indices_written(&self) -> watch::Receiver<u64>;
    async fn indices_written(&self) -> u64;
    fn collect_stream_docs(&self, up_to: u64) -> BoxStream<'_, StreamAsset>;
    async fn close(&self) -> Result<()>;
}
```

`observe_indices_written` returns `watch::Receiver<u64>` rather than the Python
`AsyncGenerator[int]`. Lossy (latest-value-wins) but adequate for fly-scan progress.

## Workspace layout

```text
bsrs/
├── Cargo.toml                         # workspace
├── crates/
│   ├── bsrs-event-model/            # typify auto-gen, Document enum
│   ├── bsrs-core/                   # Reading, Status, Msg, Plan, runtime
│   ├── bsrs-protocols-async/        # AsyncReadable, AsyncMovable, ...  (primary)
│   ├── bsrs-protocols-sync/         # Readable, Movable, ...           (sync facade)
│   ├── bsrs-engine/                 # RunEngine, Bundler, Suspender
│   ├── bsrs-plans/                  # count, scan, grid_scan, fly, stubs
│   ├── bsrs-devices/                # #[derive(Device)] + Signal + StandardDetector
│   ├── bsrs-backends/
│   │   ├── epics-ca/                  # uses ../../../epics-rs/crates/epics-ca-rs
│   │   ├── epics-pva/                 # uses ../../../epics-rs/crates/epics-pva-rs
│   │   ├── soft/                      # in-memory (ophyd-async _soft_signal_backend)
│   │   └── mock/                      # testing
│   ├── bsrs-stream/                 # FrameSource/Sink + PvaMonitorSource + Hdf5Sink
│   ├── bsrs-callbacks/              # JsonlSink, TiledSink, BestEffortCallback
│   ├── bsrs/                        # facade crate, re-exports bsrs::ophyd_async, bsrs::ophyd
│   ├── bsrs-cli/                    # bsui-equivalent REPL
│   └── bsrs-py/                     # (optional, M7) PyO3 adapter
└── examples/
    ├── async_count.rs                 # ophyd-async style
    ├── sync_count.rs                  # ophyd style (same plan)
    └── grid_scan.rs
```

The `bsrs` facade crate is what users add to `Cargo.toml`. It exposes:

```rust
// in bsrs/src/lib.rs
pub mod ophyd_async {
    pub use bsrs_protocols_async::*;
    pub use bsrs_devices::async_api::*;
    pub use bsrs_plans::async_plans::*;
}

pub mod ophyd {
    pub use bsrs_protocols_sync::*;
    pub use bsrs_devices::sync_api::*;
    pub use bsrs_plans::sync_plans::*;
}

pub mod prelude {
    pub use bsrs_event_model::Document;
    pub use bsrs_core::{Reading, Status, Msg, Plan};
    pub use bsrs_engine::RunEngine;
    // user picks ophyd_async or ophyd separately
}
```

### What Phase 2 adds (and only adds)

No existing trait or crate changes. New crates only:

```text
+ crates/bsrs-backends/rogue/         # ZMQ Variable backend (impl SignalBackend)
+ crates/bsrs-stream/sources/rogue_dma/   # impl FrameSource
```

## Concurrency model

- **tokio multi-thread runtime is required** — current-thread leads to `block_in_place`
  panics (precedent: kodex Round 1 #2).
- **RunEngine is a single task** that consumes the plan stream serially. All I/O is
  fanned out to tokio tasks.
- **Every `tokio::spawn` site is wrapped by `JoinSet` or an `AbortOnDrop` guard** (rule K1).
- **Cancellation is a single `CancellationToken` tree** (rule K8). The RunEngine holds
  the root token; bundler / suspender / monitor / framepipe all derive child tokens.
- **The bsrs runtime is a `tokio::Runtime` started lazily** in `bsrs_core::runtime()`.
  Sync entry points (`re.run_blocking(plan)`, `Signal::get_blocking()`) drive it via
  `Handle::block_on`.

## What "two orthogonal tracks" means in practice

| Track | Transport | Shape | Frequency | Size |
|---|---|---|---|---|
| Control / metadata (SignalBackend) | CA / PVA / ZMQ | scalar / small array | Hz | bytes ~ KB |
| Bulk data (FramePipe) | PVA monitor / DMA / TCP | NDArray / waveform | kHz–MHz | KB ~ MB/frame |

Plan code can use both tracks at once — they coexist inside the same RunEngine.
