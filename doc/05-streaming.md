# 05 — Streaming Data Path

## Why this layer exists

`SignalBackend` is fine for scalar PVs and small arrays. For high-rate bulk data
(camera frames, FPGA waveforms, multi-MB NTNDArrays), three things change:

- The shape becomes `Bytes` (potentially zero-copy ref-counted).
- The producer is autonomous (interrupt-driven or DMA-driven), not request/response.
- The consumer pipeline (filter / chunk / write) wants to be wired declaratively.

This is the rogue Master/Slave/Frame pattern, distilled into a backend-neutral trait
in bsrs. PVA monitors are the Phase 1 source; rogue DMA is the Phase 2 source. The
trait does not know about either.

## The trait set

```rust
pub struct Frame {
    pub payload: Bytes,        // Arc-backed, clone is free
    pub ts_ns:   u64,
    pub channel: u8,
    pub flags:   u16,
    pub seq:     u64,
}

#[async_trait]
pub trait FrameSource: Send + Sync {
    fn frames(&self) -> BoxStream<'static, Frame>;
    fn pool(&self) -> Option<&dyn FrameAllocator> { None }
    async fn start(&self) -> Result<()>;
    async fn stop(&self)  -> Result<()>;
}

#[async_trait]
pub trait FrameSink: Send + Sync {
    async fn accept(&self, frame: Frame) -> Result<()>;
}

#[async_trait]
pub trait FrameAllocator: Send + Sync {
    async fn alloc(&self, min_bytes: usize, zero_copy: bool) -> BytesMut;
    fn ret(&self, _buf: BytesMut) {}
}
```

`Bytes` carries the rogue Frame's "list of Buffers" naturally — it can chain. Cloning
is an Arc bump, so multi-sink fan-out is free.

## FramePipe — the rogue master/slave graph in Rust

```rust
pub struct FramePipe {
    primary:        Arc<dyn FrameSink>,       // also exposes pool, last to receive
    secondaries:    Vec<Arc<dyn FrameSink>>,
    overflow_drops: AtomicU64,                // K6: never silently lost
    cancel:         CancellationToken,        // K8
}

impl FramePipe {
    pub fn builder() -> FramePipeBuilder { ... }

    pub async fn send(&self, frame: Frame) {
        for s in &self.secondaries {
            if s.accept(frame.clone()).await.is_err() {
                self.overflow_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
        let _ = self.primary.accept(frame).await;   // primary last (rogue ordering)
    }
}
```

The builder pattern is mandatory (rule **K9**) — every background task spawn happens
only after `.start()`, which is the final commit. Until `.start()`, the builder owns
all resources via `Drop`.

```rust
let pipe = FramePipe::builder()
    .primary(Arc::new(hdf5_writer))
    .secondary(Arc::new(monitor_callback))
    .secondary(Arc::new(kafka_publisher))
    .start()?;                                 // background tasks spawn here
```

## Phase 1 sources and sinks

Source: `PvaMonitorSource`

```rust
pub struct PvaMonitorSource {
    pv:     PvaMonitorHandle,                 // from epics-pva-rs
    cancel: CancellationToken,
    frames: mpsc::Sender<Frame>,
}
```

When a `MonitorEvent` arrives carrying `NTNDArray`, the array payload is wrapped in
`Bytes::from(arc_ndarray.into_raw())` — zero-copy from PVA decode buffer to `Frame`.

Sink: `Hdf5Sink`

Implements both `FrameSink` and `DetectorWriter`. On `accept(frame)`:

1. Append the payload to the open HDF5 dataset (or chunk, depending on configuration).
2. `atomic_count.fetch_add(1, Ordering::Relaxed)`.
3. `watch_tx.send(count)` — drives `observe_indices_written`.

On `collect_stream_docs(up_to)`: emits a `StreamDatum` covering `[last_emitted, up_to)`.

## How a rogue source plugs in (Phase 2 preview)

```rust
// crates/bsrs-stream/sources/rogue_dma/src/lib.rs (Phase 2 only)
pub struct RogueDmaSource {
    handle: rogue::AxiStreamDma,             // via cxx FFI
    pool:   RogueFramePool,                  // exposes downstream allocator
}

#[async_trait]
impl FrameSource for RogueDmaSource {
    fn frames(&self) -> BoxStream<'static, Frame> { /* tail the DMA ring */ }
    fn pool(&self) -> Option<&dyn FrameAllocator> { Some(&self.pool) }
    async fn start(&self) -> Result<()> { self.handle.arm() }
    async fn stop(&self)  -> Result<()> { self.handle.disarm() }
}
```

The trait remains unchanged. bsrs-engine and bsrs-plans never see the rogue type.

## How bluesky meets rogue at four exact points

```
┌─── bluesky time axis (RunEngine drives) ──────────────────┐
│                                                            │
│  open_run                                       close_run  │
│    │                                                │      │
│    ▼                                                ▼      │
│   ┌──────────────────────────────────────────────┐         │
│   │                                              │         │
│   │         ★ rogue time axis ★                  │         │
│   │   (bulk stream lives inside this envelope)   │         │
│   │                                              │         │
│   │   prepare ─► kickoff ──── frames ──── complete         │
│   │                                              │         │
│   └──────────────────────────────────────────────┘         │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

The four touch points are in the `Flyable` + `Collectable` + `Stageable` traits:

| Touch point | Trait method | What happens |
|---|---|---|
| **stage / unstage** | `Stageable` | Build the FramePipe graph (master + slaves), `start()` the pool, `stop()` and join on teardown. Rule **K9** demands the build is the last step before going live. |
| **kickoff** | `Flyable::kickoff` | Send ARM. Returns `Status::done()` immediately; data arrives asynchronously through the pipe. |
| **complete** | `Flyable::complete` | Wait until `observe_indices_written` reaches the target count, then disarm. |
| **collect** | `Collectable::collect` + `WritesStreamAssets::collect_asset_docs` | Translate the stream's accumulated frames into `StreamDatum` documents. |

Plan code does not change between Phase 1 PVA-monitor sources and Phase 2 rogue DMA
sources. The `Flyable` machinery is identical.

## Failure modes and how they are handled

| Failure | Mechanism |
|---|---|
| Sink full / slow | `accept()` returns `Err`. `overflow_drops` counter increments. RunEngine emits the counter as configuration metadata at run close. K6. |
| Source crashes | `frames()` stream ends. Plan sees `complete()` time out. Run ends with `exit_status: "fail"`. |
| RunEngine cancelled mid-run | Cancellation token propagates. `stop()` runs on every source. Frames in flight are dropped after the primary sink's last accept. |
| Cancelled before `start()` | Builder drops, all owned resources released via Drop. No background tasks ever spawned. K9. |
| Source produces faster than sink | Backpressure via bounded mpsc. Producer blocks on `accept()` await. Pre-emptive drop only if explicitly configured (`policy: drop_oldest`). |
