# 07 — Milestones

## Status

| # | Status | Deliverable |
|---|---|---|
| M0 | ✅ done | `bsrs-event-model` + `bsrs-core` + protocols (async + sync) |
| M1 | ✅ done | `bsrs-backends/soft` + `bsrs-engine` + `count` plan + `JsonlSink` |
| M2 | ⚠️ partial | `bsrs-backends/epics-ca` real impl behind `--features ca` (compiles, no live IOC test) |
| M3 | ✅ done | `StandardDetector<C, W>` + `BinaryFrameSink` (DetectorWriter) + `scan` plan |
| M4 | ✅ done | pause / resume / checkpoint / suspender + SIGINT 3-tap |
| M5 | ✅ done | `bsrs-backends/epics-pva` real impl + `PvaMonitorSource` (NTNDArray zero-copy) + `BinaryFrameSink` |
| M6 | 🔄 in progress | Document sinks for the bluesky Python ecosystem (Zmq, Tiled, …) |
| M7 | ⏸ deferred | (optional) `bsrs-py` PyO3 plan generator |
| M8 | ⏸ planned | `bsrs-qs` queueserver worker (drop-in replacement) |

## Phase 1 (Rust-native)

The word "rogue" appears 0 times in code through M8. The 3 sealed traits
(`SignalBackend`, `FrameSource`/`Sink`, `DetectorWriter`) are the contract
Phase 2 will plug into.

| # | Deliverable | Acceptance |
|---|---|---|
| **M0** | `bsrs-event-model` + `bsrs-core` + `bsrs-protocols-async` + `bsrs-protocols-sync` | Round-trip test: Document JSON → deserialize → re-serialize → equal. 11 protocol traits compile. |
| **M1** | `bsrs-backends/soft` + `bsrs-engine` + `count` plan + `JsonlSink` | `count(soft_det, 5)` emits the expected 8-document sequence. |
| **M2** | `bsrs-backends/epics-ca` (`epics-ca-rs`) | `Signal<f64, EpicsCaBackend>` connect/get/put/subscribe; sharded registry (K3); pending-Notify dedup (K4). |
| **M3** | `StandardDetector<C, W>` + `scan` plan + `BinaryFrameSink` (DetectorWriter) | 5-point scan emits StreamResource + StreamDatum; Hdf5 follow-up via M5 sinks. |
| **M4** | pause / resume / checkpoint / suspender + SIGINT 3-tap | Three integration tests cover {pause→resume, abort→close-run, suspender auto-resume}. |
| **M5** | `bsrs-backends/epics-pva` + `PvaMonitorSource` (NTNDArray zero-copy) | NTNDArray decode emits Frames whose `Bytes` share the PVA decode buffer's refcount (verified by unit tests). |
| **M6** | **Document sinks for the Python ecosystem** | `ZmqDocumentSink` round-trips with bluesky `RemoteDispatcher` (msgpack envelope, prefix, name, body). `TiledSink` (minimal) registers runs in a Tiled HTTP catalog. `JsonlSink` already in M1. |
| **M7** | (deferred) `bsrs-py` PyO3 plan generator | A Python `def my_plan(): yield from ...` generator drives the bsrs RunEngine through a thin PyO3 plan adapter. |
| **M8** | **`bsrs-qs` queueserver worker** | bsrs implements the `bluesky-queueserver` worker protocol (Pipe + JSON-RPC). queueserver manager / 0MQ API / web UI / queue management unchanged. |

### M6 sub-deliverables (this PR)

| sub | what |
|---|---|
| **M6.A** | `ZmqDocumentSink` — bluesky Publisher-compatible 0MQ envelope. msgpack default; JSON option. Prefix support. PUB socket, bind/connect. |
| **M6.B** | `TiledSink` — minimal `POST /api/v1/register/<container>/<run_uid>` for RunStart, `PATCH metadata` for RunStop. Bulk Document streams should use ZmqDocumentSink → Python relay → TiledWriter. |
| **M6.C** | doc updates (this file, vision, decisions) |
| **M6.D** | end-to-end round-trip tests (PUB → SUB → decode → assert document order + body) |

## Phase 2 (rogue, when needed)

| # | Deliverable | Acceptance |
|---|---|---|
| **P2-A** | `bsrs-backends/rogue/ctrl` — ZMQ Variable backend impl `SignalBackend` | A rogue Tree Variable read/written through bsrs `Signal<T>`. K11 enforced. |
| **P2-B** | `bsrs-stream/sources/rogue_dma` impl `FrameSource` | A rogue DMA-backed detector emits Frames into the same FramePipe used by PvaMonitorSource. Plan code unchanged. |

No trait change required.

## Out-of-band tracks

Picked up at any milestone without disturbing the main path:

- **Documentation site** — render `doc/*.md` via `mdbook` after M6
- **CI** (already in place since M2) — fmt, clippy `-D warnings`, test on
  ubuntu + macos
- **Soft IOC harness** — `epics-rs/examples/ophyd-test-ioc` driving M2/M5
  tests
- **Performance baseline** — `criterion` benches for plan-loop overhead and
  document-fan-out throughput
- **Real Tiled write integration** — a Python relay binary (`bsrs-zmq2tiled`)
  that subscribes to ZmqDocumentSink and feeds bluesky's `TiledWriter`
