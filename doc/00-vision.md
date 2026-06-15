# 00 — Vision

## What we are building

A Rust-native bluesky-compatible DAQ runtime. Plans, devices, and the
RunEngine all live in Rust. The output — `event-model` Documents — flows into
the bluesky Python ecosystem (databroker, Tiled, BestEffortCallback, suitcase)
unchanged. EPICS CA/PVA backends bind directly to `epics-rs` (no Python shim).

A single user, on a single RunEngine, can:

- drive EPICS motors / detectors / waveforms — pure Rust path
- (Phase 2) drive direct-attached FPGA boards via rogue, in the same plan
- emit bluesky-compatible Document streams over 0MQ / HTTP / files, picked
  up by the existing Python analysis stack with **zero code change there**

## The Document is the contract

```
┌──────────────────────────────┐    ┌──────────────────────────────────┐
│ Rust (bsrs, hot path)      │    │ Python (bluesky stack, unchanged)│
│                               │    │                                   │
│  bsrs-plans  ──┐           │    │ ┌── BestEffortCallback             │
│  bsrs-devices  │           │    │ ├── suitcase-{jsonl,hdf5,...}     │
│  bsrs-stream   ├─► Document├────┼─┤   tiled-ingester (RemoteDispatch)│
│  bsrs-engine ──┘ event-model     │ ├── databroker (catalog query)    │
│                                    │ ├── bluesky-widgets (GUI)         │
│       ▼                            │ └── jupyter / matplotlib          │
│  ┌──────────────────────────┐      │                                    │
│  │ DocumentSink trait       │      │      ▲                             │
│  │  ├ ZmqDocumentSink       │──────┼──────┘ same Document, no change    │
│  │  ├ TiledSink (HTTP)      │──────┤                                    │
│  │  ├ JsonlSink (file)      │      │                                    │
│  │  └ KafkaSink (broker)    │      │                                    │
│  └──────────────────────────┘      │                                    │
└──────────────────────────────┘    └──────────────────────────────────┘
```

The boundary is not "Python or Rust"; the boundary is **the Document**.

## Two co-equal Rust API surfaces

bsrs is async on the inside but exposes both styles for *Rust authoring*:

| Module | Style | Origin | Users |
|---|---|---|---|
| `bsrs::ophyd_async` | async / await | ophyd-async (Python) | new Rust code |
| `bsrs::ophyd` | sync, blocking | ophyd (Python) | scripts, REPL, ophyd-trained users |

Same `Device` and `Signal` types appear in both. The sync layer drives the
async one via the bsrs tokio runtime — single implementation, two surfaces.

## Why rewrite

| Issue | bluesky + ophyd | bsrs |
|---|---|---|
| GIL on hot path | Yes | None (Rust async core) |
| EPICS protocol stack | C `libca.so` + C++ `pvxs` | pure Rust `epics-ca-rs` + `epics-pva-rs` |
| Direct-attached hardware (rogue) | Hard to integrate | One trait impl, lands cleanly |
| Memory + cancellation safety | Human discipline | Compiler-enforced + K1–K12 rules |
| Same language as IOC | Python ↔ C boundary every time | Rust IOC (`epics-rs`) lives next door |

The Python ecosystem **stays valuable** — analysis, visualization, archiving,
queue management. bsrs replaces only the orchestration hot path.

## Where the name comes from

**bsrs** = high-altitude wispy cloud. NSLS-II sky/cloud naming convention
(bluesky / nimbus / databroker / tiled). Light and fast.

## Migration story

| Stage | Move | Result |
|---|---|---|
| **0** (current) | bluesky + ophyd + pyepics | baseline |
| **1** (entry) | One beamline rewrites plans+devices in Rust. bsrs emits documents over `ZmqDocumentSink`. Python analysis Jupyter unchanged — RemoteDispatcher subscribes to bsrs. | hot path Rust; analysis stack untouched |
| **2** | More beamlines move over. queueserver worker swapped to `bsrs-qs` (M8). Manager / REST / web UI unchanged. | production deployment |
| **3** | (optional) Analysis tools also rewritten. Most facilities stop here. | full Rust |

Stage 1 is **the design's meaningful entry point**. Stage 2 is what makes it
deployable in production.

## Phase strategy

```
Phase 1: pure bsrs                       Phase 2: optional integrations
   M0 ─► M1 ─► M2 ─► M3 ─► M4 ─► M5         + rogue ZMQ / DMA backends
                                             + bsrs-py PyO3 plan generator
   M6 = Document sinks (Zmq, Tiled, ...)     + bsrs-qs queueserver worker (M8)
   M7 = (deferred) PyO3 plan authoring
```

Detailed breakdown in [`07-milestones.md`](07-milestones.md).

## Non-goals (rejected as design dead-ends)

- **`use_bsrs()` runtime monkey-patch** of bluesky.RunEngine — modest
  speedup (PyO3 boundary cost eats most of the gains), large maintenance.
- **Auto-translating Python ophyd devices to shadow Rust devices** —
  classification heuristics fragile; most users want to author in Rust
  anyway when committing to bsrs.
- **Embedded Lua / rhai scripting for plans** — between Rust (faster, safer)
  and Python (familiar, ecosystem) without dominating either. PyO3 plan
  generator supersedes this path.
- **Re-implementing bluesky.callbacks.tiled_writer's full schema-normalizing
  writer** in Rust — `ZmqDocumentSink → Python relay → TiledWriter` covers
  the production case without forking the Tiled write protocol.
