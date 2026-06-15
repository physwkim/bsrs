# 09 — References

Paths to upstream source trees that bsrs reads, cites, or depends on.

## Direct dependencies (this user's local Rust workspaces)

| Repo | Path | What bsrs uses |
|---|---|---|
| `epics-rs` | `/Users/stevek/codes/epics-rs/` | The whole Rust EPICS stack. bsrs uses `epics-ca-rs` and `epics-pva-rs` as `SignalBackend` impls. |
| `epics-rs/crates/epics-ca-rs` | `epics-rs/crates/epics-ca-rs/` | CA client + server. M2 backend. |
| `epics-rs/crates/epics-pva-rs` | `epics-rs/crates/epics-pva-rs/` | PVA client + server, NTNDArray decode. M5 backend + FrameSource. |
| `epics-rs/crates/epics-base-rs` | `epics-rs/crates/epics-base-rs/` | Base record support; useful for soft IOC test harness. |
| `epics-rs/examples/ophyd-test-ioc` | `epics-rs/examples/ophyd-test-ioc/` | Soft IOC harness for bsrs-backends/epics-ca tests. |

## Reference implementations bsrs ports

| Project | Path | Citation pattern |
|---|---|---|
| bluesky | `/Users/stevek/codes/daq/bluesky/` | RunEngine: `src/bluesky/run_engine.py:1478-2510`. Plan stubs: `src/bluesky/plan_stubs.py:62-1747`. Bundler: `src/bluesky/bundlers.py`. Protocols: `src/bluesky/protocols.py:36-526`. |
| ophyd (sync, original) | `/Users/stevek/codes/daq/ophyd/` | Status: `ophyd/status.py`. Device: `ophyd/device.py`. Signal: `ophyd/signal.py`. ophyd-style API in `bsrs::ophyd`. |
| ophyd-async | `/Volumes/NAS/codes/bluesky_source/ophyd-async/src/ophyd_async/core/` | SignalBackend: `_signal_backend.py:16-59`. StandardDetector / DetectorControl / DetectorWriter: `_detector.py:69-160`. Status: `_status.py`. ophyd-async API in `bsrs::ophyd_async`. |
| event-model | `/Users/stevek/codes/daq/event-model/` | Schemas: `src/event_model/schemas/*.json` (authoritative). Compose helpers: `src/event_model/__init__.py:1852-2528`. DocumentRouter: `src/event_model/__init__.py:311`. |

## Reference for Phase 2 (rogue)

| Project | Path | Used at |
|---|---|---|
| rogue | `/Users/stevek/codes/daq/rogue/` | Frame/Master/Slave/Pool semantics: `include/rogue/interfaces/stream/{Frame,Master,Slave,Pool}.h`. Memory plane: `include/rogue/interfaces/memory/`. ZMQ: `python/pyrogue/interfaces/_ZmqServer.py`, `_Virtual.py`. |
| aes-stream-drivers (external) | (separate SLAC repo, Linux kernel module) | Provides `/dev/datadev_N` that rogue's `AxiStreamDma` mmaps. Required at runtime; bsrs does not interact directly. |

## kodex knowledge graph references

bsrs rules K1–K12 are derived from kodex bug-pattern entries in the surrounding
`epics-rs` workspace and the rogue update sweep:

| Rule | Origin UUID | Title |
|---|---|---|
| K1 | `b11af558-6b39-4641-8521-7097f5994b9f` | Spawned tokio task needs scoped AbortOnDrop guard |
| K2 | `bc8466b2-aa71-45ae-a850-69c396e5fbbb` | DbSubscription Drop must remove subscriber slot |
| K3 | `12cca94e-3d6b-40e1-a975-bddbeea48e2c` (B2-G2) | bridge-rs UpstreamManager write-lock contention |
| K4 | `12cca94e-3d6b-40e1-a975-bddbeea48e2c` (B2-G3) | pvalink registry get_or_open dedup |
| K5 | `12cca94e-3d6b-40e1-a975-bddbeea48e2c` (B2-G4) | ControlSource subscribe rx closes immediately |
| K6 | `12cca94e-3d6b-40e1-a975-bddbeea48e2c` (B2-G7) | Group monitor mpsc(64) silent drops |
| K7 | `12cca94e-3d6b-40e1-a975-bddbeea48e2c` (B2-G5) | NDPluginPva subscribers Vec accumulates dead senders |
| K8–K12 | rogue git log: PRs `#1188` (ESROGUE-740), `#1190`, `#1191`, `#1193` | Thread-safety / lifecycle hardening sweep across pyrogue framework, ZMQ client/server, memory TCP, hardware AxiMemMap, stream Fifo/TcpCore, protocols UDP/RSSI/SRP/packetizer |

When updating a rule, refresh the kodex entry at the same time — they should not
diverge.

## Documentation conventions

When citing source code in design docs:

- Use `path/to/file.py:line` or `path/to/file.py:start-end` for ranges.
- Always relative to the upstream repo root, not the user's filesystem.
- For multiple sites, use a short table.

When citing kodex entries:

- Use the 8-character prefix of the UUID (e.g. `b11af558`).
- Include the title in the first reference per document.
