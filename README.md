# cirrus

A Rust-native re-implementation of the [bluesky](https://blueskyproject.io)
acquisition stack — RunEngine, devices, plans, document sinks — built so a
beamline data-acquisition daemon can run without dragging a Python interpreter
onto the IOC host.

cirrus emits the bluesky **event-model** documents verbatim, so the existing
Python ecosystem (`databroker`, `Tiled`, `bluesky-kafka`, `BestEffortCallback`)
consumes them unchanged. You can swap one piece at a time — see
[Migration patterns](#migration) below.

## Highlights

- **`bluesky.run_engine.RunEngine` parity** — typed `Msg` enum, full state
  machine (`Idle / Running / Paused / Aborting / Halted`), pause/resume,
  suspenders, preprocessors, callbacks, baseline, run keys, contingency.
- **ophyd-async-style devices** with a sync facade so the same plan code
  works in both worlds. `#[derive(Device)]` + `#[signal(...)]` on PV
  paths, no Python.
- **Drop-in EPICS CA + PVA backends** via `epics-ca-rs` / `epics-pva-rs`
  with sharded process-singleton registries, in-flight de-dup, RAII
  subscription tokens, and zero-copy NTNDArray decode for detector
  frames.
- **`cirrus qs-manager`** — bluesky-queueserver-compatible daemon
  speaking JSON-RPC-over-0MQ on the same control / document ports your
  `qserver` clients already talk to. ~30 RPC methods implemented.
- **`cirrus qs repl`** — attach-to-running-daemon Lua REPL.
  `motor:inspect()`, `motor:set(1.5):wait()`, `RE:run(count({det1}, 100))`
  against a live daemon, no Python needed.
- **Document sinks** for the bluesky Python ecosystem: JSONL, ZMQ
  (msgpack/JSON, bluesky `Publisher` envelope), Tiled (HTTP), Kafka,
  HDF5 (NeXus-flavored frame writer), Hdf5/Binary frame sinks for the
  D21 multi-process layout.
- **Lua dev surface** — `cirrus repl` for offline plan prototyping; the
  `#[lua_methods]` attribute macro auto-exposes Rust device methods to
  the daemon Lua state with no manual mlua wiring.
- **Operational features** — `cirrus doctor` env probe, `cirrus migrate`
  state-dir migration tool, `permissions.toml` RBAC for the JSON-RPC
  surface, Prometheus `/metrics` listener, criterion benches.

## Quickstart

Build:

```sh
git clone https://github.com/physwkim/cirrus
cd cirrus
cargo build --release
ln -s "$PWD/target/release/cirrus" ~/.local/bin/
```

Sanity-check the environment:

```sh
cirrus doctor
[ ok ]   tokio runtime (multi-thread)
[ ok ]   EPICS_CA_ADDR_LIST = 192.168.50.255
[warn]   EPICS_CA_AUTO_ADDR_LIST = NO
```

Local Lua REPL — fastest path to running a plan:

```sh
$ cirrus repl
cirrus> det1 = soft_detector("det1")
cirrus> RE:run(count({det1}, 5))
exit_status=success run_uid=8e3f...
cirrus> m1 = soft_motor("m1", 0.0)
cirrus> RE:run(scan({det1}, m1, 0, 10, 11))
exit_status=success run_uid=...
```

Production daemon + attached REPL:

```sh
# terminal 1: start the queueserver-compatible daemon
$ cirrus qs-manager --soft-detectors 2 --soft-motors 2

# terminal 2: attach a Lua shell to the running daemon
$ cirrus qs repl
qs> m1:inspect()
=> {readback=0, setpoint=0, type="SoftMotor", units="mm", connected=true, ...}
qs> m1:set(1.5):wait()
qs> m1:inspect().readback
=> 1.5
qs> RE:run(count({det1}, 100))
=> exit_status=success run_uid=...
```

## Architecture

cirrus is a Cargo workspace. Each crate has a single responsibility;
boundaries are designed so a downstream user can swap one
implementation without touching the others.

```text
cirrus                      umbrella re-exports + binary entry points
├── cirrus-cli              binary: qs-manager, qs, repl, doctor, migrate, frame-source
├── cirrus-engine           RunEngine, Msg, state machine, suspenders, preprocessors
├── cirrus-plans            bp.* / bps.* / bpp.* mirrors (count, scan, grid_scan, ...)
├── cirrus-protocols        Movable, Triggerable, Stageable, Readable (sync facade)
├── cirrus-protocols-async  ophyd-async-style traits over async fns
├── cirrus-derive           #[derive(Device)], #[lua_methods] proc-macros
├── cirrus-devices          SoftMotor, SoftDetector, NDSimDetector, ...
├── cirrus-backend-epics-ca   SignalBackend over CA  (feature: real)
├── cirrus-backend-epics-pva  SignalBackend over PVA (feature: real)
├── cirrus-callbacks        Document sinks: jsonl, zmq, tiled, kafka
├── cirrus-stream           FrameSink/FrameSource: hdf5, binary, pva
└── cirrus-qs               bluesky-queueserver-compatible daemon
```

The Document plane and the frame plane are kept separate. Frame bytes
never travel through the Document plane — `StreamResource` /
`StreamDatum` carry only path / shape descriptors, while the bytes
flow `FrameSource → FramePipe → FrameSink` locally. This is what
makes the "RunEngine on the IOC host" deployment shape work.

## Migration

cirrus is a drop-in replacement at the **Document boundary**. Three
common migration patterns:

1. **Keep bluesky-queueserver, swap the worker.** Replace
   `start-re-manager` with `cirrus qs-manager`. Existing `qserver` /
   web UI / `RemoteDispatcher` consumers connect unchanged.
2. **Keep `bluesky.RunEngine` in Python, swap the document
   plumbing.** Run cirrus's RunEngine in a small Rust helper, ZMQ-
   publish to your Python `RemoteDispatcher` setup. Same wire format
   as `bluesky.callbacks.zmq`.
3. **Native cirrus end-to-end.** New beamlines without a bluesky /
   ophyd commitment. `cirrus repl` for development, `cirrus
   qs-manager` for production.

Plan-code translation table and full migration guide:
[book/src/migration.md](book/src/migration.md).

## Optional features (Cargo flags)

| Crate              | Feature        | Adds                                    |
| ------------------ | -------------- | --------------------------------------- |
| `cirrus-callbacks` | `zmq`          | bluesky `Publisher` envelope            |
| `cirrus-callbacks` | `tiled`        | HTTP register + metadata patch          |
| `cirrus-callbacks` | `kafka`        | pure-Rust `kafka` crate, no librdkafka  |
| `cirrus-stream`    | `hdf5`         | rust-hdf5 frame writer, NeXus layout    |
| `cirrus-stream`    | `pva`          | NTNDArray monitor source                |
| `cirrus-backend-epics-{ca,pva}` | `real` | live EPICS clients              |
| `cirrus-qs`        | `metrics`      | Prometheus `/metrics` endpoint          |
| `cirrus-cli`       | `tiled`        | Lua `tiled.*` read-side namespace       |
| `cirrus-cli`       | `frame-source` | wire PVA + HDF5 into `cirrus frame-source` |

The default build is small and dependency-light. CI builds and tests
each opt-in feature on every push.

## Documentation

User-facing book (mdbook source):
[`book/src/`](book/src/) — quickstart, migration, CLI tour,
operational runbook, architecture overview.

Design notes (numbered for read-in-order):

1. [`doc/00-vision.md`](doc/00-vision.md)
2. [`doc/01-architecture.md`](doc/01-architecture.md)
3. [`doc/02-event-model.md`](doc/02-event-model.md)
4. [`doc/03-runengine.md`](doc/03-runengine.md)
5. [`doc/04-devices.md`](doc/04-devices.md)
6. [`doc/05-streaming.md`](doc/05-streaming.md)
7. [`doc/06-rules.md`](doc/06-rules.md)
8. [`doc/07-milestones.md`](doc/07-milestones.md)
9. [`doc/08-decisions.md`](doc/08-decisions.md)
10. [`doc/09-references.md`](doc/09-references.md)
11. [`doc/10-roadmap.md`](doc/10-roadmap.md) — tracked open items

## License

BSD-3-Clause.
