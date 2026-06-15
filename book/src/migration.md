# Migration from bluesky

bsrs is a drop-in replacement at the **Document** boundary. A site
running bluesky today moves to bsrs by replacing one piece at a
time, never the whole stack at once.

## Where bsrs plugs in

```text
                       bluesky / Python                bsrs / Rust
Plan source            generators (bp.*) / Lua coro    bsrs_plans::* / Lua coro
RunEngine              bluesky.RunEngine               bsrs_engine::RunEngine
Devices                ophyd / ophyd-async             bsrs_devices, bsrs-protocols-async
Backends               pyepics / aioca / p4p           epics-ca-rs / epics-pva-rs
Document plumbing      Publisher (ZMQ)                 ZmqDocumentSink
                       TiledWriter                     TiledSink + tiled-rs
                       suitcase-jsonl                  JsonlSink
                       bluesky-kafka                   KafkaDocumentSink
                       NDFileHDF5 (in IOC)             Hdf5FrameSink (bsrs-stream)
queueserver worker     bluesky-queueserver             bsrs-qs (drop-in)
queueserver manager    bluesky-queueserver             unchanged (still Python)
Catalog browse         databroker / Tiled              tiled-rs / Python tiled
Live plot              BestEffortCallback              [open]
```

## Three migration patterns

### 1. Keep bluesky-queueserver, swap the worker

You're running `bluesky-queueserver` with a Python worker today. Swap
the worker for `bsrs qs-manager`:

```sh
# Before
start-re-manager --kafka-server localhost:9092 ...

# After
bsrs qs-manager --control tcp://*:60615 --documents tcp://*:60625
```

The QS manager (and your existing 0MQ JSON-RPC clients, web UI,
queue management) keep working. bsrs-qs implements the same RPC
surface (~30 methods, see
[bsrs-qs/src/dispatch.rs](https://github.com/physwkim/bsrs/blob/main/crates/bsrs-qs/src/dispatch.rs)).

Documents fan out from bsrs-qs's PUB socket to the same
`RemoteDispatcher`s your downstream consumers already use.

### 2. Keep bluesky.RunEngine in Python, swap the document plumbing

Use bsrs's sinks from Python via bsrs-py (M7 — deferred). For
now, the inverse works: run bsrs's RunEngine in a small Rust
helper binary, ZMQ-publish documents to your Python
`RemoteDispatcher` setup. Same wire format as bluesky.callbacks.zmq.

### 3. Native bsrs end-to-end

For new beamlines that don't have a bluesky / ophyd commitment yet:

```sh
bsrs repl --init beamline_devices.lua    # interactive scans
bsrs qs-manager                          # production worker
```

Devices in Rust use `#[derive(Device)]` from `bsrs-derive`:

```rust
use bsrs::ophyd_async::*;

#[derive(Device)]
pub struct Motor<B> {
    #[signal(rw, "{prefix}.VAL")]                pub setpoint: Signal<f64, B>,
    #[signal(ro, "{prefix}.RBV", kind = hinted)] pub readback: Signal<f64, B>,
    #[signal(rw, "{prefix}.VELO", kind = config)] pub velocity: Signal<f64, B>,
}
```

## Plan code translation

bsrs-plans mirrors `bluesky.plans` 1:1 by name. Direct ports:

| bluesky                          | bsrs                              |
| -------------------------------- | ----------------------------------- |
| `bp.count(dets, n)`              | `bsrs_plans::count(dets, n)`      |
| `bp.scan(dets, m, a, b, n)`      | `bsrs_plans::scan(...)`           |
| `bp.list_scan`, `rel_list_scan`  | `bsrs_plans::list_scan`, `rel_list_scan` |
| `bp.grid_scan`, `rel_grid_scan`  | `bsrs_plans::grid_scan`, `rel_grid_scan` |
| `bp.spiral_*`                    | `bsrs_plans::spiral_*`            |
| `bp.adaptive_scan`               | `bsrs_plans::adaptive_scan`       |
| `bp.tune_centroid`               | `bsrs_plans::tune_centroid`       |
| `bp.fly`                         | `bsrs_plans::fly`                 |
| `bp.ramp_plan`                   | `bsrs_plans::ramp_plan`           |
| `bp.log_scan`                    | `bsrs_plans::log_scan`            |
| `bps.*` (one-shot Msg helpers)   | `bsrs_plans::stubs::*`            |
| `bpp.run_wrapper`                | `bsrs_plans::preprocessors::run_wrapper` |
| `bpp.subs_wrapper`               | (no-op alias — see note below)      |
| `bpp.relative_set_wrapper`       | `bsrs_plans::preprocessors::relative_set_wrapper` |
| `bpp.baseline_wrapper`           | `bsrs_plans::preprocessors::baseline_wrapper` |
| `bpp.contingency_wrapper`        | `bsrs_plans::preprocessors::contingency_wrapper` |
| `bpp.finalize_wrapper`           | `bsrs_plans::preprocessors::finalize_wrapper` |
| `bpp.configure_count_time_wrapper` | `bsrs_plans::preprocessors::configure_count_time_wrapper` |

> `subs_wrapper` is documented in bsrs as a no-op for parity. The
> recommended replacement is `re.subscribe(cb)` at engine creation
> time; bsrs has no equivalent of bluesky's per-run
> `temp_callback_ids` swap.

## Document compatibility

bsrs emits the bluesky event-model 1.x document shape verbatim,
serialized as either JSON (default) or msgpack:

- `RunStart` / `EventDescriptor` / `Event` / `EventPage`
- `RunStop`
- `Resource` / `Datum` / `DatumPage`
- `StreamResource` / `StreamDatum`

A Python `RemoteDispatcher` configured with
`deserializer=msgpack.unpackb` consumes them unchanged; ditto
`databroker` if its catalog is wired to a Tiled / suitcase-jsonl
backend.

## What's intentionally different

`doc/03-runengine.md` lists the few places bsrs diverges from
`bluesky.run_engine.RunEngine`:

- `msg_hook` / `state_hook` / `waiting_hook` → `tracing` spans +
  broadcast subscribers.
- string command + dictionary registry → typed `Msg` enum +
  `Msg::Custom { name, payload }` escape hatch.
- `_run_permit: asyncio.Event` → `tokio::sync::Notify` +
  `AtomicState`.

These are equivalents, not omissions. See doc/03 for the full
table.

## What's deferred

bsrs does not (yet) ship a Python class for `bsrs.RunEngine` —
that's the M7 PyO3 layer in `doc/10-roadmap.md`. Until it lands,
the migration path is "bsrs binary on the IOC host, Python on the
analysis side, ZMQ between them."
