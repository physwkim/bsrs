# Mini-beamline verification suite

End-to-end checks driving bsrs against the
[`epics-rs/examples/mini-beamline`](https://github.com/physwkim/epics-rs/tree/main/examples/mini-beamline)
IOC. Each script targets one capability of the bsrs stack and
asserts a concrete expected outcome.

## Setup

```sh
# terminal 1
cd ~/codes/epics-rs
cargo build --release -p mini-beamline --features ioc
./target/release/mini_ioc examples/mini-beamline/ioc/st.cmd

# terminal 2 — bump motor velocities so each step fits inside the
# default 30 s WRITE_NOTIFY put timeout
for pv in mini:ph:mtr.VELO mini:dot:mtrx.VELO mini:dot:mtry.VELO mini:dcm:theta.VELO; do
    caput "$pv" 5
done
```

## Scripts

| # | Script | What it verifies |
|--|--|--|
| 01 | [`01_scan.lua`](./01_scan.lua) | Single-axis CA scan: 17-point Gaussian profile through `mini:ph:DetValue_RBV` |
| 02 | [`02_monitor.lua`](./02_monitor.lua) | Live PV variation: 10 reads of `mini:current` over 2 s, asserts >1 mA Δ |
| 03 | [`03_grid_scan.lua`](./03_grid_scan.lua) | 2D `bp.grid_scan` (3×3) on `dot.mtrx` × `dot.mtry` |
| 04 | [`04_abort.lua`](./04_abort.lua) | `RE:abort()` mid-run cuts a 5 s plan short, sets `exit_status=abort` |
| 05 | [`05_remote_dispatcher.lua`](./05_remote_dispatcher.lua) + [`.py`](./05_remote_dispatcher.py) | bsrs → ZMQ → Python `bluesky.callbacks.zmq.RemoteDispatcher` (msgpack envelope round-trip) |
| 06 | [`06_dcm_energy.lua`](./06_dcm_energy.lua) | Kohzu DCM energy scan (6 → 12 keV via derived motor record) |
| 07 | [`07_tiled_sink.lua`](./07_tiled_sink.lua) | bsrs → tiled-rs HTTP catalog (`TiledSink` registers RunStart, PATCHes RunStop). Read-side via `tiled.from_uri` confirms the run lands. |
| 08 | [`08_suspender.lua`](./08_suspender.lua) | CA-PV-backed `SuspendThreshold` armed but not tripped — verifies arming + run + dispose introduces no overhead |
| 08b | [`08b_suspender_trip.lua`](./08b_suspender_trip.lua) | Same `SuspendThreshold` but threshold is set inside the natural beam-current oscillation (475–525) so the engine actually pauses & auto-resumes mid-plan |
| 09 | (none — see below) | `bsrs repl --doc-jsonl <PATH>` writes every Document as a JSONL line. Smoke: run any Lua scan with `--doc-jsonl /tmp/run.jsonl` and tail the file. |
| 10 | (none — see below) | bsrs-qs daemon E2E: register CA devices via `bsrs qs-manager --ca-motor name=val,rbv` / `--ca-detector name=pv`, then attach `bsrs qs --address ... repl` and drive `RE:run(scan({ph_det}, ph_mtr, ...))`. |

Run any one with:

```sh
cd ~/codes/bsrs
cargo run -p bsrs-cli -- repl --script examples/mini_beamline/01_scan.lua
```

## What's covered elsewhere (no script here)

- **#5 ZMQ envelope (bsrs → bsrs)**:
  `bsrs-callbacks::zmq_source::pub_sub_round_trip_msgpack` unit
  test covers the internal round-trip path.
- **#5 ZMQ envelope (bsrs → bluesky Python)**: see
  `05_remote_dispatcher.{lua,py}` above. Run as a pair (Python
  subscriber first, bsrs publishes ~1 s later). `RemoteDispatcher`
  consumes bsrs's documents unchanged.

## Deferred (not yet covered)

| # | Item | Reason |
|--|--|--|
| 07 | HSC-1 slits + Quad BPM (asyn port driver PVs) | needs PVA struct decode helpers |
| 08 | Waveform PVs (`mini:wf1` … `mini:wf:bundle`) | scalar-Double `EpicsCaBackend` only; need a `<Vec<f64>>` impl |
| 09 | bsrs-qs daemon E2E with mini-beamline | requires `Server::register_*` to take CA devices, daemon binary plumbing |
| 03b | Frame plane: MovingDot 2D image → Hdf5FrameSink | requires `Frame` source on top of `mini:dot:image1:ArrayData` (waveform PV) |

## Verified outcomes

Last good run on 2026-05-11 (m3 macOS, mini_ioc release build):

```
01_scan.lua             — 17 events, exit_status=success, gauss profile (28k / 96k peak / 29k)
02_monitor.lua          — Δ=49 mA over 2s
03_grid_scan.lua        — 9 events (3×3), exit_status=success
04_abort.lua            — exit_status=abort, plan cut from 5s to <0.1s
05_remote_dispatcher    — Python RemoteDispatcher receives 5 events + descriptor + stop;
                          run_uid matches bsrs side; exit_status=success
06_dcm_energy.lua       — 7 events (6→12 keV), exit_status=success
07_tiled_sink.lua       — Run registered at /bsrs/<uid> on tiled-rs HTTP server;
                          tiled-client read-side confirms the run via from_uri+keys
08_suspender.lua        — armed-but-not-tripped: arming + 5-step count completes
                          with is_paused=false start to end
08b_suspender_trip.lua  — threshold=499 below natural oscillation min (475);
                          12 × (read, sleep 0.5) plan ran 9s vs 6s baseline,
                          confirming the watcher fired suspend_until_with(...)
                          and auto-resumed when current returned above threshold
--doc-jsonl             — 20 docs (start + descriptor + 17 events + stop) appended
                          to /tmp/bsrs_test.jsonl; verified shape with `head`/`tail`
qs-manager + ca devices — `bsrs qs-manager --ca-motor / --ca-detector` registers
                          mini-beamline PVs; `bsrs qs ... repl` attaches and runs
                          scan({ph_det}, ph_mtr, -2, 2, 5); 5 frames seen
```

## tiled-rs setup (one-shot for #07)

```sh
# 1. Start tiled-rs with a sqlite catalog (write endpoints enabled
#    via --catalog-uri).
TILED_SINGLE_USER_API_KEY=test123 \
    ~/codes/tiled-rs/target/debug/tiled serve --port 8765 \
    --catalog-uri 'sqlite:///tmp/bsrs-tiled.db' --api-key test123 &

# 2. Pre-create the `bsrs` container at the catalog root.
#    bsrs's TiledSink registers each run as a child (bsrs/<run_uid>),
#    so the parent must exist first.
curl -H "Authorization: Apikey test123" -H "Content-Type: application/json" \
    -X POST 'http://localhost:8765/api/v1/register/' \
    -d '{"structure_family":"container","metadata":{},"specs":[],"key":"bsrs"}'
```
