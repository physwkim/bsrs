# Mini-beamline verification suite

End-to-end checks driving cirrus against the
[`epics-rs/examples/mini-beamline`](https://github.com/physwkim/epics-rs/tree/main/examples/mini-beamline)
IOC. Each script targets one capability of the cirrus stack and
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
| 05 | [`05_remote_dispatcher.lua`](./05_remote_dispatcher.lua) + [`.py`](./05_remote_dispatcher.py) | cirrus → ZMQ → Python `bluesky.callbacks.zmq.RemoteDispatcher` (msgpack envelope round-trip) |
| 06 | [`06_dcm_energy.lua`](./06_dcm_energy.lua) | Kohzu DCM energy scan (6 → 12 keV via derived motor record) |
| 07 | [`07_tiled_sink.lua`](./07_tiled_sink.lua) | cirrus → tiled-rs HTTP catalog (`TiledSink` registers RunStart, PATCHes RunStop). Read-side via `tiled.from_uri` confirms the run lands. |

Run any one with:

```sh
cd ~/codes/cirrus
cargo run -p cirrus-cli -- repl --script examples/mini_beamline/01_scan.lua
```

## What's covered elsewhere (no script here)

- **#5 ZMQ envelope (cirrus → cirrus)**:
  `cirrus-callbacks::zmq_source::pub_sub_round_trip_msgpack` unit
  test covers the internal round-trip path.
- **#5 ZMQ envelope (cirrus → bluesky Python)**: see
  `05_remote_dispatcher.{lua,py}` above. Run as a pair (Python
  subscriber first, cirrus publishes ~1 s later). `RemoteDispatcher`
  consumes cirrus's documents unchanged.

## Deferred (not yet covered)

| # | Item | Reason |
|--|--|--|
| 07 | HSC-1 slits + Quad BPM (asyn port driver PVs) | needs PVA struct decode helpers |
| 08 | Waveform PVs (`mini:wf1` … `mini:wf:bundle`) | scalar-Double `EpicsCaBackend` only; need a `<Vec<f64>>` impl |
| 09 | cirrus-qs daemon E2E with mini-beamline | requires `Server::register_*` to take CA devices, daemon binary plumbing |
| 03b | Frame plane: MovingDot 2D image → Hdf5FrameSink | requires `Frame` source on top of `mini:dot:image1:ArrayData` (waveform PV) |

## Verified outcomes

Last good run on 2026-05-11 (m3 macOS, mini_ioc release build):

```
01_scan.lua             — 17 events, exit_status=success, gauss profile (28k / 96k peak / 29k)
02_monitor.lua          — Δ=49 mA over 2s
03_grid_scan.lua        — 9 events (3×3), exit_status=success
04_abort.lua            — exit_status=abort, plan cut from 5s to <0.1s
05_remote_dispatcher    — Python RemoteDispatcher receives 5 events + descriptor + stop;
                          run_uid matches cirrus side; exit_status=success
06_dcm_energy.lua       — 7 events (6→12 keV), exit_status=success
07_tiled_sink.lua       — Run registered at /cirrus/<uid> on tiled-rs HTTP server;
                          tiled-client read-side confirms the run via from_uri+keys
```

## tiled-rs setup (one-shot for #07)

```sh
# 1. Start tiled-rs with a sqlite catalog (write endpoints enabled
#    via --catalog-uri).
TILED_SINGLE_USER_API_KEY=test123 \
    ~/codes/tiled-rs/target/debug/tiled serve --port 8765 \
    --catalog-uri 'sqlite:///tmp/cirrus-tiled.db' --api-key test123 &

# 2. Pre-create the `cirrus` container at the catalog root.
#    cirrus's TiledSink registers each run as a child (cirrus/<run_uid>),
#    so the parent must exist first.
curl -H "Authorization: Apikey test123" -H "Content-Type: application/json" \
    -X POST 'http://localhost:8765/api/v1/register/' \
    -d '{"structure_family":"container","metadata":{},"specs":[],"key":"cirrus"}'
```
