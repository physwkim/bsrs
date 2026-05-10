# Cookbook

Recipes for common beamline workflows. Every snippet is meant to be
pasted into either `cirrus repl` (local) or `cirrus qs repl`
(attached to a running daemon).

## Count + scan + grid

The bluesky `bp.*` namespace is mirrored in cirrus:

```lua
RE:run(bp.count({det1}, 5))
RE:run(bp.scan({det1}, m1, 0, 10, 11))
RE:run(bp.grid_scan({det1}, m1, 0, 1, 5, m2, 0, 1, 5))
RE:run(bp.spiral({det1}, m1, m2, 0, 0, 1, 1, 0.1, 0.1))
RE:run(bp.adaptive_scan({det1}, m1, 0, 10, 0.1, 1.0, 0.05))
```

## mvr with baseline

`mvr` ("move relative") records a starting position, then sets the
motor to the new value. Wrap in `baseline_wrapper` so a snapshot of
your slits / monochromator / temperature is recorded at the start
and end of every run:

```lua
local plan_with_baseline = bpp.baseline_wrapper(
    bp.scan({det1}, m1, 0, 1, 11),
    {sl1, sl2, mono, sample_temp},   -- baseline devices
    "baseline"                       -- stream name
)
RE:run(plan_with_baseline)
```

The result run has two extra streams (`baseline_pre` and
`baseline_post`) carrying one-shot reads of the listed devices.

## Fly + HDF5 frame writer

For detectors that produce frames faster than a Python `Read`-loop
can drain, run a fly scan and write frames to NeXus-flavored HDF5
locally (no Document plane bytes):

```lua
-- assume `pilatus` is registered as both Flyable and a frame
-- producer; `hdf5_sink` is an Hdf5FrameSink on the daemon.
local fly_with_writer = bpp.fly_during_wrapper(
    bp.count({pilatus}, 1),
    {{pilatus, pilatus}}     -- (flyable, collectable) pair
)
RE:run(fly_with_writer)
```

For the multi-process layout (frame source on the IOC host,
RunEngine elsewhere), use `cirrus frame-source` — see
[CLI tour → frame-source](./cli.md#frame-source).

## Custom subscriber

```lua
RE:subscribe(function(name, body)
    if name == "stop" then
        print("run finished:", body.exit_status, body.run_uid)
    end
end)
RE:run(bp.count({det1}, 5))
```

The callback fires for every Document. Filter by `name`:
`"start"`, `"descriptor"`, `"event"`, `"stop"`, `"resource"`,
`"datum"`, `"stream_resource"`, `"stream_datum"`.

## Inspect-driven debugging

Attach a REPL to the running daemon and inspect device state on the
fly:

```lua
-- in cirrus qs repl
qs> motor:inspect()
=> {readback=0.5, setpoint=0.5, type="SoftMotor", units="mm", ...}
qs> det1:inspect()
=> {counts=42, type="SoftDetector", data_key="det1_counts", ...}

qs> -- check a Status object's progress
qs> local s = motor:set(2.0)
qs> s:inspect()
=> {done=false, success=null, exception=null, progress=0.0}
qs> s:wait()
qs> s:inspect()
=> {done=true, success=true, exception=null, progress=1.0}
```

## Lua coroutine plans

For one-off scans you don't want to compile into `cirrus_plans`:

```lua
local function snake_grid(detectors, mx, my, x_pts, y_pts, exposure)
    coroutine.yield(msg.open_run({plan_name = "snake_grid"}))
    for j = 0, #y_pts - 1 do
        coroutine.yield(msg.set(my, y_pts[j+1], "main"))
        coroutine.yield(msg.wait("main"))
        local order = (j % 2 == 0) and 1 or -1
        for i = 0, #x_pts - 1 do
            local xi = (order > 0) and (i + 1) or (#x_pts - i)
            coroutine.yield(msg.set(mx, x_pts[xi], "main"))
            coroutine.yield(msg.wait("main"))
            coroutine.yield(msg.create("primary"))
            coroutine.yield(msg.read(mx))
            coroutine.yield(msg.read(my))
            for _, d in ipairs(detectors) do
                coroutine.yield(msg.read(d))
            end
            coroutine.yield(msg.save())
        end
    end
    coroutine.yield(msg.close_run("success"))
end

RE:run(plan(snake_grid, {det1}, m1, m2,
            {0, 0.5, 1.0, 1.5, 2.0},
            {0, 0.5, 1.0},
            0.1))
```

When the coroutine is verified, port to Rust by replacing every
`coroutine.yield(msg.X)` with `yield Msg::X` inside an
`async_stream::stream!` block. The Rust version executes at the
same Msg cadence with no Lua bridge cost per iteration.

## Custom device methods (Rust → Lua)

Rust devices expose their own methods to Lua via `#[lua_methods]`:

```rust
use cirrus_derive::lua_methods;

pub struct Diffractometer { /* ... */ }

impl cirrus_core::msg::NamedObj for Diffractometer { /* ... */ }

#[lua_methods]
impl Diffractometer {
    #[lua_method]
    pub async fn set_orientation(&self, h: f64, k: f64, l: f64)
        -> Result<(), CirrusError> { /* ... */ }

    #[lua_method]
    pub async fn current_hkl(&self) -> (f64, f64, f64) { /* ... */ }
}

// at startup:
let dx = Arc::new(Diffractometer::new(...));
reg.register_readable("dx", dx.clone() as Arc<dyn ReadableObj>);
reg.register_lua_methods("dx", dx.clone());
```

From the daemon REPL:

```lua
qs> dx:set_orientation(1.5, 2.5, 3.5)
qs> table.unpack(dx:current_hkl())
=> 1.5  2.5  3.5
qs> dx:name()                          -- inherited from NamedObj
=> "dx"
```

The macro generates a JSON-shaped dispatch table so cirrus-cli's
Lua bridge can wire it into the daemon's mlua state at startup.

## Configure-time exposure

To set a uniform `count_time` across multiple detectors before a
plan:

```lua
local plan = bpp.configure_count_time_wrapper(
    bp.scan({det1, det2, det3}, m1, 0, 1, 11),
    0.5,                              -- exposure seconds
    {det1, det2, det3}                -- detectors to configure
)
RE:run(plan)
```

The wrapper emits `Configure { obj, args: { count_time: 0.5 } }`
for each detector at the head of the run. Detectors that don't
accept `count_time` will surface as `Configure`-time errors — the
wrapper does not suppress them.

## Resilient cleanup

Run a finalize block whether or not the inner plan succeeds:

```lua
local cleanup = bps.mv(safety_shutter, 0)   -- close shutter
RE:run(bpp.finalize_wrapper(
    bp.scan({det1}, m1, -1, 1, 11),
    cleanup
))
```

`finalize_wrapper` and the alias `contingency_wrapper` both run
the cleanup plan after the inner plan completes (success, abort,
or error). Pair with `subs_wrapper` (no-op alias for parity with
bluesky) if you're porting `bpp.subs_wrapper` calls verbatim.

## Lock + RBAC for shared beamlines

The daemon supports a subsystem lock + per-group ACL:

```sh
# from one operator's terminal:
cirrus qs lock apply --queue --note "tuning the mono" alice-key

# any other client now sees:
$ cirrus qs queue start
{"error": {"code": -32000, "message": "operation rejected: subsystem is locked..."}}

# release:
cirrus qs lock release alice-key
```

For finer-grained control, supply a `permissions.toml` to
`cirrus qs-manager --permissions <PATH>`:

```toml
default_group = "viewer"

[user_groups.viewer]
read_only = true

[user_groups.scientist]
allowed_plans = ["count", "scan.*"]
allowed_devices = [".*"]

[user_groups.admin]
admin = true
allowed_plans = [".*"]
allowed_devices = [".*"]

[api_keys]
"k-alice" = "scientist"
"k-root"  = "admin"
```

Clients identify themselves via `params.api_key` on each RPC.
Without a key they resolve to `default_group`. Admin-class methods
(`lua_eval`, `permissions_*`, `manager_*`) are restricted to
`admin = true` groups.
