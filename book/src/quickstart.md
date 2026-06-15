# Quickstart

The fastest path: open the Lua REPL, run a count, then a scan.

```sh
$ cargo run --bin bsrs -- repl
bsrs repl (Lua 5.4) — type `:help` for commands, Ctrl-D to exit
bsrs> det1 = soft_detector("det1")
bsrs> RE:run(count({det1}, 5))
exit_status=success run_uid=8e3f...
bsrs> m1 = soft_motor("m1", 0.0)
bsrs> RE:run(scan({det1}, m1, 0, 10, 11))
exit_status=success run_uid=...
```

## Install

```sh
git clone https://github.com/physwkim/bsrs
cd bsrs
cargo build --release
ln -s "$PWD/target/release/bsrs" ~/.local/bin/
```

## Sanity-check the environment

```sh
bsrs doctor
[ ok ]   tokio runtime (multi-thread)
[ ok ]   EPICS_CA_ADDR_LIST = 192.168.50.255
[warn]   EPICS_CA_AUTO_ADDR_LIST = NO  (not auto-detecting interfaces)
```

Add `--tiled-url http://localhost:8000` and/or `--kafka host:9092` to
probe those services too.

## Write a plan

In Lua, mirror the bluesky `bp.*` / `bps.*` namespaces:

```lua
local det1 = soft_detector("det1")
local m1   = soft_motor("m1", 0.0)

-- Compound plans (bp.*)
RE:run(bp.count({det1}, 10))
RE:run(bp.scan({det1}, m1, 0, 1, 11))
RE:run(bp.grid_scan({det1}, m1, 0, 1, 5, m2, 0, 1, 5))

-- Stub plans (bps.*)
RE:run(bps.mv(m1, 0.5))
RE:run(bps.sleep(0.1))
```

Or write a coroutine-style plan that yields `Msg` values:

```lua
local function my_scan(detectors, motor, n)
    coroutine.yield(msg.open_run({plan_name = "my_scan"}))
    for i = 0, n - 1 do
        local pos = i / (n - 1)
        coroutine.yield(msg.set(motor, pos, "main"))
        coroutine.yield(msg.wait("main"))
        coroutine.yield(msg.create("primary"))
        coroutine.yield(msg.read(motor))
        for _, d in ipairs(detectors) do
            coroutine.yield(msg.read(d))
        end
        coroutine.yield(msg.save())
    end
    coroutine.yield(msg.close_run("success"))
end
RE:run(plan(my_scan, {det1}, m1, 5))
```

Use the Lua REPL for fast iteration (no recompile). When the plan
is verified, port it to Rust — every `coroutine.yield(msg.X)` line
becomes a `yield Msg::X` in an `async_stream::stream!` block.

## Subscribe to documents

```lua
RE:subscribe(function(name, body)
    if name == "stop" then print("run finished:", body) end
end)
RE:run(count({det1}, 5))
```

The callback runs on the REPL thread for every emitted Document
(start / descriptor / event / stop / resource / datum / …). For
document filters: pass an optional second arg, e.g.
`RE:subscribe(cb, "event")`.

## Persist runs to disk

```lua
-- (Inside bsrs-cli the JsonlSink is the easiest writer to wire
-- up from Rust startup; from Lua REPL the engine has no sinks
-- attached by default. Production setups attach sinks during
-- `bsrs qs-manager` startup or via a small Rust glue binary.)
```

For the queueserver-style deployment (Documents fan out over ZMQ to
bluesky `RemoteDispatcher` and Tiled), see
[CLI tour → qs-manager](./cli.md#qs-manager).
