# CLI tour

The `cirrus` binary aggregates several subcommands.

```text
$ cirrus --help
Usage: cirrus <COMMAND>

Commands:
  qs-manager    Start a cirrus-qs server (replacement for `start-re-manager`)
  qs            REQ-side client (replacement for `qserver`)
  repl          Interactive Lua REPL with cirrus types pre-registered
  doctor        Validate the local environment
  migrate       Inspect / migrate cirrus's on-disk state directory
  frame-source  Run a frame-source process (D21 multi-process IPC)
```

## qs-manager

```sh
cirrus qs-manager \
    --control   tcp://*:60615  \
    --documents tcp://*:60625  \
    --metrics   127.0.0.1:9090 \
    --soft-detectors 2 --soft-motors 2
```

Drop-in replacement for `start-re-manager`. Speaks the
bluesky-queueserver JSON-RPC dialect on the control REP socket;
fans Documents out on the document PUB socket. Implements ~30 RPC
methods (status, ping, queue_*, plans_*, devices_*, environment_*,
re_*, history_*, lock_info, task_status, task_result,
permissions_get, manager_test, manager_version).

`--metrics ADDR` enables a Prometheus `/metrics` HTTP listener; the
binary must be built with `--features cirrus-qs/metrics`.

## qs

```sh
cirrus qs status
cirrus qs queue add count det1 5
cirrus qs queue start
cirrus qs re pause
```

REQ-side client. Mirrors the `qserver` command palette.

### qs inspect

```sh
cirrus qs inspect m1
# {"success": true, "name": "m1", "state": {
#   "type": "SoftMotor", "setpoint": 1.5, "readback": 1.5,
#   "units": "mm", "kind": "Hinted", "subscribers": 0,
#   "connected": true
# }}
```

Dumps a registered device's live state via `device_inspect` RPC.
Calls `NamedObj::inspect_dyn()` server-side; sync, no I/O. The
JSON shape varies by device — the `name` and `type` fields are
always present.

### qs repl

```sh
cirrus qs repl
cirrus qs repl --api-key <KEY>           # for RBAC-gated daemons
cirrus qs repl --no-env-open             # skip auto environment_open
cirrus qs repl --poll-ms 100             # adjust task poll interval
```

Attach an interactive Lua REPL to a running daemon. Each line is
sent to the server's `lua_eval` RPC; the daemon's shared mlua state
runs it and the client polls `task_status` / `task_result`. The
attached state has every registered device pre-published as a Lua
global, so `motor:inspect()`, `motor:set(1.5):wait()`, and
`RE:run(count({det1}, 100))` all work the same as in the local
`cirrus repl`.

`lua_eval` is admin-class under RBAC — pass `--api-key` for an
admin api_key when permissions.toml is configured.

## repl

```sh
cirrus repl
cirrus repl --init ~/.cirrusrc.lua
cirrus repl --script my_scan.lua
```

Interactive Lua REPL backed by an in-process RunEngine. Tab
completion of cirrus globals, persistent history at
`~/.cirrus_repl_history`, slash-style commands (`:help`, `:quit`,
`:reset`, `:script <path>`).

## doctor

```sh
cirrus doctor
cirrus doctor --tiled-url http://localhost:8000 --kafka localhost:9092
```

Sanity-checks the local environment before a beamline session.
Prints one line per check with `[ ok ]`, `[warn]`, or `[fail]`.
Exit code 0 on all-ok / warn-only, 1 if any check failed.

## migrate

```sh
cirrus migrate                          # dry run on default state dir
cirrus migrate --state-dir /opt/cirrus  # custom dir
cirrus migrate --apply                  # actually run migrations
```

Walks the state directory (`~/.cirrus` by default, overridable via
`$XDG_CONFIG_HOME/cirrus`), enumerates recognized state files
(`profiles/`, `runs.jsonl`, `tokens/`, `config.toml`), and applies
versioned migration steps in sequence. Today the step list is
empty — the entry point is in place so future schema breaks have
a place to land.

## frame-source

```sh
cirrus frame-source \
    --output            /data/run-001.h5 \
    --doc-pub-address   tcp://*:5577 \
    --source            pva \
    --source-uri        13SIM1:Pva1:Image
```

D21 multi-process scaffold. Runs a frame source out-of-band from
the RunEngine: writes detector frames locally to disk via
`Hdf5FrameSink` / `BinaryFrameSink`; publishes only Document-plane
messages (`StreamResource` / `StreamDatum`) to the configured
PUB endpoint. The RunEngine process subscribes via
`ZmqDocumentSource` and re-broadcasts.

The acquisition backends (`pva`, `rogue`) are feature-gated and
wired in a future commit; the wire format itself is stable.
