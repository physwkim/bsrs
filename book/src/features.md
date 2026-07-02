# Optional features

bsrs is a Cargo workspace. Most opt-in functionality lives behind
feature flags so the default build stays small and dependency-free.

## Document sinks

| Crate              | Feature  | Pulls in                              | Use when                              |
| ------------------ | -------- | ------------------------------------- | ------------------------------------- |
| `bsrs-callbacks` | `zmq`    | libzmq + rmp-serde                    | bluesky `RemoteDispatcher` consumers  |
| `bsrs-callbacks` | `tiled`  | tiled-client (HTTP)                   | Tiled catalog ingestion               |
| `bsrs-callbacks` | `kafka`  | pure-Rust `kafka` crate               | Kafka topics, no librdkafka           |

```sh
cargo build -p bsrs-callbacks --features zmq,tiled,kafka
```

## Frame sinks (bsrs-stream)

| Feature  | Pulls in              | Use when                                     |
| -------- | --------------------- | -------------------------------------------- |
| `hdf5`   | rust-hdf5 (pure Rust) | NeXus-flavored HDF5 detector files           |
| `pva`    | epics-pva-rs          | NTNDArray monitor source                     |

`Hdf5FrameSink::new("det", "/data/run.h5", payload_size)` writes
into `/entry/instrument/<name>/data` (chunked, optional gzip) and
emits Resource + Datum docs pointing at the file path. The
`PvaMonitorSource` subscribes to a PVA NTNDArray PV and pushes
frames into a `FramePipe` that fans out to one or more sinks.

## EPICS backends

Both EPICS backends live in the consolidated `bsrs` crate as features
that are **on by default**:

| Crate  | Feature | Behavior without the feature       |
| ------ | ------- | ---------------------------------- |
| `bsrs` | `ca`    | Stub backend that errors on call   |
| `bsrs` | `pva`   | Stub backend that errors on call   |

```sh
# Real backends build by default; opt out for an EPICS-free build:
cargo build -p bsrs --no-default-features
```

`--no-default-features` swaps in the stub backends so the rest of the
workspace compiles cleanly on systems without EPICS. CI build-tests
both the stub and the real paths; live IOC integration testing is on
the roadmap.

## Lua read-side surface

| Crate        | Feature | Adds                                       |
| ------------ | ------- | ------------------------------------------ |
| `bsrs-cli` | `tiled` | `tiled.from_uri(url)` Lua global + methods |

```sh
cargo build -p bsrs-cli --features tiled
```

Inside the REPL:

```lua
local cat = tiled.from_uri("http://localhost:8000")
for _, k in ipairs(cat:keys()) do print(k) end
local run = cat:get("scan_42")
print(run:metadata())
```

All HTTP calls run on bsrs's tokio runtime; the REPL thread
re-enters mlua's reentrant lock, so calls inside Lua plans are
safe.

## Observability

| Crate       | Feature   | Adds                                  |
| ----------- | --------- | ------------------------------------- |
| `bsrs-qs` | `metrics` | Prometheus `/metrics` HTTP listener   |

```sh
bsrs qs-manager --metrics 127.0.0.1:9090
# build first with: cargo build -p bsrs-qs --features metrics
```

Currently exported:

- `bsrs_qs_rpc_calls_total{method=...}`
- `bsrs_qs_rpc_errors_total{method=...}` (when wired)
- `bsrs_qs_queue_depth` (gauge)
- `bsrs_qs_runs_total{exit_status=...}`
- `bsrs_qs_documents_total{name=...}`

Scrape with the standard Prometheus config:

```yaml
scrape_configs:
  - job_name: 'bsrs-qs'
    static_configs:
      - targets: ['localhost:9090']
```
