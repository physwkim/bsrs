# Operations guide

How to run bsrs in production: process layout, configuration, and
day-2 maintenance.

## Process layout

A typical beamline deployment runs three or four bsrs processes:

```text
[ qs-manager ]   — control-plane (REQ/REP) + document fan-out (PUB)
[ frame-source ] — D21 acquisition process per detector (optional)
[ tiled (Python) ] — catalog HTTP server (optional)
[ kafka broker ] — durable doc bus (optional)
```

`bsrs qs-manager` is the always-on daemon. The rest are
deployment-specific: small sites can run everything in one process
(DocumentRouter + sinks attached directly to the engine inside qs).

## Service unit (systemd)

```ini
[Unit]
Description=bsrs queueserver
After=network-online.target

[Service]
Type=simple
User=acquire
Environment=EPICS_CA_ADDR_LIST=192.168.50.255
Environment=EPICS_CA_AUTO_ADDR_LIST=NO
Environment=RUST_LOG=info,bsrs_qs=debug
ExecStart=/usr/local/bin/bsrs qs-manager \
            --control   tcp://*:60615 \
            --documents tcp://*:60625 \
            --metrics   127.0.0.1:9090
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
```

Run `bsrs doctor` from the same shell environment as the unit
file to confirm EPICS / Tiled / Kafka are reachable before
starting the unit.

## State directory

bsrs persists a small amount of state under
`$XDG_CONFIG_HOME/bsrs` (or `~/.bsrs` on macOS):

```text
~/.bsrs/
├── config.toml          # default RPC ports, tiled url, kafka brokers
├── runs.jsonl           # rolling run-history index (uid, start, stop, exit)
├── profiles/            # named device-table snapshots
└── tokens/              # auth tokens for outbound HTTP (Tiled, Kafka)
```

`bsrs migrate` walks this directory and runs versioned migration
steps. Today the migration list is empty; the entry point lands so
future schema breaks have a single owner.

## Logging

bsrs uses `tracing` with the `RUST_LOG` env var:

```sh
RUST_LOG=info                                  # default
RUST_LOG=info,bsrs_engine=debug              # engine internals
RUST_LOG=info,bsrs_qs=trace                  # full RPC trace
```

For structured logs to a file:

```sh
RUST_LOG=info bsrs qs-manager 2>> /var/log/bsrs.log
```

Use `tracing-subscriber`'s JSON format if you ship logs to Loki /
ELK — the default formatter is human-readable.

## Metrics

`/metrics` (Prometheus exposition format) is enabled by passing
`--metrics ADDR` to qs-manager (binary built with the `metrics`
feature). See [Optional features](./features.md#observability) for
the metric list.

Suggested alert rules:

```yaml
- alert: BsrsQsErrorRateHigh
  expr: rate(bsrs_qs_rpc_errors_total[5m]) > 0.5
  for: 5m

- alert: BsrsQsQueueStuck
  expr: bsrs_qs_queue_depth > 100
  for: 10m
```

## Backup & restore

The state directory is small (kilobytes) and rsync-friendly. The
authoritative run history lives in your downstream Tiled / Kafka /
JSONL sink, not in bsrs state. Treat `~/.bsrs/runs.jsonl` as
operational, not authoritative.

## Upgrades

```sh
systemctl stop bsrs
cargo install --path crates/bsrs      # or cp the binary in
bsrs migrate --apply                  # run any new schema steps
systemctl start bsrs
```

Document compatibility is part of the public contract: bsrs N+1
emits documents that are forwards-compatible with bluesky readers
that worked against bsrs N.

## Troubleshooting

| Symptom                                | Likely cause                                        |
| -------------------------------------- | --------------------------------------------------- |
| `bsrs doctor` warns on EPICS         | `EPICS_CA_ADDR_LIST` empty or unset                 |
| `qs-manager` exits with `address in use` | another process bound the control / doc port      |
| Documents arrive at consumer with gaps | consumer is not draining its 0MQ queue fast enough; raise SUB HWM |
| `re-pause` returns immediately but engine keeps running | check engine `state` via `status` RPC — pause arrives at next checkpoint, not mid-Msg |
| HDF5 file is empty after a run         | check `bsrs_qs_documents_total{name="datum"}` — frames may be writing to a different sink |

For deeper diagnosis, raise `RUST_LOG=bsrs_qs=trace` for the
duration of one failing scan and capture the resulting log.
