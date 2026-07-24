# Introduction

**bsrs** is a Rust-native re-implementation of the bluesky / ophyd
acquisition stack — RunEngine, devices, plans, document sinks — built
to be the kind of beamline data-acquisition daemon you can deploy
without dragging a Python interpreter onto the IOC host.

## What you get

- A `RunEngine` that mirrors `bluesky.run_engine.RunEngine`: messages,
  states, suspenders, preprocessors, callbacks. Same wire-format
  Documents flow into the bluesky Python ecosystem (databroker,
  Tiled, BestEffortCallback) unchanged.
- An ophyd-async-style device surface (`SignalRW`, `Movable`,
  `Triggerable`, `Stageable`, `Flyable`) plus a sync facade so the
  same plan code works in both worlds.
- Drop-in EPICS CA / PVA backends (`epics-ca-rs`, `epics-pva-rs`)
  with sharded process-singleton registry, in-flight de-dup,
  RAII subscription tokens, and zero-copy NTNDArray decode for
  detector frames.
- A bluesky-queueserver-compatible daemon (`bsrs qs-manager`) that
  speaks JSON-RPC over 0MQ on the same control / document ports.
- An interactive Lua REPL (`bsrs repl`) for prototyping plans
  without recompile.
- Document sinks for the bluesky Python ecosystem: JSONL, ZMQ
  (msgpack/JSON), Tiled (HTTP), Kafka, HDF5 (NeXus-flavored frame
  writer).

## Who is this for

- Beamline scientists / controls engineers who want a faster, more
  predictable RunEngine with the same plan / device surface they
  already know.
- Sites running bluesky who want to keep their Python analysis
  pipelines but move acquisition off Python.
- Authors of new ophyd-style devices in Rust — the backend trait
  set is small and stable (`SignalBackend`, `FrameSource` /
  `FrameSink`, `DetectorWriter`).

## Status (May 2026)

Core engine + plan library + document sinks are production-shape
and tested. Notable opt-in features behind Cargo flags:

All flags below are features of the consolidated `bsrs` crate:

| Feature                          | Notes                                    |
| -------------------------------- | ---------------------------------------- |
| `ca` / `pva` (**default**)       | live EPICS CA / PVA clients; `pva` also enables the NTNDArray monitor source |
| `zmq`                            | bluesky `Publisher` envelope             |
| `tiled`                          | HTTP register + metadata patch via tiled-rs; Lua `tiled.*` read-side namespace |
| `kafka`                          | pure-Rust `rskafka` crate, no librdkafka |
| `hdf5`                           | rust-hdf5 frame writer, NeXus layout     |
| `metrics`                        | Prometheus `/metrics` endpoint           |

Roadmap items still open are listed in `doc/10-roadmap.md`.
