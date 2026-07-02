# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-07-02

Workspace consolidation and EPICS backend modernization. The 18-crate
workspace is now a single `bsrs` crate (plus the `bsrs-derive` proc-macro
companion), and the EPICS Channel Access / PV Access backends build by
default.

### Changed

- **18 crates consolidated into a single `bsrs` crate.** All former
  `bsrs-*` library crates (engine, plans, core, devices, event-model,
  callbacks, qs, host, backends, …) are now modules of one crate behind
  Cargo features; the only remaining companion is `bsrs-derive` (a
  proc-macro crate cannot be a module of a normal crate).
- **EPICS `ca`/`pva` backends build by default** (`default = ["ca", "pva"]`),
  so the default build and CI compile the real backends. Use
  `--no-default-features` for the stub / EPICS-free build.
- **Bumped `epics-base-rs` / `epics-ca-rs` / `epics-pva-rs` 0.16.2 → 0.20.4**
  and migrated to the new API; handle the new `DbFieldType::UChar`
  (`DBF_UCHAR` / `epicsUInt8`) native type in the CA wire encoders.

### CI / tests

- Run the full test suite on Windows alongside Linux and macOS (unified
  3-OS matrix), including `mini-beamline-qs`; bind ephemeral TCP instead of
  Unix IPC in the qs/zmq tests so they run on Windows.
- Deflake the RunEngine pause/suspend/monitor tests via bounded state
  polling instead of fixed sleeps.

### Docs

- Correct the stale backend enable instructions (`--features real` →
  `--features ca` / `--features pva`).

## [0.1.0] - 2026-06-16

Initial release of **bsrs**, a Rust port of the bluesky / ophyd /
ophyd-async data-acquisition stack with EPICS Channel Access and PV Access
backends and a bluesky-queueserver-compatible service. This release brings the
RunEngine, plan library, document model, device layer, and queueserver to
wire- and behaviour-parity with the upstream Python projects.

### RunEngine (`bsrs-engine`)

- Open/close-run state machine: reject a second `OpenRun`, an explicit
  `CloseRun` with no open run, and `Kickoff`/`monitor`/`collect`/`describe`
  before a run is open.
- Bundle integrity: reject colliding data keys within one event bundle,
  reject a `configure`/`checkpoint` issued inside an open bundle, and emit no
  `Descriptor`/`Event` for an empty create/save bundle.
- Monitoring: key monitor pumps by object (not stream name), reject a second
  monitor or an `unmonitor` of a non-monitored object, restore monitors across
  pause/resume, and tear down active monitors when a run closes.
- Rewind/resume: cache `Msg::Wait` and `Msg::Configure` for replay, roll back
  sequence counters on rewind, cancel an open bundle on rewind, reset the
  rewind cache on commit points (stage/unstage/monitor/subscribe) and on a
  `Rewindable` flag change.
- Waiting: wait on a group's statuses concurrently (`FIRST_EXCEPTION`), restore
  group members on a move-on-wait timeout, and propagate status failures
  regardless of `error_on_timeout`.
- Suspenders: `SuspendOutsideBand` + `SuspendWhenChanged`, resume-delay
  (`sleep=`), `clear_suspenders()`, and an `InstallSuspender` watcher that parks
  until paused instead of force-resuming a running engine.
- Documents: `RunStop` carries the caller's abort/halt reason and a
  schema-valid `abort` status on halt; mirror bluesky `ChainMap` md precedence
  in `open_run`; write the resolved `scan_id` back to `RE.md` after each run.
- Introspection: `RE.msg_hook` per-`Msg` hook and a document-type filter for
  subscriptions.

### Plans (`bsrs-plans`)

- Scan family: `rel_*` scans return motors to start, relative moves are based
  on the setpoint (not the readback), relative/reset bases are captured lazily
  at first set, and `scan_nd` skips re-setting an unchanged motor.
- New plans: `rel_list_grid_scan`, `rel_log_scan`,
  `rel_spiral`/`rel_spiral_square`/`rel_spiral_fermat`, `x2x_scan` coupled 2:1
  relative scan, plus `rel_set`, `repeat()`, `prepare()`, and `wait_for` stubs.
- Flyers: `kickoff_all`/`complete_all` fan-out stubs; insert during-run
  wrappers inside the run envelope; skip `fly_during_wrapper` waits when there
  are no flyers.
- Bundling: dedup repeated devices in `trigger_and_read`; skip `Wait` when there
  are no triggerables (bluesky `no_wait` parity); name `monitor_during` streams
  `{signal}_monitor`.
- Rewind: emit per-step `Checkpoint` across the scan family and per-shot
  `Checkpoint` in the count family; mint process-unique default sync groups via
  `short_uid`.

### Core & protocols (`bsrs-core`, `bsrs-protocols-async`)

- `Status` cancellation + `CancelGuard` RAII; `add_callback` fires immediately
  on a cancelled status; back-fill `WatcherUpdate.time_elapsed` when omitted.
- `WatcherUpdate` + structured `Status` progress channel and `Watcher` trait;
  `Status::observe_watcher` driver.
- `SignalBackend::source` gains a `read: bool` flag; `SignalBackend::put`
  takes `Option<T>` and moves wait/timeout to the call layer.
- `FlyMotorInfo` fly-scan motor primitive.

### Devices (`bsrs-devices`, `bsrs-derive`)

- `SignalR`/`SignalW`/`SignalRW` access-role type-state split; `SignalX`
  execute-role signal + `Triggerable`.
- `StandardReadable` + `StandardReadableFormat`; `Device` trait + `DeviceVector`
  collection; `SignalCache` shared monitor + staged caching;
  `walk_signal_sources` device-tree introspection.
- Subscription combinators: `observe_value`, `wait_for_value`,
  `observe_signals_value`; carry `alarm_severity` through monitor callbacks.
- `StandardDetector`: full ophyd-async `TriggerInfo` shape + `DetectorTrigger`
  enum; `stage()` disarms first; `describe` reads cached `DataKey`s without
  re-opening the writer; `complete()` waits for the prepared frame count.
- `bsrs-derive`: `new_named` construction-time bluesky name propagation.

### Document model (`bsrs-event-model`)

- Typed `RunStop.exit_status` (`ExitStatus` enum) and `DataKey.dtype_numpy`
  (`DtypeNumpy` enum); `RunStop` round-trips unknown keys and `data_type`.
- Pages: pack/unpack `event_page` + `datum_page`, `merge_*`/`rechunk_*` for
  event and datum pages, `EventPage.filled` column-store, per-row `EventPage.uid`
  list; reject empty input instead of forging a null-descriptor page.
- `RunBundle`: idempotent `descriptor` per stream, `event_page`, `resource` +
  `ResourceComposer`.
- Schema fidelity: `Limits.rds`, `DataKey.choices`, typed `RunStart`
  data-management fields + `Projections`, optional `Resource.path_semantics`,
  `Event.filled` bool|str foreign keys, `Hints.dimensions` `str | list[str]`,
  `SignalMetadata` + `make_datakey` helper.

### Callbacks (`bsrs-callbacks`)

- `JsonlSink` writes the tagged `{name, doc}` wrapper and flushes each document
  for `JSONLinesWriter` durability; sinks emit the raw doc dict where required.
- ZMQ prefix filter unsubscribes the match-all default.

### EPICS backends (`epics-ca`, `epics-pva`, `soft`)

- `SignalBackend<Vec<f64>>` for numeric CA waveforms and NTScalarArray;
  `DBR_ENUM` backend with label↔index mapping; decode `NTEnum` value to its
  choice label.
- `get_reading` stamps the server time (not the local clock) and propagates
  `alarm_severity`; `get_datakey` reports units/precision/limits from the
  NTScalar; `soft` `get_reading` returns the value's put-time timestamp.

### Queueserver (`bsrs-qs`)

- Replace the JSON-RPC 2.0 envelope with the plain bluesky-queueserver wire
  protocol; `ping` returns the full status dict; probe-byte msgpack encoding on
  the REP socket; ZMQ CURVE encryption.
- Queue ops: `queue_item_add` returns the full item dict and supports
  positional insertion + instruction item types; `queue_item_add_batch` returns
  items + results; `queue_item_update` `replace`; `queue_item_move`
  before/after positional params; items carry user/user_group attribution.
- Status & control: `status_uid`/time/`pause_pending` fields,
  `worker_background_tasks` counter, transitional `manager_state` values,
  `re_runs` per-run `is_open` tracking, `config_get` `ip_connect_info`,
  `re_metadata` key; `manager_stop` graceful shutdown; `environment_destroy`
  force-aborts the running task.
- RBAC: `plans_allowed`/`devices_allowed` return rich dicts filtered by the
  caller's group; `function_execute` with Lua routing.

### Python bindings (`bsrs-py`)

- Expose `RunEngine.subscribe`/`unsubscribe`, soft-device Readable+Movable
  protocol methods, and `grid_scan`/`rel_scan`/`mv` plan factories.

### Documentation

- `doc/gap-analysis/`: bluesky/ophyd/ophyd-async parity gap inventory.

[0.2.0]: https://github.com/physwkim/bsrs/releases/tag/v0.2.0
[0.1.0]: https://github.com/physwkim/bsrs/releases/tag/v0.1.0
