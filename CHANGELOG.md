# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-07-18

Hardening release: closes a family of queue-server correctness and shutdown
races, resolves engine interrupt/pause edge cases, fixes the HDF5 stream writer
and frame sources, and bounds the on-disk crash-recovery journal so it can no
longer grow without limit.

### Added

- **Checkpoint journal size rotation.** The crash-recovery journal
  (`~/.bsrs/checkpoints.jsonl`) now rotates at a byte threshold
  (`DEFAULT_MAX_BYTES`, 4 MiB) to a single `.1` backup, bounding total on-disk
  size to roughly twice the threshold regardless of run count.

### Changed

- Based the EPICS CA/PVA backends on `epics-rs` 0.24.2 (from 0.24.0).
- Bumped `rust-hdf5` from 0.2 to 0.3.

### Fixed

- **Queue server**: stop the queue and disable autostart when an item cannot
  start, instead of silently skipping it; a generation-stamped queue-task slot
  closes the spawn/register/stale-clear abort-handle races; wire the documented
  but previously never-recorded metrics (queue depth, run/document counters);
  run the REP loop on a dedicated thread so a leaked server can no longer
  deadlock runtime drop; guard `environment_close` against live execution and
  validate `environment_destroy` before side effects; run `queue_item_execute`
  through the worker machinery rather than blocking the REP thread; make
  `queue_autostart` actually start the queue and validate its arguments instead
  of silently disabling; reject `re_pause` when no plan is running; derive
  paused manager/RE state live from the engine.
- **Engine**: resolve interrupts landing inside cancellable handlers by flag
  rather than surfacing them as plan failures; reset `deferred_pause` at run
  start like the other interrupt flags.
- **Stream**: create the HDF5 dataset on its leaf group instead of via a
  file-rooted deep path (deep-path creation produced h5py-unreadable files);
  use a `std` `Mutex` in the `FrameSource` impls so `frames()` works when called
  from a runtime thread.
- **Checkpoint journal**: bound the journal by wall-clock (mid-run checkpoints
  coalesce to at most one per second per run) and read only the file tail on
  startup, making boot cost independent of journal size.
- **Doctor**: flag a 4xx response from the tiled probe instead of reporting
  `[ok]`.

## [0.3.0] - 2026-07-06

Feature release on top of the consolidated single-crate workspace: a much
larger plan/engine surface closer to bluesky, an IOC-backed areaDetector
writer, queue-server batch operations, and an IPython-level interactive REPL.
In-process Python bindings are cancelled — bsrs is Rust-only.

### Added

- **IPython-level interactive REPL** (`bsrs repl`), migrated to the `reedline`
  line editor: live Lua syntax highlighting; a completion menu (Tab) that
  learns your globals, table fields, and `UserData` methods (`det1:read`,
  `RE:run`); fish-style history autosuggestion; Ctrl-R history search; true
  in-place multi-line editing; and `name?` / `name??` value introspection
  (type, signature, fields / methods).
- **`bsrs console`** — a fused local Lua REPL and queue-server sharing one
  engine.
- **Plans**: N-motor inner-product `scan` / `list_scan_nd` (PLAN-28), multi-motor
  `mv` / `mvr` (single barrier), `snake` (boustrophedon) N-D grid traversal,
  ring-based `spiral` / `spiral_fermat` with `dr_y` + tilt (PLAN-20),
  slope-normalised `adaptive_scan`, multi-pass `tune_centroid`, status-driven
  `ramp_plan`, `fly` over a list of flyers, `collect_while_completing`
  (PLAN-08), typed `broadcast_msg` fan (PLAN-29), `count` delay + infinite
  (`num=None`) mode, `per_step` / `per_shot` hooks, `SupplementalData`
  preprocessor, and a real `contingency_wrapper`.
- **Engine**: a plan↔engine response channel (`Respond` / `MsgResult`) for
  value-returning messages; multiple concurrent runs keyed by run (ENG-04);
  richer `RunResult` (all run uids, interrupted, reason, exception); suspender
  `pre_plan` / `post_plan` injection; plan start gated on already-tripped
  suspenders; `plan_type`, scan metadata, and `plan_args` stamped into
  RunStart; external-asset documents emitted on step-scan save (ENG-02).
- **Queue server**: positional + atomic batch `queue_item_add_batch` (QS-09)
  and `queue_item_move_batch` / reorder (QS-20); populated `plans_allowed`
  parameter schema (QS-03); caller-group-filtered `devices_allowed` (QS-14);
  queue-item metadata forwarded into the run (PLAN-25).
- **areaDetector / host**: an IOC-backed areaDetector HDF writer and composite
  `AreaDetector` (DB-19); NDAttribute dataset discovery + emission; a
  JPEG/TIFF multipart writer; frame shape/dtype discovery from RBVs;
  `chunk_shape` StreamResource parameters; ophyd `stage_sigs` and AD
  plugin-selection staging.

### Changed

- **BREAKING: `scan` is now N-motor (inner product).** The single-motor form
  is `scan_1d`.
- The REPL line editor moved from `rustyline` to `reedline`.
- `run_async` now rejects a concurrent call, enforcing the single-plan
  invariant.

### Removed

- **`bsrs-py` removed; in-process Python bindings cancelled** — bsrs is
  Rust-only. Python consumers connect over the wire protocol
  (`ZmqDocumentSink` → `RemoteDispatcher`) and the queue-server worker; the
  bsrs/Python boundary is the Document, not the RunEngine, so no PyO3 surface
  is needed.

### Fixed

- Engine: re-emit a stream's descriptor when a member object is reconfigured;
  the engine (not the writers) owns `StreamDatum` seq_nums; stamp
  `StreamResource.run_start` on the emission path; backstop-collect
  uncollected flyers before RunStop; validate asset drains like bluesky.
- Host: `FlushNow` is NDFileHDF5-only; treat Acquire/Capture as busy records
  (watch RBVs, not callbacks); file URIs carry the localhost authority; AD
  DataKeys carry the shape prefix and `uri` source; read frame shape in
  `[Z, Y, X]` order + ColorMode.
- Tests deflaked and Windows CI enabled (ephemeral TCP instead of unix IPC;
  unified OS matrix).

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

[0.4.0]: https://github.com/physwkim/bsrs/releases/tag/v0.4.0
[0.2.0]: https://github.com/physwkim/bsrs/releases/tag/v0.2.0
[0.1.0]: https://github.com/physwkim/bsrs/releases/tag/v0.1.0
