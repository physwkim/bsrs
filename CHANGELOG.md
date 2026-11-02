# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `bsrs::blop` (feature `blop`) — beamline optimization plans ported from
  NSLS-II blop: the `Optimizer` ask/tell protocol, `OptimizationProblem`, and
  `optimize`, `optimize_in_run`, `optimize_step`, `sample_suggestions`,
  `sample_candidates`, `acquire_baseline`, `default_acquire`,
  `list_scan_in_run`, `navigate_to_best`, `read_step`. Suggestions and
  outcomes are typed, plans that produce a value return a `PlanWithResult`,
  and routing is deterministic nearest-neighbour plus 2-opt. Optimizer
  backends are not included. See `doc/11-blop.md`.
- `bsrs::blop::{dof, objective, agent}` — the search-space layer of blop's
  `ax` package: `RangeDof`/`ChoiceDof` with a `DofBinding`, `DofConstraint`,
  `Objective`/`ScalarizedObjective`/`OutcomeConstraint`, and an `Agent` that
  renders them into a backend-neutral, JSON-serializable `ExperimentSpec`
  and builds its optimizer through a factory over it.
- `bsrs::blop::bridge` (feature `blop-bridge`) — `BridgeOptimizer`, an
  `Optimizer` served by another process over ZMQ REQ/REP as JSON, with the
  `bsrs_blop_bridge` Python server in `python/` (`random`, `ax` and
  `xopt` backends). This is how Ax and Xopt are driven from bsrs. Ax's extras are optional
  `Optimizer` methods forwarded by the agent (`set_fixed_dofs`,
  `reconfigure_search_space`, `set_checkpoint_path`), and
  `BridgeOptimizer::from_checkpoint` plus `Agent::from_optimizer` resume
  from a checkpoint.
- `bsrs::blop::callbacks` — `OptimizationRouter` (forward the `primary`
  stream of optimization runs to a shared, editable sink list) and
  `OptimizationLogger` (plain-text progress tables and running statistics),
  ports of blop's `callbacks` package as `DocumentSink`s.
- Tests ported from ophyd and ophyd-async for signals, devices, `Status`,
  motors, the EPICS backends and detectors. Those that need a live IOC boot
  one of the epics-rs example IOCs (ophyd-test-ioc, mini-beamline,
  sim-detector, qsrv-ioc) and are `#[ignore]`d; run them with
  `BSRS_EPICS_RS=<epics-rs checkout> cargo nextest run -p bsrs --run-ignored
  only` (see `tests/example_ioc`).
- `Status::with_timeout(timeout, settle_time)`, ophyd `StatusBase`'s
  timeout and settle time; `Status::and`, ophyd's `AndStatus`; and
  `Status::contains`.
- Writable `Signal`s are `AsyncMovable`/`MovableObj` and read-write ones
  `Locatable`/`LocatableObj`, so `mv`/`mvr` take a signal as ophyd-async's
  `SignalW`/`SignalRW` do. `Signal::reconnect` forces a new connect.
- `SoftSignalBackend::with_precision`, `with_getter` and `with_setter`
  (ophyd-async soft getter, setter and poll period);
  `MockSignalBackend::reset_put_calls`, `set_values`, `set_units`,
  `set_precision` and `set_put_callback_async` (ophyd-async `set_mock_*`).
- `BsrsError::ValueTimeout`, raised by `wait_for_value` with the signal,
  matcher, timeout and last value seen, and `BsrsError::Connect`, the one
  connect failure shared by every caller that joined it.
- `CaMotor::set_current_position` and `PvaMotor::set_current_position`
  (ophyd `EpicsMotor.set_current_position`).
- `StandardDetector::events_to_kickoff` / `set_events_to_kickoff`,
  `TriggerInfo::effective_exposure_timeout`, `DetectorWriter::hinted_fields`
  and `AreaDetectorCam::acquire_time_rbv` / `acquire_period_rbv`.
- `host::areadetector::PluginSignalDataLogic` (ophyd-async
  `PluginSignalDataLogic`), a `SingleDataLogic` that reads a plugin signal
  with each event after setting the new `AreaDetectorCam::wait_for_plugins`;
  and `NdStats::total`.
- `host::areadetector::NdArrayDescription::new` (ophyd-async
  `NDArrayDescription`) describes a writer's frame from any shape signals,
  such as the new `NdRoi::size_y_rbv` / `size_x_rbv` for a writer fed from
  an ROI.
- `StandardDetector::add_plugin` (ophyd-async `AreaDetector(plugins=...)`)
  and `StandardDetector::connect`, which connects the control (the new
  `DetectorControl::connect`), every writer and every plugin, so a plugin
  the IOC lacks fails the connect. `NdPlugin`, `NdFile`, `NdFileHdf5`,
  `NdStats` and `NdRoi` implement `Device`.
- `StandardDetector` implements `PreparableObj`, so a plan prepares it with
  `plans::prepare(det, json!({...}))`, the value deserialized as
  ophyd-async's `TriggerInfo` model (`TriggerInfo` and `DetectorTrigger`
  implement `Deserialize`).
- `SoftMotor` implements `protocols_async::Stoppable`.
- `BsrsError::Datatype`, ophyd-async's `make_converter` `TypeError`.

### Changed

- **Breaking**: `Status::wait` returns `Result<(), WaitError>`.
  `WaitError::Timeout` is a wait that ran out with the status pending
  (ophyd `WaitTimeoutError`); `WaitError::Status` is a status that finished
  without success. `StatusError::Timeout` carries a `TimedOut` that only the
  `with_timeout` timer builds, so a setter cannot record it, and the
  RunEngine reports a timed-out status as a status failure.
- **Breaking**: `StandardReadable::add_child(Arc<dyn ReadableObj>)` and
  `add_signal(Arc<Signal<T, B, A>>, StandardReadableFormat)` replace
  `add_readables` and return `Result`: a duplicate reading key or hint
  field ("Hint fields {f} overrides existing hint") is an error, as in
  ophyd-async. Stageable children and hinted signals are staged
  automatically, uncached formats read live, and children are gathered
  concurrently.
- **Breaking**: `#[derive(Device)]` parses `#[signal]`/`#[device]` with
  `syn`; an unknown role, key or kind is a compile error instead of
  `Kind::Normal`.
- **Breaking**: a failed connect is `BsrsError::NotConnected`, naming every
  child that failed by field and nesting each sub-device's own failures, as
  ophyd-async's `NotConnectedError`, in place of the first child's error.
  `#[derive(Device)]` `connect_all`, `DeviceVector::connect_all`,
  `StandardDetector::connect`, the areaDetector cam and plugins and the CA
  and PVA motors' setpoint and readback report it, through
  `devices::wait_for_connection` (ophyd-async `wait_for_connection`).
- **Breaking**: `EpicsCaBackend::new` and `EpicsPvaBackend::new` take only
  `f64`, `i64`, `bool`, `String` and a `Vec` of the waveform's own element
  type, as ophyd-async's `Array1D`/`Sequence[str]` (`i16`, `f32`, `u8`,
  `i32`, `f64` or `String` over CA; `bool`, `i8` through `u64`, `f32`,
  `f64` or `String` over PVA), and `connect` refuses with
  `BsrsError::Datatype` a PV the type cannot read, as ophyd-async's
  `make_converter`: `Vec<f64>` on a LONG waveform, `bool` on an enum without
  two choices, `i64` on a float record with a nonzero PREC. Generic code
  builds a backend with `BackendFromPv::from_pv`.
- **Breaking**: areaDetector enum fields (`image_mode`, `detector_state_rbv`,
  `data_type`, `color_mode`, `file_write_mode`) are `String` labels, as are
  the `AD_STATE_*`, `AD_IMAGE_MODE_*`, `AD_COLOR_MODE_*` and
  `AD_FILE_WRITE_MODE_*` constants; `ad_datatype_to_numpy` and
  `NdFile::set_write_mode` take a label, and Lua `cam:state()` and
  `cam:set_image_mode` use labels.
- `CaDetector`, `PvaDetector`, `ca_suspender` and the `CaPositioner` done PV
  read their PV as whatever number it is (ophyd-async `datatype=None`), an
  enum as its index.
- `SoftSignalBackend::set_callback`, and so `Signal::subscribe` and
  `observe_value`, delivers the current reading first, as ophyd-async does;
  readings reach each callback in store order and carry `alarm_severity` 0.
  `MockSignalBackend` delivers its value the same way.
- `put(None)` on the EPICS backends writes the value read at connect, as
  ophyd-async does, instead of `T::default()`.
- `CaEnumBackend` is `EpicsCaBackend<String>`, which reads a DBR_ENUM as its
  label with the choices in the datakey.
- EPICS datakeys take dtype and shape from the datatype as ophyd-async
  `make_datakey` does: `Vec<f64>` is `array`, a string is `|S40`, and an
  empty EGU is reported as units `""`.
- `StandardDetector` follows ophyd-async: `prepare` sets up the hardware and
  opens the writer for `collections_per_event`; `trigger` prepares
  implicitly, rejects a multi-event prepare and fails when the writer index
  stalls past the exposure timeout; `stage` drops the last prepare;
  `kickoff` fails without a prepare since stage ("Prepare not called") and
  cannot run past the prepared frames; `read` and `describe` fail before a
  prepare.
- **Breaking**: `StandardDetector<C>` holds a list of `DataLogic`s
  (ophyd-async `DetectorDataLogic`) in place of its writer parameter `W`:
  `DataLogic::Unbounded` is a `DetectorWriter`, `DataLogic::Single` a
  `SingleDataLogic` read with the event of a one-collection prepare.
  `with_data_logics`, `add_data_logic` and `connect` replace `writer()`,
  and `DetectorWriter::connect` the inherent
  `AdHdfWriter::connect`/`AdMultipartWriter::connect`. `trigger` waits for
  the least writer index, `kickoff` needs a writer and refuses writers that
  disagree on their index, asset documents chain the writers in order, and
  the hints join every data logic's.
- **Breaking**: `CamFrameInfo` is replaced by `NdArrayDescription`, whose
  fields are private; `AreaDetectorCam::frame_info` is an
  `Arc<NdArrayDescription>`.
- **Breaking**: the detector names each data logic's data key
  `name + datakey_suffix`, as ophyd-async does, so a writer's key is `det`,
  not `det_image`. `DetectorWriter::open`/`hinted_fields`,
  `SingleDataLogic::prepare_single`/`hinted_fields` and `open_writer` take
  that name, and both traits gain `datakey_suffix` (`with_datakey_suffix`
  on `AdHdfWriter` and `AdMultipartWriter`). `AdHdfWriter::new`,
  `AdMultipartWriter::{new, jpeg, tiff}`, `BinaryFrameSink::new` and
  `SoftDetectorWriter::new` no longer take a name; a writer collects no
  documents before `open` and leaves the key's `object_name` to the run
  engine.
- **Breaking**: `WritesStreamAssets::collect_asset_docs` returns `Result`,
  and `StandardDetector` fails it without a prepare since stage ("Prepare
  not called"), as ophyd-async does, instead of re-emitting the documents
  of the previous scan's prepare.
- `AreaDetectorCam::arm` resolves once the cam is acquiring, and its
  configuration is described from `AcquireTime_RBV`/`AcquirePeriod_RBV`.
- `CaMotor`, `PvaMotor` and `CaPositioner` press `.STOP` in `stop_dyn`,
  refuse moves outside their limits (ophyd-async `Motor.check_move`, ophyd
  `PVPositioner.check_value`), post `WatcherUpdate`s while moving and hint
  their readback. `PvaMotor` and `PvaDetector` are described from their PVs.
- `spiral_square_pattern` visits points in bluesky's order.
- The `mini_beamline_scan` example builds on `ca_devices` and needs the
  `host` feature.

### Removed

- **Breaking**: `StandardReadable::add_readables`, `add_stageable` and
  `StandardReadableFormat::Child`; use `add_child` / `add_signal`.
- **Breaking**: `CaStringKind` and `EpicsCaBackend::new_long_string`;
  `EpicsCaBackend<String>` reads a CHAR waveform as a long string, and
  `EpicsPvaBackend<String>` an int8 array.
- **Breaking**: `connect_area_detector_hdf` and
  `connect_area_detector_multipart`; use `StandardDetector::connect`.

### Fixed

- A `Read` inside an open run but outside any `Create`/`Save` pair failed the
  run with "read with no open bundle". bluesky's `RunBundler.read` folds a
  reading into the event only while bundling, so such a read is ad-hoc
  inspection that returns the value and touches no document; the engine now
  does the same.
- `EpicsPvaBackend::connect` honours its timeout instead of the client's
  5 s operation timeout.
- The CA decoders read DBR_CHAR, which arrives as `UChar`/`UCharArray`, so
  long strings read; CA reads ask for the current element count, not NELM.
- A PVA put to an NTEnum reaches `value.index`, by label or by index, and a
  PVA bool reads an `enum_t` index.
- A CA PV maps to the same `CaContext` shard on every lookup, so backends on
  one PV share its channel and its one put-callback in flight.
- A CA call waits for the channel with its own timeout, and a CA monitor
  survives a disconnect and resumes when the server returns.
- `Status::observe_watcher` delivers every update to each observer; a
  status's phase, outcome and waiters change under one lock, so a racing
  reader no longer sees an error-less failure or misses a wake.
- A `StandardReadable` child's configuration reaches `read_configuration`.
- `Signal::describe` takes `DataKey.source` from `Signal::source`.
- `AreaDetectorCam::arm` no longer misses acquisitions shorter than a poll,
  and `AdHdfWriter` no longer stalls `kickoff` on a FlushNow put.

## [0.5.8] - 2026-09-22

### Fixed

- An armed queue is retried when a worker frees the queue slot. Autostart was
  attempted only on the events a client raises -- item added, environment
  opened, autostart enabled -- so items added while a plan was running were
  refused for not being idle and then waited forever with the engine free:
  nothing asked again. The releasing worker cannot ask either, its own
  `SlotClaim` being live until it returns, so `QueueTaskSlot` announces the
  release and the rep thread performs the attempt. A failed or stopped item
  still disarms autostart, so the retry starts nothing after one.

## [0.5.7] - 2026-09-22

### Added

- `ServerBuilder::environment_hook` — an `EnvironmentHook` the server runs on
  the engine `environment_open` has just created, before that engine becomes
  the environment. The engine is new on every open, so anything a client
  installed on the last one (the beamline's suspenders, say) is gone, and the
  client is not told the open happened; the hook puts the daemon that owns the
  beamline in charge of preparing each engine. An `Err` from the hook fails
  the open and leaves the environment closed.

## [0.5.6] - 2026-09-21

### Fixed

- A queued item keeps the arguments it was submitted with. `QueuedItem::plan`
  put the whole submitted body into `args`, so a client reading the queue back
  found the plan's metadata nested one level down and rendered the columns it
  fills -- sample, edge energy, point count -- as blank. `args` and `kwargs`
  are now separate fields and `plan_args()` recombines them for `PlanFactory`.
- The running item is reported as the item that was queued. `current_item`
  replaces `current_plan_name` in the server state, so `queue_get` hands back
  the running item beside the pending ones instead of a name; a name is not an
  item, and a client rendering the queue as one table had every column but the
  plan empty on the one row that was actually running.

## [0.5.5] - 2026-09-21

### Changed

- EPICS backends move to epics-rs 0.30.0 (`epics-ca-rs`, `epics-base-rs`,
  `epics-pva-rs`). No bsrs source change was needed; the mini-beamline
  scan, DCM energy and `monitor_during` scripts pass against a mini_ioc
  built from that release.

## [0.5.4] - 2026-09-17

### Fixed

- The queue continues past an item that opened no run. A queued plan whose
  whole job is a side effect (a bare `mv`, a `sleep`) archives exit_status
  `no-run`, which `execute_queue_loop` read as failure and halted on, so a
  batch could not put its sample moves in the queue. `item_completed()` now
  treats `no-run` as finished in both the queue loop and
  `execute_single_item`; `plans_failed` already excluded it.

## [0.5.3] - 2026-09-16

### Fixed

- The queue-server archives why a plan item failed. `run_plan_item` mapped
  the run result to an exit status and dropped `RunResult.reason` (and the
  error on the `Err` arm), so `history_get` told a client `fail` and nothing
  else while the cause existed only in the daemon's log. The archived result
  now carries `reason` on every arm, empty for a clean run.

## [0.5.2] - 2026-09-16

A dependency-refresh release. No bsrs code, API, or behaviour changes.

### Changed

- Based the EPICS CA/PVA backends on `epics-rs` 0.29.3 (from 0.29.2). The
  release makes a record's monitor state advance on every value-class post
  whether or not the field has a subscriber, so a move that finished with no
  `.DMOV` monitor no longer swallows the next subscriber's 1→0 transition,
  which is the done-PV wait every `ca_motor` put relies on. No bsrs source
  change was needed.
- `Cargo.lock` moves `chacha20`, `der` and `spin` off their yanked versions
  (0.10.1, 0.8.0, 0.9.8) to the compatible successors; none is a direct
  dependency and no advisory names the old versions.

## [0.5.1] - 2026-09-15

A dependency-refresh release. No bsrs code, API, or behaviour changes.

### Changed

- Based the EPICS CA/PVA backends on `epics-rs` 0.29.2 (from 0.29.1). The
  release moves the SNL program start gate into `epics-base-rs`
  (`spawn_program` awaits `wait_for_pini` at the one spawn site), which is
  what makes the mini-beamline `kohzuCtl` boot with the right d-spacing; no
  bsrs source change was needed.
- **MSRV is now 1.94** (from a declared 1.80). The old floor had been
  unachievable since 0.24.3: `epics-base-rs` is edition 2024, so no toolchain
  below 1.85 can build the CA/PVA backends at all. 1.94.0 is the channel
  `epics-rs` pins for itself, and the workspace is verified to build and test
  on exactly that toolchain.

### Fixed

- The 0.4.2 section's date: the tag and publish landed on 2026-09-04 (KST),
  a day after the date the changelog carried.

## [0.5.0] - 2026-09-14

Monitorable devices: `Msg::Monitor` now works end to end over the built-in
devices, from the Rust API and from Lua. Interrupts unwind through the plan
so `finalize_wrapper` cleanup runs on abort, suspenders are engine-owned and
persist across runs, every descriptor is shaped from its objects, and
`declare_stream` / `collect` resolve streams the way bluesky's bundler does.
Public signatures change, hence the minor version.

### Changed

- Based the EPICS CA/PVA backends on `epics-rs` 0.29.1 (from 0.28.1). No
  bsrs source change was needed for 0.28.2, 0.29.0 or 0.29.1.
- **Breaking**: `Msg::DeclareStream` carries the stream's objects
  (`StreamObjs::Readable` or `StreamObjs::Collectable`) instead of raw data
  keys, so its descriptor gets their hints and configuration as bluesky's
  `declare_stream(*objs, name=, collect=)` does; `plans::declare_stream(name,
  objs)` and the Lua `msg.declare_stream(name, {devices}, [collect])` follow.
  A collectable must collect into exactly the declared stream.
  `StreamObject::from_data_keys` is replaced by `StreamObject::objectless`,
  used only for the interruptions stream.
- `collect` resolves its stream through what `declare_stream` declared for
  the object, as bluesky's `_declared_stream_names`: a named `collect`
  requires that name to have been declared for the object, an unnamed one
  takes the single declared stream and skips `describe_collect`, and an
  undeclared object still describes and declares its nested streams.
- **Breaking**: `Subscription::new` takes a third argument, the data key of
  the readings it streams, exposed as `Subscription::key`. The engine keys
  every monitor Event by it and fails the run when `describe_dyn` does not
  declare that key.
- **Breaking**: `Registry::register_positioner` additionally requires
  `MonitorableObj` and `StoppableObj` and registers the object under both
  facets.
- `SoftDetector::tick` returns the reading it published.
- **Breaking**: `ContingencySink` holds an `Option<Thrown>` (`Thrown::Error`
  or `Thrown::Interrupt(kind, reason)`) instead of an error string, and
  `contingency_wrapper`'s `except_plan` is an `ExceptPlan` built from the
  `Thrown`, as bluesky's `except_plan(e)` receives the exception.
  `run_wrapper` closes an interrupted run with the interrupt's exit status
  (`abort`, `fail`) rather than dropping it open.
- **Breaking**: `Suspender::trip` is required and the trait gains `watch`,
  `tripped`, `justification`, `pre_plan` and `post_plan`; the reference
  suspenders lose `install(self, re)`. Install through
  `RunEngine::install_suspender(Arc<dyn Suspender>) -> u64` and remove with
  `remove_suspender(id)` / `clear_suspenders()`; installed suspenders persist
  across runs as bluesky's `RE._suspenders` do. `SuspendCallback` moved to
  `bsrs::core`; `suspend_until*` take `&self`.
- **Breaking**: `RunBundler::add_readings` / `add_configuration` are replaced
  by `add_read(StreamObject, readings)`, and `declare_stream` takes
  `Vec<StreamObject>` and returns the `EventDescriptor` it emitted.
- `CaMotor::set_dyn`, `CaPositioner::set_dyn` and `CaDetector` return a
  pending `Status` at once and complete it from a task, so a multi-axis `mv`
  moves its axes together.

### Added

- `MonitorableObj` for `Signal`, `SoftDetector`, `SoftMotor`, `CaDetector`,
  `CaMotor`, `CaPositioner`, `PvaMotor` and `PvaDetector`; motors and
  positioners stream the readback.
- `Registry::register_monitorable` and `Registry::monitorable`. The `bsrs`
  binary, the qs Lua manager and `mini_beamline_qs` register soft detectors
  under the facet, and every Lua device factory reports the `monitorable`
  role.
- `Registry` tracks the stoppable, preparable, configurable and pausable
  facets (`register_stoppable` / `register_preparable` /
  `register_configurable` / `register_pausable` and the matching lookups),
  and the daemon Lua state publishes them, so `stop()`, `msg.prepare`,
  `msg.configure` and `RE:register_pausable` work on registry devices; a
  `--ca-motor` in `bsrs qs-manager` had no `stop()`.
- `CollectableObj::hint_fields` (default `None`); `StandardDetector` forwards
  its readable hint fields, so collect descriptors carry hints as bluesky's
  `maybe_update_hints` gives them.
- `examples/mini_beamline/09_monitor_during.lua`: `bpp.monitor_during` over
  a `ca_motor`, a `ca_detector` and a `pva_detector` against the mini-beamline
  IOC.

### Fixed

- **Engine**: monitor Events are keyed by the device's described data key
  rather than its name, so a detector's `<name>_counts` Event matches its
  Descriptor; a monitor emits the device's current reading as its first Event
  (a new `SignalCache` listener starts with the cached reading pending, as
  ophyd's `subscribe(run=True)` does); `Msg::Unmonitor` delivers an update
  still pending in the subscription before stopping the pump, where the
  abort could drop every Event of a short `monitor_during` scan under CPU
  load.
- **Soft backend**: `SoftDetector` counts on every `read`, so `count` and
  `scan` (which read without triggering) no longer report 0 at every point.
  `SoftDetector`, `SignalCache` and `SoftDetectorControl` store into their
  watch channels with `send_replace`, since `watch::Sender::send` drops the
  value while no receiver is alive (a reading published before the first
  subscriber, a staged cache with no listener, an arm before
  `subscribe_index`).
- **Lua**: subscribers receive worker-thread documents (monitor Events) in
  emission order. The REPL thread replays a subscriber's buffer before each
  of its own deliveries instead of only after `RE:run` returns.
- **Engine**: `stop` and `abort` are thrown into the plan at the message
  they interrupt, so `finalize_wrapper`, `run_wrapper` and `stage_wrapper`
  tails run before the run closes, as bluesky's `RequestStop` /
  `RequestAbort` unwind a generator; `halt` still drops the plan. A pause
  cancels an in-flight `Msg::Wait` instead of applying after the move
  finished, so `stop_on_pause` reaches a motor that is still moving.
- **Suspenders**: a tripped suspender gates the next run's start, records its
  justification (not "pause") on the suspension, runs its pre/post plans, and
  holds the engine until every tripped suspender has released; a manual
  `pause` during a suspension is not lifted by the suspender's release.
- **Descriptors**: `object_keys` lists every object's read keys (an unhinted
  object had no entry) and `declare_stream` descriptors from `collect` and
  `monitor` carry their objects, hints and configuration.
- **CA devices**: `describe_dyn` reports the PV's units, precision and limits
  from DBR_CTRL and sources the key as `ca://<pv>`.
- **Lua**: `LuaDevice` `__tostring` lists the preparable, configurable,
  collectable and pausable roles, and `inspect()` resolves through the
  preparable and configurable facets instead of answering `type = "Unknown"`.

## [0.4.2] - 2026-09-04

A dependency-refresh release. No bsrs code, API, or behaviour changes.

### Changed

- Based the EPICS CA/PVA backends on `epics-rs` 0.28.1 (from 0.24.3). The
  0.26.0 and 0.27.0 API breaks (`AcfCell`, the `PvString` record description,
  `ClientCreds`, the `check_write_request` rename, 18 removed items) do not
  reach anything bsrs calls. `epics-libcom-rs` and `epics-rtems-boot` enter
  `Cargo.lock` as new transitive dependencies of `epics-base-rs`.

### Fixed

- Two `pva_mon` round-trip tests decode through `as_chunks` rather than
  `chunks_exact`, which Rust 1.98's `clippy::chunks_exact_to_as_chunks` fails
  under the `-D warnings` gate.

## [0.4.1] - 2026-07-20

A dependency-refresh release. No bsrs code, API, or behaviour changes.

### Changed

- Based the EPICS CA/PVA backends on `epics-rs` 0.24.3 (from 0.24.2).

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

[0.5.5]: https://github.com/physwkim/bsrs/releases/tag/v0.5.5
[0.5.4]: https://github.com/physwkim/bsrs/releases/tag/v0.5.4
[0.5.3]: https://github.com/physwkim/bsrs/releases/tag/v0.5.3
[0.5.2]: https://github.com/physwkim/bsrs/releases/tag/v0.5.2
[0.5.1]: https://github.com/physwkim/bsrs/releases/tag/v0.5.1
[0.5.0]: https://github.com/physwkim/bsrs/releases/tag/v0.5.0
[0.4.2]: https://github.com/physwkim/bsrs/releases/tag/v0.4.2
[0.4.1]: https://github.com/physwkim/bsrs/releases/tag/v0.4.1
[0.4.0]: https://github.com/physwkim/bsrs/releases/tag/v0.4.0
[0.2.0]: https://github.com/physwkim/bsrs/releases/tag/v0.2.0
[0.1.0]: https://github.com/physwkim/bsrs/releases/tag/v0.1.0
