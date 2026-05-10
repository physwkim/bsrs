# 10 — Roadmap (post-2026-05-10)

This file tracks the items from the comprehensive audit ("cirrus 기능을
완벽하게 하려고 할 때 부족한 점") that **remain unaddressed** after the
spring 2026 milestone push. Items already shipped are listed at the
bottom for reference.

## Tier 1 — production-blocking residue

### 1.1 Live IOC integration test
- **Status**: manual end-to-end verified — `crates/cirrus/examples/mini_beamline_scan.rs`
  drives `epics-rs/examples/mini-beamline` IOC via cirrus's CA backend,
  runs a 17-point scan, and asserts a Gaussian profile from the
  PinHole point detector. Captured docs land in
  `/tmp/cirrus_mini_beamline_*.jsonl`. Run with:
  ```
  cd ~/codes/epics-rs && ./target/release/mini_ioc \
      examples/mini-beamline/ioc/st.cmd &
  cd ~/codes/cirrus && cargo run --example mini_beamline_scan
  ```
- **Remaining**: wrap as a CI-automated test (spawn `mini_ioc`
  inside the test fixture). Blocked on either packaging the IOC
  binary alongside cirrus or vendoring `epics-rs::IocBuilder`.
- **Side fix needed**: `cirrus_backend_epics_ca::ca_context()`
  panics with "Cannot start a runtime from within a runtime" when
  called from inside a tokio runtime — bootstrap path uses
  `block_on` directly. The example works around it by calling
  `ca_context()` once from a sync `main` before spawning the
  runtime; the backend itself should detect `Handle::try_current()`
  and route via `spawn_blocking` when already in a runtime.

## Tier 2 — ecosystem residue

### 2.1 PyO3 layer (M7)
- **Status**: deferred per doc 07 milestone table.
- **Plan**: a thin `cirrus-py` crate that exposes `cirrus.RunEngine`,
  device factories, and a yield-to-Rust plan adapter. Multi-day
  effort; depends on which subset of ophyd-async API to mirror.

### 2.2 More plan-library leaves
- **Status**: count, scan, list_scan, log_scan, grid_scan, scan_nd,
  fly, spiral, spiral_fermat, spiral_square, ramp_plan,
  adaptive_scan, tune_centroid all shipped.
- **Plan**: `tweak` (interactive nudge — needs Lua + msg.input
  integration) and `x2x_scan` (specialty rotation scan, low priority)
  remain.

### 2.3 More preprocessors
- **Status**: plan_mutator, msg_mutator, pchain, run_wrapper,
  inject_md_wrapper, rewindable_wrapper, monitor_during_wrapper,
  stage_wrapper, baseline_wrapper, finalize_wrapper, subs_wrapper
  (no-op — see decision below), relative_set_wrapper,
  print_summary_wrapper, suspend_wrapper, fly_during_wrapper,
  contingency_wrapper, reset_positions_wrapper,
  configure_count_time_wrapper all shipped.
- **Plan**: `lazily_stage_wrapper` (auto-stage on first touch),
  `set_run_key_wrapper` (multi-run plans), `stub_wrapper` (assert
  no open run) remain. Each <1 day.

### 2.4 Real frame-source backends behind D21
- **Status**: `cirrus frame-source` subcommand + Document-plane wire
  format (`ZmqDocumentSource`/`Sink`) shipped (D21 scaffold).
- **Plan**: wire `cirrus-stream::PvaMonitorSource` and
  `Hdf5FrameSink` into the frame-source binary; same for rogue
  (Phase 2 / P2-A/B). Each ~1 day.

## Tier 3 — operational residue

### 3.1 Backup / recovery for in-progress runs
- **Status**: pause/resume is in-process only.
- **Plan**: write `Msg::Checkpoint` cache to disk every N seconds;
  on restart, replay from the latest checkpoint. Multi-day, needs
  format design + integration with `RunEngine::run_async`.

### 3.2 Prometheus metrics + health probes
- **Status**: SHIPPED (`54e0bc8`). `cirrus-qs/metrics` feature exposes
  `/metrics` HTTP endpoint via `metrics-exporter-prometheus`.
  `cirrus_qs_rpc_calls_total{method=...}` instrumented in dispatch.
- **Remaining**: wire queue_depth gauge, run_finished counter, and
  per-document counters at their natural call sites.

### 3.3 Soak / stress tests + criterion benches
- **Status**: criterion benches SHIPPED (`92bc602`) — `plan_loop`
  measures count(N) for N ∈ {1, 10, 100, 1000} (~2µs/Msg) and
  `document_fanout` measures 10-point count with {0,1,4,16,64} subs.
- **Remaining**: long-running soak harness driving 10k+ scans /
  detector frames asserting no leak / no slowdown. ~1 week.

## Tier 4 — UX / docs residue

### 4.1 User manual / migration guide / cookbook
- **Status**: SHIPPED (`1800420`). `book/` mdbook source with
  introduction / quickstart / migration / cli / features /
  operations / architecture chapters.
- **Remaining**: cookbook chapter of common plan patterns + recipes;
  GitHub Pages CI publish step.

### 4.2 Live plot / BestEffortCallback equivalent
- **Status**: none.
- **Plan**: a `cirrus-plot` callback that subscribes to Document
  stream and drives a `plotters` (or `egui`) window for live scan
  visualization. Multi-day GUI work.

### 4.3 Web UI for cirrus-qs
- **Status**: cirrus-qs exposes JSON-RPC over ZMQ; no HTTP/web
  front-end.
- **Plan**: a separate `cirrus-qs-web` axum binary that proxies
  ZMQ → REST + serves a small SPA dashboard. Separate project
  scope.

### 4.4 cirrus-cli REPL UX (autocompletion etc.)
- **Status**: SHIPPED (`a6ed9b9`). `CirrusReplHelper` registers a
  custom rustyline completer with curated keyword list (RE:*, msg.*,
  bp.*, bps.*, bpt.*, bpp.*, tiled.*); persistent history at
  `~/.cirrus_repl_history`.
- **Remaining**: completion of device names introspected from the
  live Lua state (vs. the static keyword list).

## Tier 5 — security residue

### 5.1 RBAC / TLS / audit log
- **Status**: TILED_API_KEY env only; cirrus-qs has no per-method
  ACL; no structured audit log; no TLS termination examples.
- **Plan**: integrate `axum-rustls` for the HTTP probes; add a
  cirrus-qs ACL middleware that consults a `permissions.toml`
  (the `permissions_get` RPC stub already returns a permissive
  default — wire that through the actual dispatcher gates). Each
  multi-day, policy-heavy.

## Tier 6 — Lua residue (intentional limits)

- **`msg.custom`**: `Box<dyn Any>` payload is hard to express
  cleanly from Lua. Rejected for now (use `RE:register_command` +
  Rust-emitted `Msg::Custom` if a Lua plan needs to trigger a
  custom command).
- **`RE:add_preprocessor`**: Plan→Plan callback would require Lua
  to manipulate the cirrus Plan stream type. Not feasible without
  a richer bridge layer; out of scope.

## Shipped in 2026-05 push (reference)

- M0: SuspendBoolHigh / SuspendBoolLow / SuspendThreshold
  reference impls (commit `92433fe`)
- M1: `Hdf5FrameSink` (NeXus layout, dedicated thread, pure-Rust
  rust-hdf5) (`2b9dfa8`)
- M2: `adaptive_scan` + `tune_centroid` plans (`8e47395`)
- M3: CI feature matrix (zmq/tiled/hdf5/pva/EPICS-real builds)
  (`68baaf5`)
- M4: `tiled.*` Lua surface (`041e8ec`)
- M5: `rel_adaptive_scan` + `configure_count_time_wrapper`
  (`0b79946`)
- KafkaSink (`b9547cf`)
- `cirrus doctor` + `cirrus migrate` CLI tools (`f67b79c`)
- M8: cirrus-qs bluesky-queueserver wire compat — task_status,
  task_result, manager_test, permissions_get, manager_version
  (`819bf6e`)
- D21 scaffolding: `ZmqDocumentSource` (SUB side) +
  `cirrus frame-source` subcommand (`dac7c56`)
- REPL Tab completion + persistent history (`a6ed9b9`)
- criterion benches: `plan_loop` + `document_fanout` (`92bc602`)
- Prometheus `/metrics` endpoint behind `metrics` feature
  (`54e0bc8`)
- mdbook user manual: introduction / quickstart / migration / cli /
  features / operations / architecture (`1800420`)
