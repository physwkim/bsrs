# Gap Analysis 02 — Plans, Plan Stubs, Plan Patterns, Preprocessors

**Date:** 2026-06-14  
**Scope:** `crates/cirrus-plans/src/` (lib.rs, patterns.rs, preprocessors.rs)  
**Reference:** `daq/bluesky/src/bluesky/{plans,plan_stubs,plan_patterns,preprocessors}.py`

---

## Executive Summary

cirrus-plans covers roughly 60% of the bluesky surface area.  The core compound plans
(`count`, `scan`, `grid_scan`, `list_scan`, spiral family, `adaptive_scan`,
`spiral_*`, `fly`, `ramp_plan`) are present.  All preprocessor wrappers are present.
The primary gaps are:

- **No `per_step`/`per_shot` hooks** in any plan — the inner loop is hardcoded and
  cannot be customized (P0, PLAN-01).
- **No staging** inside any compound plan — devices are never armed (P0, PLAN-09).
- **Three core plans have divergent algorithms** from bluesky: `ramp_plan`,
  `tune_centroid`, `adaptive_scan` (P0, PLAN-04/05/06).
- **`scan` is 1D only**; bluesky's is N-D (P0, PLAN-03).
- **`mv`/`mvr` are single-motor only**; bluesky accepts parallel multi-motor pairs (P1,
  PLAN-10).
- **Snake traversal absent** from all N-D grid scans and pattern generators (P1,
  PLAN-19).
- **No `md` parameter** on any plan — downstream tools cannot identify scan types or
  motors (P1, PLAN-25).
- `plan_mutator` lacks the (head, tail) insertion pair that bluesky uses in
  `trigger_and_read` (P1, PLAN-21).

Priority counts: **P0: 9 · P1: 19 · P2: 10**

---

## P0 — Correctness / Protocol Divergence / Commonly-Used Feature Entirely Missing

### PLAN-01 — No `per_step`/`per_shot` hooks; no `Checkpoint` in scan inner loops

- **cirrus:** `lib.rs:322–1155` — all plans hardcode `Create → Read* → Save` per step
- **ref:** `plans.py:66,130,484,1026` — every plan accepts `per_step: PerStep | None`
  defaulting to `one_nd_step`/`one_1d_step`/`one_shot`; `plan_stubs.py:1626–1743` for
  the default callbacks
- **Gap:** Bluesky's plans delegate each inner-loop step to a `per_step` callable.
  The default `one_nd_step` / `one_1d_step` emit `Msg("checkpoint")` before each step
  and call `trigger_and_read` (which supports `drop` on exception).  `count` delegates
  to `one_shot` which emits `Msg("checkpoint")` before each shot.  Without these hooks:
  (a) the inner loop is fixed and cannot be customized without rewriting the plan;
  (b) no `Checkpoint` is emitted at step boundaries so pause-and-resume fails to
  rewind correctly; (c) re-triggering vs read-only detectors cannot be distinguished
  per-shot.  This is the primary extensibility mechanism in bluesky.
- **Fix sketch:** Add `per_step: Option<Box<dyn Fn(…) -> Plan + Send>>` to `scan`,
  `grid_scan`, `list_scan`, `scan_nd`, `inner_product_scan`.  Add stubs
  `one_1d_step`, `one_nd_step`, `one_shot`, `move_per_step` that emit `Checkpoint`
  then call `trigger_and_read`.  Default plans call the stub when `per_step` is `None`.
  Add `per_shot` to `count`.
- **Effort:** L

---

### PLAN-02 — `count` missing `delay`, `per_shot`, and `num=None` infinite mode

- **cirrus:** `lib.rs:322–341`
- **ref:** `plans.py:66–129` — `num: int | None = 1`, `delay: ScalarOrIterableFloat = 0.0`,
  `per_shot: PerShot | None = None`
- **Gap:** Bluesky `count` supports inter-shot delay (scalar or per-shot iterable), a
  `per_shot` hook, and `num=None` for indefinite acquisition until canceled.  Cirrus
  `count` always uses `num` repetitions with no delay and no hook.
- **Fix sketch:** Add `num: Option<usize>`, `delay: Option<Duration>`,
  `per_shot: Option<Box<dyn Fn(…) -> Plan>>`.  Use `bps::repeat` (see PLAN-26) for the
  loop body.
- **Effort:** M

---

### PLAN-03 — `scan` is 1D single-motor; bluesky `scan` accepts N motors (inner product)

- **cirrus:** `lib.rs:377–419` — `scan(dets, motor, reader, start, stop, num)`
- **ref:** `plans.py:1185–1291` — `scan(dets, *args, num=N)` where `args` is
  `motor1, start1, stop1, motor2, start2, stop2, …` then N; delegates to `scan_nd`
- **Gap:** Bluesky's `scan` moves all specified motors simultaneously (inner product).
  Cirrus's `scan` is 1D only.  The existing `inner_product_scan` covers the N-D case
  but uses a separate API surface; the canonical `scan` name is wrong.  Multi-motor 1D
  scans (e.g. coupled goniometer + detector arm) are standard beamline patterns.
- **Fix sketch:** Replace `scan(dets, motor, reader, start, stop, num)` with
  `scan(dets, axes: Vec<ScanAxis>, num)` delegating to `inner_product_scan`; keep the
  1D convenience form as an alias.  Align naming with bluesky.
- **Effort:** M

---

### PLAN-04 — `ramp_plan` algorithm divergence — status-driven vs fixed-sample-count

- **cirrus:** `lib.rs:729–757` — runs `go_plan` then loops `samples` times, sleeping
  `period` each iteration
- **ref:** `plans.py:2214–2302` — loops `while not status.done`, supports
  `take_pre_data`, `timeout`, `period`-rate-limiting, monitors `monitor_sig`
- **Gap:** Bluesky `ramp_plan` polls until the ramp Status object reports completion,
  providing a `take_pre_data` pre-shot, optional `timeout`, and minimum-period
  rate-limiting.  Cirrus runs exactly `samples` iterations with no completion
  monitoring.  A ramp that finishes early over-samples; a slow ramp silently
  stops before completion.
- **Fix sketch:** Return a `StatusHandle` from `go_plan`; loop on `Msg::Wait(timeout=period, error_on_timeout=false)` until status completes; add `take_pre_data: bool`, `timeout: Option<Duration>`, `period: Option<Duration>`.  Wrap body with `monitor_during_wrapper([monitor_sig])`.
- **Effort:** L

---

### PLAN-05 — `tune_centroid` is single-pass; bluesky iteratively refines with shrinking range

- **cirrus:** `lib.rs:1037–1100` — one uniform scan, one centroid computed, one final move
- **ref:** `plans.py:873–1023` — loops `while abs(step) >= min_step`, re-centers range
  on centroid each pass, reduces range by `step_factor`, supports `snake`
- **Gap:** Bluesky's `tune_centroid` is a multi-pass iterative algorithm.  It re-scans
  within a progressively smaller window centered on the signal centroid until the step
  size reaches `min_step`.  `step_factor`, `num`, `snake`, and `min_step` all control
  convergence.  Cirrus's version cannot converge on a peak — it makes one pass and
  stops.  For any broad peak whose centroid requires sub-step-size accuracy, the single-
  pass result is wrong.
- **Fix sketch:** Reimplement the inner loop as `while abs_step >= min_step`, tracking
  `sum_I`/`sum_xI` per pass and computing `new_start = centroid - new_range/2`.  Add
  `min_step: f64`, `step_factor: f64 = 3.0`, `snake: bool = false`.
- **Effort:** M

---

### PLAN-06 — `adaptive_scan` step-sizing algorithm diverges from bluesky

- **cirrus:** `lib.rs:945–1027` — halves step on `|delta| > target * 1.5`, doubles on
  `|delta| < target * 0.5`; no slope, no smoothing, no `threshold`
- **ref:** `plans.py:673–799` — `slope = dI/step; new_step = clip(target_delta/slope,
  min, max)`; backstep when `new_step < step * threshold` (default 0.8); exponential
  smoothing `step = 0.2*new_step + 0.8*step`
- **Gap:** Bluesky computes step size from the measured signal gradient (slope-normalized),
  providing smooth adaptation to actual local curvature.  Cirrus uses a crude halving/
  doubling rule that can oscillate around narrow features.  Backstep threshold is
  hard-coded at 1.5×/0.5× in cirrus vs configurable `threshold` in bluesky.  Bluesky
  also emits `Msg("checkpoint")` before each step (via `bps.mv`); cirrus does not.
- **Fix sketch:** Replace halving/doubling with `slope = (n - p).abs() / step; new_step = clip(target_delta / slope, min_step, max_step)` (guarded for slope=0).  Add `threshold: f64 = 0.8` param.  Apply exponential smoothing.  Emit `Checkpoint` before each position.
- **Effort:** M

---

### PLAN-07 — `fly` handles only single flyer; bluesky `fly` handles a list of flyers

- **cirrus:** `lib.rs:889–927` — single `FlyableObj` + single `CollectableObj`
- **ref:** `plans.py:2305–2338` — `fly(flyers: list[Flyable])`, kicks off all, completes
  all, collects all
- **Gap:** Bluesky kicks off all flyers into one group, waits once, completes all into
  another group, waits, collects all.  Cirrus handles exactly one flyer and mixes
  staging into the plan body (bluesky uses `stage_decorator` externally).
- **Fix sketch:** Accept `Vec<(Arc<dyn FlyableObj>, Arc<dyn CollectableObj>)>`; fan out
  `Kickoff` for each into group "kick", wait; fan out `Complete` for each into group
  "done", wait; then collect each.  Remove inline staging (see PLAN-09).
- **Effort:** S

---

### PLAN-08 — `collect_while_completing` stub missing

- **cirrus:** not present
- **ref:** `plan_stubs.py:1013–1046` — `collect_while_completing(flyers, dets, flush_period, stream_name, watch)`
- **Gap:** This is the idiomatic bluesky pattern for streaming flyers: call
  `complete_all`, then loop `wait(timeout=flush_period, error_on_timeout=False) →
  collect(dets)` until all flyers are done.  Without it, streaming flyer data must be
  collected in a single bulk call after completion, which can accumulate unbounded
  in-memory data.
- **Fix sketch:** Add stub that issues `Msg::Complete` for each flyer into a group,
  then loops emitting `Msg::Wait(error_on_timeout=false, timeout=flush_period)` and
  `Msg::Collect` until done.  Requires `Msg::Wait` to return a "done" flag (see PLAN-24).
- **Effort:** S

---

### PLAN-09 — No staging in any compound plan — devices never armed/configured

- **cirrus:** `lib.rs:322–1155` — no plan emits `Stage`/`Unstage`
- **ref:** `plans.py:489,614,748,etc.` — every compound plan wraps body with
  `@bpp.stage_decorator(list(detectors) + [motor])`
- **Gap:** Bluesky stages all detectors and motors before executing the plan body and
  unstages in LIFO order after.  Staging is where detectors arm, motors enable limits,
  etc.  Cirrus plans never stage/unstage.  Any device that requires staging to function
  correctly will fail silently when used in a cirrus plan.
- **Fix sketch:** Either (a) wrap each compound plan body with `stage_wrapper(inner, devices + motors)`, or (b) document explicitly that callers are responsible for staging via `stage_wrapper`.  Option (b) is simpler and keeps plan composition clean; add a top-level note and a test.
- **Effort:** M

---

## P1 — Meaningful Completeness Gaps

### PLAN-10 — `mv` and `mvr` are single-motor only

- **cirrus:** `lib.rs:94–123`
- **ref:** `plan_stubs.py:357–446` — `mv(*args)` accepts `(motor1, val1, motor2, val2, …)` pairs, all in one group
- **Gap:** Bluesky `mv` fires all motors simultaneously into one group and waits once.
  Cirrus is limited to one motor.  Multi-motor parallel moves are a daily pattern at
  beamlines (e.g. set sample position + detector distance concurrently).
- **Fix sketch:** Accept `Vec<(Arc<dyn MovableObj>, f64)>`; issue `Msg::Set` for each
  into a shared group; then `Msg::Wait`.
- **Effort:** S

---

### PLAN-11 — `rel_set` stub missing; `abs_set` lacks `wait: bool`

- **cirrus:** `lib.rs:88–92`
- **ref:** `plan_stubs.py:256–345` — `abs_set(obj, val, group, wait=False)`, `rel_set(obj, delta, group, wait=False)`
- **Gap:** No `wait=True` inline form for `abs_set`.  No `rel_set` at all.
- **Fix sketch:** Add `wait: bool = false` to `abs_set`; add `rel_set(obj, delta, group, wait)` using `locate_dyn` + offset.
- **Effort:** S

---

### PLAN-12 — `trigger`, `kickoff`, `complete` stubs missing `wait: bool`

- **cirrus:** `lib.rs:126–131, 199–215`
- **ref:** `plan_stubs.py:571–604, 793–926` — all have `wait: bool = False`
- **Gap:** Bluesky stubs allow inline wait as a convenience.  Callers who want to wait
  immediately after trigger must add a separate `wait` call.
- **Fix sketch:** Add `wait: bool` param; emit `Msg::Wait` when `true`.
- **Effort:** S

---

### PLAN-13 — `stage`/`unstage` stubs missing `group` and `wait` params

- **cirrus:** `lib.rs:220–231`
- **ref:** `plan_stubs.py:1080–1244`
- **Gap:** Bluesky supports async group staging (`stage(obj, group="g")` → set multiple
  staging in flight → `wait(group="g")`).  Cirrus `stage` always blocks inline.
- **Fix sketch:** Add `group: Option<String>`, `wait: bool` params; emit `Msg::Wait`
  when `wait=true`.
- **Effort:** S

---

### PLAN-14 — `kickoff_all` / `complete_all` stubs missing

- **cirrus:** not present
- **ref:** `plan_stubs.py:837–973`
- **Gap:** Fan-out kickoff/complete for multiple flyers into one group, wait once.
  Needed for PLAN-07 fix and multi-flyer workflows.
- **Fix sketch:** `kickoff_all(flyers, group, wait)` issues `Msg::Kickoff` for each into
  a shared group; mirrors `complete_all`.
- **Effort:** S

---

### PLAN-15 — `rel_spiral`, `rel_spiral_fermat`, `rel_spiral_square` missing

- **cirrus:** not present
- **ref:** `plans.py:1972–2043, 1791–1865, 2144–2211`
- **Gap:** All relative spiral variants absent.
- **Fix sketch:** Snapshot current `(x, y)` position with `locate_dyn`; offset (x_start, y_start) by (0, 0) relative to current position; call the absolute forms with the offset.
- **Effort:** S

---

### PLAN-16 — `rel_log_scan` missing

- **cirrus:** not present
- **ref:** `plans.py:626–670`
- **Gap:** No relative log scan.
- **Fix sketch:** Wrap `log_scan` with `relative_set_wrapper` + `reset_positions_wrapper`.
- **Effort:** S

---

### PLAN-17 — `rel_list_grid_scan` missing

- **cirrus:** not present
- **ref:** `plans.py:359–418`
- **Gap:** `list_grid_scan` has no relative variant.
- **Fix sketch:** Wrap `list_grid_scan` with `relative_set_wrapper`.
- **Effort:** S

---

### PLAN-18 — `x2x_scan` (theta-2theta) missing

- **cirrus:** not present
- **ref:** `plans.py:2341–2400`
- **Gap:** Coupled 2:1 relative inner product scan for diffractometers.
- **Fix sketch:** Implement via `inner_product_scan` with two motors where the second
  motor's start/stop are `start/2, stop/2`.
- **Effort:** S

---

### PLAN-19 — Snake (boustrophedon) traversal absent from all N-D scans and patterns

- **cirrus:** `patterns.rs:39–94` (`outer_product`, `outer_list_product` — no snake);
  `lib.rs:484–644` (`grid_scan`, `list_grid_scan` — no snake)
- **ref:** `plan_patterns.py:289–345` (`outer_list_product(args, snake_axes)`);
  `plans.py:1294–1468` (`grid_scan(…, snake_axes=False|True|list)`)
- **Gap:** Snake traversal reverses alternating fast-axis passes to minimize dead travel.
  Standard at most 2D/3D scanning beamlines.  Neither the pattern generators nor the
  grid scan plans support it.  The docstring in `patterns.rs:37–38` acknowledges this:
  "Per-axis `snake` in this Rust port is **not** implemented."
- **Fix sketch:** Add `snake: bool` to `outer_product` and `outer_list_product`; for
  snaked rows, reverse every other fast-axis pass.  Propagate `snake_axes: bool |
  Vec<usize>` to `grid_scan` and `list_grid_scan`.
- **Effort:** M

---

### PLAN-20 — `spiral` and `spiral_fermat` patterns missing `dr_y` (ellipse aspect) and `tilt`

- **cirrus:** `patterns.rs:102–133` (spiral), `220–250` (spiral_fermat)
- **ref:** `plan_patterns.py:18–77` (spiral has `dr_y=None`, `tilt=0.0`), `200–257`
  (spiral_fermat same)
- **Gap:** Bluesky supports elliptical spirals (`dr_y` changes radial step in y) and
  tilted spirals (`tilt` rotates the coordinate frame).  Cirrus spirals are circular,
  untilted only.
- **Fix sketch:** Add `dr_y: Option<f64>` (default to `dr`), `tilt: f64 = 0.0` to both
  pattern functions.  Apply `dr_aspect = dr_y / dr` scaling to `y`; apply rotation
  matrix `(cos(tilt), sin(tilt); -sin(tilt), cos(tilt))` to final `(x, y)`.
- **Effort:** S

---

### PLAN-21 — `plan_mutator` uses replacement-only API; bluesky uses `(head, tail)` pair

- **cirrus:** `preprocessors.rs:34–54` — `f: &Msg → Option<Plan>` (replace or pass-through)
- **ref:** `preprocessors.py:33–228` — `msg_proc(msg) → (head, tail)` where `head` is
  substituted and `tail` is appended after the original; result of last head message
  is routed back to the calling plan
- **Gap:** Bluesky's `plan_mutator` allows inserting messages both before and after the
  original message, with correct result routing.  It is used by `trigger_and_read` to
  implement the exception path (`drop` on read failure), and by
  `configure_count_time_wrapper` to emit a `set` before the original message then let
  the original through.  Cirrus's `plan_mutator` can only replace a message; it cannot
  insert a tail, so the exception-path and configure-then-passthrough patterns are
  inexpressible.
- **Fix sketch:** Change the closure type to `FnMut(&Msg) -> (Option<Plan>, Option<Plan>)` — (head, tail).  If `head` is `Some`, drain it and route its last response back; then yield `tail` if present.  This is the minimal mechanical change; the coroutine-based send-result-back portion requires a pin-projected future in the async-stream body.
- **Effort:** L

---

### PLAN-22 — `contingency_wrapper` is `finalize_wrapper` alias; missing `except_plan`/`else_plan`

- **cirrus:** `preprocessors.rs:349–351`
- **ref:** `preprocessors.py:508–` (finalize_wrapper uses contingency internally);
  `trigger_and_read` (`plan_stubs.py:1445–1481`) uses `contingency_wrapper(read_plan(), except_plan=exception_path, else_plan=standard_path)`
- **Gap:** Bluesky's `contingency_wrapper` provides try/except/else/finally branching.
  `trigger_and_read` uses it to `drop` the open event bundle when `read` raises an
  exception (instead of calling `save`).  Without this, an exception during `read`
  leaves an open bundle without being dropped — state leakage.
- **Fix sketch:** Add `except_plan: Option<Plan>`, `else_plan: Option<Plan>` params; implement as a state machine tracking whether inner plan completed normally or via error.
- **Effort:** M

---

### PLAN-23 — `subscribe`/`unsubscribe` stubs missing

- **cirrus:** not present
- **ref:** `plan_stubs.py:1247–1295`
- **Gap:** Plans cannot dynamically register document subscribers.  Needed for subs_wrapper (PLAN-38) and for plans that want to observe their own documents.
- **Fix sketch:** Add `Msg::Subscribe { doc_type: String, callback: Arc<dyn Fn(…)> }` and `Msg::Unsubscribe { token: u64 }`; add stubs; implement in engine.
- **Effort:** M

---

### PLAN-24 — `wait` stub missing `watch` parameter

- **cirrus:** `lib.rs:154–160`
- **ref:** `plan_stubs.py:628–659` — `wait(group, timeout, error_on_timeout, watch=(…))`
- **Gap:** Bluesky `wait` accepts additional `watch` groups whose failure triggers
  an error even when the primary group is still running.  Needed for multi-flyer
  choreography.
- **Fix sketch:** Add `watch: Vec<String>` to `Msg::Wait` and `stubs::wait`.
- **Effort:** S

---

### PLAN-25 — No `md` parameter on any compound plan; minimal RunMetadata

- **cirrus:** `lib.rs:322–1155` — only `plan_name` is set in `RunMetadata`
- **ref:** `plans.py:66,107–116` — every plan builds a `_md` dict with `detectors`,
  `motors`, `num_points`, `num_intervals`, `plan_args`, `plan_name`, `hints`
- **Gap:** Bluesky run metadata is consumed by the Best-Effort Callback (auto-plots),
  data catalogs (Tiled, Databroker), and the queue server.  Without `detectors`,
  `motors`, `num_points`, and `hints.dimensions`, downstream tools cannot label axes,
  route documents, or reconstruct scans.  This affects every plan.
- **Fix sketch:** Extend `RunMetadata` with `detectors: Vec<String>`, `motors: Vec<String>`, `num_points: Option<usize>`, `plan_args: HashMap<String, Value>`, `hints: HashMap<String, Value>`.  Populate each plan's `_md` equivalent.  Add optional `md: Option<RunMetadata>` parameter to every plan that merges user overrides.
- **Effort:** M

---

### PLAN-26 — `repeat` stub (checkpoint + delay loop) missing; `repeater` has no `n=None` infinite mode

- **cirrus:** `lib.rs:299–315` — `repeater(n, plan_fn)` — no checkpoint, no delay
- **ref:** `plan_stubs.py:1746–1825` — `repeat(plan_fn, num, delay)` — emits
  `Checkpoint` before each, sleeps `delay` after; `repeater` supports `n=None`
- **Gap:** The standard bluesky `count` plan delegates to `bps.repeat`.  Without
  `Checkpoint`, pause-and-resume between repetitions doesn't work.
- **Fix sketch:** Add `repeat(plan_fn, num: Option<usize>, delay: Option<Duration>)` stub that emits `Checkpoint`, runs plan, then optionally `Sleep`; add `None` infinite mode to `repeater`.
- **Effort:** S

---

### PLAN-27 — `rd` stub missing — no plan-level single-scalar read

- **cirrus:** not present
- **ref:** `plan_stubs.py:453–552`
- **Gap:** `rd(obj)` reads a single scalar from a `Readable`, using hints or `Locatable`
  protocol.  Used in plan bodies that need the current detector/motor value inline.
- **Fix sketch:** Add `rd(obj: Arc<dyn ReadableObj>) -> Plan` that calls `read_dyn`, extracts the hint field or single field, and returns the value via a `Msg::ReadScalar` or by updating a shared cell.
- **Effort:** S

---

### PLAN-28 — `list_scan` is single-motor only; bluesky supports multi-motor inner-list-product

- **cirrus:** `lib.rs:423–457`
- **ref:** `plans.py:132–222` — `list_scan(dets, motor1, [pts1], motor2, [pts2], …, per_step=…)`
- **Gap:** Bluesky `list_scan` zips multiple motor position lists (inner product) and
  can accept a `per_step` callback.  Cirrus takes one motor and one `Vec<f64>`.
- **Fix sketch:** Accept `Vec<(Arc<dyn MovableObj>, Arc<dyn ReadableObj>, Vec<f64>)>` and call `inner_list_product`; add `per_step` hook.
- **Effort:** S

---

## P2 — Nice to Have

### PLAN-29 — `broadcast_msg` missing

- **cirrus:** not present
- **ref:** `plan_stubs.py:1488–1520`
- **Gap:** Fan out a single command (e.g. `Trigger`, `Stage`) to multiple objects.
- **Fix sketch:** Simple iterator over objects emitting `Msg(command, obj)`.
- **Effort:** S

---

### PLAN-30 — Decorator variants of preprocessors absent

- **cirrus:** `preprocessors.rs` — wrapper forms only
- **ref:** `preprocessors.py` — `make_decorator` generates `run_decorator`,
  `stage_decorator`, `relative_set_decorator`, `reset_positions_decorator`,
  `baseline_decorator`, `monitor_during_decorator`, `fly_during_decorator`
- **Gap:** Bluesky plan libraries use `@bpp.run_decorator(md=_md)` heavily for
  composing plan factories.  Cirrus has no decorator equivalents; callers must use
  wrappers inline.
- **Fix sketch:** For each wrapper `foo_wrapper(inner, args) -> Plan`, add a
  `foo_decorator(args) -> impl Fn(F) -> Plan` that calls `foo_wrapper(inner_fn(), args)`.
- **Effort:** M

---

### PLAN-31 — `classify_outer_product_args_pattern` / `chunk_outer_product_args` missing

- **cirrus:** not present
- **ref:** `plan_patterns.py:378–527`
- **Gap:** Needed if `grid_scan` is extended to accept the variadic bluesky args format
  `(motor, start, stop, num[, snake])`.
- **Fix sketch:** Not needed until the variadic API is adopted; block until PLAN-19 is done.
- **Effort:** S

---

### PLAN-32 — `locate` plan stub missing

- **cirrus:** `LocatableObj` is used internally in `mvr`, `rel_list_scan`, etc., but
  no `Msg::Locate` exists
- **ref:** `plan_stubs.py:172–192` — `locate(*objs, squeeze=True)`
- **Gap:** Plans cannot read multiple motor positions in one message.
- **Fix sketch:** Add `Msg::Locate(Vec<Arc<dyn LocatableObj>>)` and `stubs::locate`.
- **Effort:** S

---

### PLAN-33 — `tweak` interactive plan missing

- **cirrus:** not present
- **ref:** `plans.py:1599–1682`
- **Gap:** Interactive TTY motor-jog loop.  Low priority for non-interactive automation.
- **Fix sketch:** Implement only if TUI interactive mode is added to cirrus.
- **Effort:** M

---

### PLAN-34 — `wait_for` (asyncio Future) stub missing

- **cirrus:** not present
- **ref:** `plan_stubs.py:1399–1419`
- **Gap:** Low-level asyncio Future wait not surfaced to plans.
- **Fix sketch:** Add `Msg::WaitFor(Vec<Pin<Box<dyn Future + Send>>>)`.
- **Effort:** M

---

### PLAN-35 — `prepare` stub (Preparable protocol) missing

- **cirrus:** not present
- **ref:** `plan_stubs.py:757–788`
- **Gap:** `prepare(obj, *args, group, wait)` for devices that need setup before
  `kickoff`/`trigger`.
- **Fix sketch:** Add `PreparableObj` trait and `Msg::Prepare` variant.
- **Effort:** M

---

### PLAN-36 — `caching_repeater` missing (deprecated but in bluesky API)

- **cirrus:** not present
- **ref:** `plan_stubs.py:1562–1593`
- **Gap:** Materializes a plan to `Vec<Msg>`, re-emits n times.
- **Fix sketch:** Drain plan; emit each `Msg` n times.  Mark deprecated.
- **Effort:** S

---

### PLAN-37 — `input_plan` stub missing

- **cirrus:** not present
- **ref:** `plan_stubs.py:735–754` — `input_plan(prompt) -> str`
- **Gap:** Interactive user input inside a plan.
- **Fix sketch:** Add `Msg::Input { prompt: String }` and stub.
- **Effort:** S

---

### PLAN-38 — `subs_wrapper` is a no-op; should emit subscribe/unsubscribe

- **cirrus:** `preprocessors.rs:232–237` — acknowledged no-op
- **ref:** `preprocessors.py:374–425`
- **Gap:** Once PLAN-23 adds `Msg::Subscribe`/`Msg::Unsubscribe`, this wrapper should
  emit them bracketing the inner plan.
- **Fix sketch:** Implement after PLAN-23.
- **Effort:** S

---

## What Already Matches (no gap)

- `open_run`, `close_run`, `create`, `save`, `drop_bundle`, `read`, `null`,
  `abs_set`, `wait`, `checkpoint`, `clear_checkpoint`, `pause`, `deferred_pause`,
  `resume`, `sleep`, `trigger`, `stop`, `kickoff`, `complete`, `collect`,
  `stage`, `unstage`, `stage_all`, `unstage_all`, `configure`, `monitor`, `unmonitor`,
  `trigger_and_read` (simplified), `one_shot` (simplified), `repeater` (partial):
  all have valid implementations covering the basic protocol.
- `pchain`, `run_wrapper`, `inject_md_wrapper`, `rewindable_wrapper`,
  `monitor_during_wrapper`, `stage_wrapper`, `baseline_wrapper`, `finalize_wrapper`,
  `relative_set_wrapper`, `reset_positions_wrapper`, `suspend_wrapper`,
  `fly_during_wrapper`, `print_summary_wrapper`, `msg_mutator`, `lazily_stage_wrapper`,
  `set_run_key_wrapper`, `stub_wrapper`, `configure_count_time_wrapper`:
  all present and functionally adequate.
- `inner_product`, `outer_product`, `inner_list_product`, `outer_list_product`,
  `spiral`, `spiral_square_pattern`, `spiral_fermat_pattern`:
  all present (snake/dr_y/tilt gaps noted in P1/P2 above).
- `inner_product_scan`, `scan_nd` (pre-computed), `list_grid_scan`, `rel_scan`,
  `rel_list_scan`, `rel_grid_scan`, `log_scan`, `spiral`, `spiral_fermat`,
  `spiral_square`, `adaptive_scan`, `rel_adaptive_scan`:
  present (algorithm / multi-motor gaps noted above).
