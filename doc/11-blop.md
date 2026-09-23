# 11 — Beamline optimization (`bsrs::blop`)

`bsrs::blop` is a port of the orchestration layer of
[blop](https://github.com/NSLS-II/blop) (NSLS-II's Bayesian beamline
optimization package) onto the bsrs RunEngine. It is gated by the `blop`
Cargo feature and adds no dependencies.

The reference is blop `src/blop/{plans.py, plan_stubs.py, protocols.py,
utils.py}`; the tests in `crates/bsrs/tests/blop_plans.rs` are ported from
`src/blop/tests/test_plans.py` and `test_plan_stubs.py`.

## What is ported

| blop | bsrs |
|---|---|
| `Optimizer` protocol (`suggest` / `ingest` / `best_points`) and the optional `SupportsRegisterSuggestions`, `SupportsStoppingCriteria`, `SupportsCheckpoint` protocols | one `Optimizer` trait; the optional protocols are defaulted methods behind `supports_*` flags |
| `OptimizationProblem` | `OptimizationProblem<U>`, generic over the acquisition identifier |
| `optimize`, `optimize_in_run`, `optimize_step`, `sample_suggestions`, `acquire_baseline` | same names in `blop::plans` |
| `default_acquire`, `list_scan_in_run`, `navigate_to_best`, `read_step`, `seq_read` | same names in `blop::stubs` |
| `InferredReadable`, `Source` | `blop::tracking::{TrackingReadable, Source}` |
| `route_suggestions` | `blop::routing::route_suggestions` |

Optimizer backends (Ax, BoTorch) are not ported. Any type implementing
`Optimizer` plugs into the plans (decision D22).

## Design differences

- **Typed ids.** A suggestion is `Suggestion { id: TrialId, params }` and an
  outcome `Outcome { id, values }`, not an `"_id"`-keyed mapping. The
  presence and hashability checks of blop disappear; duplicate ids and a
  suggestion/outcome id mismatch still fail the plan with blop's messages.
- **Plan return values.** A blop generator `return`s its result. A bsrs plan
  is a stream, so a producing plan returns `PlanWithResult<T>`: the plan and
  a `oneshot::Receiver<T>` that resolves once the plan has run. A caller
  forwards the plan's items and awaits the receiver.
- **Acquisition identifier.** blop's acquisition plan returns "any hashable"
  and the default is chosen by inspecting the function. bsrs makes it the
  type parameter `U: AcquisitionUid`; `String` (a run uid) defaults to
  `default_acquire`, `Vec<TrialId>` defaults to `list_scan_in_run`, and a
  custom `U` supplies its own plan.
- **Run routing.** Every blop plan runs under a run key
  (`default_acquire`, `optimize`, `optimize_in_run`, `sample_suggestions`)
  so an acquisition run can open inside an optimization run. bluesky's
  `set_run_key_wrapper` sets `msg.run` on each message; bsrs's only records
  `run_key` in the start document, so `blop::stubs::in_run_wrapper` wraps
  each run's bundler-touching messages in `Msg::InRun`. Both wrappers are
  applied, so the start document carries `run_key` as in blop.
- **Setpoints are `f64`.** `Msg::Set` carries a number; a non-numeric
  parameter fails the acquisition. A choice-valued degree of freedom needs
  a custom `per_step`.
- **Routing.** Deterministic nearest-neighbour plus 2-opt over the numeric
  parameters, seeded from the actuators' current position, in place of
  networkx's simulated-annealing TSP. Integer parameters are routed too.
- **Padding.** `read_step` pads a short step (fewer points than
  `n_points`) with `null`, not NaN, since the event data is JSON.

## Tests not ported

Cases whose precondition cannot exist in the typed port: sensors that are
not `Readable`, unhashable ids, and suggestions without an `_id`.
