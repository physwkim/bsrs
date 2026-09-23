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
| `ax.dof.{RangeDOF, ChoiceDOF, DOFConstraint}` | `blop::dof::{RangeDof, ChoiceDof, DofConstraint}` with the binding as `DofBinding` |
| `ax.objective.{Objective, ScalarizedObjective, OutcomeConstraint, to_ax_objective_str}` | `blop::objective::{Objective, ScalarizedObjective, OutcomeConstraint, objective_expression}` |
| `ax.agent.Agent` | `blop::agent::Agent`, built over an optimizer factory taking an `ExperimentSpec` |

Optimizer backends (Ax, BoTorch) are not ported; Ax is reached through
the bridge below. Any type implementing `Optimizer` plugs into the plans
(decision D22). The agent renders its
DOFs, objectives and constraints into an `ExperimentSpec` — the arguments of
Ax's `configure_experiment` / `configure_optimization`, serializable as JSON
— and calls a factory with it, so an in-process optimizer or a bridge to an
out-of-process one is configured the same way.

## Optimizer bridge (`blop-bridge`)

The `blop-bridge` feature adds `blop::bridge::BridgeOptimizer`, an
`Optimizer` whose calls go to another process over a ZMQ REQ/REP socket
as JSON. The server is the `bsrs_blop_bridge` Python package in
[`python/`](../python/); it serves any object with blop's `Optimizer`
surface, and ships two backends:

- `random` — uniform random search over the spec, no dependencies beyond
  pyzmq; the reference for tests and a random-search baseline.
- `ax` — `blop.ax.optimizer.AxOptimizer` configured from the spec
  (`pip install blop[ax]`).

```sh
cd python && pip install -e '.[ax]'
python -m bsrs_blop_bridge --endpoint tcp://127.0.0.1:5590 --backend ax
```

```rust
let agent = Agent::builder(dofs, Objective::maximize("intensity"), eval)
    .build(BridgeOptimizer::factory("tcp://127.0.0.1:5590", BridgeOptions::default()))?;
re.run_async(agent.optimize(Some(20), 1)).await?;
```

`connect` sends the agent's `ExperimentSpec` as `configure` and learns the
optimizer's name and which optional protocols it implements
(`register_suggestions`, `stopping_criteria`, `checkpoint`); those answer
the `supports_*` methods, so the plans branch exactly as they do for an
in-process optimizer. One request is one JSON frame,
`{"method": …, "params": …}`, answered by `{"result": …}` or
`{"error": …}`; the methods and their shapes are documented in
`python/bsrs_blop_bridge/server.py`. Values are JSON, so a NaN outcome
crosses as `null`.

The `Optimizer` trait is synchronous and the plans call it from inside a
stream, so a bridge call blocks for up to `BridgeOptions::timeout`
(default five minutes: Ax refits its model before each `suggest`). On a
multi-thread tokio runtime the call runs under `block_in_place`; on a
current-thread runtime it blocks the thread. A call that times out fails
that step and leaves the socket usable: the REQ socket runs relaxed and
correlated, so the next request goes out and a late reply to the old one
is dropped.

`crates/bsrs/tests/blop_bridge.rs` drives the client against a scripted
REP server in a thread; set `BSRS_BLOP_PYTHON=/path/to/python` (an
interpreter with pyzmq) to also run it against the real server's `random`
backend. `python/tests/` covers the server with pytest.

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
- **DOF binding.** blop's `DOF` has optional `name` and `actuator` fields
  and rejects the combinations that make no sense at construction. bsrs
  makes each legal combination a `DofBinding` variant: `Actuator` (a device
  the RunEngine moves), `External` (a name something else sets), `Remote`
  (a queueserver variable name, with the device name when it differs). The
  agent refuses `Remote`, as blop's does a string actuator.
- **Template validation.** A constraint or scalarized objective names its
  inputs by template variables. blop checks each variable with a substring
  test and substitutes with `\b…\b`; bsrs uses one word tokenizer for both,
  so a variable that validates is the one substituted.
- **Agent surface.** The Ax-only methods (`fixed_dofs`,
  `reconfigure_search_space`, `from_checkpoint`, `plot_objective`) and the
  per-agent callback list are not ported; document sinks subscribe on the
  RunEngine.
- **Padding.** `read_step` pads a short step (fewer points than
  `n_points`) with `null`, not NaN, since the event data is JSON.

## Tests not ported

Cases whose precondition cannot exist in the typed port: sensors that are
not `Readable`, unhashable ids, suggestions without an `_id`, and a DOF with
both an actuator and a name or with neither. Cases that exercise Ax itself
(`test_ax_api_consistency_*`, the Ax-client assertions of `test_agent.py`)
are replaced by assertions on the rendered `ExperimentSpec`.
