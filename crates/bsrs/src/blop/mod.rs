//! Beamline optimization on the RunEngine — bsrs's port of
//! [blop](https://github.com/NSLS-II/blop) (feature `blop`).
//!
//! The port covers blop's orchestration layer: the [`Optimizer`] ask/tell
//! protocol, the [`OptimizationProblem`] that binds an optimizer to
//! actuators, sensors and an evaluation function, and the plans that drive
//! it (`optimize`, `optimize_in_run`, `optimize_step`, `sample_suggestions`,
//! `acquire_baseline`, `default_acquire`, `list_scan_in_run`,
//! `navigate_to_best`), and the search-space layer blop's `ax` package
//! defines around them: [`Dof`], [`Objective`], the two constraint kinds,
//! and the [`Agent`] that binds them to an optimizer. Optimizer backends are
//! not part of this module; any type implementing [`Optimizer`] plugs in,
//! and the agent hands it the backend-neutral [`ExperimentSpec`]. Feature
//! `blop-bridge` adds `bridge::BridgeOptimizer`, which serves that spec to
//! an out-of-process optimizer (Ax, through the `bsrs_blop_bridge` Python
//! package) over ZMQ.
//!
//! Differences from blop that a user porting a workflow will meet:
//!
//! - Suggestions and outcomes are typed ([`Suggestion`], [`Outcome`],
//!   [`TrialId`]) instead of `"_id"`-keyed mappings, so the id presence and
//!   hashability checks are gone; duplicate ids and mismatched outcome ids
//!   still fail the plan.
//! - A plan that produces a value (`default_acquire`, `optimize_step`,
//!   `list_scan_in_run`, …) returns a [`PlanWithResult`]: the plan plus a
//!   receiver that resolves once the plan has run.
//! - Actuators take `f64` setpoints only, as bsrs's `Msg::Set` does. A
//!   non-numeric parameter fails the acquisition; a choice-valued degree of
//!   freedom needs a custom `per_step`.
//! - Routing uses deterministic nearest-neighbour plus 2-opt instead of
//!   networkx's simulated annealing (see [`routing`]).
//! - `read_step` pads a short step with `null`, not NaN.
//! - A DOF's binding is an enum ([`DofBinding`]) rather than blop's
//!   optional `name` plus optional `actuator`, so the rejected combinations
//!   cannot be written. Constraint and scalarized
//!   objective templates are validated by whole word, where blop tests for
//!   a substring.
//! - The agent takes an optimizer factory over the `ExperimentSpec` instead
//!   of constructing Ax's client. Ax's extras (`fixed_dofs`,
//!   `reconfigure_search_space`, `checkpoint_path`) are optional
//!   [`Optimizer`] methods the agent forwards, and `from_checkpoint` is the
//!   optimizer's to offer, then
//!   [`Agent::from_optimizer`](crate::blop::agent::Agent::from_optimizer).
//!   `plot_objective` and the per-agent callback list are not ported;
//!   blop's callbacks are
//!   [`DocumentSink`](crate::engine::sink::DocumentSink)s in
//!   [`callbacks`], subscribed on the RunEngine.
//! - Each blop run key is a bsrs run: bluesky's `set_run_key_wrapper` sets
//!   `msg.run`, bsrs's only records `run_key` in the start document, so
//!   [`in_run_wrapper`] routes each run's messages through
//!   [`Msg::InRun`](crate::core::Msg::InRun). That is what lets a
//!   `default_acquire` scan open its own run inside an `optimize` run.

pub mod agent;
#[cfg(feature = "blop-bridge")]
pub mod bridge;
pub mod callbacks;
pub mod dof;
mod expr;
pub mod objective;
pub mod plans;
pub mod protocol;
pub mod routing;
pub mod stubs;
pub mod tracking;

pub use agent::{Agent, AgentBuilder, ExperimentSpec};
pub use callbacks::{OptimizationLogger, OptimizationRouter, RunningStats, SinkList};
pub use dof::{
    ChoiceDof, Dof, DofBinding, DofConstraint, ParameterConfig, ParameterType, RangeDof, RangeType,
    Scaling,
};
pub use objective::{
    objective_expression, Objective, Objectives, OutcomeConstraint, OutcomeName,
    ScalarizedObjective,
};
pub use plans::{
    acquire_baseline, default_acquire, optimize, optimize_in_run, optimize_step, sample_candidates,
    sample_suggestions, StepOptions, StepResult, DEFAULT_ACQUIRE_RUN_KEY, OPTIMIZE_IN_RUN_KEY,
    OPTIMIZE_IN_RUN_TRACKING_STREAM, OPTIMIZE_RUN_KEY, SAMPLE_SUGGESTIONS_RUN_KEY,
};
pub use protocol::{
    collect_optimization_metadata, shared, validate_outcomes, validate_suggestions, AcquireArgs,
    AcquisitionPlan, AcquisitionUid, Actuator, BestPoint, Candidate, EvaluationFunction,
    OptimizationProblem, Optimizer, Outcome, Params, PlanWithResult, SearchSpace, Sensor,
    SharedOptimizer, Suggestion, TrialId, ID_KEY,
};
pub use routing::route_suggestions;
pub use stubs::{
    in_run_wrapper, list_scan_in_run, navigate_to_best, read_step, seq_read, ACQUISITION_UID_KEY,
    SUGGESTION_IDS_KEY,
};
pub use tracking::{readable_cache, ReadableCache, Source, TrackingReadable};
