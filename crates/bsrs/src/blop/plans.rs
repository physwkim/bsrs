//! Optimization plans — bsrs's port of `blop.plans`.
//!
//! Every plan here drives an [`OptimizationProblem`]: ask the optimizer,
//! acquire with the problem's acquisition plan, evaluate, tell the optimizer,
//! and record the step as an event. `optimize` opens one run per step plus
//! an outer tracking run; `optimize_in_run` keeps everything in one run.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use futures::StreamExt;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::core::msg::{Msg, RunMetadata};
use crate::core::plan::{plan_items, Plan, PlanItem};
use crate::plans::list_scan_nd;
use crate::plans::preprocessors::{
    contingency_wrapper, msg_mutator, plan_mutator, run_wrapper, set_run_key_wrapper, stub_wrapper,
    ExceptPlan,
};

use super::protocol::{
    collect_optimization_metadata, validate_outcomes, validate_suggestions, AcquireArgs,
    AcquisitionPlan, AcquisitionUid, Actuator, Candidate, OptimizationProblem, Outcome, Params,
    PlanWithResult, Sensor, SharedOptimizer, Suggestion, TrialId, ID_KEY,
};
use super::stubs::{
    capture_run_uid, empty_plan, in_run_wrapper, list_scan_axes, list_scan_in_run, read_step,
    routed, seq_read,
};
use super::tracking::{readable_cache, ReadableCache};

/// `run_key` of the run `default_acquire` opens.
pub const DEFAULT_ACQUIRE_RUN_KEY: &str = "default_acquire";
/// `run_key` of the run `sample_suggestions` opens.
pub const SAMPLE_SUGGESTIONS_RUN_KEY: &str = "sample_suggestions";
/// `run_key` of the tracking run `optimize` opens.
pub const OPTIMIZE_RUN_KEY: &str = "optimize";
/// `run_key` of the single run `optimize_in_run` opens.
pub const OPTIMIZE_IN_RUN_KEY: &str = "optimize_in_run";
/// Stream `optimize_in_run` records each step into.
pub const OPTIMIZE_IN_RUN_TRACKING_STREAM: &str = "optimization";

/// The result of one optimization step.
#[derive(Clone, Debug)]
pub struct StepResult<U> {
    /// What the acquisition plan produced.
    pub uid: U,
    /// The suggestions evaluated, in the optimizer's order.
    pub suggestions: Vec<Suggestion>,
    /// Their outcomes.
    pub outcomes: Vec<Outcome>,
}

/// Per-step settings shared by the optimization plans.
#[derive(Clone, Default)]
pub struct StepOptions {
    /// Points to suggest per iteration; `0` is treated as `1`.
    pub n_points: usize,
    /// Iterations between optimizer checkpoints; `None` or `Some(0)` never
    /// checkpoints. Any other value needs an optimizer with
    /// [`supports_checkpoint`](super::protocol::Optimizer::supports_checkpoint).
    pub checkpoint_interval: Option<usize>,
    /// Readables to record steps with; `None` creates a fresh cache.
    pub readable_cache: Option<ReadableCache>,
    /// Passed through to the acquisition plan.
    pub acquire: AcquireArgs,
}

impl StepOptions {
    fn n_points(&self) -> usize {
        self.n_points.max(1)
    }

    fn cache(&self) -> ReadableCache {
        self.readable_cache.clone().unwrap_or_else(readable_cache)
    }
}

/// The default acquisition: one `list_scan` run over the routed
/// suggestions — blop's `default_acquire`. Produces the run's uid. The start
/// document carries `blop_suggestions` (the routed suggestions) and
/// `blop_acquisition_order` (their ids in scan order); `Checkpoint` messages
/// are dropped so the run cannot be rewound mid-acquisition.
pub fn default_acquire(
    suggestions: Vec<Suggestion>,
    actuators: Vec<Actuator>,
    sensors: Vec<Sensor>,
    args: AcquireArgs,
) -> PlanWithResult<String> {
    let (tx, rx) = oneshot::channel();
    let plan = plan_items(async_stream::stream! {
        let (mut route, ordered) = routed(suggestions, &actuators);
        while let Some(item) = route.next().await {
            yield item;
        }
        let Ok(suggestions) = ordered.await else { return };
        let mut md: HashMap<String, Value> = args.md.into_iter().collect();
        md.insert(
            "blop_suggestions".into(),
            Value::Array(suggestions.iter().map(Suggestion::to_value).collect()),
        );
        md.insert(
            "blop_acquisition_order".into(),
            Value::Array(suggestions.iter().map(|s| Value::from(&s.id)).collect()),
        );
        md.insert("run_key".into(), Value::from(DEFAULT_ACQUIRE_RUN_KEY));
        let axes = match list_scan_axes(&suggestions, &actuators) {
            Ok(a) => a,
            Err(e) => {
                yield PlanItem::from(Msg::Fail(e.to_string()));
                return;
            }
        };
        let scan = list_scan_nd(sensors, axes, args.per_step);
        // bluesky's `list_scan(md=…)` lets the caller's keys override the
        // scan's own, so merge over rather than `inject_md_wrapper`'s
        // insert-if-absent.
        let scan = msg_mutator(scan, move |m| match m {
            Msg::OpenRun(mut meta) => {
                meta.extra.extend(md.clone());
                Msg::OpenRun(meta)
            }
            other => other,
        });
        let scan = set_run_key_wrapper(scan, DEFAULT_ACQUIRE_RUN_KEY);
        let scan = plan_mutator(scan, |m| matches!(m, Msg::Checkpoint).then(empty_plan));
        let (scan, uid) = capture_run_uid(scan);
        let mut scan = in_run_wrapper(scan, DEFAULT_ACQUIRE_RUN_KEY);
        while let Some(item) = scan.next().await {
            yield item;
        }
        if let Ok(uid) = uid.await {
            let _ = tx.send(uid);
        }
    });
    (plan, rx)
}

impl AcquisitionUid for String {
    fn event_value(&self) -> Value {
        Value::from(self.as_str())
    }

    fn default_acquisition_plan() -> Option<AcquisitionPlan<Self>> {
        Some(AcquisitionPlan::new("default_acquire", default_acquire))
    }
}

impl AcquisitionUid for Vec<TrialId> {
    fn event_value(&self) -> Value {
        Value::Array(self.iter().map(Value::from).collect())
    }

    fn default_in_run_acquisition_plan() -> Option<AcquisitionPlan<Self>> {
        Some(AcquisitionPlan::new("list_scan_in_run", |s, a, d, args| {
            list_scan_in_run(s, a, d, args.per_step)
        }))
    }
}

/// Which default acquisition a plan falls back to when the problem names
/// none.
#[derive(Clone, Copy)]
enum Default {
    /// A run per step (`default_acquire`).
    PerStep,
    /// Inside the current run (`list_scan_in_run`).
    InRun,
}

fn acquisition_for<U: AcquisitionUid>(
    problem: &OptimizationProblem<U>,
    default: Default,
) -> Option<AcquisitionPlan<U>> {
    problem.acquisition_plan.clone().or_else(|| match default {
        Default::PerStep => U::default_acquisition_plan(),
        Default::InRun => U::default_in_run_acquisition_plan(),
    })
}

fn no_acquisition<U>() -> String {
    format!(
        "no acquisition plan: the problem names none and `{}` has no default for this plan",
        std::any::type_name::<U>()
    )
}

/// Acquire and evaluate `suggestions`, telling the optimizer about failures
/// on the way — the `try` block of `optimize_step` and `optimize_in_run`.
/// `in_run` rejects run control from the acquisition plan and validates the
/// outcomes inside the guarded region, as `optimize_in_run` does.
fn acquire_and_evaluate<U: AcquisitionUid>(
    problem: OptimizationProblem<U>,
    suggestions: Vec<Suggestion>,
    args: AcquireArgs,
    default: Default,
) -> PlanWithResult<(U, Vec<Outcome>)> {
    let (tx, rx) = oneshot::channel();
    let in_run = matches!(default, Default::InRun);
    let plan = plan_items(async_stream::stream! {
        let Some(acquisition) = acquisition_for(&problem, default) else {
            yield PlanItem::from(Msg::Fail(no_acquisition::<U>()));
            return;
        };
        let (acquire, uid) = acquisition.build(
            suggestions.clone(),
            problem.actuators.clone(),
            problem.sensors.clone(),
            args,
        );
        let acquire = if in_run { stub_wrapper(acquire) } else { acquire };
        let except: ExceptPlan = {
            let optimizer = problem.optimizer.clone();
            let failed = suggestions.clone();
            Box::new(move |_| {
                optimizer.lock().unwrap().register_failures(&failed);
                empty_plan()
            })
        };
        // On a thrown error the wrapper re-raises and this stream is never
        // polled again, so what follows runs only after a clean acquisition.
        let mut guarded = contingency_wrapper(acquire, Some(except), None, None, true);
        while let Some(item) = guarded.next().await {
            yield item;
        }
        let Ok(uid) = uid.await else {
            yield PlanItem::from(Msg::Fail(format!(
                "acquisition plan '{}' ended without producing an acquisition uid",
                acquisition.name()
            )));
            return;
        };
        let evaluated = problem.evaluation_function.call(uid.clone(), suggestions.clone()).await;
        let evaluated = evaluated.and_then(|outcomes| {
            if in_run {
                validate_outcomes(&outcomes, &suggestions)?;
            }
            Ok(outcomes)
        });
        match evaluated {
            Ok(outcomes) => {
                let _ = tx.send((uid, outcomes));
            }
            Err(e) => {
                problem.optimizer.lock().unwrap().register_failures(&suggestions);
                yield PlanItem::from(Msg::Fail(e.to_string()));
            }
        }
    });
    (plan, rx)
}

/// One step of the loop: suggest, acquire, evaluate, ingest — blop's
/// `optimize_step`. Produces the step's uid, suggestions, and outcomes.
pub fn optimize_step<U: AcquisitionUid>(
    problem: &OptimizationProblem<U>,
    n_points: usize,
    args: AcquireArgs,
) -> PlanWithResult<StepResult<U>> {
    let problem = problem.clone();
    let (tx, rx) = oneshot::channel();
    let plan = plan_items(async_stream::stream! {
        let suggested = problem.optimizer.lock().unwrap().suggest(n_points.max(1));
        let suggestions = match suggested.and_then(|s| validate_suggestions(&s).map(|_| s)) {
            Ok(s) => s,
            Err(e) => {
                yield PlanItem::from(Msg::Fail(e.to_string()));
                return;
            }
        };
        let (mut step, result) =
            acquire_and_evaluate(problem.clone(), suggestions.clone(), args, Default::PerStep);
        while let Some(item) = step.next().await {
            yield item;
        }
        let Ok((uid, outcomes)) = result.await else { return };
        let ingested = validate_outcomes(&outcomes, &suggestions)
            .and_then(|_| problem.optimizer.lock().unwrap().ingest(outcomes.clone()));
        if let Err(e) = ingested {
            yield PlanItem::from(Msg::Fail(e.to_string()));
            return;
        }
        let _ = tx.send(StepResult { uid, suggestions, outcomes });
    });
    (plan, rx)
}

/// Checkpoint the optimizer when `iteration` (0-based) completes an
/// interval — blop's `_maybe_checkpoint`. An interval on an optimizer that
/// cannot checkpoint is an error.
fn maybe_checkpoint(
    optimizer: &SharedOptimizer,
    interval: Option<usize>,
    iteration: usize,
) -> crate::core::error::Result<()> {
    let Some(interval) = interval.filter(|i| *i > 0) else {
        return Ok(());
    };
    if !(iteration + 1).is_multiple_of(interval) {
        return Ok(());
    }
    let mut optimizer = optimizer.lock().unwrap();
    if !optimizer.supports_checkpoint() {
        return Err(crate::core::error::BsrsError::Plan(
            "The optimizer is not checkpointable. Please review your optimizer configuration or implementation."
                .into(),
        ));
    }
    optimizer.checkpoint()
}

fn run_md(
    problem_md: BTreeMap<String, Value>,
    plan_name: &str,
    extra: impl IntoIterator<Item = (&'static str, Value)>,
) -> RunMetadata {
    let mut md: HashMap<String, Value> = problem_md.into_iter().collect();
    md.extend(extra.into_iter().map(|(k, v)| (k.to_owned(), v)));
    RunMetadata {
        scan_id: None,
        extra: md,
        plan_name: Some(plan_name.to_owned()),
    }
}

/// Solve the problem with one acquisition run per step, recorded in an outer
/// tracking run — blop's `optimize`. `iterations = None` runs until the
/// optimizer's stopping criterion fires and requires
/// [`supports_stopping_criteria`](super::protocol::Optimizer::supports_stopping_criteria).
/// Each iteration ends with a `Checkpoint`, so the engine can pause between
/// steps.
pub fn optimize<U: AcquisitionUid>(
    problem: &OptimizationProblem<U>,
    iterations: Option<usize>,
    opts: StepOptions,
) -> Plan {
    let problem = problem.clone();
    let md = run_md(
        collect_optimization_metadata(&problem, "default_acquire"),
        "optimize",
        [
            ("iterations", iterations.map_or(Value::Null, Value::from)),
            ("n_points", Value::from(opts.n_points())),
            (
                "checkpoint_interval",
                opts.checkpoint_interval.map_or(Value::Null, Value::from),
            ),
            ("run_key", Value::from(OPTIMIZE_RUN_KEY)),
        ],
    );
    let n_points = opts.n_points();
    let cache = opts.cache();
    let inner = plan_items(async_stream::stream! {
        let stopping = problem.optimizer.lock().unwrap().supports_stopping_criteria();
        if iterations.is_none() && !stopping {
            yield PlanItem::from(Msg::Fail(
                "iterations=None requires an optimizer that implements SupportsStoppingCriteria".into(),
            ));
            return;
        }
        let mut i = 0usize;
        while iterations.is_none_or(|n| i < n) {
            let (mut step, result) = optimize_step(&problem, n_points, opts.acquire.clone());
            while let Some(item) = step.next().await {
                yield item;
            }
            let Ok(step) = result.await else { return };
            if stopping {
                let (stop, reason) = problem.optimizer.lock().unwrap().should_stop();
                if stop {
                    tracing::info!(
                        "Global stopping triggered at iteration {}: {}",
                        i + 1,
                        reason.unwrap_or_else(|| "No reason provided".into())
                    );
                    return;
                }
            }
            let mut read = read_step(&step.uid, &step.suggestions, &step.outcomes, n_points, &cache, "primary");
            while let Some(item) = read.next().await {
                yield item;
            }
            if let Err(e) = maybe_checkpoint(&problem.optimizer, opts.checkpoint_interval, i) {
                yield PlanItem::from(Msg::Fail(e.to_string()));
                return;
            }
            yield PlanItem::from(Msg::Checkpoint);
            i += 1;
        }
    });
    in_run_wrapper(
        set_run_key_wrapper(run_wrapper(inner, md), OPTIMIZE_RUN_KEY),
        OPTIMIZE_RUN_KEY,
    )
}

/// Solve the problem inside one run — blop's `optimize_in_run`. The
/// acquisition plan must not open or close runs; each step is recorded into
/// the `optimization` stream. Experimental in blop, and here.
pub fn optimize_in_run<U: AcquisitionUid>(
    problem: &OptimizationProblem<U>,
    iterations: usize,
    opts: StepOptions,
) -> Plan {
    let problem = problem.clone();
    let md = run_md(
        collect_optimization_metadata(&problem, "list_scan_in_run"),
        "optimize_in_run",
        [
            ("iterations", Value::from(iterations)),
            ("n_points", Value::from(opts.n_points())),
            (
                "checkpoint_interval",
                opts.checkpoint_interval.map_or(Value::Null, Value::from),
            ),
            ("run_key", Value::from(OPTIMIZE_IN_RUN_KEY)),
            (
                "optimization_stream",
                Value::from(OPTIMIZE_IN_RUN_TRACKING_STREAM),
            ),
        ],
    );
    let n_points = opts.n_points();
    let cache = opts.cache();
    let inner = plan_items(async_stream::stream! {
        for i in 0..iterations {
            let suggested = problem.optimizer.lock().unwrap().suggest(n_points);
            let suggestions = match suggested.and_then(|s| validate_suggestions(&s).map(|_| s)) {
                Ok(s) => s,
                Err(e) => {
                    yield PlanItem::from(Msg::Fail(e.to_string()));
                    return;
                }
            };
            let (mut step, result) =
                acquire_and_evaluate(problem.clone(), suggestions.clone(), opts.acquire.clone(), Default::InRun);
            while let Some(item) = step.next().await {
                yield item;
            }
            let Ok((uid, outcomes)) = result.await else { return };
            // Bound first: an `if let` scrutinee's guard would live across
            // the yield below.
            let ingested = problem.optimizer.lock().unwrap().ingest(outcomes.clone());
            if let Err(e) = ingested {
                yield PlanItem::from(Msg::Fail(e.to_string()));
                return;
            }
            let mut read = read_step(
                &uid,
                &suggestions,
                &outcomes,
                n_points,
                &cache,
                OPTIMIZE_IN_RUN_TRACKING_STREAM,
            );
            while let Some(item) = read.next().await {
                yield item;
            }
            let stopping = problem.optimizer.lock().unwrap().supports_stopping_criteria();
            if stopping {
                let (stop, reason) = problem.optimizer.lock().unwrap().should_stop();
                if stop {
                    tracing::info!(
                        "Global stopping triggered at iteration {}: {}",
                        i + 1,
                        reason.unwrap_or_else(|| "No reason provided".into())
                    );
                    return;
                }
            }
            if let Err(e) = maybe_checkpoint(&problem.optimizer, opts.checkpoint_interval, i) {
                yield PlanItem::from(Msg::Fail(e.to_string()));
                return;
            }
        }
    });
    in_run_wrapper(
        set_run_key_wrapper(run_wrapper(inner, md), OPTIMIZE_IN_RUN_KEY),
        OPTIMIZE_IN_RUN_KEY,
    )
}

/// Evaluate specific, already-identified suggestions and ingest the
/// outcomes — blop's `sample_suggestions` for points that carry an id. For
/// caller-chosen points without one, see [`sample_candidates`].
pub fn sample_suggestions<U: AcquisitionUid>(
    problem: &OptimizationProblem<U>,
    suggestions: Vec<Suggestion>,
    opts: StepOptions,
) -> PlanWithResult<StepResult<U>> {
    let problem = problem.clone();
    let (tx, rx) = oneshot::channel();
    let md = run_md(
        collect_optimization_metadata(&problem, "default_acquire"),
        "sample_suggestions",
        [
            (
                "suggestions",
                Value::Array(suggestions.iter().map(Suggestion::to_value).collect()),
            ),
            ("run_key", Value::from(SAMPLE_SUGGESTIONS_RUN_KEY)),
        ],
    );
    let cache = opts.cache();
    let inner = plan_items(async_stream::stream! {
        let Some(acquisition) = acquisition_for(&problem, Default::PerStep) else {
            yield PlanItem::from(Msg::Fail(no_acquisition::<U>()));
            return;
        };
        let (mut acquire, uid) = acquisition.build(
            suggestions.clone(),
            problem.actuators.clone(),
            problem.sensors.clone(),
            opts.acquire.clone(),
        );
        while let Some(item) = acquire.next().await {
            yield item;
        }
        let Ok(uid) = uid.await else {
            yield PlanItem::from(Msg::Fail(format!(
                "acquisition plan '{}' ended without producing an acquisition uid",
                acquisition.name()
            )));
            return;
        };
        let outcomes = match problem.evaluation_function.call(uid.clone(), suggestions.clone()).await {
            Ok(o) => o,
            Err(e) => {
                yield PlanItem::from(Msg::Fail(e.to_string()));
                return;
            }
        };
        let ingested = problem.optimizer.lock().unwrap().ingest(outcomes.clone());
        if let Err(e) = ingested {
            yield PlanItem::from(Msg::Fail(e.to_string()));
            return;
        }
        let mut read = read_step(&uid, &suggestions, &outcomes, suggestions.len(), &cache, "primary");
        while let Some(item) = read.next().await {
            yield item;
        }
        let _ = tx.send(StepResult { uid, suggestions, outcomes });
    });
    (
        in_run_wrapper(
            set_run_key_wrapper(run_wrapper(inner, md), SAMPLE_SUGGESTIONS_RUN_KEY),
            SAMPLE_SUGGESTIONS_RUN_KEY,
        ),
        rx,
    )
}

/// Register caller-chosen points with the optimizer, then evaluate them as
/// [`sample_suggestions`] does — blop's `sample_suggestions` for mappings
/// without an id. Requires
/// [`supports_register_suggestions`](super::protocol::Optimizer::supports_register_suggestions).
pub fn sample_candidates<U: AcquisitionUid>(
    problem: &OptimizationProblem<U>,
    candidates: Vec<Candidate>,
    opts: StepOptions,
) -> PlanWithResult<StepResult<U>> {
    let problem = problem.clone();
    let (tx, rx) = oneshot::channel();
    let plan = plan_items(async_stream::stream! {
        let registered = {
            let mut optimizer = problem.optimizer.lock().unwrap();
            if !optimizer.supports_register_suggestions() {
                Err(crate::core::error::BsrsError::Plan(format!(
                    "All suggestions must contain an '{ID_KEY}' key to later match with the outcomes or your \
                     optimizer must implement register_suggestions. Please review your optimizer implementation."
                )))
            } else {
                optimizer.register_suggestions(candidates)
            }
        };
        let suggestions = match registered {
            Ok(s) => s,
            Err(e) => {
                yield PlanItem::from(Msg::Fail(e.to_string()));
                return;
            }
        };
        let (mut sample, result) = sample_suggestions(&problem, suggestions, opts);
        while let Some(item) = sample.next().await {
            yield item;
        }
        if let Ok(r) = result.await {
            let _ = tx.send(r);
        }
    });
    (plan, rx)
}

/// Acquire a baseline point and ingest it — blop's `acquire_baseline`. With
/// no `parameterization` the actuators' current positions are read, which
/// needs every actuator to have a readback. The trial id is `"baseline"`
/// unless the parameterization carries an `_id` entry; the ingested outcome
/// also carries the parameters, for relative outcome constraints.
pub fn acquire_baseline<U: AcquisitionUid>(
    problem: &OptimizationProblem<U>,
    parameterization: Option<Params>,
    args: AcquireArgs,
) -> Plan {
    let problem = problem.clone();
    plan_items(async_stream::stream! {
        let params = match parameterization {
            Some(p) => p,
            None => {
                let readers: Option<Vec<Arc<dyn crate::core::msg::ReadableObj>>> =
                    problem.actuators.iter().map(|a| a.reader.clone()).collect();
                let Some(readers) = readers else {
                    yield PlanItem::from(Msg::Fail(
                        "All actuators must also implement the Readable protocol to acquire a \
                         baseline from current positions."
                            .into(),
                    ));
                    return;
                };
                let (mut read, pos) = seq_read(readers);
                while let Some(item) = read.next().await {
                    yield item;
                }
                let Ok(p) = pos.await else { return };
                p
            }
        };
        let mut params = params;
        let id = match params.remove(ID_KEY) {
            Some(Value::String(s)) => TrialId::Str(s),
            Some(Value::Number(n)) if n.is_i64() => TrialId::Int(n.as_i64().unwrap()),
            Some(other) => TrialId::Str(other.to_string()),
            None => TrialId::Str("baseline".into()),
        };
        let suggestion = Suggestion { id, params };
        let Some(acquisition) = acquisition_for(&problem, Default::PerStep) else {
            yield PlanItem::from(Msg::Fail(no_acquisition::<U>()));
            return;
        };
        let (mut acquire, uid) = acquisition.build(
            vec![suggestion.clone()],
            problem.actuators.clone(),
            problem.sensors.clone(),
            args,
        );
        while let Some(item) = acquire.next().await {
            yield item;
        }
        let Ok(uid) = uid.await else {
            yield PlanItem::from(Msg::Fail(format!(
                "acquisition plan '{}' ended without producing an acquisition uid",
                acquisition.name()
            )));
            return;
        };
        let outcome = problem
            .evaluation_function
            .call(uid, vec![suggestion.clone()])
            .await
            .and_then(|mut outcomes| {
                if outcomes.is_empty() {
                    Err(crate::core::error::BsrsError::Plan(
                        "the evaluation function returned no outcome for the baseline".into(),
                    ))
                } else {
                    Ok(outcomes.remove(0))
                }
            });
        let ingested = outcome.and_then(|outcome| {
            let mut values = outcome.values;
            values.extend(suggestion.params.clone());
            problem
                .optimizer
                .lock()
                .unwrap()
                .ingest(vec![Outcome { id: suggestion.id.clone(), values }])
        });
        if let Err(e) = ingested {
            yield PlanItem::from(Msg::Fail(e.to_string()));
        }
    })
}
