//! Core types of the optimization loop — bsrs's port of `blop.protocols`.
//!
//! blop passes suggestions and outcomes as loose mappings keyed by `"_id"`
//! and validates the id at four sites; here the id is a typed field
//! ([`Suggestion::id`], [`Outcome::id`]) and only the one check no type can
//! express remains — that the outcomes name exactly the suggested ids
//! ([`validate_outcomes`]).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::core::error::{BsrsError, Result};
use crate::core::msg::{MovableObj, ReadableObj};
use crate::core::plan::Plan;
use crate::plans::PerStep;

/// Parameter or outcome values keyed by name. Ordered so the emitted event
/// data and metadata are deterministic.
pub type Params = BTreeMap<String, Value>;

/// Identity of one trial, blop's hashable `"_id"`. Ax-style optimizers use
/// trial indices, others use strings; `"baseline"` is the id
/// [`acquire_baseline`](super::plans::acquire_baseline) assigns.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TrialId {
    /// Integer trial index.
    Int(i64),
    /// Free-form identifier.
    Str(String),
}

impl fmt::Display for TrialId {
    /// The `str(_id)` form blop groups by and records in `suggestion_ids`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrialId::Int(i) => write!(f, "{i}"),
            TrialId::Str(s) => f.write_str(s),
        }
    }
}

impl From<i64> for TrialId {
    fn from(i: i64) -> Self {
        TrialId::Int(i)
    }
}

impl From<&str> for TrialId {
    fn from(s: &str) -> Self {
        TrialId::Str(s.to_owned())
    }
}

impl From<String> for TrialId {
    fn from(s: String) -> Self {
        TrialId::Str(s)
    }
}

impl From<&TrialId> for Value {
    fn from(id: &TrialId) -> Self {
        match id {
            TrialId::Int(i) => Value::from(*i),
            TrialId::Str(s) => Value::from(s.as_str()),
        }
    }
}

/// A parameterization without an id — a point the caller wants evaluated
/// that the optimizer has not issued. [`Optimizer::register_suggestions`]
/// turns it into a [`Suggestion`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Candidate {
    /// Parameter values keyed by actuator name.
    pub params: Params,
}

/// One point the optimizer wants evaluated.
#[derive(Clone, Debug, PartialEq)]
pub struct Suggestion {
    /// Trial identity, matched against [`Outcome::id`].
    pub id: TrialId,
    /// Parameter values keyed by actuator name.
    pub params: Params,
}

impl Suggestion {
    /// Build from an id and `(name, value)` pairs.
    pub fn new(id: impl Into<TrialId>, params: impl IntoIterator<Item = (String, Value)>) -> Self {
        Self {
            id: id.into(),
            params: params.into_iter().collect(),
        }
    }

    /// The blop mapping form, `{"_id": id, name: value, …}`, as recorded in
    /// `blop_suggestions` run metadata.
    pub fn to_value(&self) -> Value {
        let mut map = serde_json::Map::new();
        map.insert(ID_KEY.into(), Value::from(&self.id));
        for (k, v) in &self.params {
            map.insert(k.clone(), v.clone());
        }
        Value::Object(map)
    }
}

/// The evaluated result of one [`Suggestion`].
#[derive(Clone, Debug, PartialEq)]
pub struct Outcome {
    /// Trial identity, matched against [`Suggestion::id`].
    pub id: TrialId,
    /// Outcome values keyed by objective name.
    pub values: Params,
}

impl Outcome {
    /// Build from an id and `(name, value)` pairs.
    pub fn new(id: impl Into<TrialId>, values: impl IntoIterator<Item = (String, Value)>) -> Self {
        Self {
            id: id.into(),
            values: values.into_iter().collect(),
        }
    }
}

/// blop's `ID_KEY`, the mapping key the id is serialized under.
pub const ID_KEY: &str = "_id";

/// One entry of [`Optimizer::best_points`]: the trial, its parameters, and
/// its outcomes.
#[derive(Clone, Debug, PartialEq)]
pub struct BestPoint {
    /// Trial identity.
    pub id: TrialId,
    /// Parameter values keyed by actuator name.
    pub params: Params,
    /// Outcome values keyed by objective name.
    pub outcomes: Params,
}

/// The ask/tell optimizer the plans drive — blop's `Optimizer` protocol with
/// its optional protocols (`CanRegisterSuggestions`, `TrialFaultAware`,
/// `SupportsStoppingCriteria`, `Checkpointable`) folded in as defaulted
/// methods. Where blop branches on `isinstance`, a plan here consults the
/// matching `supports_*` query; an optimizer that overrides one of those
/// methods must also override its query, or the plan never calls it.
pub trait Optimizer: Send + Sync {
    /// Short name recorded in run metadata as `optimizer`.
    fn name(&self) -> String {
        std::any::type_name::<Self>()
            .rsplit("::")
            .next()
            .unwrap_or("Optimizer")
            .to_owned()
    }

    /// Propose `n` points to evaluate.
    fn suggest(&mut self, n: usize) -> Result<Vec<Suggestion>>;

    /// Feed evaluated outcomes back.
    fn ingest(&mut self, outcomes: Vec<Outcome>) -> Result<()>;

    /// The best point(s) so far — one for a single objective, the Pareto set
    /// for several.
    fn best_points(&self) -> Result<Vec<BestPoint>>;

    /// `true` if [`register_suggestions`](Self::register_suggestions) is
    /// implemented (blop's `CanRegisterSuggestions`).
    fn supports_register_suggestions(&self) -> bool {
        false
    }

    /// Attach caller-chosen points as trials so their outcomes can be
    /// ingested, returning them with ids assigned.
    fn register_suggestions(&mut self, candidates: Vec<Candidate>) -> Result<Vec<Suggestion>> {
        let _ = candidates;
        Err(BsrsError::Plan(format!(
            "optimizer {} does not implement register_suggestions",
            self.name()
        )))
    }

    /// Called when acquiring or evaluating `suggestions` failed, before the
    /// error propagates (blop's `TrialFaultAware`). Default: ignore.
    fn register_failures(&mut self, suggestions: &[Suggestion]) {
        let _ = suggestions;
    }

    /// `true` if [`should_stop`](Self::should_stop) carries a real stopping
    /// criterion (blop's `SupportsStoppingCriteria`). Required by
    /// [`optimize`](super::plans::optimize) with `iterations = None`.
    fn supports_stopping_criteria(&self) -> bool {
        false
    }

    /// Whether the loop should end now, with an optional reason.
    fn should_stop(&self) -> (bool, Option<String>) {
        (false, None)
    }

    /// `true` if [`checkpoint`](Self::checkpoint) persists state (blop's
    /// `Checkpointable`). Required when a plan is given a
    /// `checkpoint_interval`.
    fn supports_checkpoint(&self) -> bool {
        false
    }

    /// Persist the optimizer state.
    fn checkpoint(&mut self) -> Result<()> {
        Ok(())
    }
}

/// An optimizer shared between the plan that drives it and the caller that
/// inspects it afterwards. Plans lock it only around a call, never across a
/// yielded message.
pub type SharedOptimizer = Arc<Mutex<dyn Optimizer>>;

/// Wrap an optimizer for use in an [`OptimizationProblem`].
pub fn shared<O: Optimizer + 'static>(optimizer: O) -> SharedOptimizer {
    Arc::new(Mutex::new(optimizer))
}

/// An actuator: the movable a suggestion's value is written to, plus its
/// readback for routing and baseline reads. blop's actuators are
/// `NamedMovable` and optionally `Readable`; a move-only actuator has no
/// `reader`, so plans that need the current position skip the read (routing)
/// or fail (baseline from current position).
#[derive(Clone)]
pub struct Actuator {
    /// The device that is moved.
    pub movable: Arc<dyn MovableObj>,
    /// The device read back into the acquisition stream, if any.
    pub reader: Option<Arc<dyn ReadableObj>>,
}

impl Actuator {
    /// An actuator that is also read back — the common motor case.
    pub fn new<T: MovableObj + ReadableObj + 'static>(device: Arc<T>) -> Self {
        Self {
            movable: device.clone(),
            reader: Some(device),
        }
    }

    /// An actuator with no readback.
    pub fn move_only(movable: Arc<dyn MovableObj>) -> Self {
        Self {
            movable,
            reader: None,
        }
    }

    /// The actuator's name, the key its value carries in a [`Suggestion`].
    pub fn name(&self) -> &str {
        self.movable.name()
    }
}

impl fmt::Debug for Actuator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Actuator")
            .field("name", &self.name())
            .field("readable", &self.reader.is_some())
            .finish()
    }
}

/// A sensor: anything readable into the acquisition stream.
pub type Sensor = Arc<dyn ReadableObj>;

/// Marker for the value an acquisition plan hands to the evaluation function
/// (blop's `TUid`). [`event_value`](Self::event_value) is what
/// [`read_step`](super::stubs::read_step) records under `acquisition_uid`:
/// blop keeps array-like identifiers as-is and stores `repr(uid)` for
/// anything else, so implement it as the JSON form for structured ids and as
/// a display string otherwise.
pub trait AcquisitionUid: Clone + Send + Sync + 'static {
    /// The event-readable form of this identifier.
    fn event_value(&self) -> Value;

    /// The acquisition plan the run-per-step plans (`optimize`,
    /// `sample_suggestions`, `acquire_baseline`) use when the problem names
    /// none. blop's default is `default_acquire`, whose uid is the run uid,
    /// so only `String` has one; a problem with another uid type must name
    /// its acquisition plan or those plans fail at once.
    fn default_acquisition_plan() -> Option<AcquisitionPlan<Self>> {
        None
    }

    /// The acquisition plan `optimize_in_run` uses when the problem names
    /// none. blop's default is `list_scan_in_run`, whose uid is the ordered
    /// suggestion ids, so only `Vec<TrialId>` has one.
    fn default_in_run_acquisition_plan() -> Option<AcquisitionPlan<Self>> {
        None
    }
}

impl AcquisitionUid for Vec<String> {
    fn event_value(&self) -> Value {
        Value::from(self.clone())
    }
}

/// What an [`AcquisitionPlan`] receives beyond the suggestions: the run
/// metadata to attach and the step hook to use — blop's `md=` and
/// `per_step=` keyword arguments.
#[derive(Clone, Default)]
pub struct AcquireArgs {
    /// Extra run metadata for the acquisition's start document.
    pub md: BTreeMap<String, Value>,
    /// Custom per-step hook for the underlying list scan.
    pub per_step: Option<PerStep>,
}

/// A plan and the value it will produce when it completes. The plan sends
/// exactly one value; a plan that ends without sending (it failed, or was
/// interrupted) leaves the receiver closed.
pub type PlanWithResult<T> = (Plan, oneshot::Receiver<T>);

/// A plan that acquires data for `suggestions` on `actuators` and `sensors`
/// and yields an identifier the evaluation function can correlate the data
/// with. blop's `AcquisitionPlan[TUid]`.
pub struct AcquisitionPlan<U> {
    name: String,
    #[allow(clippy::type_complexity)]
    build: Arc<
        dyn Fn(Vec<Suggestion>, Vec<Actuator>, Vec<Sensor>, AcquireArgs) -> PlanWithResult<U>
            + Send
            + Sync,
    >,
}

impl<U> Clone for AcquisitionPlan<U> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            build: self.build.clone(),
        }
    }
}

impl<U> AcquisitionPlan<U> {
    /// Wrap a plan builder under `name` (recorded in run metadata as
    /// `acquisition_plan`).
    pub fn new<F>(name: impl Into<String>, build: F) -> Self
    where
        F: Fn(Vec<Suggestion>, Vec<Actuator>, Vec<Sensor>, AcquireArgs) -> PlanWithResult<U>
            + Send
            + Sync
            + 'static,
    {
        Self {
            name: name.into(),
            build: Arc::new(build),
        }
    }

    /// The plan's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Build the plan for one acquisition.
    pub fn build(
        &self,
        suggestions: Vec<Suggestion>,
        actuators: Vec<Actuator>,
        sensors: Vec<Sensor>,
        args: AcquireArgs,
    ) -> PlanWithResult<U> {
        (self.build)(suggestions, actuators, sensors, args)
    }
}

impl<U> fmt::Debug for AcquisitionPlan<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcquisitionPlan")
            .field("name", &self.name)
            .finish()
    }
}

/// Turns an acquisition identifier and the suggestions it covers into
/// outcomes. Async so it can query a document store; blop's
/// `EvaluationFunction[TUid]`.
pub struct EvaluationFunction<U> {
    name: String,
    #[allow(clippy::type_complexity)]
    call: Arc<dyn Fn(U, Vec<Suggestion>) -> BoxFuture<'static, Result<Vec<Outcome>>> + Send + Sync>,
}

impl<U> Clone for EvaluationFunction<U> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            call: self.call.clone(),
        }
    }
}

impl<U> EvaluationFunction<U> {
    /// Wrap an async evaluator under `name` (recorded in run metadata as
    /// `evaluation_function`).
    pub fn new<F>(name: impl Into<String>, call: F) -> Self
    where
        F: Fn(U, Vec<Suggestion>) -> BoxFuture<'static, Result<Vec<Outcome>>>
            + Send
            + Sync
            + 'static,
    {
        Self {
            name: name.into(),
            call: Arc::new(call),
        }
    }

    /// Wrap a synchronous evaluator.
    pub fn sync<F>(name: impl Into<String>, call: F) -> Self
    where
        F: Fn(U, Vec<Suggestion>) -> Result<Vec<Outcome>> + Send + Sync + 'static,
    {
        Self::new(name, move |uid, suggestions| {
            let out = call(uid, suggestions);
            Box::pin(async move { out })
        })
    }

    /// The function's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Evaluate `suggestions` from the data identified by `uid`.
    pub fn call(
        &self,
        uid: U,
        suggestions: Vec<Suggestion>,
    ) -> BoxFuture<'static, Result<Vec<Outcome>>> {
        (self.call)(uid, suggestions)
    }
}

impl<U> fmt::Debug for EvaluationFunction<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvaluationFunction")
            .field("name", &self.name)
            .finish()
    }
}

/// Everything an optimization plan needs — blop's `OptimizationProblem`.
/// Cheap to clone: every part is shared.
#[derive(Clone)]
pub struct OptimizationProblem<U> {
    /// The optimizer that proposes points and ingests outcomes.
    pub optimizer: SharedOptimizer,
    /// The devices suggestions are written to.
    pub actuators: Vec<Actuator>,
    /// The devices read at each point.
    pub sensors: Vec<Sensor>,
    /// Turns acquired data into outcomes.
    pub evaluation_function: EvaluationFunction<U>,
    /// How data is acquired; `None` selects the plan's default
    /// (`default_acquire` for run-per-step plans, `list_scan_in_run` for
    /// `optimize_in_run`).
    pub acquisition_plan: Option<AcquisitionPlan<U>>,
}

impl<U> OptimizationProblem<U> {
    /// Build a problem that acquires with the plan's default.
    pub fn new(
        optimizer: SharedOptimizer,
        actuators: Vec<Actuator>,
        sensors: Vec<Sensor>,
        evaluation_function: EvaluationFunction<U>,
    ) -> Self {
        Self {
            optimizer,
            actuators,
            sensors,
            evaluation_function,
            acquisition_plan: None,
        }
    }

    /// Use `plan` to acquire instead of the default.
    pub fn with_acquisition_plan(mut self, plan: AcquisitionPlan<U>) -> Self {
        self.acquisition_plan = Some(plan);
        self
    }
}

impl<U> fmt::Debug for OptimizationProblem<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OptimizationProblem")
            .field("optimizer", &self.optimizer.lock().unwrap().name())
            .field("actuators", &self.actuators)
            .field(
                "sensors",
                &self.sensors.iter().map(|s| s.name()).collect::<Vec<_>>(),
            )
            .field("evaluation_function", &self.evaluation_function)
            .field("acquisition_plan", &self.acquisition_plan)
            .finish()
    }
}

/// Reject duplicate ids — blop's `_validate_suggestions` (the hashability and
/// presence checks are carried by the [`Suggestion`] type).
pub fn validate_suggestions(suggestions: &[Suggestion]) -> Result<()> {
    validate_unique_ids(suggestions.iter().map(|s| &s.id), "suggestions")
}

/// Reject outcomes whose ids are duplicated or do not name exactly the
/// suggested trials — blop's `_validate_outcomes`.
pub fn validate_outcomes(outcomes: &[Outcome], suggestions: &[Suggestion]) -> Result<()> {
    validate_unique_ids(outcomes.iter().map(|o| &o.id), "outcomes")?;
    let suggested: std::collections::BTreeSet<&TrialId> =
        suggestions.iter().map(|s| &s.id).collect();
    let evaluated: std::collections::BTreeSet<&TrialId> = outcomes.iter().map(|o| &o.id).collect();
    if suggested != evaluated {
        return Err(BsrsError::Plan(format!(
            "The suggestions and outcomes must contain the same IDs. Got suggestions: {:?} and outcomes: {:?}",
            suggested.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
            evaluated.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        )));
    }
    Ok(())
}

fn validate_unique_ids<'a>(ids: impl Iterator<Item = &'a TrialId>, label: &str) -> Result<()> {
    let ids: Vec<&TrialId> = ids.collect();
    let unique: std::collections::BTreeSet<&TrialId> = ids.iter().copied().collect();
    if unique.len() != ids.len() {
        return Err(BsrsError::Plan(format!(
            "All {label} must contain unique '{ID_KEY}' values. Got {:?}.",
            ids.iter().map(|id| id.to_string()).collect::<Vec<_>>()
        )));
    }
    Ok(())
}

/// The run metadata every optimization plan attaches — blop's
/// `collect_optimization_metadata`.
pub fn collect_optimization_metadata<U>(
    problem: &OptimizationProblem<U>,
    default_acquire_name: &str,
) -> BTreeMap<String, Value> {
    let optimizer = problem.optimizer.lock().unwrap().name();
    let acquisition = problem
        .acquisition_plan
        .as_ref()
        .map(|p| p.name().to_owned())
        .unwrap_or_else(|| default_acquire_name.to_owned());
    BTreeMap::from([
        (
            "evaluation_function".into(),
            Value::from(problem.evaluation_function.name()),
        ),
        ("acquisition_plan".into(), Value::from(acquisition)),
        ("optimizer".into(), Value::from(optimizer)),
        (
            "sensors".into(),
            Value::from(
                problem
                    .sensors
                    .iter()
                    .map(|s| s.name().to_owned())
                    .collect::<Vec<_>>(),
            ),
        ),
        (
            "actuators".into(),
            Value::from(
                problem
                    .actuators
                    .iter()
                    .map(|a| a.name().to_owned())
                    .collect::<Vec<_>>(),
            ),
        ),
    ])
}
