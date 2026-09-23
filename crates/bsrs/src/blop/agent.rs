//! The agent — bsrs's port of `blop.ax.agent.Agent`, minus Ax.
//!
//! An [`Agent`] binds DOFs, objectives, sensors and an evaluation function
//! to an optimizer and exposes the optimization plans on that bundle. blop's
//! agent constructs an `AxOptimizer` itself; this one renders the
//! backend-neutral [`ExperimentSpec`] and hands it to whatever builds the
//! optimizer — an in-process implementation or a bridge to one — so the
//! agent stays independent of the backend.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::error::{BsrsError, Result};
use crate::core::plan::Plan;

use super::dof::{Dof, DofBinding, DofConstraint, ParameterConfig};
use super::objective::{Objectives, OutcomeConstraint};
use super::plans::{
    acquire_baseline, optimize, optimize_in_run, sample_candidates, sample_suggestions,
    StepOptions, StepResult,
};
use super::protocol::{
    AcquireArgs, AcquisitionPlan, AcquisitionUid, Actuator, BestPoint, Candidate,
    EvaluationFunction, OptimizationProblem, Outcome, Params, PlanWithResult, SearchSpace, Sensor,
    SharedOptimizer, Suggestion,
};
use super::stubs::navigate_to_best;
use super::tracking::{readable_cache, ReadableCache};

/// Everything an optimizer needs to be configured, rendered from the DOFs,
/// objectives and constraints: the arguments of Ax's
/// `Client.configure_experiment` and `configure_optimization`, in their
/// field names, so a bridge can pass it on as JSON.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExperimentSpec {
    /// The search space, one entry per DOF.
    pub parameters: Vec<ParameterConfig>,
    /// The objective expression (`"intensity, -width"`).
    pub objective: String,
    /// Inequalities over parameter names.
    #[serde(default)]
    pub parameter_constraints: Vec<String>,
    /// Inequalities over outcome names.
    #[serde(default)]
    pub outcome_constraints: Vec<String>,
}

impl ExperimentSpec {
    /// The parameter names, in DOF order.
    pub fn parameter_names(&self) -> Vec<&str> {
        self.parameters.iter().map(ParameterConfig::name).collect()
    }
}

/// Builds an [`Agent`]; see [`Agent::builder`].
pub struct AgentBuilder<U: AcquisitionUid> {
    dofs: Vec<Dof>,
    objectives: Objectives,
    evaluation_function: EvaluationFunction<U>,
    sensors: Vec<Sensor>,
    acquisition_plan: Option<AcquisitionPlan<U>>,
    dof_constraints: Vec<DofConstraint>,
    outcome_constraints: Vec<OutcomeConstraint>,
}

impl<U: AcquisitionUid> AgentBuilder<U> {
    /// The sensors read at each point — the minimal set the evaluation
    /// function needs.
    pub fn sensors(mut self, sensors: impl IntoIterator<Item = Sensor>) -> Self {
        self.sensors = sensors.into_iter().collect();
        self
    }

    /// How data is acquired; the plans' default when not set.
    pub fn acquisition_plan(mut self, plan: AcquisitionPlan<U>) -> Self {
        self.acquisition_plan = Some(plan);
        self
    }

    /// Inequalities that refine the search space.
    pub fn dof_constraints(mut self, constraints: impl IntoIterator<Item = DofConstraint>) -> Self {
        self.dof_constraints = constraints.into_iter().collect();
        self
    }

    /// Soft inequalities on outcomes.
    pub fn outcome_constraints(
        mut self,
        constraints: impl IntoIterator<Item = OutcomeConstraint>,
    ) -> Self {
        self.outcome_constraints = constraints.into_iter().collect();
        self
    }

    /// Render the optimizer configuration. Fails on a DOF bound by a
    /// queueserver variable name (blop: "DOFs with actuators must be
    /// `Actuator` instances, not strings") and on two DOFs sharing a
    /// parameter name (what Ax rejects with "names must be unique").
    pub fn spec(&self) -> Result<ExperimentSpec> {
        let remote: Vec<&str> = self
            .dofs
            .iter()
            .filter_map(|d| match d.binding() {
                DofBinding::Remote { actuator, .. } => Some(actuator.as_str()),
                _ => None,
            })
            .collect();
        if !remote.is_empty() {
            return Err(BsrsError::Plan(format!(
                "DOFs with actuators must be `Actuator` instances, not names a worker resolves. \
                 Got names for: {remote:?}"
            )));
        }
        let mut seen = std::collections::BTreeSet::new();
        let duplicates: Vec<&str> = self
            .dofs
            .iter()
            .map(Dof::parameter_name)
            .filter(|n| !seen.insert(*n))
            .collect();
        if !duplicates.is_empty() {
            return Err(BsrsError::Plan(format!(
                "DOF parameter names must be unique. Got duplicates: {duplicates:?}"
            )));
        }
        Ok(ExperimentSpec {
            parameters: self.dofs.iter().map(Dof::to_parameter_config).collect(),
            objective: self.objectives.expression(),
            parameter_constraints: self
                .dof_constraints
                .iter()
                .map(DofConstraint::expression)
                .collect(),
            outcome_constraints: self
                .outcome_constraints
                .iter()
                .map(OutcomeConstraint::expression)
                .collect(),
        })
    }

    /// Build the agent around the optimizer `make_optimizer` configures
    /// from the rendered [`ExperimentSpec`].
    pub fn build<F>(self, make_optimizer: F) -> Result<Agent<U>>
    where
        F: FnOnce(&ExperimentSpec) -> Result<SharedOptimizer>,
    {
        let spec = self.spec()?;
        let optimizer = make_optimizer(&spec)?;
        Ok(Agent {
            actuators: self
                .dofs
                .iter()
                .filter_map(Dof::actuator)
                .cloned()
                .collect(),
            dofs: self.dofs,
            sensors: self.sensors,
            evaluation_function: self.evaluation_function,
            acquisition_plan: self.acquisition_plan,
            optimizer,
            spec: Some(spec),
            readable_cache: readable_cache(),
        })
    }
}

/// DOFs, objectives, sensors and an evaluation function bound to one
/// optimizer, with the optimization plans on the bundle — blop's `Agent`.
///
/// The readables that record steps are kept across the agent's plans, so
/// every run it produces shares one event shape.
pub struct Agent<U: AcquisitionUid> {
    dofs: Vec<Dof>,
    actuators: Vec<Actuator>,
    sensors: Vec<Sensor>,
    evaluation_function: EvaluationFunction<U>,
    acquisition_plan: Option<AcquisitionPlan<U>>,
    optimizer: SharedOptimizer,
    /// `None` for an agent restored around an existing optimizer, whose
    /// configuration lives in the optimizer alone.
    spec: Option<ExperimentSpec>,
    readable_cache: ReadableCache,
}

impl<U: AcquisitionUid> fmt::Debug for Agent<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Agent")
            .field("spec", &self.spec)
            .field("optimizer", &self.optimizer.lock().unwrap().name())
            .field("actuators", &self.actuators)
            .field(
                "sensors",
                &self.sensors.iter().map(|s| s.name()).collect::<Vec<_>>(),
            )
            .field("evaluation_function", &self.evaluation_function.name())
            .field(
                "acquisition_plan",
                &self.acquisition_plan.as_ref().map(AcquisitionPlan::name),
            )
            .finish()
    }
}

impl<U: AcquisitionUid> Agent<U> {
    /// Start building an agent over `dofs` driving `objectives`, with
    /// `evaluation_function` turning acquired data into outcomes.
    pub fn builder<D: Into<Dof>>(
        dofs: impl IntoIterator<Item = D>,
        objectives: impl Into<Objectives>,
        evaluation_function: EvaluationFunction<U>,
    ) -> AgentBuilder<U> {
        AgentBuilder {
            dofs: dofs.into_iter().map(Into::into).collect(),
            objectives: objectives.into(),
            evaluation_function,
            sensors: Vec::new(),
            acquisition_plan: None,
            dof_constraints: Vec::new(),
            outcome_constraints: Vec::new(),
        }
    }

    /// Bind an already configured optimizer — blop's `Agent.from_checkpoint`
    /// shape, with the restoring left to the optimizer (for the bridge,
    /// `BridgeOptimizer::from_checkpoint`). Only the optimizer state was
    /// saved, so the agent has no DOFs and no [`spec`](Self::spec); the
    /// actuators must cover the names the optimizer suggests.
    pub fn from_optimizer(
        optimizer: SharedOptimizer,
        actuators: impl IntoIterator<Item = Actuator>,
        sensors: impl IntoIterator<Item = Sensor>,
        evaluation_function: EvaluationFunction<U>,
        acquisition_plan: Option<AcquisitionPlan<U>>,
    ) -> Self {
        Self {
            dofs: Vec::new(),
            actuators: actuators.into_iter().collect(),
            sensors: sensors.into_iter().collect(),
            evaluation_function,
            acquisition_plan,
            optimizer,
            spec: None,
            readable_cache: readable_cache(),
        }
    }

    /// The configuration the optimizer was built from; `None` for an agent
    /// from [`from_optimizer`](Self::from_optimizer).
    pub fn spec(&self) -> Option<&ExperimentSpec> {
        self.spec.as_ref()
    }

    /// Where the optimizer checkpoints, if it checkpoints to a file.
    pub fn checkpoint_path(&self) -> Result<Option<String>> {
        self.optimizer.lock().unwrap().checkpoint_path()
    }

    /// Set where the optimizer checkpoints.
    pub fn set_checkpoint_path(&self, path: impl Into<String>) -> Result<()> {
        self.optimizer
            .lock()
            .unwrap()
            .set_checkpoint_path(Some(path.into()))
    }

    /// The DOFs held fixed, by parameter name, or `None` when all are free.
    pub fn fixed_dofs(&self) -> Result<Option<Params>> {
        self.optimizer.lock().unwrap().fixed_parameters()
    }

    /// Fix DOFs to values for future suggestions; an empty iterator frees
    /// them all, as blop's `fixed_dofs = None` does.
    pub fn set_fixed_dofs<'a>(
        &self,
        fixed: impl IntoIterator<Item = (&'a Dof, Value)>,
    ) -> Result<()> {
        let fixed: Params = fixed
            .into_iter()
            .map(|(dof, value)| (dof.parameter_name().to_owned(), value))
            .collect();
        self.optimizer
            .lock()
            .unwrap()
            .set_fixed_parameters((!fixed.is_empty()).then_some(fixed))
    }

    /// Update the bounds or values of existing DOFs for future suggestions.
    pub fn reconfigure_search_space<'a>(
        &self,
        updates: impl IntoIterator<Item = (&'a Dof, SearchSpace)>,
    ) -> Result<()> {
        let updates = updates
            .into_iter()
            .map(|(dof, space)| (dof.parameter_name().to_owned(), space))
            .collect();
        self.optimizer
            .lock()
            .unwrap()
            .reconfigure_search_space(updates)
    }

    /// The degrees of freedom.
    pub fn dofs(&self) -> &[Dof] {
        &self.dofs
    }

    /// The actuators the RunEngine moves: the DOFs bound to a device, in
    /// DOF order.
    pub fn actuators(&self) -> &[Actuator] {
        &self.actuators
    }

    /// The sensors read at each point.
    pub fn sensors(&self) -> &[Sensor] {
        &self.sensors
    }

    /// The function turning acquired data into outcomes.
    pub fn evaluation_function(&self) -> &EvaluationFunction<U> {
        &self.evaluation_function
    }

    /// The acquisition plan, or `None` for the plans' default.
    pub fn acquisition_plan(&self) -> Option<&AcquisitionPlan<U>> {
        self.acquisition_plan.as_ref()
    }

    /// The optimizer.
    pub fn optimizer(&self) -> &SharedOptimizer {
        &self.optimizer
    }

    /// The readables recording steps, shared by every plan of the agent.
    pub fn readable_cache(&self) -> &ReadableCache {
        &self.readable_cache
    }

    /// The problem the plans run on.
    pub fn to_optimization_problem(&self) -> OptimizationProblem<U> {
        let problem = OptimizationProblem::new(
            self.optimizer.clone(),
            self.actuators.clone(),
            self.sensors.clone(),
            self.evaluation_function.clone(),
        );
        match &self.acquisition_plan {
            Some(plan) => problem.with_acquisition_plan(plan.clone()),
            None => problem,
        }
    }

    fn options(&self, n_points: usize) -> StepOptions {
        StepOptions {
            n_points,
            checkpoint_interval: None,
            readable_cache: Some(self.readable_cache.clone()),
            acquire: AcquireArgs::default(),
        }
    }

    /// Ask the optimizer for the next `n` points.
    pub fn suggest(&self, n: usize) -> Result<Vec<Suggestion>> {
        self.optimizer.lock().unwrap().suggest(n)
    }

    /// Tell the optimizer outcomes — of suggested points (by id) or of
    /// external data.
    pub fn ingest(&self, outcomes: Vec<Outcome>) -> Result<()> {
        self.optimizer.lock().unwrap().ingest(outcomes)
    }

    /// The optimizer's best point, or Pareto set for several objectives.
    pub fn best_points(&self) -> Result<Vec<BestPoint>> {
        self.optimizer.lock().unwrap().best_points()
    }

    /// Save the optimizer's state, if it supports that.
    pub fn checkpoint(&self) -> Result<()> {
        let mut optimizer = self.optimizer.lock().unwrap();
        if !optimizer.supports_checkpoint() {
            return Err(BsrsError::Plan(format!(
                "The optimizer '{}' is not checkpointable.",
                optimizer.name()
            )));
        }
        optimizer.checkpoint()
    }

    /// Run the optimization loop: `iterations` steps of `n_points`, or
    /// until the optimizer's stopping criterion when `iterations` is
    /// `None` — [`optimize`] on the agent's problem.
    pub fn optimize(&self, iterations: Option<usize>, n_points: usize) -> Plan {
        optimize(
            &self.to_optimization_problem(),
            iterations,
            self.options(n_points),
        )
    }

    /// Run the optimization loop inside one run — [`optimize_in_run`] on
    /// the agent's problem.
    pub fn optimize_in_run(&self, iterations: usize, n_points: usize) -> Plan {
        optimize_in_run(
            &self.to_optimization_problem(),
            iterations,
            self.options(n_points),
        )
    }

    /// Evaluate optimizer suggestions (points that carry an id) and ingest
    /// the outcomes.
    pub fn sample_suggestions(
        &self,
        suggestions: Vec<Suggestion>,
    ) -> PlanWithResult<StepResult<U>> {
        sample_suggestions(
            &self.to_optimization_problem(),
            suggestions,
            self.options(1),
        )
    }

    /// Register caller-chosen points with the optimizer, evaluate them and
    /// ingest the outcomes.
    pub fn sample_candidates(&self, candidates: Vec<Candidate>) -> PlanWithResult<StepResult<U>> {
        sample_candidates(&self.to_optimization_problem(), candidates, self.options(1))
    }

    /// Acquire a reference point at `parameterization`, or at the current
    /// positions when `None`.
    pub fn acquire_baseline(&self, parameterization: Option<Params>) -> Plan {
        acquire_baseline(
            &self.to_optimization_problem(),
            parameterization,
            AcquireArgs::default(),
        )
    }

    /// Move the actuators to `parameterization`, or to the optimizer's best
    /// point when `None`.
    pub fn navigate_to_best(&self, parameterization: Option<Params>) -> Plan {
        navigate_to_best(
            self.actuators.clone(),
            Some(self.optimizer.clone()),
            parameterization,
        )
    }
}
