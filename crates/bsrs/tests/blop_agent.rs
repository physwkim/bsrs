//! blop agent tests — ports of the backend-independent cases of blop's
//! `tests/ax/test_agent.py` onto `bsrs::blop::Agent`, with a recording
//! optimizer where blop inspects the Ax client.
//!
//! Not ported: `fixed_dofs`, `reconfigure_search_space`, `from_checkpoint`
//! and the `plot_objective` surface (Ax client state), and the callback
//! list tests (`subscribe`/`unsubscribe`), since bsrs subscribes document
//! sinks on the RunEngine rather than per plan.

#![cfg(feature = "blop")]

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::sync::oneshot;

use bsrs::backends::soft::{SoftDetector, SoftMotor};
use bsrs::blop::{
    shared, AcquisitionPlan, AcquisitionUid, Actuator, Agent, BestPoint, ChoiceDof, Dof,
    DofBinding, DofConstraint, EvaluationFunction, ExperimentSpec, Objective, Optimizer, Outcome,
    OutcomeConstraint, ParameterType, RangeDof, RangeType, ScalarizedObjective, Sensor,
    SharedOptimizer, Suggestion, TrialId,
};
use bsrs::callbacks::CapturingSink;
use bsrs::core::plan::plan_box;
use bsrs::event_model::Document;
use bsrs::prelude::*;

// ---------------------------------------------------------------------------
//  fixtures
// ---------------------------------------------------------------------------

/// An optimizer that records what it was configured with and what it was
/// told, and suggests the centre of each range — the stand-in for Ax.
#[derive(Default)]
struct RecordingOptimizer {
    spec: Option<ExperimentSpec>,
    next_id: i64,
    suggest_calls: Vec<usize>,
    ingested: Vec<Outcome>,
    checkpoints: usize,
    best: Vec<BestPoint>,
}

impl RecordingOptimizer {
    fn configured(spec: &ExperimentSpec) -> Self {
        Self {
            spec: Some(spec.clone()),
            ..Default::default()
        }
    }
}

impl Optimizer for RecordingOptimizer {
    fn suggest(&mut self, n: usize) -> Result<Vec<Suggestion>> {
        self.suggest_calls.push(n);
        let spec = self.spec.as_ref().expect("configured");
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let params = spec.parameters.iter().map(|p| {
                let value = match p {
                    bsrs::blop::ParameterConfig::Range { bounds, .. } => {
                        json!((bounds.0 + bounds.1) / 2.0)
                    }
                    bsrs::blop::ParameterConfig::Choice { values, .. } => values[0].clone(),
                };
                (p.name().to_owned(), value)
            });
            out.push(Suggestion::new(self.next_id, params));
            self.next_id += 1;
        }
        Ok(out)
    }
    fn ingest(&mut self, outcomes: Vec<Outcome>) -> Result<()> {
        self.ingested.extend(outcomes);
        Ok(())
    }
    fn best_points(&self) -> Result<Vec<BestPoint>> {
        Ok(self.best.clone())
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.checkpoints += 1;
        Ok(())
    }
}

/// The optimizer the agent built, once `build` has run.
type Handle = Arc<Mutex<Option<Arc<Mutex<RecordingOptimizer>>>>>;

/// Build the optimizer through the agent and keep a handle on it.
fn recording() -> (
    Handle,
    impl FnOnce(&ExperimentSpec) -> Result<SharedOptimizer>,
) {
    let handle: Handle = Arc::new(Mutex::new(None));
    let h = handle.clone();
    let make = move |spec: &ExperimentSpec| {
        let opt = Arc::new(Mutex::new(RecordingOptimizer::configured(spec)));
        *h.lock().unwrap() = Some(opt.clone());
        Ok(Arc::new(Mutex::new(Forward(opt))) as SharedOptimizer)
    };
    (handle, make)
}

/// Forwards to a shared `RecordingOptimizer` so the test can inspect it.
struct Forward(Arc<Mutex<RecordingOptimizer>>);

impl Optimizer for Forward {
    fn name(&self) -> String {
        "RecordingOptimizer".into()
    }
    fn suggest(&mut self, n: usize) -> Result<Vec<Suggestion>> {
        self.0.lock().unwrap().suggest(n)
    }
    fn ingest(&mut self, outcomes: Vec<Outcome>) -> Result<()> {
        self.0.lock().unwrap().ingest(outcomes)
    }
    fn best_points(&self) -> Result<Vec<BestPoint>> {
        self.0.lock().unwrap().best_points()
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.0.lock().unwrap().checkpoint()
    }
}

fn movable(name: &str) -> Actuator {
    Actuator::new(Arc::new(SoftMotor::new(name, Some(0.0))))
}

fn readable(name: &str) -> Sensor {
    SoftDetector::new(name) as Sensor
}

fn range(binding: impl Into<DofBinding>) -> Dof {
    RangeDof::new(binding, (0.0, 10.0), RangeType::Float).into()
}

fn out(id: impl Into<TrialId>, pairs: &[(&str, Value)]) -> Outcome {
    Outcome::new(id, pairs.iter().map(|(k, v)| (k.to_string(), v.clone())))
}

/// The `MagicMock(spec=EvaluationFunction)` of the Python tests: returns
/// one fixed outcome per suggestion.
fn mock_eval<U: AcquisitionUid>() -> EvaluationFunction<U> {
    EvaluationFunction::sync("mock_evaluation_function", |_uid: U, suggestions| {
        Ok(suggestions
            .iter()
            .map(|s| out(s.id.clone(), &[("test_objective", json!(1.0))]))
            .collect())
    })
}

/// The `MagicMock(spec=AcquisitionPlan)`: one `Null`, then the uid.
fn mock_acquisition() -> AcquisitionPlan<String> {
    AcquisitionPlan::new("mock_acquisition_plan", |_, _, _, _| {
        let (tx, rx) = oneshot::channel();
        let plan = plan_box(async_stream::stream! {
            yield Msg::Null;
            let _ = tx.send("mock-uid".to_string());
        });
        (plan, rx)
    })
}

// ---------------------------------------------------------------------------
//  tests
// ---------------------------------------------------------------------------

#[test]
fn agent_init() {
    let (movable1, movable2) = (movable("test_movable1"), movable("test_movable2"));
    let (dof1, dof2) = (range(movable1.clone()), range(movable2.clone()));
    let constraint = DofConstraint::new(
        "x1 + x2 <= 10",
        [("x1", dof1.clone()), ("x2", dof2.clone())],
    )
    .unwrap();
    let (handle, make) = recording();
    let agent = Agent::builder(
        [dof1, dof2],
        Objective::maximize("test_objective"),
        mock_eval(),
    )
    .sensors([readable("test_readable")])
    .dof_constraints([constraint])
    .acquisition_plan(mock_acquisition())
    .build(make)
    .unwrap();

    let names: Vec<&str> = agent.sensors().iter().map(|s| s.name()).collect();
    assert_eq!(names, ["test_readable"]);
    let names: Vec<&str> = agent.actuators().iter().map(Actuator::name).collect();
    assert_eq!(names, ["test_movable1", "test_movable2"]);
    assert_eq!(
        agent.evaluation_function().name(),
        "mock_evaluation_function"
    );
    assert_eq!(
        agent.acquisition_plan().map(AcquisitionPlan::name),
        Some("mock_acquisition_plan")
    );
    let configured = handle.lock().unwrap().clone().unwrap();
    let spec = configured.lock().unwrap().spec.clone().unwrap();
    assert_eq!(spec, *agent.spec());
    assert_eq!(spec.parameter_names(), ["test_movable1", "test_movable2"]);
    assert_eq!(spec.objective, "test_objective");
    assert_eq!(
        spec.parameter_constraints,
        ["test_movable1 + test_movable2 <= 10"]
    );
    assert!(spec.outcome_constraints.is_empty());
}

#[test]
fn agent_spec_is_json_for_a_bridge() {
    let dof = ChoiceDof::new("filter", ["none", "Al"], ParameterType::Str);
    let objective = Objective::minimize("width");
    let constraint = OutcomeConstraint::new("w <= 100", [("w", &objective)]).unwrap();
    let (_, make) = recording();
    let agent = Agent::builder(
        [Dof::from(dof), range("x")],
        objective,
        mock_eval::<String>(),
    )
    .outcome_constraints([constraint])
    .build(make)
    .unwrap();
    assert_eq!(
        serde_json::to_value(agent.spec()).unwrap(),
        json!({
            "parameters": [
                {"kind": "choice", "name": "filter", "values": ["none", "Al"], "parameter_type": "str"},
                {"kind": "range", "name": "x", "bounds": [0.0, 10.0], "parameter_type": "float"},
            ],
            "objective": "-width",
            "parameter_constraints": [],
            "outcome_constraints": ["width <= 100"],
        })
    );
}

#[test]
fn agent_to_optimization_problem() {
    let (movable1, movable2) = (movable("test_movable1"), movable("test_movable2"));
    let (dof1, dof2) = (range(movable1), range(movable2));
    let constraint = DofConstraint::new(
        "x1 + x2 <= 10",
        [("x1", dof1.clone()), ("x2", dof2.clone())],
    )
    .unwrap();
    let (_, make) = recording();
    let agent = Agent::builder(
        [dof1, dof2],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .dof_constraints([constraint])
    .build(make)
    .unwrap();
    let problem = agent.to_optimization_problem();
    assert_eq!(
        problem.evaluation_function.name(),
        "mock_evaluation_function"
    );
    let names: Vec<&str> = problem.actuators.iter().map(Actuator::name).collect();
    assert_eq!(names, ["test_movable1", "test_movable2"]);
    assert!(problem.sensors.is_empty());
    assert_eq!(
        problem.optimizer.lock().unwrap().name(),
        "RecordingOptimizer"
    );
    assert!(Arc::ptr_eq(&problem.optimizer, agent.optimizer()));
    assert!(problem.acquisition_plan.is_none());
}

#[test]
fn agent_suggest() {
    let (_, make) = recording();
    let agent = Agent::builder(
        [
            range(movable("test_movable1")),
            range(movable("test_movable2")),
        ],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .build(make)
    .unwrap();
    let suggestions = agent.suggest(1).unwrap();
    assert_eq!(suggestions.len(), 1);
    assert_eq!(suggestions[0].id, TrialId::Int(0));
    assert!(suggestions[0].params["test_movable1"].is_number());
    assert!(suggestions[0].params["test_movable2"].is_number());
}

#[test]
fn agent_suggest_multiple() {
    let (_, make) = recording();
    let agent = Agent::builder(
        [
            range(movable("test_movable1")),
            range(movable("test_movable2")),
        ],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .build(make)
    .unwrap();
    let suggestions = agent.suggest(5).unwrap();
    assert_eq!(suggestions.len(), 5);
    for (i, s) in suggestions.iter().enumerate() {
        assert_eq!(s.id, TrialId::Int(i as i64));
        assert!(s.params.contains_key("test_movable1"));
        assert!(s.params.contains_key("test_movable2"));
    }
}

#[test]
fn agent_ingest_multiple_and_baseline() {
    let (handle, make) = recording();
    let agent = Agent::builder(
        [
            range(movable("test_movable1")),
            range(movable("test_movable2")),
        ],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .build(make)
    .unwrap();
    let points = vec![
        out(
            0,
            &[
                ("test_movable1", json!(0.1)),
                ("test_movable2", json!(0.2)),
                ("test_objective", json!(0.3)),
            ],
        ),
        out(
            1,
            &[
                ("test_movable1", json!(1.1)),
                ("test_movable2", json!(1.2)),
                ("test_objective", json!(1.3)),
            ],
        ),
        out(
            "baseline",
            &[
                ("test_movable1", json!(0.0)),
                ("test_objective", json!(0.0)),
            ],
        ),
    ];
    agent.ingest(points.clone()).unwrap();
    let configured = handle.lock().unwrap().clone().unwrap();
    assert_eq!(configured.lock().unwrap().ingested, points);
}

#[test]
fn agent_best_points_and_checkpoint_delegate() {
    let (handle, make) = recording();
    let agent = Agent::builder(
        [range("x1")],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .build(make)
    .unwrap();
    let configured = handle.lock().unwrap().clone().unwrap();
    let best = BestPoint {
        id: TrialId::Int(1),
        params: [("x1".to_string(), json!(7.0))].into_iter().collect(),
        outcomes: [("test_objective".to_string(), json!(20.0))]
            .into_iter()
            .collect(),
    };
    configured.lock().unwrap().best = vec![best.clone()];
    assert_eq!(agent.best_points().unwrap(), [best]);
    agent.checkpoint().unwrap();
    assert_eq!(configured.lock().unwrap().checkpoints, 1);
}

#[test]
fn agent_init_remote_actuator_raises() {
    let remote = |actuator: &str| {
        range(DofBinding::Remote {
            actuator: actuator.into(),
            name: None,
        })
    };
    let (_, make) = recording();
    let err = Agent::builder(
        [remote("test_movable1"), remote("test_movable2")],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .build(make)
    .unwrap_err();
    assert!(
        err.to_string().contains("must be `Actuator` instances"),
        "{err}"
    );
    assert!(err.to_string().contains("test_movable1"), "{err}");
}

#[test]
fn agent_init_duplicate_dof_names() {
    let same = |name: &str| ChoiceDof::new(name, ["A, B"], ParameterType::Str);
    let (_, make) = recording();
    let err = Agent::builder(
        [range("test_same_name"), Dof::from(same("test_same_name"))],
        Objective::minimize("test_objective"),
        mock_eval::<String>(),
    )
    .build(make)
    .unwrap_err();
    assert!(err.to_string().contains("names must be unique"), "{err}");

    let (_, make) = recording();
    Agent::builder(
        [
            range("test_same_name"),
            Dof::from(same("test_different_name")),
        ],
        Objective::minimize("test_objective"),
        mock_eval::<String>(),
    )
    .build(make)
    .unwrap();
}

#[test]
fn agent_scalarized_objective() {
    let scalarized =
        ScalarizedObjective::new("-50 * x + 100 * y", false, [("x", "obj1"), ("y", "obj2")])
            .unwrap();
    let (_, make) = recording();
    let agent = Agent::builder([range("test1")], scalarized, mock_eval::<String>())
        .build(make)
        .unwrap();
    assert_eq!(agent.spec().objective, "-50 * obj1 + 100 * obj2");
}

#[tokio::test]
async fn agent_optimize_runs_on_the_engine() {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let (handle, make) = recording();
    let agent = Agent::builder(
        [
            range(movable("test_movable1")),
            range(movable("test_movable2")),
        ],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .sensors([readable("test_readable")])
    .acquisition_plan(mock_acquisition())
    .build(make)
    .unwrap();

    let result = re.run_async(agent.optimize(Some(3), 2)).await.unwrap();
    assert_eq!(result.exit_status, "success", "{}", result.reason);

    let events = sink
        .snapshot()
        .await
        .iter()
        .filter(|d| matches!(d, Document::Event(_)))
        .count();
    assert_eq!(events, 3);
    let configured = handle.lock().unwrap().clone().unwrap();
    let configured = configured.lock().unwrap();
    assert_eq!(configured.suggest_calls, [2, 2, 2]);
    assert_eq!(configured.ingested.len(), 6);
}

#[tokio::test]
async fn agent_plans_share_one_readable_cache() {
    let re = RunEngine::new(Vec::<Arc<dyn DocumentSink>>::new());
    let (_, make) = recording();
    let agent = Agent::builder(
        [range(movable("x"))],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .acquisition_plan(mock_acquisition())
    .build(make)
    .unwrap();
    assert!(agent.readable_cache().lock().unwrap().is_empty());
    re.run_async(agent.optimize(Some(1), 1)).await.unwrap();
    let after_optimize = agent.readable_cache().lock().unwrap().len();
    assert!(after_optimize > 0);
    let (plan, _) =
        agent.sample_suggestions(vec![Suggestion::new(9, [("x".to_string(), json!(1.0))])]);
    re.run_async(plan).await.unwrap();
    assert_eq!(agent.readable_cache().lock().unwrap().len(), after_optimize);
}

#[tokio::test]
async fn agent_navigate_to_best_moves_the_actuators() {
    let re = RunEngine::new(Vec::<Arc<dyn DocumentSink>>::new());
    let motor = Arc::new(SoftMotor::new("x", Some(0.0)));
    let (handle, make) = recording();
    let agent = Agent::builder(
        [range(Actuator::new(motor.clone()))],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .build(make)
    .unwrap();
    let configured = handle.lock().unwrap().clone().unwrap();
    configured.lock().unwrap().best = vec![BestPoint {
        id: TrialId::Int(0),
        params: [("x".to_string(), json!(4.5))].into_iter().collect(),
        outcomes: Default::default(),
    }];
    let result = re.run_async(agent.navigate_to_best(None)).await.unwrap();
    assert!(!result.interrupted, "{}", result.reason);
    let pos = motor.read().await.unwrap()["x"].value.as_f64().unwrap();
    assert_eq!(pos, 4.5);
}

#[test]
fn shared_helper_builds_a_shared_optimizer() {
    let opt: SharedOptimizer = shared(RecordingOptimizer::default());
    assert_eq!(opt.lock().unwrap().name(), "RecordingOptimizer");
}
