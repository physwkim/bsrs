//! blop plan tests — ports of blop's `tests/test_plans.py` and
//! `tests/test_plan_stubs.py` onto the bsrs `blop` module.
//!
//! Not ported, because the typed API cannot express the input:
//! `test_list_scan_in_run_ignores_non_readable_sensors` (a `Sensor` is
//! readable by type), `test_optimize_in_run_rejects_unhashable_suggestion_ids`,
//! `test_optimize_step_rejects_suggestion_without_id`, and the two
//! missing-/unhashable-id cases of `test_optimize_step_rejects_invalid_outcome_ids`
//! (a `TrialId` is always present and hashable). The four `repr`-based
//! acquisition-uid tests collapse into `custom_uid_records_its_event_value`.

#![cfg(feature = "blop")]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use bsrs::backends::soft::{SoftDetector, SoftMotor};
use bsrs::blop::{
    acquire_baseline, default_acquire, list_scan_in_run, navigate_to_best, optimize,
    optimize_in_run, optimize_step, shared, AcquireArgs, AcquisitionPlan, AcquisitionUid, Actuator,
    BestPoint, EvaluationFunction, OptimizationProblem, Optimizer, Outcome, Params, Sensor,
    SharedOptimizer, StepOptions, Suggestion, TrialId,
};
use bsrs::callbacks::CapturingSink;
use bsrs::core::msg::{MovableObj, NamedObj, ReadableObj, RunMetadata, StageableObj};
use bsrs::core::plan::{plan_box, plan_items, Plan};
use bsrs::core::reading::ReadingValue;
use bsrs::event_model::{DataKey, Dtype, Event};
use bsrs::plans::preprocessors::pchain;
use bsrs::plans::{stubs as bps, PerStep};
use bsrs::prelude::*;

// ---------------------------------------------------------------------------
//  fixtures
// ---------------------------------------------------------------------------

/// A scripted optimizer recording every call — the `MagicMock(spec=Optimizer)`
/// of the Python tests.
#[derive(Default)]
struct MockOptimizer {
    suggestions: Vec<Suggestion>,
    suggest_calls: Vec<usize>,
    ingest_calls: Vec<Vec<Outcome>>,
    failure_calls: Vec<Vec<Suggestion>>,
    stopping: bool,
    stop_script: VecDeque<(bool, Option<String>)>,
    stop_calls: usize,
    checkpointable: bool,
    checkpoint_calls: usize,
    best: Vec<BestPoint>,
}

impl MockOptimizer {
    fn suggesting(suggestions: Vec<Suggestion>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            suggestions,
            ..Default::default()
        }))
    }

    fn stopping(self, script: Vec<(bool, Option<&str>)>) -> Self {
        Self {
            stopping: true,
            stop_script: script
                .into_iter()
                .map(|(s, r)| (s, r.map(str::to_owned)))
                .collect(),
            ..self
        }
    }

    fn checkpointable(self) -> Self {
        Self {
            checkpointable: true,
            ..self
        }
    }
}

impl Optimizer for MockOptimizer {
    fn suggest(&mut self, n: usize) -> Result<Vec<Suggestion>> {
        self.suggest_calls.push(n);
        Ok(self.suggestions.clone())
    }
    fn ingest(&mut self, outcomes: Vec<Outcome>) -> Result<()> {
        self.ingest_calls.push(outcomes);
        Ok(())
    }
    fn best_points(&self) -> Result<Vec<BestPoint>> {
        Ok(self.best.clone())
    }
    fn register_failures(&mut self, suggestions: &[Suggestion]) {
        self.failure_calls.push(suggestions.to_vec());
    }
    fn supports_stopping_criteria(&self) -> bool {
        self.stopping
    }
    fn should_stop(&self) -> (bool, Option<String>) {
        // `should_stop` is `&self`; the script is consumed through interior
        // mutability elsewhere, so count here and read by index.
        let idx = self.stop_calls;
        self.stop_script.get(idx).cloned().unwrap_or((false, None))
    }
    fn supports_checkpoint(&self) -> bool {
        self.checkpointable
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.checkpoint_calls += 1;
        Ok(())
    }
}

/// `should_stop` takes `&self`, so a call counter needs a wrapper that
/// advances the script — this is that wrapper.
struct CountingStop(Arc<Mutex<MockOptimizer>>);

impl Optimizer for CountingStop {
    fn name(&self) -> String {
        "MockOptimizer".into()
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
    fn register_failures(&mut self, suggestions: &[Suggestion]) {
        self.0.lock().unwrap().register_failures(suggestions)
    }
    fn supports_stopping_criteria(&self) -> bool {
        self.0.lock().unwrap().stopping
    }
    fn should_stop(&self) -> (bool, Option<String>) {
        let mut inner = self.0.lock().unwrap();
        let decision = inner.should_stop();
        inner.stop_calls += 1;
        decision
    }
    fn supports_checkpoint(&self) -> bool {
        self.0.lock().unwrap().checkpointable
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.0.lock().unwrap().checkpoint()
    }
}

/// Values a closure records for the test to inspect afterwards.
type Captured<T> = Arc<Mutex<Vec<T>>>;

fn captured<T>() -> Captured<T> {
    Arc::new(Mutex::new(vec![]))
}

fn as_shared(mock: &Arc<Mutex<MockOptimizer>>) -> SharedOptimizer {
    Arc::new(Mutex::new(CountingStop(mock.clone())))
}

fn sug(id: impl Into<TrialId>, pairs: &[(&str, Value)]) -> Suggestion {
    Suggestion::new(id, pairs.iter().map(|(k, v)| (k.to_string(), v.clone())))
}

fn out(id: impl Into<TrialId>, pairs: &[(&str, Value)]) -> Outcome {
    Outcome::new(id, pairs.iter().map(|(k, v)| (k.to_string(), v.clone())))
}

fn params(pairs: &[(&str, Value)]) -> Params {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn motor(name: &str, initial: f64) -> Arc<SoftMotor> {
    Arc::new(SoftMotor::new(name, Some(initial)))
}

fn actuator(name: &str, initial: f64) -> Actuator {
    Actuator::new(motor(name, initial))
}

fn sensor(name: &str) -> Sensor {
    SoftDetector::new(name) as Sensor
}

async fn position(m: &SoftMotor) -> f64 {
    m.read().await.unwrap()[m.name()].value.as_f64().unwrap()
}

/// An evaluation function returning fixed outcomes and counting its calls.
fn const_eval<U: AcquisitionUid>(
    outcomes: Vec<Outcome>,
) -> (EvaluationFunction<U>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let f = EvaluationFunction::sync("const_eval", move |_uid: U, _s| {
        c.fetch_add(1, Ordering::SeqCst);
        Ok(outcomes.clone())
    });
    (f, calls)
}

/// An acquisition plan that emits one `Null` and produces `uid`.
fn null_acquisition<U: AcquisitionUid>(uid: U) -> AcquisitionPlan<U> {
    AcquisitionPlan::new("null_acquisition", move |_, _, _, _| {
        let uid = uid.clone();
        let (tx, rx) = oneshot::channel();
        let plan = plan_box(async_stream::stream! {
            yield Msg::Null;
            let _ = tx.send(uid);
        });
        (plan, rx)
    })
}

fn no_args() -> AcquireArgs {
    AcquireArgs::default()
}

struct Harness {
    re: RunEngine,
    sink: Arc<CapturingSink>,
}

impl Harness {
    fn new() -> Self {
        let sink = Arc::new(CapturingSink::new());
        let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
        Self { re, sink }
    }

    async fn run(&self, plan: Plan) -> RunResult {
        self.re.run_async(plan).await.expect("engine error")
    }

    /// A plan that ends cleanly: `success` when it opened a run, `no-run`
    /// when it did not (bluesky returns `()` for both).
    async fn run_ok(&self, plan: Plan) -> RunResult {
        let r = self.run(plan).await;
        assert!(
            matches!(r.exit_status.as_str(), "success" | "no-run"),
            "exit {:?}, reason: {}",
            r.exit_status,
            r.reason
        );
        r
    }

    async fn run_fails_with(&self, plan: Plan, needle: &str) -> RunResult {
        let r = self.run(plan).await;
        assert_eq!(
            r.exit_status, "fail",
            "expected failure containing {needle:?}"
        );
        assert!(
            r.reason.contains(needle),
            "reason {:?} lacks {needle:?}",
            r.reason
        );
        r
    }

    async fn docs(&self) -> Vec<Document> {
        self.sink.snapshot().await
    }

    /// Events of the outer run whose `run_key` is `key` — the Python
    /// `_collect_optimize_events`.
    async fn run_events(&self, key: &str) -> Vec<Event> {
        let docs = self.docs().await;
        let run = docs.iter().find_map(|d| match d {
            Document::Start(s) if s.extra.get("run_key") == Some(&json!(key)) => {
                Some(s.uid.clone())
            }
            _ => None,
        });
        let Some(run) = run else { return vec![] };
        let descriptors: HashSet<String> = docs
            .iter()
            .filter_map(|d| match d {
                Document::Descriptor(desc) if desc.run_start == run => Some(desc.uid.clone()),
                _ => None,
            })
            .collect();
        docs.into_iter()
            .filter_map(|d| match d {
                Document::Event(e) if descriptors.contains(&e.descriptor) => Some(e),
                _ => None,
            })
            .collect()
    }

    /// Events grouped by stream name — the Python `_events_by_stream`.
    async fn events_by_stream(&self) -> HashMap<String, Vec<Event>> {
        events_by_stream(&self.docs().await)
    }

    async fn start_docs(&self) -> Vec<RunStart> {
        self.docs()
            .await
            .into_iter()
            .filter_map(|d| match d {
                Document::Start(s) => Some(s),
                _ => None,
            })
            .collect()
    }

    /// Every message the engine dispatched, by variant name.
    fn record_commands(&self) -> Arc<Mutex<Vec<String>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s = seen.clone();
        self.re.set_msg_hook(Some(Arc::new(move |msg: &Msg| {
            // A run-routed message is recorded as its command, as bluesky's
            // `msg_hook` sees `msg.command` whatever `msg.run` is.
            let msg = match msg {
                Msg::InRun { inner, .. } => inner.as_ref(),
                other => other,
            };
            let repr = format!("{msg:?}");
            let head = repr.split([' ', '(', '{']).next().unwrap_or("").to_string();
            s.lock().unwrap().push(head);
        })));
        seen
    }
}

fn events_by_stream(docs: &[Document]) -> HashMap<String, Vec<Event>> {
    let names: HashMap<String, String> = docs
        .iter()
        .filter_map(|d| match d {
            Document::Descriptor(desc) => {
                Some((desc.uid.clone(), desc.name.clone().unwrap_or_default()))
            }
            _ => None,
        })
        .collect();
    let mut out: HashMap<String, Vec<Event>> = HashMap::new();
    for d in docs {
        if let Document::Event(e) = d {
            out.entry(names[&e.descriptor].clone())
                .or_default()
                .push(e.clone());
        }
    }
    out
}

fn as_list(v: &Value) -> Vec<Value> {
    match v {
        Value::Array(items) => items.clone(),
        other => vec![other.clone()],
    }
}

/// A motor counting its reads and recording its setpoints — the
/// `patch.object(..., wraps=...)` of the Python tests.
struct CountingMotor {
    name: String,
    value: Mutex<f64>,
    reads: AtomicUsize,
    sets: Mutex<Vec<f64>>,
}

impl CountingMotor {
    fn new(name: &str, initial: f64) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            value: Mutex::new(initial),
            reads: AtomicUsize::new(0),
            sets: Mutex::new(vec![]),
        })
    }
    fn value(&self) -> f64 {
        *self.value.lock().unwrap()
    }
    fn sets(&self) -> Vec<f64> {
        self.sets.lock().unwrap().clone()
    }
}

impl NamedObj for CountingMotor {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl ReadableObj for CountingMotor {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(HashMap::from([(
            self.name.clone(),
            ReadingValue::new(json!(self.value()), 0.0),
        )]))
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        Ok(HashMap::from([(self.name.clone(), number_key(&self.name))]))
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![self.name.clone()])
    }
}

#[async_trait]
impl MovableObj for CountingMotor {
    async fn set_dyn(&self, value: f64) -> Status {
        *self.value.lock().unwrap() = value;
        self.sets.lock().unwrap().push(value);
        Status::done()
    }
}

fn number_key(source: &str) -> DataKey {
    DataKey {
        source: source.to_owned(),
        dtype: Dtype::Number,
        shape: vec![],
        dtype_numpy: None,
        external: None,
        units: None,
        precision: None,
        object_name: None,
        dims: None,
        limits: None,
        choices: None,
    }
}

/// A sensor counting its reads and, optionally, its stage/unstage calls.
struct CountingSensor {
    name: String,
    reads: AtomicUsize,
    stages: AtomicUsize,
    unstages: AtomicUsize,
    stageable: bool,
}

impl CountingSensor {
    fn new(name: &str, stageable: bool) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            reads: AtomicUsize::new(0),
            stages: AtomicUsize::new(0),
            unstages: AtomicUsize::new(0),
            stageable,
        })
    }
}

impl NamedObj for CountingSensor {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl ReadableObj for CountingSensor {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(HashMap::from([(
            self.name.clone(),
            ReadingValue::new(json!(0.0), 0.0),
        )]))
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        Ok(HashMap::from([(self.name.clone(), number_key(&self.name))]))
    }
    fn as_stageable(self: Arc<Self>) -> Option<Arc<dyn StageableObj>> {
        self.stageable.then_some(self as Arc<dyn StageableObj>)
    }
}

#[async_trait]
impl StageableObj for CountingSensor {
    async fn stage_dyn(&self) -> Result<()> {
        self.stages.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn unstage_dyn(&self) -> Result<()> {
        self.unstages.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A uid type with no JSON form of its own; its event value is its `Debug`
/// repr, as blop stores `repr(uid)` for non-array-like identifiers.
#[derive(Clone, Debug, PartialEq)]
struct CustomUid {
    correlation_uid: String,
    item_uid: Option<String>,
}

impl AcquisitionUid for CustomUid {
    fn event_value(&self) -> Value {
        Value::from(format!("{self:?}"))
    }
}

// ---------------------------------------------------------------------------
//  optimize
// ---------------------------------------------------------------------------

#[tokio::test]
async fn optimize_records_one_event_per_step() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.0))])]);
    let (eval, calls) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_ok(optimize(&problem, Some(1), StepOptions::default()))
        .await;

    let events = h.run_events("optimize").await;
    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls, [1]);
    assert_eq!(m.ingest_calls, [vec![out(0, &[("objective", json!(0.0))])]]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    assert_eq!(events.len(), 1);
    let data = &events[0].data;
    assert!(data.contains_key("suggestion_ids"));
    assert_eq!(data["x1"], json!(0.0));
    assert_eq!(data["objective"], json!(0.0));
    assert!(data["acquisition_uid"]
        .as_str()
        .is_some_and(|s| !s.is_empty()));
}

#[tokio::test]
async fn acquisition_failure_registers_failures_and_skips_evaluation() {
    let h = Harness::new();
    let suggestions = vec![sug(0, &[("x1", json!(0.0))])];
    let mock = MockOptimizer::suggesting(suggestions.clone());
    let (eval, calls) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let failing = AcquisitionPlan::new("failing", |_, _, _, _| {
        let (_tx, rx) = oneshot::channel::<String>();
        let plan = plan_box(async_stream::stream! {
            yield Msg::Fail("acquisition broke".into());
        });
        (plan, rx)
    });
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(failing);

    h.run_fails_with(
        optimize(&problem, Some(1), StepOptions::default()),
        "acquisition broke",
    )
    .await;

    assert_eq!(mock.lock().unwrap().failure_calls, [suggestions]);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn evaluation_failure_registers_suggestions() {
    let h = Harness::new();
    let suggestions = vec![sug(0, &[("x1", json!(0.0))])];
    let mock = MockOptimizer::suggesting(suggestions.clone());
    let eval = EvaluationFunction::<String>::sync("broken", |_, _| {
        Err(BsrsError::Other("evaluation failed".into()))
    });
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(null_acquisition("test-uid-123".to_string()));

    let (plan, _) = optimize_step(&problem, 1, no_args());
    h.run_fails_with(plan, "evaluation failed").await;

    let m = mock.lock().unwrap();
    assert_eq!(m.failure_calls, [suggestions]);
    assert!(m.ingest_calls.is_empty());
}

#[tokio::test]
async fn optimize_multiple_iterations() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.0))])]);
    let (eval, calls) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_ok(optimize(&problem, Some(5), StepOptions::default()))
        .await;

    let events = h.run_events("optimize").await;
    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls, [1; 5]);
    assert_eq!(m.ingest_calls.len(), 5);
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    assert_eq!(events.len(), 5);
    for e in &events {
        assert!(e.data.contains_key("suggestion_ids"));
        assert!(e.data.contains_key("acquisition_uid"));
        assert_eq!(e.data["x1"], json!(0.0));
        assert_eq!(e.data["objective"], json!(0.0));
    }
}

#[tokio::test]
async fn optimize_multiple_with_n_points() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![
        sug(0, &[("x1", json!(0.0))]),
        sug(1, &[("x1", json!(0.1))]),
    ]);
    let outcomes = vec![
        out(0, &[("objective", json!(0.0))]),
        out(1, &[("objective", json!(0.1))]),
    ];
    let (eval, calls) = const_eval::<String>(outcomes.clone());
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_ok(optimize(
        &problem,
        Some(5),
        StepOptions {
            n_points: 2,
            ..Default::default()
        },
    ))
    .await;

    let events = h.run_events("optimize").await;
    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls, [2; 5]);
    assert_eq!(m.ingest_calls.last(), Some(&outcomes));
    assert_eq!(m.ingest_calls.len(), 5);
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    assert_eq!(events.len(), 5);
    for e in &events {
        assert_eq!(as_list(&e.data["suggestion_ids"]).len(), 2);
        assert_eq!(as_list(&e.data["x1"]), [json!(0.0), json!(0.1)]);
        assert_eq!(as_list(&e.data["objective"]), [json!(0.0), json!(0.1)]);
    }
}

#[tokio::test]
async fn optimize_complex_case() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![
        sug(
            0,
            &[("x1", json!(0.0)), ("x2", json!(0.0)), ("x3", json!(0.0))],
        ),
        sug(
            1,
            &[("x1", json!(0.1)), ("x2", json!(0.2)), ("x3", json!(0.3))],
        ),
    ]);
    let outcomes = vec![
        out(0, &[("objective1", json!(0.0)), ("objective2", json!(0.1))]),
        out(1, &[("objective1", json!(0.1)), ("objective2", json!(0.2))]),
    ];
    let (eval, calls) = const_eval::<String>(outcomes.clone());
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![
            actuator("x1", -1.0),
            actuator("x2", -1.0),
            actuator("x3", -1.0),
        ],
        vec![sensor("readable1"), sensor("readable2")],
        eval,
    );

    let result = h
        .run_ok(optimize(
            &problem,
            Some(2),
            StepOptions {
                n_points: 2,
                ..Default::default()
            },
        ))
        .await;

    let events = h.run_events("optimize").await;
    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls, [2, 2]);
    assert_eq!(m.ingest_calls, [outcomes.clone(), outcomes]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(events.len(), 2);
    for e in &events {
        assert_eq!(as_list(&e.data["x1"]), [json!(0.0), json!(0.1)]);
        assert_eq!(as_list(&e.data["x2"]), [json!(0.0), json!(0.2)]);
        assert_eq!(as_list(&e.data["x3"]), [json!(0.0), json!(0.3)]);
        assert_eq!(as_list(&e.data["objective1"]), [json!(0.0), json!(0.1)]);
        assert_eq!(as_list(&e.data["objective2"]), [json!(0.1), json!(0.2)]);
        assert_eq!(as_list(&e.data["suggestion_ids"]), [json!("0"), json!("1")]);
        let uid = e.data["acquisition_uid"].as_str().unwrap();
        assert!(
            result.run_uids.iter().any(|u| u == uid),
            "{uid} not in {:?}",
            result.run_uids
        );
    }
}

#[tokio::test]
async fn optimize_checkpoints_at_the_interval() {
    for interval in [0usize, 1, 2, 3] {
        let h = Harness::new();
        let mock = Arc::new(Mutex::new(
            MockOptimizer {
                suggestions: vec![sug(0, &[("x1", json!(0.0))])],
                ..Default::default()
            }
            .checkpointable(),
        ));
        let (eval, _) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
        let problem = OptimizationProblem::new(
            as_shared(&mock),
            vec![actuator("x1", -1.0)],
            vec![sensor("objective")],
            eval,
        );

        h.run_ok(optimize(
            &problem,
            Some(5),
            StepOptions {
                n_points: 2,
                checkpoint_interval: Some(interval),
                ..Default::default()
            },
        ))
        .await;

        let expected = 5usize.checked_div(interval).unwrap_or(0);
        assert_eq!(
            mock.lock().unwrap().checkpoint_calls,
            expected,
            "interval {interval}"
        );
    }
}

#[tokio::test]
async fn optimize_rejects_checkpoint_interval_on_non_checkpointable_optimizer() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.0))])]);
    let (eval, _) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_fails_with(
        optimize(
            &problem,
            Some(5),
            StepOptions {
                n_points: 2,
                checkpoint_interval: Some(1),
                ..Default::default()
            },
        ),
        "optimizer is not checkpointable",
    )
    .await;
}

// ---------------------------------------------------------------------------
//  optimize_in_run / list_scan_in_run
// ---------------------------------------------------------------------------

#[tokio::test]
async fn optimize_in_run_defaults_to_ordered_suggestion_ids() {
    let h = Harness::new();
    let suggestions = vec![
        sug("far", &[("x1", json!(10.0))]),
        sug("near", &[("x1", json!(1.0))]),
    ];
    let mock = MockOptimizer::suggesting(suggestions.clone());
    let sink = h.sink.clone();
    let captured: Captured<(Vec<TrialId>, Vec<Suggestion>)> = captured();
    let cap = captured.clone();
    let expected = suggestions.clone();
    let eval = EvaluationFunction::<Vec<TrialId>>::new("ordered", move |uid, suggestions| {
        let sink = sink.clone();
        let cap = cap.clone();
        let expected = expected.clone();
        Box::pin(async move {
            cap.lock().unwrap().push((uid.clone(), suggestions.clone()));
            let docs = sink.snapshot().await;
            let primary: Vec<Value> = events_by_stream(&docs)["primary"]
                .iter()
                .map(|e| e.data["x1"].clone())
                .collect();
            assert_eq!(uid, vec![TrialId::from("near"), TrialId::from("far")]);
            assert_eq!(suggestions, expected);
            assert_eq!(primary, [json!(1.0), json!(10.0)]);
            Ok(uid
                .iter()
                .enumerate()
                .map(|(i, id)| {
                    Outcome::new(id.clone(), [("objective".to_string(), json!(i as f64))])
                })
                .collect())
        })
    });
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", 0.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_ok(optimize_in_run(
        &problem,
        1,
        StepOptions {
            n_points: 2,
            ..Default::default()
        },
    ))
    .await;

    assert_eq!(
        *captured.lock().unwrap(),
        vec![(
            vec![TrialId::from("near"), TrialId::from("far")],
            suggestions
        )]
    );
    assert_eq!(
        mock.lock().unwrap().ingest_calls,
        [vec![
            out("near", &[("objective", json!(0.0))]),
            out("far", &[("objective", json!(1.0))]),
        ]]
    );
    let starts = h.start_docs().await;
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].extra["run_key"], json!("optimize_in_run"));
    let by_stream = h.events_by_stream().await;
    assert_eq!(by_stream["primary"].len(), 2);
    assert_eq!(by_stream["optimization"].len(), 1);
    let data = &by_stream["optimization"][0].data;
    assert_eq!(
        as_list(&data["suggestion_ids"]),
        [json!("far"), json!("near")]
    );
    assert_eq!(
        as_list(&data["acquisition_uid"]),
        [json!("near"), json!("far")]
    );
}

#[tokio::test]
async fn list_scan_in_run_allows_custom_per_step_streams() {
    let h = Harness::new();
    let per_step: PerStep = Arc::new(|dets, motors| {
        pchain(vec![
            bps::one_nd_step(dets.clone(), motors),
            bps::trigger_and_read(vec![], dets.clone(), "monitor"),
            bps::trigger_and_read(vec![], dets, "monitor"),
        ])
    });
    let suggestions = vec![sug(0, &[("x1", json!(0.0))]), sug(1, &[("x1", json!(1.0))])];
    let mock = MockOptimizer::suggesting(suggestions.clone());
    let expected = suggestions.clone();
    let eval = EvaluationFunction::<Vec<TrialId>>::sync("check", move |uid, suggestions| {
        assert_eq!(uid, vec![TrialId::from(0), TrialId::from(1)]);
        assert_eq!(suggestions, expected);
        Ok(vec![
            out(0, &[("objective", json!(0.0))]),
            out(1, &[("objective", json!(1.0))]),
        ])
    });
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_ok(optimize_in_run(
        &problem,
        1,
        StepOptions {
            n_points: 2,
            acquire: AcquireArgs {
                per_step: Some(per_step),
                ..Default::default()
            },
            ..Default::default()
        },
    ))
    .await;

    assert_eq!(
        mock.lock().unwrap().ingest_calls,
        [vec![
            out(0, &[("objective", json!(0.0))]),
            out(1, &[("objective", json!(1.0))]),
        ]]
    );
    let by_stream = h.events_by_stream().await;
    assert_eq!(by_stream["primary"].len(), 2);
    assert_eq!(by_stream["monitor"].len(), 4);
    assert_eq!(by_stream["optimization"].len(), 1);
}

#[tokio::test]
async fn list_scan_in_run_accepts_no_sensors() {
    let h = Harness::new();
    let m = motor("x1", -1.0);
    let (scan, ids) = list_scan_in_run(
        vec![sug("point", &[("x1", json!(0.0))])],
        vec![Actuator::new(m.clone())],
        vec![],
        None,
    );
    let plan = pchain(vec![
        bps::open_run(RunMetadata::default()),
        scan,
        bps::close_run("success", None),
    ]);

    h.run_ok(plan).await;

    assert_eq!(ids.await.unwrap(), vec![TrialId::from("point")]);
    assert_eq!(position(&m).await, 0.0);
}

#[tokio::test]
async fn list_scan_in_run_routes_without_actuator_readback() {
    let h = Harness::new();
    let per_step: PerStep = Arc::new(|_, motors| bps::move_per_step(motors));
    let suggestions = vec![
        sug("far", &[("x1", json!(10.0))]),
        sug("near", &[("x1", json!(1.0))]),
    ];
    let m = CountingMotor::new("x1", 0.0);
    let (scan, ids) = list_scan_in_run(
        suggestions.clone(),
        vec![Actuator::move_only(m.clone())],
        vec![],
        Some(per_step),
    );
    let plan = pchain(vec![
        bps::open_run(RunMetadata::default()),
        scan,
        bps::close_run("success", None),
    ]);

    h.run_ok(plan).await;

    let ids = ids.await.unwrap();
    let visited: HashSet<TrialId> = ids.iter().cloned().collect();
    assert_eq!(
        visited,
        HashSet::from([TrialId::from("far"), TrialId::from("near")])
    );
    let last = suggestions
        .iter()
        .find(|s| s.id == ids[ids.len() - 1])
        .unwrap();
    assert_eq!(m.value(), last.params["x1"].as_f64().unwrap());
    assert_eq!(
        m.reads.load(Ordering::SeqCst),
        0,
        "a move-only actuator is never read"
    );
}

#[tokio::test]
async fn list_scan_in_run_preserves_stage_lifecycle() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.0))])]);
    let readable = CountingSensor::new("objective", true);
    let (eval, _) = const_eval::<Vec<TrialId>>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![readable.clone() as Sensor],
        eval,
    );

    h.run_ok(optimize_in_run(&problem, 1, StepOptions::default()))
        .await;

    assert_eq!(readable.stages.load(Ordering::SeqCst), 1);
    assert_eq!(readable.unstages.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn optimize_in_run_rejects_child_run_control() {
    let h = Harness::new();
    let child = AcquisitionPlan::new("child", |_, _, sensors, _| {
        let (tx, rx) = oneshot::channel::<Vec<TrialId>>();
        let plan = plan_items(async_stream::stream! {
            let mut inner = pchain(vec![
                bps::open_run(RunMetadata {
                    extra: HashMap::from([("run_key".to_string(), json!("child"))]),
                    ..Default::default()
                }),
                bps::trigger_and_read(vec![], sensors, "primary"),
                bps::close_run("success", None),
            ]);
            while let Some(item) = inner.next().await {
                yield item;
            }
            let _ = tx.send(vec![]);
        });
        (plan, rx)
    });
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.0))])]);
    let (eval, calls) = const_eval::<Vec<TrialId>>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(child);

    h.run_fails_with(
        optimize_in_run(&problem, 1, StepOptions::default()),
        "must not emit OpenRun",
    )
    .await;

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(mock.lock().unwrap().ingest_calls.is_empty());
}

#[tokio::test]
async fn optimize_in_run_rejects_duplicate_suggestion_ids() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![
        sug("same", &[("x1", json!(0.0))]),
        sug("same", &[("x1", json!(1.0))]),
    ]);
    let (eval, calls) = const_eval::<Vec<TrialId>>(vec![out("same", &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_fails_with(
        optimize_in_run(
            &problem,
            1,
            StepOptions {
                n_points: 2,
                ..Default::default()
            },
        ),
        "unique '_id'",
    )
    .await;

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(mock.lock().unwrap().ingest_calls.is_empty());
}

#[tokio::test]
async fn optimize_in_run_registers_failures() {
    let h = Harness::new();
    let suggestions = vec![sug(0, &[("x1", json!(0.0))])];
    let mock = MockOptimizer::suggesting(suggestions.clone());
    let eval = EvaluationFunction::<String>::sync("broken", |_, _| {
        Err(BsrsError::Other("evaluation failed".into()))
    });
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(null_acquisition("test-uid-123".to_string()));

    h.run_fails_with(
        optimize_in_run(&problem, 1, StepOptions::default()),
        "evaluation failed",
    )
    .await;

    let m = mock.lock().unwrap();
    assert_eq!(m.failure_calls, [suggestions]);
    assert!(m.ingest_calls.is_empty());
}

#[tokio::test]
async fn optimize_step_rejects_invalid_outcome_ids() {
    let cases: Vec<(Vec<Outcome>, &str)> = vec![
        (
            vec![
                out("a", &[("objective", json!(0.0))]),
                out("a", &[("objective", json!(1.0))]),
            ],
            "unique '_id'",
        ),
        (
            vec![
                out("a", &[("objective", json!(0.0))]),
                out("c", &[("objective", json!(1.0))]),
            ],
            "same IDs",
        ),
    ];
    for (outcomes, needle) in cases {
        let h = Harness::new();
        let mock = MockOptimizer::suggesting(vec![
            sug("a", &[("x1", json!(0.0))]),
            sug("b", &[("x1", json!(1.0))]),
        ]);
        let (eval, calls) = const_eval::<String>(outcomes);
        let problem = OptimizationProblem::new(
            as_shared(&mock),
            vec![actuator("x1", -1.0)],
            vec![sensor("objective")],
            eval,
        )
        .with_acquisition_plan(null_acquisition("test-uid-123".to_string()));

        let (plan, _) = optimize_step(&problem, 2, no_args());
        h.run_fails_with(plan, needle).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(mock.lock().unwrap().ingest_calls.is_empty());
    }
}

#[tokio::test]
async fn optimize_in_run_stops_early() {
    let h = Harness::new();
    let mock = Arc::new(Mutex::new(
        MockOptimizer {
            suggestions: vec![sug(0, &[("x1", json!(0.0))])],
            ..Default::default()
        }
        .stopping(vec![(false, None), (true, Some("converged"))]),
    ));
    let (eval, _) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(null_acquisition("test-uid-123".to_string()));

    h.run_ok(optimize_in_run(&problem, 5, StepOptions::default()))
        .await;

    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls.len(), 2);
    assert_eq!(m.ingest_calls.len(), 2);
    assert_eq!(m.stop_calls, 2);
}

#[tokio::test]
async fn optimize_in_run_checkpoints() {
    let h = Harness::new();
    let mock = Arc::new(Mutex::new(
        MockOptimizer {
            suggestions: vec![sug(0, &[("x1", json!(0.0))])],
            ..Default::default()
        }
        .checkpointable(),
    ));
    let (eval, _) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(null_acquisition("test-uid-123".to_string()));

    h.run_ok(optimize_in_run(
        &problem,
        3,
        StepOptions {
            checkpoint_interval: Some(2),
            ..Default::default()
        },
    ))
    .await;

    assert_eq!(mock.lock().unwrap().checkpoint_calls, 1);
}

// ---------------------------------------------------------------------------
//  optimize_step
// ---------------------------------------------------------------------------

#[tokio::test]
async fn optimize_step_default() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.0))])]);
    let (eval, calls) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    );

    let (plan, result) = optimize_step(&problem, 1, no_args());
    h.run_ok(plan).await;

    let step = result.await.unwrap();
    assert_eq!(step.suggestions, [sug(0, &[("x1", json!(0.0))])]);
    assert_eq!(step.outcomes, [out(0, &[("objective", json!(0.0))])]);
    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls, [1]);
    assert_eq!(m.ingest_calls, [vec![out(0, &[("objective", json!(0.0))])]]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn optimize_event_document_structure() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.5))])]);
    let (eval, _) = const_eval::<String>(vec![out(0, &[("objective", json!(1.25))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(null_acquisition("test-uid-123".to_string()));

    h.run_ok(optimize(&problem, Some(1), StepOptions::default()))
        .await;

    let events = h.run_events("optimize").await;
    assert_eq!(events.len(), 1);
    let data = &events[0].data;
    assert_eq!(data["acquisition_uid"], json!("test-uid-123"));
    assert_eq!(data["x1"], json!(0.5));
    assert_eq!(data["objective"], json!(1.25));
    assert_eq!(data["suggestion_ids"], json!("0"));
}

#[tokio::test]
async fn tuple_acquisition_identifier_is_passed_through() {
    let h = Harness::new();
    let suggestion = sug(0, &[("x1", json!(0.5))]);
    let mock = MockOptimizer::suggesting(vec![suggestion.clone()]);
    let seen: Captured<(Vec<String>, Vec<Suggestion>)> = captured();
    let s = seen.clone();
    let eval = EvaluationFunction::<Vec<String>>::sync("capture", move |uid, suggestions| {
        s.lock().unwrap().push((uid, suggestions));
        Ok(vec![out(0, &[("objective", json!(1.25))])])
    });
    let uid = vec!["event-a".to_string(), "event-b".to_string()];
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(null_acquisition(uid.clone()));

    h.run_ok(optimize(&problem, Some(1), StepOptions::default()))
        .await;

    assert_eq!(*seen.lock().unwrap(), vec![(uid, vec![suggestion])]);
    let events = h.run_events("optimize").await;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].data["acquisition_uid"],
        json!(["event-a", "event-b"])
    );
}

#[tokio::test]
async fn custom_uid_records_its_event_value() {
    let h = Harness::new();
    let suggestion = sug(0, &[("x1", json!(0.5))]);
    let mock = MockOptimizer::suggesting(vec![suggestion.clone()]);
    let uid = CustomUid {
        correlation_uid: "correlation-123".into(),
        item_uid: None,
    };
    let seen: Captured<(CustomUid, Vec<Suggestion>)> = captured();
    let s = seen.clone();
    let eval = EvaluationFunction::<CustomUid>::sync("capture", move |uid, suggestions| {
        s.lock().unwrap().push((uid, suggestions));
        Ok(vec![out(0, &[("objective", json!(1.25))])])
    });
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(null_acquisition(uid.clone()));

    h.run_ok(optimize(&problem, Some(1), StepOptions::default()))
        .await;

    assert_eq!(*seen.lock().unwrap(), vec![(uid.clone(), vec![suggestion])]);
    let events = h.run_events("optimize").await;
    assert_eq!(events[0].data["acquisition_uid"], json!(format!("{uid:?}")));
}

#[tokio::test]
async fn optimize_step_custom_acquisition_plan_receives_the_problem() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.0))])]);
    let (eval, calls) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let seen: Captured<(Vec<Suggestion>, Vec<String>, Vec<String>)> = captured();
    let s = seen.clone();
    let recording = AcquisitionPlan::new("recording", move |suggestions, actuators, sensors, _| {
        s.lock().unwrap().push((
            suggestions,
            actuators.iter().map(|a| a.name().to_owned()).collect(),
            sensors.iter().map(|d| d.name().to_owned()).collect(),
        ));
        let (tx, rx) = oneshot::channel();
        let plan = plan_box(async_stream::stream! {
            yield Msg::Null;
            let _ = tx.send("uid".to_string());
        });
        (plan, rx)
    });
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    )
    .with_acquisition_plan(recording);

    let (plan, _) = optimize_step(&problem, 1, no_args());
    h.run_ok(plan).await;

    assert_eq!(
        *seen.lock().unwrap(),
        vec![(
            vec![sug(0, &[("x1", json!(0.0))])],
            vec!["x1".to_string()],
            vec!["objective".to_string()],
        )]
    );
    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls, [1]);
    assert_eq!(m.ingest_calls, [vec![out(0, &[("objective", json!(0.0))])]]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
//  default_acquire
// ---------------------------------------------------------------------------

#[tokio::test]
async fn default_acquire_single_movable_readable() {
    let h = Harness::new();
    let m = CountingMotor::new("x1", -1.0);
    let readable = CountingSensor::new("objective", false);
    let (plan, uid) = default_acquire(
        vec![sug(0, &[("x1", json!(0.0))])],
        vec![Actuator::new(m.clone())],
        vec![readable.clone() as Sensor],
        no_args(),
    );

    let result = h.run_ok(plan).await;

    assert_eq!(readable.reads.load(Ordering::SeqCst), 1);
    assert_eq!(m.value(), 0.0);
    assert_eq!(uid.await.unwrap(), result.run_uids[0]);
}

#[tokio::test]
async fn default_acquire_merges_metadata() {
    let h = Harness::new();
    let suggestions = vec![sug(0, &[("x1", json!(0.0))])];
    let (plan, _) = default_acquire(
        suggestions.clone(),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        AcquireArgs {
            md: [(
                "blop_correlation_uid".to_string(),
                json!("test-correlation-uid"),
            )]
            .into(),
            per_step: None,
        },
    );

    h.run_ok(plan).await;

    let starts = h.start_docs().await;
    assert_eq!(starts.len(), 1);
    let md = &starts[0].extra;
    assert_eq!(md["blop_correlation_uid"], json!("test-correlation-uid"));
    assert_eq!(md["blop_suggestions"], json!([{"_id": 0, "x1": 0.0}]));
    assert_eq!(md["run_key"], json!("default_acquire"));
}

#[tokio::test]
async fn default_acquire_records_acquisition_order() {
    let h = Harness::new();
    let suggestions = vec![
        sug("far", &[("x1", json!(10.0))]),
        sug("near", &[("x1", json!(0.0))]),
    ];
    let (plan, _) = default_acquire(
        suggestions.clone(),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        no_args(),
    );

    h.run_ok(plan).await;

    let starts = h.start_docs().await;
    assert_eq!(starts.len(), 1);
    assert_eq!(
        starts[0].extra["blop_suggestions"],
        json!([suggestions[1].to_value(), suggestions[0].to_value()])
    );
    assert_eq!(
        starts[0].extra["blop_acquisition_order"],
        json!(["near", "far"])
    );
}

#[tokio::test]
async fn default_acquire_multiple_movables_readables() {
    let h = Harness::new();
    let m1 = CountingMotor::new("x1", -1.0);
    let m2 = CountingMotor::new("x2", -1.0);
    let r1 = CountingSensor::new("objective1", false);
    let r2 = CountingSensor::new("objective2", false);
    let (plan, _) = default_acquire(
        vec![
            sug(0, &[("x1", json!(0.0)), ("x2", json!(0.0))]),
            sug(1, &[("x1", json!(0.1)), ("x2", json!(0.1))]),
        ],
        vec![Actuator::new(m1.clone()), Actuator::new(m2.clone())],
        vec![r1.clone() as Sensor, r2.clone() as Sensor],
        no_args(),
    );

    h.run_ok(plan).await;

    assert_eq!(m1.sets(), [0.0, 0.1]);
    assert_eq!(m2.sets(), [0.0, 0.1]);
    assert_eq!(r1.reads.load(Ordering::SeqCst), 2);
    assert_eq!(r2.reads.load(Ordering::SeqCst), 2);
    assert_eq!(m1.value(), 0.1);
    assert_eq!(m2.value(), 0.1);
}

#[tokio::test]
async fn default_acquire_removes_checkpoints() {
    let h = Harness::new();
    let commands = h.record_commands();
    let (plan, _) = default_acquire(
        vec![sug(0, &[("x1", json!(0.0))]), sug(1, &[("x1", json!(0.1))])],
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        no_args(),
    );

    h.run_ok(plan).await;

    let seen = commands.lock().unwrap();
    assert!(!seen.iter().any(|c| c == "Checkpoint"), "{seen:?}");
    assert!(seen.iter().any(|c| c == "Set"), "{seen:?}");
}

// ---------------------------------------------------------------------------
//  acquire_baseline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acquire_baseline_with_parameterization() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![]);
    let (eval, calls) = const_eval::<String>(vec![out("baseline", &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", -1.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_ok(acquire_baseline(
        &problem,
        Some(params(&[("x1", json!(0.0))])),
        no_args(),
    ))
    .await;

    let m = mock.lock().unwrap();
    assert!(m.suggest_calls.is_empty());
    assert_eq!(
        m.ingest_calls,
        [vec![out(
            "baseline",
            &[("objective", json!(0.0)), ("x1", json!(0.0))]
        )]]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn acquire_baseline_from_current_position() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![]);
    let (eval, calls) = const_eval::<String>(vec![out("baseline", &[("objective", json!(0.0))])]);
    let m = CountingMotor::new("x1", -1.0);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![Actuator::new(m.clone())],
        vec![sensor("objective")],
        eval,
    );

    h.run_ok(acquire_baseline(&problem, None, no_args())).await;

    // Read once for the baseline, once during the acquisition; set once to
    // the current value.
    assert_eq!(m.reads.load(Ordering::SeqCst), 2);
    assert_eq!(m.sets(), [-1.0]);
    let mk = mock.lock().unwrap();
    assert!(mk.suggest_calls.is_empty());
    assert_eq!(
        mk.ingest_calls,
        [vec![out(
            "baseline",
            &[("objective", json!(0.0)), ("x1", json!(-1.0))]
        )]]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn acquire_baseline_from_current_needs_readable_actuators() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![]);
    let (eval, _) = const_eval::<String>(vec![]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![Actuator::move_only(motor("x1", 0.0))],
        vec![],
        eval,
    );

    h.run_fails_with(
        acquire_baseline(&problem, None, no_args()),
        "All actuators must also implement the Readable protocol",
    )
    .await;
}

// ---------------------------------------------------------------------------
//  stopping criteria
// ---------------------------------------------------------------------------

fn stopping_problem(
    script: Vec<(bool, Option<&str>)>,
) -> (Arc<Mutex<MockOptimizer>>, OptimizationProblem<String>) {
    let mock = Arc::new(Mutex::new(
        MockOptimizer {
            suggestions: vec![sug(0, &[("x1", json!(0.0))])],
            ..Default::default()
        }
        .stopping(script),
    ));
    let (eval, _) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", 0.0)],
        vec![sensor("objective")],
        eval,
    );
    (mock, problem)
}

#[tokio::test]
async fn optimize_without_iteration_limit_stops_at_criterion() {
    let h = Harness::new();
    let (mock, problem) = stopping_problem(vec![
        (false, None),
        (false, None),
        (true, Some("converged")),
    ]);

    h.run_ok(optimize(&problem, None, StepOptions::default()))
        .await;

    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls.len(), 3);
    assert_eq!(m.stop_calls, 3);
}

#[tokio::test]
async fn optimize_without_iteration_limit_requires_stopping_criteria() {
    let h = Harness::new();
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.0))])]);
    let (eval, _) = const_eval::<String>(vec![]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", 0.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_fails_with(
        optimize(&problem, None, StepOptions::default()),
        "iterations=None requires an optimizer that implements SupportsStoppingCriteria",
    )
    .await;

    assert!(mock.lock().unwrap().suggest_calls.is_empty());
}

#[tokio::test]
async fn optimize_stops_before_the_iteration_limit() {
    let h = Harness::new();
    let (mock, problem) = stopping_problem(vec![(false, None), (true, Some("converged"))]);

    h.run_ok(optimize(&problem, Some(5), StepOptions::default()))
        .await;

    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls.len(), 2);
    assert_eq!(m.stop_calls, 2);
}

#[tokio::test]
async fn optimize_iteration_limit_wins_when_criterion_is_not_met() {
    let h = Harness::new();
    let (mock, problem) = stopping_problem(vec![(false, None), (false, None), (false, None)]);

    h.run_ok(optimize(&problem, Some(2), StepOptions::default()))
        .await;

    let m = mock.lock().unwrap();
    assert_eq!(m.suggest_calls.len(), 2);
    assert_eq!(m.stop_calls, 2);
}

/// Stops when the objective changes by no more than `tolerance` between
/// consecutive ingests.
struct ToleranceStopOptimizer {
    tolerance: f64,
    last: Option<f64>,
    previous: Option<f64>,
}

impl Optimizer for ToleranceStopOptimizer {
    fn suggest(&mut self, _n: usize) -> Result<Vec<Suggestion>> {
        Ok(vec![sug(0, &[("x1", json!(0.0))])])
    }
    fn ingest(&mut self, outcomes: Vec<Outcome>) -> Result<()> {
        self.previous = self.last;
        self.last = outcomes[0].values["objective"].as_f64();
        Ok(())
    }
    fn best_points(&self) -> Result<Vec<BestPoint>> {
        Ok(vec![])
    }
    fn supports_stopping_criteria(&self) -> bool {
        true
    }
    fn should_stop(&self) -> (bool, Option<String>) {
        match (self.previous, self.last) {
            (Some(p), Some(l)) if (l - p).abs() <= self.tolerance => {
                (true, Some("objective change within tolerance".into()))
            }
            _ => (false, None),
        }
    }
}

#[tokio::test]
async fn optimize_stops_when_change_is_within_tolerance() {
    let h = Harness::new();
    let optimizer = shared(ToleranceStopOptimizer {
        tolerance: 0.1,
        last: None,
        previous: None,
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let eval = EvaluationFunction::<String>::sync("stepping", move |_, _| {
        let n = c.fetch_add(1, Ordering::SeqCst);
        let value = [0.5, 0.55].get(n).copied().expect("called at most twice");
        Ok(vec![out(0, &[("objective", json!(value))])])
    });
    let problem = OptimizationProblem::new(
        optimizer,
        vec![actuator("x1", 0.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_ok(optimize(&problem, Some(5), StepOptions::default()))
        .await;

    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn optimize_emits_a_checkpoint_per_iteration() {
    let h = Harness::new();
    let commands = h.record_commands();
    let mock = MockOptimizer::suggesting(vec![sug(0, &[("x1", json!(0.0))])]);
    let (eval, _) = const_eval::<String>(vec![out(0, &[("objective", json!(0.0))])]);
    let problem = OptimizationProblem::new(
        as_shared(&mock),
        vec![actuator("x1", 0.0)],
        vec![sensor("objective")],
        eval,
    );

    h.run_ok(optimize(&problem, Some(5), StepOptions::default()))
        .await;

    let n = commands
        .lock()
        .unwrap()
        .iter()
        .filter(|c| *c == "Checkpoint")
        .count();
    assert_eq!(n, 5);
}

// ---------------------------------------------------------------------------
//  navigate_to_best (test_plan_stubs.py)
// ---------------------------------------------------------------------------

fn best(id: i64, p: &[(&str, Value)], o: &[(&str, Value)]) -> BestPoint {
    BestPoint {
        id: TrialId::Int(id),
        params: params(p),
        outcomes: params(o),
    }
}

#[tokio::test]
async fn navigate_single_objective() {
    let h = Harness::new();
    let mock = Arc::new(Mutex::new(MockOptimizer {
        best: vec![best(
            123,
            &[("x1", json!(1.5)), ("x2", json!(2.5))],
            &[("objective", json!(10.0))],
        )],
        ..Default::default()
    }));
    let x1 = motor("x1", 0.0);
    let x2 = motor("x2", 0.0);

    h.run_ok(navigate_to_best(
        vec![Actuator::new(x1.clone()), Actuator::new(x2.clone())],
        Some(as_shared(&mock)),
        None,
    ))
    .await;

    assert_eq!(position(&x1).await, 1.5);
    assert_eq!(position(&x2).await, 2.5);
}

#[tokio::test]
async fn navigate_explicit_parameterization() {
    let h = Harness::new();
    let x1 = motor("x1", 0.0);

    h.run_ok(navigate_to_best(
        vec![Actuator::new(x1.clone())],
        None,
        Some(params(&[("x1", json!(3.0))])),
    ))
    .await;

    assert_eq!(position(&x1).await, 3.0);
}

#[tokio::test]
async fn navigate_fails_on_multiple_pareto_points() {
    let h = Harness::new();
    let mock = Arc::new(Mutex::new(MockOptimizer {
        best: vec![
            best(
                1,
                &[("x1", json!(1.0))],
                &[("obj_a", json!(5.0)), ("obj_b", json!(2.0))],
            ),
            best(
                5,
                &[("x1", json!(2.0))],
                &[("obj_a", json!(3.0)), ("obj_b", json!(4.0))],
            ),
            best(
                100,
                &[("x1", json!(3.0))],
                &[("obj_a", json!(1.0)), ("obj_b", json!(6.0))],
            ),
        ],
        ..Default::default()
    }));
    let x1 = motor("x1", 0.0);

    h.run_fails_with(
        navigate_to_best(
            vec![Actuator::new(x1.clone())],
            Some(as_shared(&mock)),
            None,
        ),
        "3 Pareto-optimal points",
    )
    .await;

    assert_eq!(position(&x1).await, 0.0);
}

#[tokio::test]
async fn navigate_fails_without_optimizer_or_parameterization() {
    let h = Harness::new();
    let x1 = motor("x1", 0.0);

    h.run_fails_with(
        navigate_to_best(vec![Actuator::new(x1)], None, None),
        "parameterization or use an optimizer",
    )
    .await;
}

#[tokio::test]
async fn navigate_ignores_unknown_params() {
    let h = Harness::new();
    let mock = Arc::new(Mutex::new(MockOptimizer {
        best: vec![best(
            1,
            &[("x1", json!(5.0)), ("unknown_param", json!(99.0))],
            &[("objective", json!(1.0))],
        )],
        ..Default::default()
    }));
    let x1 = motor("x1", 0.0);

    h.run_ok(navigate_to_best(
        vec![Actuator::new(x1.clone())],
        Some(as_shared(&mock)),
        None,
    ))
    .await;

    assert_eq!(position(&x1).await, 5.0);
}
