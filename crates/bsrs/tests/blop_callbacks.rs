//! `bsrs::blop::callbacks` — ports of blop's `tests/callbacks/test_router.py`
//! and `test_logger.py`. The logger's console mock becomes a shared byte
//! buffer the tests read back.

#![cfg(feature = "blop")]

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use bsrs::backends::soft::{SoftDetector, SoftMotor};
use bsrs::blop::{
    shared, Actuator, BestPoint, EvaluationFunction, OptimizationLogger, OptimizationProblem,
    OptimizationRouter, Optimizer, Outcome, Sensor, Suggestion, OPTIMIZE_RUN_KEY,
    SAMPLE_SUGGESTIONS_RUN_KEY,
};
use bsrs::callbacks::CapturingSink;
use bsrs::event_model::{
    DataKey, Document, Dtype, Event, EventDescriptor, ExitStatus, RunStart, RunStop,
};
use bsrs::plans::preprocessors::SupplementalData;
use bsrs::prelude::*;

// ---------------------------------------------------------------------------
//  document builders
// ---------------------------------------------------------------------------

fn start(extra: Value) -> Document {
    let extra: HashMap<String, Value> = serde_json::from_value(extra).unwrap();
    Document::Start(RunStart {
        uid: "start-001".into(),
        time: 0.0,
        extra,
        ..Default::default()
    })
}

fn data_key(source: &str, dtype: Dtype) -> DataKey {
    DataKey {
        source: source.into(),
        dtype,
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

fn descriptor_with(keys: &[(&str, &str, Dtype)]) -> Document {
    Document::Descriptor(EventDescriptor {
        uid: "desc-001".into(),
        run_start: "start-001".into(),
        time: 0.0,
        data_keys: keys
            .iter()
            .map(|(k, s, d)| (k.to_string(), data_key(s, *d)))
            .collect(),
        configuration: HashMap::new(),
        name: Some("primary".into()),
        hints: None,
        object_keys: HashMap::new(),
    })
}

fn descriptor() -> Document {
    descriptor_with(&[
        ("x", "optimization-parameter", Dtype::Number),
        ("y", "optimization-outcome", Dtype::Number),
    ])
}

fn event(data: Value) -> Document {
    let data: HashMap<String, Value> = serde_json::from_value(data).unwrap();
    Document::Event(Event {
        uid: "event-001".into(),
        descriptor: "desc-001".into(),
        time: 0.0,
        seq_num: 1,
        timestamps: data.keys().map(|k| (k.clone(), 0.0)).collect(),
        data,
        filled: HashMap::new(),
    })
}

fn stop(exit_status: ExitStatus, reason: &str) -> Document {
    Document::Stop(RunStop {
        uid: "stop-001".into(),
        run_start: "start-001".into(),
        time: 0.0,
        exit_status,
        reason: (!reason.is_empty()).then(|| reason.to_owned()),
        ..Default::default()
    })
}

// ---------------------------------------------------------------------------
//  router
// ---------------------------------------------------------------------------

async fn feed(sink: &dyn DocumentSink, docs: &[Document]) {
    for doc in docs {
        sink.dispatch(doc).await.unwrap();
    }
}

fn starts(sink: &CapturingSink) -> usize {
    sink.docs
        .try_lock()
        .unwrap()
        .iter()
        .filter(|d| matches!(d, Document::Start(_)))
        .count()
}

#[tokio::test]
async fn routes_matching_run_keys() {
    for run_key in [OPTIMIZE_RUN_KEY, SAMPLE_SUGGESTIONS_RUN_KEY] {
        let cb = Arc::new(CapturingSink::new());
        let router = OptimizationRouter::with_sinks([cb.clone() as Arc<dyn DocumentSink>]);
        feed(&router, &[start(json!({ "run_key": run_key }))]).await;
        assert_eq!(starts(&cb), 1, "{run_key}");
    }
}

#[tokio::test]
async fn ignores_non_matching_and_missing_run_key() {
    let cb = Arc::new(CapturingSink::new());
    let router = OptimizationRouter::with_sinks([cb.clone() as Arc<dyn DocumentSink>]);
    feed(&router, &[start(json!({ "run_key": "some_other_run" }))]).await;
    feed(&router, &[start(json!({}))]).await;
    assert_eq!(starts(&cb), 0);
}

#[tokio::test]
async fn mutating_list_affects_next_run() {
    let (cb1, cb2) = (
        Arc::new(CapturingSink::new()),
        Arc::new(CapturingSink::new()),
    );
    let router = OptimizationRouter::with_sinks([cb1.clone() as Arc<dyn DocumentSink>]);
    feed(&router, &[start(json!({ "run_key": OPTIMIZE_RUN_KEY }))]).await;
    assert_eq!((starts(&cb1), starts(&cb2)), (1, 0));

    router.sinks().lock().unwrap().push(cb2.clone());
    feed(&router, &[start(json!({ "run_key": OPTIMIZE_RUN_KEY }))]).await;
    assert_eq!((starts(&cb1), starts(&cb2)), (2, 1));
}

#[tokio::test]
async fn empty_sink_list() {
    let router = OptimizationRouter::with_sinks([]);
    feed(&router, &[start(json!({ "run_key": OPTIMIZE_RUN_KEY }))]).await;
}

/// The optimizer of blop's `optimization_problem` fixture: one fixed point.
struct OnePoint;

impl Optimizer for OnePoint {
    fn suggest(&mut self, _n: usize) -> Result<Vec<Suggestion>> {
        Ok(vec![Suggestion::new(0, [("x1".to_string(), json!(0.0))])])
    }
    fn ingest(&mut self, _outcomes: Vec<Outcome>) -> Result<()> {
        Ok(())
    }
    fn best_points(&self) -> Result<Vec<BestPoint>> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn baseline_stream_is_filtered_out() {
    let problem = OptimizationProblem::new(
        shared(OnePoint),
        vec![Actuator::new(Arc::new(SoftMotor::new("x1", Some(-1.0))))],
        vec![SoftDetector::new("objective") as Sensor],
        EvaluationFunction::<String>::sync("eval", |_uid, suggestions| {
            Ok(suggestions
                .iter()
                .map(|s| Outcome::new(s.id.clone(), [("objective".to_string(), json!(0.0))]))
                .collect())
        }),
    );
    let cb = Arc::new(CapturingSink::new());
    let router = Arc::new(OptimizationRouter::with_sinks([
        cb.clone() as Arc<dyn DocumentSink>
    ]));
    let re = RunEngine::new(vec![router as Arc<dyn DocumentSink>]);
    let sd = Arc::new(SupplementalData::new());
    sd.add_baseline(SoftDetector::new("temperature"));
    re.add_preprocessor(sd.as_preprocessor());

    let result = re
        .run_async(bsrs::blop::optimize(&problem, Some(1), Default::default()))
        .await
        .unwrap();
    assert_eq!(result.exit_status, "success", "{}", result.reason);

    let received = cb.snapshot().await;
    let stream_names: Vec<String> = received
        .iter()
        .filter_map(|d| match d {
            Document::Descriptor(desc) => desc.name.clone(),
            _ => None,
        })
        .collect();
    assert_eq!(stream_names, ["primary"]);
    let primary_uids: Vec<&str> = received
        .iter()
        .filter_map(|d| match d {
            Document::Descriptor(desc) => Some(desc.uid.as_str()),
            _ => None,
        })
        .collect();
    let event_descriptors: Vec<&str> = received
        .iter()
        .filter_map(|d| match d {
            Document::Event(e) => Some(e.descriptor.as_str()),
            _ => None,
        })
        .collect();
    assert!(!event_descriptors.is_empty());
    assert!(event_descriptors.iter().all(|u| primary_uids.contains(u)));
    let stops = received
        .iter()
        .filter(|d| matches!(d, Document::Stop(_)))
        .count();
    assert_eq!(stops, 1);
    let starts = received
        .iter()
        .filter(|d| matches!(d, Document::Start(_)))
        .count();
    assert_eq!(
        starts, 1,
        "the inner default_acquire run must not be routed"
    );
}

// ---------------------------------------------------------------------------
//  logger
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Buffer {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
    fn count(&self, needle: &str) -> usize {
        self.text().matches(needle).count()
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn logger() -> (OptimizationLogger, Buffer) {
    let out = Buffer::default();
    (OptimizationLogger::new(out.clone()), out)
}

#[tokio::test]
async fn start_minimal() {
    let (logger, out) = logger();
    feed(&logger, &[start(json!({}))]).await;
    let text = out.text();
    assert!(text.contains("== Optimization =="), "{text}");
    assert!(text.contains("Optimizer   Unknown"), "{text}");
    assert!(text.contains("Actuators   N/A"), "{text}");
    assert!(
        text.contains("Iterations  Until stopping criterion\n"),
        "{text}"
    );
    assert!(text.contains("Run UID     start-001"), "{text}");
}

#[tokio::test]
async fn start_with_full_metadata() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[start(json!({
            "iterations": 10, "n_points": 3, "optimizer": "BoTorch",
            "actuators": ["mirror_x", "mirror_y"], "sensors": ["detector"],
        }))],
    )
    .await;
    let text = out.text();
    assert!(text.contains("Optimizer   BoTorch"), "{text}");
    assert!(text.contains("Actuators   mirror_x, mirror_y"), "{text}");
    assert!(text.contains("Sensors     detector"), "{text}");
    assert!(text.contains("Iterations  10  Points/iter 3"), "{text}");
}

#[tokio::test]
async fn start_continuation_accumulates_iterations() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[
            start(json!({ "iterations": 5 })),
            descriptor(),
            event(json!({ "x": 1.0, "y": 2.0 })),
            event(json!({ "x": 1.0, "y": 2.0 })),
            start(json!({ "iterations": 3 })),
            event(json!({ "x": 1.0, "y": 2.0 })),
        ],
    )
    .await;
    let text = out.text();
    assert!(
        text.contains("Iterations  3 more (2 completed, 5 total)"),
        "{text}"
    );
    assert!(text.contains("Iteration 3 / 5"), "{text}");
}

#[tokio::test]
async fn descriptor_groups_keys_by_source() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[
            descriptor_with(&[
                ("x2", "optimization-parameter", Dtype::Number),
                ("x1", "optimization-parameter", Dtype::Number),
                ("y", "optimization-outcome", Dtype::Number),
                ("other", "optimization-other", Dtype::String),
            ]),
            event(json!({ "x1": 1.0, "x2": 2.0, "y": 3.0, "other": "o" })),
        ],
    )
    .await;
    let text = out.text();
    assert!(text.contains("x1  x2  y\n"), "{text}");
    assert!(!text.contains("other"), "{text}");
}

#[tokio::test]
async fn event_empty_data_prints_nothing() {
    let (logger, out) = logger();
    feed(&logger, &[descriptor(), event(json!({}))]).await;
    assert_eq!(out.text(), "");
}

#[tokio::test]
async fn event_scalar_data() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[descriptor(), event(json!({ "x": 1.5, "y": 3.25 }))],
    )
    .await;
    let text = out.text();
    assert_eq!(out.count("---- Iteration"), 1);
    assert!(text.contains("Event  Suggestion ID    x     y\n"), "{text}");
    assert!(text.contains("    0                 1.5  3.25\n"), "{text}");
    assert!(
        text.contains("  y  min: 3.25  max: 3.25  mean: 3.25\n"),
        "{text}"
    );
    assert!(text.contains("  (1 pts sampled)\n"), "{text}");
}

#[tokio::test]
async fn event_without_iteration_limit_omits_total() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[
            start(json!({ "iterations": null })),
            descriptor(),
            event(json!({ "x": 1.5, "y": 3.25 })),
        ],
    )
    .await;
    assert!(
        out.text().contains("---- Iteration 1 ----\n"),
        "{}",
        out.text()
    );
}

#[tokio::test]
async fn event_batch_data() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[
            descriptor(),
            event(json!({
                "x": [1.0, 2.0, 3.0], "y": [10.0, 20.0, 30.0],
                "suggestion_ids": ["s1", "s2", "s3"], "acquisition_uid": "run-1",
            })),
        ],
    )
    .await;
    let text = out.text();
    assert!(
        text.contains("---- Iteration 1  (3 points) ----\n"),
        "{text}"
    );
    assert!(text.contains("  Acquire UID  run-1\n"), "{text}");
    assert!(text.contains("    2             s3  3  30\n"), "{text}");
    assert!(text.contains("  y  min: 10  max: 30  mean: 20\n"), "{text}");
    assert!(text.contains("  (3 pts sampled)\n"), "{text}");
}

#[tokio::test]
async fn event_batch_with_null_padding() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[
            descriptor(),
            event(json!({
                "x": [1.0, 2.0, null], "y": [10.0, 20.0, null],
                "suggestion_ids": ["s1", "s2", ""],
            })),
        ],
    )
    .await;
    let text = out.text();
    assert!(text.contains("(2 points)"), "{text}");
    assert!(!text.contains("nan"), "{text}");
    assert!(text.contains("  (2 pts sampled)\n"), "{text}");
}

#[tokio::test]
async fn event_multiple_iterations() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[
            descriptor(),
            event(json!({ "x": 1.0, "y": 10.0 })),
            event(json!({ "x": 3.0, "y": 20.0 })),
        ],
    )
    .await;
    let text = out.text();
    assert_eq!(out.count("---- Iteration"), 2);
    assert!(text.contains("  y  min: 10  max: 20  mean: 15\n"), "{text}");
}

#[tokio::test]
async fn event_non_numeric_data() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[
            descriptor_with(&[
                ("x", "optimization-parameter", Dtype::Number),
                ("label", "optimization-outcome", Dtype::String),
            ]),
            event(json!({ "x": 1.0, "label": "good" })),
        ],
    )
    .await;
    let text = out.text();
    assert!(text.contains("good"), "{text}");
    assert!(!text.contains("min:"), "{text}");
}

async fn run_to_stop(exit_status: ExitStatus, reason: &str) -> Buffer {
    let (logger, out) = logger();
    feed(
        &logger,
        &[
            start(json!({ "iterations": 1 })),
            descriptor(),
            event(json!({ "x": 1.0, "y": 5.0 })),
            stop(exit_status, reason),
        ],
    )
    .await;
    out
}

#[tokio::test]
async fn stop_success() {
    let out = run_to_stop(ExitStatus::Success, "").await;
    let text = out.text();
    assert!(text.contains("Summary Statistics\n"), "{text}");
    assert!(
        text.contains("Name     Type  Min  Max  Mean  Std  Count\n"),
        "{text}"
    );
    assert!(
        text.contains("   x    param    1    1     1   --      1\n"),
        "{text}"
    );
    assert!(
        text.contains("   y  outcome    5    5     5   --      1\n"),
        "{text}"
    );
    assert!(text.contains("---- Optimization Complete ----\n"), "{text}");
}

#[tokio::test]
async fn stop_abort_with_reason() {
    let out = run_to_stop(ExitStatus::Abort, "user interrupt").await;
    assert!(
        out.text()
            .contains("---- Optimization Aborted  (user interrupt) ----\n"),
        "{}",
        out.text()
    );
}

#[tokio::test]
async fn stop_other_status() {
    let out = run_to_stop(ExitStatus::Fail, "hardware fault").await;
    assert!(
        out.text()
            .contains("---- Optimization Stopped  (fail)  hardware fault ----\n"),
        "{}",
        out.text()
    );
}

#[tokio::test]
async fn stop_without_events() {
    let (logger, out) = logger();
    feed(
        &logger,
        &[
            start(json!({})),
            descriptor(),
            stop(ExitStatus::Success, ""),
        ],
    )
    .await;
    let text = out.text();
    assert!(!text.contains("Summary Statistics"), "{text}");
    assert!(text.contains("---- Optimization Complete ----\n"), "{text}");
}

#[tokio::test]
async fn full_lifecycle() {
    let (logger, out) = logger();
    let mut docs = vec![
        start(json!({
            "iterations": 3, "n_points": 2, "optimizer": "BoTorch",
            "actuators": ["x"], "sensors": ["y"],
        })),
        descriptor(),
    ];
    for i in 0..3 {
        let f = i as f64;
        docs.push(event(json!({
            "x": [f, f + 0.5], "y": [f * 10.0, f * 10.0 + 5.0],
            "suggestion_ids": [format!("s{}", 2 * i), format!("s{}", 2 * i + 1)],
        })));
    }
    docs.push(stop(ExitStatus::Success, ""));
    feed(&logger, &docs).await;
    let text = out.text();
    assert_eq!(out.count("---- Iteration"), 3);
    assert!(text.contains("Iteration 3 / 3  (2 points)"), "{text}");
    assert!(text.contains("  (6 pts sampled)\n"), "{text}");
    assert!(
        text.contains("     y  outcome    0   25  12.5   9.35414      6\n"),
        "{text}"
    );
}

/// The logger behind the router on the engine: the header shows the
/// problem's metadata and the events reach the table.
#[tokio::test]
async fn logger_behind_router_on_the_engine() {
    let problem = OptimizationProblem::new(
        shared(OnePoint),
        vec![Actuator::new(Arc::new(SoftMotor::new("x1", Some(-1.0))))],
        vec![SoftDetector::new("objective") as Sensor],
        EvaluationFunction::<String>::sync("eval", |_uid, suggestions| {
            Ok(suggestions
                .iter()
                .map(|s| Outcome::new(s.id.clone(), [("objective".to_string(), json!(0.25))]))
                .collect())
        }),
    );
    let (logger, out) = logger();
    let router = OptimizationRouter::with_sinks([Arc::new(logger) as Arc<dyn DocumentSink>]);
    let re = RunEngine::new(vec![Arc::new(router) as Arc<dyn DocumentSink>]);
    let result = re
        .run_async(bsrs::blop::optimize(&problem, Some(2), Default::default()))
        .await
        .unwrap();
    assert_eq!(result.exit_status, "success", "{}", result.reason);
    let text = out.text();
    assert!(text.contains("Actuators   x1\n"), "{text}");
    assert!(text.contains("Sensors     objective\n"), "{text}");
    assert!(text.contains("Iterations  2\n"), "{text}");
    assert_eq!(out.count("---- Iteration"), 2);
    assert!(text.contains("Iteration 2 / 2"), "{text}");
    assert!(
        text.contains("  objective  min: 0.25  max: 0.25  mean: 0.25\n"),
        "{text}"
    );
    assert!(text.contains("---- Optimization Complete ----\n"), "{text}");
}
