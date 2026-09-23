//! `bsrs::blop::bridge::BridgeOptimizer` against a fake REP server in a
//! thread, and — when `BSRS_BLOP_PYTHON` names an interpreter with pyzmq —
//! against the real `bsrs_blop_bridge` Python server's `random` backend.

#![cfg(feature = "blop-bridge")]

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::oneshot;

use bsrs::backends::soft::{SoftDetector, SoftMotor};
use bsrs::blop::bridge::{BridgeOptimizer, BridgeOptions};
use bsrs::blop::{
    AcquisitionPlan, AcquisitionUid, Actuator, Agent, Candidate, Dof, DofBinding,
    EvaluationFunction, ExperimentSpec, Objective, Optimizer, Outcome, ParameterConfig, Params,
    RangeDof, RangeType, Sensor, Suggestion, TrialId,
};
use bsrs::callbacks::CapturingSink;
use bsrs::core::plan::plan_box;
use bsrs::event_model::Document;
use bsrs::prelude::*;

// ---------------------------------------------------------------------------
//  fake server
// ---------------------------------------------------------------------------

/// A REP server in a thread that answers with a scripted optimizer: ids
/// count up, `best_points` is whatever was ingested, and every request is
/// recorded.
struct FakeServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    replies: Arc<AtomicUsize>,
    delay_next_suggest: Arc<AtomicBool>,
    /// What `should_stop` answers; `[true, "scripted"]` when set.
    say_stop: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl FakeServer {
    fn start(capabilities: Value) -> Self {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::REP).unwrap();
        socket.set_linger(0).unwrap();
        socket.set_rcvtimeo(20).unwrap();
        socket.bind("tcp://127.0.0.1:*").unwrap();
        let endpoint = socket.get_last_endpoint().unwrap().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let replies = Arc::new(AtomicUsize::new(0));
        let delay_next_suggest = Arc::new(AtomicBool::new(false));
        let say_stop = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let (req, rep, delay, say, stop2) = (
            requests.clone(),
            replies.clone(),
            delay_next_suggest.clone(),
            say_stop.clone(),
            stop.clone(),
        );
        let thread = std::thread::spawn(move || {
            let mut next_id = 0i64;
            let mut points: Vec<Value> = Vec::new();
            while !stop2.load(Ordering::SeqCst) {
                let bytes = match socket.recv_bytes(0) {
                    Ok(b) => b,
                    Err(zmq::Error::EAGAIN) => continue,
                    Err(e) => panic!("{e}"),
                };
                let request: Value = serde_json::from_slice(&bytes).unwrap();
                req.lock().unwrap().push(request.clone());
                let method = request["method"].as_str().unwrap();
                let params = &request["params"];
                let reply = match method {
                    "configure" => json!({ "result": {
                        "name": "FakeOptimizer", "capabilities": capabilities,
                    }}),
                    "suggest" => {
                        if delay.swap(false, Ordering::SeqCst) {
                            std::thread::sleep(Duration::from_millis(600));
                        }
                        let n = params["n"].as_u64().unwrap();
                        let items: Vec<Value> = (0..n)
                            .map(|_| {
                                let id = next_id;
                                next_id += 1;
                                json!({ "_id": id, "x1": 1.0 + id as f64, "x2": 2.5 })
                            })
                            .collect();
                        json!({ "result": items })
                    }
                    "register_suggestions" => {
                        let items: Vec<Value> = params["suggestions"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|s| {
                                let mut s = s.clone();
                                s["_id"] = json!(next_id);
                                next_id += 1;
                                s
                            })
                            .collect();
                        json!({ "result": items })
                    }
                    "ingest" => {
                        points.extend(params["points"].as_array().unwrap().iter().cloned());
                        json!({ "result": null })
                    }
                    "best_points" => json!({ "result": points.iter().map(|p| json!({
                        "_id": p["_id"],
                        "params": { "x1": 1.0 },
                        "outcomes": { "test_objective": p["test_objective"] },
                    })).collect::<Vec<_>>() }),
                    "should_stop" if say.load(Ordering::SeqCst) => {
                        json!({ "result": [true, "scripted"] })
                    }
                    "should_stop" => json!({ "result": [false, null] }),
                    "checkpoint" | "register_failures" => json!({ "result": null }),
                    "boom" => json!({ "error": "RuntimeError: boom" }),
                    other => json!({ "error": format!("unknown method {other:?}") }),
                };
                socket.send(serde_json::to_vec(&reply).unwrap(), 0).unwrap();
                rep.fetch_add(1, Ordering::SeqCst);
            }
        });
        Self {
            endpoint,
            requests,
            replies,
            delay_next_suggest,
            say_stop,
            stop,
            thread: Some(thread),
        }
    }

    fn requests(&self) -> Vec<Value> {
        self.requests.lock().unwrap().clone()
    }

    fn methods(&self) -> Vec<String> {
        self.requests()
            .iter()
            .map(|r| r["method"].as_str().unwrap().to_owned())
            .collect()
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            t.join().unwrap();
        }
    }
}

fn all_capabilities() -> Value {
    json!({ "register_suggestions": true, "stopping_criteria": true, "checkpoint": true })
}

fn spec() -> ExperimentSpec {
    ExperimentSpec {
        parameters: vec![
            ParameterConfig::Range {
                name: "x1".into(),
                bounds: (0.0, 10.0),
                parameter_type: RangeType::Float,
                step_size: None,
                scaling: None,
            },
            ParameterConfig::Range {
                name: "x2".into(),
                bounds: (0.0, 5.0),
                parameter_type: RangeType::Float,
                step_size: None,
                scaling: None,
            },
        ],
        objective: "test_objective".into(),
        parameter_constraints: vec![],
        outcome_constraints: vec![],
    }
}

fn options(timeout_ms: u64) -> BridgeOptions {
    BridgeOptions {
        backend: json!({ "seed": 1 }),
        timeout: Duration::from_millis(timeout_ms),
    }
}

fn out(id: impl Into<TrialId>, value: f64) -> Outcome {
    Outcome::new(id, [("test_objective".to_string(), json!(value))])
}

// ---------------------------------------------------------------------------
//  protocol
// ---------------------------------------------------------------------------

#[test]
fn connect_sends_the_spec_and_learns_name_and_capabilities() {
    let server = FakeServer::start(all_capabilities());
    let bridge = BridgeOptimizer::connect(&server.endpoint, &spec(), options(2000)).unwrap();

    assert_eq!(bridge.name(), "FakeOptimizer");
    assert!(bridge.supports_register_suggestions());
    assert!(bridge.supports_stopping_criteria());
    assert!(bridge.supports_checkpoint());
    assert_eq!(bridge.endpoint(), server.endpoint);

    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["method"], "configure");
    assert_eq!(requests[0]["params"]["backend"], json!({ "seed": 1 }));
    let sent = &requests[0]["params"]["spec"];
    assert_eq!(sent["objective"], "test_objective");
    assert_eq!(sent["parameters"][0]["kind"], "range");
    assert_eq!(sent["parameters"][0]["name"], "x1");
    assert_eq!(sent["parameters"][0]["bounds"], json!([0.0, 10.0]));
    assert_eq!(sent["parameters"][1]["bounds"], json!([0.0, 5.0]));
}

#[test]
fn missing_capabilities_default_to_unsupported() {
    let server = FakeServer::start(json!({ "checkpoint": true }));
    let bridge = BridgeOptimizer::connect(&server.endpoint, &spec(), options(2000)).unwrap();
    assert!(!bridge.supports_register_suggestions());
    assert!(!bridge.supports_stopping_criteria());
    assert!(bridge.supports_checkpoint());
}

#[test]
fn suggest_ingest_best_points_round_trip() {
    let server = FakeServer::start(all_capabilities());
    let mut bridge = BridgeOptimizer::connect(&server.endpoint, &spec(), options(2000)).unwrap();

    let suggestions = bridge.suggest(2).unwrap();
    assert_eq!(
        suggestions,
        [
            Suggestion::new(
                0,
                [
                    ("x1".to_string(), json!(1.0)),
                    ("x2".to_string(), json!(2.5))
                ]
            ),
            Suggestion::new(
                1,
                [
                    ("x1".to_string(), json!(2.0)),
                    ("x2".to_string(), json!(2.5))
                ]
            ),
        ]
    );

    bridge.ingest(vec![out(0, 3.0), out(1, 4.0)]).unwrap();
    let ingest = &server.requests()[2];
    assert_eq!(ingest["method"], "ingest");
    assert_eq!(
        ingest["params"]["points"],
        json!([
            { "_id": 0, "test_objective": 3.0 },
            { "_id": 1, "test_objective": 4.0 },
        ])
    );

    let best = bridge.best_points().unwrap();
    assert_eq!(best.len(), 2);
    assert_eq!(best[1].id, TrialId::Int(1));
    assert_eq!(
        best[1].params,
        Params::from([("x1".to_string(), json!(1.0))])
    );
    assert_eq!(
        best[1].outcomes,
        Params::from([("test_objective".to_string(), json!(4.0))])
    );
}

#[test]
fn optional_protocols_go_over_the_wire() {
    let server = FakeServer::start(all_capabilities());
    let mut bridge = BridgeOptimizer::connect(&server.endpoint, &spec(), options(2000)).unwrap();

    let registered = bridge
        .register_suggestions(vec![Candidate {
            params: Params::from([
                ("x1".to_string(), json!(9.0)),
                ("x2".to_string(), json!(1.0)),
            ]),
        }])
        .unwrap();
    assert_eq!(
        registered,
        [Suggestion::new(
            0,
            [
                ("x1".to_string(), json!(9.0)),
                ("x2".to_string(), json!(1.0))
            ]
        )]
    );

    bridge.register_failures(&registered);
    assert_eq!(bridge.should_stop(), (false, None));
    server.say_stop.store(true, Ordering::SeqCst);
    assert_eq!(bridge.should_stop(), (true, Some("scripted".into())));
    bridge.checkpoint().unwrap();

    assert_eq!(
        server.methods(),
        [
            "configure",
            "register_suggestions",
            "register_failures",
            "should_stop",
            "should_stop",
            "checkpoint"
        ]
    );
    let failures = &server.requests()[2]["params"]["suggestions"];
    assert_eq!(failures, &json!([{ "_id": 0, "x1": 9.0, "x2": 1.0 }]));
}

#[test]
fn string_ids_round_trip() {
    let server = FakeServer::start(all_capabilities());
    let bridge = BridgeOptimizer::connect(&server.endpoint, &spec(), options(2000)).unwrap();
    // `register_suggestions` echoes what it was given plus an id, so a
    // string id in the echo is the server's choice of id type.
    let reply = bridge
        .call(
            "register_suggestions",
            json!({ "suggestions": [{ "x1": 1.0 }] }),
        )
        .unwrap();
    assert_eq!(reply[0]["_id"], 0);

    let mut bridge = bridge;
    bridge.ingest(vec![out("trial-a", 1.0)]).unwrap();
    assert_eq!(
        server.requests()[2]["params"]["points"][0]["_id"],
        "trial-a"
    );
    let best = bridge.best_points().unwrap();
    assert_eq!(best[0].id, TrialId::Str("trial-a".into()));
}

#[test]
fn error_reply_fails_the_call_and_the_socket_stays_usable() {
    let server = FakeServer::start(all_capabilities());
    let mut bridge = BridgeOptimizer::connect(&server.endpoint, &spec(), options(2000)).unwrap();

    let err = bridge.call("boom", json!({})).unwrap_err();
    assert!(
        matches!(&err, BsrsError::Backend(m) if m.contains("boom") && m.contains("RuntimeError")),
        "{err}"
    );
    assert_eq!(bridge.suggest(1).unwrap().len(), 1);
}

#[test]
fn no_reply_times_out_and_the_next_call_recovers() {
    let server = FakeServer::start(all_capabilities());
    let mut bridge = BridgeOptimizer::connect(&server.endpoint, &spec(), options(200)).unwrap();

    server.delay_next_suggest.store(true, Ordering::SeqCst);
    let err = bridge.suggest(1).unwrap_err();
    assert!(
        matches!(&err, BsrsError::Backend(m) if m.contains("no reply within")),
        "{err}"
    );

    // Let the stale reply reach the client; REQ_CORRELATE must drop it.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while server.replies.load(Ordering::SeqCst) < 2 {
        assert!(std::time::Instant::now() < deadline, "server never replied");
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(50));

    let fresh = bridge.suggest(1).unwrap();
    assert_eq!(fresh[0].id, TrialId::Int(1), "got the stale reply back");
}

#[test]
fn connect_fails_when_nothing_listens() {
    let err = BridgeOptimizer::connect("tcp://127.0.0.1:1", &spec(), options(200)).unwrap_err();
    assert!(
        matches!(&err, BsrsError::Backend(m) if m.contains("configure") && m.contains("no reply")),
        "{err}"
    );
}

// ---------------------------------------------------------------------------
//  through the agent, on the engine
// ---------------------------------------------------------------------------

fn movable(name: &str) -> Actuator {
    Actuator::new(Arc::new(SoftMotor::new(name, Some(0.0))))
}

fn range(binding: impl Into<DofBinding>) -> Dof {
    RangeDof::new(binding, (0.0, 10.0), RangeType::Float).into()
}

fn mock_eval<U: AcquisitionUid>() -> EvaluationFunction<U> {
    EvaluationFunction::sync("mock_evaluation_function", |_uid: U, suggestions| {
        Ok(suggestions
            .iter()
            .map(|s| out(s.id.clone(), s.params["x1"].as_f64().unwrap() * 2.0))
            .collect())
    })
}

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

async fn optimize_over(endpoint: &str, options: BridgeOptions) -> (Vec<String>, usize) {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let agent = Agent::builder(
        [range(movable("x1")), range(movable("x2"))],
        Objective::maximize("test_objective"),
        mock_eval::<String>(),
    )
    .sensors([SoftDetector::new("det") as Sensor])
    .acquisition_plan(mock_acquisition())
    .build(BridgeOptimizer::factory(endpoint, options))
    .unwrap();

    let result = re.run_async(agent.optimize(Some(3), 2)).await.unwrap();
    assert_eq!(result.exit_status, "success", "{}", result.reason);
    let events = sink
        .snapshot()
        .await
        .iter()
        .filter(|d| matches!(d, Document::Event(_)))
        .count();
    let best = agent.best_points().unwrap();
    (best.iter().map(|b| b.id.to_string()).collect(), events)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_optimizes_over_the_bridge_on_a_multi_thread_runtime() {
    let server = FakeServer::start(all_capabilities());
    let (best, events) = optimize_over(&server.endpoint, options(5000)).await;
    assert_eq!(events, 3);
    assert_eq!(best, ["0", "1", "2", "3", "4", "5"]);
    let methods = server.methods();
    assert_eq!(&methods[..3], ["configure", "suggest", "ingest"]);
    assert_eq!(methods.iter().filter(|m| *m == "suggest").count(), 3);
    assert_eq!(methods.iter().filter(|m| *m == "ingest").count(), 3);
}

#[tokio::test]
async fn agent_optimizes_over_the_bridge_on_a_current_thread_runtime() {
    let server = FakeServer::start(all_capabilities());
    let (best, events) = optimize_over(&server.endpoint, options(5000)).await;
    assert_eq!(events, 3);
    assert_eq!(best.len(), 6);
}

// ---------------------------------------------------------------------------
//  the real Python server (opt-in)
// ---------------------------------------------------------------------------

/// `BSRS_BLOP_PYTHON=/path/to/python cargo nextest run --features blop-bridge`
/// runs `python -m bsrs_blop_bridge --backend <backend>` from `python/`
/// and returns the child and its resolved endpoint; `None` when the
/// variable is unset.
fn spawn_python_server(backend: &str) -> Option<(PythonServer, String)> {
    let Ok(python) = std::env::var("BSRS_BLOP_PYTHON") else {
        eprintln!("BSRS_BLOP_PYTHON unset; skipping the Python end-to-end test");
        return None;
    };
    let python_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../python");
    let mut child = Command::new(python)
        .args([
            "-m",
            "bsrs_blop_bridge",
            "--endpoint",
            "tcp://127.0.0.1:*",
            "--backend",
            backend,
        ])
        .current_dir(&python_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the bridge server");
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let endpoint = line
        .strip_prefix("endpoint ")
        .unwrap_or_else(|| panic!("unexpected first line {line:?}"))
        .trim()
        .to_owned();
    Some((PythonServer(child), endpoint))
}

/// Kills the server on every exit path, so a failed assertion leaves no
/// interpreter behind.
struct PythonServer(std::process::Child);

impl Drop for PythonServer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_random_backend_end_to_end() {
    let Some((_server, endpoint)) = spawn_python_server("random") else {
        return;
    };
    let (best, events) = optimize_over(&endpoint, options(5000)).await;
    assert_eq!(events, 3);
    // The random backend keeps one best point: the largest x1 seen.
    assert_eq!(best.len(), 1);
}

/// Needs `blop[ax]` in the interpreter; skipped (with a note) when the
/// server fails to configure, as that is the `ImportError` of a missing Ax.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_ax_backend_end_to_end() {
    let Some((_server, endpoint)) = spawn_python_server("ax") else {
        return;
    };
    // Ax takes `backend` as `configure_experiment` keywords, so no seed.
    let ax_options = || BridgeOptions {
        backend: json!({}),
        timeout: Duration::from_secs(120),
    };
    let probe = BridgeOptimizer::connect(&endpoint, &spec(), ax_options());
    if let Err(e) = &probe {
        if e.to_string().contains("ImportError") {
            eprintln!("ax not installed in BSRS_BLOP_PYTHON; skipping: {e}");
            return;
        }
    }
    let probe = probe.unwrap();
    assert_eq!(probe.name(), "AxOptimizer");
    assert!(probe.supports_register_suggestions());
    assert!(probe.supports_stopping_criteria());
    assert!(probe.supports_checkpoint());
    drop(probe);

    let (best, events) = optimize_over(&endpoint, ax_options()).await;
    assert_eq!(events, 3);
    assert_eq!(best.len(), 1);
}
