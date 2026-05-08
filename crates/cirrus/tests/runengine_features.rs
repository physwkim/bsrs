//! Tests for the bluesky-parity RunEngine features:
//!   - state() reflects pause/abort/halt
//!   - md persistent metadata appears in RunStart
//!   - scan_id auto-increments across runs
//!   - md_validator rejects bad metadata
//!   - before_plan / after_plan hooks fire
//!   - subscribe / unsubscribe sees Documents
//!   - register_command + Msg::Custom dispatch
//!   - Msg::Publish goes through broadcast
//!   - loop_timeout aborts overrun plans

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use cirrus::backends::soft::SoftDetector;
use cirrus::callbacks::CapturingSink;
use cirrus::prelude::*;
use cirrus_core::msg::Msg;
use cirrus_core::plan::{plan_box, Plan};
use cirrus_engine::EngineRunState;
use cirrus_event_model::Document;
use serde_json::Value;

fn one_count_plan() -> Plan {
    let det = SoftDetector::new("det1");
    cirrus::ophyd_async::count(vec![det], 1)
}

#[tokio::test]
async fn state_idle_after_construction() {
    let re = RunEngine::new(vec![]);
    assert_eq!(re.state(), EngineRunState::Idle);
}

#[tokio::test]
async fn md_persistent_appears_in_runstart() {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    re.md_set("operator", Value::String("alice".into()));
    re.md_set("beamline", Value::String("BL-7".into()));

    re.run_async(one_count_plan()).await.unwrap();

    let docs = sink.snapshot().await;
    let start = match &docs[0] {
        Document::Start(s) => s,
        _ => panic!("first doc is not Start"),
    };
    assert_eq!(
        start.extra.get("operator"),
        Some(&Value::String("alice".into()))
    );
    assert_eq!(
        start.extra.get("beamline"),
        Some(&Value::String("BL-7".into()))
    );
}

#[tokio::test]
async fn scan_id_auto_increments() {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);

    re.run_async(one_count_plan()).await.unwrap();
    re.run_async(one_count_plan()).await.unwrap();
    re.run_async(one_count_plan()).await.unwrap();

    let docs = sink.snapshot().await;
    let starts: Vec<u64> = docs
        .iter()
        .filter_map(|d| match d {
            Document::Start(s) => s.scan_id,
            _ => None,
        })
        .collect();
    assert_eq!(starts.len(), 3);
    // Strictly monotonic.
    assert!(starts[0] < starts[1]);
    assert!(starts[1] < starts[2]);
}

#[tokio::test]
async fn md_validator_rejects_run() {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    re.set_md_validator(Some(Arc::new(|md| {
        if md.contains_key("forbidden") {
            Err(cirrus_core::error::CirrusError::Plan(
                "forbidden key".into(),
            ))
        } else {
            Ok(())
        }
    })));
    re.md_set("forbidden", Value::Bool(true));

    let result = re.run_async(one_count_plan()).await.unwrap();
    assert_eq!(
        result.exit_status, "fail",
        "validator failure should mark run failed"
    );
}

#[tokio::test]
async fn before_after_plan_hooks_fire() {
    let counter = Arc::new(AtomicU64::new(0));
    let cb = counter.clone();
    let ca = counter.clone();
    let re = RunEngine::new(vec![]);
    re.set_before_plan(Some(Arc::new(move || {
        cb.fetch_add(1, Ordering::SeqCst);
    })));
    re.set_after_plan(Some(Arc::new(move || {
        ca.fetch_add(10, Ordering::SeqCst);
    })));

    re.run_async(one_count_plan()).await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 11);
}

#[tokio::test]
async fn subscribe_receives_all_documents() {
    let received = Arc::new(StdMutex::new(Vec::<String>::new()));
    let r = received.clone();
    let re = RunEngine::new(vec![]);
    let id = re.subscribe(Arc::new(move |d: &Document| {
        let kind = match d {
            Document::Start(_) => "start",
            Document::Descriptor(_) => "descriptor",
            Document::Event(_) => "event",
            Document::Stop(_) => "stop",
            _ => "other",
        };
        r.lock().unwrap().push(kind.into());
    }));

    re.run_async(one_count_plan()).await.unwrap();
    re.unsubscribe(id);

    let kinds = received.lock().unwrap().clone();
    assert_eq!(kinds.first().map(String::as_str), Some("start"));
    assert!(kinds.iter().any(|s| s == "descriptor"));
    assert!(kinds.iter().any(|s| s == "event"));
    assert_eq!(kinds.last().map(String::as_str), Some("stop"));
}

#[tokio::test]
async fn unsubscribe_stops_receiving() {
    let received = Arc::new(AtomicU64::new(0));
    let r = received.clone();
    let re = RunEngine::new(vec![]);
    let id = re.subscribe(Arc::new(move |_| {
        r.fetch_add(1, Ordering::SeqCst);
    }));

    re.run_async(one_count_plan()).await.unwrap();
    let after_first = received.load(Ordering::SeqCst);
    assert!(after_first > 0);

    re.unsubscribe(id);
    re.run_async(one_count_plan()).await.unwrap();
    assert_eq!(
        received.load(Ordering::SeqCst),
        after_first,
        "unsubscribe should stop new docs"
    );
}

#[tokio::test]
async fn register_command_dispatched_via_msg_custom() {
    let counter = Arc::new(AtomicU64::new(0));
    let c2 = counter.clone();
    let re = RunEngine::new(vec![]);
    re.register_command(
        "bump",
        Arc::new(move |payload: &(dyn std::any::Any + Send + Sync)| {
            let c = c2.clone();
            let n = *payload.downcast_ref::<u64>().unwrap_or(&1);
            Box::pin(async move {
                c.fetch_add(n, Ordering::SeqCst);
                Ok(())
            })
        }),
    );

    let plan = plan_box(async_stream::stream! {
        yield Msg::Custom { name: "bump", payload: Box::new(7u64) };
        yield Msg::Custom { name: "bump", payload: Box::new(3u64) };
    });
    re.run_async(plan).await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 10);
}

#[tokio::test]
async fn msg_publish_goes_through_broadcast() {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);

    let resource = Document::Resource(cirrus_event_model::Resource {
        uid: "r-1".into(),
        spec: "AD_HDF5_SWMR_STREAM".into(),
        root: "/data".into(),
        resource_path: "shot.h5".into(),
        path_semantics: "posix".into(),
        run_start: Some("external".into()),
        resource_kwargs: Default::default(),
    });
    let plan = plan_box(async_stream::stream! {
        yield Msg::Publish(Box::new(resource));
    });
    re.run_async(plan).await.unwrap();

    let docs = sink.snapshot().await;
    assert!(docs.iter().any(|d| matches!(d, Document::Resource(_))));
}

#[tokio::test]
async fn loop_timeout_fires_on_overrun() {
    let re = RunEngine::new(vec![]);
    re.set_loop_timeout(Some(Duration::from_millis(120)));

    let plan = plan_box(async_stream::stream! {
        // Far longer than the loop timeout.
        yield Msg::Sleep(Duration::from_secs(5));
    });
    let result = re.run_async(plan).await;
    assert!(result.is_err(), "should time out");
}

#[tokio::test]
async fn unknown_custom_command_errors() {
    let re = RunEngine::new(vec![]);
    let plan = plan_box(async_stream::stream! {
        yield Msg::Custom { name: "no_such", payload: Box::new(()) };
    });
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(
        result.exit_status, "fail",
        "unknown custom command must mark run failed"
    );
}

// -- Monitor → Event flow --------------------------------------------------
//
// MonitorableObj has no backend impl in the crate-soft yet; we fabricate one
// here against a `tokio::sync::watch` channel.

struct TestMonitor {
    name: String,
    tx: tokio::sync::watch::Sender<cirrus_core::reading::ReadingValue>,
}

impl TestMonitor {
    fn new(name: &str) -> Arc<Self> {
        let (tx, _rx) = tokio::sync::watch::channel(cirrus_core::reading::ReadingValue {
            value: Value::from(0.0),
            timestamp: 0.0,
            alarm_severity: None,
            message: None,
        });
        Arc::new(Self {
            name: name.into(),
            tx,
        })
    }
    fn push(&self, v: f64, ts: f64) {
        let _ = self.tx.send(cirrus_core::reading::ReadingValue {
            value: Value::from(v),
            timestamp: ts,
            alarm_severity: None,
            message: None,
        });
    }
    fn rx(&self) -> tokio::sync::watch::Receiver<cirrus_core::reading::ReadingValue> {
        self.tx.subscribe()
    }
}

impl cirrus_core::msg::NamedObj for TestMonitor {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl cirrus_core::msg::ReadableObj for TestMonitor {
    async fn read_dyn(
        &self,
    ) -> Result<
        std::collections::HashMap<String, cirrus_core::reading::ReadingValue>,
        cirrus_core::error::CirrusError,
    > {
        let v = self.tx.borrow().clone();
        let mut out = std::collections::HashMap::new();
        out.insert(self.name.clone(), v);
        Ok(out)
    }
    async fn describe_dyn(
        &self,
    ) -> Result<
        std::collections::HashMap<String, cirrus_event_model::DataKey>,
        cirrus_core::error::CirrusError,
    > {
        let mut out = std::collections::HashMap::new();
        out.insert(
            self.name.clone(),
            cirrus_event_model::DataKey {
                source: format!("test://{}", self.name),
                dtype: cirrus_event_model::Dtype::Number,
                shape: vec![],
                dtype_numpy: Some("<f8".into()),
                external: None,
                units: None,
                precision: None,
                object_name: None,
                dims: None,
                limits: None,
            },
        );
        Ok(out)
    }
}

#[async_trait::async_trait]
impl cirrus_core::msg::MonitorableObj for TestMonitor {
    async fn subscribe_dyn(
        &self,
    ) -> Result<cirrus_core::subscription::Subscription, cirrus_core::error::CirrusError> {
        let rx = self.rx();
        Ok(cirrus_core::subscription::Subscription::new(
            rx,
            cirrus_core::status::SubToken::noop(),
        ))
    }
}

#[tokio::test]
async fn monitor_emits_descriptor_then_events() {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let mon = TestMonitor::new("mon1");
    let mon_for_plan: Arc<dyn cirrus_core::msg::MonitorableObj> = mon.clone();

    let mon_for_drive = mon.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Monitor { obj: mon_for_plan.clone(), name: None };
        // Wait long enough for the pump to install before pushing values.
        yield Msg::Sleep(Duration::from_millis(50));
        for i in 1..=3 {
            // Push from outside the engine, but inside the same tokio runtime
            // by capturing mon_for_drive in the plan stream.
            mon_for_drive.push(i as f64, i as f64);
            yield Msg::Sleep(Duration::from_millis(50));
        }
        yield Msg::Unmonitor(mon_for_plan);
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    re.run_async(plan).await.unwrap();

    let docs = sink.snapshot().await;
    let descriptors = docs
        .iter()
        .filter(|d| matches!(d, Document::Descriptor(_)))
        .count();
    let events = docs
        .iter()
        .filter(|d| matches!(d, Document::Event(_)))
        .count();
    assert!(descriptors >= 1, "expected at least one descriptor");
    assert!(
        events >= 1,
        "expected at least one Event from the monitor pump"
    );
}

#[tokio::test]
async fn pause_changes_state_to_paused() {
    let re = Arc::new(RunEngine::new(vec![]));
    assert_eq!(re.state(), EngineRunState::Idle);

    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        // Sleep gives the test time to call pause and observe state.
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(re.state(), EngineRunState::Running);
    re.pause(false);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(re.state(), EngineRunState::Paused);
    re.resume();
    let _ = join.await.unwrap();
    assert_eq!(re.state(), EngineRunState::Idle);
}
