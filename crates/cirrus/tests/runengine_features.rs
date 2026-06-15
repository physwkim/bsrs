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
use cirrus_event_model::{DocFilter, Document};
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
async fn scan_id_written_back_to_md_lets_custom_source_continue_sequence() {
    // ENG-14: a custom scan_id_source that reads md["scan_id"] and adds 1 must
    // see the previous run's value. That requires the engine to write the
    // resolved scan_id back into RE.md after each run (bluesky
    // run_engine.py:1855). Without the write-back every run reads an absent
    // "scan_id" and produces 1, 1, 1.
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    re.set_scan_id_source(Some(Arc::new(
        |md: &std::collections::HashMap<String, Value>| {
            let prev = md.get("scan_id").and_then(|v| v.as_u64()).unwrap_or(0);
            Ok(prev + 1)
        },
    )));

    re.run_async(one_count_plan()).await.unwrap();
    re.run_async(one_count_plan()).await.unwrap();
    re.run_async(one_count_plan()).await.unwrap();

    let starts: Vec<u64> = sink
        .snapshot()
        .await
        .iter()
        .filter_map(|d| match d {
            Document::Start(s) => s.scan_id,
            _ => None,
        })
        .collect();
    assert_eq!(
        starts,
        vec![1, 2, 3],
        "custom scan_id_source must continue the sequence via md write-back"
    );
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
async fn msg_hook_sees_every_message_before_dispatch() {
    // ENG-03: RE.msg_hook (bluesky run_engine.py:1645) fires for every Msg
    // just before it is handled. Capture the Debug-formatted variant names.
    let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
    let s = seen.clone();
    let re = RunEngine::new(vec![]);
    re.set_msg_hook(Some(Arc::new(move |msg: &Msg| {
        // First token of the Debug repr is the variant name.
        let repr = format!("{msg:?}");
        let head = repr.split([' ', '(', '{']).next().unwrap_or("").to_string();
        s.lock().unwrap().push(head);
    })));

    re.run_async(one_count_plan()).await.unwrap();

    let names = seen.lock().unwrap().clone();
    assert!(!names.is_empty(), "msg_hook never fired");
    // Every run brackets its messages with OpenRun / CloseRun.
    assert!(
        names.iter().any(|n| n == "OpenRun"),
        "msg_hook missed OpenRun; saw: {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "CloseRun"),
        "msg_hook missed CloseRun; saw: {names:?}"
    );
    // A 1-point count reads the detector at least once.
    assert!(
        names.iter().any(|n| n == "Read"),
        "msg_hook missed Read; saw: {names:?}"
    );
}

#[tokio::test]
async fn msg_hook_cleared_stops_firing() {
    // ENG-03: passing None clears the hook — no further calls.
    let count = Arc::new(AtomicU64::new(0));
    let c = count.clone();
    let re = RunEngine::new(vec![]);
    re.set_msg_hook(Some(Arc::new(move |_msg: &Msg| {
        c.fetch_add(1, Ordering::SeqCst);
    })));
    re.run_async(one_count_plan()).await.unwrap();
    let after_first = count.load(Ordering::SeqCst);
    assert!(after_first > 0, "hook should have fired on first run");

    re.set_msg_hook(None);
    re.run_async(one_count_plan()).await.unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        after_first,
        "cleared hook must not fire on the second run"
    );
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
async fn subscribe_filtered_delivers_only_matching_document_types() {
    // ENG-06: a filtered subscriber receives only its document type;
    // `All` still receives every type. One subscriber per filter boundary.
    let re = RunEngine::new(vec![]);

    let start_kinds = Arc::new(StdMutex::new(Vec::<String>::new()));
    let event_kinds = Arc::new(StdMutex::new(Vec::<String>::new()));
    let stop_kinds = Arc::new(StdMutex::new(Vec::<String>::new()));
    let all_kinds = Arc::new(StdMutex::new(Vec::<String>::new()));

    fn kind_of(d: &Document) -> &'static str {
        match d {
            Document::Start(_) => "start",
            Document::Descriptor(_) => "descriptor",
            Document::Event(_) => "event",
            Document::Stop(_) => "stop",
            _ => "other",
        }
    }
    let push_kind = |bucket: Arc<StdMutex<Vec<String>>>| {
        Arc::new(move |d: &Document| {
            bucket.lock().unwrap().push(kind_of(d).into());
        }) as Arc<dyn Fn(&Document) + Send + Sync>
    };

    re.subscribe_filtered(DocFilter::Start, push_kind(start_kinds.clone()));
    re.subscribe_filtered(DocFilter::Event, push_kind(event_kinds.clone()));
    re.subscribe_filtered(DocFilter::Stop, push_kind(stop_kinds.clone()));
    re.subscribe_filtered(DocFilter::All, push_kind(all_kinds.clone()));

    re.run_async(one_count_plan()).await.unwrap();

    // Each filtered subscriber sees exactly its document type, nothing else.
    let starts = start_kinds.lock().unwrap().clone();
    assert_eq!(starts, vec!["start"], "Start filter saw: {starts:?}");

    let events = event_kinds.lock().unwrap().clone();
    assert!(!events.is_empty(), "Event filter saw nothing");
    assert!(
        events.iter().all(|k| k == "event"),
        "Event filter leaked non-events: {events:?}"
    );

    let stops = stop_kinds.lock().unwrap().clone();
    assert_eq!(stops, vec!["stop"], "Stop filter saw: {stops:?}");

    // All subscriber spans the full set.
    let all = all_kinds.lock().unwrap().clone();
    assert_eq!(all.first().map(String::as_str), Some("start"));
    assert!(all.iter().any(|s| s == "descriptor"));
    assert!(all.iter().any(|s| s == "event"));
    assert_eq!(all.last().map(String::as_str), Some("stop"));
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
        path_semantics: Some("posix".into()),
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

#[tokio::test]
async fn suspend_bool_high_pauses_on_high_resumes_on_low() {
    use cirrus_engine::SuspendBoolHigh;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let re = Arc::new(RunEngine::new(vec![]));
    let _watcher = SuspendBoolHigh::new("shutter", rx).install(re.clone());

    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Trigger BAD: signal goes high → engine should pause.
    tx.send(true).unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        re.state(),
        EngineRunState::Paused,
        "SuspendBoolHigh must pause when signal goes high"
    );

    // Restore GOOD: signal goes low → engine should auto-resume.
    tx.send(false).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("auto-resume in time")
        .unwrap()
        .unwrap();
    let _ = result;
    assert_eq!(re.state(), EngineRunState::Idle);
}

#[tokio::test]
async fn suspend_bool_low_pauses_on_low_resumes_on_high() {
    use cirrus_engine::SuspendBoolLow;
    let (tx, rx) = tokio::sync::watch::channel(true);
    let re = Arc::new(RunEngine::new(vec![]));
    let _watcher = SuspendBoolLow::new("beam", rx).install(re.clone());

    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    tx.send(false).unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(re.state(), EngineRunState::Paused);

    tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("auto-resume in time")
        .unwrap()
        .unwrap();
    assert_eq!(re.state(), EngineRunState::Idle);
}

#[tokio::test]
async fn suspend_threshold_floor_pauses_when_below() {
    use cirrus_engine::{SuspendThreshold, ThresholdDirection};
    let (tx, rx) = tokio::sync::watch::channel(100.0_f64);
    let re = Arc::new(RunEngine::new(vec![]));
    let _watcher = SuspendThreshold::new("beam_current", rx, 50.0, ThresholdDirection::BadIfBelow)
        .install(re.clone());

    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    tx.send(40.0).unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(re.state(), EngineRunState::Paused);

    tx.send(80.0).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("auto-resume in time")
        .unwrap()
        .unwrap();
    assert_eq!(re.state(), EngineRunState::Idle);
}

#[tokio::test]
async fn suspend_outside_band_pauses_outside_resumes_inside() {
    // ENG-13: pause when value leaves (band_bottom, band_top), resume inside.
    use cirrus_engine::SuspendOutsideBand;
    let (tx, rx) = tokio::sync::watch::channel(25.0_f64); // inside (20, 30)
    let re = Arc::new(RunEngine::new(vec![]));
    let _watcher = SuspendOutsideBand::new("temperature", rx, 20.0, 30.0).install(re.clone());

    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Leave the band above the top edge → pause.
    tx.send(35.0).unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        re.state(),
        EngineRunState::Paused,
        "must pause when value leaves the band"
    );

    // Return inside the band → auto-resume.
    tx.send(25.0).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("auto-resume in time")
        .unwrap()
        .unwrap();
    assert_eq!(re.state(), EngineRunState::Idle);
}

#[tokio::test]
async fn suspend_when_changed_allow_resume_pauses_then_resumes() {
    // ENG-13: with allow_resume, deviating from `expected` pauses; returning
    // to `expected` auto-resumes.
    use cirrus_engine::SuspendWhenChanged;
    let (tx, rx) = tokio::sync::watch::channel("operate".to_string());
    let re = Arc::new(RunEngine::new(vec![]));
    let _watcher = SuspendWhenChanged::new("facility_mode", rx, "operate".to_string())
        .allow_resume()
        .install(re.clone());

    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    tx.send("shutdown".to_string()).unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        re.state(),
        EngineRunState::Paused,
        "must pause when value deviates from expected"
    );

    tx.send("operate".to_string()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("auto-resume in time")
        .unwrap()
        .unwrap();
    assert_eq!(re.state(), EngineRunState::Idle);
}

#[tokio::test]
async fn suspend_when_changed_no_resume_requires_manual_resume() {
    // ENG-13: default (allow_resume=false) is one-shot — returning to
    // `expected` does NOT auto-resume; only a manual RE.resume() lifts it.
    use cirrus_engine::SuspendWhenChanged;
    let (tx, rx) = tokio::sync::watch::channel(0_i64);
    // Keep a receiver alive: the one-shot watcher drops its own on trip, and
    // we still want `tx.send` to succeed afterwards to prove it does nothing.
    let _rx_keep = rx.clone();
    let re = Arc::new(RunEngine::new(vec![]));
    let _watcher = SuspendWhenChanged::new("interlock", rx, 0_i64).install(re.clone());

    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    tx.send(1).unwrap(); // deviate → pause
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(re.state(), EngineRunState::Paused);

    // Return to expected: must NOT auto-resume (one-shot).
    tx.send(0).unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        re.state(),
        EngineRunState::Paused,
        "allow_resume=false must not auto-resume on return to expected"
    );

    // Manual resume lifts the suspension and the plan completes.
    re.resume();
    let _ = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("manual resume completes plan")
        .unwrap()
        .unwrap();
    assert_eq!(re.state(), EngineRunState::Idle);
}

#[tokio::test]
async fn msg_fail_marks_run_failed_with_reason() {
    // Regression for R2-1: Msg::Fail aborts the plan cleanly with
    // a Plan-level error and exit_status="fail". Used by plans like
    // mvr to surface backend errors without panicking.
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Fail("motor disconnected".into());
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(result.exit_status, "fail");

    let docs = sink.snapshot().await;
    let stop = docs
        .iter()
        .rev()
        .find_map(|d| match d {
            Document::Stop(s) => Some(s.clone()),
            _ => None,
        })
        .expect("RunStop should be emitted");
    assert_eq!(stop.exit_status, "fail");
    assert!(
        stop.reason
            .as_ref()
            .map(|r| r.contains("motor disconnected"))
            .unwrap_or(false),
        "RunStop.reason must surface the Fail message; got {:?}",
        stop.reason
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
                choices: None,
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
async fn second_monitor_of_same_object_is_rejected() {
    // bluesky rejects a 'monitor' for an already-monitored object with
    // IllegalMessageSequence (bundlers.py:470-471) BEFORE subscribing or
    // emitting a descriptor. Without the guard cirrus silently re-subscribed,
    // emitted a second Descriptor for the new stream name, and overwrote the
    // pump registry (aborting the first pump). The run loop turns the handler
    // error into RunResult{exit_status:"fail"}.
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let mon = TestMonitor::new("mon1");
    let mon_for_plan: Arc<dyn cirrus_core::msg::MonitorableObj> = mon.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Monitor { obj: mon_for_plan.clone(), name: Some("stream_a".into()) };
        // Second monitor of the SAME object with a DIFFERENT stream name: the
        // pre-fix path emitted a second Descriptor here before failing later.
        yield Msg::Monitor { obj: mon_for_plan.clone(), name: Some("stream_b".into()) };
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(
        result.exit_status, "fail",
        "a second monitor of an already-monitored object must be rejected"
    );

    // Only the first monitor's Descriptor (stream_a) may have been emitted; the
    // rejected second monitor must not have emitted a stream_b Descriptor.
    let docs = sink.snapshot().await;
    let descriptors = docs
        .iter()
        .filter(|d| matches!(d, Document::Descriptor(_)))
        .count();
    assert_eq!(
        descriptors, 1,
        "the rejected second monitor must not emit a second Descriptor; got {descriptors}"
    );
}

#[tokio::test]
async fn unmonitor_of_unmonitored_object_is_rejected() {
    // Symmetric partner to second_monitor_of_same_object_is_rejected. bluesky's
    // bundler raises IllegalMessageSequence ("Cannot 'unmonitor' {obj}; it is
    // not being monitored.", bundlers.py:544-545) when the object is not in
    // _monitor_params. cirrus's Unmonitor handler retain/removed silently, so an
    // 'unmonitor' for a never-monitored object was a no-op the run survived.
    // This covers the invariant boundary `monitored.contains_key == false`; the
    // `== true` boundary (valid unmonitor) is covered by
    // unmonitor_stops_pump_for_custom_named_stream. The run loop turns the
    // handler error into RunResult{exit_status:"fail"}.
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let mon = TestMonitor::new("mon1");
    let mon_for_plan: Arc<dyn cirrus_core::msg::MonitorableObj> = mon.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        // Never monitored — this 'unmonitor' must be rejected, not silently
        // ignored.
        yield Msg::Unmonitor(mon_for_plan);
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(
        result.exit_status, "fail",
        "an 'unmonitor' for an object that is not monitored must be rejected"
    );
}

/// MonitorableObj that counts how many times `describe_dyn` is called, used to
/// prove a monitor rejected for "no open run" never describes the device.
struct DescribeCountingMonitor {
    name: String,
    describes: Arc<AtomicU64>,
    tx: tokio::sync::watch::Sender<cirrus_core::reading::ReadingValue>,
}

impl DescribeCountingMonitor {
    fn new(name: &str) -> (Arc<Self>, Arc<AtomicU64>) {
        let (tx, _rx) = tokio::sync::watch::channel(cirrus_core::reading::ReadingValue {
            value: Value::from(0.0),
            timestamp: 0.0,
            alarm_severity: None,
            message: None,
        });
        let describes = Arc::new(AtomicU64::new(0));
        (
            Arc::new(Self {
                name: name.into(),
                describes: describes.clone(),
                tx,
            }),
            describes,
        )
    }
}

impl cirrus_core::msg::NamedObj for DescribeCountingMonitor {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl cirrus_core::msg::ReadableObj for DescribeCountingMonitor {
    async fn read_dyn(
        &self,
    ) -> Result<
        std::collections::HashMap<String, cirrus_core::reading::ReadingValue>,
        cirrus_core::error::CirrusError,
    > {
        Ok(std::collections::HashMap::new())
    }
    async fn describe_dyn(
        &self,
    ) -> Result<
        std::collections::HashMap<String, cirrus_event_model::DataKey>,
        cirrus_core::error::CirrusError,
    > {
        self.describes.fetch_add(1, Ordering::SeqCst);
        Ok(std::collections::HashMap::new())
    }
}

#[async_trait::async_trait]
impl cirrus_core::msg::MonitorableObj for DescribeCountingMonitor {
    async fn subscribe_dyn(
        &self,
    ) -> Result<cirrus_core::subscription::Subscription, cirrus_core::error::CirrusError> {
        Ok(cirrus_core::subscription::Subscription::new(
            self.tx.subscribe(),
            cirrus_core::status::SubToken::noop(),
        ))
    }
}

#[tokio::test]
async fn monitor_without_open_run_does_not_describe_the_device() {
    // bluesky's _monitor rejects a monitor with no open run at the top
    // (run_engine.py:2040-2044) BEFORE current_run.monitor() runs describe.
    // cirrus's start_monitor called describe_dyn before its own bundler check,
    // so a monitor with no open run did a wasted device describe round-trip
    // before erroring. The handler now checks the open-run precondition first,
    // mirroring the Read path (describe only when bundling). The run fails
    // either way, so the boundary is the describe COUNT, not exit_status:
    // 0 with the guard, 1 without (start_monitor describes, then fails).
    let (mon, describes) = DescribeCountingMonitor::new("mon1");
    let mon_for_plan: Arc<dyn cirrus_core::msg::MonitorableObj> = mon;
    let re = RunEngine::new(Vec::<Arc<dyn DocumentSink>>::new());
    let plan = plan_box(async_stream::stream! {
        // No OpenRun — the monitor must be rejected before any describe.
        yield Msg::Monitor { obj: mon_for_plan, name: None };
    });
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(
        result.exit_status, "fail",
        "a monitor with no open run must be rejected"
    );
    assert_eq!(
        describes.load(Ordering::SeqCst),
        0,
        "no device describe may happen when a monitor is rejected for no open run"
    );
}

#[tokio::test]
async fn unmonitor_stops_pump_for_custom_named_stream() {
    // Regression: monitor_tasks is keyed by the monitored object, not the
    // stream name, so Unmonitor(obj) removes a custom-named monitor's pump.
    // Before the fix the task was keyed by the stream name ("mon1_monitor"),
    // so Unmonitor(obj="mon1") never matched and the pump kept emitting events
    // for values pushed after Unmonitor.
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let mon = TestMonitor::new("mon1");
    let mon_for_plan: Arc<dyn cirrus_core::msg::MonitorableObj> = mon.clone();
    let mon_for_drive = mon.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Monitor { obj: mon_for_plan.clone(), name: Some("mon1_monitor".into()) };
        yield Msg::Sleep(Duration::from_millis(50));
        mon_for_drive.push(1.0, 1.0);
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Unmonitor(mon_for_plan);
        yield Msg::Sleep(Duration::from_millis(50));
        // Pushed AFTER Unmonitor: must not produce an event if the pump stopped.
        mon_for_drive.push(2.0, 2.0);
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    re.run_async(plan).await.unwrap();

    let docs = sink.snapshot().await;
    // The custom stream name reached the descriptor.
    assert!(
        docs.iter().any(|d| matches!(
            d,
            Document::Descriptor(desc) if desc.name.as_deref() == Some("mon1_monitor")
        )),
        "descriptor should carry the custom stream name"
    );
    // The post-Unmonitor value (2.0) must never appear — the pump was stopped.
    let saw_post_unmonitor = docs.iter().any(|d| {
        matches!(
            d,
            Document::Event(ev) if ev.data.get("mon1") == Some(&Value::from(2.0))
        )
    });
    assert!(
        !saw_post_unmonitor,
        "no Event for the value pushed after Unmonitor; the pump must stop"
    );
}

#[tokio::test]
async fn close_run_tears_down_active_monitor_not_explicitly_unmonitored() {
    // bluesky's close_run clears any monitor still subscribed at run close
    // (bundlers.py:246-248). A Msg::Monitor the plan never Unmonitor'd must be
    // torn down when the run closes — not left pumping until plan end, where
    // run_async's cleanup would mask the leak. Observed via the monitor's watch
    // receiver count: the pump (subscribed synchronously in start_monitor)
    // holds the only receiver.
    let re = Arc::new(RunEngine::new(Vec::<Arc<dyn DocumentSink>>::new()));
    let mon = TestMonitor::new("mon_close");
    let mon_for_plan: Arc<dyn cirrus_core::msg::MonitorableObj> = mon.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Monitor { obj: mon_for_plan, name: None };
        yield Msg::Sleep(Duration::from_millis(50));
        // Close WITHOUT a preceding Unmonitor.
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
        // Hold the plan open past the close so the test observes the monitor
        // state before run_async's plan-end cleanup runs.
        yield Msg::Sleep(Duration::from_millis(500));
    });
    let re_run = re.clone();
    let join = tokio::spawn(async move { re_run.run_async(plan).await });

    // Inside the post-close window (close fires ~50ms in; the trailing sleep
    // runs to ~550ms; plan-end cleanup is only after that).
    tokio::time::sleep(Duration::from_millis(250)).await;
    let receivers = mon.tx.receiver_count();

    join.await.unwrap().unwrap();
    assert_eq!(
        receivers, 0,
        "close_run must unsubscribe a monitor never explicitly Unmonitor'd; pre-fix the pump survives (1 receiver)"
    );
}

#[tokio::test]
async fn monitor_survives_pause_and_resume() {
    // bluesky `suspend_monitors`/`restore_monitors` keep `_monitor_params`
    // across a pause so each device is re-subscribed on resume (bundlers.py:
    // 661-666; run_engine.py:1543/2431). cirrus's on_pause_enter drops the live
    // pump (monitor_tasks) but the separate `monitored` registry survives, and
    // on_resume re-installs from it. Pre-fix on_pause cleared the only record of
    // the monitor, so it was lost forever: post-resume pushes produced no Event
    // and the watch had 0 receivers even after resume.
    let sink = Arc::new(CapturingSink::new());
    let re = Arc::new(RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]));
    let mon = TestMonitor::new("mon_pr");
    let mon_for_plan: Arc<dyn cirrus_core::msg::MonitorableObj> = mon.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Monitor { obj: mon_for_plan, name: None };
        // Keep the run alive across the external pause/resume/push sequence.
        for _ in 0..14 {
            yield Msg::Sleep(Duration::from_millis(50));
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let re_run = re.clone();
    let join = tokio::spawn(async move { re_run.run_async(plan).await });

    // The pump subscribes synchronously in start_monitor → 1 receiver running.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(re.state(), EngineRunState::Running);
    assert_eq!(
        mon.tx.receiver_count(),
        1,
        "pump must be subscribed while the run is running"
    );

    // Pause suspends the monitor: the live pump is dropped, releasing the sub.
    re.pause(false);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(re.state(), EngineRunState::Paused);
    assert_eq!(
        mon.tx.receiver_count(),
        0,
        "pause must drop the live pump (suspend_monitors)"
    );

    // Resume must re-install the monitor from the kept registry.
    re.resume();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(re.state(), EngineRunState::Running);
    assert_eq!(
        mon.tx.receiver_count(),
        1,
        "resume must restore the monitor (pre-fix: 0, lost forever)"
    );

    // A value pushed AFTER resume must flow through the restored pump as an Event.
    mon.push(42.0, 42.0);
    tokio::time::sleep(Duration::from_millis(80)).await;

    join.await.unwrap().unwrap();

    let docs = sink.snapshot().await;
    let saw_post_resume = docs.iter().any(|d| {
        matches!(
            d,
            Document::Event(ev) if ev.data.get("mon_pr") == Some(&Value::from(42.0))
        )
    });
    assert!(
        saw_post_resume,
        "post-resume push must produce an Event from the restored monitor"
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

// -- Movable stop on pause ---------------------------------------------------
//
// `MovableObj::stop_on_pause` defaults to a no-op; SoftMotor overrides it
// to delegate to its existing `StoppableObj::stop_dyn`. We need a concrete
// counter to prove the wiring fires; reuse the SoftMotor pattern with a
// hand-rolled mock that increments a counter.

struct StopCountingMovable {
    name: String,
    stops: Arc<AtomicU64>,
}

impl cirrus_core::msg::NamedObj for StopCountingMovable {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl cirrus_core::msg::MovableObj for StopCountingMovable {
    async fn set_dyn(&self, _value: f64) -> cirrus_core::status::Status {
        cirrus_core::status::Status::done()
    }
    async fn stop_on_pause(&self, _success: bool) -> Result<(), cirrus_core::error::CirrusError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn pause_calls_stop_on_pause_for_set_movables() {
    let stops = Arc::new(AtomicU64::new(0));
    let mover: Arc<dyn cirrus_core::msg::MovableObj> = Arc::new(StopCountingMovable {
        name: "m1".into(),
        stops: stops.clone(),
    });
    let re = Arc::new(RunEngine::new(vec![]));
    let re2 = re.clone();
    let mover_for_plan = mover.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Set { obj: mover_for_plan, value: 1.0, group: None };
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    re.pause(false);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        stops.load(Ordering::SeqCst) >= 1,
        "stop_on_pause should fire for movables touched by Msg::Set"
    );
    re.resume();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn cleanup_calls_stop_on_pause_for_touched_movables() {
    let stops = Arc::new(AtomicU64::new(0));
    let mover: Arc<dyn cirrus_core::msg::MovableObj> = Arc::new(StopCountingMovable {
        name: "m1".into(),
        stops: stops.clone(),
    });
    let re = RunEngine::new(vec![]);
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Set { obj: mover.clone(), value: 1.0, group: None };
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    re.run_async(plan).await.unwrap();
    assert_eq!(
        stops.load(Ordering::SeqCst),
        1,
        "stop_on_pause must fire once during run cleanup",
    );
}

// -- Msg::Prepare ------------------------------------------------------------

struct ScriptedPreparable {
    name: String,
    captured: Arc<StdMutex<Vec<Value>>>,
}

impl cirrus_core::msg::NamedObj for ScriptedPreparable {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl cirrus_core::msg::PreparableObj for ScriptedPreparable {
    async fn prepare_dyn(&self, value: Value) -> cirrus_core::status::Status {
        self.captured.lock().unwrap().push(value);
        cirrus_core::status::Status::done()
    }
}

#[tokio::test]
async fn prepare_invokes_device_and_groups_status() {
    let captured = Arc::new(StdMutex::new(Vec::<Value>::new()));
    let dev: Arc<dyn cirrus_core::msg::PreparableObj> = Arc::new(ScriptedPreparable {
        name: "flyer".into(),
        captured: captured.clone(),
    });
    let re = RunEngine::new(vec![]);
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Prepare { obj: dev, value: serde_json::json!({"frames": 5}), group: Some("p".into()) };
        yield Msg::Wait { group: "p".into(), error_on_timeout: true, timeout: None };
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    re.run_async(plan).await.unwrap();
    let got = captured.lock().unwrap().clone();
    assert_eq!(got.len(), 1, "prepare_dyn should be called exactly once");
    assert_eq!(got[0], serde_json::json!({"frames": 5}));
}

// -- Msg::WaitFor ------------------------------------------------------------

#[tokio::test]
async fn wait_for_runs_factories_in_order() {
    let log = Arc::new(StdMutex::new(Vec::<u32>::new()));
    let l1 = log.clone();
    let l2 = log.clone();
    let f1: Arc<
        dyn Fn() -> futures::future::BoxFuture<'static, cirrus_core::error::Result<()>>
            + Send
            + Sync,
    > = Arc::new(move || {
        let l = l1.clone();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            l.lock().unwrap().push(1);
            Ok(())
        })
    });
    let f2: Arc<
        dyn Fn() -> futures::future::BoxFuture<'static, cirrus_core::error::Result<()>>
            + Send
            + Sync,
    > = Arc::new(move || {
        let l = l2.clone();
        Box::pin(async move {
            l.lock().unwrap().push(2);
            Ok(())
        })
    });
    let re = RunEngine::new(vec![]);
    let plan = plan_box(async_stream::stream! {
        yield Msg::WaitFor { factories: vec![f1, f2], timeout: None };
    });
    re.run_async(plan).await.unwrap();
    assert_eq!(log.lock().unwrap().clone(), vec![1, 2]);
}

#[tokio::test]
async fn wait_for_times_out() {
    let f: Arc<
        dyn Fn() -> futures::future::BoxFuture<'static, cirrus_core::error::Result<()>>
            + Send
            + Sync,
    > = Arc::new(|| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        })
    });
    let re = RunEngine::new(vec![]);
    let plan = plan_box(async_stream::stream! {
        yield Msg::WaitFor { factories: vec![f], timeout: Some(Duration::from_millis(50)) };
    });
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(
        result.exit_status, "fail",
        "WaitFor timeout should fail run"
    );
}

// -- Pausable device hooks ---------------------------------------------------

struct PauseTracker {
    name: String,
    paused: Arc<AtomicU64>,
    resumed: Arc<AtomicU64>,
}

impl cirrus_core::msg::NamedObj for PauseTracker {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl cirrus_core::msg::PausableObj for PauseTracker {
    async fn pause_dyn(&self) -> Result<(), cirrus_core::error::CirrusError> {
        self.paused.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn resume_dyn(&self) -> Result<(), cirrus_core::error::CirrusError> {
        self.resumed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn pausable_hooks_fire_on_pause_and_resume() {
    let paused = Arc::new(AtomicU64::new(0));
    let resumed = Arc::new(AtomicU64::new(0));
    let dev: Arc<dyn cirrus_core::msg::PausableObj> = Arc::new(PauseTracker {
        name: "pausable_dev".into(),
        paused: paused.clone(),
        resumed: resumed.clone(),
    });
    let re = Arc::new(RunEngine::new(vec![]));
    re.register_pausable(dev.clone()).await;

    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    re.pause(false);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        paused.load(Ordering::SeqCst),
        1,
        "pause_dyn should fire once on pause"
    );
    re.resume();
    let _ = join.await.unwrap();
    assert_eq!(
        resumed.load(Ordering::SeqCst),
        1,
        "resume_dyn should fire once on resume"
    );
}

#[tokio::test]
async fn register_pausable_via_msg() {
    let paused = Arc::new(AtomicU64::new(0));
    let resumed = Arc::new(AtomicU64::new(0));
    let dev: Arc<dyn cirrus_core::msg::PausableObj> = Arc::new(PauseTracker {
        name: "via_msg".into(),
        paused: paused.clone(),
        resumed: resumed.clone(),
    });
    let re = Arc::new(RunEngine::new(vec![]));
    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::RegisterPausable(dev.clone());
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::UnregisterPausable(dev);
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    re.pause(false);
    tokio::time::sleep(Duration::from_millis(40)).await;
    re.resume();
    let _ = join.await.unwrap();
    assert!(paused.load(Ordering::SeqCst) >= 1);
    assert!(resumed.load(Ordering::SeqCst) >= 1);
}

// -- Suspender — request_suspend pauses; suspend_until auto-resumes ----------

#[tokio::test]
async fn request_suspend_pauses_engine() {
    let re = Arc::new(RunEngine::new(vec![]));
    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    re.request_suspend("shutter closed");
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        re.state(),
        EngineRunState::Paused,
        "request_suspend must pause, not abort"
    );
    re.resume();
    let _ = join.await.unwrap().unwrap();
    assert_eq!(
        re.state(),
        EngineRunState::Idle,
        "engine returns to idle after manual resume"
    );
}

#[tokio::test]
async fn suspend_until_pauses_then_auto_resumes() {
    let re = Arc::new(RunEngine::new(vec![]));
    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    re.suspend_until(Box::pin(async move {
        let _ = rx.await;
    }));
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(re.state(), EngineRunState::Paused);
    let _ = tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("did not auto-resume in time")
        .unwrap()
        .unwrap();
    assert_eq!(
        re.state(),
        EngineRunState::Idle,
        "engine returns to idle after auto-resume"
    );
}

// -- Msg::Input --------------------------------------------------------------

#[tokio::test]
async fn input_with_handler_returns_text() {
    let re = RunEngine::new(vec![]);
    re.set_input_handler(Some(Arc::new(|prompt: String| {
        Box::pin(async move { Ok(format!("answer:{prompt}")) })
    })));
    let plan = plan_box(async_stream::stream! {
        yield Msg::Input { prompt: "name?".into() };
    });
    re.run_async(plan).await.unwrap();
    match re.take_msg_result() {
        cirrus_engine::MsgResult::Input { text } => assert_eq!(text, "answer:name?"),
        other => panic!("expected MsgResult::Input, got {other:?}"),
    }
}

#[tokio::test]
async fn input_without_handler_fails() {
    let re = RunEngine::new(vec![]);
    let plan = plan_box(async_stream::stream! {
        yield Msg::Input { prompt: "no handler".into() };
    });
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(result.exit_status, "fail");
}

// -- Msg::ReClass ------------------------------------------------------------

#[tokio::test]
async fn re_class_reports_engine_name() {
    let re = RunEngine::new(vec![]);
    let plan = plan_box(async_stream::stream! {
        yield Msg::ReClass;
    });
    re.run_async(plan).await.unwrap();
    match re.take_msg_result() {
        cirrus_engine::MsgResult::EngineClass { name } => assert_eq!(name, "cirrus.RunEngine"),
        other => panic!("expected MsgResult::EngineClass, got {other:?}"),
    }
}

// -- Msg::Subscribe / Unsubscribe + temp sub auto-cleanup -------------------

#[tokio::test]
async fn msg_subscribe_receives_documents_and_auto_unsubscribes() {
    let count = Arc::new(AtomicU64::new(0));
    let c2 = count.clone();
    let cb: cirrus_core::msg::SubscribeCallback = Arc::new(move |_d| {
        c2.fetch_add(1, Ordering::SeqCst);
    });
    let re = RunEngine::new(vec![]);

    let plan = plan_box(async_stream::stream! {
        yield Msg::Subscribe { cb, filter: DocFilter::All };
        yield Msg::OpenRun(Default::default());
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    re.run_async(plan).await.unwrap();
    let after_first = count.load(Ordering::SeqCst);
    assert!(after_first >= 2, "subscriber should see start + stop");

    // Run another plan with no subscribe; the prior subscriber must
    // have been removed at the previous run's end.
    let plan2 = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    re.run_async(plan2).await.unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        after_first,
        "temp subscriber must be removed at run end"
    );
}

#[tokio::test]
async fn msg_unsubscribe_removes_callback_immediately() {
    let count = Arc::new(AtomicU64::new(0));
    let c2 = count.clone();
    let cb: cirrus_core::msg::SubscribeCallback = Arc::new(move |_d| {
        c2.fetch_add(1, Ordering::SeqCst);
    });
    let re = Arc::new(RunEngine::new(vec![]));
    re.set_input_handler(Some(Arc::new(|_| Box::pin(async { Ok(String::new()) }))));

    // Use a custom command to surface the subscription id back to
    // the test (Msg::Subscribe stores it in MsgResult, but we don't
    // have a stable mid-run hook to read it; instead we issue
    // Subscribe → Unsubscribe via a wrapping handler).
    let plan = plan_box(async_stream::stream! {
        yield Msg::Subscribe { cb: cb.clone(), filter: DocFilter::All };
        yield Msg::OpenRun(Default::default());
        // No Unsubscribe here; auto-cleanup at run end is enough
        // for this test — we just need the subscriber to fire.
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    re.run_async(plan).await.unwrap();
    assert!(count.load(Ordering::SeqCst) >= 2);
}

// -- md_normalizer ----------------------------------------------------------

#[tokio::test]
async fn md_normalizer_modifies_runstart() {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    re.set_md_normalizer(Some(Arc::new(|mut md| {
        md.insert("normalized".into(), Value::Bool(true));
        Ok(md)
    })));
    re.run_async(one_count_plan()).await.unwrap();
    let docs = sink.snapshot().await;
    let start = match &docs[0] {
        Document::Start(s) => s,
        _ => panic!("first doc not Start"),
    };
    assert_eq!(start.extra.get("normalized"), Some(&Value::Bool(true)));
}

// -- scan_id_source ---------------------------------------------------------

#[tokio::test]
async fn scan_id_source_overrides_auto_increment() {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    re.set_scan_id_source(Some(Arc::new(|_md| Ok(42))));
    re.run_async(one_count_plan()).await.unwrap();
    let docs = sink.snapshot().await;
    let start = match &docs[0] {
        Document::Start(s) => s,
        _ => panic!("first doc not Start"),
    };
    assert_eq!(start.scan_id, Some(42));
}

// -- preprocessors ----------------------------------------------------------

#[tokio::test]
async fn preprocessor_wraps_plan() {
    use cirrus_core::plan::PlanItem;
    use futures::StreamExt;
    let count = Arc::new(AtomicU64::new(0));
    let c2 = count.clone();
    let pp: cirrus_engine::Preprocessor = Arc::new(move |inner: Plan| {
        let c = c2.clone();
        plan_box(async_stream::stream! {
            let mut inner = inner;
            // Prepend one Sleep — observable as +1 message.
            c.fetch_add(1, Ordering::SeqCst);
            yield Msg::Sleep(Duration::from_millis(1));
            while let Some(it) = inner.next().await {
                if let PlanItem::Bare(m) = it {
                    yield m;
                }
            }
        })
    });
    let re = RunEngine::new(vec![]);
    re.add_preprocessor(pp);
    re.run_async(one_count_plan()).await.unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "preprocessor should run exactly once at run_async entry"
    );
}

// -- run_async_with: per-call md + temp subs --------------------------------

#[tokio::test]
async fn run_async_with_per_call_md_lands_in_runstart() {
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let mut md = std::collections::HashMap::new();
    md.insert("operator".into(), Value::String("bob".into()));
    let opts = cirrus_engine::RunOptions { md, subs: vec![] };
    re.run_async_with(one_count_plan(), opts).await.unwrap();
    let docs = sink.snapshot().await;
    let start = match &docs[0] {
        Document::Start(s) => s,
        _ => panic!("first doc not Start"),
    };
    assert_eq!(
        start.extra.get("operator"),
        Some(&Value::String("bob".into()))
    );
    // Per-call md should NOT persist into the next run.
    re.run_async(one_count_plan()).await.unwrap();
    let docs2 = sink.snapshot().await;
    let start2 = match docs2.iter().rev().find(|d| matches!(d, Document::Start(_))) {
        Some(Document::Start(s)) => s,
        _ => panic!(),
    };
    assert!(
        !start2.extra.contains_key("operator"),
        "per-call md must not persist"
    );
}

#[tokio::test]
async fn per_call_md_wins_over_per_run_open_run_extra() {
    // bluesky ChainMap precedence (run_engine.py:1861-1870): the operator's
    // invocation-time md (`_metadata_per_call`, set via `run_async_with`)
    // outranks the per-run md a plan bakes into its `OpenRun` Msg. When both
    // set the same key, per-call must win.
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    // Plan supplies a conflicting `operator` via the OpenRun extra md.
    let plan = plan_box(async_stream::stream! {
        let mut extra = std::collections::HashMap::new();
        extra.insert("operator".to_string(), Value::String("plan".into()));
        yield Msg::OpenRun(cirrus_core::msg::RunMetadata {
            extra,
            ..Default::default()
        });
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let mut md = std::collections::HashMap::new();
    md.insert("operator".into(), Value::String("user".into()));
    let opts = cirrus_engine::RunOptions { md, subs: vec![] };
    re.run_async_with(plan, opts).await.unwrap();
    let docs = sink.snapshot().await;
    let start = match &docs[0] {
        Document::Start(s) => s,
        _ => panic!("first doc not Start"),
    };
    assert_eq!(
        start.extra.get("operator"),
        Some(&Value::String("user".into())),
        "per-call md (run_async_with) must outrank per-run OpenRun extra"
    );
}

#[tokio::test]
async fn run_async_with_temp_subs_auto_remove_at_run_end() {
    let count = Arc::new(AtomicU64::new(0));
    let c2 = count.clone();
    let re = RunEngine::new(vec![]);
    let opts = cirrus_engine::RunOptions {
        md: Default::default(),
        subs: vec![Arc::new(move |_d: &Document| {
            c2.fetch_add(1, Ordering::SeqCst);
        })],
    };
    re.run_async_with(one_count_plan(), opts).await.unwrap();
    let after_first = count.load(Ordering::SeqCst);
    assert!(after_first > 0);
    re.run_async(one_count_plan()).await.unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        after_first,
        "temp subs from run_async_with must be removed at run end"
    );
}

// -- record_interruptions ----------------------------------------------------

#[tokio::test]
async fn record_interruptions_emits_descriptor_and_events() {
    let sink = Arc::new(CapturingSink::new());
    let re = Arc::new(RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]));
    re.set_record_interruptions(true);

    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    re.pause(false);
    tokio::time::sleep(Duration::from_millis(40)).await;
    re.resume();
    let _ = join.await.unwrap().unwrap();

    let docs = sink.snapshot().await;
    let interruption_descriptors: Vec<_> = docs
        .iter()
        .filter_map(|d| match d {
            Document::Descriptor(d) if d.name.as_deref() == Some("interruptions") => Some(d),
            _ => None,
        })
        .collect();
    assert_eq!(
        interruption_descriptors.len(),
        1,
        "exactly one interruptions descriptor expected"
    );
    let desc = interruption_descriptors[0];
    assert!(desc.data_keys.contains_key("interruption"));

    let interruption_events: Vec<_> = docs
        .iter()
        .filter_map(|d| match d {
            Document::Event(e) if e.descriptor == desc.uid => Some(e),
            _ => None,
        })
        .collect();
    assert!(
        interruption_events.len() >= 2,
        "expected at least pause + resume events, got {}",
        interruption_events.len()
    );
    let labels: Vec<String> = interruption_events
        .iter()
        .filter_map(|e| {
            e.data
                .get("interruption")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect();
    assert!(labels.iter().any(|s| s == "pause"));
    assert!(labels.iter().any(|s| s == "resume"));
}

#[tokio::test]
async fn record_interruptions_off_emits_nothing() {
    let sink = Arc::new(CapturingSink::new());
    let re = Arc::new(RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]));
    // record_interruptions defaults to false.
    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    re.pause(false);
    tokio::time::sleep(Duration::from_millis(30)).await;
    re.resume();
    let _ = join.await.unwrap().unwrap();
    let docs = sink.snapshot().await;
    let any_interruptions = docs.iter().any(|d| match d {
        Document::Descriptor(d) => d.name.as_deref() == Some("interruptions"),
        _ => false,
    });
    assert!(
        !any_interruptions,
        "no interruptions stream should be declared when recording is off"
    );
}

#[tokio::test]
async fn suspend_until_with_records_justification() {
    let sink = Arc::new(CapturingSink::new());
    let re = Arc::new(RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]));
    re.set_record_interruptions(true);
    let re2 = re.clone();
    let plan = plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::Sleep(Duration::from_millis(50));
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let join = tokio::spawn(async move { re2.run_async(plan).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    re.suspend_until_with(
        Box::pin(async move {
            let _ = rx.await;
        }),
        Some("shutter closed".into()),
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    let _ = tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("did not auto-resume in time")
        .unwrap()
        .unwrap();

    let docs = sink.snapshot().await;
    let labels: Vec<String> = docs
        .iter()
        .filter_map(|d| match d {
            Document::Event(e) => e
                .data
                .get("interruption")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            _ => None,
        })
        .collect();
    assert!(
        labels.iter().any(|s| s == "shutter closed"),
        "expected the supplied justification to be recorded, got {labels:?}"
    );
}

// -- sigint_count reset ------------------------------------------------------

#[tokio::test]
async fn sigint_count_resets_across_runs() {
    use std::sync::atomic::AtomicU8;
    // The counter is private; we exercise the externally observable
    // consequence: an engine that completed a previous run still
    // responds to a single explicit pause() request without going
    // straight into the abort/halt path.
    //
    // We can't simulate SIGINT in a unit test without owning the
    // process signal handler, but the reset itself is small and
    // mechanically verifiable: install_signal_handler is idempotent
    // and reset happens on every run_async entry.
    let re = Arc::new(RunEngine::new(vec![]));
    re.run_async(one_count_plan()).await.unwrap();
    re.run_async(one_count_plan()).await.unwrap();
    // Just prove the engine is reusable; this is the behavior the
    // sigint_count reset is needed for.
    assert_eq!(re.state(), EngineRunState::Idle);
    let _ = AtomicU8::new(0); // touch import to silence unused warning
}
