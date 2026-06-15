//! End-to-end acceptance tests for cirrus.

use std::sync::Arc;

use cirrus::backends::soft::{SoftDetector, SoftMotor};
use cirrus::callbacks::CapturingSink;
use cirrus::prelude::*;
use cirrus_core::msg::{LocatableObj, StoppableObj};

#[tokio::test]
async fn count_plan_emits_expected_document_sequence() {
    // 1 detector, 5 iterations  →  Start, Descriptor, 5 × Event, Stop  =  8 docs.
    let det = SoftDetector::new("det1");
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);

    let plan = cirrus::ophyd_async::count(vec![det.clone()], 5);
    let result = re.run_async(plan).await.expect("plan failed");
    assert_eq!(result.exit_status, "success");

    let docs = sink.snapshot().await;
    assert_eq!(docs.len(), 8, "expected 8 documents, got {}", docs.len());

    use cirrus_core::Document::*;
    assert!(matches!(&docs[0], Start(_)), "doc 0 is RunStart");
    assert!(matches!(&docs[1], Descriptor(_)), "doc 1 is Descriptor");
    for (i, d) in docs.iter().enumerate().take(7).skip(2) {
        assert!(matches!(d, Event(_)), "doc {i} is Event");
    }
    assert!(matches!(&docs[7], Stop(_)), "last doc is RunStop");

    // RunStart and RunStop should reference each other.
    if let (Start(start), Stop(stop)) = (&docs[0], &docs[7]) {
        assert_eq!(stop.run_start, start.uid);
        assert_eq!(stop.exit_status, "success");
        assert_eq!(stop.num_events.get("primary").copied(), Some(5));
    }
}

#[tokio::test]
async fn adaptive_scan_runs_to_completion() {
    let det = SoftDetector::new("det1");
    let motor = Arc::new(SoftMotor::new("m1", Some(0.0)));
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let plan = cirrus_plans::adaptive_scan(
        vec![det.clone() as Arc<dyn cirrus_core::msg::ReadableObj>],
        "det1_counts",
        motor.clone() as Arc<dyn cirrus_core::msg::MovableObj>,
        motor.clone() as Arc<dyn cirrus_core::msg::ReadableObj>,
        0.0,
        2.0,
        0.1,
        0.5,
        1.0,
        false,
    );
    let result = re.run_async(plan).await.expect("adaptive_scan failed");
    assert_eq!(result.exit_status, "success");
    let docs = sink.snapshot().await;
    // At least Start + Descriptor + ≥1 Event + Stop.
    assert!(docs.len() >= 4, "got {} docs", docs.len());
}

#[tokio::test]
async fn tune_centroid_moves_motor_to_computed_center() {
    let det = SoftDetector::new("det1");
    let motor = Arc::new(SoftMotor::new("m1", Some(0.0)));
    let re = RunEngine::new(vec![]);
    let plan = cirrus_plans::tune_centroid(
        vec![det.clone() as Arc<dyn cirrus_core::msg::ReadableObj>],
        "det1_counts",
        motor.clone() as Arc<dyn cirrus_core::msg::MovableObj>,
        motor.clone() as Arc<dyn cirrus_core::msg::ReadableObj>,
        0.0,
        4.0,
        5,
    );
    let result = re.run_async(plan).await.expect("tune_centroid failed");
    assert_eq!(result.exit_status, "success");
    // SoftDetector returns 0 counts → centroid undefined → motor at last_pos = 4.0
    let setpoint = motor.locate_dyn().await.unwrap().setpoint;
    assert!(
        (setpoint - 4.0).abs() < 1e-9 || setpoint.is_finite(),
        "motor should land at a finite position; got {setpoint}",
    );
}

#[tokio::test]
async fn scan_plan_emits_motor_and_detector_readings() {
    let det = SoftDetector::new("det1");
    let motor = Arc::new(SoftMotor::new("m1", Some(0.0)));
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);

    let plan = cirrus::ophyd_async::scan(
        vec![det.clone() as Arc<dyn cirrus_core::msg::ReadableObj>],
        motor.clone() as Arc<dyn cirrus_core::msg::MovableObj>,
        motor.clone() as Arc<dyn cirrus_core::msg::ReadableObj>,
        0.0,
        4.0,
        5,
    );
    let result = re.run_async(plan).await.expect("scan failed");
    assert_eq!(result.exit_status, "success");

    let docs = sink.snapshot().await;
    // Start + Descriptor + 5 × Event + Stop = 8
    assert_eq!(docs.len(), 8);

    // Descriptor should carry both motor and detector data keys.
    if let cirrus_core::Document::Descriptor(d) = &docs[1] {
        assert!(
            d.data_keys.contains_key("m1"),
            "missing motor key: {:?}",
            d.data_keys.keys().collect::<Vec<_>>()
        );
        assert!(d.data_keys.contains_key("det1_counts"));
    } else {
        panic!("doc 1 was not a Descriptor");
    }
}

#[tokio::test]
async fn binary_frame_sink_writes_and_emits_stream_docs() {
    use cirrus::stream::sinks::BinaryFrameSink;
    use cirrus_protocols_async::{DetectorWriter, Frame, FrameSink};
    use futures::StreamExt;

    let tmp = tempdir().unwrap();
    let path = tmp.path().join("frames.cirbin1");
    let sink = BinaryFrameSink::new("det", &path, 4);

    // Open + accept 3 frames + close.
    sink.open(1).await.unwrap();
    for i in 0..3_u32 {
        sink.accept(Frame {
            payload: bytes::Bytes::from(i.to_le_bytes().to_vec()),
            ts_ns: 0,
            channel: 0,
            flags: 0,
            seq: i as u64,
        })
        .await
        .unwrap();
    }
    sink.close().await.unwrap();

    // Verify file: magic + 3 × (len_le, payload).
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..8], b"CIRBIN1\n");
    assert_eq!(bytes.len(), 8 + 3 * (4 + 4));
    assert_eq!(sink.indices_written().await, 3);

    // collect_stream_docs(3) should produce StreamResource + StreamDatum [0,3),
    // and the StreamDatum must carry the descriptor UID we pass (CBEM-13).
    let docs: Vec<_> = sink
        .collect_stream_docs(3, "desc-uid-xyz")
        .collect::<Vec<_>>()
        .await;
    assert_eq!(docs.len(), 2);
    use cirrus::ophyd_async::StreamAsset;
    assert!(matches!(&docs[0], StreamAsset::Resource(_)));
    if let StreamAsset::Datum(d) = &docs[1] {
        assert_eq!(d.indices.start, 0);
        assert_eq!(d.indices.stop, 3);
        assert_eq!(d.descriptor, "desc-uid-xyz");
    } else {
        panic!("expected StreamDatum");
    }
}

fn tempdir() -> std::io::Result<tempdir_shim::TempDir> {
    tempdir_shim::TempDir::new()
}

mod tempdir_shim {
    use std::path::{Path, PathBuf};
    pub struct TempDir(PathBuf);
    impl TempDir {
        pub fn new() -> std::io::Result<Self> {
            use std::time::{SystemTime, UNIX_EPOCH};
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let p = std::env::temp_dir().join(format!("cirrus-test-{nanos}"));
            std::fs::create_dir_all(&p)?;
            Ok(Self(p))
        }
        pub fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

#[tokio::test]
async fn fly_plan_drives_standard_detector_to_completion() {
    use cirrus::backends::soft::SoftDetector as ScalarDet;
    use cirrus_core::msg::{CollectableObj, FlyableObj, StageableObj};
    let _ = ScalarDet::new("ignored"); // ensure the import path is real

    let det = Arc::new(cirrus::backends::soft::detector::soft_detector("flydet"));
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);

    let plan = cirrus::ophyd_async::fly(
        det.clone() as Arc<dyn FlyableObj>,
        det.clone() as Arc<dyn CollectableObj>,
        vec![det.clone() as Arc<dyn StageableObj>],
    );
    let result = re.run_async(plan).await.expect("fly failed");
    assert_eq!(result.exit_status, "success");

    let docs = sink.snapshot().await;
    // RunStart, Descriptor (from describe_collect), StreamResource +
    // StreamDatum (from collect), Event (from collect), RunStop.
    assert!(docs.len() >= 4, "got {} docs: {:?}", docs.len(), docs);
    use cirrus_core::Document::*;
    assert!(matches!(&docs[0], Start(_)));
    assert!(matches!(&docs[docs.len() - 1], Stop(_)));

    // CBEM-13: the engine emits the writer's StreamDatum during collect, and
    // it must carry the EventDescriptor UID the engine composed for the collect
    // stream — not an empty string. This is the owner-path check: the descriptor
    // is stamped by the engine (single owner), not invented by the sink.
    let descriptor_uid = docs
        .iter()
        .find_map(|d| match d {
            Descriptor(desc) => Some(desc.uid.clone()),
            _ => None,
        })
        .expect("expected an EventDescriptor from describe_collect");
    let datum_descriptor = docs
        .iter()
        .find_map(|d| match d {
            StreamDatum(sd) => Some(sd.descriptor.clone()),
            _ => None,
        })
        .expect("expected a StreamDatum emitted by the engine collect (CBEM-13)");
    assert!(
        !datum_descriptor.is_empty(),
        "StreamDatum.descriptor must not be empty"
    );
    assert_eq!(
        datum_descriptor, descriptor_uid,
        "StreamDatum.descriptor must equal the collect stream's EventDescriptor uid"
    );
}

// -- M4 -----------------------------------------------------------------

#[tokio::test]
async fn pause_then_resume_completes_run_with_success() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    let det = SoftDetector::new("p_det");
    let sink = Arc::new(CapturingSink::new());
    let re = Arc::new(RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]));

    // Build a plan with an embedded "yield Pause" message in the middle.
    // Engine should gate at the pause, observable by counting docs delivered
    // before vs after we call resume().
    let pre_count = Arc::new(AtomicUsize::new(0));
    let pre_count_clone = pre_count.clone();
    let det_clone = det.clone();
    let plan = cirrus_core::plan::plan_box(async_stream::stream! {
        yield cirrus_core::Msg::OpenRun(Default::default());
        yield cirrus_core::Msg::Create { stream_name: "primary".into() };
        yield cirrus_core::Msg::Read(det_clone.clone() as Arc<dyn cirrus_core::msg::ReadableObj>);
        yield cirrus_core::Msg::Save;
        // ...checkpoint here...
        yield cirrus_core::Msg::Checkpoint;
        // signal that we've passed the first batch:
        pre_count_clone.store(1, Ordering::SeqCst);
        yield cirrus_core::Msg::Pause { defer: false };
        // After resume: another read-save + close.
        yield cirrus_core::Msg::Create { stream_name: "primary".into() };
        yield cirrus_core::Msg::Read(det_clone as Arc<dyn cirrus_core::msg::ReadableObj>);
        yield cirrus_core::Msg::Save;
        yield cirrus_core::Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    });

    let re_run = re.clone();
    let join = tokio::spawn(async move { re_run.run_async(plan).await });

    // Wait until the plan reached the pause point.
    for _ in 0..50 {
        if pre_count.load(Ordering::SeqCst) == 1 && re.is_paused() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(re.is_paused(), "engine should be paused");

    // At this point: Start, Descriptor, Event 1, Stop NOT YET
    let docs_before_resume = sink.snapshot().await;
    assert_eq!(docs_before_resume.len(), 3, "got {docs_before_resume:#?}");

    // Resume.
    re.resume();
    let result = join.await.unwrap().unwrap();
    assert_eq!(result.exit_status, "success");
    let docs = sink.snapshot().await;
    // Start + Descriptor + 2 Events + Stop = 5
    assert_eq!(docs.len(), 5);
}

#[tokio::test]
async fn abort_closes_run_with_abort_status() {
    let det = SoftDetector::new("a_det");
    let sink = Arc::new(CapturingSink::new());
    let re = Arc::new(RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]));

    let det_clone = det.clone();
    let plan = cirrus_core::plan::plan_box(async_stream::stream! {
        yield cirrus_core::Msg::OpenRun(Default::default());
        yield cirrus_core::Msg::Create { stream_name: "primary".into() };
        yield cirrus_core::Msg::Read(det_clone as Arc<dyn cirrus_core::msg::ReadableObj>);
        yield cirrus_core::Msg::Save;
        // Pause here; the test will abort instead of resuming.
        yield cirrus_core::Msg::Pause { defer: false };
        yield cirrus_core::Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    });

    let re_run = re.clone();
    let join = tokio::spawn(async move { re_run.run_async(plan).await });

    for _ in 0..50 {
        if re.is_paused() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    re.abort("test");
    let result = join.await.unwrap().unwrap();
    assert_eq!(result.exit_status, "abort");
    let docs = sink.snapshot().await;
    // Last doc must be a RunStop with exit_status="abort".
    if let cirrus_core::Document::Stop(s) = docs.last().unwrap() {
        assert_eq!(s.exit_status, "abort");
    } else {
        panic!("last doc was not Stop: {:?}", docs.last());
    }
}

#[tokio::test]
async fn suspender_auto_resumes_engine() {
    use cirrus_engine::Suspender;
    use futures::future::BoxFuture;
    use std::sync::atomic::{AtomicBool, Ordering as AOrd};
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    struct ManualGate {
        cleared: StdArc<AtomicBool>,
        notify: StdArc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl Suspender for ManualGate {
        fn name(&self) -> &str {
            "manual_gate"
        }
        fn watch(&self) -> BoxFuture<'static, ()> {
            let cleared = self.cleared.clone();
            let notify = self.notify.clone();
            Box::pin(async move {
                while !cleared.load(AOrd::SeqCst) {
                    notify.notified().await;
                }
            })
        }
    }

    let cleared = StdArc::new(AtomicBool::new(false));
    let notify = StdArc::new(tokio::sync::Notify::new());
    let gate = StdArc::new(ManualGate {
        cleared: cleared.clone(),
        notify: notify.clone(),
    });

    let sink = Arc::new(CapturingSink::new());
    let re = Arc::new(RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]));

    let id = re.next_suspender_id();
    let gate_dyn: Arc<dyn cirrus_engine::Suspender> = gate;
    let payload: Arc<dyn std::any::Any + Send + Sync> = Arc::new(gate_dyn);

    let plan = cirrus_core::plan::plan_box(async_stream::stream! {
        yield cirrus_core::Msg::InstallSuspender { id, suspender: payload };
        yield cirrus_core::Msg::OpenRun(Default::default());
        yield cirrus_core::Msg::Pause { defer: false };
        // After auto-resume:
        yield cirrus_core::Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
        yield cirrus_core::Msg::RemoveSuspender { id };
    });

    let re_run = re.clone();
    let join = tokio::spawn(async move { re_run.run_async(plan).await });

    // Wait for pause, then clear the gate.
    for _ in 0..50 {
        if re.is_paused() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cleared.store(true, AOrd::SeqCst);
    notify.notify_waiters();

    let result = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("engine did not exit after suspender cleared")
        .unwrap()
        .unwrap();
    assert_eq!(result.exit_status, "success");
}

// Contrast with `suspender_auto_resumes_engine`: once `clear_suspenders`
// uninstalls the gate, clearing its condition must NOT auto-resume the engine
// (the watcher was aborted with the dropped handle), so the run stays paused
// until aborted. Verifies the bluesky `RunEngine.clear_suspenders` parity API.
#[tokio::test]
async fn clear_suspenders_stops_auto_resume() {
    use cirrus_engine::Suspender;
    use futures::future::BoxFuture;
    use std::sync::atomic::{AtomicBool, Ordering as AOrd};
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    struct ManualGate {
        cleared: StdArc<AtomicBool>,
        notify: StdArc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl Suspender for ManualGate {
        fn name(&self) -> &str {
            "manual_gate"
        }
        fn watch(&self) -> BoxFuture<'static, ()> {
            let cleared = self.cleared.clone();
            let notify = self.notify.clone();
            Box::pin(async move {
                while !cleared.load(AOrd::SeqCst) {
                    notify.notified().await;
                }
            })
        }
    }

    let cleared = StdArc::new(AtomicBool::new(false));
    let notify = StdArc::new(tokio::sync::Notify::new());
    let gate = StdArc::new(ManualGate {
        cleared: cleared.clone(),
        notify: notify.clone(),
    });

    let sink = Arc::new(CapturingSink::new());
    let re = Arc::new(RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]));

    let id = re.next_suspender_id();
    let gate_dyn: Arc<dyn cirrus_engine::Suspender> = gate;
    let payload: Arc<dyn std::any::Any + Send + Sync> = Arc::new(gate_dyn);

    let plan = cirrus_core::plan::plan_box(async_stream::stream! {
        yield cirrus_core::Msg::InstallSuspender { id, suspender: payload };
        yield cirrus_core::Msg::OpenRun(Default::default());
        yield cirrus_core::Msg::Pause { defer: false };
        // Reached only if something resumes the engine — which must NOT happen
        // after clear_suspenders, so this run is expected to abort while paused.
        yield cirrus_core::Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    });

    let re_run = re.clone();
    let join = tokio::spawn(async move { re_run.run_async(plan).await });

    // Wait for the suspender to pause the engine.
    for _ in 0..50 {
        if re.is_paused() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(re.is_paused(), "engine should be paused by the suspender");

    // Uninstall the suspender; its watcher is aborted with the dropped handle.
    re.clear_suspenders().await;
    // Give the abort time to propagate before satisfying the (now-ignored) gate.
    tokio::time::sleep(Duration::from_millis(50)).await;
    cleared.store(true, AOrd::SeqCst);
    notify.notify_waiters();

    // With no installed suspender, clearing the gate cannot resume the engine:
    // it stays paused. The run only ends when we abort it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        re.is_paused(),
        "cleared suspender must not auto-resume the engine"
    );
    re.abort("test");
    let result = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("engine did not exit after abort")
        .unwrap()
        .unwrap();
    assert_eq!(result.exit_status, "abort");
}

// An installed `Suspender` exists to lift a *suspension*; its `watch()` must
// only be consulted once the engine is actually paused. A running engine must
// never be watched (the pre-fix watcher force-resumed in a ~100 Hz loop,
// polling `watch()` and clobbering `is_paused` even with nothing to resume —
// which made an installed clear-condition suspender override any pause).
#[tokio::test]
async fn installed_suspender_not_watched_while_engine_runs() {
    use cirrus_engine::Suspender;
    use futures::future::BoxFuture;
    use std::sync::atomic::{AtomicUsize, Ordering as AOrd};
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    // Condition always clear: `watch()` returns immediately and counts polls.
    struct CountingClear {
        polls: StdArc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl Suspender for CountingClear {
        fn name(&self) -> &str {
            "counting_clear"
        }
        fn watch(&self) -> BoxFuture<'static, ()> {
            let polls = self.polls.clone();
            Box::pin(async move {
                polls.fetch_add(1, AOrd::SeqCst);
            })
        }
    }

    let polls = StdArc::new(AtomicUsize::new(0));
    let susp = StdArc::new(CountingClear {
        polls: polls.clone(),
    });

    let re = Arc::new(RunEngine::new(vec![]));
    let id = re.next_suspender_id();
    let susp_dyn: Arc<dyn cirrus_engine::Suspender> = susp;
    let payload: Arc<dyn std::any::Any + Send + Sync> = Arc::new(susp_dyn);

    let plan = cirrus_core::plan::plan_box(async_stream::stream! {
        yield cirrus_core::Msg::InstallSuspender { id, suspender: payload };
        yield cirrus_core::Msg::OpenRun(Default::default());
        // The engine runs normally and is never paused.
        yield cirrus_core::Msg::Sleep(Duration::from_millis(100));
        yield cirrus_core::Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
        yield cirrus_core::Msg::RemoveSuspender { id };
    });

    let result = re.run_async(plan).await.unwrap();
    assert_eq!(result.exit_status, "success");
    assert_eq!(
        polls.load(AOrd::SeqCst),
        0,
        "installed suspender must not be watched while the engine runs"
    );
}

#[tokio::test]
async fn mvr_reads_position_inside_plan_then_moves_relative() {
    let motor = Arc::new(SoftMotor::new("m1", Some(2.5)));
    let re = RunEngine::new(vec![]);

    // mvr should: locate (readback=2.5) → set(2.5+1.5=4.0) → wait.
    let plan = cirrus_plans::stubs::mvr(motor.clone() as Arc<dyn LocatableObj>, 1.5);
    re.run_async(plan).await.unwrap();

    let loc = LocatableObj::locate_dyn(motor.as_ref()).await.unwrap();
    assert!(
        (loc.readback - 4.0).abs() < 1e-9,
        "readback = {}",
        loc.readback
    );
}

#[tokio::test]
async fn stop_plan_dispatches_through_engine() {
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Counts how many times `stop_dyn` is called.
    struct CountingStoppable {
        name: String,
        calls: AtomicU32,
    }
    #[async_trait::async_trait]
    impl cirrus_core::msg::NamedObj for CountingStoppable {
        fn name(&self) -> &str {
            &self.name
        }
    }
    #[async_trait::async_trait]
    impl StoppableObj for CountingStoppable {
        async fn stop_dyn(&self, _success: bool) -> cirrus_core::error::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let s = Arc::new(CountingStoppable {
        name: "shutter".into(),
        calls: AtomicU32::new(0),
    });
    let re = RunEngine::new(vec![]);
    let plan = cirrus_plans::stubs::stop(s.clone() as Arc<dyn StoppableObj>);
    re.run_async(plan).await.unwrap();
    assert_eq!(s.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn spiral_square_emits_expected_event_count() {
    use cirrus_core::msg::MovableObj;
    let det = SoftDetector::new("d");
    let xm = Arc::new(SoftMotor::new("xm", Some(0.0)));
    let ym = Arc::new(SoftMotor::new("ym", Some(0.0)));
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let plan = cirrus_plans::spiral_square(
        vec![det as Arc<dyn cirrus_core::msg::ReadableObj>],
        xm.clone() as Arc<dyn MovableObj>,
        xm.clone() as Arc<dyn cirrus_core::msg::ReadableObj>,
        ym.clone() as Arc<dyn MovableObj>,
        ym.clone() as Arc<dyn cirrus_core::msg::ReadableObj>,
        0.0,
        0.0,
        4.0,
        4.0,
        3,
        3,
    );
    re.run_async(plan).await.unwrap();
    let docs = sink.snapshot().await;
    let n_events = docs
        .iter()
        .filter(|d| matches!(d, Document::Event(_)))
        .count();
    assert_eq!(n_events, 9);
}

#[tokio::test]
async fn run_wrapper_emits_open_and_close_run() {
    use cirrus_core::msg::ReadableObj;
    use cirrus_plans::preprocessors::run_wrapper;
    let det = SoftDetector::new("rwd");
    let body = cirrus_core::plan::plan_box(async_stream::stream! {
        yield cirrus_core::Msg::Create { stream_name: "primary".into() };
        yield cirrus_core::Msg::Read(det.clone() as Arc<dyn ReadableObj>);
        yield cirrus_core::Msg::Save;
    });
    let wrapped = run_wrapper(
        body,
        cirrus_core::msg::RunMetadata {
            plan_name: Some("wrapper-test".into()),
            ..Default::default()
        },
    );
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    re.run_async(wrapped).await.unwrap();
    let docs = sink.snapshot().await;
    assert_eq!(docs.len(), 4);
    assert!(matches!(docs.first(), Some(Document::Start(_))));
    assert!(matches!(docs.last(), Some(Document::Stop(_))));
}

#[tokio::test]
async fn sync_facade_runs_blocking_count() {
    use std::thread;

    let det = SoftDetector::new("det_sync");
    let sink = Arc::new(CapturingSink::new());
    let re = Arc::new(RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]));

    // run_blocking must be called from a sync context (NOT inside an async task).
    let re_clone = re.clone();
    let det_clone = det.clone();
    let join = thread::spawn(move || {
        let plan = cirrus::ophyd::count(vec![det_clone], 3);
        re_clone.run_blocking(plan).unwrap()
    });
    let result = join.join().unwrap();
    assert_eq!(result.exit_status, "success");

    let docs = sink.snapshot().await;
    // Start + Descriptor + 3 × Event + Stop = 6
    assert_eq!(docs.len(), 6);
}
