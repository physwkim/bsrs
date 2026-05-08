//! End-to-end acceptance tests for cirrus.

use std::sync::Arc;

use cirrus::backends::soft::{SoftDetector, SoftMotor};
use cirrus::callbacks::CapturingSink;
use cirrus::prelude::*;

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

    // collect_stream_docs(3) should produce StreamResource + StreamDatum [0,3).
    let docs: Vec<_> = sink.collect_stream_docs(3).collect::<Vec<_>>().await;
    assert_eq!(docs.len(), 2);
    use cirrus::ophyd_async::StreamAsset;
    assert!(matches!(&docs[0], StreamAsset::Resource(_)));
    if let StreamAsset::Datum(d) = &docs[1] {
        assert_eq!(d.indices.start, 0);
        assert_eq!(d.indices.stop, 3);
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
    // RunStart, Descriptor (from describe_collect), Event (from collect),
    // RunStop  → 4 documents minimum.
    assert!(docs.len() >= 4, "got {} docs: {:?}", docs.len(), docs);
    use cirrus_core::Document::*;
    assert!(matches!(&docs[0], Start(_)));
    assert!(matches!(&docs[docs.len() - 1], Stop(_)));
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
