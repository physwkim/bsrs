//! Point detectors and the MovingDot camera of the mini-beamline example IOC,
//! checking what ophyd-async's demo point-detector and adcore data-logic tests
//! check. `cargo test` runs these against one shared IOC, so the tests that
//! drive the point detectors hold `PD_LOCK`.
#![cfg(all(feature = "ca", feature = "host"))]

mod example_ioc;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::callbacks::CapturingSink;
use bsrs::core::msg::{MonitorableObj, ReadableObj};
use bsrs::event_model::{Dtype, DtypeNumpy};
use bsrs::host::areadetector::{
    area_detector_hdf, connect_area_detector_hdf, NdFile, StaticPathProvider,
};
use bsrs::host::ca_devices::CaDetector;
use bsrs::prelude::*;
use bsrs::protocols_async::{
    DetectorControl, SignalBackend, Stageable, StreamAsset, Triggerable, WritesStreamAssets,
};
use futures::StreamExt;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(10);
/// The beam current every point detector is held at.
const CURRENT: f64 = 500.0;
/// `(detector, ExposureTime, DetSigma, DetCenter, DetValue_RBV)` with the
/// motor held at 0: the pinhole's Gaussian and the edge's erfc are at their
/// centre (factor 1 and 1/2), the slit's centre is far enough out that its
/// erfc difference is 2. Each value is `200 * CURRENT * exposure * factor`
/// (`physics.rs:72`), exact in binary.
const POINT_DETECTORS: [(&str, f64, f64, f64, f64); 3] = [
    ("ph", 0.5, 5.0, 0.0, 50000.0),
    ("edge", 0.25, 2.5, 0.0, 12500.0),
    ("slit", 0.0625, 2.5, 100.0, 6250.0),
];

static PD_LOCK: Mutex<()> = Mutex::const_new(());

fn boot() {
    example_ioc::boot("mini-ioc", "mini-beamline");
}

async fn put<V>(pv: &str, v: V)
where
    V: Clone + Send + Sync + 'static,
    EpicsCaBackend<V>: SignalBackend<V>,
{
    let ch = EpicsCaBackend::<V>::new(pv);
    ch.connect(TIMEOUT)
        .await
        .unwrap_or_else(|e| panic!("connect {pv}: {e}"));
    timeout(TIMEOUT, SignalBackend::put(&ch, Some(v)))
        .await
        .unwrap_or_else(|_| panic!("put {pv} timed out"))
        .unwrap_or_else(|e| panic!("put {pv}: {e}"));
}

/// Cut the CP link `{pv}` takes from the motor or the beam current and hold
/// it at `value`, so the point detector computes from fixed inputs.
async fn hold(pv: &str, value: f64) {
    put::<String>(&format!("{pv}.OMSL"), "supervisory".into()).await;
    put::<f64>(pv, value).await;
}

/// Hold every point detector's inputs, then wait until each has posted the
/// value they give.
async fn ready_point_detectors() -> Vec<Arc<CaDetector>> {
    let mut dets = Vec::new();
    for (name, exposure, sigma, center, expected) in POINT_DETECTORS {
        let p = format!("mini:{name}:");
        hold(&format!("{p}BeamCurrentLink"), CURRENT).await;
        hold(&format!("{p}MotorPosLink"), 0.0).await;
        put::<f64>(&format!("{p}DetSigma"), sigma).await;
        put::<f64>(&format!("{p}DetCenter"), center).await;
        put::<f64>(&format!("{p}ExposureTime"), exposure).await;
        let det = CaDetector::connect_async(name, &format!("{p}DetValue_RBV"))
            .await
            .unwrap_or_else(|e| panic!("connect {name}: {e}"));
        let sub = MonitorableObj::subscribe_dyn(det.as_ref())
            .await
            .expect("subscribe");
        let mut rx = sub.clone_rx();
        let reached = timeout(TIMEOUT, rx.wait_for(|r| r.value == json!(expected)))
            .await
            .is_ok();
        let last = sub.rx().borrow().value.clone();
        assert!(
            reached,
            "{name}: DetValue_RBV {last} never reached {expected}"
        );
        dets.push(det);
    }
    dets
}

/// ophyd-async `test_epics_demo.py:265` `test_point_detector_read_and_describe`
/// (and `:215`): each point detector describes its value PV as a scalar
/// number sourced from it, with the record's display metadata, and reads the
/// value the IOC computed with a timestamp and no alarm.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn point_detectors_read_and_describe_their_value_pv() {
    boot();
    let _pd = PD_LOCK.lock().await;
    let dets = ready_point_detectors().await;
    for (det, (name, .., expected)) in dets.iter().zip(POINT_DETECTORS) {
        let keys = ReadableObj::describe_dyn(det.as_ref())
            .await
            .expect("describe");
        assert_eq!(keys.len(), 1, "{keys:?}");
        let key = &keys[name];
        assert_eq!(key.source, format!("ca://mini:{name}:DetValue_RBV"));
        assert_eq!(key.dtype, Dtype::Number);
        assert!(key.shape.is_empty(), "{:?}", key.shape);
        assert_eq!(
            key.dtype_numpy,
            Some(DtypeNumpy::Scalar("<f8".into())),
            "{name}"
        );
        assert_eq!(key.precision, Some(2), "{name}");
        assert_eq!(key.units.as_deref(), Some(""), "{name}");

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let reading = ReadableObj::read_dyn(det.as_ref()).await.expect("read");
        assert_eq!(reading.len(), 1, "{reading:?}");
        let r = &reading[name];
        assert_eq!(r.value, json!(expected), "{name}");
        assert_eq!(r.alarm_severity, Some(0), "{name}");
        assert!(
            r.timestamp > now - 60.0 && r.timestamp <= now + 1.0,
            "{name}: timestamp {} vs now {now}",
            r.timestamp
        );
    }
}

/// ophyd-async `test_epics_demo.py:232` `test_point_detector_in_plan`:
/// `count` over the point detectors emits one start, descriptor and stop and
/// one event per shot, each carrying every detector's value.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn count_reads_every_point_detector_into_each_event() {
    boot();
    let _pd = PD_LOCK.lock().await;
    let dets = ready_point_detectors().await;
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let plan = bsrs::plans::count(
        dets.iter()
            .map(|d| d.clone() as Arc<dyn ReadableObj>)
            .collect(),
        2,
    );
    let result = timeout(Duration::from_secs(30), re.run_async(plan))
        .await
        .expect("run not done within 30 s")
        .expect("run");
    assert_eq!(result.exit_status, "success");

    let docs = sink.snapshot().await;
    let mut counts: HashMap<&str, usize> = HashMap::new();
    let mut events = Vec::new();
    for d in &docs {
        let kind = match d {
            Document::Start(_) => "start",
            Document::Descriptor(_) => "descriptor",
            Document::Event(e) => {
                events.push(e.clone());
                "event"
            }
            Document::Stop(_) => "stop",
            _ => "other",
        };
        *counts.entry(kind).or_default() += 1;
    }
    assert_eq!(
        counts,
        HashMap::from([("start", 1), ("descriptor", 1), ("event", 2), ("stop", 1)])
    );
    let expected: HashMap<String, serde_json::Value> = POINT_DETECTORS
        .iter()
        .map(|(name, .., v)| (name.to_string(), json!(v)))
        .collect();
    for e in &events {
        assert_eq!(e.data, expected, "event {}", e.seq_num);
    }
}

/// ophyd-async `test_data_logic.py:232` `test_describe_different_color_modes`
/// (Mono): the MovingDot makes UInt16 frames, binned 2x2 here, and the image
/// key, resource and file take their shape and dtype from the driver's
/// readbacks, whatever autosave restored (Continuous/100 by default).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn moving_dot_frames_are_described_from_the_driver_readbacks() {
    boot();
    let dir = tempfile::tempdir().unwrap();
    let det = area_detector_hdf(
        "dot",
        "mini:dot:cam1:",
        "mini:dot:HDF1:",
        StaticPathProvider::new(dir.path().to_str().unwrap(), "dot"),
    );
    connect_area_detector_hdf(&det, TIMEOUT)
        .await
        .expect("connect");
    let cam = det.control();
    DetectorControl::disarm(cam).await.expect("disarm");
    for plugin in ["HDF1:", "TIFF1:", "JPEG1:"] {
        let f = NdFile::new(format!("mini:dot:{plugin}"));
        f.connect(TIMEOUT).await.expect("connect file plugin");
        f.stop_capture().await.expect("stop capture");
        let feed = plugin == "HDF1:";
        if feed {
            f.plugin.set_source_port("DOT").await.expect("NDArrayPort");
            f.plugin
                .set_blocking(true)
                .await
                .expect("BlockingCallbacks");
        }
        f.plugin.set_enabled(feed).await.expect("EnableCallbacks");
    }
    for (suffix, v) in [
        ("ImageMode", 0),
        ("NumImages", 1),
        ("NumExposures", 1),
        ("ColorMode", 0),
        ("DataType", 3),
        ("ArrayCallbacks", 1),
        ("BinX", 2),
        ("BinY", 2),
        ("MinX", 0),
        ("MinY", 0),
        ("SizeX", 64),
        ("SizeY", 48),
    ] {
        put::<i64>(&format!("mini:dot:cam1:{suffix}"), v).await;
    }
    put::<f64>("mini:dot:cam1:AcquireTime", 0.01).await;
    put::<f64>("mini:dot:cam1:AcquirePeriod", 0.01).await;
    put::<i64>("mini:dot:HDF1:NumFramesChunks", 1).await;
    timeout(
        TIMEOUT,
        SignalBackend::put(cam.nd_attributes_file.as_ref(), Some(String::new())),
    )
    .await
    .expect("NDAttributesFile put timed out")
    .expect("clear NDAttributesFile");
    cam.warmup().await.expect("warmup");

    Stageable::stage(&det).await.expect("stage");
    let status = timeout(TIMEOUT, Triggerable::trigger(&det))
        .await
        .expect("trigger started");
    timeout(TIMEOUT, status)
        .await
        .expect("trigger not done")
        .expect("trigger");
    let keys = ReadableObj::describe_dyn(&det).await.expect("describe");
    let image = &keys["dot_image"];
    let h5 = dir.path().join("dot.h5");
    let uri = format!("file://localhost{}", h5.display());
    assert_eq!(image.dtype, Dtype::Array);
    assert_eq!(image.shape, vec![Some(1), Some(24), Some(32)]);
    assert_eq!(image.dtype_numpy, Some(DtypeNumpy::Scalar("<u2".into())));
    assert_eq!(image.source, uri);

    let up_to = det.get_index().await.expect("index");
    assert_eq!(up_to, 1);
    let mut docs = det.collect_asset_docs(up_to, "");
    let mut resources = Vec::new();
    let mut datums = Vec::new();
    while let Some(doc) = docs.next().await {
        match doc {
            StreamAsset::Resource(r) => resources.push(r),
            StreamAsset::Datum(d) => datums.push(d),
        }
    }
    drop(docs);
    assert_eq!(resources.len(), 1, "{resources:?}");
    assert_eq!(resources[0].uri, uri);
    assert_eq!(resources[0].parameters["chunk_shape"], json!([1, 24, 32]));
    assert_eq!(datums.len(), 1, "{datums:?}");
    assert_eq!((datums[0].indices.start, datums[0].indices.stop), (0, 1));
    Stageable::unstage(&det).await.expect("unstage");

    #[cfg(feature = "hdf5")]
    {
        let file = rust_hdf5::H5File::open(&h5).expect("open the IOC's .h5");
        let data = file.dataset("entry/data/data").expect("entry/data/data");
        assert_eq!(data.shape(), vec![1, 24, 32]);
    }
}
