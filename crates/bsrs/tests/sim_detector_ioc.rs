//! `StandardDetector` against the sim-detector example IOC's real
//! areaDetector records (`SIM1:cam1:` and its commonPlugins), checking what
//! ophyd-async's detector tests check. `cargo test` runs these against one
//! shared IOC, so every test that drives the cam or reconfigures its plugins
//! holds `CAM_LOCK`.
#![cfg(all(feature = "ca", feature = "host"))]

mod example_ioc;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::callbacks::CapturingSink;
use bsrs::core::msg::{ConfigurableObj, ReadableObj, StageableObj, TriggerableObj};
use bsrs::core::status::Status;
use bsrs::devices::{
    BackendFromPv, DataLogic, SignalConfig, SignalR, StandardDetector, TriggerInfo,
};
use bsrs::event_model::{DataKey, Dtype, DtypeNumpy, StreamDatum, StreamResource};
use bsrs::host::areadetector::{
    area_detector_hdf, connect_area_detector_hdf, num_rois, AdHdfWriter, AdMultipartWriter,
    AreaDetectorCam, AreaDetectorHdf, NdArrayDescription, NdFile, NdFileHdf5, NdFileIo,
    NdPluginFileIo, NdRoi, NdStats, PluginSignalDataLogic, StaticPathProvider, AD_COLOR_MODE_MONO,
    AD_COLOR_MODE_RGB1, AD_IMAGE_MODE_CONTINUOUS, AD_IMAGE_MODE_MULTIPLE, AD_IMAGE_MODE_SINGLE,
};
use bsrs::prelude::*;
use bsrs::protocols_async::{
    DetectorControl, DetectorWriter, Flyable, Preparable, SignalBackend, Stageable, StreamAsset,
    Triggerable, WritesStreamAssets,
};
use futures::StreamExt;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::timeout;

const CAM: &str = "SIM1:cam1:";
const PORT: &str = "SIM1";
const HDF: &str = "SIM1:HDF1:";
const TIFF: &str = "SIM1:TIFF1:";
const JPEG: &str = "SIM1:JPEG1:";
const TIMEOUT: Duration = Duration::from_secs(10);
const EXPOSURE: f64 = 0.01;
const WIDTH: i64 = 64;
const HEIGHT: i64 = 48;

static CAM_LOCK: Mutex<()> = Mutex::const_new(());

fn boot() {
    example_ioc::boot("sim-ioc", "sim-detector");
}

async fn channel<V>(pv: &str) -> EpicsCaBackend<V>
where
    V: Clone + Send + Sync + 'static,
    EpicsCaBackend<V>: SignalBackend<V> + BackendFromPv,
{
    let ch = EpicsCaBackend::<V>::from_pv(pv);
    ch.connect(TIMEOUT)
        .await
        .unwrap_or_else(|e| panic!("connect {pv}: {e}"));
    ch
}

async fn put<V>(pv: &str, v: V)
where
    V: Clone + Send + Sync + 'static,
    EpicsCaBackend<V>: SignalBackend<V> + BackendFromPv,
{
    let ch = channel::<V>(pv).await;
    timeout(TIMEOUT, SignalBackend::put(&ch, Some(v)))
        .await
        .unwrap_or_else(|_| panic!("put {pv} timed out"))
        .unwrap_or_else(|e| panic!("put {pv}: {e}"));
}

async fn get<V>(pv: &str) -> V
where
    V: Clone + Send + Sync + 'static,
    EpicsCaBackend<V>: SignalBackend<V> + BackendFromPv,
{
    let ch = channel::<V>(pv).await;
    SignalBackend::get_value(&ch)
        .await
        .unwrap_or_else(|e| panic!("get {pv}: {e}"))
}

async fn file_plugin(prefix: &str) -> NdFile {
    let f = NdFile::new(prefix);
    f.connect(TIMEOUT).await.expect("connect file plugin");
    f
}

/// Bring the cam to a known state whatever autosave or an earlier test left:
/// idle, Single/1, small mono UInt8 frames, no NDAttributes, the file plugin
/// `feed` fed from the cam with blocking callbacks and every other one off,
/// then one warmup frame so the ArraySize readbacks describe the frame and
/// `feed` holds an array, which a capture in Stream mode needs.
async fn ready_cam(cam: &AreaDetectorCam, feed: &str) {
    DetectorControl::disarm(cam).await.expect("disarm");
    for prefix in [HDF, TIFF, JPEG] {
        let f = file_plugin(prefix).await;
        f.stop_capture().await.expect("stop capture");
        if prefix == feed {
            f.plugin.set_source_port(PORT).await.expect("NDArrayPort");
            f.plugin
                .set_blocking(true)
                .await
                .expect("BlockingCallbacks");
        }
        f.plugin
            .set_enabled(prefix == feed)
            .await
            .expect("EnableCallbacks");
    }
    for (suffix, v) in [
        ("NumImages", 1),
        ("NumExposures", 1),
        ("BinX", 1),
        ("BinY", 1),
        ("MinX", 0),
        ("MinY", 0),
        ("SizeX", WIDTH),
        ("SizeY", HEIGHT),
    ] {
        put::<i64>(&format!("{CAM}{suffix}"), v).await;
    }
    for (suffix, label) in [
        ("ImageMode", AD_IMAGE_MODE_SINGLE),
        ("ColorMode", AD_COLOR_MODE_MONO),
        ("DataType", "UInt8"),
    ] {
        put::<String>(&format!("{CAM}{suffix}"), label.into()).await;
    }
    put::<bool>(&format!("{CAM}ArrayCallbacks"), true).await;
    put::<f64>(&format!("{CAM}AcquireTime"), EXPOSURE).await;
    put::<f64>(&format!("{CAM}AcquirePeriod"), EXPOSURE).await;
    timeout(
        TIMEOUT,
        SignalBackend::put(cam.nd_attributes_file.as_ref(), Some(String::new())),
    )
    .await
    .expect("NDAttributesFile put timed out")
    .expect("clear NDAttributesFile");
    cam.warmup().await.expect("warmup");
}

fn provider(dir: &Path, filename: &str) -> StaticPathProvider {
    StaticPathProvider::new(dir.to_str().expect("utf-8 tempdir"), filename)
}

async fn hdf_det(name: &str, dir: &Path, filename: &str) -> AreaDetectorHdf {
    let det = area_detector_hdf(name, CAM, HDF, provider(dir, filename));
    connect_area_detector_hdf(&det, TIMEOUT)
        .await
        .expect("connect");
    ready_cam(det.control(), HDF).await;
    put::<i64>(&format!("{HDF}NumFramesChunks"), 1).await;
    det
}

async fn finish(status: Status, what: &str) -> Result<(), String> {
    match timeout(TIMEOUT, status).await {
        Ok(r) => r.map_err(|e| format!("{what}: {e:?}")),
        Err(_) => Err(format!("{what}: not done within {TIMEOUT:?}")),
    }
}

async fn trigger(det: &dyn Triggerable) -> Result<(), String> {
    match timeout(TIMEOUT, Triggerable::trigger(det)).await {
        Ok(status) => finish(status, "trigger").await,
        Err(_) => Err(format!("trigger: not started within {TIMEOUT:?}")),
    }
}

async fn index(det: &dyn WritesStreamAssets) -> u64 {
    det.get_index().await.expect("index")
}

async fn drain(det: &dyn WritesStreamAssets) -> (Vec<StreamResource>, Vec<StreamDatum>) {
    let up_to = index(det).await;
    let mut resources = Vec::new();
    let mut datums = Vec::new();
    let mut docs = det.collect_asset_docs(up_to, "");
    while let Some(doc) = docs.next().await {
        match doc {
            StreamAsset::Resource(r) => resources.push(r),
            StreamAsset::Datum(d) => datums.push(d),
        }
    }
    (resources, datums)
}

fn file_uri(path: &Path) -> String {
    format!("file://localhost{}", path.display())
}

fn numpy(key: &DataKey) -> Option<&str> {
    match key.dtype_numpy.as_ref() {
        Some(DtypeNumpy::Scalar(s)) => Some(s),
        _ => None,
    }
}

fn shape(dims: &[u64]) -> Vec<Option<u64>> {
    dims.iter().copied().map(Some).collect()
}

#[cfg(feature = "hdf5")]
fn h5_shape(path: &Path, dataset: &str) -> Vec<usize> {
    let file = rust_hdf5::H5File::open(path).expect("open the IOC's .h5");
    file.dataset(dataset)
        .unwrap_or_else(|e| panic!("{dataset} in {}: {e}", path.display()))
        .shape()
}

/// ophyd-async `test_use_cases.py:17` `test_step_scan_hdf_detector_with_stats_and_temp`
/// and `test_adsim_system.py:161` `test_software_triggering`: describe needs
/// a prepare or trigger, the image key points at the file, one resource per
/// staging, one datum per trigger.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn hdf_step_scan_points_its_documents_at_the_file_the_ioc_wrote() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "step").await;
    let h5 = dir.path().join("step.h5");

    Stageable::stage(&det).await.expect("stage");
    let err = ReadableObj::describe_dyn(&det)
        .await
        .expect_err("describe before prepare or trigger");
    assert!(err.to_string().contains("Prepare not run"), "{err}");

    trigger(&det).await.unwrap();
    let keys = ReadableObj::describe_dyn(&det).await.expect("describe");
    let image = &keys["det_image"];
    assert_eq!(image.shape, shape(&[1, HEIGHT as u64, WIDTH as u64]));
    assert_eq!(image.dtype, Dtype::Array);
    assert_eq!(numpy(image), Some("|u1"));
    assert_eq!(image.external.as_deref(), Some("STREAM:"));
    assert_eq!(image.source, file_uri(&h5));
    assert_eq!(index(&det).await, 1);
    let (resources, datums) = drain(&det).await;
    assert_eq!(resources.len(), 1, "{resources:?}");
    let r = &resources[0];
    assert_eq!(r.data_key, "det_image");
    assert_eq!(r.mimetype, "application/x-hdf5");
    assert_eq!(r.uri, file_uri(&h5));
    assert_eq!(r.parameters["dataset"], json!("/entry/data/data"));
    assert_eq!(r.parameters["chunk_shape"], json!([1, HEIGHT, WIDTH]));
    assert_eq!(datums.len(), 1, "{datums:?}");
    assert_eq!(datums[0].stream_resource, r.uid);
    assert_eq!((datums[0].indices.start, datums[0].indices.stop), (0, 1));

    trigger(&det).await.unwrap();
    let (resources, datums) = drain(&det).await;
    assert!(resources.is_empty(), "{resources:?}");
    assert_eq!(datums.len(), 1, "{datums:?}");
    assert_eq!(datums[0].stream_resource, r.uid);
    assert_eq!((datums[0].indices.start, datums[0].indices.stop), (1, 2));

    Stageable::unstage(&det).await.expect("unstage");
    assert!(h5.exists(), "{} was not written", h5.display());
    #[cfg(feature = "hdf5")]
    assert_eq!(
        h5_shape(&h5, "entry/data/data"),
        vec![2, HEIGHT as usize, WIDTH as usize]
    );
}

/// ophyd-async `test_use_cases.py:17`: every NDAttribute named in the cam's
/// inline NDAttributesFile becomes a scalar external key with its own
/// resource and datum in the same file; an EPICS_PV attribute's source is
/// its PV, a PARAM attribute's the file.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn hdf_ndattributes_get_a_key_resource_and_datum_each() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "attrs").await;
    let h5 = dir.path().join("attrs.h5");
    // NDAttributesFile is a 256-element CHAR waveform.
    let xml = format!(
        "<Attributes>\
         <Attribute name=\"AcqTime\" type=\"PARAM\" source=\"ACQ_TIME\" datatype=\"DOUBLE\"/>\
         <Attribute name=\"Temp\" type=\"EPICS_PV\" source=\"{CAM}Temperature_RBV\" dbrtype=\"DBR_DOUBLE\"/>\
         </Attributes>"
    );
    assert!(xml.len() < 256);
    let attrs_file = EpicsCaBackend::<String>::new(format!("{CAM}NDAttributesFile"));
    attrs_file.connect(TIMEOUT).await.unwrap();
    timeout(TIMEOUT, SignalBackend::put(&attrs_file, Some(xml)))
        .await
        .unwrap()
        .unwrap();

    Stageable::stage(&det).await.expect("stage");
    trigger(&det).await.unwrap();
    let keys = ReadableObj::describe_dyn(&det).await.expect("describe");
    let names: HashSet<&str> = keys.keys().map(String::as_str).collect();
    assert_eq!(names, HashSet::from(["det_image", "AcqTime", "Temp"]));
    for (name, source) in [
        ("AcqTime", file_uri(&h5)),
        ("Temp", format!("ca://{CAM}Temperature_RBV")),
    ] {
        let k = &keys[name];
        assert_eq!(k.source, source, "{name}");
        assert_eq!(k.shape, shape(&[1]), "{name}");
        assert_eq!(k.dtype, Dtype::Number, "{name}");
        assert_eq!(numpy(k), Some("<f8"), "{name}");
        assert_eq!(k.external.as_deref(), Some("STREAM:"), "{name}");
    }

    let (resources, datums) = drain(&det).await;
    Stageable::unstage(&det).await.expect("unstage");
    timeout(
        TIMEOUT,
        SignalBackend::put(&attrs_file, Some(String::new())),
    )
    .await
    .unwrap()
    .unwrap();

    let by_key: HashMap<&str, &StreamResource> =
        resources.iter().map(|r| (r.data_key.as_str(), r)).collect();
    assert_eq!(resources.len(), 3, "{resources:?}");
    for name in ["AcqTime", "Temp"] {
        let r = by_key[name];
        assert_eq!(r.uri, file_uri(&h5));
        assert_eq!(
            r.parameters["dataset"],
            json!(format!("/entry/instrument/NDAttributes/{name}"))
        );
        assert_eq!(r.parameters["chunk_shape"], json!([16384]));
    }
    assert_eq!(datums.len(), 3, "{datums:?}");
    let datum_resources: HashSet<&str> =
        datums.iter().map(|d| d.stream_resource.as_str()).collect();
    let resource_uids: HashSet<&str> = resources.iter().map(|r| r.uid.as_str()).collect();
    assert_eq!(datum_resources, resource_uids);
    assert!(datums
        .iter()
        .all(|d| (d.indices.start, d.indices.stop) == (0, 1)));
    #[cfg(feature = "hdf5")]
    for name in ["AcqTime", "Temp"] {
        assert_eq!(
            h5_shape(&h5, &format!("entry/instrument/NDAttributes/{name}")),
            vec![1],
            "{name}"
        );
    }
}

/// ophyd-async `test_use_cases.py:266` `test_flyscan_aravis_detector` and
/// `test_adsimdetector.py:43` `test_prepare_internal`: kickoff applies the
/// prepared exposure plan, complete waits for that many frames, and a second
/// prepare/kickoff continues the index.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn fly_scan_takes_the_prepared_frames_and_continues_the_index() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "fly").await;
    let h5 = dir.path().join("fly.h5");
    Stageable::stage(&det).await.expect("stage");

    let info = |n| TriggerInfo {
        number_of_events: n,
        livetime: Some(Duration::from_millis(20)),
        deadtime: Some(Duration::from_millis(30)),
        ..TriggerInfo::default()
    };
    finish(Preparable::prepare(&det, info(5)).await, "prepare")
        .await
        .unwrap();
    finish(Flyable::kickoff(&det).await, "kickoff")
        .await
        .unwrap();
    assert_eq!(
        get::<String>(&format!("{CAM}ImageMode")).await,
        AD_IMAGE_MODE_MULTIPLE
    );
    assert_eq!(get::<i64>(&format!("{CAM}NumImages")).await, 5);
    assert_eq!(get::<f64>(&format!("{CAM}AcquireTime")).await, 0.02);
    assert!((get::<f64>(&format!("{CAM}AcquirePeriod")).await - 0.05).abs() < 1e-9);
    finish(Flyable::complete(&det).await, "complete")
        .await
        .unwrap();
    assert_eq!(index(&det).await, 5);
    let (resources, datums) = drain(&det).await;
    assert_eq!(resources.len(), 1);
    assert_eq!((datums[0].indices.start, datums[0].indices.stop), (0, 5));

    finish(Preparable::prepare(&det, info(3)).await, "prepare")
        .await
        .unwrap();
    finish(Flyable::kickoff(&det).await, "kickoff")
        .await
        .unwrap();
    finish(Flyable::complete(&det).await, "complete")
        .await
        .unwrap();
    assert_eq!(index(&det).await, 8);
    let (resources, datums) = drain(&det).await;
    assert!(resources.is_empty());
    assert_eq!((datums[0].indices.start, datums[0].indices.stop), (5, 8));
    Stageable::unstage(&det).await.expect("unstage");
    #[cfg(feature = "hdf5")]
    assert_eq!(
        h5_shape(&h5, "entry/data/data"),
        vec![8, HEIGHT as usize, WIDTH as usize]
    );
    #[cfg(not(feature = "hdf5"))]
    assert!(h5.exists());
}

/// ophyd-async `test_acquire_logic.py:95`
/// `test_start_acquiring_driver_and_ensure_status_timing` and
/// `_detector.py:677` `kickoff`: kickoff's status is done once the detector
/// is acquiring, not when the acquisition ends.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn kickoff_resolves_once_the_detector_is_acquiring() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "kickoff").await;
    Stageable::stage(&det).await.expect("stage");
    let info = TriggerInfo {
        number_of_events: 5,
        livetime: Some(Duration::from_millis(200)),
        deadtime: Some(Duration::ZERO),
        ..TriggerInfo::default()
    };
    finish(Preparable::prepare(&det, info).await, "prepare")
        .await
        .unwrap();
    // The call and its status: kickoff reads the index without a flush.
    let early = timeout(Duration::from_millis(500), async {
        Flyable::kickoff(&det).await.await
    })
    .await;
    let acquiring = get::<bool>(&format!("{CAM}Acquire_RBV")).await;
    let completed = finish(Flyable::complete(&det).await, "complete").await;
    Stageable::unstage(&det).await.expect("unstage");
    assert!(
        matches!(early, Ok(Ok(()))),
        "kickoff of a 1 s acquisition not done after 500 ms: {early:?}"
    );
    assert!(acquiring, "the 1 s acquisition ended within 500 ms");
    completed.unwrap();
}

/// ophyd-async `_detector.py:616` `trigger` prepares `TriggerInfo()` when
/// nothing was prepared since stage, so a trigger takes exactly one frame
/// whatever image mode the cam was left in.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn trigger_takes_one_frame_after_a_continuous_acquisition() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "cont").await;
    Stageable::stage(&det).await.expect("stage");
    put::<String>(&format!("{CAM}ImageMode"), AD_IMAGE_MODE_CONTINUOUS.into()).await;
    let triggered = timeout(Duration::from_secs(5), trigger(&det)).await;
    DetectorControl::disarm(det.control())
        .await
        .expect("disarm");
    let n = index(&det).await;
    Stageable::unstage(&det).await.expect("unstage");
    assert!(
        matches!(triggered, Ok(Ok(()))),
        "trigger left in Continuous: {triggered:?}"
    );
    assert_eq!(n, 1);
}

/// As above with the cam left at Multiple/3 by an earlier fly scan.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn trigger_takes_one_frame_after_a_multi_image_acquisition() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "multi").await;
    Stageable::stage(&det).await.expect("stage");
    put::<String>(&format!("{CAM}ImageMode"), AD_IMAGE_MODE_MULTIPLE.into()).await;
    put::<i64>(&format!("{CAM}NumImages"), 3).await;
    let triggered = trigger(&det).await;
    let n = index(&det).await;
    Stageable::unstage(&det).await.expect("unstage");
    triggered.unwrap();
    assert_eq!(n, 1);
}

/// ophyd-async `test_standard_detector.py:426`
/// `test_trigger_after_multi_event_prepare_raises`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn trigger_after_a_multi_event_prepare_fails() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "multiprep").await;
    Stageable::stage(&det).await.expect("stage");
    let info = TriggerInfo {
        number_of_events: 5,
        ..TriggerInfo::default()
    };
    finish(Preparable::prepare(&det, info).await, "prepare")
        .await
        .unwrap();
    let triggered = trigger(&det).await;
    Stageable::unstage(&det).await.expect("unstage");
    let err = triggered.expect_err("trigger after prepare(number_of_events=5) succeeded");
    assert!(
        err.contains("trigger() is not supported for multiple events")
            && err.contains("number_of_events=5."),
        "{err}"
    );
}

/// ophyd-async `test_use_cases.py:191`: a trigger whose frame never reaches
/// the file times out after `exposure_timeout` instead of reporting a frame
/// that is not there.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn trigger_fails_when_the_writer_does_not_advance() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "stall").await;
    Stageable::stage(&det).await.expect("stage");
    trigger(&det).await.unwrap();
    assert_eq!(index(&det).await, 1);
    // The plugin stops consuming frames; the cam still acquires.
    file_plugin(HDF)
        .await
        .plugin
        .set_enabled(false)
        .await
        .unwrap();
    let info = TriggerInfo {
        exposure_timeout: Some(Duration::from_millis(500)),
        ..TriggerInfo::default()
    };
    finish(Preparable::prepare(&det, info).await, "prepare")
        .await
        .unwrap();
    let triggered = trigger(&det).await;
    let n = index(&det).await;
    Stageable::unstage(&det).await.expect("unstage");
    assert_eq!(n, 1);
    let err = triggered.expect_err("trigger succeeded though the writer index stayed at 1");
    assert!(err.contains("timeout after 500ms"), "{err}");
}

/// ophyd-async `test_standard_detector.py:574` `test_stage_resets_state`: a
/// fresh stage forgets the previous scan's prepare, so a bare kickoff takes
/// one frame.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn stage_forgets_the_previous_prepare() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "restage").await;
    Stageable::stage(&det).await.expect("stage");
    let info = TriggerInfo {
        number_of_events: 3,
        ..TriggerInfo::default()
    };
    finish(Preparable::prepare(&det, info).await, "prepare")
        .await
        .unwrap();
    finish(Flyable::kickoff(&det).await, "kickoff")
        .await
        .unwrap();
    finish(Flyable::complete(&det).await, "complete")
        .await
        .unwrap();
    Stageable::unstage(&det).await.expect("unstage");

    Stageable::stage(&det).await.expect("restage");
    finish(Flyable::kickoff(&det).await, "kickoff")
        .await
        .unwrap();
    finish(Flyable::complete(&det).await, "complete")
        .await
        .unwrap();
    let num_images = get::<i64>(&format!("{CAM}NumImages")).await;
    let n = index(&det).await;
    Stageable::unstage(&det).await.expect("unstage");
    assert_eq!(num_images, 1);
    assert_eq!(n, 1);
}

/// ophyd-async `test_standard_detector.py:543`
/// `test_kickoff_respects_prepare_bounds`: a second kickoff without a new
/// prepare asks for frames beyond the prepared ones.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn kickoff_beyond_the_prepared_frames_fails() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "bounds").await;
    Stageable::stage(&det).await.expect("stage");
    let info = TriggerInfo {
        number_of_events: 2,
        ..TriggerInfo::default()
    };
    finish(Preparable::prepare(&det, info).await, "prepare")
        .await
        .unwrap();
    finish(Flyable::kickoff(&det).await, "kickoff")
        .await
        .unwrap();
    finish(Flyable::complete(&det).await, "complete")
        .await
        .unwrap();
    let again = finish(Flyable::kickoff(&det).await, "kickoff").await;
    if again.is_ok() {
        let _ = finish(Flyable::complete(&det).await, "complete").await;
    }
    let n = index(&det).await;
    Stageable::unstage(&det).await.expect("unstage");
    let err = again.expect_err(&format!("second kickoff took {} more frames", n - 2));
    assert!(
        err.contains("Kickoff requested 2:4, but detector was only prepared up to 2"),
        "{err}"
    );
}

/// ophyd-async `test_use_cases.py:266` `test_flyscan_aravis_detector`
/// (`collections_per_event=3`) and `_detector.py:466`
/// `_update_prepare_context`/`get_index`: the image key leads with the
/// collections per event, the index and datums count events, the kickoff
/// bound counts collections, and a prepare with the same
/// `collections_per_event` keeps the resource.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn collections_per_event_groups_frames_into_events() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "cpe").await;
    let h5 = dir.path().join("cpe.h5");
    Stageable::stage(&det).await.expect("stage");
    let info = |events| TriggerInfo {
        number_of_events: events,
        collections_per_event: 2,
        ..TriggerInfo::default()
    };
    finish(Preparable::prepare(&det, info(2)).await, "prepare")
        .await
        .unwrap();
    let keys = ReadableObj::describe_dyn(&det).await.expect("describe");
    assert_eq!(
        keys["det_image"].shape,
        shape(&[2, HEIGHT as u64, WIDTH as u64])
    );
    assert_eq!(keys["det_image"].dtype, Dtype::Array);
    finish(Flyable::kickoff(&det).await, "kickoff")
        .await
        .unwrap();
    finish(Flyable::complete(&det).await, "complete")
        .await
        .unwrap();
    assert_eq!(get::<i64>(&format!("{CAM}NumImages")).await, 4);
    assert_eq!(index(&det).await, 2);
    let (resources, datums) = drain(&det).await;
    assert_eq!(resources.len(), 1, "{resources:?}");
    assert_eq!(
        resources[0].parameters["chunk_shape"],
        json!([1, HEIGHT, WIDTH])
    );
    assert_eq!(datums.len(), 1, "{datums:?}");
    assert_eq!((datums[0].indices.start, datums[0].indices.stop), (0, 2));

    let again = finish(Flyable::kickoff(&det).await, "kickoff").await;
    if again.is_ok() {
        let _ = finish(Flyable::complete(&det).await, "complete").await;
    }
    finish(Preparable::prepare(&det, info(1)).await, "prepare")
        .await
        .unwrap();
    finish(Flyable::kickoff(&det).await, "kickoff")
        .await
        .unwrap();
    finish(Flyable::complete(&det).await, "complete")
        .await
        .unwrap();
    let n = index(&det).await;
    let (resources, datums) = drain(&det).await;
    Stageable::unstage(&det).await.expect("unstage");
    let err = again.expect_err("kickoff past the prepared events succeeded");
    assert!(
        err.contains("Kickoff requested 4:8, but detector was only prepared up to 4"),
        "{err}"
    );
    assert_eq!(n, 3);
    assert!(resources.is_empty(), "{resources:?}");
    assert_eq!(datums.len(), 1, "{datums:?}");
    assert_eq!((datums[0].indices.start, datums[0].indices.stop), (2, 3));
    #[cfg(feature = "hdf5")]
    assert_eq!(
        h5_shape(&h5, "entry/data/data"),
        vec![6, HEIGHT as usize, WIDTH as usize]
    );
    #[cfg(not(feature = "hdf5"))]
    assert!(h5.exists());
}

/// ophyd-async `test_standard_detector.py:625`
/// `test_config_signals_in_describe_configuration` and
/// `test_adsim_system.py:209`/`:217`: the exposure time and period are the
/// configuration, read from and described by their `_RBV` PVs.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn configuration_describes_the_cam_exposure_pvs() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("cfg", dir.path(), "cfg").await;
    put::<f64>(&format!("{CAM}AcquireTime"), 0.05).await;
    put::<f64>(&format!("{CAM}AcquirePeriod"), 0.07).await;
    // The readbacks update from the driver's callback, after the put returns.
    let deadline = Instant::now() + TIMEOUT;
    while get::<f64>(&format!("{CAM}AcquireTime_RBV")).await != 0.05
        || get::<f64>(&format!("{CAM}AcquirePeriod_RBV")).await != 0.07
    {
        assert!(
            Instant::now() < deadline,
            "AcquireTime/AcquirePeriod_RBV did not follow"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let described = ConfigurableObj::describe_configuration_dyn(&det)
        .await
        .expect("describe_configuration");
    let read = ConfigurableObj::read_configuration_dyn(&det)
        .await
        .expect("read_configuration");
    let names: HashSet<&str> = described.keys().map(String::as_str).collect();
    assert_eq!(
        names,
        HashSet::from(["cfg_acquire_time", "cfg_acquire_period"])
    );
    let read_names: HashSet<&str> = read.keys().map(String::as_str).collect();
    assert_eq!(read_names, names);
    for (key, pv, value) in [
        ("cfg_acquire_time", "AcquireTime", 0.05),
        ("cfg_acquire_period", "AcquirePeriod", 0.07),
    ] {
        let k = &described[key];
        assert_eq!(k.source, format!("ca://{CAM}{pv}_RBV"), "{key}");
        assert_eq!(k.dtype, Dtype::Number, "{key}");
        assert_eq!(k.shape, Vec::<Option<u64>>::new(), "{key}");
        assert_eq!(numpy(k), Some("<f8"), "{key}");
        assert_eq!(read[key].value, json!(value), "{key}");
    }
}

/// ophyd-async `test_standard_detector.py:752` `test_read_returns_correct_values`
/// and `test_adsim_system.py:161` (event data `{}`): every field `read`
/// returns is one `describe` declares.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn read_reports_only_described_fields() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "read").await;
    Stageable::stage(&det).await.expect("stage");
    trigger(&det).await.unwrap();
    let described = ReadableObj::describe_dyn(&det).await.expect("describe");
    let read = ReadableObj::read_dyn(&det).await.expect("read");
    let hints = ReadableObj::hint_fields(&det).unwrap_or_default();
    Stageable::unstage(&det).await.expect("unstage");
    let undeclared: Vec<&String> = read
        .keys()
        .filter(|k| !described.contains_key(*k))
        .collect();
    assert!(
        undeclared.is_empty(),
        "read but not described: {undeclared:?}"
    );
    let unhinted: Vec<&String> = hints
        .iter()
        .filter(|k| !described.contains_key(*k))
        .collect();
    assert!(
        unhinted.is_empty(),
        "hinted but not described: {unhinted:?}"
    );
}

/// ophyd-async `test_data_logic.py:36` `test_hdf_writer_file_not_found` and
/// `PathInfo.create_dir_depth`: a directory the IOC cannot see fails
/// `prepare(TriggerInfo())`; allowed to create the last two levels, the IOC
/// makes them.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn hdf_prepare_needs_the_directory_or_leave_to_create_it() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("a").join("b");

    let det = hdf_det("det", &missing, "nodir").await;
    Stageable::stage(&det).await.expect("stage");
    let prepared = finish(
        Preparable::prepare(&det, TriggerInfo::default()).await,
        "prepare",
    )
    .await;
    let _ = Stageable::unstage(&det).await;
    let err = prepared.expect_err("prepare into a missing dir");
    assert!(err.contains("doesn't exist"), "{err}");
    assert!(!missing.exists());

    let det = area_detector_hdf(
        "det",
        CAM,
        HDF,
        provider(&missing, "mkdir").with_create_dir_depth(-2),
    );
    connect_area_detector_hdf(&det, TIMEOUT).await.unwrap();
    Stageable::stage(&det)
        .await
        .expect("stage with create_dir_depth -2");
    trigger(&det).await.unwrap();
    Stageable::unstage(&det).await.expect("unstage");
    assert!(missing.join("mkdir.h5").exists());
}

/// ophyd-async `test_data_logic.py:232` `test_describe_different_color_modes`:
/// an RGB1 frame gets a leading colour dimension.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn rgb1_frames_get_a_colour_dimension() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "rgb").await;
    put::<String>(&format!("{CAM}ColorMode"), AD_COLOR_MODE_RGB1.into()).await;
    det.control().warmup().await.expect("warmup");
    let staged = Stageable::stage(&det).await;
    let prepared = finish(
        Preparable::prepare(&det, TriggerInfo::default()).await,
        "prepare",
    )
    .await;
    let keys = ReadableObj::describe_dyn(&det).await;
    let _ = Stageable::unstage(&det).await;
    put::<String>(&format!("{CAM}ColorMode"), AD_COLOR_MODE_MONO.into()).await;
    staged.expect("stage");
    prepared.unwrap();
    assert_eq!(
        keys.expect("describe")["det_image"].shape,
        shape(&[1, 3, HEIGHT as u64, WIDTH as u64])
    );
}

/// The polled Acquire_RBV wait in `arm` must not miss an acquisition shorter
/// than its poll; ophyd-async `ADAcquireLogic.start_acquiring`
/// (`_acquire_logic.py:25`) waits on a monitor.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn trigger_completes_short_exposures() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "short").await;
    put::<f64>(&format!("{CAM}AcquireTime"), 0.001).await;
    put::<f64>(&format!("{CAM}AcquirePeriod"), 0.001).await;
    Stageable::stage(&det).await.expect("stage");
    let mut slow = Vec::new();
    for i in 0..10 {
        let t0 = Instant::now();
        let r = timeout(Duration::from_secs(3), trigger(&det)).await;
        if !matches!(r, Ok(Ok(()))) {
            slow.push((i, t0.elapsed(), r));
            DetectorControl::disarm(det.control())
                .await
                .expect("disarm");
        }
    }
    let n = index(&det).await;
    Stageable::unstage(&det).await.expect("unstage");
    assert!(slow.is_empty(), "triggers over 3 s or failed: {slow:?}");
    assert_eq!(n, 10);
}

/// ophyd test_areadetector stage_sigs and ophyd-async `test_use_cases.py:404`
/// `test_2_rois_with_hdf`: `num_rois` enables the first n ROIs only while
/// they are staged.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn num_rois_enables_the_first_rois_while_staged() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let rois: Vec<NdRoi> = (1..=4)
        .map(|i| NdRoi::new(format!("SIM1:ROI{i}:")))
        .collect();
    for roi in &rois {
        roi.connect(TIMEOUT).await.unwrap();
        roi.plugin.set_enabled(false).await.unwrap();
    }
    rois[3].plugin.set_enabled(true).await.unwrap();
    let refs: Vec<&NdRoi> = rois.iter().collect();
    assert!(num_rois(&refs, 5).is_err());
    num_rois(&refs, 2).unwrap();

    async fn enabled(rois: &[NdRoi]) -> Vec<bool> {
        let mut out = Vec::new();
        for roi in rois {
            out.push(
                SignalBackend::<bool>::get_value(roi.plugin.enable_callbacks.as_ref())
                    .await
                    .unwrap(),
            );
        }
        out
    }
    assert_eq!(enabled(&rois).await, [false, false, false, true]);
    for roi in &rois {
        StageableObj::stage_dyn(roi).await.unwrap();
    }
    assert_eq!(enabled(&rois).await, [true, true, false, false]);
    for roi in &rois {
        StageableObj::unstage_dyn(roi).await.unwrap();
    }
    assert_eq!(enabled(&rois).await, [false, false, false, true]);
    rois[3].plugin.set_enabled(false).await.unwrap();
}

/// ophyd-async `test_use_cases.py:404` `test_2_rois_with_hdf`, with TIFF1 as
/// the second writer since the IOC has one HDF plugin: a writer fed from an
/// ROI describes the ROI's frame, and the hints and asset documents follow
/// the writers' order.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn writers_fed_from_rois_describe_the_roi_frames() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let cam = AreaDetectorCam::new(CAM);
    cam.connect(TIMEOUT).await.expect("connect cam");
    ready_cam(&cam, "").await;
    let rois = [NdRoi::new("SIM1:ROI1:"), NdRoi::new("SIM1:ROI2:")];
    // ROI2 asks past the frame's edge, which clips it to WIDTH - 50.
    for (roi, [min_x, x, y]) in rois.iter().zip([[0, 40, 30], [50, 1000, 10]]) {
        roi.connect(TIMEOUT).await.unwrap();
        roi.plugin.set_source_port(PORT).await.unwrap();
        roi.set_bounds(min_x, 0, x, y).await.unwrap();
        roi.set_enabled_xy(true, true).await.unwrap();
        roi.plugin.set_blocking(true).await.unwrap();
        roi.plugin.set_enabled(true).await.unwrap();
    }
    for (prefix, port) in [(HDF, "ROI1"), (TIFF, "ROI2")] {
        let f = file_plugin(prefix).await;
        f.plugin.set_source_port(port).await.unwrap();
        f.plugin.set_blocking(true).await.unwrap();
        f.plugin.set_enabled(true).await.unwrap();
    }
    put::<i64>(&format!("{HDF}NumFramesChunks"), 1).await;
    // A frame through the ROIs sets their SizeX/Y_RBV.
    cam.warmup().await.expect("warmup");
    let shape_of = |roi: &NdRoi| {
        Arc::new(NdArrayDescription::new(
            &cam,
            vec![roi.size_y_rbv.clone(), roi.size_x_rbv.clone()],
        ))
    };
    let hdf: Arc<dyn DetectorWriter> = Arc::new(AdHdfWriter::new(
        "det_roi1",
        Arc::new(NdFileIo::new(Arc::new(NdFileHdf5::new(HDF)))),
        shape_of(&rois[0]),
        provider(dir.path(), "roi1"),
    ));
    let tiff: Arc<dyn DetectorWriter> = Arc::new(AdMultipartWriter::tiff(
        "det_roi2",
        Arc::new(NdPluginFileIo::new(Arc::new(NdFile::new(TIFF)))),
        shape_of(&rois[1]),
        provider(dir.path(), "roi2"),
    ));
    let det = StandardDetector::with_data_logics(
        "det",
        cam,
        vec![DataLogic::Unbounded(hdf), DataLogic::Unbounded(tiff)],
    );
    det.connect_writers(TIMEOUT).await.expect("connect writers");

    Stageable::stage(&det).await.expect("stage");
    let triggered = trigger(&det).await;
    let keys = ReadableObj::describe_dyn(&det).await;
    let hints = ReadableObj::hint_fields(&det);
    let mut docs = Vec::new();
    if triggered.is_ok() {
        let mut stream = det.collect_asset_docs(index(&det).await, "");
        while let Some(doc) = stream.next().await {
            docs.push(doc);
        }
    }
    let _ = Stageable::unstage(&det).await;
    for (prefix, _) in [(HDF, "ROI1"), (TIFF, "ROI2")] {
        let f = file_plugin(prefix).await;
        f.plugin.set_source_port(PORT).await.unwrap();
        f.plugin.set_enabled(false).await.unwrap();
    }
    for roi in &rois {
        roi.plugin.set_enabled(false).await.unwrap();
    }

    triggered.unwrap();
    let keys = keys.expect("describe");
    assert_eq!(keys.len(), 2, "{keys:?}");
    for (key, dims) in [
        ("det_roi1_image", [1, 30, 40]),
        ("det_roi2_image", [1, 10, 14]),
    ] {
        assert_eq!(keys[key].shape, shape(&dims), "{key}");
        assert_eq!(numpy(&keys[key]), Some("|u1"), "{key}");
        assert_eq!(keys[key].external.as_deref(), Some("STREAM:"), "{key}");
    }
    assert_eq!(
        keys["det_roi1_image"].source,
        file_uri(&dir.path().join("roi1.h5"))
    );
    assert_eq!(
        hints,
        Some(vec![
            "det_roi1_image".to_string(),
            "det_roi2_image".to_string()
        ])
    );
    let order: Vec<String> = docs
        .iter()
        .map(|d| match d {
            StreamAsset::Resource(r) => format!("resource {}", r.data_key),
            StreamAsset::Datum(d) => format!("datum {}..{}", d.indices.start, d.indices.stop),
        })
        .collect();
    assert_eq!(
        order,
        [
            "resource det_roi1_image",
            "datum 0..1",
            "resource det_roi2_image",
            "datum 0..1"
        ]
    );
    for (i, chunk) in [[1, 30, 40], [1, 10, 14]].iter().enumerate() {
        let StreamAsset::Resource(r) = &docs[2 * i] else {
            unreachable!()
        };
        let StreamAsset::Datum(d) = &docs[2 * i + 1] else {
            unreachable!()
        };
        assert_eq!(d.stream_resource, r.uid);
        if i == 0 {
            assert_eq!(r.parameters["chunk_shape"], json!(chunk));
        }
    }
    #[cfg(feature = "hdf5")]
    assert_eq!(
        h5_shape(&dir.path().join("roi1.h5"), "entry/data/data"),
        [1, 30, 40]
    );
}

/// ophyd-async `test_use_cases.py:404` (ROI bounds) and `:532`
/// `test_simdetector_with_stats_signal` (stats on): the plugin setters reach
/// their records.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn roi_and_stats_setters_reach_their_records() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let roi = NdRoi::new("SIM1:ROI1:");
    roi.connect(TIMEOUT).await.unwrap();
    roi.set_bounds(4, 6, 20, 10).await.unwrap();
    roi.set_enabled_xy(true, false).await.unwrap();
    for (suffix, want) in [("MinX", 4), ("MinY", 6), ("SizeX", 20), ("SizeY", 10)] {
        assert_eq!(
            get::<i64>(&format!("SIM1:ROI1:{suffix}")).await,
            want,
            "{suffix}"
        );
    }
    assert!(get::<bool>("SIM1:ROI1:EnableX").await);
    assert!(!get::<bool>("SIM1:ROI1:EnableY").await);
    roi.set_enabled_xy(true, true).await.unwrap();

    let stats = NdStats::new("SIM1:Stats1:");
    stats.connect(TIMEOUT).await.unwrap();
    stats.plugin.set_enabled(false).await.unwrap();
    put::<bool>("SIM1:Stats1:ComputeStatistics", false).await;
    stats.force_enable_stats().await.unwrap();
    assert!(get::<bool>("SIM1:Stats1:EnableCallbacks").await);
    assert!(get::<bool>("SIM1:Stats1:ComputeStatistics").await);
}

/// ophyd-async `test_use_cases.py:532` `test_simdetector_with_stats_signal`:
/// a detector with no writer reads a plugin's signal, and its trigger makes
/// the driver wait for the plugins first.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn plugin_signal_is_read_after_the_driver_waits_for_plugins() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let cam = AreaDetectorCam::new(CAM);
    cam.connect(TIMEOUT).await.expect("connect cam");
    ready_cam(&cam, "").await;
    let stats = NdStats::new("SIM1:Stats1:");
    stats.connect(TIMEOUT).await.unwrap();
    stats.plugin.set_source_port(PORT).await.unwrap();
    stats.force_enable_stats().await.unwrap();
    let total = Arc::new(SignalR::new(
        stats.total.clone(),
        SignalConfig {
            source: "SIM1:Stats1:Total_RBV".into(),
            kind: Kind::Normal,
            name: "det_stats_total".into(),
        },
    ));
    let logic = PluginSignalDataLogic::new(&cam, total);
    let det =
        StandardDetector::with_data_logics("det", cam, vec![DataLogic::Single(Arc::new(logic))]);
    let wait_for_plugins = format!("{CAM}WaitForPlugins");
    put::<bool>(&wait_for_plugins, false).await;

    Stageable::stage(&det).await.expect("stage");
    let after_stage = get::<bool>(&wait_for_plugins).await;
    let triggered = trigger(&det).await;
    let after_trigger = get::<bool>(&wait_for_plugins).await;
    let keys = ReadableObj::describe_dyn(&det).await;
    let reading = ReadableObj::read_dyn(&det).await;
    let want_total = get::<f64>("SIM1:Stats1:Total_RBV").await;
    let hints = ReadableObj::hint_fields(&det);
    let _ = Stageable::unstage(&det).await;
    put::<bool>(&wait_for_plugins, false).await;
    stats.plugin.set_enabled(false).await.unwrap();

    assert!(!after_stage);
    triggered.unwrap();
    assert!(after_trigger);
    let keys = keys.expect("describe");
    assert_eq!(keys.len(), 1, "{keys:?}");
    let key = &keys["det_stats_total"];
    assert_eq!(key.dtype, Dtype::Number);
    assert_eq!(numpy(key), Some("<f8"));
    assert!(key.shape.is_empty());
    assert_eq!(key.source, "ca://SIM1:Stats1:Total_RBV");
    let reading = reading.expect("read");
    assert_eq!(reading.len(), 1, "{reading:?}");
    assert_eq!(reading["det_stats_total"].value, json!(want_total));
    assert_eq!(hints, Some(vec!["det_stats_total".to_string()]));
}

/// ophyd-async `test_adsim_system.py:161` `test_software_triggering`, through
/// the run engine: two triggered reads give one resource and consecutive
/// datums carrying the events' sequence numbers, and the descriptor carries
/// the cam configuration.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn step_scan_through_the_run_engine_links_datums_to_events() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = Arc::new(hdf_det("adsim", dir.path(), "adsim").await);
    let h5 = dir.path().join("adsim.h5");
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let d = det.clone();
    let plan = bsrs::core::plan::plan_box(async_stream::stream! {
        yield Msg::OpenRun(Default::default());
        yield Msg::Stage(d.clone() as Arc<dyn StageableObj>);
        for _ in 0..2 {
            yield Msg::Create { stream_name: "primary".into() };
            yield Msg::Trigger { obj: d.clone() as Arc<dyn TriggerableObj>, group: None };
            yield Msg::Read(d.clone() as Arc<dyn ReadableObj>);
            yield Msg::Save;
        }
        yield Msg::Unstage(d.clone() as Arc<dyn StageableObj>);
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    let result = timeout(Duration::from_secs(60), re.run_async(plan))
        .await
        .expect("run not done within 60 s")
        .expect("run");
    assert_eq!(result.exit_status, "success");

    use bsrs::core::Document;
    let docs = sink.snapshot().await;
    let descriptor = docs
        .iter()
        .find_map(|d| match d {
            Document::Descriptor(d) => Some(d.clone()),
            _ => None,
        })
        .expect("descriptor");
    let image = &descriptor.data_keys["adsim_image"];
    assert_eq!(image.source, file_uri(&h5));
    assert_eq!(image.shape, shape(&[1, HEIGHT as u64, WIDTH as u64]));
    let config = &descriptor.configuration["adsim"];
    for (key, pv) in [
        ("adsim_acquire_time", "AcquireTime"),
        ("adsim_acquire_period", "AcquirePeriod"),
    ] {
        assert_eq!(
            config.data_keys[key].source,
            format!("ca://{CAM}{pv}_RBV"),
            "{key}"
        );
        assert!(config.data.contains_key(key), "{key}");
    }
    let events: Vec<_> = docs
        .iter()
        .filter_map(|d| match d {
            Document::Event(e) => Some(e.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(events.len(), 2);
    // The frames reach the events through StreamDatum only (`data={}` at
    // test_adsim_system.py:265/:281).
    for e in &events {
        assert!(e.data.is_empty(), "event data: {:?}", e.data);
    }
    // `hints={"adsim": {"fields": ["adsim"]}}` at test_adsim_system.py:241,
    // with bsrs's `_image` data key.
    let hints = descriptor.hints.as_ref().expect("descriptor hints");
    assert_eq!(
        hints["adsim"].fields.as_deref(),
        Some(&["adsim_image".to_string()][..])
    );
    let resources: Vec<_> = docs
        .iter()
        .filter_map(|d| match d {
            Document::StreamResource(r) => Some(r.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].uri, file_uri(&h5));
    let datums: Vec<_> = docs
        .iter()
        .filter_map(|d| match d {
            Document::StreamDatum(d) => Some(d.clone()),
            _ => None,
        })
        .collect();
    let ranges: Vec<_> = datums
        .iter()
        .map(|d| {
            (
                d.indices.start,
                d.indices.stop,
                d.seq_nums.start,
                d.seq_nums.stop,
                d.descriptor == descriptor.uid,
            )
        })
        .collect();
    assert_eq!(ranges, [(0, 1, 1, 2, true), (1, 2, 2, 3, true)]);
    #[cfg(feature = "hdf5")]
    assert_eq!(
        h5_shape(&h5, "entry/data/data"),
        vec![2, HEIGHT as usize, WIDTH as usize]
    );
}
