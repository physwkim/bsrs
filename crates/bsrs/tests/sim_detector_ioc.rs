//! `StandardDetector` against the sim-detector example IOC's real
//! areaDetector records (`SIM1:cam1:` and its commonPlugins), checking what
//! ophyd-async's detector tests check. `cargo test` runs these against one
//! shared IOC, so every test that drives the cam holds `CAM_LOCK`.
#![cfg(all(feature = "ca", feature = "host"))]

mod example_ioc;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::core::msg::{ReadableObj, StageableObj};
use bsrs::core::status::Status;
use bsrs::devices::TriggerInfo;
use bsrs::event_model::{DataKey, Dtype, DtypeNumpy, StreamDatum, StreamResource};
use bsrs::host::areadetector::{
    area_detector_hdf, connect_area_detector_hdf, num_rois, AreaDetectorCam, AreaDetectorHdf,
    NdFile, NdRoi, NdStats, StaticPathProvider, AD_IMAGE_MODE_CONTINUOUS, AD_IMAGE_MODE_MULTIPLE,
};
use bsrs::prelude::*;
use bsrs::protocols_async::{
    DetectorControl, Flyable, Preparable, SignalBackend, Stageable, StreamAsset, Triggerable,
    WritesStreamAssets,
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
    EpicsCaBackend<V>: SignalBackend<V>,
{
    let ch = EpicsCaBackend::<V>::new(pv);
    ch.connect(TIMEOUT)
        .await
        .unwrap_or_else(|e| panic!("connect {pv}: {e}"));
    ch
}

async fn put<V>(pv: &str, v: V)
where
    V: Clone + Send + Sync + 'static,
    EpicsCaBackend<V>: SignalBackend<V>,
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
    EpicsCaBackend<V>: SignalBackend<V>,
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
        ("ImageMode", 0),
        ("NumImages", 1),
        ("NumExposures", 1),
        ("ColorMode", 0),
        ("DataType", 1),
        ("ArrayCallbacks", 1),
        ("BinX", 1),
        ("BinY", 1),
        ("MinX", 0),
        ("MinY", 0),
        ("SizeX", WIDTH),
        ("SizeY", HEIGHT),
    ] {
        put::<i64>(&format!("{CAM}{suffix}"), v).await;
    }
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
/// and `test_adsim_system.py:161` `test_software_triggering`: the image key
/// points at the file, one resource per staging, one datum per trigger.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn hdf_step_scan_points_its_documents_at_the_file_the_ioc_wrote() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "step").await;
    let h5 = dir.path().join("step.h5");

    Stageable::stage(&det).await.expect("stage");
    let keys = ReadableObj::describe_dyn(&det).await.expect("describe");
    let image = &keys["det_image"];
    assert_eq!(image.shape, shape(&[1, HEIGHT as u64, WIDTH as u64]));
    assert_eq!(image.dtype, Dtype::Array);
    assert_eq!(numpy(image), Some("|u1"));
    assert_eq!(image.external.as_deref(), Some("STREAM:"));
    assert_eq!(image.source, file_uri(&h5));

    trigger(&det).await.unwrap();
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
    let attrs_file = EpicsCaBackend::<String>::new_long_string(format!("{CAM}NDAttributesFile"));
    attrs_file.connect(TIMEOUT).await.unwrap();
    timeout(TIMEOUT, SignalBackend::put(&attrs_file, Some(xml)))
        .await
        .unwrap()
        .unwrap();

    Stageable::stage(&det).await.expect("stage");
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

    trigger(&det).await.unwrap();
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
        get::<i64>(&format!("{CAM}ImageMode")).await,
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
    let kickoff = Flyable::kickoff(&det).await;
    let early = timeout(Duration::from_millis(500), kickoff).await;
    let acquiring = get::<bool>(&format!("{CAM}Acquire_RBV")).await;
    let completed = finish(Flyable::complete(&det).await, "complete").await;
    Stageable::unstage(&det).await.expect("unstage");
    assert!(acquiring, "the 1 s acquisition ended within 500 ms");
    assert!(
        matches!(early, Ok(Ok(()))),
        "kickoff of a 1 s acquisition not done after 500 ms: {early:?}"
    );
    completed.unwrap();
}

/// ophyd-async `test_standard_detector.py:894` `test_unstage_disarms_detector`
/// and `test_acquire_logic.py:123` `test_acquire_logic_disarm`: stage and
/// unstage both leave a free-running cam stopped.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn stage_and_unstage_stop_a_running_acquisition() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let det = hdf_det("det", dir.path(), "disarm").await;
    let cam = det.control();
    let acquiring = format!("{CAM}Acquire_RBV");

    put::<i64>(&format!("{CAM}ImageMode"), AD_IMAGE_MODE_CONTINUOUS).await;
    cam.acquire.put_nowait(true).await.unwrap();
    assert!(wait_for(&acquiring, true).await);
    Stageable::stage(&det).await.expect("stage");
    assert!(
        !get::<bool>(&acquiring).await,
        "stage left the cam acquiring"
    );

    cam.acquire.put_nowait(true).await.unwrap();
    assert!(wait_for(&acquiring, true).await);
    Stageable::unstage(&det).await.expect("unstage");
    assert!(
        !get::<bool>(&acquiring).await,
        "unstage left the cam acquiring"
    );
}

async fn wait_for(pv: &str, want: bool) -> bool {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        if get::<bool>(pv).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// ophyd-async `test_data_logic.py:36` `test_hdf_writer_file_not_found` and
/// `PathInfo.create_dir_depth`: a directory the IOC cannot see fails stage;
/// allowed to create the last two levels, the IOC makes them.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn hdf_stage_needs_the_directory_or_leave_to_create_it() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("a").join("b");

    let det = hdf_det("det", &missing, "nodir").await;
    let err = Stageable::stage(&det)
        .await
        .expect_err("stage into a missing dir");
    assert!(err.to_string().contains("doesn't exist"), "{err}");
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
    put::<i64>(&format!("{CAM}ColorMode"), 2).await;
    det.control().warmup().await.expect("warmup");
    let staged = Stageable::stage(&det).await;
    let keys = ReadableObj::describe_dyn(&det).await;
    let _ = Stageable::unstage(&det).await;
    put::<i64>(&format!("{CAM}ColorMode"), 0).await;
    staged.expect("stage");
    assert_eq!(
        keys.expect("describe")["det_image"].shape,
        shape(&[1, 3, HEIGHT as u64, WIDTH as u64])
    );
}

/// The polled Acquire_RBV wait in `arm` must not miss an acquisition shorter
/// than its poll; ophyd-async `test_acquire_logic.py:97` waits on a monitor.
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

/// ophyd-async `test_use_cases.py:404` (ROI bounds) and `:532`
/// `test_simdetector_with_stats_signal` (stats on): the plugin setters reach
/// their records.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn roi_and_stats_setters_reach_their_records() {
    boot();
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
    put::<i64>("SIM1:Stats1:ComputeStatistics", 0).await;
    stats.force_enable_stats().await.unwrap();
    assert!(get::<bool>("SIM1:Stats1:EnableCallbacks").await);
    assert!(get::<bool>("SIM1:Stats1:ComputeStatistics").await);
}
