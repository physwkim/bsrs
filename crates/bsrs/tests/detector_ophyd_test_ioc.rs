//! areaDetector staging and triggering against the ophyd-test-ioc example's
//! SimDetector, which serves the same `SIM` port and plugins under both
//! prefixes ophyd's `ad_prefix` fixture tries (`conftest.py:73`). Every test
//! drives that one port, so each holds `CAM_LOCK`.
#![cfg(all(feature = "ca", feature = "host"))]

mod example_ioc;

use std::time::{Duration, Instant};

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::core::msg::StageableObj;
use bsrs::host::areadetector::{
    area_detector_hdf, connect_area_detector_hdf, select_save_plugin, AreaDetectorCam, NdFile,
    StaticPathProvider,
};
use bsrs::protocols_async::{
    DetectorControl, SignalBackend, Stageable, Triggerable, WritesStreamAssets,
};
use tokio::sync::Mutex;
use tokio::time::timeout;

const PREFIXES: [&str; 2] = ["ADSIM:", "XF:31IDA-BI{Cam:Tbl}:"];
const PORT: &str = "SIM";
const TIMEOUT: Duration = Duration::from_secs(10);
const WIDTH: i64 = 64;
const HEIGHT: i64 = 48;

static CAM_LOCK: Mutex<()> = Mutex::const_new(());

fn boot() {
    example_ioc::boot("ophyd-test-ioc", "ophyd-test-ioc");
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

/// Bring the cam under `p` to a known state whatever autosave or an earlier
/// test left: idle, Single/1, small mono UInt8 frames, no NDAttributes,
/// HDF1 fed from the cam with blocking callbacks and the other file plugins
/// off, then one short warmup frame so HDF1 holds an array.
async fn ready_cam(p: &str, cam: &AreaDetectorCam) {
    DetectorControl::disarm(cam).await.expect("disarm");
    for plugin in ["HDF1:", "TIFF1:", "JPEG1:"] {
        let f = file_plugin(&format!("{p}{plugin}")).await;
        f.stop_capture().await.expect("stop capture");
        let feed = plugin == "HDF1:";
        if feed {
            f.plugin.set_source_port(PORT).await.expect("NDArrayPort");
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
        ("DataType", 1),
        ("ArrayCallbacks", 1),
        ("BinX", 1),
        ("BinY", 1),
        ("MinX", 0),
        ("MinY", 0),
        ("SizeX", WIDTH),
        ("SizeY", HEIGHT),
    ] {
        put::<i64>(&format!("{p}cam1:{suffix}"), v).await;
    }
    put::<f64>(&format!("{p}cam1:AcquireTime"), 0.01).await;
    put::<f64>(&format!("{p}cam1:AcquirePeriod"), 0.01).await;
    timeout(
        TIMEOUT,
        SignalBackend::put(cam.nd_attributes_file.as_ref(), Some(String::new())),
    )
    .await
    .expect("NDAttributesFile put timed out")
    .expect("clear NDAttributesFile");
    cam.warmup().await.expect("warmup");
}

#[cfg(feature = "hdf5")]
fn h5_frames(path: &std::path::Path) -> usize {
    let file = rust_hdf5::H5File::open(path).expect("open the IOC's .h5");
    file.dataset("entry/data/data")
        .unwrap_or_else(|e| panic!("entry/data/data in {}: {e}", path.display()))
        .shape()[0]
}

/// ophyd `test_areadetector.py:132` `test_basic` and `:967` `test_posix_path`,
/// under each `ad_prefix`: with a 0.5 s Single exposure, a staged detector's
/// trigger finishes within 5 s, and not before the frame was taken.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn trigger_waits_for_the_exposure_through_either_prefix() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    for p in PREFIXES {
        let dir = tempfile::tempdir().unwrap();
        let det = area_detector_hdf(
            "test",
            format!("{p}cam1:"),
            format!("{p}HDF1:"),
            StaticPathProvider::new(dir.path().to_str().unwrap(), "basic"),
        );
        connect_area_detector_hdf(&det, TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("connect under {p}: {e}"));
        ready_cam(p, det.control()).await;
        put::<i64>(&format!("{p}HDF1:NumFramesChunks"), 1).await;
        put::<f64>(&format!("{p}cam1:AcquireTime"), 0.5).await;
        put::<f64>(&format!("{p}cam1:AcquirePeriod"), 0.5).await;

        Stageable::stage(&det).await.expect("stage");
        let started = Instant::now();
        let status = timeout(TIMEOUT, Triggerable::trigger(&det))
            .await
            .expect("trigger started");
        let done = timeout(Duration::from_secs(5), status)
            .await
            .unwrap_or_else(|_| panic!("trigger under {p} not done within 5 s"));
        let took = started.elapsed();
        done.unwrap_or_else(|e| panic!("trigger under {p}: {e:?}"));
        assert!(
            took >= Duration::from_millis(450),
            "{p}: done after {took:?}"
        );
        assert_eq!(det.get_index().await.expect("index"), 1, "{p}");
        Stageable::unstage(&det).await.expect("unstage");

        let h5 = dir.path().join("basic.h5");
        assert!(h5.exists(), "{p}: {} was not written", h5.display());
        #[cfg(feature = "hdf5")]
        assert_eq!(h5_frames(&h5), 1, "{p}");
    }
}

/// ophyd `test_areadetector.py:236-262`: what stage applies, unstage puts
/// back. `select_save_plugin` only records the routing; staging the plugins
/// feeds HDF1 from the cam and turns its siblings off, unstaging restores
/// each record to what it held before.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn select_save_plugin_routes_on_stage_and_unstage_restores_it() {
    boot();
    let _cam = CAM_LOCK.lock().await;
    let p = PREFIXES[0];
    let hdf1 = file_plugin(&format!("{p}HDF1:")).await;
    let tiff1 = file_plugin(&format!("{p}TIFF1:")).await;
    let jpeg1 = file_plugin(&format!("{p}JPEG1:")).await;
    hdf1.plugin.set_enabled(false).await.unwrap();
    hdf1.plugin.set_source_port("ROI1").await.unwrap();
    tiff1.plugin.set_enabled(true).await.unwrap();
    jpeg1.plugin.set_enabled(true).await.unwrap();

    async fn state(p: &str) -> (bool, String, bool, bool) {
        (
            get::<bool>(&format!("{p}HDF1:EnableCallbacks")).await,
            get::<String>(&format!("{p}HDF1:NDArrayPort")).await,
            get::<bool>(&format!("{p}TIFF1:EnableCallbacks")).await,
            get::<bool>(&format!("{p}JPEG1:EnableCallbacks")).await,
        )
    }
    let before = (false, "ROI1".to_string(), true, true);
    assert_eq!(state(p).await, before);

    select_save_plugin(&hdf1, PORT, &[&tiff1, &jpeg1]);
    assert_eq!(
        state(p).await,
        before,
        "select_save_plugin wrote to the IOC"
    );

    for f in [&hdf1, &tiff1, &jpeg1] {
        StageableObj::stage_dyn(f).await.expect("stage");
    }
    assert_eq!(state(p).await, (true, PORT.to_string(), false, false));

    for f in [&jpeg1, &tiff1, &hdf1] {
        StageableObj::unstage_dyn(f).await.expect("unstage");
    }
    assert_eq!(state(p).await, before);

    tiff1.plugin.set_enabled(false).await.unwrap();
    jpeg1.plugin.set_enabled(false).await.unwrap();
    hdf1.plugin.set_source_port(PORT).await.unwrap();
}
