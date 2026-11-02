//! areaDetector helpers — `AreaDetectorCam`, `NdPlugin`, and the
//! `NdFile` / `NdStats` / `NdRoi` specializations. Mirrors ophyd-async's
//! `AreaDetector` / `NDPlugin` layer for driving an areaDetector
//! NDPlugin chain from bsrs plans.
//!
//! ## PV name convention
//!
//! All helpers take a `prefix` that is concatenated with each PV's
//! field name (e.g. prefix `"13SIM1:cam1:"` + field `"AcquireTime"` →
//! `"13SIM1:cam1:AcquireTime"`). Pass the prefix exactly as it appears
//! in the IOC (typically `<P><R>` where `P` is the IOC name and `R`
//! is the record group, both ending in `:`).
//!
//! ## warmup
//!
//! `AreaDetectorCam::warmup` is the ophyd-async-style first-frame
//! prime: snapshot `ImageMode`/`NumImages`, switch to Single+1, fire
//! Acquire, wait for `DetectorState_RBV = Idle (0)`, then restore. The
//! HDF5 file plugin uses the first frame to discover array dimensions
//! before opening, so a warmup is required when the IOC's
//! `lazy_open=1` flag is not in effect.
//!
//! ## Wire conventions
//!
//! - `ImageMode`/`FileWriteMode`/`DetectorState_RBV`/`DataType_RBV`/
//!   `ColorMode_RBV`: `mbbo`/`mbbi` (enum), read and written by label as
//!   ophyd-async's `StrictEnum`/`SubsetEnum` signals do.
//! - `Acquire`/`EnableCallbacks`/`BlockingCallbacks`/`AutoIncrement`/
//!   `Capture`/`Compute*`/`EnableX`/`EnableY`: `bo` (binary). We treat
//!   these as `bool` via `EpicsCaBackend<bool>` (DBR_LONG on the wire).
//! - `FilePath`/`FileName`/`FileTemplate`: `waveform` of `CHAR`, which
//!   `EpicsCaBackend::<String>` reads and writes as a long string.
//! - `NDArrayPort`: `stringout` (DBR_STRING).

use crate::backends::epics_ca::EpicsCaBackend;
use crate::core::error::{BsrsError, Result};
use crate::core::msg::{NamedObj, StageableObj};
use crate::core::reading::ReadingValue;
use crate::core::status::{Status, StatusError, SubToken};
use crate::devices::device::Device;
use crate::devices::signal::{Read, Readable, Signal};
use crate::devices::stage_sigs::StageSigs;
use crate::devices::{wait_for_connection, PathInfo, PathProvider, StandardDetector};
use crate::event_model::{DataKey, Dtype, StreamDatum, StreamRange, StreamResource};
use crate::protocols_async::{
    DetectorControl, DetectorTrigger, DetectorWriter, SignalBackend, SingleDataLogic, StreamAsset,
    TriggerInfo,
};
use futures::stream::{BoxStream, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

const PUT_TIMEOUT: Duration = Duration::from_secs(10);
const WARMUP_IDLE_POLL: Duration = Duration::from_millis(100);
const WARMUP_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const ACQUIRE_TIME_RBV: &str = "AcquireTime_RBV";
const ACQUIRE_PERIOD_RBV: &str = "AcquirePeriod_RBV";

/// `DetectorState_RBV` label for the idle state (ophyd-async `ADState.IDLE`).
pub const AD_STATE_IDLE: &str = "Idle";

/// `DetectorState_RBV` label for an aborted acquisition — a "good" terminal
/// state alongside Idle (ophyd-async waits for `{IDLE, ABORTED}`).
pub const AD_STATE_ABORTED: &str = "Aborted";

/// `ImageMode` label for single-frame acquisition (`ADImageMode.SINGLE`).
pub const AD_IMAGE_MODE_SINGLE: &str = "Single";

/// `ImageMode` label for a bounded burst of `NumImages` frames.
pub const AD_IMAGE_MODE_MULTIPLE: &str = "Multiple";

/// `ImageMode` label for unbounded acquisition until `Acquire` is cleared.
pub const AD_IMAGE_MODE_CONTINUOUS: &str = "Continuous";

/// `FileWriteMode` label for streaming capture (`ADFileWriteMode.STREAM`).
pub const AD_FILE_WRITE_MODE_STREAM: &str = "Stream";

/// `ColorMode_RBV` label for a monochrome frame (`ADBaseColorMode.MONO`).
pub const AD_COLOR_MODE_MONO: &str = "Mono";
/// `ColorMode_RBV` label for pixel-interleaved RGB (`ADBaseColorMode.RGB1`).
pub const AD_COLOR_MODE_RGB1: &str = "RGB1";

/// Map an areaDetector `DataType_RBV` label (`ADBaseDataType`) to its numpy
/// dtype string, ophyd-async's `np.dtype(datatype.value.lower()).str`.
/// Returns `None` for any other label, including the blank one a driver
/// database leaves for an unsupported type.
pub fn ad_datatype_to_numpy(dt: &str) -> Option<&'static str> {
    Some(match dt {
        "Int8" => "|i1",
        "UInt8" => "|u1",
        "Int16" => "<i2",
        "UInt16" => "<u2",
        "Int32" => "<i4",
        "UInt32" => "<u4",
        "Int64" => "<i8",
        "UInt64" => "<u8",
        "Float32" => "<f4",
        "Float64" => "<f8",
        _ => return None,
    })
}

fn join(prefix: &str, suffix: &str) -> String {
    format!("{prefix}{suffix}")
}

/// Run a backend `put` to completion, bounding it with [`PUT_TIMEOUT`].
/// The backend `put` always waits for completion (CP-11); the per-put
/// timeout lives here, at the call layer, not in the backend.
async fn await_put<F>(put: F, what: &str) -> Result<()>
where
    F: std::future::Future<Output = Result<()>>,
{
    match tokio::time::timeout(PUT_TIMEOUT, put).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(BsrsError::Backend(format!("{what}: {e}"))),
        Err(_) => Err(BsrsError::Backend(format!(
            "{what}: put timed out after {PUT_TIMEOUT:?}"
        ))),
    }
}

/// Run `set`, then wait until the bool readback `rbv` has been `want`,
/// bounded by [`PUT_TIMEOUT`]; returns what `set` returned. Port of
/// ophyd-async `set_and_wait_for_other_value(..., wait_for_set_completion=
/// False)`, used for busy records whose put-callback tracks the operation,
/// not the value. The readback's monitor has delivered its current value
/// before `set` runs, so a transition back out of `want` (a 1 ms
/// acquisition's Acquire_RBV pulse) is still seen.
async fn set_and_wait_for_bool<R>(
    rbv: &EpicsCaBackend<bool>,
    want: bool,
    set: impl std::future::Future<Output = Result<R>>,
    what: &str,
) -> Result<R> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let _sub = SignalBackend::<bool>::set_callback(
        rbv,
        Some(Box::new(move |v: &bool, _, _| {
            let _ = tx.send(*v);
        })),
    );
    let deadline = tokio::time::Instant::now() + PUT_TIMEOUT;
    let mut last = match tokio::time::timeout_at(deadline, rx.recv()).await {
        Ok(Some(v)) => v,
        _ => {
            return Err(BsrsError::Backend(format!(
                "{what}: no initial value within {PUT_TIMEOUT:?}, is it connected?"
            )))
        }
    };
    let out = set.await?;
    while last != want {
        last = match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(v)) => v,
            _ => {
                return Err(BsrsError::Backend(format!(
                    "{what}: readback did not reach {want} within {PUT_TIMEOUT:?}"
                )))
            }
        };
    }
    Ok(out)
}

/// Cam-side handle: `ImageMode`/`NumImages`/`Acquire`/`AcquireTime`
/// setters plus `ArrayCounter_RBV` / `DetectorState_RBV` readbacks.
pub struct AreaDetectorCam {
    /// PV prefix, e.g. `"13SIM1:cam1:"`.
    pub prefix: String,
    /// `ImageMode` (mbbo: Single, Multiple, Continuous).
    pub image_mode: Arc<EpicsCaBackend<String>>,
    /// `NumImages` (longout) — how many frames to acquire in
    /// `Multiple` mode.
    pub num_images: Arc<EpicsCaBackend<i64>>,
    /// `Acquire` (busy) — true to start, false to stop. A put-callback for
    /// `1` completes when the whole acquisition ends, so arming watches
    /// [`acquire_rbv`](Self::acquire_rbv) instead of the callback.
    pub acquire: Arc<EpicsCaBackend<bool>>,
    /// `Acquire_RBV` (bi) — whether the driver is acquiring.
    pub acquire_rbv: Arc<EpicsCaBackend<bool>>,
    /// `AcquireTime` (ao) — exposure time, seconds.
    pub acquire_time: Arc<EpicsCaBackend<f64>>,
    /// `AcquirePeriod` (ao) — seconds from one exposure start to the next
    /// (exposure + dead time).
    pub acquire_period: Arc<EpicsCaBackend<f64>>,
    /// `AcquireTime_RBV` (ai) — the exposure time the driver applied.
    pub acquire_time_rbv: Arc<EpicsCaBackend<f64>>,
    /// `AcquirePeriod_RBV` (ai) — the period the driver applied.
    pub acquire_period_rbv: Arc<EpicsCaBackend<f64>>,
    /// `ArrayCounter_RBV` (longin) — total frames produced.
    pub array_counter_rbv: Arc<EpicsCaBackend<i64>>,
    /// `DetectorState_RBV` (mbbi).
    pub detector_state_rbv: Arc<EpicsCaBackend<String>>,
    /// `NDAttributesFile` (waveform) — per-frame attribute config, either
    /// inline XML or a filename on the IOC host. Read at `open()` to discover
    /// NDAttribute datasets for the emitted stream documents.
    pub nd_attributes_file: Arc<EpicsCaBackend<String>>,
    /// `WaitForPlugins` (bo) — whether `Acquire` completes only once the
    /// plugins have processed the frames.
    pub wait_for_plugins: Arc<EpicsCaBackend<bool>>,
    /// Frame description readbacks (`ArraySizeZ/Y/X_RBV`, `DataType_RBV`,
    /// `ColorMode_RBV`) — the shape/dtype source for the emitted `DataKey`,
    /// read from the driver as in ophyd-async's `ADWriterFactory` default.
    pub frame_info: Arc<NdArrayDescription>,
    /// The last arm's `Acquire=1` put-callback: done when that acquisition
    /// ends (ophyd-async `ADAcquireLogic.acquire_status`).
    acquire_status: std::sync::Mutex<Option<Status>>,
}

/// ophyd-async `NDArrayDescription` (`_data_logic.py:54`): the signals a
/// frame's shape is read from, slowest first, and the driver's
/// `DataType_RBV` and `ColorMode_RBV`. A cheap `Arc` handle so the writer
/// reads it after the cam has been moved into a `StandardDetector`.
pub struct NdArrayDescription {
    /// The driver's prefix, naming `DataType_RBV`/`ColorMode_RBV` in errors.
    prefix: String,
    shape: Vec<Arc<EpicsCaBackend<i64>>>,
    data_type: Arc<EpicsCaBackend<String>>,
    color_mode: Arc<EpicsCaBackend<String>>,
}

impl NdArrayDescription {
    /// The driver's own frame, `ArraySizeZ/Y/X_RBV`: ophyd-async's default.
    fn of_driver(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            shape: ["ArraySizeZ_RBV", "ArraySizeY_RBV", "ArraySizeX_RBV"]
                .into_iter()
                .map(|s| Arc::new(EpicsCaBackend::<i64>::new(join(prefix, s))))
                .collect(),
            data_type: Arc::new(EpicsCaBackend::<String>::new(join(prefix, "DataType_RBV"))),
            color_mode: Arc::new(EpicsCaBackend::<String>::new(join(prefix, "ColorMode_RBV"))),
        }
    }

    /// `driver`'s data type and colour mode over a frame of `shape`, e.g. an
    /// [`NdRoi`]'s `size_y_rbv` and `size_x_rbv` for a writer fed from that
    /// ROI. Zero dims are dropped. The writer does not connect `shape`; the
    /// detector does once the ROI is added with
    /// [`StandardDetector::add_plugin`].
    pub fn new(driver: &AreaDetectorCam, shape: Vec<Arc<EpicsCaBackend<i64>>>) -> Self {
        let own = &driver.frame_info;
        Self {
            prefix: own.prefix.clone(),
            shape,
            data_type: own.data_type.clone(),
            color_mode: own.color_mode.clone(),
        }
    }

    async fn connect(&self, timeout: Duration) -> Result<()> {
        let shape = self.shape.iter().enumerate().map(|(i, s)| {
            let connect = SignalBackend::<i64>::connect(s.as_ref(), timeout);
            (format!("shape[{i}]"), connect)
        });
        let data_type = SignalBackend::<String>::connect(self.data_type.as_ref(), timeout);
        let color_mode = SignalBackend::<String>::connect(self.color_mode.as_ref(), timeout);
        wait_for_connection(shape.chain([
            ("data_type".to_string(), data_type),
            ("color_mode".to_string(), color_mode),
        ]))
        .await
    }
}

/// Source of the frame shape + numpy dtype for the emitted `DataKey`. The
/// detector writer reads it at `open()`; the production impl is
/// [`NdArrayDescription`].
#[async_trait::async_trait]
pub trait AdFrameInfoSource: Send + Sync {
    /// Frame shape (slowest-varying first, zero dims dropped, color-mode
    /// adjusted) and numpy dtype string.
    async fn frame_info(&self) -> Result<(Vec<u64>, String)>;
}

/// The frame shape and numpy dtype ophyd-async's `get_ndarray_resource_info`
/// (`_data_logic.py:69-106`) derives from the shape readbacks, `data_type`
/// (read from `data_type_source`) and `color_mode`: `dims` slowest first
/// (matching the row-major HDF dataset), zero dims dropped, RGB1 prepending
/// the color dim; a blank data type and any other color mode are refused.
fn ndarray_frame_info(
    dims: &[i64],
    data_type: &str,
    color_mode: &str,
    data_type_source: &str,
) -> Result<(Vec<u64>, String)> {
    let mut shape: Vec<u64> = dims
        .iter()
        .copied()
        .filter(|&d| d > 0)
        .map(|d| d as u64)
        .collect();
    if data_type.is_empty() {
        return Err(BsrsError::Other(format!(
            "{data_type_source} is blank, this is not supported"
        )));
    }
    match color_mode {
        AD_COLOR_MODE_MONO => {}
        AD_COLOR_MODE_RGB1 => shape.insert(0, 3),
        other => {
            return Err(BsrsError::Other(format!(
                "Unsupported ColorMode {other}! Only Mono and RGB1 are supported."
            )));
        }
    }
    // Read as a `String`, the PV can hold a label `ADBaseDataType` lacks.
    let dtype_numpy = ad_datatype_to_numpy(data_type).ok_or_else(|| {
        BsrsError::Backend(format!(
            "{data_type_source}={data_type:?} is not a known ADBaseDataType"
        ))
    })?;
    Ok((shape, dtype_numpy.to_string()))
}

#[async_trait::async_trait]
impl AdFrameInfoSource for NdArrayDescription {
    async fn frame_info(&self) -> Result<(Vec<u64>, String)> {
        let (dims, dt, cm) = tokio::join!(
            futures::future::try_join_all(
                self.shape
                    .iter()
                    .map(|s| SignalBackend::<i64>::get_value(s.as_ref())),
            ),
            SignalBackend::<String>::get_value(self.data_type.as_ref()),
            SignalBackend::<String>::get_value(self.color_mode.as_ref()),
        );
        let source = SignalBackend::<String>::source(
            self.data_type.as_ref(),
            &join(&self.prefix, "DataType_RBV"),
            true,
        );
        ndarray_frame_info(&dims?, &dt?, &cm?, &source)
    }
}

impl AreaDetectorCam {
    /// Build the handle. Does NOT connect; call `connect` afterwards.
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self {
            image_mode: Arc::new(EpicsCaBackend::<String>::new(join(&prefix, "ImageMode"))),
            num_images: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "NumImages"))),
            acquire: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "Acquire"))),
            acquire_rbv: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "Acquire_RBV"))),
            acquire_time: Arc::new(EpicsCaBackend::<f64>::new(join(&prefix, "AcquireTime"))),
            acquire_period: Arc::new(EpicsCaBackend::<f64>::new(join(&prefix, "AcquirePeriod"))),
            acquire_time_rbv: Arc::new(EpicsCaBackend::<f64>::new(join(&prefix, ACQUIRE_TIME_RBV))),
            acquire_period_rbv: Arc::new(EpicsCaBackend::<f64>::new(join(
                &prefix,
                ACQUIRE_PERIOD_RBV,
            ))),
            array_counter_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "ArrayCounter_RBV",
            ))),
            detector_state_rbv: Arc::new(EpicsCaBackend::<String>::new(join(
                &prefix,
                "DetectorState_RBV",
            ))),
            nd_attributes_file: Arc::new(EpicsCaBackend::<String>::new(join(
                &prefix,
                "NDAttributesFile",
            ))),
            wait_for_plugins: Arc::new(EpicsCaBackend::<bool>::new(join(
                &prefix,
                "WaitForPlugins",
            ))),
            frame_info: Arc::new(NdArrayDescription::of_driver(&prefix)),
            acquire_status: std::sync::Mutex::new(None),
            prefix,
        }
    }

    /// Connect every channel in parallel.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        wait_for_connection([
            (
                "image_mode",
                SignalBackend::<String>::connect(self.image_mode.as_ref(), timeout),
            ),
            (
                "num_images",
                SignalBackend::<i64>::connect(self.num_images.as_ref(), timeout),
            ),
            (
                "acquire",
                SignalBackend::<bool>::connect(self.acquire.as_ref(), timeout),
            ),
            (
                "acquire_rbv",
                SignalBackend::<bool>::connect(self.acquire_rbv.as_ref(), timeout),
            ),
            (
                "acquire_time",
                SignalBackend::<f64>::connect(self.acquire_time.as_ref(), timeout),
            ),
            (
                "acquire_period",
                SignalBackend::<f64>::connect(self.acquire_period.as_ref(), timeout),
            ),
            (
                "acquire_time_rbv",
                SignalBackend::<f64>::connect(self.acquire_time_rbv.as_ref(), timeout),
            ),
            (
                "acquire_period_rbv",
                SignalBackend::<f64>::connect(self.acquire_period_rbv.as_ref(), timeout),
            ),
            (
                "array_counter_rbv",
                SignalBackend::<i64>::connect(self.array_counter_rbv.as_ref(), timeout),
            ),
            (
                "detector_state_rbv",
                SignalBackend::<String>::connect(self.detector_state_rbv.as_ref(), timeout),
            ),
            (
                "nd_attributes_file",
                SignalBackend::<String>::connect(self.nd_attributes_file.as_ref(), timeout),
            ),
            (
                "wait_for_plugins",
                SignalBackend::<bool>::connect(self.wait_for_plugins.as_ref(), timeout),
            ),
            (
                "frame_info",
                futures::FutureExt::boxed(self.frame_info.connect(timeout)),
            ),
        ])
        .await
    }

    /// The descriptor `configuration` signals, as ophyd-async `AreaDetector`
    /// registers them (`driver.acquire_time`, `driver.acquire_period`): read
    /// from their `_RBV` PVs, which are also the `DataKey` source.
    fn config_signals(&self) -> [(&'static str, &EpicsCaBackend<f64>, String); 2] {
        [
            (
                "acquire_time",
                self.acquire_time_rbv.as_ref(),
                join(&self.prefix, ACQUIRE_TIME_RBV),
            ),
            (
                "acquire_period",
                self.acquire_period_rbv.as_ref(),
                join(&self.prefix, ACQUIRE_PERIOD_RBV),
            ),
        ]
    }

    /// Poll `DetectorState_RBV` until it reports a good terminal state —
    /// Idle or Aborted (ophyd-async `wait_for_good_state({IDLE, ABORTED})`)
    /// — or `timeout` elapses.
    pub async fn wait_for_idle(&self, timeout: Duration) -> Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let state =
                SignalBackend::<String>::get_value(self.detector_state_rbv.as_ref()).await?;
            if state == AD_STATE_IDLE || state == AD_STATE_ABORTED {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(BsrsError::Backend(format!(
                    "{}DetectorState_RBV did not reach Idle/Aborted within {:?} (last={state:?})",
                    self.prefix, timeout
                )));
            }
            tokio::time::sleep(WARMUP_IDLE_POLL).await;
        }
    }

    /// ophyd-async-style warmup: acquire exactly one frame so the
    /// downstream HDF5 writer can discover array dimensions before
    /// `Capture`. Snapshots `ImageMode`/`NumImages`, switches to
    /// `Single`+1, fires Acquire, waits for `DetectorState_RBV=Idle`,
    /// then restores the original values.
    pub async fn warmup(&self) -> Result<()> {
        let prev_image_mode = SignalBackend::<String>::get_value(self.image_mode.as_ref()).await?;
        let prev_num_images = SignalBackend::<i64>::get_value(self.num_images.as_ref()).await?;

        await_put(
            SignalBackend::<String>::put(
                self.image_mode.as_ref(),
                Some(AD_IMAGE_MODE_SINGLE.to_string()),
            ),
            "warmup: set ImageMode=Single",
        )
        .await?;
        await_put(
            SignalBackend::<i64>::put(self.num_images.as_ref(), Some(1)),
            "warmup: set NumImages=1",
        )
        .await?;
        // `wait = true` is critical here: with put-callback semantics
        // the bo record's processing chain (acquisition busy) only
        // releases when the IOC reports Idle again. Without it, a
        // fire-and-forget put returns before `DetectorState_RBV`
        // has even transitioned to Acquire, and `wait_for_idle` then
        // samples a stale Idle and returns immediately — the test
        // sees zero frames acquired.
        await_put(
            SignalBackend::<bool>::put(self.acquire.as_ref(), Some(true)),
            "warmup: trigger Acquire",
        )
        .await?;

        // Belt-and-braces: even with put-callback, some sim drivers
        // don't tie the busy flag to DetectorState. Poll the RBV.
        self.wait_for_idle(WARMUP_IDLE_TIMEOUT).await?;

        // Restore. Failures here are best-effort: log via Err in the
        // outer Result so callers see them.
        await_put(
            SignalBackend::<String>::put(self.image_mode.as_ref(), Some(prev_image_mode)),
            "warmup: restore ImageMode",
        )
        .await?;
        await_put(
            SignalBackend::<i64>::put(self.num_images.as_ref(), Some(prev_num_images)),
            "warmup: restore NumImages",
        )
        .await?;
        Ok(())
    }
}

/// Generic NDPlugin-base handle — `EnableCallbacks`, `BlockingCallbacks`,
/// `NDArrayPort`, `QueueSize`. Every concrete plugin (`NdFile`,
/// `NdStats`, `NdRoi`) embeds one of these.
pub struct NdPlugin {
    /// PV prefix, e.g. `"13SIM1:Stats1:"`.
    pub prefix: String,
    /// `EnableCallbacks` (bo: 0=Disable, 1=Enable).
    pub enable_callbacks: Arc<EpicsCaBackend<bool>>,
    /// `BlockingCallbacks` (bo: 0=No, 1=Yes).
    pub blocking_callbacks: Arc<EpicsCaBackend<bool>>,
    /// `NDArrayPort` (stringout) — name of the upstream port from
    /// which this plugin consumes NDArrays.
    pub nd_array_port: Arc<EpicsCaBackend<String>>,
    /// `QueueSize` (longout).
    pub queue_size: Arc<EpicsCaBackend<i64>>,
    /// Staged configuration (ophyd `stage_sigs`): values applied when this
    /// plugin is staged and reverted on unstage. Populated via
    /// [`NdPlugin::stage_enabled`] / [`NdPlugin::stage_source_port`] and by
    /// [`select_save_plugin`] / [`num_rois`].
    pub stage_sigs: StageSigs,
}

impl NdPlugin {
    /// Build the handle. Does NOT connect.
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self {
            enable_callbacks: Arc::new(EpicsCaBackend::<bool>::new(join(
                &prefix,
                "EnableCallbacks",
            ))),
            blocking_callbacks: Arc::new(EpicsCaBackend::<bool>::new(join(
                &prefix,
                "BlockingCallbacks",
            ))),
            // NDArrayPort is a port name (short string) — DBR_STRING is fine.
            nd_array_port: Arc::new(EpicsCaBackend::<String>::new(join(&prefix, "NDArrayPort"))),
            queue_size: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "QueueSize"))),
            stage_sigs: StageSigs::new(),
            prefix,
        }
    }

    /// Record `EnableCallbacks = enabled` into this plugin's `stage_sigs`, so
    /// it is applied on stage and reverted on unstage.
    pub fn stage_enabled(&self, enabled: bool) {
        self.stage_sigs.set(
            self.enable_callbacks.clone() as Arc<dyn SignalBackend<bool>>,
            enabled,
            join(&self.prefix, "EnableCallbacks"),
        );
    }

    /// Record `NDArrayPort = port` into this plugin's `stage_sigs`, so the
    /// re-route is applied on stage and reverted on unstage.
    pub fn stage_source_port(&self, port: impl Into<String>) {
        self.stage_sigs.set(
            self.nd_array_port.clone() as Arc<dyn SignalBackend<String>>,
            port.into(),
            join(&self.prefix, "NDArrayPort"),
        );
    }

    /// Connect all four channels.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        wait_for_connection([
            (
                "enable_callbacks",
                SignalBackend::<bool>::connect(self.enable_callbacks.as_ref(), timeout),
            ),
            (
                "blocking_callbacks",
                SignalBackend::<bool>::connect(self.blocking_callbacks.as_ref(), timeout),
            ),
            (
                "nd_array_port",
                SignalBackend::<String>::connect(self.nd_array_port.as_ref(), timeout),
            ),
            (
                "queue_size",
                SignalBackend::<i64>::connect(self.queue_size.as_ref(), timeout),
            ),
        ])
        .await
    }

    /// Set `EnableCallbacks`.
    pub async fn set_enabled(&self, enabled: bool) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.enable_callbacks.as_ref(), Some(enabled)),
            "NdPlugin::set_enabled",
        )
        .await
    }

    /// Set `BlockingCallbacks`.
    pub async fn set_blocking(&self, blocking: bool) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.blocking_callbacks.as_ref(), Some(blocking)),
            "NdPlugin::set_blocking",
        )
        .await
    }

    /// Set `NDArrayPort` — re-route this plugin to consume frames
    /// from a different upstream.
    pub async fn set_source_port(&self, port: &str) -> Result<()> {
        await_put(
            SignalBackend::<String>::put(self.nd_array_port.as_ref(), Some(port.to_string())),
            "NdPlugin::set_source_port",
        )
        .await
    }
}

impl NamedObj for NdPlugin {
    fn name(&self) -> &str {
        &self.prefix
    }
}

#[async_trait::async_trait]
impl StageableObj for NdPlugin {
    async fn stage_dyn(&self) -> Result<()> {
        self.stage_sigs.stage().await
    }
    async fn unstage_dyn(&self) -> Result<()> {
        self.stage_sigs.unstage().await
    }
}

/// `NDFile*` plugin — file writer (HDF5/TIFF/JPEG/etc.) handle. Adds
/// long-string `FilePath`/`FileName`/`FileTemplate`,
/// `AutoIncrement`, `FileWriteMode`, and `Capture` to the
/// `NdPlugin` base.
pub struct NdFile {
    /// Embedded plugin-base handle.
    pub plugin: NdPlugin,
    /// `FilePath` (CHAR waveform) — directory.
    pub file_path: Arc<EpicsCaBackend<String>>,
    /// `FileName` (CHAR waveform) — file basename.
    pub file_name: Arc<EpicsCaBackend<String>>,
    /// `FileTemplate` (CHAR waveform) — printf-style template
    /// applied to FilePath/FileName/FileNumber.
    pub file_template: Arc<EpicsCaBackend<String>>,
    /// `AutoIncrement` (bo).
    pub auto_increment: Arc<EpicsCaBackend<bool>>,
    /// `FileWriteMode` (mbbo: Single, Capture, Stream).
    pub file_write_mode: Arc<EpicsCaBackend<String>>,
    /// `Capture` (busy) — start/stop capture in Capture/Stream mode. A
    /// put-callback for `1` completes only when capture ENDS, so
    /// start/stop watch [`capture_rbv`](Self::capture_rbv) instead.
    pub capture: Arc<EpicsCaBackend<bool>>,
    /// `Capture_RBV` (bi) — whether the plugin is capturing.
    pub capture_rbv: Arc<EpicsCaBackend<bool>>,
    /// `NumCapture` (longout) — number of frames to capture in
    /// Capture/Stream mode (`0` = capture until `Capture` is cleared).
    pub num_capture: Arc<EpicsCaBackend<i64>>,
    /// `NumCaptured_RBV` (longin) — frames written to the file so far.
    /// This is the per-frame write index the `DetectorWriter` reports.
    pub num_captured_rbv: Arc<EpicsCaBackend<i64>>,
    /// `FullFileName_RBV` (CHAR waveform) — absolute path the IOC
    /// resolved from `FilePath`/`FileName`/`FileTemplate`/`FileNumber`.
    /// The `StreamResource` URI is built from this readback so it points
    /// at the file the IOC actually wrote, not at a re-derived guess.
    pub full_file_name_rbv: Arc<EpicsCaBackend<String>>,
    /// `FileNumber` (longout) — next file's sequence number, substituted
    /// into `FileTemplate`.
    pub file_number: Arc<EpicsCaBackend<i64>>,
    /// `FilePathExists_RBV` (bi) — whether the IOC can see (and write)
    /// `FilePath`. Checked after configuring the paths.
    pub file_path_exists_rbv: Arc<EpicsCaBackend<bool>>,
    /// `CreateDirectory` (longout) — how many missing trailing path levels
    /// the IOC may create when `FilePath` is processed (`0` = none).
    pub create_directory: Arc<EpicsCaBackend<i64>>,
}

impl NdFile {
    /// Build the handle. Does NOT connect.
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        let plugin = NdPlugin::new(prefix.clone());
        Self {
            file_path: Arc::new(EpicsCaBackend::<String>::new(join(&prefix, "FilePath"))),
            file_name: Arc::new(EpicsCaBackend::<String>::new(join(&prefix, "FileName"))),
            file_template: Arc::new(EpicsCaBackend::<String>::new(join(&prefix, "FileTemplate"))),
            auto_increment: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "AutoIncrement"))),
            file_write_mode: Arc::new(EpicsCaBackend::<String>::new(join(
                &prefix,
                "FileWriteMode",
            ))),
            capture: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "Capture"))),
            capture_rbv: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "Capture_RBV"))),
            num_capture: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "NumCapture"))),
            num_captured_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "NumCaptured_RBV",
            ))),
            full_file_name_rbv: Arc::new(EpicsCaBackend::<String>::new(join(
                &prefix,
                "FullFileName_RBV",
            ))),
            file_number: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "FileNumber"))),
            file_path_exists_rbv: Arc::new(EpicsCaBackend::<bool>::new(join(
                &prefix,
                "FilePathExists_RBV",
            ))),
            create_directory: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "CreateDirectory",
            ))),
            plugin,
        }
    }

    /// Connect plugin base + every file-specific channel.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        wait_for_connection([
            (
                "plugin",
                futures::FutureExt::boxed(self.plugin.connect(timeout)),
            ),
            (
                "file_path",
                SignalBackend::<String>::connect(self.file_path.as_ref(), timeout),
            ),
            (
                "file_name",
                SignalBackend::<String>::connect(self.file_name.as_ref(), timeout),
            ),
            (
                "file_template",
                SignalBackend::<String>::connect(self.file_template.as_ref(), timeout),
            ),
            (
                "auto_increment",
                SignalBackend::<bool>::connect(self.auto_increment.as_ref(), timeout),
            ),
            (
                "file_write_mode",
                SignalBackend::<String>::connect(self.file_write_mode.as_ref(), timeout),
            ),
            (
                "capture",
                SignalBackend::<bool>::connect(self.capture.as_ref(), timeout),
            ),
            (
                "num_capture",
                SignalBackend::<i64>::connect(self.num_capture.as_ref(), timeout),
            ),
            (
                "num_captured_rbv",
                SignalBackend::<i64>::connect(self.num_captured_rbv.as_ref(), timeout),
            ),
            (
                "full_file_name_rbv",
                SignalBackend::<String>::connect(self.full_file_name_rbv.as_ref(), timeout),
            ),
            (
                "file_number",
                SignalBackend::<i64>::connect(self.file_number.as_ref(), timeout),
            ),
            (
                "file_path_exists_rbv",
                SignalBackend::<bool>::connect(self.file_path_exists_rbv.as_ref(), timeout),
            ),
            (
                "create_directory",
                SignalBackend::<i64>::connect(self.create_directory.as_ref(), timeout),
            ),
            (
                "capture_rbv",
                SignalBackend::<bool>::connect(self.capture_rbv.as_ref(), timeout),
            ),
        ])
        .await
    }

    /// Set `FilePath` — directory the writer drops files into.
    pub async fn set_path(&self, path: &str) -> Result<()> {
        await_put(
            SignalBackend::<String>::put(self.file_path.as_ref(), Some(path.to_string())),
            "NdFile::set_path",
        )
        .await
    }

    /// Set `FileName` — basename pre-template.
    pub async fn set_name(&self, name: &str) -> Result<()> {
        await_put(
            SignalBackend::<String>::put(self.file_name.as_ref(), Some(name.to_string())),
            "NdFile::set_name",
        )
        .await
    }

    /// Set `FileTemplate` — typical value `"%s%s_%6.6d.h5"`.
    pub async fn set_template(&self, template: &str) -> Result<()> {
        await_put(
            SignalBackend::<String>::put(self.file_template.as_ref(), Some(template.to_string())),
            "NdFile::set_template",
        )
        .await
    }

    /// Set `FileWriteMode` by label (`Single`, `Capture`, `Stream`).
    pub async fn set_write_mode(&self, mode: &str) -> Result<()> {
        await_put(
            SignalBackend::<String>::put(self.file_write_mode.as_ref(), Some(mode.to_string())),
            "NdFile::set_write_mode",
        )
        .await
    }

    /// Set `NumCapture` — frames to capture (`0` = until `Capture` cleared).
    pub async fn set_num_capture(&self, n: i64) -> Result<()> {
        await_put(
            SignalBackend::<i64>::put(self.num_capture.as_ref(), Some(n)),
            "NdFile::set_num_capture",
        )
        .await
    }

    /// Set `AutoIncrement` — bump `FileNumber` after each file.
    pub async fn set_auto_increment(&self, on: bool) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.auto_increment.as_ref(), Some(on)),
            "NdFile::set_auto_increment",
        )
        .await
    }

    /// Set `FileNumber` — the next file's sequence number.
    pub async fn set_file_number(&self, n: i64) -> Result<()> {
        await_put(
            SignalBackend::<i64>::put(self.file_number.as_ref(), Some(n)),
            "NdFile::set_file_number",
        )
        .await
    }

    /// Set `CreateDirectory` — missing trailing path levels the IOC may
    /// create. Must be set BEFORE `FilePath`: the directory-creation callback
    /// fires when the path PV is processed (ophyd-async
    /// `prepare_file_paths`, `_data_logic.py:127`).
    pub async fn set_create_directory(&self, depth: i64) -> Result<()> {
        await_put(
            SignalBackend::<i64>::put(self.create_directory.as_ref(), Some(depth)),
            "NdFile::set_create_directory",
        )
        .await
    }

    /// Read `FilePathExists_RBV` — whether the IOC can see the configured
    /// `FilePath`.
    pub async fn file_path_exists(&self) -> Result<bool> {
        SignalBackend::<bool>::get_value(self.file_path_exists_rbv.as_ref()).await
    }

    /// Start capture (`Capture=1`). In Stream mode the file opens on this
    /// transition (or on the first frame when `LazyOpen` is set). `Capture`
    /// is a busy record — its put-callback would complete only when capture
    /// ENDS — so the put is fire-and-forget and completion is
    /// `Capture_RBV=1` (ophyd-async `set_and_wait_for_value(capture, True,
    /// wait_for_set_completion=False)`).
    pub async fn start_capture(&self) -> Result<()> {
        set_and_wait_for_bool(
            self.capture_rbv.as_ref(),
            true,
            self.capture.put_nowait(true),
            "NdFile::start_capture: Capture_RBV",
        )
        .await
    }

    /// Stop capture (`Capture=0`) — flushes and closes the file. Same busy
    /// record: fire-and-forget the put, wait for `Capture_RBV=0`
    /// (ophyd-async `stop_busy_record`).
    pub async fn stop_capture(&self) -> Result<()> {
        set_and_wait_for_bool(
            self.capture_rbv.as_ref(),
            false,
            self.capture.put_nowait(false),
            "NdFile::stop_capture: Capture_RBV",
        )
        .await
    }

    /// Read `NumCaptured_RBV` — frames written so far (clamped to `>= 0`).
    pub async fn num_captured(&self) -> Result<u64> {
        let n = SignalBackend::<i64>::get_value(self.num_captured_rbv.as_ref()).await?;
        Ok(n.max(0) as u64)
    }

    /// Read `FullFileName_RBV` — the absolute path the IOC resolved, with any
    /// trailing NUL padding from the CHAR waveform stripped.
    pub async fn full_file_name(&self) -> Result<String> {
        let s = SignalBackend::<String>::get_value(self.full_file_name_rbv.as_ref()).await?;
        Ok(s.trim_end_matches('\0').to_string())
    }

    /// Point the plugin at `path_info`'s directory and filename with
    /// `template`, switch it to Stream mode with `AutoIncrement` on and
    /// `FileNumber` reset, and set `NumCapture`. Single owner of the put
    /// ordering (depth before path: the directory-creation callback fires when
    /// `FilePath` is processed) and of the `FilePathExists_RBV` check. Port of
    /// ophyd-async's `prepare_file_paths` (`_data_logic.py:124`).
    pub async fn configure(
        &self,
        path_info: &PathInfo,
        template: &str,
        num_capture: i64,
    ) -> Result<()> {
        // With the separator left off, areaDetector appends it and the
        // `FilePath` readback never matches the put. POSIX IOC hosts only:
        // ophyd-async's `PureWindowsPath` + `\` arm is not ported.
        let directory = format!("{}/", path_info.directory_path().display());
        self.set_create_directory(path_info.create_dir_depth())
            .await?;
        self.set_path(&directory).await?;
        self.set_name(path_info.filename()).await?;
        self.set_template(template).await?;
        self.set_auto_increment(true).await?;
        self.set_file_number(0).await?;
        self.set_write_mode(AD_FILE_WRITE_MODE_STREAM).await?;
        if !self.file_path_exists().await? {
            return Err(BsrsError::Backend(format!(
                "{}FilePath {directory} doesn't exist on the IOC host or is \
                 not writable",
                self.plugin.prefix
            )));
        }
        self.set_num_capture(num_capture).await?;
        Ok(())
    }
}

/// `NDFileHDF5` plugin — the generic [`NdFile`] channels plus the
/// HDF5-specific records (`NumFramesChunks`/`ChunkSizeAuto`). Mirrors
/// ophyd-async's `NDFileIO` vs `NDFileHDF5IO` split so the JPEG/TIFF file
/// plugins can share `NdFile` without growing records they don't have.
pub struct NdFileHdf5 {
    /// Embedded generic file-plugin handle.
    pub file: NdFile,
    /// `NumFramesChunks` (longout) — frames per HDF chunk, written back when
    /// the readback reports 0 (fresh IOC startup).
    pub num_frames_chunks: Arc<EpicsCaBackend<i64>>,
    /// `NumFramesChunks_RBV` (longin).
    pub num_frames_chunks_rbv: Arc<EpicsCaBackend<i64>>,
    /// `ChunkSizeAuto` (bo) — let the plugin derive chunking from the frame.
    pub chunk_size_auto: Arc<EpicsCaBackend<bool>>,
    /// `LazyOpen` (bo) — open the file on the first frame instead of on
    /// `Capture=1`, so no warmup frame is needed to size the dataset.
    pub lazy_open: Arc<EpicsCaBackend<bool>>,
    /// `SWMRMode` (bo) — single-writer/multiple-reader, so consumers can
    /// read the file while the IOC is still writing it.
    pub swmr_mode: Arc<EpicsCaBackend<bool>>,
    /// `NumExtraDims` (longout) — extra virtual dimensions (unused here).
    pub num_extra_dims: Arc<EpicsCaBackend<i64>>,
    /// `XMLFileName` (CHAR waveform) — custom HDF5 layout XML; cleared so
    /// the default `/entry/data/data` layout applies.
    pub xml_file_name: Arc<EpicsCaBackend<String>>,
    /// `FlushNow` (busy) — force the plugin to flush buffered frames to the
    /// file for SWMR readers. An `NDFileHDF5`-only record; the JPEG/TIFF
    /// plugins do not have it.
    pub flush_now: Arc<EpicsCaBackend<bool>>,
}

impl NdFileHdf5 {
    /// Build the handle. Does NOT connect.
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self {
            num_frames_chunks: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "NumFramesChunks",
            ))),
            num_frames_chunks_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "NumFramesChunks_RBV",
            ))),
            chunk_size_auto: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "ChunkSizeAuto"))),
            lazy_open: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "LazyOpen"))),
            swmr_mode: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "SWMRMode"))),
            num_extra_dims: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "NumExtraDims"))),
            xml_file_name: Arc::new(EpicsCaBackend::<String>::new(join(&prefix, "XMLFileName"))),
            flush_now: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "FlushNow"))),
            file: NdFile::new(prefix),
        }
    }

    /// Connect the embedded file plugin + the HDF5-specific channels.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        wait_for_connection([
            (
                "file",
                futures::FutureExt::boxed(self.file.connect(timeout)),
            ),
            (
                "num_frames_chunks",
                SignalBackend::<i64>::connect(self.num_frames_chunks.as_ref(), timeout),
            ),
            (
                "num_frames_chunks_rbv",
                SignalBackend::<i64>::connect(self.num_frames_chunks_rbv.as_ref(), timeout),
            ),
            (
                "chunk_size_auto",
                SignalBackend::<bool>::connect(self.chunk_size_auto.as_ref(), timeout),
            ),
            (
                "lazy_open",
                SignalBackend::<bool>::connect(self.lazy_open.as_ref(), timeout),
            ),
            (
                "swmr_mode",
                SignalBackend::<bool>::connect(self.swmr_mode.as_ref(), timeout),
            ),
            (
                "num_extra_dims",
                SignalBackend::<i64>::connect(self.num_extra_dims.as_ref(), timeout),
            ),
            (
                "xml_file_name",
                SignalBackend::<String>::connect(self.xml_file_name.as_ref(), timeout),
            ),
            (
                "flush_now",
                SignalBackend::<bool>::connect(self.flush_now.as_ref(), timeout),
            ),
        ])
        .await
    }

    /// The HDF5-specific arming puts of ophyd-async's `prepare_unbounded`
    /// (`_data_logic.py:185-191`): no extra virtual dims, lazy open (no
    /// warmup frame needed to size the dataset), SWMR on (readable while
    /// growing), custom layout XML cleared.
    pub async fn setup_hdf_writer(&self) -> Result<()> {
        let (a, b, c, d) = tokio::join!(
            await_put(
                SignalBackend::<i64>::put(self.num_extra_dims.as_ref(), Some(0)),
                "NdFileHdf5::setup: NumExtraDims=0",
            ),
            await_put(
                SignalBackend::<bool>::put(self.lazy_open.as_ref(), Some(true)),
                "NdFileHdf5::setup: LazyOpen=1",
            ),
            await_put(
                SignalBackend::<bool>::put(self.swmr_mode.as_ref(), Some(true)),
                "NdFileHdf5::setup: SWMRMode=1",
            ),
            await_put(
                SignalBackend::<String>::put(self.xml_file_name.as_ref(), Some(String::new())),
                "NdFileHdf5::setup: XMLFileName=\"\"",
            ),
        );
        a?;
        b?;
        c?;
        d?;
        Ok(())
    }

    /// Frames per HDF chunk, always `>= 1`. A fresh IOC reports 0 until the
    /// first capture; ophyd-async writes 1 back to the plugin in that case
    /// and uses 1 (`prepare_unbounded`, `_data_logic.py:180`).
    pub async fn frames_per_chunk(&self) -> Result<u64> {
        let n = SignalBackend::<i64>::get_value(self.num_frames_chunks_rbv.as_ref()).await?;
        if n <= 0 {
            await_put(
                SignalBackend::<i64>::put(self.num_frames_chunks.as_ref(), Some(1)),
                "NdFileHdf5::frames_per_chunk: NumFramesChunks=1",
            )
            .await?;
            return Ok(1);
        }
        Ok(n as u64)
    }

    /// Set `ChunkSizeAuto`.
    pub async fn set_chunk_size_auto(&self, on: bool) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.chunk_size_auto.as_ref(), Some(on)),
            "NdFileHdf5::set_chunk_size_auto",
        )
        .await
    }

    /// Force a flush (`FlushNow=1`) so SWMR readers see the frames written
    /// so far.
    pub async fn flush(&self) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.flush_now.as_ref(), Some(true)),
            "NdFileHdf5::flush",
        )
        .await
    }
}

impl NamedObj for NdFile {
    fn name(&self) -> &str {
        &self.plugin.prefix
    }
}

#[async_trait::async_trait]
impl StageableObj for NdFile {
    async fn stage_dyn(&self) -> Result<()> {
        self.plugin.stage_dyn().await
    }
    async fn unstage_dyn(&self) -> Result<()> {
        self.plugin.unstage_dyn().await
    }
}

/// `NDStats` plugin — adds `ComputeStatistics`/`ComputeCentroid`/
/// `ComputeProfiles`/`ComputeHistogram` to the `NdPlugin` base.
pub struct NdStats {
    /// Embedded plugin-base handle.
    pub plugin: NdPlugin,
    /// `ComputeStatistics` (bo).
    pub compute_statistics: Arc<EpicsCaBackend<bool>>,
    /// `ComputeCentroid` (bo).
    pub compute_centroid: Arc<EpicsCaBackend<bool>>,
    /// `ComputeProfiles` (bo).
    pub compute_profiles: Arc<EpicsCaBackend<bool>>,
    /// `ComputeHistogram` (bo).
    pub compute_histogram: Arc<EpicsCaBackend<bool>>,
    /// `Total_RBV` (ai) — the sum of the last frame's pixels.
    pub total: Arc<EpicsCaBackend<f64>>,
}

impl NdStats {
    /// Build the handle. Does NOT connect.
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        let plugin = NdPlugin::new(prefix.clone());
        Self {
            compute_statistics: Arc::new(EpicsCaBackend::<bool>::new(join(
                &prefix,
                "ComputeStatistics",
            ))),
            compute_centroid: Arc::new(EpicsCaBackend::<bool>::new(join(
                &prefix,
                "ComputeCentroid",
            ))),
            compute_profiles: Arc::new(EpicsCaBackend::<bool>::new(join(
                &prefix,
                "ComputeProfiles",
            ))),
            compute_histogram: Arc::new(EpicsCaBackend::<bool>::new(join(
                &prefix,
                "ComputeHistogram",
            ))),
            total: Arc::new(EpicsCaBackend::<f64>::new(join(&prefix, "Total_RBV"))),
            plugin,
        }
    }

    /// Connect plugin base + every stats-compute channel.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        wait_for_connection([
            (
                "plugin",
                futures::FutureExt::boxed(self.plugin.connect(timeout)),
            ),
            (
                "compute_statistics",
                SignalBackend::<bool>::connect(self.compute_statistics.as_ref(), timeout),
            ),
            (
                "compute_centroid",
                SignalBackend::<bool>::connect(self.compute_centroid.as_ref(), timeout),
            ),
            (
                "compute_profiles",
                SignalBackend::<bool>::connect(self.compute_profiles.as_ref(), timeout),
            ),
            (
                "compute_histogram",
                SignalBackend::<bool>::connect(self.compute_histogram.as_ref(), timeout),
            ),
            (
                "total",
                SignalBackend::<f64>::connect(self.total.as_ref(), timeout),
            ),
        ])
        .await
    }

    /// `EnableCallbacks = true` AND `ComputeStatistics = true`. Other
    /// compute flags are left untouched.
    pub async fn force_enable_stats(&self) -> Result<()> {
        self.plugin.set_enabled(true).await?;
        await_put(
            SignalBackend::<bool>::put(self.compute_statistics.as_ref(), Some(true)),
            "NdStats::force_enable_stats",
        )
        .await
    }
}

impl NamedObj for NdStats {
    fn name(&self) -> &str {
        &self.plugin.prefix
    }
}

#[async_trait::async_trait]
impl StageableObj for NdStats {
    async fn stage_dyn(&self) -> Result<()> {
        self.plugin.stage_dyn().await
    }
    async fn unstage_dyn(&self) -> Result<()> {
        self.plugin.unstage_dyn().await
    }
}

/// `NDROI` plugin — adds ROI bounds + per-axis enable flags.
pub struct NdRoi {
    /// Embedded plugin-base handle.
    pub plugin: NdPlugin,
    /// `MinX` (longout).
    pub min_x: Arc<EpicsCaBackend<i64>>,
    /// `MinY` (longout).
    pub min_y: Arc<EpicsCaBackend<i64>>,
    /// `SizeX` (longout).
    pub size_x: Arc<EpicsCaBackend<i64>>,
    /// `SizeY` (longout).
    pub size_y: Arc<EpicsCaBackend<i64>>,
    /// `SizeX_RBV` (longin): the output width, `SizeX` clipped to the input
    /// frame, set as a frame passes.
    pub size_x_rbv: Arc<EpicsCaBackend<i64>>,
    /// `SizeY_RBV` (longin): the output height, as `size_x_rbv`.
    pub size_y_rbv: Arc<EpicsCaBackend<i64>>,
    /// `EnableX` (bo).
    pub enable_x: Arc<EpicsCaBackend<bool>>,
    /// `EnableY` (bo).
    pub enable_y: Arc<EpicsCaBackend<bool>>,
}

impl NdRoi {
    /// Build the handle. Does NOT connect.
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        let plugin = NdPlugin::new(prefix.clone());
        Self {
            min_x: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "MinX"))),
            min_y: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "MinY"))),
            size_x: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "SizeX"))),
            size_y: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "SizeY"))),
            size_x_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "SizeX_RBV"))),
            size_y_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "SizeY_RBV"))),
            enable_x: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "EnableX"))),
            enable_y: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "EnableY"))),
            plugin,
        }
    }

    /// Connect plugin base + every ROI channel.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        wait_for_connection([
            (
                "plugin",
                futures::FutureExt::boxed(self.plugin.connect(timeout)),
            ),
            (
                "min_x",
                SignalBackend::<i64>::connect(self.min_x.as_ref(), timeout),
            ),
            (
                "min_y",
                SignalBackend::<i64>::connect(self.min_y.as_ref(), timeout),
            ),
            (
                "size_x",
                SignalBackend::<i64>::connect(self.size_x.as_ref(), timeout),
            ),
            (
                "size_y",
                SignalBackend::<i64>::connect(self.size_y.as_ref(), timeout),
            ),
            (
                "size_x_rbv",
                SignalBackend::<i64>::connect(self.size_x_rbv.as_ref(), timeout),
            ),
            (
                "size_y_rbv",
                SignalBackend::<i64>::connect(self.size_y_rbv.as_ref(), timeout),
            ),
            (
                "enable_x",
                SignalBackend::<bool>::connect(self.enable_x.as_ref(), timeout),
            ),
            (
                "enable_y",
                SignalBackend::<bool>::connect(self.enable_y.as_ref(), timeout),
            ),
        ])
        .await
    }

    /// Set the four ROI bounds at once.
    pub async fn set_bounds(&self, min_x: i64, min_y: i64, size_x: i64, size_y: i64) -> Result<()> {
        await_put(
            SignalBackend::<i64>::put(self.min_x.as_ref(), Some(min_x)),
            "NdRoi::set_bounds: MinX",
        )
        .await?;
        await_put(
            SignalBackend::<i64>::put(self.min_y.as_ref(), Some(min_y)),
            "NdRoi::set_bounds: MinY",
        )
        .await?;
        await_put(
            SignalBackend::<i64>::put(self.size_x.as_ref(), Some(size_x)),
            "NdRoi::set_bounds: SizeX",
        )
        .await?;
        await_put(
            SignalBackend::<i64>::put(self.size_y.as_ref(), Some(size_y)),
            "NdRoi::set_bounds: SizeY",
        )
        .await?;
        Ok(())
    }

    /// Set per-axis enable flags.
    pub async fn set_enabled_xy(&self, x: bool, y: bool) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.enable_x.as_ref(), Some(x)),
            "NdRoi::set_enabled_xy: EnableX",
        )
        .await?;
        await_put(
            SignalBackend::<bool>::put(self.enable_y.as_ref(), Some(y)),
            "NdRoi::set_enabled_xy: EnableY",
        )
        .await
    }
}

impl NamedObj for NdRoi {
    fn name(&self) -> &str {
        &self.plugin.prefix
    }
}

#[async_trait::async_trait]
impl StageableObj for NdRoi {
    async fn stage_dyn(&self) -> Result<()> {
        self.plugin.stage_dyn().await
    }
    async fn unstage_dyn(&self) -> Result<()> {
        self.plugin.unstage_dyn().await
    }
}

// Every plugin handle is a `Device` named by its prefix, so a detector can
// connect it with itself (`StandardDetector::add_plugin`).
macro_rules! plugin_device {
    ($($ty:ty => $($prefix:ident).+;)*) => {$(
        impl Device for $ty {
            fn name(&self) -> &str {
                &self.$($prefix).+
            }
            fn connect_all_boxed<'a>(
                &'a self,
                timeout: Duration,
            ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
                Box::pin(self.connect(timeout))
            }
        }
    )*};
}

plugin_device! {
    NdPlugin => prefix;
    NdFile => plugin.prefix;
    NdFileHdf5 => file.plugin.prefix;
    NdStats => plugin.prefix;
    NdRoi => plugin.prefix;
}

/// Route `file` to consume frames from `source_port` and enable its
/// callbacks, disabling every sibling in `siblings` (i.e. its
/// `EnableCallbacks` set to false). Useful when an IOC carries multiple
/// save plugins (HDF5 / TIFF / JPEG) but only one should be active per
/// scan.
///
/// The changes are recorded into each plugin's [`stage_sigs`](NdPlugin::stage_sigs)
/// rather than written immediately: they take effect when the plugins are
/// staged and revert on unstage, mirroring ophyd where plugin routing lives
/// in `stage_sigs`. Stage `file` **and every sibling** to apply the routing
/// (e.g. `Msg::Stage` each, or add them to the plan's stage list).
pub fn select_save_plugin(file: &NdFile, source_port: &str, siblings: &[&NdFile]) {
    file.plugin.stage_source_port(source_port);
    file.plugin.stage_enabled(true);
    for s in siblings {
        s.plugin.stage_enabled(false);
    }
}

/// Record "enable the first `n` ROIs, disable the rest" into each ROI's
/// [`stage_sigs`](NdPlugin::stage_sigs). Out-of-range `n` (greater than
/// `rois.len()`) is reported as an error so the caller can react if the index
/// was a typo. Like [`select_save_plugin`], the enable flags take effect when
/// the ROIs are staged and revert on unstage; stage every ROI to apply.
pub fn num_rois(rois: &[&NdRoi], n: usize) -> Result<()> {
    if n > rois.len() {
        return Err(BsrsError::Status(StatusError::Failed(format!(
            "num_rois: requested {n} but only {} ROIs available",
            rois.len()
        ))));
    }
    for (i, roi) in rois.iter().enumerate() {
        roi.plugin.stage_enabled(i < n);
    }
    Ok(())
}

// -- IOC-backed HDF file writer ----------------------------------------------

/// Default HDF5 dataset the NDFileHDF5 plugin writes frames into. Matches
/// ophyd-async's `parameters={"dataset": "/entry/data/data"}`.
const AD_HDF_DATASET: &str = "/entry/data/data";

/// `chunk_shape` for the scalar NDAttribute datasets — NDFileHDF5 writes them
/// in fixed 16384-element chunks (ophyd-async `_data_logic.py:216`).
const AD_HDF_ATTR_CHUNK: u64 = 16384;
/// Default `FileTemplate` — `<path><name>.h5`, no auto-increment suffix, so the
/// file is `PathInfo.directory_uri` + `filename` + `.h5`.
const AD_HDF_TEMPLATE: &str = "%s%s.h5";

/// `FileTemplate` base for the multipart (one-file-per-frame) writers —
/// `<path><name>_<6-digit frame number>`; the extension is appended per
/// plugin. Matches ophyd-async's `"%s%s_%6.6d" + self.extension`
/// (`ADMultipartDataLogic.prepare_unbounded`).
const AD_MULTIPART_TEMPLATE: &str = "%s%s_%6.6d";

/// The subset of an areaDetector file plugin the [`AdHdfWriter`] drives.
/// Splitting this out (mirroring ophyd-async's `NDFileHDF5IO` vs `ADHDFWriter`)
/// keeps the document-composition logic unit-testable against an in-memory fake
/// with no live IOC.
#[async_trait::async_trait]
pub trait AdFileIo: Send + Sync {
    /// Connect the underlying transport and install the frames-written monitor.
    async fn connect(&self, timeout: Duration) -> Result<()>;
    /// Point the plugin at `path_info`'s directory and filename with
    /// `template`, switch it to Stream mode with `AutoIncrement` on and
    /// `FileNumber` reset, and set `NumCapture` (`0` = until `stop_capture`).
    /// The IOC may create `path_info.create_dir_depth()` missing levels;
    /// errors if it then still cannot see the directory
    /// (`FilePathExists_RBV`). Port of ophyd-async's `prepare_file_paths`
    /// (`_data_logic.py:124`).
    async fn configure(&self, path_info: &PathInfo, template: &str, num_capture: i64)
        -> Result<()>;
    /// Enable the plugin's callbacks (`EnableCallbacks=1`) so it consumes
    /// frames from its NDArray port.
    async fn enable_callbacks(&self) -> Result<()>;
    /// Start capture — the plugin opens the file and accepts frames.
    async fn start_capture(&self) -> Result<()>;
    /// Stop capture — flush and close the file.
    async fn stop_capture(&self) -> Result<()>;
    /// Frames written so far (the per-frame write index).
    async fn num_captured(&self) -> Result<u64>;
    /// Watch the frames-written counter, for `complete()` in fly scans.
    fn observe_num_captured(&self) -> watch::Receiver<u64>;
}

/// The HDF5-specific extension of [`AdFileIo`] the [`AdHdfWriter`] needs on
/// top of the generic file-plugin operations (mirrors ophyd-async's
/// `NDFileHDF5IO` over `NDFileIO`).
#[async_trait::async_trait]
pub trait AdHdfFileIo: AdFileIo {
    /// Frames per HDF chunk, always `>= 1` (implementations write 1 back to
    /// the plugin when it reports 0 — a fresh IOC before its first capture).
    async fn frames_per_chunk(&self) -> Result<u64>;
    /// Set `ChunkSizeAuto` — let the plugin derive chunking from the frame.
    async fn set_chunk_size_auto(&self, on: bool) -> Result<()>;
    /// The HDF5-specific arming puts: `NumExtraDims=0`, `LazyOpen=1`,
    /// `SWMRMode=1`, `XMLFileName=""` (ophyd-async `prepare_unbounded`).
    async fn setup_hdf_writer(&self) -> Result<()>;
    /// Force a flush (`FlushNow=1`) so SWMR readers see the frames written so
    /// far. HDF5-only: `FlushNow` exists on `NDFileHDF5`, not on the
    /// JPEG/TIFF file plugins.
    async fn flush(&self) -> Result<()>;
}

/// [`AdHdfFileIo`] backed by a real [`NdFileHdf5`] over Channel Access.
pub struct NdFileIo {
    file: Arc<NdFileHdf5>,
    index_rx: watch::Receiver<u64>,
    index_tx: Arc<watch::Sender<u64>>,
    /// Kept alive so the `NumCaptured_RBV` monitor feeding `index_rx` is not
    /// torn down. Installed by [`connect`](AdFileIo::connect).
    token: std::sync::Mutex<Option<SubToken>>,
}

impl NdFileIo {
    /// Wrap an `NdFileHdf5`. Call [`connect`](AdFileIo::connect) before use.
    pub fn new(file: Arc<NdFileHdf5>) -> Self {
        let (tx, rx) = watch::channel(0u64);
        Self {
            file,
            index_rx: rx,
            index_tx: Arc::new(tx),
            token: std::sync::Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl AdFileIo for NdFileIo {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.file.connect(timeout).await?;
        let tx = self.index_tx.clone();
        let token = SignalBackend::<i64>::set_callback(
            self.file.file.num_captured_rbv.as_ref(),
            Some(Box::new(move |v: &i64, _ts, _alarm| {
                let _ = tx.send((*v).max(0) as u64);
            })),
        );
        *self.token.lock().unwrap() = Some(token);
        Ok(())
    }
    async fn configure(
        &self,
        path_info: &PathInfo,
        template: &str,
        num_capture: i64,
    ) -> Result<()> {
        self.file
            .file
            .configure(path_info, template, num_capture)
            .await
    }
    async fn enable_callbacks(&self) -> Result<()> {
        self.file.file.plugin.set_enabled(true).await
    }
    async fn start_capture(&self) -> Result<()> {
        self.file.file.start_capture().await
    }
    async fn stop_capture(&self) -> Result<()> {
        self.file.file.stop_capture().await
    }
    async fn num_captured(&self) -> Result<u64> {
        self.file.file.num_captured().await
    }
    fn observe_num_captured(&self) -> watch::Receiver<u64> {
        self.index_rx.clone()
    }
}

#[async_trait::async_trait]
impl AdHdfFileIo for NdFileIo {
    async fn frames_per_chunk(&self) -> Result<u64> {
        self.file.frames_per_chunk().await
    }
    async fn set_chunk_size_auto(&self, on: bool) -> Result<()> {
        self.file.set_chunk_size_auto(on).await
    }
    async fn setup_hdf_writer(&self) -> Result<()> {
        self.file.setup_hdf_writer().await
    }
    async fn flush(&self) -> Result<()> {
        self.file.flush().await
    }
}

/// [`AdFileIo`] backed by a generic [`NdFile`] over Channel Access — the
/// JPEG/TIFF file plugins, which have none of the HDF5-specific records
/// (mirrors ophyd-async's `NDPluginFileIO` vs `NDFileHDF5IO`).
pub struct NdPluginFileIo {
    file: Arc<NdFile>,
    index_rx: watch::Receiver<u64>,
    index_tx: Arc<watch::Sender<u64>>,
    /// Kept alive so the `NumCaptured_RBV` monitor feeding `index_rx` is not
    /// torn down. Installed by [`connect`](AdFileIo::connect).
    token: std::sync::Mutex<Option<SubToken>>,
}

impl NdPluginFileIo {
    /// Wrap an `NdFile`. Call [`connect`](AdFileIo::connect) before use.
    pub fn new(file: Arc<NdFile>) -> Self {
        let (tx, rx) = watch::channel(0u64);
        Self {
            file,
            index_rx: rx,
            index_tx: Arc::new(tx),
            token: std::sync::Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl AdFileIo for NdPluginFileIo {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.file.connect(timeout).await?;
        let tx = self.index_tx.clone();
        let token = SignalBackend::<i64>::set_callback(
            self.file.num_captured_rbv.as_ref(),
            Some(Box::new(move |v: &i64, _ts, _alarm| {
                let _ = tx.send((*v).max(0) as u64);
            })),
        );
        *self.token.lock().unwrap() = Some(token);
        Ok(())
    }
    async fn configure(
        &self,
        path_info: &PathInfo,
        template: &str,
        num_capture: i64,
    ) -> Result<()> {
        self.file.configure(path_info, template, num_capture).await
    }
    async fn enable_callbacks(&self) -> Result<()> {
        self.file.plugin.set_enabled(true).await
    }
    async fn start_capture(&self) -> Result<()> {
        self.file.start_capture().await
    }
    async fn stop_capture(&self) -> Result<()> {
        self.file.stop_capture().await
    }
    async fn num_captured(&self) -> Result<u64> {
        self.file.num_captured().await
    }
    fn observe_num_captured(&self) -> watch::Receiver<u64> {
        self.index_rx.clone()
    }
}

/// One per-frame NDAttribute the IOC's NDFileHDF5 plugin writes into the same
/// `.h5` alongside the main image, at `/entry/instrument/NDAttributes/<name>`.
/// Discovered from the IOC's `NDAttributesFile` XML at `open()` (see
/// [`NdAttributeXmlSource`]) or declared explicitly via
/// [`AdHdfWriter::with_ndattributes`]; the writer then emits a
/// `StreamResource`/`StreamDatum` for it just like ophyd-async's
/// `ndattribute_datasets` (`_data_logic.py`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NdAttributeDataset {
    /// Attribute name — used as both the `DataKey` and the HDF dataset leaf.
    pub name: String,
    /// numpy dtype string of the attribute values, e.g. `"<f8"`.
    pub dtype_numpy: String,
    /// `DataKey.source` for the attribute — `ca://<pv>` for `EPICS_PV`
    /// attributes; empty means "fall back to the file URI" (ophyd-async
    /// `resource.source or self.uri`, `_data_providers.py:127`).
    pub source: String,
}

impl NdAttributeDataset {
    /// Build a spec for attribute `name` with numpy dtype `dtype_numpy` and no
    /// source (the `DataKey.source` falls back to the file URI).
    pub fn new(name: impl Into<String>, dtype_numpy: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            dtype_numpy: dtype_numpy.into(),
            source: String::new(),
        }
    }
}

/// Map an NDAttribute `dbrtype` (an `EPICS_PV`-type attribute in the
/// `NDAttributesFile` XML) to a numpy dtype string. Port of ophyd-async's
/// `NDAttributePvDbrType` (`_ndattribute.py`). `DBR_NATIVE` is unsupported
/// there and here — returns `None`, as for unknown strings.
pub fn ndattr_dbrtype_to_numpy(dbrtype: &str) -> Option<&'static str> {
    Some(match dbrtype {
        "DBR_SHORT" | "DBR_ENUM" => "<i2",
        "DBR_INT" | "DBR_LONG" => "<i4",
        "DBR_FLOAT" => "<f4",
        "DBR_DOUBLE" => "<f8",
        "DBR_STRING" => "S40",
        "DBR_CHAR" => "|i1",
        _ => return None,
    })
}

/// Map an NDAttribute `datatype` (a `PARAM`-type attribute in the
/// `NDAttributesFile` XML) to a numpy dtype string. Port of ophyd-async's
/// `NDAttributeDataType` (`_ndattribute.py`).
pub fn ndattr_datatype_to_numpy(datatype: &str) -> Option<&'static str> {
    Some(match datatype {
        "INT" => "<i4",
        "INT64" => "<i8",
        "DOUBLE" => "<f8",
        "STRING" => "S40",
        _ => return None,
    })
}

/// Parse ADCore `NDAttributesFile` XML (`<Attributes><Attribute .../></Attributes>`)
/// into dataset specs, in document order. Port of ophyd-async's
/// `get_ndattribute_dtype_source` (`_data_logic.py:94`): `type` defaults to
/// `EPICS_PV`; an `EPICS_PV` attribute maps `dbrtype` (default `DBR_NATIVE`,
/// which is rejected) and sources from `ca://<source>`; any other type maps
/// `datatype` (default `INT`) with an empty source. Same-name duplicates are
/// resolved by the caller's merge, not here.
pub fn parse_ndattributes_xml(xml: &str) -> Result<Vec<NdAttributeDataset>> {
    let doc = roxmltree::Document::parse(xml)
        .map_err(|e| BsrsError::Backend(format!("NDAttributesFile XML parse error: {e}")))?;
    let mut out = Vec::new();
    for child in doc.root_element().children().filter(|n| n.is_element()) {
        let name = child.attribute("name").ok_or_else(|| {
            BsrsError::Backend("NDAttributesFile XML: <Attribute> without a name".to_string())
        })?;
        let (dtype_numpy, source) = if child.attribute("type").unwrap_or("EPICS_PV") == "EPICS_PV" {
            let dbrtype = child.attribute("dbrtype").unwrap_or("DBR_NATIVE");
            let dtype = ndattr_dbrtype_to_numpy(dbrtype).ok_or_else(|| {
                BsrsError::Backend(format!(
                    "NDAttribute {name} has dbrtype {dbrtype}, which is not supported"
                ))
            })?;
            let pv = child.attribute("source").ok_or_else(|| {
                BsrsError::Backend(format!("NDAttribute {name} (EPICS_PV) has no source"))
            })?;
            (dtype, format!("ca://{pv}"))
        } else {
            let datatype = child.attribute("datatype").unwrap_or("INT");
            let dtype = ndattr_datatype_to_numpy(datatype).ok_or_else(|| {
                BsrsError::Backend(format!(
                    "NDAttribute {name} has datatype {datatype}, which is not supported"
                ))
            })?;
            (dtype, String::new())
        };
        out.push(NdAttributeDataset {
            name: name.to_string(),
            dtype_numpy: dtype_numpy.to_string(),
            source,
        });
    }
    Ok(out)
}

/// Merge `new` specs into `acc` by attribute name: a later entry replaces an
/// earlier one in place, keeping first-seen order — the dict semantics of
/// ophyd-async's `get_ndattribute_dtype_source` accumulating across sources.
fn merge_ndattributes(acc: &mut Vec<NdAttributeDataset>, new: Vec<NdAttributeDataset>) {
    for a in new {
        if let Some(slot) = acc.iter_mut().find(|x| x.name == a.name) {
            *slot = a;
        } else {
            acc.push(a);
        }
    }
}

/// A source of ADCore `NDAttributesFile` content — the driver's or a plugin's
/// `NDAttributesFile` PV. `open()` reads each source and, when the content is
/// inline XML (contains `<Attributes>`, ADCore's own check), parses it into
/// per-frame attribute datasets; a filename is skipped, since the file lives on
/// the IOC host. Mirrors ophyd-async reading `(driver, *plugins)`
/// (`_data_logic.py:207`).
#[async_trait::async_trait]
pub trait NdAttributeXmlSource: Send + Sync {
    /// Current `NDAttributesFile` content: inline XML or a filename.
    async fn nd_attributes_xml(&self) -> Result<String>;
}

#[async_trait::async_trait]
impl NdAttributeXmlSource for EpicsCaBackend<String> {
    async fn nd_attributes_xml(&self) -> Result<String> {
        let v = SignalBackend::<String>::get_value(self).await?;
        Ok(v.trim_end_matches('\0').to_string())
    }
}

/// Compose a `StreamResource` for an HDF dataset in the IOC's file.
fn ad_stream_resource(
    uid: String,
    data_key: String,
    uri: &str,
    dataset: &str,
    chunk_shape: &[u64],
) -> StreamResource {
    let mut parameters = HashMap::new();
    parameters.insert(
        "dataset".to_string(),
        serde_json::Value::String(dataset.to_string()),
    );
    parameters.insert(
        "chunk_shape".to_string(),
        serde_json::Value::Array(
            chunk_shape
                .iter()
                .map(|&d| serde_json::Value::from(d))
                .collect(),
        ),
    );
    StreamResource {
        uid,
        data_key,
        mimetype: "application/x-hdf5".to_string(),
        uri: uri.to_string(),
        parameters,
        run_start: None,
    }
}

/// Compose a `StreamDatum` covering frames `[start, stop)` for a resource.
/// `seq_nums` is left at `{0, 0}`: the run engine owns the sequence counter
/// and fills it at the Save/Collect drain (bluesky
/// `_pack_seq_nums_into_stream_datum`, bundlers.py:830).
fn ad_stream_datum(resource_uid: String, descriptor: &str, start: u64, stop: u64) -> StreamDatum {
    StreamDatum {
        uid: uuid::Uuid::new_v4().to_string(),
        stream_resource: resource_uid,
        descriptor: descriptor.to_string(),
        indices: StreamRange { start, stop },
        seq_nums: StreamRange { start: 0, stop: 0 },
    }
}

/// Emit state for [`AdHdfWriter`]: the data key and attribute set staged by
/// the last `open()` (attributes discovered from the XML sources, merged with
/// the declared specs), the once-emitted `StreamResource` UIDs (main image +
/// one per NDAttribute), and the event cursor (`StreamDatum`s cover events
/// `[last_emitted, up_to)`). `open()` is the only writer of `data_key` and
/// `attrs`; `collect_stream_docs` only reads them, so the emitted datasets
/// always match the data keys reported at `open()`.
struct AdEmitState {
    data_key: String,
    /// The `.h5` URI from the `PathInfo` staged by `open()`: the
    /// `StreamResource` URI and the attribute `DataKey.source` fallback.
    uri: String,
    attrs: Vec<NdAttributeDataset>,
    main_resource_uid: Option<String>,
    attr_resource_uids: HashMap<String, String>,
    last_emitted: u64,
    /// Frame shape discovered at `open()`, for the main resource's
    /// `chunk_shape` parameter.
    frame_shape: Vec<u64>,
    /// `NumFramesChunks` read at `open()` (always `>= 1`).
    frames_per_chunk: u64,
}

/// `DetectorWriter` for an areaDetector NDFileHDF5 plugin. The IOC writes the
/// actual `.h5`; this writer only arms the plugin and emits the
/// `StreamResource`/`StreamDatum` documents that point a downstream consumer
/// (e.g. a Tiled-writing process) at the IOC's file. Port of ophyd-async's
/// `ADHDFWriter` data-logic (`epics/adcore/_data_logic.py`).
pub struct AdHdfWriter {
    datakey_suffix: String,
    io: Arc<dyn AdHdfFileIo>,
    /// Driver-side frame description (`ArraySizeZ/Y/X_RBV` etc.), read at
    /// `open()` for the image `DataKey`'s shape/dtype.
    frame: Arc<dyn AdFrameInfoSource>,
    path_provider: Arc<dyn PathProvider>,
    /// `NDAttributesFile` sources (driver + extra plugins) read at `open()` to
    /// discover the per-frame NDAttribute datasets in the IOC's file.
    attr_sources: Vec<Arc<dyn NdAttributeXmlSource>>,
    /// Explicitly declared NDAttribute datasets, merged over the discovered set
    /// at `open()` (declared wins on a name collision). Covers the case where
    /// `NDAttributesFile` holds a filename on the IOC host that discovery
    /// cannot read.
    ndattributes: Vec<NdAttributeDataset>,
    /// Guards single-`StreamResource` emission and the datum cursor; `None`
    /// until the first `open()`.
    emit: tokio::sync::Mutex<Option<AdEmitState>>,
}

impl AdHdfWriter {
    /// Build a writer over `io`, describing frames per `frame`, writing files
    /// where `path_provider` says at each `open()`.
    pub fn new(
        io: Arc<dyn AdHdfFileIo>,
        frame: Arc<dyn AdFrameInfoSource>,
        path_provider: Arc<dyn PathProvider>,
    ) -> Self {
        Self {
            datakey_suffix: String::new(),
            io,
            frame,
            path_provider,
            attr_sources: Vec::new(),
            ndattributes: Vec::new(),
            emit: tokio::sync::Mutex::new(None),
        }
    }

    /// Append `suffix` to the detector's name for this writer's data key
    /// (ophyd-async `ADWriterFactory.hdf(datakey_suffix=...)`), e.g. `-roi1`
    /// for a second writer of one detector.
    pub fn with_datakey_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.datakey_suffix = suffix.into();
        self
    }

    /// Set the `NDAttributesFile` sources (driver + extra plugins) whose inline
    /// XML is parsed at `open()` to discover NDAttribute datasets — the
    /// ophyd-async `(driver, *plugins)` sweep (`_data_logic.py:207`).
    pub fn with_ndattribute_sources(mut self, sources: Vec<Arc<dyn NdAttributeXmlSource>>) -> Self {
        self.attr_sources = sources;
        self
    }

    /// Declare per-frame NDAttribute datasets explicitly, merged over the
    /// discovered set at `open()` (declared wins on a name collision), so the
    /// writer emits a `StreamResource`/`StreamDatum` for each in addition to
    /// the main image.
    pub fn with_ndattributes(mut self, attrs: Vec<NdAttributeDataset>) -> Self {
        self.ndattributes = attrs;
        self
    }

    /// Effective NDAttribute set for this staging: every source's inline XML,
    /// merged in order, then the declared specs on top. A source whose content
    /// has no `<Attributes>` is a filename on the IOC host (ADCore's own
    /// check, `_data_logic.py:104`) — unreadable here, so it is skipped.
    async fn discover_ndattributes(&self) -> Result<Vec<NdAttributeDataset>> {
        let mut attrs = Vec::new();
        for src in &self.attr_sources {
            let text = src.nd_attributes_xml().await?;
            if text.contains("<Attributes>") {
                merge_ndattributes(&mut attrs, parse_ndattributes_xml(&text)?);
            } else if !text.trim().is_empty() {
                tracing::warn!(
                    ndattributes_file = %text,
                    "NDAttributesFile is a filename on the IOC host; cannot \
                     discover NDAttribute datasets from it — declare them via \
                     AdHdfWriter::with_ndattributes if they should be emitted"
                );
            }
        }
        merge_ndattributes(&mut attrs, self.ndattributes.clone());
        Ok(attrs)
    }
}

/// ophyd-async's `StreamResourceDataProvider.make_datakeys` dtype rule:
/// `"array" if collections_per_event > 1 or len(shape) > 1 else "number"`,
/// where `shape` is the full DataKey shape including the multiplier prefix.
fn stream_datakey_dtype(multiplier: u64, shape_len: usize) -> Dtype {
    if multiplier > 1 || shape_len > 1 {
        Dtype::Array
    } else {
        Dtype::Number
    }
}

#[async_trait::async_trait]
impl DetectorWriter for AdHdfWriter {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.io.connect(timeout).await
    }
    async fn open(&self, datakey_name: &str, multiplier: u32) -> Result<HashMap<String, DataKey>> {
        let multiplier = u64::from(multiplier.max(1));
        let path_info = self.path_provider.path_info(Some(datakey_name))?;
        // The file the `%s%s.h5` template names, as a reader resolves it
        // (ophyd-async `_data_logic.py:233`).
        let uri = format!("{}{}.h5", path_info.directory_uri(), path_info.filename());
        // Frames per HDF chunk — read before the setup puts, then let the
        // plugin derive the rest of the chunking from the frame
        // (ophyd-async `prepare_unbounded`, `_data_logic.py:180-186`).
        let frames_per_chunk = self.io.frames_per_chunk().await?;
        self.io.set_chunk_size_auto(true).await?;
        // HDF-specific arming (lazy open, SWMR, no extra dims, default
        // layout) and plugin callbacks, then the file paths — the
        // `prepare_unbounded` setup gather (`_data_logic.py:185-195`).
        self.io.setup_hdf_writer().await?;
        self.io.enable_callbacks().await?;
        // Configure + arm the IOC file plugin. NumCapture=0: capture until
        // close() clears Capture, matching a step scan whose frame count is
        // unknown until the plan ends.
        self.io.configure(&path_info, AD_HDF_TEMPLATE, 0).await?;
        self.io.start_capture().await?;
        // Discover the frame shape + dtype from the driver's ArraySizeZ/Y/X +
        // DataType + ColorMode RBVs. Requires a primed detector — the shape is
        // `[]` until the first frame flows (e.g. after
        // `AreaDetectorCam::warmup`).
        let (shape, dtype_numpy) = self.frame.frame_info().await?;
        // Discover the NDAttribute datasets from the NDAttributesFile sources,
        // merged with the declared specs.
        let attrs = self.discover_ndattributes().await?;
        // Fresh emit state for this staging; the staged data key and attribute
        // set are what collect_stream_docs will emit resources/datums for.
        *self.emit.lock().await = Some(AdEmitState {
            data_key: datakey_name.to_string(),
            uri: uri.clone(),
            attrs: attrs.clone(),
            main_resource_uid: None,
            attr_resource_uids: HashMap::new(),
            last_emitted: 0,
            frame_shape: shape.clone(),
            frames_per_chunk,
        });
        // DataKey shapes carry the frames-per-index prefix, `[multiplier,
        // *frame_shape]` for the image and `[multiplier]` for the scalar
        // NDAttributes (ophyd-async `make_datakeys`); the image source is the
        // file URI, not a PV.
        let image_shape: Vec<Option<u64>> =
            std::iter::once(multiplier).chain(shape).map(Some).collect();
        let mut out = HashMap::new();
        out.insert(
            datakey_name.to_string(),
            DataKey {
                source: uri.clone(),
                dtype: stream_datakey_dtype(multiplier, image_shape.len()),
                shape: image_shape,
                dtype_numpy: Some(dtype_numpy.into()),
                external: Some("STREAM:".into()),
                units: None,
                precision: None,
                object_name: None,
                dims: None,
                limits: None,
                choices: None,
            },
        );
        // One scalar external data key per staged NDAttribute dataset. Source
        // is the attribute's PV source, or the file URI when it has none
        // (ophyd-async `resource.source or self.uri`).
        for a in &attrs {
            out.insert(
                a.name.clone(),
                DataKey {
                    source: if a.source.is_empty() {
                        uri.clone()
                    } else {
                        a.source.clone()
                    },
                    dtype: stream_datakey_dtype(multiplier, 1),
                    shape: vec![Some(multiplier)],
                    dtype_numpy: Some(a.dtype_numpy.clone().into()),
                    external: Some("STREAM:".into()),
                    units: None,
                    precision: None,
                    object_name: None,
                    dims: None,
                    limits: None,
                    choices: None,
                },
            );
        }
        Ok(out)
    }
    fn observe_indices_written(&self) -> watch::Receiver<u64> {
        self.io.observe_num_captured()
    }
    async fn indices_written(&self) -> u64 {
        self.io.num_captured().await.unwrap_or(0)
    }
    fn collect_stream_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset> {
        let descriptor = descriptor.to_string();
        // Build the docs in one future (the emit lock and flush are async),
        // then flatten to a stream. `stream::once(..).flat_map(stream::iter)`
        // gives a `BoxStream` without pulling in a generator macro.
        let fut = async move {
            let mut docs: Vec<StreamAsset> = Vec::new();
            let mut guard = self.emit.lock().await;
            let Some(st) = guard.as_mut() else {
                return docs;
            };
            // ophyd-async `make_stream_docs`: flush so SWMR readers see the
            // frames these datums cover. The stream has no error channel.
            if let Err(e) = self.io.flush().await {
                tracing::warn!("{}: {e}", st.data_key);
            }
            // The data key and attribute set staged by the last open().
            let data_key = st.data_key.clone();
            let attrs = st.attrs.clone();
            // Emit each StreamResource once. The main image and every
            // NDAttribute live in the same .h5, so they share the URI staged
            // by open().
            let uri = st.uri.clone();
            if st.main_resource_uid.is_none() {
                let uid = uuid::Uuid::new_v4().to_string();
                st.main_resource_uid = Some(uid.clone());
                // The IOC chunks the image dataset [frames_per_chunk,
                // *frame_shape] (ophyd-async `_data_logic.py:88`).
                let chunk: Vec<u64> = std::iter::once(st.frames_per_chunk.max(1))
                    .chain(st.frame_shape.iter().copied())
                    .collect();
                docs.push(StreamAsset::Resource(ad_stream_resource(
                    uid,
                    data_key.clone(),
                    &uri,
                    AD_HDF_DATASET,
                    &chunk,
                )));
            }
            for a in &attrs {
                if !st.attr_resource_uids.contains_key(&a.name) {
                    let uid = uuid::Uuid::new_v4().to_string();
                    st.attr_resource_uids.insert(a.name.clone(), uid.clone());
                    let dataset = format!("/entry/instrument/NDAttributes/{}", a.name);
                    docs.push(StreamAsset::Resource(ad_stream_resource(
                        uid,
                        a.name.clone(),
                        &uri,
                        &dataset,
                        &[AD_HDF_ATTR_CHUNK],
                    )));
                }
            }
            // One StreamDatum per dataset (main + each attribute) for the new
            // events, all covering the same index range.
            if up_to > st.last_emitted {
                let start = st.last_emitted;
                st.last_emitted = up_to;
                let main_uid = st
                    .main_resource_uid
                    .clone()
                    .expect("main resource uid set above on first emission");
                docs.push(StreamAsset::Datum(ad_stream_datum(
                    main_uid,
                    &descriptor,
                    start,
                    up_to,
                )));
                for a in &attrs {
                    let auid = st
                        .attr_resource_uids
                        .get(&a.name)
                        .cloned()
                        .expect("attr resource uid set above");
                    docs.push(StreamAsset::Datum(ad_stream_datum(
                        auid,
                        &descriptor,
                        start,
                        up_to,
                    )));
                }
            }
            docs
        };
        futures::stream::once(fut)
            .flat_map(futures::stream::iter)
            .boxed()
    }
    async fn close(&self) -> Result<()> {
        self.io.stop_capture().await
    }
    /// ophyd-async `_data_logic.py`: the main NDArray dataset is always hinted.
    fn hinted_fields(&self, datakey_name: &str) -> Vec<String> {
        vec![datakey_name.to_string()]
    }
    fn datakey_suffix(&self) -> &str {
        &self.datakey_suffix
    }
}

/// Emit state for [`AdMultipartWriter`]: the data key staged by `open()`,
/// the once-emitted `StreamResource` UID, the event cursor, and the shape
/// staged by `open()`.
struct AdMultipartEmitState {
    data_key: String,
    /// `PathInfo.directory_uri` staged by `open()`: the `StreamResource` URI
    /// and the `DataKey.source` (the frames' filenames are in `template`).
    directory_uri: String,
    /// The `template` `StreamResource` parameter, Python-format style; the
    /// consumer substitutes the frame number:
    /// `path_info.filename + "_{:06d}" + extension`.
    template: String,
    resource_uid: Option<String>,
    last_emitted: u64,
    /// Frame shape discovered at `open()`, for the resource's `chunk_shape`
    /// parameter.
    frame_shape: Vec<u64>,
}

/// `DetectorWriter` for a multipart areaDetector file plugin (NDFileJPEG /
/// NDFileTIFF) — one file per frame under a common directory. The IOC writes
/// the actual files; this writer arms the plugin and emits a single
/// `StreamResource` whose URI is the *directory* and whose `template`
/// parameter reconstructs each frame's filename. Port of ophyd-async's
/// `ADMultipartDataLogic` (`epics/adcore/_data_logic.py:240`).
pub struct AdMultipartWriter {
    datakey_suffix: String,
    io: Arc<dyn AdFileIo>,
    /// Driver-side frame description (`ArraySizeZ/Y/X_RBV` etc.), read at
    /// `open()` for the image `DataKey`'s shape/dtype.
    frame: Arc<dyn AdFrameInfoSource>,
    path_provider: Arc<dyn PathProvider>,
    /// File extension including the dot, e.g. `".jpg"`.
    extension: String,
    /// `StreamResource` mimetype, e.g. `"multipart/related;type=image/jpeg"`.
    mimetype: String,
    /// Guards single-`StreamResource` emission and the datum cursor; `None`
    /// until the first `open()`.
    emit: tokio::sync::Mutex<Option<AdMultipartEmitState>>,
}

impl AdMultipartWriter {
    /// Build a writer over `io`, describing frames per `frame`, writing files
    /// where `path_provider` says at each `open()`, with the given file
    /// `extension` (dot included) and `StreamResource` `mimetype`.
    pub fn new(
        io: Arc<dyn AdFileIo>,
        frame: Arc<dyn AdFrameInfoSource>,
        path_provider: Arc<dyn PathProvider>,
        extension: impl Into<String>,
        mimetype: impl Into<String>,
    ) -> Self {
        Self {
            datakey_suffix: String::new(),
            io,
            frame,
            path_provider,
            extension: extension.into(),
            mimetype: mimetype.into(),
            emit: tokio::sync::Mutex::new(None),
        }
    }

    /// Append `suffix` to the detector's name for this writer's data key
    /// (ophyd-async `ADWriterFactory.jpeg/tiff(datakey_suffix=...)`).
    pub fn with_datakey_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.datakey_suffix = suffix.into();
        self
    }

    /// JPEG variant (`.jpg`, `multipart/related;type=image/jpeg`) — the
    /// `ADWriterType.JPEG` arm of ophyd-async's `make_writer_data_logic`.
    pub fn jpeg(
        io: Arc<dyn AdFileIo>,
        frame: Arc<dyn AdFrameInfoSource>,
        path_provider: Arc<dyn PathProvider>,
    ) -> Self {
        Self::new(
            io,
            frame,
            path_provider,
            ".jpg",
            "multipart/related;type=image/jpeg",
        )
    }

    /// TIFF variant (`.tiff`, `multipart/related;type=image/tiff`) — the
    /// `ADWriterType.TIFF` arm of ophyd-async's `make_writer_data_logic`.
    pub fn tiff(
        io: Arc<dyn AdFileIo>,
        frame: Arc<dyn AdFrameInfoSource>,
        path_provider: Arc<dyn PathProvider>,
    ) -> Self {
        Self::new(
            io,
            frame,
            path_provider,
            ".tiff",
            "multipart/related;type=image/tiff",
        )
    }
}

#[async_trait::async_trait]
impl DetectorWriter for AdMultipartWriter {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.io.connect(timeout).await
    }
    async fn open(&self, datakey_name: &str, multiplier: u32) -> Result<HashMap<String, DataKey>> {
        let multiplier = u64::from(multiplier.max(1));
        let path_info = self.path_provider.path_info(Some(datakey_name))?;
        // Plugin callbacks on, then the file paths. ophyd-async's
        // `ADMultipartDataLogic.prepare_unbounded` does not enable callbacks
        // itself (its plugin chain is armed elsewhere); bsrs's open() is the
        // only arming path, so it does — deliberate deviation.
        self.io.enable_callbacks().await?;
        // Configure + arm the IOC file plugin: one file per frame with a
        // 6-digit frame number, capture until close() (`NumCapture=0`).
        self.io
            .configure(
                &path_info,
                &format!("{AD_MULTIPART_TEMPLATE}{}", self.extension),
                0,
            )
            .await?;
        self.io.start_capture().await?;
        // Frame shape + dtype from the driver's readbacks (primed detector
        // required, as for the HDF writer).
        let (shape, dtype_numpy) = self.frame.frame_info().await?;
        *self.emit.lock().await = Some(AdMultipartEmitState {
            data_key: datakey_name.to_string(),
            directory_uri: path_info.directory_uri().to_string(),
            template: format!("{}_{{:06d}}{}", path_info.filename(), self.extension),
            resource_uid: None,
            last_emitted: 0,
            frame_shape: shape.clone(),
        });
        let image_shape: Vec<Option<u64>> =
            std::iter::once(multiplier).chain(shape).map(Some).collect();
        let mut out = HashMap::new();
        out.insert(
            datakey_name.to_string(),
            DataKey {
                source: path_info.directory_uri().to_string(),
                dtype: stream_datakey_dtype(multiplier, image_shape.len()),
                shape: image_shape,
                dtype_numpy: Some(dtype_numpy.into()),
                external: Some("STREAM:".into()),
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
    fn observe_indices_written(&self) -> watch::Receiver<u64> {
        self.io.observe_num_captured()
    }
    async fn indices_written(&self) -> u64 {
        self.io.num_captured().await.unwrap_or(0)
    }
    fn collect_stream_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset> {
        let descriptor = descriptor.to_string();
        let fut = async move {
            let mut docs: Vec<StreamAsset> = Vec::new();
            let mut guard = self.emit.lock().await;
            let Some(st) = guard.as_mut() else {
                return docs;
            };
            // Emit the one StreamResource on first use. The URI is the
            // directory; the consumer rebuilds each frame's filename from the
            // `template` parameter.
            if st.resource_uid.is_none() {
                let uid = uuid::Uuid::new_v4().to_string();
                st.resource_uid = Some(uid.clone());
                let mut parameters = HashMap::new();
                parameters.insert(
                    "template".to_string(),
                    serde_json::Value::String(st.template.clone()),
                );
                // One file per frame: chunk_shape [1, *frame_shape]
                // (ophyd-async `get_ndarray_resource_info` with its default
                // frames_per_chunk=1).
                let chunk: Vec<u64> = std::iter::once(1u64)
                    .chain(st.frame_shape.iter().copied())
                    .collect();
                parameters.insert(
                    "chunk_shape".to_string(),
                    serde_json::Value::Array(
                        chunk.iter().map(|&d| serde_json::Value::from(d)).collect(),
                    ),
                );
                docs.push(StreamAsset::Resource(StreamResource {
                    uid,
                    data_key: st.data_key.clone(),
                    mimetype: self.mimetype.clone(),
                    uri: st.directory_uri.clone(),
                    parameters,
                    run_start: None,
                }));
            }
            if up_to > st.last_emitted {
                let start = st.last_emitted;
                st.last_emitted = up_to;
                let uid = st
                    .resource_uid
                    .clone()
                    .expect("resource uid set above on first emission");
                docs.push(StreamAsset::Datum(ad_stream_datum(
                    uid,
                    &descriptor,
                    start,
                    up_to,
                )));
            }
            docs
        };
        futures::stream::once(fut)
            .flat_map(futures::stream::iter)
            .boxed()
    }
    async fn close(&self) -> Result<()> {
        self.io.stop_capture().await
    }
    /// ophyd-async `_data_logic.py`: the main NDArray dataset is always hinted.
    fn hinted_fields(&self, datakey_name: &str) -> Vec<String> {
        vec![datakey_name.to_string()]
    }
    fn datakey_suffix(&self) -> &str {
        &self.datakey_suffix
    }
}

/// The record values `prepare` derives from a [`TriggerInfo`], split out
/// from the CA I/O so the exposure policy (ophyd-async `prepare_exposures`,
/// `_trigger_logic.py`) is unit-testable without an IOC. `acquire_time` /
/// `acquire_period` are `None` when the trigger info does not pin them, in
/// which case `prepare` leaves the corresponding record untouched.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ExposurePlan {
    image_mode: &'static str,
    num_images: i64,
    acquire_time: Option<f64>,
    acquire_period: Option<f64>,
}

impl ExposurePlan {
    fn from_trigger_info(info: &TriggerInfo) -> Result<Self> {
        if info.trigger != DetectorTrigger::Internal {
            return Err(BsrsError::Backend(format!(
                "AreaDetectorCam supports only DetectorTrigger::Internal, got {:?}",
                info.trigger
            )));
        }
        // number_of_events == 0 means "acquire until told to stop"
        // (ophyd-async ADImageMode.CONTINUOUS when num == 0); NumImages is set
        // either way — the driver ignores it in Continuous mode.
        let num = info.number_of_exposures();
        let image_mode = if num == 0 {
            AD_IMAGE_MODE_CONTINUOUS
        } else {
            AD_IMAGE_MODE_MULTIPLE
        };
        let acquire_time = info.livetime.map(|lt| lt.as_secs_f64());
        // AcquirePeriod = exposure + dead time, set only when both are known
        // (ophyd-async's `if livetime and deadtime`). A bare deadtime with no
        // livetime leaves the period record as-is, matching ophyd-async.
        let acquire_period = match (info.livetime, info.deadtime) {
            (Some(lt), Some(dt)) => Some((lt + dt).as_secs_f64()),
            _ => None,
        };
        Ok(Self {
            image_mode,
            num_images: num as i64,
            acquire_time,
            acquire_period,
        })
    }
}

#[async_trait::async_trait]
impl DetectorControl for AreaDetectorCam {
    fn deadtime(&self, _exposure: Option<Duration>) -> Duration {
        // The generic areaDetector minimum dead-time is driver-specific; expose
        // zero here. Callers that need a real dead-time pass it via TriggerInfo.
        Duration::ZERO
    }
    async fn prepare(&self, info: TriggerInfo) -> Result<()> {
        // All exposure policy lives in ExposurePlan (pure, IOC-free
        // testable); prepare only issues the derived CA puts. A field left
        // `None` in the plan means "leave the record as-is" and its put is
        // skipped, mirroring ophyd-async's conditional sets.
        let plan = ExposurePlan::from_trigger_info(&info)?;
        await_put(
            SignalBackend::<String>::put(
                self.image_mode.as_ref(),
                Some(plan.image_mode.to_string()),
            ),
            "prepare: ImageMode",
        )
        .await?;
        await_put(
            SignalBackend::<i64>::put(self.num_images.as_ref(), Some(plan.num_images)),
            "prepare: NumImages",
        )
        .await?;
        if let Some(t) = plan.acquire_time {
            await_put(
                SignalBackend::<f64>::put(self.acquire_time.as_ref(), Some(t)),
                "prepare: AcquireTime",
            )
            .await?;
        }
        if let Some(p) = plan.acquire_period {
            await_put(
                SignalBackend::<f64>::put(self.acquire_period.as_ref(), Some(p)),
                "prepare: AcquirePeriod",
            )
            .await?;
        }
        Ok(())
    }
    async fn arm(&self) -> Status {
        // ophyd-async `ADAcquireLogic.start_acquiring`: armed once
        // Acquire_RBV reports acquiring. Acquire is a busy record whose
        // put-callback completes when the whole acquisition ends; that
        // completion is stashed for `wait_for_idle`, deliberately unbounded.
        // Acquire=1 is sent here, not in the spawned task, so a later
        // disarm's Acquire=0 cannot overtake it on the wire.
        match set_and_wait_for_bool(
            self.acquire_rbv.as_ref(),
            true,
            SignalBackend::<bool>::put_start(self.acquire.as_ref(), Some(true)),
            "arm: Acquire_RBV",
        )
        .await
        {
            Ok(done) => {
                let (acquired, setter) = Status::new();
                let prefix = self.prefix.clone();
                tokio::spawn(async move {
                    match done.await {
                        Ok(()) => setter.success(),
                        Err(e) => {
                            setter.fail(StatusError::Failed(format!("{prefix}arm: Acquire: {e}")))
                        }
                    }
                });
                *self.acquire_status.lock().unwrap() = Some(acquired);
                Status::done()
            }
            Err(e) => Status::fail(StatusError::Failed(format!("{}{e}", self.prefix))),
        }
    }
    async fn wait_for_idle(&self) -> Result<()> {
        // ophyd-async `ADAcquireLogic.wait_for_idle`: the acquisition's end,
        // then a good DetectorState.
        let acquired = self.acquire_status.lock().unwrap().clone();
        if let Some(acquired) = acquired {
            acquired.await?;
        }
        // Inherent `wait_for_idle(timeout)` takes priority in method resolution.
        self.wait_for_idle(WARMUP_IDLE_TIMEOUT).await
    }
    async fn disarm(&self) -> Result<()> {
        // stop_busy_record: a put-callback for Acquire=0 can deadlock on a
        // busy record, so fire-and-forget then wait for the readback.
        set_and_wait_for_bool(
            self.acquire_rbv.as_ref(),
            false,
            self.acquire.put_nowait(false),
            "disarm: Acquire_RBV",
        )
        .await
    }
    async fn read_configuration(&self, name: &str) -> Result<HashMap<String, ReadingValue>> {
        futures::future::try_join_all(self.config_signals().map(|(key, sig, _)| async move {
            Ok::<_, BsrsError>((
                format!("{name}_{key}"),
                SignalBackend::<f64>::get_reading(sig).await?,
            ))
        }))
        .await
        .map(HashMap::from_iter)
    }
    async fn describe_configuration(&self, name: &str) -> Result<HashMap<String, DataKey>> {
        // `get_datakey` takes the bare PV and adds the `ca://` scheme itself.
        futures::future::try_join_all(self.config_signals().map(|(key, sig, pv)| async move {
            Ok::<_, BsrsError>((
                format!("{name}_{key}"),
                SignalBackend::<f64>::get_datakey(sig, &pv).await?,
            ))
        }))
        .await
        .map(HashMap::from_iter)
    }
    async fn connect(&self, timeout: Duration) -> Result<()> {
        AreaDetectorCam::connect(self, timeout).await
    }
}

/// ophyd-async `PluginSignalDataLogic`: `signal`, a plugin's result, read
/// with the event of a one-collection prepare, which makes the driver wait
/// for its plugins so the signal holds the frame's result.
pub struct PluginSignalDataLogic<T, B, A = Read>
where
    T: Clone + Send + Sync + serde::Serialize + 'static,
    B: SignalBackend<T> + 'static,
    A: Readable,
{
    wait_for_plugins: Arc<EpicsCaBackend<bool>>,
    signal: Arc<Signal<T, B, A>>,
    hinted: bool,
}

impl<T, B, A> PluginSignalDataLogic<T, B, A>
where
    T: Clone + Send + Sync + serde::Serialize + 'static,
    B: SignalBackend<T> + 'static,
    A: Readable,
{
    /// Read `signal` with each event of `driver`'s detector, hinted.
    pub fn new(driver: &AreaDetectorCam, signal: Arc<Signal<T, B, A>>) -> Self {
        Self {
            wait_for_plugins: driver.wait_for_plugins.clone(),
            signal,
            hinted: true,
        }
    }

    /// Whether `signal` is hinted.
    pub fn hinted(mut self, hinted: bool) -> Self {
        self.hinted = hinted;
        self
    }
}

#[async_trait::async_trait]
impl<T, B, A> SingleDataLogic for PluginSignalDataLogic<T, B, A>
where
    T: Clone + Send + Sync + serde::Serialize + 'static,
    B: SignalBackend<T> + 'static,
    A: Readable,
{
    async fn prepare_single(&self, _datakey_name: &str) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.wait_for_plugins.as_ref(), Some(true)),
            "PluginSignalDataLogic::prepare_single",
        )
        .await
    }
    async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        self.signal.describe().await
    }
    async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        self.signal.read_cached(Some(false)).await
    }
    fn hinted_fields(&self, _datakey_name: &str) -> Vec<String> {
        if self.hinted {
            vec![self.signal.name().to_string()]
        } else {
            Vec::new()
        }
    }
}

/// A `StandardDetector` composed of an areaDetector camera driver and an
/// NDFileHDF5 plugin: the cam arms/acquires, the HDF plugin (inside the IOC)
/// writes the `.h5`, and bsrs emits the pointing `StreamResource`/`StreamDatum`.
pub type AreaDetectorHdf = StandardDetector<AreaDetectorCam>;

/// Build an [`AreaDetectorHdf`] from a cam prefix and an HDF-plugin prefix,
/// writing files where `path_provider` says. Call [`StandardDetector::connect`]
/// before running a plan.
pub fn area_detector_hdf(
    name: impl Into<String>,
    cam_prefix: impl Into<String>,
    hdf_prefix: impl Into<String>,
    path_provider: Arc<dyn PathProvider>,
) -> AreaDetectorHdf {
    let name = name.into();
    let cam = AreaDetectorCam::new(cam_prefix);
    let io: Arc<dyn AdHdfFileIo> = Arc::new(NdFileIo::new(Arc::new(NdFileHdf5::new(hdf_prefix))));
    // The cam's NDAttributesFile is the discovery source for NDAttribute
    // datasets, as in ophyd-async's `(driver, *plugins)` sweep — extra plugin
    // sources can be added by composing AdHdfWriter manually. The cam's
    // ArraySize/DataType/ColorMode readbacks are the frame-shape source
    // (ophyd-async `make_writer_data_logic` shape_signals).
    let writer = AdHdfWriter::new(
        io,
        cam.frame_info.clone() as Arc<dyn AdFrameInfoSource>,
        path_provider,
    )
    .with_ndattribute_sources(vec![
        cam.nd_attributes_file.clone() as Arc<dyn NdAttributeXmlSource>
    ]);
    StandardDetector::new(name, cam, writer)
}

/// A `StandardDetector` composed of an areaDetector camera driver and a
/// multipart (one-file-per-frame) file plugin — NDFileJPEG or NDFileTIFF.
pub type AreaDetectorMultipart = StandardDetector<AreaDetectorCam>;

fn area_detector_multipart(
    name: String,
    cam_prefix: impl Into<String>,
    file_prefix: impl Into<String>,
    path_provider: Arc<dyn PathProvider>,
    make_writer: impl FnOnce(
        Arc<dyn AdFileIo>,
        Arc<dyn AdFrameInfoSource>,
        Arc<dyn PathProvider>,
    ) -> AdMultipartWriter,
) -> AreaDetectorMultipart {
    let cam = AreaDetectorCam::new(cam_prefix);
    let io: Arc<dyn AdFileIo> = Arc::new(NdPluginFileIo::new(Arc::new(NdFile::new(file_prefix))));
    let writer = make_writer(
        io,
        cam.frame_info.clone() as Arc<dyn AdFrameInfoSource>,
        path_provider,
    );
    StandardDetector::new(name, cam, writer)
}

/// Build an [`AreaDetectorMultipart`] over an NDFileJPEG plugin from a cam
/// prefix and the plugin prefix, writing files where `path_provider` says. Call
/// [`StandardDetector::connect`] before running a plan. The
/// `ADWriterType.JPEG` arm of ophyd-async's `make_writer_data_logic`.
pub fn area_detector_jpeg(
    name: impl Into<String>,
    cam_prefix: impl Into<String>,
    jpeg_prefix: impl Into<String>,
    path_provider: Arc<dyn PathProvider>,
) -> AreaDetectorMultipart {
    area_detector_multipart(
        name.into(),
        cam_prefix,
        jpeg_prefix,
        path_provider,
        AdMultipartWriter::jpeg,
    )
}

/// Build an [`AreaDetectorMultipart`] over an NDFileTIFF plugin — the
/// `ADWriterType.TIFF` arm of ophyd-async's `make_writer_data_logic`.
pub fn area_detector_tiff(
    name: impl Into<String>,
    cam_prefix: impl Into<String>,
    tiff_prefix: impl Into<String>,
    path_provider: Arc<dyn PathProvider>,
) -> AreaDetectorMultipart {
    area_detector_multipart(
        name.into(),
        cam_prefix,
        tiff_prefix,
        path_provider,
        AdMultipartWriter::tiff,
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn cam_pv_names_concat_prefix_and_suffix() {
        let cam = AreaDetectorCam::new("13SIM1:cam1:");
        assert_eq!(cam.prefix, "13SIM1:cam1:");
        // We can only observe PV names via the public fields; the
        // backend itself doesn't expose its pv string, but the
        // PV-name construction is unambiguous from the `join` helper.
        // Test the join helper directly.
        assert_eq!(join("13SIM1:cam1:", "ImageMode"), "13SIM1:cam1:ImageMode");
        assert_eq!(
            join("13SIM1:cam1:", "DetectorState_RBV"),
            "13SIM1:cam1:DetectorState_RBV"
        );
    }

    #[test]
    fn nd_file_builds_long_string_for_file_path() {
        // Constructor must not panic.
        let f = NdFile::new("13SIM1:HDF1:");
        // Smoke-test the helper structure: plugin and file_path exist.
        assert_eq!(f.plugin.prefix, "13SIM1:HDF1:");
        let _ = f.file_path.clone();
        let _ = f.file_template.clone();
    }

    #[test]
    fn ad_state_idle_matches_template() {
        // The mbbi/mbbo labels in ADBase.template (DetectorState_RBV ZRST,
        // TEST; ImageMode ZRST/ONST/TWST).
        assert_eq!(AD_STATE_IDLE, "Idle");
        assert_eq!(AD_STATE_ABORTED, "Aborted");
        assert_eq!(AD_IMAGE_MODE_SINGLE, "Single");
        assert_eq!(AD_IMAGE_MODE_MULTIPLE, "Multiple");
        assert_eq!(AD_IMAGE_MODE_CONTINUOUS, "Continuous");
    }

    // ExposurePlan boundary tests — the IOC-free half of prepare().
    // TriggerInfo::number_of_exposures() = number_of_events *
    // collections_per_event * exposures_per_collection.

    /// A bounded burst (number_of_events > 0) arms MULTIPLE; NumImages counts
    /// every exposure across collections/averaging.
    #[test]
    fn exposure_plan_bounded_burst_is_multiple() {
        let info = TriggerInfo {
            number_of_events: 3,
            collections_per_event: 2,
            exposures_per_collection: 4,
            ..Default::default()
        };
        let plan = ExposurePlan::from_trigger_info(&info).unwrap();
        assert_eq!(plan.image_mode, AD_IMAGE_MODE_MULTIPLE);
        assert_eq!(plan.num_images, 24);
    }

    /// number_of_events == 0 ("acquire until stopped") arms CONTINUOUS;
    /// NumImages is 0 but the driver ignores it in that mode.
    #[test]
    fn exposure_plan_zero_events_is_continuous() {
        let info = TriggerInfo {
            number_of_events: 0,
            ..Default::default()
        };
        let plan = ExposurePlan::from_trigger_info(&info).unwrap();
        assert_eq!(plan.image_mode, AD_IMAGE_MODE_CONTINUOUS);
        assert_eq!(plan.num_images, 0);
    }

    /// No livetime → neither AcquireTime nor AcquirePeriod is pinned (both
    /// records left as-is).
    #[test]
    fn exposure_plan_no_livetime_pins_no_times() {
        let info = TriggerInfo {
            livetime: None,
            deadtime: Some(Duration::from_millis(1)),
            ..Default::default()
        };
        let plan = ExposurePlan::from_trigger_info(&info).unwrap();
        assert_eq!(plan.acquire_time, None);
        assert_eq!(plan.acquire_period, None);
    }

    /// Livetime alone pins AcquireTime; AcquirePeriod needs deadtime too.
    #[test]
    fn exposure_plan_livetime_only_pins_time_not_period() {
        let info = TriggerInfo {
            livetime: Some(Duration::from_millis(100)),
            deadtime: None,
            ..Default::default()
        };
        let plan = ExposurePlan::from_trigger_info(&info).unwrap();
        assert_eq!(plan.acquire_time, Some(0.1));
        assert_eq!(plan.acquire_period, None);
    }

    /// Livetime + deadtime pins AcquirePeriod = livetime + deadtime.
    #[test]
    fn exposure_plan_livetime_and_deadtime_pins_period() {
        let info = TriggerInfo {
            livetime: Some(Duration::from_millis(100)),
            deadtime: Some(Duration::from_millis(5)),
            ..Default::default()
        };
        let plan = ExposurePlan::from_trigger_info(&info).unwrap();
        assert_eq!(plan.acquire_time, Some(0.1));
        assert_eq!(plan.acquire_period, Some(0.105));
    }

    /// AreaDetectorCam is internal-trigger only; any external trigger is
    /// rejected before any put is derived.
    #[test]
    fn exposure_plan_rejects_external_trigger() {
        let info = TriggerInfo {
            trigger: DetectorTrigger::ExternalEdge,
            ..Default::default()
        };
        let err = ExposurePlan::from_trigger_info(&info).unwrap_err();
        assert!(err.to_string().contains("Internal"), "got: {err}");
    }

    // -------------------------------------------------------------
    // Live-IOC smoke tests. Default target is
    // `epics-rs/examples/sim-detector`; they also run against the
    // mini-beamline MovingDot detector (`mini:dot:`, asyn port `DOT`).
    // Marked `#[ignore]` so they only run with `--ignored`.
    //
    // Setup (sim-detector):
    //   cd ~/codes/epics-rs/examples/sim-detector
    //   cargo run --bin sim-ioc --features ioc -- ioc/st.cmd
    //
    // Then in another shell:
    //   cargo nextest run -p bsrs --features host --run-ignored only \
    //       -E 'kind(lib) & test(/^host::/)'
    //
    // Retarget with env overrides (defaults are sim-detector `SIM1:` /
    // `SIM1`); for the mini-beamline:
    //   BSRS_AD_PREFIX=mini:dot:  BSRS_AD_PORT=DOT
    // -------------------------------------------------------------

    /// Held for a live test's whole run. The tests drive one IOC's cam and
    /// check it is restored afterwards, so they take turns, across threads
    /// (`cargo test`) and processes (nextest) alike.
    pub(crate) fn lock_live_ioc() -> std::fs::File {
        let path = std::env::temp_dir().join("bsrs-live-ad-ioc.lock");
        let file = std::fs::File::create(path).expect("live IOC lock file");
        file.lock().expect("live IOC lock");
        file
    }

    pub(crate) fn ad_prefix() -> String {
        std::env::var("BSRS_AD_PREFIX").unwrap_or_else(|_| "SIM1:".to_string())
    }

    /// The detector's asyn port name — the `NDArrayPort` source that
    /// `select_save_plugin` routes file plugins to — overridable with
    /// `BSRS_AD_PORT`. Defaults to sim-detector's `SIM1`; use `DOT` for
    /// the mini-beamline MovingDot detector.
    fn ad_port() -> String {
        std::env::var("BSRS_AD_PORT").unwrap_or_else(|_| "SIM1".to_string())
    }

    /// Smoke: `AreaDetectorCam::warmup` must (a) acquire ≥ 1 frame —
    /// so `ArrayCounter_RBV` increments — and (b) leave the detector
    /// back at `DetectorState_RBV = Idle`. The cam should also be
    /// restored to its prior `ImageMode` and `NumImages` (warmup snaps
    /// and restores).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn ad_warmup_against_sim_detector() {
        let _ioc = lock_live_ioc();
        let prefix = ad_prefix();
        let cam = AreaDetectorCam::new(format!("{prefix}cam1:"));
        cam.connect(Duration::from_secs(5))
            .await
            .expect("connect cam1");

        let prev_mode = SignalBackend::<String>::get_value(cam.image_mode.as_ref())
            .await
            .expect("read ImageMode");
        let prev_num = SignalBackend::<i64>::get_value(cam.num_images.as_ref())
            .await
            .expect("read NumImages");
        let c0 = SignalBackend::<i64>::get_value(cam.array_counter_rbv.as_ref())
            .await
            .expect("read ArrayCounter_RBV pre");

        cam.warmup().await.expect("warmup");

        let c1 = SignalBackend::<i64>::get_value(cam.array_counter_rbv.as_ref())
            .await
            .expect("read ArrayCounter_RBV post");
        let state = SignalBackend::<String>::get_value(cam.detector_state_rbv.as_ref())
            .await
            .expect("read DetectorState_RBV post");
        let restored_mode = SignalBackend::<String>::get_value(cam.image_mode.as_ref())
            .await
            .expect("read ImageMode post");
        let restored_num = SignalBackend::<i64>::get_value(cam.num_images.as_ref())
            .await
            .expect("read NumImages post");

        eprintln!(
            "warmup smoke: counter {c0} -> {c1}, state={state}, \
             mode {prev_mode} -> {restored_mode}, num {prev_num} -> {restored_num}"
        );
        assert!(
            c1 > c0,
            "warmup did not acquire any frame (counter {c0} -> {c1})"
        );
        assert_eq!(
            state, AD_STATE_IDLE,
            "post-warmup DetectorState is not Idle"
        );
        assert_eq!(restored_mode, prev_mode, "warmup did not restore ImageMode");
        assert_eq!(restored_num, prev_num, "warmup did not restore NumImages");
    }

    /// `stage_sigs` lifecycle smoke: `select_save_plugin(hdf1, <port>, [jpeg1,
    /// tiff1, nexus1])` (port from `BSRS_AD_PORT`, default `SIM1`) records the
    /// routing into each plugin's `stage_sigs` **without touching the IOC**;
    /// staging then applies it — (a) `HDF1:NDArrayPort = <port>`,
    /// (b) `HDF1:EnableCallbacks` enabled, (c) siblings' `EnableCallbacks`
    /// disabled — and unstaging restores every signal to its pre-stage value.
    /// Also exercises the long-string `HDF1:FilePath` (CHAR waveform)
    /// get/put path.
    ///
    /// Non-destructive: FilePath, every plugin enable, and HDF1's port are
    /// captured and restored, so the IOC is left as it was found.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn ad_select_save_plugin_against_sim_detector() {
        let _ioc = lock_live_ioc();
        async fn en(f: &NdFile) -> bool {
            SignalBackend::<bool>::get_value(f.plugin.enable_callbacks.as_ref())
                .await
                .expect("read EnableCallbacks")
        }
        async fn port_of(f: &NdFile) -> String {
            SignalBackend::<String>::get_value(f.plugin.nd_array_port.as_ref())
                .await
                .expect("read NDArrayPort")
        }

        let prefix = ad_prefix();
        let hdf1 = NdFile::new(format!("{prefix}HDF1:"));
        let jpeg1 = NdFile::new(format!("{prefix}JPEG1:"));
        let tiff1 = NdFile::new(format!("{prefix}TIFF1:"));
        let nexus1 = NdFile::new(format!("{prefix}Nexus1:"));

        let timeout = Duration::from_secs(5);
        tokio::try_join!(
            hdf1.connect(timeout),
            jpeg1.connect(timeout),
            tiff1.connect(timeout),
            nexus1.connect(timeout),
        )
        .expect("connect file plugins");

        // (1) Long-string round-trip on FilePath (CHAR waveform),
        // capturing and restoring the original so the IOC is untouched.
        let orig_path = SignalBackend::<String>::get_value(hdf1.file_path.as_ref())
            .await
            .expect("read HDF1.FilePath orig");
        let path = "/tmp/bsrs_smoke/";
        hdf1.set_path(path).await.expect("HDF1.set_path");
        let read_back = SignalBackend::<String>::get_value(hdf1.file_path.as_ref())
            .await
            .expect("read HDF1.FilePath");
        eprintln!("file_path round-trip: {path:?} -> {read_back:?}");
        assert_eq!(
            read_back.trim_end_matches('\0'),
            path,
            "FilePath long-string round-trip mismatch"
        );
        hdf1.set_path(orig_path.trim_end_matches('\0'))
            .await
            .expect("restore HDF1.FilePath");

        // (2) Capture the pre-stage state of every signal the routing touches.
        let port = ad_port();
        let pre_hdf1_en = en(&hdf1).await;
        let pre_jpeg1_en = en(&jpeg1).await;
        let pre_tiff1_en = en(&tiff1).await;
        let pre_nexus1_en = en(&nexus1).await;
        let pre_hdf1_port = port_of(&hdf1).await;

        // Record the routing into stage_sigs — this must NOT write to the IOC.
        let siblings = [&jpeg1, &tiff1, &nexus1];
        select_save_plugin(&hdf1, &port, &siblings);
        assert_eq!(
            en(&hdf1).await,
            pre_hdf1_en,
            "select_save_plugin wrote to the IOC before staging"
        );

        // Stage: apply the recorded routing to hdf1 and every sibling.
        hdf1.stage_dyn().await.expect("stage HDF1");
        for s in &siblings {
            s.stage_dyn().await.expect("stage sibling");
        }

        let hdf1_port = port_of(&hdf1).await;
        let hdf1_en = en(&hdf1).await;
        let jpeg1_en = en(&jpeg1).await;
        let tiff1_en = en(&tiff1).await;
        let nexus1_en = en(&nexus1).await;
        eprintln!(
            "staged: HDF1.port={hdf1_port:?}, \
             enables HDF1={hdf1_en} JPEG1={jpeg1_en} TIFF1={tiff1_en} Nexus1={nexus1_en}"
        );
        assert_eq!(hdf1_port, port, "HDF1.NDArrayPort not routed to {port}");
        assert!(hdf1_en, "HDF1 was not enabled by staging");
        assert!(!jpeg1_en, "JPEG1 not disabled by staging");
        assert!(!tiff1_en, "TIFF1 not disabled by staging");
        assert!(!nexus1_en, "Nexus1 not disabled by staging");

        // Unstage (reverse order): restore every signal to its pre-stage value.
        for s in siblings.iter().rev() {
            s.unstage_dyn().await.expect("unstage sibling");
        }
        hdf1.unstage_dyn().await.expect("unstage HDF1");

        assert_eq!(
            en(&hdf1).await,
            pre_hdf1_en,
            "HDF1 enable not restored on unstage"
        );
        assert_eq!(en(&jpeg1).await, pre_jpeg1_en, "JPEG1 enable not restored");
        assert_eq!(en(&tiff1).await, pre_tiff1_en, "TIFF1 enable not restored");
        assert_eq!(
            en(&nexus1).await,
            pre_nexus1_en,
            "Nexus1 enable not restored"
        );
        assert_eq!(
            port_of(&hdf1).await,
            pre_hdf1_port,
            "HDF1 NDArrayPort not restored on unstage"
        );
        eprintln!("unstage restored HDF1 enable={pre_hdf1_en}, port={pre_hdf1_port:?}");
    }
}

#[cfg(test)]
mod ad_hdf_tests {
    use super::*;
    use crate::devices::{
        AutoIncrementFilenameProvider, AutoIncrementingPathProvider, StaticFilenameProvider,
        StaticPathProvider,
    };
    use crate::protocols_async::{Preparable, Stageable, Triggerable, WritesStreamAssets};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// What [`AdFileIo::configure`] received: `(path_info, template,
    /// num_capture)`.
    type ConfigureArgs = (PathInfo, String, i64);

    /// In-memory `AdFileIo` so the document-composition logic is testable with
    /// no live IOC (mirrors ophyd-async's IO-vs-writer split).
    struct FakeAdFileIo {
        captured: AtomicU64,
        index_tx: Arc<watch::Sender<u64>>,
        index_rx: watch::Receiver<u64>,
        configured: std::sync::Mutex<Option<ConfigureArgs>>,
        capturing: AtomicBool,
        flushes: AtomicU64,
        frames_per_chunk: AtomicU64,
        chunk_size_auto: AtomicBool,
        hdf_setup: AtomicBool,
        callbacks_enabled: AtomicBool,
        path_exists: AtomicBool,
    }

    impl FakeAdFileIo {
        fn new() -> Arc<Self> {
            let (tx, rx) = watch::channel(0u64);
            Arc::new(Self {
                captured: AtomicU64::new(0),
                index_tx: Arc::new(tx),
                index_rx: rx,
                configured: std::sync::Mutex::new(None),
                capturing: AtomicBool::new(false),
                flushes: AtomicU64::new(0),
                frames_per_chunk: AtomicU64::new(1),
                chunk_size_auto: AtomicBool::new(false),
                hdf_setup: AtomicBool::new(false),
                callbacks_enabled: AtomicBool::new(false),
                path_exists: AtomicBool::new(true),
            })
        }
        fn set_captured(&self, n: u64) {
            self.captured.store(n, Ordering::SeqCst);
            let _ = self.index_tx.send(n);
        }
    }

    #[async_trait::async_trait]
    impl AdFileIo for FakeAdFileIo {
        async fn connect(&self, _t: Duration) -> Result<()> {
            Ok(())
        }
        async fn configure(&self, path_info: &PathInfo, t: &str, n: i64) -> Result<()> {
            if !self.path_exists.load(Ordering::SeqCst) {
                return Err(BsrsError::Backend(format!(
                    "FilePath {} doesn't exist on the IOC host or is not writable",
                    path_info.directory_path().display()
                )));
            }
            *self.configured.lock().unwrap() = Some((path_info.clone(), t.into(), n));
            Ok(())
        }
        async fn enable_callbacks(&self) -> Result<()> {
            self.callbacks_enabled.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn start_capture(&self) -> Result<()> {
            self.capturing.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn stop_capture(&self) -> Result<()> {
            self.capturing.store(false, Ordering::SeqCst);
            Ok(())
        }
        async fn num_captured(&self) -> Result<u64> {
            Ok(self.captured.load(Ordering::SeqCst))
        }
        fn observe_num_captured(&self) -> watch::Receiver<u64> {
            self.index_rx.clone()
        }
    }

    #[async_trait::async_trait]
    impl AdHdfFileIo for FakeAdFileIo {
        async fn frames_per_chunk(&self) -> Result<u64> {
            Ok(self.frames_per_chunk.load(Ordering::SeqCst).max(1))
        }
        async fn set_chunk_size_auto(&self, on: bool) -> Result<()> {
            self.chunk_size_auto.store(on, Ordering::SeqCst);
            Ok(())
        }
        async fn setup_hdf_writer(&self) -> Result<()> {
            self.hdf_setup.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn flush(&self) -> Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Fixed frame description: a 20x10 (Y, X) `<u2` mono frame.
    struct FakeFrameInfo;

    #[async_trait::async_trait]
    impl AdFrameInfoSource for FakeFrameInfo {
        async fn frame_info(&self) -> Result<(Vec<u64>, String)> {
            Ok((vec![20, 10], "<u2".to_string()))
        }
    }

    /// `scan` files in `/data/scans`.
    fn scans() -> Arc<dyn PathProvider> {
        Arc::new(StaticPathProvider::new(
            Arc::new(StaticFilenameProvider::new("scan")),
            "/data/scans/",
        ))
    }

    fn writer_with(io: Arc<FakeAdFileIo>) -> AdHdfWriter {
        AdHdfWriter::new(io, Arc::new(FakeFrameInfo), scans())
    }

    fn multipart_writer_with(io: Arc<FakeAdFileIo>) -> AdMultipartWriter {
        AdMultipartWriter::jpeg(io, Arc::new(FakeFrameInfo), scans())
    }

    #[tokio::test]
    async fn file_writers_hint_their_image_key() {
        // ophyd-async `_data_logic.py:243`/`:299`: the main dataset is hinted.
        for (w, label) in [
            (
                Box::new(writer_with(FakeAdFileIo::new())) as Box<dyn DetectorWriter>,
                "hdf",
            ),
            (
                Box::new(multipart_writer_with(FakeAdFileIo::new())),
                "multipart",
            ),
        ] {
            let keys = w.open("det", 1).await.unwrap();
            let hinted = w.hinted_fields("det");
            assert_eq!(hinted, ["det"], "{label}");
            assert!(keys.contains_key(&hinted[0]), "{label}");
        }
    }

    /// Fixed-content `NDAttributesFile` source (inline XML or a filename).
    struct FakeXmlSource(String);

    #[async_trait::async_trait]
    impl NdAttributeXmlSource for FakeXmlSource {
        async fn nd_attributes_xml(&self) -> Result<String> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn ndarray_frame_info_orders_zyx_and_handles_color_modes() {
        let info = |dims: &[i64], dt: &str, cm: &str| {
            ndarray_frame_info(dims, dt, cm, "ca://P:DataType_RBV").map_err(|e| e.to_string())
        };
        // Mono 2-D frame: Z unused (0) → [Y, X].
        assert_eq!(
            info(&[0, 768, 1024], "UInt16", AD_COLOR_MODE_MONO),
            Ok((vec![768, 1024], "<u2".into()))
        );
        // 3-D mono: [Z, Y, X], slowest first.
        assert_eq!(
            info(&[4, 20, 10], "Float64", AD_COLOR_MODE_MONO),
            Ok((vec![4, 20, 10], "<f8".into()))
        );
        // RGB1 prepends the color dim (ophyd-async `shape = [3, *shape]`).
        assert_eq!(
            info(&[0, 768, 1024], "UInt8", AD_COLOR_MODE_RGB1),
            Ok((vec![3, 768, 1024], "|u1".into()))
        );
        // Any other color mode is unsupported.
        assert_eq!(
            info(&[0, 768, 1024], "UInt8", "Bayer"),
            Err("Unsupported ColorMode Bayer! Only Mono and RGB1 are supported.".into())
        );
        // A blank data type is refused first, whatever the color mode.
        assert_eq!(
            info(&[0, 768, 1024], "", "Bayer"),
            Err("ca://P:DataType_RBV is blank, this is not supported".into())
        );
    }

    #[test]
    fn parse_ndattributes_xml_maps_epics_pv_and_param() {
        let xml = r#"<?xml version="1.0"?>
            <Attributes>
                <!-- an EPICS_PV attribute with an explicit dbrtype -->
                <Attribute name="temp" type="EPICS_PV" source="SIM:TEMP" dbrtype="DBR_DOUBLE"/>
                <Attribute name="ring" source="SR:CURRENT" dbrtype="DBR_FLOAT"/>
                <Attribute name="id" type="PARAM" source="UNIQUE_ID" datatype="INT64"/>
                <Attribute name="color" type="PARAM" source="COLOR_MODE"/>
            </Attributes>"#;
        let attrs = parse_ndattributes_xml(xml).unwrap();
        assert_eq!(
            attrs,
            vec![
                NdAttributeDataset {
                    name: "temp".into(),
                    dtype_numpy: "<f8".into(),
                    source: "ca://SIM:TEMP".into(),
                },
                // `type` defaults to EPICS_PV
                NdAttributeDataset {
                    name: "ring".into(),
                    dtype_numpy: "<f4".into(),
                    source: "ca://SR:CURRENT".into(),
                },
                NdAttributeDataset {
                    name: "id".into(),
                    dtype_numpy: "<i8".into(),
                    source: String::new(),
                },
                // PARAM `datatype` defaults to INT
                NdAttributeDataset {
                    name: "color".into(),
                    dtype_numpy: "<i4".into(),
                    source: String::new(),
                },
            ]
        );
    }

    #[test]
    fn parse_ndattributes_xml_rejects_native_dbrtype_and_missing_name() {
        // dbrtype defaults to DBR_NATIVE, which ophyd-async rejects.
        let native = r#"<Attributes><Attribute name="t" source="SIM:T"/></Attributes>"#;
        let err = parse_ndattributes_xml(native).unwrap_err().to_string();
        assert!(err.contains("DBR_NATIVE"), "{err}");
        let unnamed = r#"<Attributes><Attribute type="PARAM" source="X"/></Attributes>"#;
        assert!(parse_ndattributes_xml(unnamed).is_err());
        let no_source = r#"<Attributes><Attribute name="t" dbrtype="DBR_INT"/></Attributes>"#;
        assert!(parse_ndattributes_xml(no_source).is_err());
        assert!(parse_ndattributes_xml("not xml at all").is_err());
    }

    #[tokio::test]
    async fn open_discovers_ndattributes_from_xml_sources() {
        let io = FakeAdFileIo::new();
        let xml = r#"<Attributes>
            <Attribute name="temp" type="EPICS_PV" source="SIM:TEMP" dbrtype="DBR_DOUBLE"/>
            <Attribute name="id" type="PARAM" source="UNIQUE_ID" datatype="INT64"/>
        </Attributes>"#;
        let w = writer_with(io.clone())
            .with_ndattribute_sources(vec![Arc::new(FakeXmlSource(xml.into()))]);
        let keys = w.open("det", 1).await.unwrap();
        // EPICS_PV attribute sources from its PV; PARAM falls back to the
        // path-provider file URI (ophyd `resource.source or self.uri`).
        assert_eq!(keys["temp"].source, "ca://SIM:TEMP");
        assert_eq!(
            keys["temp"].dtype_numpy,
            Some(crate::event_model::DtypeNumpy::Scalar("<f8".to_string()))
        );
        assert_eq!(keys["id"].source, "file://localhost/data/scans/scan.h5");
        // Discovered attrs are emitted like declared ones.
        io.set_captured(1);
        let docs: Vec<StreamAsset> = w.collect_stream_docs(1, "d").collect().await;
        let datasets: Vec<String> = docs
            .iter()
            .filter_map(|a| match a {
                StreamAsset::Resource(r) => Some(
                    r.parameters
                        .get("dataset")
                        .and_then(|v| v.as_str())
                        .unwrap()
                        .to_string(),
                ),
                _ => None,
            })
            .collect();
        assert!(datasets.contains(&"/entry/instrument/NDAttributes/temp".to_string()));
        assert!(datasets.contains(&"/entry/instrument/NDAttributes/id".to_string()));
        assert_eq!(
            docs.iter()
                .filter(|a| matches!(a, StreamAsset::Datum(_)))
                .count(),
            3,
            "main + 2 discovered attribute datums"
        );
    }

    #[tokio::test]
    async fn declared_ndattributes_override_discovered_by_name() {
        let io = FakeAdFileIo::new();
        let xml = r#"<Attributes>
            <Attribute name="temp" type="EPICS_PV" source="SIM:TEMP" dbrtype="DBR_DOUBLE"/>
        </Attributes>"#;
        let w = writer_with(io.clone())
            .with_ndattribute_sources(vec![Arc::new(FakeXmlSource(xml.into()))])
            .with_ndattributes(vec![
                NdAttributeDataset::new("temp", "<i4"),
                NdAttributeDataset::new("extra", "<f8"),
            ]);
        let keys = w.open("det", 1).await.unwrap();
        // Declared "temp" replaces the discovered one (dtype <i4, no source).
        assert_eq!(
            keys["temp"].dtype_numpy,
            Some(crate::event_model::DtypeNumpy::Scalar("<i4".to_string()))
        );
        assert!(keys.contains_key("extra"));
        // det + temp + extra, no duplicate for the collision.
        assert_eq!(keys.len(), 3);
    }

    #[tokio::test]
    async fn filename_ndattributes_source_is_skipped() {
        let io = FakeAdFileIo::new();
        // ADCore treats content without "<Attributes>" as a filename on the
        // IOC host; discovery cannot read it, so it contributes nothing.
        let w = writer_with(io.clone()).with_ndattribute_sources(vec![Arc::new(FakeXmlSource(
            "/epics/attributes.xml".into(),
        ))]);
        let keys = w.open("det", 1).await.unwrap();
        assert_eq!(keys.len(), 1, "main image only");
        assert!(keys.contains_key("det"));
    }

    #[tokio::test]
    async fn open_configures_stream_capture_and_reports_external_datakey() {
        let io = FakeAdFileIo::new();
        let w = writer_with(io.clone());
        let keys = w.open("det", 1).await.unwrap();
        let cfg = io
            .configured
            .lock()
            .unwrap()
            .clone()
            .expect("configure called");
        assert_eq!(cfg.0.directory_path(), std::path::Path::new("/data/scans"));
        assert_eq!(cfg.0.filename(), "scan");
        assert_eq!(cfg.1, AD_HDF_TEMPLATE);
        assert_eq!(cfg.2, 0, "NumCapture=0 (capture until stopped)");
        assert_eq!(
            cfg.0.create_dir_depth(),
            0,
            "CreateDirectory depth defaults to 0"
        );
        assert!(io.capturing.load(Ordering::SeqCst), "capture started");
        assert!(
            io.hdf_setup.load(Ordering::SeqCst),
            "HDF arming puts (LazyOpen/SWMR/NumExtraDims/XMLFileName) applied"
        );
        assert!(
            io.callbacks_enabled.load(Ordering::SeqCst),
            "plugin callbacks enabled"
        );
        let dk = keys.get("det").expect("det data key");
        assert_eq!(dk.external.as_deref(), Some("STREAM:"));
        assert_eq!(dk.dtype, Dtype::Array);
        // Shape = [multiplier, *frame_shape]; frame shape + dtype discovered
        // from the driver (ArraySizeZ/Y/X / DataType / ColorMode RBVs).
        assert_eq!(dk.shape, vec![Some(1), Some(20), Some(10)]);
        // Source is the file URI the path provider resolves to, not a PV.
        assert_eq!(dk.source, "file://localhost/data/scans/scan.h5");
        assert_eq!(
            dk.dtype_numpy,
            Some(crate::event_model::DtypeNumpy::Scalar("<u2".to_string()))
        );
    }

    #[tokio::test]
    async fn open_fails_when_file_path_does_not_exist_on_ioc() {
        let io = FakeAdFileIo::new();
        io.path_exists.store(false, Ordering::SeqCst);
        let w = writer_with(io.clone());
        let err = w.open("det", 1).await.unwrap_err().to_string();
        assert!(err.contains("doesn't exist"), "{err}");
        assert!(
            !io.capturing.load(Ordering::SeqCst),
            "capture must not start when the path check fails"
        );
    }

    /// Each `open()` asks the provider once, and that staging's data keys and
    /// `StreamResource` carry its file at the reader's `directory_uri`
    /// (ophyd-async `_data_logic.py:185`, `:233`).
    #[tokio::test]
    async fn open_stages_the_providers_path_info_per_staging() {
        let io = FakeAdFileIo::new();
        let provider = StaticPathProvider::new(
            Arc::new(AutoIncrementFilenameProvider::new("scan")),
            "/data/scans",
        )
        .with_directory_uri("http://files.example/scans");
        let w = AdHdfWriter::new(io.clone(), Arc::new(FakeFrameInfo), Arc::new(provider));
        for n in 0..2 {
            let keys = w.open("det", 1).await.unwrap();
            let file = format!("scan_{n:05}");
            let uri = format!("http://files.example/scans/{file}.h5");
            assert_eq!(keys["det"].source, uri);
            let cfg = io.configured.lock().unwrap().clone().unwrap();
            assert_eq!(cfg.0.filename(), file);
            io.set_captured(1);
            let docs: Vec<StreamAsset> = w.collect_stream_docs(1, "d").collect().await;
            match &docs[0] {
                StreamAsset::Resource(r) => assert_eq!(r.uri, uri),
                _ => panic!("first doc must be StreamResource"),
            }
        }
    }

    #[tokio::test]
    async fn open_fails_on_a_provider_error_before_arming_the_plugin() {
        let io = FakeAdFileIo::new();
        let provider =
            StaticPathProvider::new(Arc::new(StaticFilenameProvider::new("scan")), "relative");
        let w = AdHdfWriter::new(io.clone(), Arc::new(FakeFrameInfo), Arc::new(provider));
        let err = w.open("det", 1).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("directory_path must be an absolute path"),
            "{err}"
        );
        assert!(io.configured.lock().unwrap().is_none());
        assert!(!io.hdf_setup.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn open_leads_every_key_with_the_multiplier_and_counts_frames() {
        // ophyd-async `make_datakeys(collections_per_event)` shapes, a
        // `collections_written_signal` of frames, and datums of events
        // (`make_stream_docs`, `_data_providers.py:138-155`).
        let io = FakeAdFileIo::new();
        let w =
            writer_with(io.clone()).with_ndattributes(vec![NdAttributeDataset::new("temp", "<f8")]);
        let keys = w.open("det", 2).await.unwrap();
        assert_eq!(keys["det"].shape, [Some(2), Some(20), Some(10)]);
        assert_eq!(keys["temp"].shape, [Some(2)]);
        assert_eq!(keys["temp"].dtype, Dtype::Array);
        io.set_captured(4);
        assert_eq!(w.indices_written().await, 4);
        assert_eq!(*w.observe_indices_written().borrow(), 4);
        let docs: Vec<StreamAsset> = w.collect_stream_docs(2, "d").collect().await;
        let ranges: Vec<(u64, u64)> = docs
            .iter()
            .filter_map(|a| match a {
                StreamAsset::Datum(d) => Some((d.indices.start, d.indices.stop)),
                _ => None,
            })
            .collect();
        assert_eq!(ranges, [(0, 2), (0, 2)]);
    }

    #[test]
    fn ad_datatype_maps_all_known_codes_and_rejects_unknown() {
        // ADBaseDataType labels → numpy; the blank UNDEFINED label and any
        // other string are None.
        let want = [
            ("Int8", "|i1"),
            ("UInt8", "|u1"),
            ("Int16", "<i2"),
            ("UInt16", "<u2"),
            ("Int32", "<i4"),
            ("UInt32", "<u4"),
            ("Int64", "<i8"),
            ("UInt64", "<u8"),
            ("Float32", "<f4"),
            ("Float64", "<f8"),
        ];
        for (label, np) in want {
            assert_eq!(ad_datatype_to_numpy(label), Some(np));
        }
        assert_eq!(ad_datatype_to_numpy(""), None);
        assert_eq!(ad_datatype_to_numpy("int8"), None);
    }

    #[tokio::test]
    async fn collect_emits_resource_pointing_at_ioc_file_then_datum() {
        let io = FakeAdFileIo::new();
        io.frames_per_chunk.store(4, Ordering::SeqCst);
        let w = writer_with(io.clone());
        w.open("det", 1).await.unwrap();
        assert!(
            io.chunk_size_auto.load(Ordering::SeqCst),
            "open() sets ChunkSizeAuto"
        );
        io.set_captured(1);
        let up_to = w.indices_written().await;
        assert_eq!(up_to, 1);
        let docs: Vec<StreamAsset> = w.collect_stream_docs(up_to, "desc-uid-1").collect().await;
        assert_eq!(docs.len(), 2, "resource + datum");
        let resource_uid = match &docs[0] {
            StreamAsset::Resource(r) => {
                assert_eq!(r.uri, "file://localhost/data/scans/scan.h5");
                assert_eq!(r.mimetype, "application/x-hdf5");
                assert_eq!(r.data_key, "det");
                assert_eq!(
                    r.parameters.get("dataset").and_then(|v| v.as_str()),
                    Some("/entry/data/data")
                );
                // chunk_shape = [NumFramesChunks, *frame_shape].
                assert_eq!(
                    r.parameters.get("chunk_shape"),
                    Some(&serde_json::json!([4, 20, 10]))
                );
                r.uid.clone()
            }
            _ => panic!("first doc must be StreamResource"),
        };
        match &docs[1] {
            StreamAsset::Datum(d) => {
                assert_eq!(d.descriptor, "desc-uid-1");
                assert_eq!(d.stream_resource, resource_uid);
                assert_eq!(d.indices.start, 0);
                assert_eq!(d.indices.stop, 1);
                // Writers leave seq_nums unset; the engine fills them at
                // the Save/Collect drain.
                assert_eq!(d.seq_nums.start, 0);
                assert_eq!(d.seq_nums.stop, 0);
            }
            _ => panic!("second doc must be StreamDatum"),
        }
        // A second collect emits only the incremental datum, no new resource.
        io.set_captured(3);
        let docs2: Vec<StreamAsset> = w
            .collect_stream_docs(w.indices_written().await, "desc-uid-1")
            .collect()
            .await;
        assert_eq!(docs2.len(), 1, "only the incremental datum");
        match &docs2[0] {
            StreamAsset::Datum(d) => {
                assert_eq!(d.indices.start, 1);
                assert_eq!(d.indices.stop, 3);
                assert_eq!(d.stream_resource, resource_uid, "same resource uid reused");
            }
            _ => panic!("must be StreamDatum"),
        }
    }

    #[tokio::test]
    async fn ndattributes_emit_extra_resource_and_datum_per_attribute() {
        let io = FakeAdFileIo::new();
        let w = writer_with(io.clone()).with_ndattributes(vec![
            NdAttributeDataset::new("temp", "<f8"),
            NdAttributeDataset::new("counter", "<i4"),
        ]);
        // open() reports a scalar external data key per attribute + the image.
        let keys = w.open("det", 1).await.unwrap();
        assert!(keys.contains_key("det"));
        for name in ["temp", "counter"] {
            let dk = keys
                .get(name)
                .unwrap_or_else(|| panic!("missing data key {name}"));
            assert_eq!(dk.external.as_deref(), Some("STREAM:"));
            assert_eq!(dk.dtype, Dtype::Number);
            assert_eq!(dk.shape, vec![Some(1)], "scalar per index: [multiplier]");
        }
        io.set_captured(1);
        let up_to = w.indices_written().await;
        let docs: Vec<StreamAsset> = w.collect_stream_docs(up_to, "descN").collect().await;
        let resources: Vec<&StreamResource> = docs
            .iter()
            .filter_map(|a| match a {
                StreamAsset::Resource(r) => Some(r),
                _ => None,
            })
            .collect();
        let datums: Vec<&StreamDatum> = docs
            .iter()
            .filter_map(|a| match a {
                StreamAsset::Datum(d) => Some(d),
                _ => None,
            })
            .collect();
        assert_eq!(resources.len(), 3, "main + 2 attribute resources");
        assert_eq!(datums.len(), 3, "main + 2 attribute datums");
        let datasets: Vec<&str> = resources
            .iter()
            .map(|r| {
                r.parameters
                    .get("dataset")
                    .and_then(|v| v.as_str())
                    .unwrap()
            })
            .collect();
        assert!(datasets.contains(&"/entry/data/data"));
        assert!(datasets.contains(&"/entry/instrument/NDAttributes/temp"));
        assert!(datasets.contains(&"/entry/instrument/NDAttributes/counter"));
        // Attribute datasets chunk in fixed 16384-element blocks.
        for r in &resources {
            if r.data_key != "det" {
                assert_eq!(
                    r.parameters.get("chunk_shape"),
                    Some(&serde_json::json!([16384])),
                    "attr {} chunk_shape",
                    r.data_key
                );
            }
        }
        // All datums cover the same frame range + descriptor and each points at
        // a distinct emitted resource.
        let resource_uids: std::collections::HashSet<&str> =
            resources.iter().map(|r| r.uid.as_str()).collect();
        for d in &datums {
            assert_eq!(d.indices.start, 0);
            assert_eq!(d.indices.stop, 1);
            assert_eq!(d.descriptor, "descN");
            assert!(
                resource_uids.contains(d.stream_resource.as_str()),
                "datum must reference an emitted resource"
            );
        }
        // A second collect emits no new resources, one datum per dataset.
        io.set_captured(2);
        let docs2: Vec<StreamAsset> = w
            .collect_stream_docs(w.indices_written().await, "descN")
            .collect()
            .await;
        assert_eq!(
            docs2
                .iter()
                .filter(|a| matches!(a, StreamAsset::Resource(_)))
                .count(),
            0,
            "resources emitted once"
        );
        assert_eq!(
            docs2
                .iter()
                .filter(|a| matches!(a, StreamAsset::Datum(_)))
                .count(),
            3,
            "one datum per dataset on the increment"
        );
    }

    #[tokio::test]
    async fn only_collect_stream_docs_flushes() {
        // ophyd-async flushes in make_stream_docs (`_data_providers.py:142`)
        // and reads the count without one in get_index (`_detector.py:779`).
        let io = FakeAdFileIo::new();
        let w = writer_with(io.clone());
        w.open("det", 1).await.unwrap();
        io.set_captured(4);
        assert_eq!(w.indices_written().await, 4);
        assert_eq!(io.flushes.load(Ordering::SeqCst), 0, "indices_written");
        let _: Vec<StreamAsset> = w.collect_stream_docs(4, "d").collect().await;
        assert_eq!(io.flushes.load(Ordering::SeqCst), 1, "collect_stream_docs");
    }

    #[tokio::test]
    async fn observe_reflects_num_captured_updates() {
        let io = FakeAdFileIo::new();
        let w = writer_with(io.clone());
        let mut rx = w.observe_indices_written();
        assert_eq!(*rx.borrow_and_update(), 0);
        io.set_captured(5);
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow_and_update(), 5);
    }

    #[tokio::test]
    async fn close_stops_capture() {
        let io = FakeAdFileIo::new();
        let w = writer_with(io.clone());
        w.open("det", 1).await.unwrap();
        assert!(io.capturing.load(Ordering::SeqCst));
        w.close().await.unwrap();
        assert!(!io.capturing.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn composite_writes_stream_assets_drains_ad_writer() {
        // The WritesStreamAssets seam — the exact one the engine's step/fly
        // save path drains — yields the AD writer's resource + datum. The
        // cam needs an IOC to prepare; area_detector_hdf_live drives it.
        let io = FakeAdFileIo::new();
        let writer: Arc<dyn DetectorWriter> = Arc::new(writer_with(io.clone()));
        let det = StandardDetector::with_data_logics(
            "det",
            crate::backends::soft::SoftDetectorControl::new(Duration::ZERO),
            vec![crate::devices::DataLogic::Unbounded(writer)],
        );
        Preparable::<TriggerInfo>::prepare(&det, TriggerInfo::default())
            .await
            .await
            .unwrap();
        io.set_captured(2);
        let docs: Vec<StreamAsset> = det.collect_asset_docs(2, "descZ").unwrap().collect().await;
        assert_eq!(docs.len(), 2);
        assert!(matches!(docs[0], StreamAsset::Resource(_)));
        match &docs[1] {
            StreamAsset::Datum(d) => assert_eq!(d.descriptor, "descZ"),
            _ => panic!("second doc must be StreamDatum"),
        }
    }

    // Live-IOC smoke test — drives a real areaDetector cam + NDFileHDF5 plugin
    // end to end and asserts the emitted StreamResource points at the file the
    // IOC actually wrote. `#[ignore]`; run with `--ignored` against an IOC.
    //   BSRS_AD_PREFIX  IOC prefix   (default SIM1:), as for `tests`
    //   BSRS_AD_DIR     write dir    (default /tmp/), must be IOC-visible
    #[tokio::test]
    #[ignore]
    async fn area_detector_hdf_live_emits_real_file_stream_resource() {
        let _ioc = super::tests::lock_live_ioc();
        let prefix = super::tests::ad_prefix();
        let dir = std::env::var("BSRS_AD_DIR").unwrap_or_else(|_| "/tmp/".to_string());
        let det = area_detector_hdf(
            "addet",
            format!("{prefix}cam1:"),
            format!("{prefix}HDF1:"),
            Arc::new(StaticPathProvider::new(
                Arc::new(StaticFilenameProvider::new("bsrs_ad_live")),
                dir,
            )),
        );
        det.connect(Duration::from_secs(5)).await.expect("connect");
        // Prime array dims so the HDF plugin can open the file.
        det.control().warmup().await.expect("warmup");
        Stageable::stage(&det).await.expect("stage");
        // Prepare opens (configures + starts capture) the writer.
        Preparable::<TriggerInfo>::prepare(&det, TriggerInfo::default())
            .await
            .await
            .expect("prepare");
        // The cam reports idle before the HDF plugin has written the frame;
        // trigger waits for NumCaptured as well.
        Triggerable::trigger(&det).await.await.expect("trigger");
        let up_to = WritesStreamAssets::get_index(&det).await.expect("index");
        assert!(up_to >= 1, "expected >=1 frame written, got {up_to}");
        let docs: Vec<StreamAsset> = det
            .collect_asset_docs(up_to, "live-desc")
            .expect("collect")
            .collect()
            .await;
        let resource = docs
            .iter()
            .find_map(|a| match a {
                StreamAsset::Resource(r) => Some(r),
                _ => None,
            })
            .expect("a StreamResource was emitted");
        assert!(
            resource.uri.starts_with("file://"),
            "uri should be a file URI, got {}",
            resource.uri
        );
        assert!(
            resource.uri.ends_with(".h5"),
            "uri should point at the .h5, got {}",
            resource.uri
        );
        Stageable::unstage(&det).await.expect("unstage");
    }

    // -- AdMultipartWriter (JPEG/TIFF) ---------------------------------------

    /// `open()` arms the plugin with the multipart template (extension
    /// appended), enables callbacks, starts capture, and describes the image
    /// key with the *directory* URI as its source (ophyd-async
    /// `ADMultipartDataLogic.prepare_unbounded` + `make_datakeys`).
    #[tokio::test]
    async fn multipart_open_arms_plugin_and_describes_directory_source() {
        let io = FakeAdFileIo::new();
        let w = multipart_writer_with(io.clone());
        let keys = w.open("det", 1).await.unwrap();

        let cfg = io.configured.lock().unwrap().clone().unwrap();
        assert_eq!(cfg.0.directory_path(), std::path::Path::new("/data/scans"));
        assert_eq!(cfg.0.filename(), "scan");
        assert_eq!(cfg.1, "%s%s_%6.6d.jpg");
        assert_eq!(cfg.2, 0, "NumCapture=0: capture until close()");
        assert_eq!(cfg.0.create_dir_depth(), 0, "default create_dir_depth");
        assert!(io.callbacks_enabled.load(Ordering::SeqCst));
        assert!(io.capturing.load(Ordering::SeqCst));

        let dk = keys.get("det").expect("image data key");
        assert_eq!(dk.source, "file://localhost/data/scans/");
        assert_eq!(dk.shape, vec![Some(1), Some(20), Some(10)]);
        assert_eq!(dk.dtype, Dtype::Array);
        assert_eq!(
            dk.dtype_numpy,
            Some(crate::event_model::DtypeNumpy::Scalar("<u2".to_string()))
        );
        assert_eq!(dk.external.as_deref(), Some("STREAM:"));
    }

    /// The directory comes from the provider at each `open()`; a provider
    /// that names it by data key gets the writer's key.
    #[tokio::test]
    async fn multipart_open_stages_the_providers_directory() {
        let io = FakeAdFileIo::new();
        let provider = AutoIncrementingPathProvider::new(
            Arc::new(StaticFilenameProvider::new("img")),
            "/data/scans",
        );
        let w = AdMultipartWriter::tiff(io.clone(), Arc::new(FakeFrameInfo), Arc::new(provider));
        let keys = w.open("det", 1).await.unwrap();
        assert_eq!(keys["det"].source, "file://localhost/data/scans/det_00000/");
        let docs: Vec<StreamAsset> = w.collect_stream_docs(0, "d").collect().await;
        match &docs[0] {
            StreamAsset::Resource(r) => {
                assert_eq!(r.uri, "file://localhost/data/scans/det_00000/");
                assert_eq!(r.parameters["template"], "img_{:06d}.tiff");
            }
            _ => panic!("first doc must be StreamResource"),
        }
        w.open("det", 1).await.unwrap();
        let cfg = io.configured.lock().unwrap().clone().unwrap();
        assert_eq!(
            cfg.0.directory_path(),
            std::path::Path::new("/data/scans/det_00001")
        );
    }

    #[tokio::test]
    async fn multipart_open_leads_the_key_with_the_multiplier_and_counts_frames() {
        let io = FakeAdFileIo::new();
        let w = multipart_writer_with(io.clone());
        let keys = w.open("det", 2).await.unwrap();
        assert_eq!(keys["det"].shape, [Some(2), Some(20), Some(10)]);
        io.set_captured(4);
        assert_eq!(w.indices_written().await, 4);
        assert_eq!(*w.observe_indices_written().borrow(), 4);
    }

    /// The one `StreamResource` points at the directory with the frame-number
    /// `template` parameter and `chunk_shape` `[1, *frame_shape]` (one file
    /// per frame); datums cover the new frames with `seq_nums` left `{0,0}`
    /// for the engine. A second collect emits only the incremental datum.
    #[tokio::test]
    async fn multipart_collect_emits_directory_resource_then_datums() {
        let io = FakeAdFileIo::new();
        let w = multipart_writer_with(io.clone());
        w.open("det", 1).await.unwrap();
        io.set_captured(2);
        let docs: Vec<StreamAsset> = w
            .collect_stream_docs(w.indices_written().await, "desc-mp-1")
            .collect()
            .await;
        assert_eq!(docs.len(), 2);
        let resource_uid = match &docs[0] {
            StreamAsset::Resource(r) => {
                assert_eq!(r.data_key, "det");
                assert_eq!(r.mimetype, "multipart/related;type=image/jpeg");
                assert_eq!(r.uri, "file://localhost/data/scans/");
                assert_eq!(
                    r.parameters.get("template"),
                    Some(&serde_json::json!("scan_{:06d}.jpg"))
                );
                assert_eq!(
                    r.parameters.get("chunk_shape"),
                    Some(&serde_json::json!([1, 20, 10]))
                );
                assert!(
                    !r.parameters.contains_key("dataset"),
                    "multipart resources carry no HDF dataset path"
                );
                r.uid.clone()
            }
            _ => panic!("first doc must be StreamResource"),
        };
        match &docs[1] {
            StreamAsset::Datum(d) => {
                assert_eq!(d.stream_resource, resource_uid);
                assert_eq!(d.descriptor, "desc-mp-1");
                assert_eq!((d.indices.start, d.indices.stop), (0, 2));
                assert_eq!((d.seq_nums.start, d.seq_nums.stop), (0, 0));
            }
            _ => panic!("second doc must be StreamDatum"),
        }
        // Incremental collect: datum only, no second resource.
        io.set_captured(5);
        let docs2: Vec<StreamAsset> = w
            .collect_stream_docs(w.indices_written().await, "desc-mp-1")
            .collect()
            .await;
        assert_eq!(docs2.len(), 1);
        match &docs2[0] {
            StreamAsset::Datum(d) => {
                assert_eq!((d.indices.start, d.indices.stop), (2, 5));
            }
            _ => panic!("incremental doc must be StreamDatum"),
        }
        // No flush path: the multipart plugins have no FlushNow record.
        assert_eq!(io.flushes.load(Ordering::SeqCst), 0);
    }

    /// `close()` stops capture (ophyd-async `stop` → `stop_busy_record`);
    /// the TIFF variant carries its own extension + mimetype.
    #[tokio::test]
    async fn multipart_close_stops_capture_and_tiff_variant_maps_mimetype() {
        let io = FakeAdFileIo::new();
        let w = multipart_writer_with(io.clone());
        w.open("det", 1).await.unwrap();
        assert!(io.capturing.load(Ordering::SeqCst));
        w.close().await.unwrap();
        assert!(!io.capturing.load(Ordering::SeqCst));

        let tio = FakeAdFileIo::new();
        let t = AdMultipartWriter::tiff(tio.clone(), Arc::new(FakeFrameInfo), scans());
        t.open("det", 1).await.unwrap();
        let cfg = tio.configured.lock().unwrap().clone().unwrap();
        assert_eq!(cfg.1, "%s%s_%6.6d.tiff");
        let docs: Vec<StreamAsset> = t.collect_stream_docs(0, "d").collect().await;
        match &docs[0] {
            StreamAsset::Resource(r) => {
                assert_eq!(r.mimetype, "multipart/related;type=image/tiff");
                assert_eq!(
                    r.parameters.get("template"),
                    Some(&serde_json::json!("scan_{:06d}.tiff"))
                );
            }
            _ => panic!("first doc must be StreamResource"),
        }
    }
}
