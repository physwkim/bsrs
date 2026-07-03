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
//! - `ImageMode`/`FileWriteMode`/`DetectorState_RBV`: `mbbo`/`mbbi`
//!   (enum). We treat the wire value as `i64` and rely on the EPICS
//!   server's numeric coercion.
//! - `Acquire`/`EnableCallbacks`/`BlockingCallbacks`/`AutoIncrement`/
//!   `Capture`/`Compute*`/`EnableX`/`EnableY`: `bo` (binary). We treat
//!   these as `bool` via `EpicsCaBackend<bool>` (DBR_LONG on the wire).
//! - `FilePath`/`FileName`/`FileTemplate`: `waveform` of `CHAR` —
//!   constructed with `EpicsCaBackend::new_long_string` so the put
//!   path uses DBR_CHAR rather than the 40-byte DBR_STRING.
//! - `NDArrayPort`: `stringout` (DBR_STRING). Built with the default
//!   `EpicsCaBackend::<String>::new` (short form).

use crate::backends::epics_ca::EpicsCaBackend;
use crate::core::error::{BsrsError, Result};
use crate::core::msg::{NamedObj, StageableObj};
use crate::core::status::{Status, StatusError, SubToken};
use crate::devices::stage_sigs::StageSigs;
use crate::devices::StandardDetector;
use crate::event_model::{DataKey, Dtype, StreamDatum, StreamRange, StreamResource};
use crate::protocols_async::{
    DetectorControl, DetectorTrigger, DetectorWriter, SignalBackend, StreamAsset, TriggerInfo,
};
use futures::stream::{BoxStream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

const PUT_TIMEOUT: Duration = Duration::from_secs(10);
const WARMUP_IDLE_POLL: Duration = Duration::from_millis(100);
const WARMUP_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// `DetectorState_RBV` value for the idle state. mbbi ordering from
/// `ADBase.template`: 0=Idle, 1=Acquire, 2=Readout, 3=Correct, 4=Saving,
/// 5=Aborting, 6=Error, 7=Waiting, 8=Initializing, 9=Disconnected,
/// 10=Aborted.
pub const AD_STATE_IDLE: i64 = 0;

/// `DetectorState_RBV` value for an aborted acquisition — a "good" terminal
/// state alongside Idle (ophyd-async waits for `{IDLE, ABORTED}`).
pub const AD_STATE_ABORTED: i64 = 10;

/// `ImageMode` value for single-frame acquisition (mbbo ordering from
/// `ADBase.template`: 0=Single, 1=Multiple, 2=Continuous).
pub const AD_IMAGE_MODE_SINGLE: i64 = 0;

/// `ImageMode` value for a bounded burst of `NumImages` frames.
pub const AD_IMAGE_MODE_MULTIPLE: i64 = 1;

/// `FileWriteMode` value for streaming capture (mbbo ordering from
/// `NDFile.template`: 0=Single, 1=Capture, 2=Stream).
pub const AD_FILE_WRITE_MODE_STREAM: i64 = 2;

/// `ColorMode_RBV` values (`NDColorMode_t` ordering from `NDArray.h`:
/// 0=Mono, 1=Bayer, 2=RGB1, 3=RGB2, 4=RGB3, 5=YUV444, 6=YUV422, 7=YUV420).
pub const AD_COLOR_MODE_MONO: i64 = 0;
/// See [`AD_COLOR_MODE_MONO`].
pub const AD_COLOR_MODE_RGB1: i64 = 2;

/// Map an areaDetector `DataType_RBV` (`NDDataType_t` enum ordering) to the
/// corresponding numpy dtype string (little-endian; single-byte types carry no
/// byte order). Returns `None` for an unknown code. Matches
/// `np.dtype(datatype).str` in ophyd-async's `get_ndarray_resource_info`.
pub const fn ad_datatype_to_numpy(dt: i64) -> Option<&'static str> {
    Some(match dt {
        0 => "|i1", // NDInt8
        1 => "|u1", // NDUInt8
        2 => "<i2", // NDInt16
        3 => "<u2", // NDUInt16
        4 => "<i4", // NDInt32
        5 => "<u4", // NDUInt32
        6 => "<i8", // NDInt64
        7 => "<u8", // NDUInt64
        8 => "<f4", // NDFloat32
        9 => "<f8", // NDFloat64
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

/// Poll a bool readback until it equals `want`, bounded by `timeout`. The
/// wait half of ophyd-async's `set_and_wait_for_value(...,
/// wait_for_set_completion=False)` / `stop_busy_record` for busy records,
/// whose put-callback tracks the *operation*, not the value change.
async fn wait_for_bool(
    ch: &EpicsCaBackend<bool>,
    want: bool,
    timeout: Duration,
    what: &str,
) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if SignalBackend::<bool>::get_value(ch).await? == want {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(BsrsError::Backend(format!(
                "{what}: readback did not reach {want} within {timeout:?}"
            )));
        }
        tokio::time::sleep(WARMUP_IDLE_POLL).await;
    }
}

/// Cam-side handle: `ImageMode`/`NumImages`/`Acquire`/`AcquireTime`
/// setters plus `ArrayCounter_RBV` / `DetectorState_RBV` readbacks.
pub struct AreaDetectorCam {
    /// PV prefix, e.g. `"13SIM1:cam1:"`.
    pub prefix: String,
    /// `ImageMode` (mbbo: 0=Single, 1=Multiple, 2=Continuous).
    pub image_mode: Arc<EpicsCaBackend<i64>>,
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
    /// `ArrayCounter_RBV` (longin) — total frames produced.
    pub array_counter_rbv: Arc<EpicsCaBackend<i64>>,
    /// `DetectorState_RBV` (mbbi).
    pub detector_state_rbv: Arc<EpicsCaBackend<i64>>,
    /// `NDAttributesFile` (waveform) — per-frame attribute config, either
    /// inline XML or a filename on the IOC host. Read at `open()` to discover
    /// NDAttribute datasets for the emitted stream documents.
    pub nd_attributes_file: Arc<EpicsCaBackend<String>>,
    /// Frame description readbacks (`ArraySizeX/Y/Z_RBV`, `DataType_RBV`,
    /// `ColorMode_RBV`) — the shape/dtype source for the emitted `DataKey`,
    /// read from the driver as in ophyd-async's `make_writer_data_logic`.
    pub frame_info: Arc<CamFrameInfo>,
}

/// The driver-side NDArray description: `ArraySizeX/Y/Z_RBV`, `DataType_RBV`,
/// and `ColorMode_RBV`. A cheap `Arc` handle so the detector writer can
/// discover the frame shape/dtype after the cam has been moved into a
/// `StandardDetector`. Port of ophyd-async's `NDArrayDescription` and
/// `get_ndarray_resource_info` (`_data_logic.py:54-91`).
pub struct CamFrameInfo {
    /// PV prefix (for error messages).
    pub prefix: String,
    /// `ArraySizeX_RBV` (longin).
    pub size_x: Arc<EpicsCaBackend<i64>>,
    /// `ArraySizeY_RBV` (longin).
    pub size_y: Arc<EpicsCaBackend<i64>>,
    /// `ArraySizeZ_RBV` (longin).
    pub size_z: Arc<EpicsCaBackend<i64>>,
    /// `DataType_RBV` (mbbi, `NDDataType_t` ordering).
    pub data_type: Arc<EpicsCaBackend<i64>>,
    /// `ColorMode_RBV` (mbbi, `NDColorMode_t` ordering).
    pub color_mode: Arc<EpicsCaBackend<i64>>,
}

impl CamFrameInfo {
    fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            size_x: Arc::new(EpicsCaBackend::<i64>::new(join(prefix, "ArraySizeX_RBV"))),
            size_y: Arc::new(EpicsCaBackend::<i64>::new(join(prefix, "ArraySizeY_RBV"))),
            size_z: Arc::new(EpicsCaBackend::<i64>::new(join(prefix, "ArraySizeZ_RBV"))),
            data_type: Arc::new(EpicsCaBackend::<i64>::new(join(prefix, "DataType_RBV"))),
            color_mode: Arc::new(EpicsCaBackend::<i64>::new(join(prefix, "ColorMode_RBV"))),
        }
    }

    async fn connect(&self, timeout: Duration) -> Result<()> {
        let (a, b, c, d, e) = tokio::join!(
            SignalBackend::<i64>::connect(self.size_x.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.size_y.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.size_z.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.data_type.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.color_mode.as_ref(), timeout),
        );
        a?;
        b?;
        c?;
        d?;
        e?;
        Ok(())
    }
}

/// Source of the frame shape + numpy dtype for the emitted `DataKey`. The
/// detector writer reads it at `open()`; the production impl is the driver's
/// readbacks ([`CamFrameInfo`]).
#[async_trait::async_trait]
pub trait AdFrameInfoSource: Send + Sync {
    /// Frame shape (slowest-varying first, zero dims dropped, color-mode
    /// adjusted) and numpy dtype string.
    async fn frame_info(&self) -> Result<(Vec<u64>, String)>;
}

/// Compose the frame shape ophyd-async's `get_ndarray_resource_info`
/// (`_data_logic.py:61-84`) derives from the driver readbacks: dims in
/// `[Z, Y, X]` order (slowest first, matching the row-major HDF dataset),
/// zero dims dropped, RGB1 prepending the color dim; any other non-Mono
/// color mode is unsupported.
fn compose_frame_shape(sz: i64, sy: i64, sx: i64, color_mode: i64, pv: &str) -> Result<Vec<u64>> {
    let mut shape: Vec<u64> = [sz, sy, sx]
        .into_iter()
        .filter(|&d| d > 0)
        .map(|d| d as u64)
        .collect();
    match color_mode {
        AD_COLOR_MODE_MONO => {}
        AD_COLOR_MODE_RGB1 => shape.insert(0, 3),
        other => {
            return Err(BsrsError::Backend(format!(
                "{pv}ColorMode_RBV={other} is not supported (only Mono and RGB1)"
            )));
        }
    }
    Ok(shape)
}

#[async_trait::async_trait]
impl AdFrameInfoSource for CamFrameInfo {
    async fn frame_info(&self) -> Result<(Vec<u64>, String)> {
        let (sz, sy, sx, dt, cm) = tokio::join!(
            SignalBackend::<i64>::get_value(self.size_z.as_ref()),
            SignalBackend::<i64>::get_value(self.size_y.as_ref()),
            SignalBackend::<i64>::get_value(self.size_x.as_ref()),
            SignalBackend::<i64>::get_value(self.data_type.as_ref()),
            SignalBackend::<i64>::get_value(self.color_mode.as_ref()),
        );
        let shape = compose_frame_shape(sz?, sy?, sx?, cm?, &self.prefix)?;
        let dt = dt?;
        let dtype_numpy = ad_datatype_to_numpy(dt).ok_or_else(|| {
            BsrsError::Backend(format!(
                "{}DataType_RBV={dt} is not a known NDDataType_t",
                self.prefix
            ))
        })?;
        Ok((shape, dtype_numpy.to_string()))
    }
}

impl AreaDetectorCam {
    /// Build the handle. Does NOT connect; call `connect` afterwards.
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self {
            image_mode: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "ImageMode"))),
            num_images: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "NumImages"))),
            acquire: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "Acquire"))),
            acquire_rbv: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "Acquire_RBV"))),
            acquire_time: Arc::new(EpicsCaBackend::<f64>::new(join(&prefix, "AcquireTime"))),
            array_counter_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "ArrayCounter_RBV",
            ))),
            detector_state_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "DetectorState_RBV",
            ))),
            nd_attributes_file: Arc::new(EpicsCaBackend::<String>::new_long_string(join(
                &prefix,
                "NDAttributesFile",
            ))),
            frame_info: Arc::new(CamFrameInfo::new(&prefix)),
            prefix,
        }
    }

    /// Connect every channel in parallel.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        let (a, b, c, d, e, f, g, h, i) = tokio::join!(
            SignalBackend::<i64>::connect(self.image_mode.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.num_images.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.acquire.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.acquire_rbv.as_ref(), timeout),
            SignalBackend::<f64>::connect(self.acquire_time.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.array_counter_rbv.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.detector_state_rbv.as_ref(), timeout),
            SignalBackend::<String>::connect(self.nd_attributes_file.as_ref(), timeout),
            self.frame_info.connect(timeout),
        );
        a?;
        b?;
        c?;
        d?;
        e?;
        f?;
        g?;
        h?;
        i?;
        Ok(())
    }

    /// Poll `DetectorState_RBV` until it reports a good terminal state —
    /// Idle or Aborted (ophyd-async `wait_for_good_state({IDLE, ABORTED})`)
    /// — or `timeout` elapses.
    pub async fn wait_for_idle(&self, timeout: Duration) -> Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let state = SignalBackend::<i64>::get_value(self.detector_state_rbv.as_ref()).await?;
            if state == AD_STATE_IDLE || state == AD_STATE_ABORTED {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(BsrsError::Backend(format!(
                    "{}DetectorState_RBV did not reach Idle/Aborted within {:?} (last={state})",
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
        let prev_image_mode = SignalBackend::<i64>::get_value(self.image_mode.as_ref()).await?;
        let prev_num_images = SignalBackend::<i64>::get_value(self.num_images.as_ref()).await?;

        await_put(
            SignalBackend::<i64>::put(self.image_mode.as_ref(), Some(AD_IMAGE_MODE_SINGLE)),
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
            SignalBackend::<i64>::put(self.image_mode.as_ref(), Some(prev_image_mode)),
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
        let (a, b, c, d) = tokio::join!(
            SignalBackend::<bool>::connect(self.enable_callbacks.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.blocking_callbacks.as_ref(), timeout),
            SignalBackend::<String>::connect(self.nd_array_port.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.queue_size.as_ref(), timeout),
        );
        a?;
        b?;
        c?;
        d?;
        Ok(())
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
    /// `FileWriteMode` (mbbo: 0=Single, 1=Capture, 2=Stream).
    pub file_write_mode: Arc<EpicsCaBackend<i64>>,
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
            file_path: Arc::new(EpicsCaBackend::<String>::new_long_string(join(
                &prefix, "FilePath",
            ))),
            file_name: Arc::new(EpicsCaBackend::<String>::new_long_string(join(
                &prefix, "FileName",
            ))),
            file_template: Arc::new(EpicsCaBackend::<String>::new_long_string(join(
                &prefix,
                "FileTemplate",
            ))),
            auto_increment: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "AutoIncrement"))),
            file_write_mode: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "FileWriteMode"))),
            capture: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "Capture"))),
            capture_rbv: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "Capture_RBV"))),
            num_capture: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "NumCapture"))),
            num_captured_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "NumCaptured_RBV",
            ))),
            full_file_name_rbv: Arc::new(EpicsCaBackend::<String>::new_long_string(join(
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
        let p = self.plugin.connect(timeout);
        let (a, b, c, d, e, f, g, h, i, j) = tokio::join!(
            p,
            SignalBackend::<String>::connect(self.file_path.as_ref(), timeout),
            SignalBackend::<String>::connect(self.file_name.as_ref(), timeout),
            SignalBackend::<String>::connect(self.file_template.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.auto_increment.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.file_write_mode.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.capture.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.num_capture.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.num_captured_rbv.as_ref(), timeout),
            SignalBackend::<String>::connect(self.full_file_name_rbv.as_ref(), timeout),
        );
        a?;
        b?;
        c?;
        d?;
        e?;
        f?;
        g?;
        h?;
        i?;
        j?;
        let (k, l, m, n) = tokio::join!(
            SignalBackend::<i64>::connect(self.file_number.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.file_path_exists_rbv.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.create_directory.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.capture_rbv.as_ref(), timeout),
        );
        k?;
        l?;
        m?;
        n?;
        Ok(())
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

    /// Set `FileWriteMode` (0=Single, 1=Capture, 2=Stream).
    pub async fn set_write_mode(&self, mode: i64) -> Result<()> {
        await_put(
            SignalBackend::<i64>::put(self.file_write_mode.as_ref(), Some(mode)),
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
        self.capture.put_nowait(true).await?;
        wait_for_bool(
            self.capture_rbv.as_ref(),
            true,
            PUT_TIMEOUT,
            "NdFile::start_capture: Capture_RBV",
        )
        .await
    }

    /// Stop capture (`Capture=0`) — flushes and closes the file. Same busy
    /// record: fire-and-forget the put, wait for `Capture_RBV=0`
    /// (ophyd-async `stop_busy_record`).
    pub async fn stop_capture(&self) -> Result<()> {
        self.capture.put_nowait(false).await?;
        wait_for_bool(
            self.capture_rbv.as_ref(),
            false,
            PUT_TIMEOUT,
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

    /// Point the plugin at `directory`/`filename` with `template`, switch it
    /// to Stream mode with `AutoIncrement` on and `FileNumber` reset, and set
    /// `NumCapture`. Single owner of the put ordering (depth before path: the
    /// directory-creation callback fires when `FilePath` is processed) and of
    /// the `FilePathExists_RBV` check. Port of ophyd-async's
    /// `prepare_file_paths` (`_data_logic.py:124`).
    pub async fn configure(
        &self,
        directory: &str,
        filename: &str,
        template: &str,
        num_capture: i64,
        create_dir_depth: i64,
    ) -> Result<()> {
        self.set_create_directory(create_dir_depth).await?;
        self.set_path(directory).await?;
        self.set_name(filename).await?;
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
    /// `FlushNow` (bo) — force the plugin to flush buffered frames to the
    /// file. An `NDFileHDF5`-only record (SWMR streaming; the write index
    /// only reflects flushed frames, so we flush before reading
    /// `NumCaptured_RBV`) — the JPEG/TIFF plugins do not have it.
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
            xml_file_name: Arc::new(EpicsCaBackend::<String>::new_long_string(join(
                &prefix,
                "XMLFileName",
            ))),
            flush_now: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "FlushNow"))),
            file: NdFile::new(prefix),
        }
    }

    /// Connect the embedded file plugin + the HDF5-specific channels.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        let (a, b, c, d, e, f, g, h, i) = tokio::join!(
            self.file.connect(timeout),
            SignalBackend::<i64>::connect(self.num_frames_chunks.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.num_frames_chunks_rbv.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.chunk_size_auto.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.lazy_open.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.swmr_mode.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.num_extra_dims.as_ref(), timeout),
            SignalBackend::<String>::connect(self.xml_file_name.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.flush_now.as_ref(), timeout),
        );
        a?;
        b?;
        c?;
        d?;
        e?;
        f?;
        g?;
        h?;
        i?;
        Ok(())
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

    /// Force a flush (`FlushNow=1`) so `NumCaptured_RBV` reflects frames
    /// actually written to the file.
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
            plugin,
        }
    }

    /// Connect plugin base + every stats-compute channel.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        let p = self.plugin.connect(timeout);
        let (a, b, c, d, e) = tokio::join!(
            p,
            SignalBackend::<bool>::connect(self.compute_statistics.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.compute_centroid.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.compute_profiles.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.compute_histogram.as_ref(), timeout),
        );
        a?;
        b?;
        c?;
        d?;
        e?;
        Ok(())
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
            enable_x: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "EnableX"))),
            enable_y: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "EnableY"))),
            plugin,
        }
    }

    /// Connect plugin base + every ROI channel.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        let p = self.plugin.connect(timeout);
        let (a, b, c, d, e, f, g) = tokio::join!(
            p,
            SignalBackend::<i64>::connect(self.min_x.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.min_y.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.size_x.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.size_y.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.enable_x.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.enable_y.as_ref(), timeout),
        );
        a?;
        b?;
        c?;
        d?;
        e?;
        f?;
        g?;
        Ok(())
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
/// IOC-resolved `FullFileName_RBV` is deterministic.
const AD_HDF_TEMPLATE: &str = "%s%s.h5";

/// `FileTemplate` base for the multipart (one-file-per-frame) writers —
/// `<path><name>_<6-digit frame number>`; the extension is appended per
/// plugin. Matches ophyd-async's `"%s%s_%6.6d" + self.extension`
/// (`ADMultipartDataLogic.prepare_unbounded`).
const AD_MULTIPART_TEMPLATE: &str = "%s%s_%6.6d";

/// Where the areaDetector file plugin should write and under what basename.
/// Mirrors ophyd-async's `PathProvider`/`PathInfo`: the writer sets the IOC's
/// `FilePath`/`FileName` from this, then builds the `StreamResource` URI from
/// the IOC's `FullFileName_RBV` readback (so it points at what the IOC wrote).
#[derive(Clone, Debug)]
pub struct StaticPathProvider {
    /// Directory the IOC writes into (must be visible on the IOC host).
    /// Always stored with a trailing `/`, fixed at construction — left to the
    /// IOC, areaDetector appends the separator itself and the `FilePath`
    /// readback never matches the put (ophyd-async `prepare_file_paths`,
    /// `_data_logic.py:130`). POSIX IOC hosts only; ophyd's
    /// `PureWindowsPath` + `\` variant is not ported.
    pub directory: String,
    /// File basename (pre-template), e.g. `"scan"`.
    pub filename: String,
    /// `CreateDirectory` depth — how many missing trailing path levels the
    /// IOC may create (`0` = create nothing). Set via
    /// [`with_create_dir_depth`](Self::with_create_dir_depth).
    pub create_dir_depth: i64,
}

impl StaticPathProvider {
    /// Build a provider that always returns the same directory + basename.
    pub fn new(directory: impl Into<String>, filename: impl Into<String>) -> Self {
        let mut directory = directory.into();
        if !directory.ends_with('/') {
            directory.push('/');
        }
        Self {
            directory,
            filename: filename.into(),
            create_dir_depth: 0,
        }
    }

    /// Let the IOC create up to `depth` missing trailing path levels
    /// (`CreateDirectory` PV; ophyd-async `PathInfo.create_dir_depth`).
    pub fn with_create_dir_depth(mut self, depth: i64) -> Self {
        self.create_dir_depth = depth;
        self
    }
}

/// The subset of an areaDetector file plugin the [`AdHdfWriter`] drives.
/// Splitting this out (mirroring ophyd-async's `NDFileHDF5IO` vs `ADHDFWriter`)
/// keeps the document-composition logic unit-testable against an in-memory fake
/// with no live IOC.
#[async_trait::async_trait]
pub trait AdFileIo: Send + Sync {
    /// Connect the underlying transport and install the frames-written monitor.
    async fn connect(&self, timeout: Duration) -> Result<()>;
    /// Point the plugin at `directory`/`filename` with `template`, switch it
    /// to Stream mode with `AutoIncrement` on and `FileNumber` reset, and set
    /// `NumCapture` (`0` = until `stop_capture`). `create_dir_depth` missing
    /// trailing path levels may be created by the IOC; errors if the IOC then
    /// still cannot see the directory (`FilePathExists_RBV`). Port of
    /// ophyd-async's `prepare_file_paths` (`_data_logic.py:124`).
    async fn configure(
        &self,
        directory: &str,
        filename: &str,
        template: &str,
        num_capture: i64,
        create_dir_depth: i64,
    ) -> Result<()>;
    /// Enable the plugin's callbacks (`EnableCallbacks=1`) so it consumes
    /// frames from its NDArray port.
    async fn enable_callbacks(&self) -> Result<()>;
    /// Start capture — the plugin opens the file and accepts frames.
    async fn start_capture(&self) -> Result<()>;
    /// Stop capture — flush and close the file.
    async fn stop_capture(&self) -> Result<()>;
    /// Frames written so far (the per-frame write index).
    async fn num_captured(&self) -> Result<u64>;
    /// Absolute path the IOC resolved for the open file.
    async fn full_file_name(&self) -> Result<String>;
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
    /// Force a flush (`FlushNow=1`) so `num_captured` reflects frames written
    /// to the file. HDF5-only: `FlushNow` exists on `NDFileHDF5` (SWMR), not
    /// on the JPEG/TIFF file plugins.
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
        directory: &str,
        filename: &str,
        template: &str,
        num_capture: i64,
        create_dir_depth: i64,
    ) -> Result<()> {
        self.file
            .file
            .configure(directory, filename, template, num_capture, create_dir_depth)
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
    async fn full_file_name(&self) -> Result<String> {
        self.file.file.full_file_name().await
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
        directory: &str,
        filename: &str,
        template: &str,
        num_capture: i64,
        create_dir_depth: i64,
    ) -> Result<()> {
        self.file
            .configure(directory, filename, template, num_capture, create_dir_depth)
            .await
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
    async fn full_file_name(&self) -> Result<String> {
        self.file.full_file_name().await
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

/// Emit state for [`AdHdfWriter`]: the attribute set staged by the last
/// `open()` (discovered from the XML sources, merged with the declared specs),
/// the once-emitted `StreamResource` UIDs (main image + one per NDAttribute),
/// and the frame cursor (`StreamDatum`s cover `[last_emitted, up_to)`).
/// `open()` is the only writer of `attrs`; `collect_stream_docs` only reads it,
/// so the emitted datasets always match the data keys reported at `open()`.
#[derive(Default)]
struct AdEmitState {
    attrs: Vec<NdAttributeDataset>,
    main_resource_uid: Option<String>,
    attr_resource_uids: HashMap<String, String>,
    last_emitted: u64,
    /// Frames per index (ophyd-async `collections_per_event`), staged by
    /// `open(multiplier)`. `0` = not yet opened; readers clamp to 1.
    multiplier: u64,
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
    name: String,
    io: Arc<dyn AdHdfFileIo>,
    /// Driver-side frame description (`ArraySizeZ/Y/X_RBV` etc.), read at
    /// `open()` for the image `DataKey`'s shape/dtype.
    frame: Arc<dyn AdFrameInfoSource>,
    path_provider: StaticPathProvider,
    /// `NDAttributesFile` sources (driver + extra plugins) read at `open()` to
    /// discover the per-frame NDAttribute datasets in the IOC's file.
    attr_sources: Vec<Arc<dyn NdAttributeXmlSource>>,
    /// Explicitly declared NDAttribute datasets, merged over the discovered set
    /// at `open()` (declared wins on a name collision). Covers the case where
    /// `NDAttributesFile` holds a filename on the IOC host that discovery
    /// cannot read.
    ndattributes: Vec<NdAttributeDataset>,
    /// Guards single-`StreamResource` emission and the datum cursor.
    emit: tokio::sync::Mutex<AdEmitState>,
}

impl AdHdfWriter {
    /// Build a writer over `io`, describing frames per `frame`, writing files
    /// per `path_provider`.
    pub fn new(
        name: impl Into<String>,
        io: Arc<dyn AdHdfFileIo>,
        frame: Arc<dyn AdFrameInfoSource>,
        path_provider: StaticPathProvider,
    ) -> Self {
        Self {
            name: name.into(),
            io,
            frame,
            path_provider,
            attr_sources: Vec::new(),
            ndattributes: Vec::new(),
            emit: tokio::sync::Mutex::new(AdEmitState::default()),
        }
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

    /// Connect the underlying file-plugin IO.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        self.io.connect(timeout).await
    }

    fn data_key_name(&self) -> String {
        format!("{}_image", self.name)
    }

    /// The file URI the path provider + `%s%s.h5` template resolve to — the
    /// `DataKey.source` fallback for attributes with no PV source, mirroring
    /// ophyd-async composing the uri from `path_info` (`_data_logic.py:224`).
    fn provider_uri(&self) -> String {
        crate::event_model::file_uri(&format!(
            "{}{}.h5",
            self.path_provider.directory, self.path_provider.filename
        ))
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
    async fn open(&self, multiplier: u32) -> Result<HashMap<String, DataKey>> {
        // The frames-per-index multiplier shapes the DataKeys and divides the
        // frame count into indices. >1 is rejected until the fly-mode index
        // observation (`observe_indices_written`, raw frame counts) also
        // scales by it — shipping one path divided and the other raw would
        // complete a fly scan at the wrong frame count.
        if multiplier > 1 {
            return Err(BsrsError::Plan(format!(
                "AdHdfWriter::open(multiplier={multiplier}): multiplier > 1 is \
                 not supported yet"
            )));
        }
        let multiplier = u64::from(multiplier.max(1));
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
        self.io
            .configure(
                &self.path_provider.directory,
                &self.path_provider.filename,
                AD_HDF_TEMPLATE,
                0,
                self.path_provider.create_dir_depth,
            )
            .await?;
        self.io.start_capture().await?;
        // Discover the frame shape + dtype from the driver's ArraySizeZ/Y/X +
        // DataType + ColorMode RBVs. Requires a primed detector — the shape is
        // `[]` until the first frame flows (e.g. after
        // `AreaDetectorCam::warmup`).
        let (shape, dtype_numpy) = self.frame.frame_info().await?;
        // Discover the NDAttribute datasets from the NDAttributesFile sources,
        // merged with the declared specs.
        let attrs = self.discover_ndattributes().await?;
        // Reset emit state for this staging; the staged attribute set is what
        // collect_stream_docs will emit resources/datums for.
        {
            let mut st = self.emit.lock().await;
            st.attrs = attrs.clone();
            st.main_resource_uid = None;
            st.attr_resource_uids.clear();
            st.last_emitted = 0;
            st.multiplier = multiplier;
            st.frame_shape = shape.clone();
            st.frames_per_chunk = frames_per_chunk;
        }
        // DataKey shapes carry the frames-per-index prefix, `[multiplier,
        // *frame_shape]` for the image and `[multiplier]` for the scalar
        // NDAttributes (ophyd-async `make_datakeys`); the image source is the
        // file URI the writer resolves to, not a PV.
        let image_shape: Vec<Option<u64>> =
            std::iter::once(multiplier).chain(shape).map(Some).collect();
        let mut out = HashMap::new();
        out.insert(
            self.data_key_name(),
            DataKey {
                source: self.provider_uri(),
                dtype: stream_datakey_dtype(multiplier, image_shape.len()),
                shape: image_shape,
                dtype_numpy: Some(dtype_numpy.into()),
                external: Some("STREAM:".into()),
                units: None,
                precision: None,
                object_name: Some(self.name.clone()),
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
                        self.provider_uri()
                    } else {
                        a.source.clone()
                    },
                    dtype: stream_datakey_dtype(multiplier, 1),
                    shape: vec![Some(multiplier)],
                    dtype_numpy: Some(a.dtype_numpy.clone().into()),
                    external: Some("STREAM:".into()),
                    units: None,
                    precision: None,
                    object_name: Some(self.name.clone()),
                    dims: None,
                    limits: None,
                    choices: None,
                },
            );
        }
        Ok(out)
    }
    fn observe_indices_written(&self) -> watch::Receiver<u64> {
        // Raw frame counts — equal to indices while `open` rejects
        // multiplier > 1.
        self.io.observe_num_captured()
    }
    async fn indices_written(&self) -> u64 {
        // Flush first so the count reflects frames actually on disk (SWMR),
        // mirroring ophyd-async setting flush_signal before reading the count.
        // Best-effort: a plugin without SWMR flush support just yields the
        // unflushed count.
        let _ = self.io.flush().await;
        let frames = self.io.num_captured().await.unwrap_or(0);
        // Frames -> indices (ophyd-async `collections_written //
        // collections_per_event`).
        frames / self.emit.lock().await.multiplier.max(1)
    }
    fn collect_stream_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset> {
        let descriptor = descriptor.to_string();
        let data_key = self.data_key_name();
        // Build the docs in one future (the FullFileName_RBV read is async), then
        // flatten to a stream. `stream::once(..).flat_map(stream::iter)` gives a
        // `BoxStream` without pulling in a generator macro.
        let fut = async move {
            let mut docs: Vec<StreamAsset> = Vec::new();
            let mut st = self.emit.lock().await;
            // The attribute set staged by the last open().
            let attrs = st.attrs.clone();
            // Emit each StreamResource once. The main image and every
            // NDAttribute live in the same .h5, so they share one URI resolved
            // from FullFileName_RBV — read it only when a resource still needs
            // emitting.
            let need_main = st.main_resource_uid.is_none();
            let need_attr = attrs
                .iter()
                .any(|a| !st.attr_resource_uids.contains_key(&a.name));
            if need_main || need_attr {
                let path = self.io.full_file_name().await.unwrap_or_default();
                let uri = if path.is_empty() {
                    String::new()
                } else {
                    crate::event_model::file_uri(&path)
                };
                if need_main {
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
            }
            // One StreamDatum per dataset (main + each attribute) for the new
            // frames, all covering the same index range in the same event.
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
}

/// Emit state for [`AdMultipartWriter`]: the once-emitted `StreamResource`
/// UID, the frame cursor, and the shape/multiplier staged by `open()`.
#[derive(Default)]
struct AdMultipartEmitState {
    resource_uid: Option<String>,
    last_emitted: u64,
    /// Frames per index; `0` = not yet opened, readers clamp to 1.
    multiplier: u64,
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
    name: String,
    io: Arc<dyn AdFileIo>,
    /// Driver-side frame description (`ArraySizeZ/Y/X_RBV` etc.), read at
    /// `open()` for the image `DataKey`'s shape/dtype.
    frame: Arc<dyn AdFrameInfoSource>,
    path_provider: StaticPathProvider,
    /// File extension including the dot, e.g. `".jpg"`.
    extension: String,
    /// `StreamResource` mimetype, e.g. `"multipart/related;type=image/jpeg"`.
    mimetype: String,
    /// Guards single-`StreamResource` emission and the datum cursor.
    emit: tokio::sync::Mutex<AdMultipartEmitState>,
}

impl AdMultipartWriter {
    /// Build a writer over `io`, describing frames per `frame`, writing files
    /// per `path_provider`, with the given file `extension` (dot included)
    /// and `StreamResource` `mimetype`.
    pub fn new(
        name: impl Into<String>,
        io: Arc<dyn AdFileIo>,
        frame: Arc<dyn AdFrameInfoSource>,
        path_provider: StaticPathProvider,
        extension: impl Into<String>,
        mimetype: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            io,
            frame,
            path_provider,
            extension: extension.into(),
            mimetype: mimetype.into(),
            emit: tokio::sync::Mutex::new(AdMultipartEmitState::default()),
        }
    }

    /// JPEG variant (`.jpg`, `multipart/related;type=image/jpeg`) — the
    /// `ADWriterType.JPEG` arm of ophyd-async's `make_writer_data_logic`.
    pub fn jpeg(
        name: impl Into<String>,
        io: Arc<dyn AdFileIo>,
        frame: Arc<dyn AdFrameInfoSource>,
        path_provider: StaticPathProvider,
    ) -> Self {
        Self::new(
            name,
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
        name: impl Into<String>,
        io: Arc<dyn AdFileIo>,
        frame: Arc<dyn AdFrameInfoSource>,
        path_provider: StaticPathProvider,
    ) -> Self {
        Self::new(
            name,
            io,
            frame,
            path_provider,
            ".tiff",
            "multipart/related;type=image/tiff",
        )
    }

    /// Connect the underlying file-plugin IO.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        self.io.connect(timeout).await
    }

    fn data_key_name(&self) -> String {
        format!("{}_image", self.name)
    }

    /// The directory URI the frames land under — both the `StreamResource`
    /// URI and the `DataKey.source` (ophyd-async's multipart provider uses
    /// `path_info.directory_uri` for both; the per-frame filename lives in
    /// the `template` parameter).
    fn directory_uri(&self) -> String {
        crate::event_model::file_uri(&self.path_provider.directory)
    }

    /// The `template` `StreamResource` parameter — Python-format style, the
    /// consumer substitutes the frame number:
    /// `path_info.filename + "_{:06d}" + extension`.
    fn template_parameter(&self) -> String {
        format!("{}_{{:06d}}{}", self.path_provider.filename, self.extension)
    }
}

#[async_trait::async_trait]
impl DetectorWriter for AdMultipartWriter {
    async fn open(&self, multiplier: u32) -> Result<HashMap<String, DataKey>> {
        // Same guard as AdHdfWriter::open: the index observation
        // (`observe_indices_written`) reports raw frame counts, so shipping
        // a divided DataKey path with a raw observe path would complete a
        // fly scan at the wrong frame count.
        if multiplier > 1 {
            return Err(BsrsError::Plan(format!(
                "AdMultipartWriter::open(multiplier={multiplier}): multiplier \
                 > 1 is not supported yet"
            )));
        }
        let multiplier = u64::from(multiplier.max(1));
        // Plugin callbacks on, then the file paths. ophyd-async's
        // `ADMultipartDataLogic.prepare_unbounded` does not enable callbacks
        // itself (its plugin chain is armed elsewhere); bsrs's open() is the
        // only arming path, so it does — deliberate deviation.
        self.io.enable_callbacks().await?;
        // Configure + arm the IOC file plugin: one file per frame with a
        // 6-digit frame number, capture until close() (`NumCapture=0`).
        self.io
            .configure(
                &self.path_provider.directory,
                &self.path_provider.filename,
                &format!("{AD_MULTIPART_TEMPLATE}{}", self.extension),
                0,
                self.path_provider.create_dir_depth,
            )
            .await?;
        self.io.start_capture().await?;
        // Frame shape + dtype from the driver's readbacks (primed detector
        // required, as for the HDF writer).
        let (shape, dtype_numpy) = self.frame.frame_info().await?;
        {
            let mut st = self.emit.lock().await;
            st.resource_uid = None;
            st.last_emitted = 0;
            st.multiplier = multiplier;
            st.frame_shape = shape.clone();
        }
        let image_shape: Vec<Option<u64>> =
            std::iter::once(multiplier).chain(shape).map(Some).collect();
        let mut out = HashMap::new();
        out.insert(
            self.data_key_name(),
            DataKey {
                source: self.directory_uri(),
                dtype: stream_datakey_dtype(multiplier, image_shape.len()),
                shape: image_shape,
                dtype_numpy: Some(dtype_numpy.into()),
                external: Some("STREAM:".into()),
                units: None,
                precision: None,
                object_name: Some(self.name.clone()),
                dims: None,
                limits: None,
                choices: None,
            },
        );
        Ok(out)
    }
    fn observe_indices_written(&self) -> watch::Receiver<u64> {
        // Raw frame counts — equal to indices while `open` rejects
        // multiplier > 1.
        self.io.observe_num_captured()
    }
    async fn indices_written(&self) -> u64 {
        // No flush: the multipart plugins write one whole file per frame, so
        // `NumCaptured_RBV` already reflects files on disk (ophyd-async's
        // multipart provider passes no flush_signal).
        let frames = self.io.num_captured().await.unwrap_or(0);
        frames / self.emit.lock().await.multiplier.max(1)
    }
    fn collect_stream_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset> {
        let descriptor = descriptor.to_string();
        let data_key = self.data_key_name();
        let fut = async move {
            let mut docs: Vec<StreamAsset> = Vec::new();
            let mut st = self.emit.lock().await;
            // Emit the one StreamResource on first use. The URI is the
            // directory (known up front from the provider — no
            // FullFileName_RBV read; that readback names only the latest
            // frame's file); the per-frame filename is reconstructed by the
            // consumer from the `template` parameter.
            if st.resource_uid.is_none() {
                let uid = uuid::Uuid::new_v4().to_string();
                st.resource_uid = Some(uid.clone());
                let mut parameters = HashMap::new();
                parameters.insert(
                    "template".to_string(),
                    serde_json::Value::String(self.template_parameter()),
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
                    data_key: data_key.clone(),
                    mimetype: self.mimetype.clone(),
                    uri: self.directory_uri(),
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
}

#[async_trait::async_trait]
impl DetectorControl for AreaDetectorCam {
    fn deadtime(&self, _exposure: Option<Duration>) -> Duration {
        // The generic areaDetector minimum dead-time is driver-specific; expose
        // zero here. Callers that need a real dead-time pass it via TriggerInfo.
        Duration::ZERO
    }
    async fn prepare(&self, info: TriggerInfo) -> Result<()> {
        if info.trigger != DetectorTrigger::Internal {
            return Err(BsrsError::Backend(format!(
                "AreaDetectorCam supports only DetectorTrigger::Internal, got {:?}",
                info.trigger
            )));
        }
        await_put(
            SignalBackend::<i64>::put(self.image_mode.as_ref(), Some(AD_IMAGE_MODE_MULTIPLE)),
            "prepare: ImageMode=Multiple",
        )
        .await?;
        await_put(
            SignalBackend::<i64>::put(
                self.num_images.as_ref(),
                Some(info.number_of_exposures() as i64),
            ),
            "prepare: NumImages",
        )
        .await?;
        if let Some(lt) = info.livetime {
            await_put(
                SignalBackend::<f64>::put(self.acquire_time.as_ref(), Some(lt.as_secs_f64())),
                "prepare: AcquireTime",
            )
            .await?;
        }
        Ok(())
    }
    async fn arm(&self) -> Status {
        // Acquire is a busy record: its put-callback completes when the whole
        // acquisition ends, so it becomes the returned Status — deliberately
        // unbounded, an N-frame acquisition takes as long as it takes. Before
        // returning, wait until Acquire_RBV confirms the driver is actually
        // acquiring (ophyd-async `set_and_wait_for_value(acquire, True,
        // wait_for_set_completion=False)`), so a fly scan's kickoff doesn't
        // complete with the detector still idle.
        let (status, setter) = Status::new();
        let acquire = self.acquire.clone();
        let prefix = self.prefix.clone();
        tokio::spawn(async move {
            match SignalBackend::<bool>::put(acquire.as_ref(), Some(true)).await {
                Ok(()) => setter.success(),
                Err(e) => setter.fail(StatusError::Failed(format!("{prefix}arm: Acquire: {e}"))),
            }
        });
        if let Err(e) = wait_for_bool(
            self.acquire_rbv.as_ref(),
            true,
            PUT_TIMEOUT,
            "arm: Acquire_RBV",
        )
        .await
        {
            let (failed, setter) = Status::new();
            setter.fail(StatusError::Failed(format!("{}{e}", self.prefix)));
            return failed;
        }
        status
    }
    async fn wait_for_idle(&self) -> Result<()> {
        // Inherent `wait_for_idle(timeout)` takes priority in method resolution.
        self.wait_for_idle(WARMUP_IDLE_TIMEOUT).await
    }
    async fn disarm(&self) -> Result<()> {
        // stop_busy_record: a put-callback for Acquire=0 can deadlock on a
        // busy record, so fire-and-forget then wait for the readback.
        self.acquire.put_nowait(false).await?;
        wait_for_bool(
            self.acquire_rbv.as_ref(),
            false,
            PUT_TIMEOUT,
            "disarm: Acquire_RBV",
        )
        .await
    }
}

/// A `StandardDetector` composed of an areaDetector camera driver and an
/// NDFileHDF5 plugin: the cam arms/acquires, the HDF plugin (inside the IOC)
/// writes the `.h5`, and bsrs emits the pointing `StreamResource`/`StreamDatum`.
pub type AreaDetectorHdf = StandardDetector<AreaDetectorCam, AdHdfWriter>;

/// Build an [`AreaDetectorHdf`] from a cam prefix and an HDF-plugin prefix,
/// writing files per `path_provider`. Call
/// [`connect_area_detector_hdf`] before running a plan.
pub fn area_detector_hdf(
    name: impl Into<String>,
    cam_prefix: impl Into<String>,
    hdf_prefix: impl Into<String>,
    path_provider: StaticPathProvider,
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
        name.clone(),
        io,
        cam.frame_info.clone() as Arc<dyn AdFrameInfoSource>,
        path_provider,
    )
    .with_ndattribute_sources(vec![
        cam.nd_attributes_file.clone() as Arc<dyn NdAttributeXmlSource>
    ]);
    StandardDetector::new(name, cam, writer)
}

/// Connect both halves of an [`AreaDetectorHdf`] (cam driver + HDF plugin IO).
pub async fn connect_area_detector_hdf(det: &AreaDetectorHdf, timeout: Duration) -> Result<()> {
    det.control().connect(timeout).await?;
    det.writer().connect(timeout).await?;
    Ok(())
}

/// A `StandardDetector` composed of an areaDetector camera driver and a
/// multipart (one-file-per-frame) file plugin — NDFileJPEG or NDFileTIFF.
pub type AreaDetectorMultipart = StandardDetector<AreaDetectorCam, AdMultipartWriter>;

fn area_detector_multipart(
    name: String,
    cam_prefix: impl Into<String>,
    file_prefix: impl Into<String>,
    path_provider: StaticPathProvider,
    make_writer: impl FnOnce(
        String,
        Arc<dyn AdFileIo>,
        Arc<dyn AdFrameInfoSource>,
        StaticPathProvider,
    ) -> AdMultipartWriter,
) -> AreaDetectorMultipart {
    let cam = AreaDetectorCam::new(cam_prefix);
    let io: Arc<dyn AdFileIo> = Arc::new(NdPluginFileIo::new(Arc::new(NdFile::new(file_prefix))));
    let writer = make_writer(
        name.clone(),
        io,
        cam.frame_info.clone() as Arc<dyn AdFrameInfoSource>,
        path_provider,
    );
    StandardDetector::new(name, cam, writer)
}

/// Build an [`AreaDetectorMultipart`] over an NDFileJPEG plugin from a cam
/// prefix and the plugin prefix, writing files per `path_provider`. Call
/// [`connect_area_detector_multipart`] before running a plan. The
/// `ADWriterType.JPEG` arm of ophyd-async's `make_writer_data_logic`.
pub fn area_detector_jpeg(
    name: impl Into<String>,
    cam_prefix: impl Into<String>,
    jpeg_prefix: impl Into<String>,
    path_provider: StaticPathProvider,
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
    path_provider: StaticPathProvider,
) -> AreaDetectorMultipart {
    area_detector_multipart(
        name.into(),
        cam_prefix,
        tiff_prefix,
        path_provider,
        AdMultipartWriter::tiff,
    )
}

/// Connect both halves of an [`AreaDetectorMultipart`] (cam driver + file
/// plugin IO).
pub async fn connect_area_detector_multipart(
    det: &AreaDetectorMultipart,
    timeout: Duration,
) -> Result<()> {
    det.control().connect(timeout).await?;
    det.writer().connect(timeout).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
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
        // Guard against accidental renumbering. ZRST=Idle ZRVL=0 in
        // ADBase.template:388-389.
        assert_eq!(AD_STATE_IDLE, 0);
        assert_eq!(AD_IMAGE_MODE_SINGLE, 0);
    }

    // -------------------------------------------------------------
    // Live-IOC smoke tests. Default target is
    // `epics-rs/examples/sim-detector`; they also run against the
    // mini-beamline MovingDot detector (`mini:dot:`, asyn port `DOT`).
    // Marked `#[ignore]` so they only run with `--ignored`.
    //
    // Setup (sim-detector):
    //   cd ~/codes/epics-rs/examples/sim-detector
    //   cargo run --bin sim_ioc --features ioc -- ioc/st.cmd
    //
    // Then in another shell:
    //   cargo test -p bsrs --features host \
    //       areadetector::tests:: -- --ignored --nocapture
    //
    // Retarget with env overrides (defaults are sim-detector `SIM1:` /
    // `SIM1`); for the mini-beamline:
    //   BSRS_AD_PREFIX=mini:dot:  BSRS_AD_PORT=DOT
    // -------------------------------------------------------------

    fn ad_prefix() -> String {
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
        let prefix = ad_prefix();
        let cam = AreaDetectorCam::new(format!("{prefix}cam1:"));
        cam.connect(Duration::from_secs(5))
            .await
            .expect("connect cam1");

        let prev_mode = SignalBackend::<i64>::get_value(cam.image_mode.as_ref())
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
        let state = SignalBackend::<i64>::get_value(cam.detector_state_rbv.as_ref())
            .await
            .expect("read DetectorState_RBV post");
        let restored_mode = SignalBackend::<i64>::get_value(cam.image_mode.as_ref())
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
    /// magick1, nexus1])` (port from `BSRS_AD_PORT`, default `SIM1`) records the
    /// routing into each plugin's `stage_sigs` **without touching the IOC**;
    /// staging then applies it — (a) `HDF1:NDArrayPort = <port>`,
    /// (b) `HDF1:EnableCallbacks` enabled, (c) siblings' `EnableCallbacks`
    /// disabled — and unstaging restores every signal to its pre-stage value.
    /// Also exercises the long-string `HDF1:FilePath` `CaStringKind::Long`
    /// get/put path.
    ///
    /// Non-destructive: FilePath, every plugin enable, and HDF1's port are
    /// captured and restored, so the IOC is left as it was found.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn ad_select_save_plugin_against_sim_detector() {
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
        let magick1 = NdFile::new(format!("{prefix}Magick1:"));
        let nexus1 = NdFile::new(format!("{prefix}Nexus1:"));

        let timeout = Duration::from_secs(5);
        tokio::try_join!(
            hdf1.connect(timeout),
            jpeg1.connect(timeout),
            magick1.connect(timeout),
            nexus1.connect(timeout),
        )
        .expect("connect file plugins");

        // (1) Long-string round-trip on FilePath (CaStringKind::Long),
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
        let pre_magick1_en = en(&magick1).await;
        let pre_nexus1_en = en(&nexus1).await;
        let pre_hdf1_port = port_of(&hdf1).await;

        // Record the routing into stage_sigs — this must NOT write to the IOC.
        let siblings = [&jpeg1, &magick1, &nexus1];
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
        let magick1_en = en(&magick1).await;
        let nexus1_en = en(&nexus1).await;
        eprintln!(
            "staged: HDF1.port={hdf1_port:?}, \
             enables HDF1={hdf1_en} JPEG1={jpeg1_en} Magick1={magick1_en} Nexus1={nexus1_en}"
        );
        assert_eq!(hdf1_port, port, "HDF1.NDArrayPort not routed to {port}");
        assert!(hdf1_en, "HDF1 was not enabled by staging");
        assert!(!jpeg1_en, "JPEG1 not disabled by staging");
        assert!(!magick1_en, "Magick1 not disabled by staging");
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
        assert_eq!(
            en(&magick1).await,
            pre_magick1_en,
            "Magick1 enable not restored"
        );
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
    use crate::protocols_async::{Stageable, WritesStreamAssets};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// What [`AdFileIo::configure`] received: `(directory, filename,
    /// template, num_capture, create_dir_depth)`.
    type ConfigureArgs = (String, String, String, i64, i64);

    /// In-memory `AdFileIo` so the document-composition logic is testable with
    /// no live IOC (mirrors ophyd-async's IO-vs-writer split).
    struct FakeAdFileIo {
        captured: AtomicU64,
        full_name: String,
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
        fn new(full_name: &str) -> Arc<Self> {
            let (tx, rx) = watch::channel(0u64);
            Arc::new(Self {
                captured: AtomicU64::new(0),
                full_name: full_name.to_string(),
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
        async fn configure(&self, d: &str, f: &str, t: &str, n: i64, depth: i64) -> Result<()> {
            if !self.path_exists.load(Ordering::SeqCst) {
                return Err(BsrsError::Backend(format!(
                    "FilePath {d} doesn't exist on the IOC host or is not writable"
                )));
            }
            *self.configured.lock().unwrap() = Some((d.into(), f.into(), t.into(), n, depth));
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
        async fn full_file_name(&self) -> Result<String> {
            Ok(self.full_name.clone())
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

    fn writer_with(io: Arc<FakeAdFileIo>) -> AdHdfWriter {
        AdHdfWriter::new(
            "det",
            io,
            Arc::new(FakeFrameInfo),
            StaticPathProvider::new("/data/scans/", "scan"),
        )
    }

    fn multipart_writer_with(io: Arc<FakeAdFileIo>) -> AdMultipartWriter {
        AdMultipartWriter::jpeg(
            "det",
            io,
            Arc::new(FakeFrameInfo),
            StaticPathProvider::new("/data/scans/", "scan"),
        )
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
    fn compose_frame_shape_orders_zyx_and_handles_color_modes() {
        // Mono 2-D frame: Z unused (0) → [Y, X].
        assert_eq!(
            compose_frame_shape(0, 768, 1024, AD_COLOR_MODE_MONO, "P:").unwrap(),
            vec![768, 1024]
        );
        // 3-D mono: [Z, Y, X], slowest first.
        assert_eq!(
            compose_frame_shape(4, 20, 10, AD_COLOR_MODE_MONO, "P:").unwrap(),
            vec![4, 20, 10]
        );
        // RGB1 prepends the color dim (ophyd-async `shape = [3, *shape]`).
        assert_eq!(
            compose_frame_shape(0, 768, 1024, AD_COLOR_MODE_RGB1, "P:").unwrap(),
            vec![3, 768, 1024]
        );
        // Any other non-Mono color mode is unsupported.
        let err = compose_frame_shape(0, 768, 1024, 1, "P:")
            .unwrap_err()
            .to_string();
        assert!(err.contains("ColorMode_RBV=1"), "{err}");
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
        let io = FakeAdFileIo::new("/data/scans/scan.h5");
        let xml = r#"<Attributes>
            <Attribute name="temp" type="EPICS_PV" source="SIM:TEMP" dbrtype="DBR_DOUBLE"/>
            <Attribute name="id" type="PARAM" source="UNIQUE_ID" datatype="INT64"/>
        </Attributes>"#;
        let w = writer_with(io.clone())
            .with_ndattribute_sources(vec![Arc::new(FakeXmlSource(xml.into()))]);
        let keys = w.open(1).await.unwrap();
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
        let io = FakeAdFileIo::new("/x.h5");
        let xml = r#"<Attributes>
            <Attribute name="temp" type="EPICS_PV" source="SIM:TEMP" dbrtype="DBR_DOUBLE"/>
        </Attributes>"#;
        let w = writer_with(io.clone())
            .with_ndattribute_sources(vec![Arc::new(FakeXmlSource(xml.into()))])
            .with_ndattributes(vec![
                NdAttributeDataset::new("temp", "<i4"),
                NdAttributeDataset::new("extra", "<f8"),
            ]);
        let keys = w.open(1).await.unwrap();
        // Declared "temp" replaces the discovered one (dtype <i4, no source).
        assert_eq!(
            keys["temp"].dtype_numpy,
            Some(crate::event_model::DtypeNumpy::Scalar("<i4".to_string()))
        );
        assert!(keys.contains_key("extra"));
        // det_image + temp + extra, no duplicate for the collision.
        assert_eq!(keys.len(), 3);
    }

    #[tokio::test]
    async fn filename_ndattributes_source_is_skipped() {
        let io = FakeAdFileIo::new("/x.h5");
        // ADCore treats content without "<Attributes>" as a filename on the
        // IOC host; discovery cannot read it, so it contributes nothing.
        let w = writer_with(io.clone()).with_ndattribute_sources(vec![Arc::new(FakeXmlSource(
            "/epics/attributes.xml".into(),
        ))]);
        let keys = w.open(1).await.unwrap();
        assert_eq!(keys.len(), 1, "main image only");
        assert!(keys.contains_key("det_image"));
    }

    #[tokio::test]
    async fn open_configures_stream_capture_and_reports_external_datakey() {
        let io = FakeAdFileIo::new("/data/scans/scan.h5");
        let w = writer_with(io.clone());
        let keys = w.open(1).await.unwrap();
        let cfg = io
            .configured
            .lock()
            .unwrap()
            .clone()
            .expect("configure called");
        assert_eq!(cfg.0, "/data/scans/");
        assert_eq!(cfg.1, "scan");
        assert_eq!(cfg.2, AD_HDF_TEMPLATE);
        assert_eq!(cfg.3, 0, "NumCapture=0 (capture until stopped)");
        assert_eq!(cfg.4, 0, "CreateDirectory depth defaults to 0");
        assert!(io.capturing.load(Ordering::SeqCst), "capture started");
        assert!(
            io.hdf_setup.load(Ordering::SeqCst),
            "HDF arming puts (LazyOpen/SWMR/NumExtraDims/XMLFileName) applied"
        );
        assert!(
            io.callbacks_enabled.load(Ordering::SeqCst),
            "plugin callbacks enabled"
        );
        let dk = keys.get("det_image").expect("det_image data key");
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
        let io = FakeAdFileIo::new("/x.h5");
        io.path_exists.store(false, Ordering::SeqCst);
        let w = writer_with(io.clone());
        let err = w.open(1).await.unwrap_err().to_string();
        assert!(err.contains("doesn't exist"), "{err}");
        assert!(
            !io.capturing.load(Ordering::SeqCst),
            "capture must not start when the path check fails"
        );
    }

    #[test]
    fn static_path_provider_normalizes_trailing_slash_and_depth() {
        let p = StaticPathProvider::new("/data/scans", "scan");
        assert_eq!(p.directory, "/data/scans/");
        assert_eq!(p.create_dir_depth, 0);
        let p = StaticPathProvider::new("/data/scans/", "scan").with_create_dir_depth(2);
        assert_eq!(p.directory, "/data/scans/", "no double slash");
        assert_eq!(p.create_dir_depth, 2);
    }

    #[tokio::test]
    async fn open_rejects_multiplier_above_one() {
        let io = FakeAdFileIo::new("/x.h5");
        let w = writer_with(io);
        let err = w.open(2).await.unwrap_err().to_string();
        assert!(err.contains("multiplier=2"), "{err}");
    }

    #[test]
    fn ad_datatype_maps_all_known_codes_and_rejects_unknown() {
        // NDDataType_t 0..=9 → numpy; anything else is None.
        let want = [
            "|i1", "|u1", "<i2", "<u2", "<i4", "<u4", "<i8", "<u8", "<f4", "<f8",
        ];
        for (code, np) in want.iter().enumerate() {
            assert_eq!(ad_datatype_to_numpy(code as i64), Some(*np));
        }
        assert_eq!(ad_datatype_to_numpy(10), None);
        assert_eq!(ad_datatype_to_numpy(-1), None);
    }

    #[tokio::test]
    async fn collect_emits_resource_pointing_at_ioc_file_then_datum() {
        let io = FakeAdFileIo::new("/data/scans/scan.h5");
        io.frames_per_chunk.store(4, Ordering::SeqCst);
        let w = writer_with(io.clone());
        w.open(1).await.unwrap();
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
                assert_eq!(r.data_key, "det_image");
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
        let io = FakeAdFileIo::new("/data/scans/scan.h5");
        let w = writer_with(io.clone()).with_ndattributes(vec![
            NdAttributeDataset::new("temp", "<f8"),
            NdAttributeDataset::new("counter", "<i4"),
        ]);
        // open() reports a scalar external data key per attribute + the image.
        let keys = w.open(1).await.unwrap();
        assert!(keys.contains_key("det_image"));
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
            if r.data_key != "det_image" {
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
    async fn empty_full_file_name_yields_empty_uri() {
        let io = FakeAdFileIo::new("");
        let w = writer_with(io.clone());
        w.open(1).await.unwrap();
        io.set_captured(1);
        let docs: Vec<StreamAsset> = w.collect_stream_docs(1, "d").collect().await;
        match &docs[0] {
            StreamAsset::Resource(r) => assert_eq!(r.uri, "", "no readback → empty URI"),
            _ => panic!("first doc must be StreamResource"),
        }
    }

    #[tokio::test]
    async fn indices_written_flushes_before_reading_count() {
        // Parity with ophyd-async make_stream_docs: flush before reading the
        // frame count so it reflects frames actually on disk (SWMR).
        let io = FakeAdFileIo::new("/x.h5");
        let w = writer_with(io.clone());
        io.set_captured(4);
        let n = w.indices_written().await;
        assert_eq!(n, 4);
        assert_eq!(
            io.flushes.load(Ordering::SeqCst),
            1,
            "indices_written must flush exactly once before reading the count"
        );
    }

    #[tokio::test]
    async fn observe_reflects_num_captured_updates() {
        let io = FakeAdFileIo::new("/x.h5");
        let w = writer_with(io.clone());
        let mut rx = w.observe_indices_written();
        assert_eq!(*rx.borrow_and_update(), 0);
        io.set_captured(5);
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow_and_update(), 5);
    }

    #[tokio::test]
    async fn close_stops_capture() {
        let io = FakeAdFileIo::new("/x.h5");
        let w = writer_with(io.clone());
        w.open(1).await.unwrap();
        assert!(io.capturing.load(Ordering::SeqCst));
        w.close().await.unwrap();
        assert!(!io.capturing.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn composite_writes_stream_assets_drains_ad_writer() {
        // The composite StandardDetector<AreaDetectorCam, AdHdfWriter> builds,
        // and its WritesStreamAssets seam — the exact one the engine's step/fly
        // save path drains — yields the AD writer's resource + datum.
        let io = FakeAdFileIo::new("/data/scan.h5");
        let writer = writer_with(io.clone());
        let cam = AreaDetectorCam::new("13SIM1:cam1:");
        let det = StandardDetector::new("det", cam, writer);
        det.writer().open(1).await.unwrap();
        io.set_captured(2);
        let up_to = WritesStreamAssets::get_index(&det).await.unwrap();
        assert_eq!(up_to, 2);
        let docs: Vec<StreamAsset> = det.collect_asset_docs(up_to, "descZ").collect().await;
        assert_eq!(docs.len(), 2);
        assert!(matches!(docs[0], StreamAsset::Resource(_)));
        match &docs[1] {
            StreamAsset::Datum(d) => assert_eq!(d.descriptor, "descZ"),
            _ => panic!("second doc must be StreamDatum"),
        }
    }

    #[test]
    fn path_provider_stores_directory_and_filename() {
        let p = StaticPathProvider::new("/gpfs/data/", "img");
        assert_eq!(p.directory, "/gpfs/data/");
        assert_eq!(p.filename, "img");
    }

    // Live-IOC smoke test — drives a real areaDetector cam + NDFileHDF5 plugin
    // end to end and asserts the emitted StreamResource points at the file the
    // IOC actually wrote. `#[ignore]`; run with `--ignored` against an IOC.
    //   BSRS_AD_PREFIX  cam prefix   (default 13SIM1:cam1:)
    //   BSRS_AD_HDF     HDF1 prefix  (default 13SIM1:HDF1:)
    //   BSRS_AD_DIR     write dir    (default /tmp/), must be IOC-visible
    #[tokio::test]
    #[ignore]
    async fn area_detector_hdf_live_emits_real_file_stream_resource() {
        let cam_prefix =
            std::env::var("BSRS_AD_PREFIX").unwrap_or_else(|_| "13SIM1:cam1:".to_string());
        let hdf_prefix =
            std::env::var("BSRS_AD_HDF").unwrap_or_else(|_| "13SIM1:HDF1:".to_string());
        let dir = std::env::var("BSRS_AD_DIR").unwrap_or_else(|_| "/tmp/".to_string());
        let det = area_detector_hdf(
            "addet",
            cam_prefix,
            hdf_prefix,
            StaticPathProvider::new(dir, "bsrs_ad_live"),
        );
        connect_area_detector_hdf(&det, Duration::from_secs(5))
            .await
            .expect("connect");
        // Prime array dims so the HDF plugin can open the file.
        det.control().warmup().await.expect("warmup");
        // Stage opens (configures + starts capture) the writer.
        Stageable::stage(&det).await.expect("stage");
        // One internal-trigger acquisition.
        det.control().arm().await.await.expect("acquire");
        // Fully-qualified: the inherent `wait_for_idle(timeout)` shadows the
        // zero-arg trait method by name.
        DetectorControl::wait_for_idle(det.control())
            .await
            .expect("idle");
        let up_to = WritesStreamAssets::get_index(&det).await.expect("index");
        assert!(up_to >= 1, "expected >=1 frame written, got {up_to}");
        let docs: Vec<StreamAsset> = det.collect_asset_docs(up_to, "live-desc").collect().await;
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
        let io = FakeAdFileIo::new("/data/scans/scan_000000.jpg");
        let w = multipart_writer_with(io.clone());
        let keys = w.open(1).await.unwrap();

        let cfg = io.configured.lock().unwrap().clone().unwrap();
        assert_eq!(cfg.0, "/data/scans/");
        assert_eq!(cfg.1, "scan");
        assert_eq!(cfg.2, "%s%s_%6.6d.jpg");
        assert_eq!(cfg.3, 0, "NumCapture=0: capture until close()");
        assert_eq!(cfg.4, 0, "default create_dir_depth");
        assert!(io.callbacks_enabled.load(Ordering::SeqCst));
        assert!(io.capturing.load(Ordering::SeqCst));

        let dk = keys.get("det_image").expect("image data key");
        assert_eq!(dk.source, "file://localhost/data/scans/");
        assert_eq!(dk.shape, vec![Some(1), Some(20), Some(10)]);
        assert_eq!(dk.dtype, Dtype::Array);
        assert_eq!(
            dk.dtype_numpy,
            Some(crate::event_model::DtypeNumpy::Scalar("<u2".to_string()))
        );
        assert_eq!(dk.external.as_deref(), Some("STREAM:"));
    }

    #[tokio::test]
    async fn multipart_open_rejects_multiplier_above_one() {
        let io = FakeAdFileIo::new("");
        let w = multipart_writer_with(io);
        let err = w.open(2).await.unwrap_err();
        assert!(
            err.to_string().contains("multiplier > 1"),
            "unexpected error: {err}"
        );
    }

    /// The one `StreamResource` points at the directory with the frame-number
    /// `template` parameter and `chunk_shape` `[1, *frame_shape]` (one file
    /// per frame); datums cover the new frames with `seq_nums` left `{0,0}`
    /// for the engine. A second collect emits only the incremental datum.
    #[tokio::test]
    async fn multipart_collect_emits_directory_resource_then_datums() {
        let io = FakeAdFileIo::new("/data/scans/scan_000001.jpg");
        let w = multipart_writer_with(io.clone());
        w.open(1).await.unwrap();
        io.set_captured(2);
        let docs: Vec<StreamAsset> = w
            .collect_stream_docs(w.indices_written().await, "desc-mp-1")
            .collect()
            .await;
        assert_eq!(docs.len(), 2);
        let resource_uid = match &docs[0] {
            StreamAsset::Resource(r) => {
                assert_eq!(r.data_key, "det_image");
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
        let io = FakeAdFileIo::new("");
        let w = multipart_writer_with(io.clone());
        w.open(1).await.unwrap();
        assert!(io.capturing.load(Ordering::SeqCst));
        w.close().await.unwrap();
        assert!(!io.capturing.load(Ordering::SeqCst));

        let tio = FakeAdFileIo::new("");
        let t = AdMultipartWriter::tiff(
            "det",
            tio.clone(),
            Arc::new(FakeFrameInfo),
            StaticPathProvider::new("/data/scans/", "scan"),
        );
        t.open(1).await.unwrap();
        let cfg = tio.configured.lock().unwrap().clone().unwrap();
        assert_eq!(cfg.2, "%s%s_%6.6d.tiff");
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
