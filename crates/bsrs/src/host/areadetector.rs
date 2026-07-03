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
/// 5=Aborting, 6=Error.
pub const AD_STATE_IDLE: i64 = 0;

/// `ImageMode` value for single-frame acquisition (mbbo ordering from
/// `ADBase.template`: 0=Single, 1=Multiple, 2=Continuous).
pub const AD_IMAGE_MODE_SINGLE: i64 = 0;

/// `ImageMode` value for a bounded burst of `NumImages` frames.
pub const AD_IMAGE_MODE_MULTIPLE: i64 = 1;

/// `FileWriteMode` value for streaming capture (mbbo ordering from
/// `NDFile.template`: 0=Single, 1=Capture, 2=Stream).
pub const AD_FILE_WRITE_MODE_STREAM: i64 = 2;

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
    /// `Acquire` (bo) — true to start, false to stop.
    pub acquire: Arc<EpicsCaBackend<bool>>,
    /// `AcquireTime` (ao) — exposure time, seconds.
    pub acquire_time: Arc<EpicsCaBackend<f64>>,
    /// `ArrayCounter_RBV` (longin) — total frames produced.
    pub array_counter_rbv: Arc<EpicsCaBackend<i64>>,
    /// `DetectorState_RBV` (mbbi).
    pub detector_state_rbv: Arc<EpicsCaBackend<i64>>,
}

impl AreaDetectorCam {
    /// Build the handle. Does NOT connect; call `connect` afterwards.
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self {
            image_mode: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "ImageMode"))),
            num_images: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "NumImages"))),
            acquire: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "Acquire"))),
            acquire_time: Arc::new(EpicsCaBackend::<f64>::new(join(&prefix, "AcquireTime"))),
            array_counter_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "ArrayCounter_RBV",
            ))),
            detector_state_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "DetectorState_RBV",
            ))),
            prefix,
        }
    }

    /// Connect every channel in parallel.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        let (a, b, c, d, e, f) = tokio::join!(
            SignalBackend::<i64>::connect(self.image_mode.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.num_images.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.acquire.as_ref(), timeout),
            SignalBackend::<f64>::connect(self.acquire_time.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.array_counter_rbv.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.detector_state_rbv.as_ref(), timeout),
        );
        a?;
        b?;
        c?;
        d?;
        e?;
        f?;
        Ok(())
    }

    /// Poll `DetectorState_RBV` until it reports idle, or `timeout`
    /// elapses.
    pub async fn wait_for_idle(&self, timeout: Duration) -> Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let state = SignalBackend::<i64>::get_value(self.detector_state_rbv.as_ref()).await?;
            if state == AD_STATE_IDLE {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(BsrsError::Backend(format!(
                    "{}DetectorState_RBV did not reach Idle within {:?} (last={state})",
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
    /// `Capture` (bo) — start/stop capture in Capture/Stream mode.
    pub capture: Arc<EpicsCaBackend<bool>>,
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
    /// `ArraySize0_RBV`/`ArraySize1_RBV`/`ArraySize2_RBV` (longin) — the
    /// dimensions of the incoming NDArray, fastest-varying first. Zero for
    /// unused dimensions. Read at `open()` to shape the emitted `DataKey`.
    pub array_size_rbv: [Arc<EpicsCaBackend<i64>>; 3],
    /// `DataType_RBV` (mbbi) — the NDArray element type (`NDDataType_t`
    /// ordering), mapped to a numpy dtype string for the `DataKey`.
    pub data_type_rbv: Arc<EpicsCaBackend<i64>>,
    /// `FlushNow` (bo) — force the plugin to flush buffered frames to the
    /// file. Meaningful in SWMR streaming; the write index only reflects
    /// flushed frames, so we flush before reading `NumCaptured_RBV`.
    pub flush_now: Arc<EpicsCaBackend<bool>>,
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
            num_capture: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "NumCapture"))),
            num_captured_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(
                &prefix,
                "NumCaptured_RBV",
            ))),
            full_file_name_rbv: Arc::new(EpicsCaBackend::<String>::new_long_string(join(
                &prefix,
                "FullFileName_RBV",
            ))),
            array_size_rbv: [
                Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "ArraySize0_RBV"))),
                Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "ArraySize1_RBV"))),
                Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "ArraySize2_RBV"))),
            ],
            data_type_rbv: Arc::new(EpicsCaBackend::<i64>::new(join(&prefix, "DataType_RBV"))),
            flush_now: Arc::new(EpicsCaBackend::<bool>::new(join(&prefix, "FlushNow"))),
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
        // Frame-info readbacks (shape + dtype), connected in a second batch to
        // keep the tuple manageable.
        let (k, l, m, n, o) = tokio::join!(
            SignalBackend::<i64>::connect(self.array_size_rbv[0].as_ref(), timeout),
            SignalBackend::<i64>::connect(self.array_size_rbv[1].as_ref(), timeout),
            SignalBackend::<i64>::connect(self.array_size_rbv[2].as_ref(), timeout),
            SignalBackend::<i64>::connect(self.data_type_rbv.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.flush_now.as_ref(), timeout),
        );
        k?;
        l?;
        m?;
        n?;
        o?;
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

    /// Start capture (`Capture=1`). In Stream mode the file opens on this
    /// transition (or on the first frame when `LazyOpen` is set).
    pub async fn start_capture(&self) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.capture.as_ref(), Some(true)),
            "NdFile::start_capture",
        )
        .await
    }

    /// Stop capture (`Capture=0`) — flushes and closes the file.
    pub async fn stop_capture(&self) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.capture.as_ref(), Some(false)),
            "NdFile::stop_capture",
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

    /// Discover the incoming frame's shape and numpy dtype from
    /// `ArraySize{0,1,2}_RBV` + `DataType_RBV`. Zero-sized dimensions are
    /// dropped (unused / not yet primed), fastest-varying first — matching
    /// ophyd-async's `get_ndarray_resource_info` (`_data_logic.py`). Errors if
    /// `DataType_RBV` is not a known `NDDataType_t`.
    pub async fn frame_info(&self) -> Result<(Vec<u64>, String)> {
        let (s0, s1, s2, dt) = tokio::join!(
            SignalBackend::<i64>::get_value(self.array_size_rbv[0].as_ref()),
            SignalBackend::<i64>::get_value(self.array_size_rbv[1].as_ref()),
            SignalBackend::<i64>::get_value(self.array_size_rbv[2].as_ref()),
            SignalBackend::<i64>::get_value(self.data_type_rbv.as_ref()),
        );
        let shape: Vec<u64> = [s0?, s1?, s2?]
            .into_iter()
            .filter(|&d| d > 0)
            .map(|d| d as u64)
            .collect();
        let dt = dt?;
        let dtype_numpy = ad_datatype_to_numpy(dt).ok_or_else(|| {
            BsrsError::Backend(format!(
                "{}DataType_RBV={dt} is not a known NDDataType_t",
                self.plugin.prefix
            ))
        })?;
        Ok((shape, dtype_numpy.to_string()))
    }

    /// Force a flush (`FlushNow=1`) so `NumCaptured_RBV` reflects frames
    /// actually written to the file. A no-op on plugins without SWMR support.
    pub async fn flush(&self) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.flush_now.as_ref(), Some(true)),
            "NdFile::flush",
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
/// Default `FileTemplate` — `<path><name>.h5`, no auto-increment suffix, so the
/// IOC-resolved `FullFileName_RBV` is deterministic.
const AD_HDF_TEMPLATE: &str = "%s%s.h5";

/// Where the areaDetector file plugin should write and under what basename.
/// Mirrors ophyd-async's `PathProvider`/`PathInfo`: the writer sets the IOC's
/// `FilePath`/`FileName` from this, then builds the `StreamResource` URI from
/// the IOC's `FullFileName_RBV` readback (so it points at what the IOC wrote).
#[derive(Clone, Debug)]
pub struct StaticPathProvider {
    /// Directory the IOC writes into (must be visible on the IOC host).
    pub directory: String,
    /// File basename (pre-template), e.g. `"scan"`.
    pub filename: String,
}

impl StaticPathProvider {
    /// Build a provider that always returns the same directory + basename.
    pub fn new(directory: impl Into<String>, filename: impl Into<String>) -> Self {
        Self {
            directory: directory.into(),
            filename: filename.into(),
        }
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
    /// Point the plugin at `directory`/`filename` with `template`, switch it to
    /// Stream mode, and set `NumCapture` (`0` = until `stop_capture`).
    async fn configure(
        &self,
        directory: &str,
        filename: &str,
        template: &str,
        num_capture: i64,
    ) -> Result<()>;
    /// Start capture — the plugin opens the file and accepts frames.
    async fn start_capture(&self) -> Result<()>;
    /// Stop capture — flush and close the file.
    async fn stop_capture(&self) -> Result<()>;
    /// Frames written so far (the per-frame write index).
    async fn num_captured(&self) -> Result<u64>;
    /// Absolute path the IOC resolved for the open file.
    async fn full_file_name(&self) -> Result<String>;
    /// Incoming frame shape (fastest-varying first, zero dims dropped) and
    /// numpy dtype string, for the emitted `DataKey`.
    async fn frame_info(&self) -> Result<(Vec<u64>, String)>;
    /// Force a flush so `num_captured` reflects frames written to the file.
    async fn flush(&self) -> Result<()>;
    /// Watch the frames-written counter, for `complete()` in fly scans.
    fn observe_num_captured(&self) -> watch::Receiver<u64>;
}

/// [`AdFileIo`] backed by a real [`NdFile`] over Channel Access.
pub struct NdFileIo {
    file: Arc<NdFile>,
    index_rx: watch::Receiver<u64>,
    index_tx: Arc<watch::Sender<u64>>,
    /// Kept alive so the `NumCaptured_RBV` monitor feeding `index_rx` is not
    /// torn down. Installed by [`connect`](AdFileIo::connect).
    token: std::sync::Mutex<Option<SubToken>>,
}

impl NdFileIo {
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
impl AdFileIo for NdFileIo {
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
    ) -> Result<()> {
        self.file.set_path(directory).await?;
        self.file.set_name(filename).await?;
        self.file.set_template(template).await?;
        self.file.set_write_mode(AD_FILE_WRITE_MODE_STREAM).await?;
        self.file.set_num_capture(num_capture).await?;
        Ok(())
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
    async fn frame_info(&self) -> Result<(Vec<u64>, String)> {
        self.file.frame_info().await
    }
    async fn flush(&self) -> Result<()> {
        self.file.flush().await
    }
    fn observe_num_captured(&self) -> watch::Receiver<u64> {
        self.index_rx.clone()
    }
}

/// One per-frame NDAttribute the IOC's NDFileHDF5 plugin writes into the same
/// `.h5` alongside the main image, at `/entry/instrument/NDAttributes/<name>`.
/// Declared explicitly (bsrs has no NDAttributes-config discovery); the writer
/// then emits a `StreamResource`/`StreamDatum` for it just like ophyd-async's
/// `ndattribute_datasets` (`_data_logic.py`).
#[derive(Clone, Debug)]
pub struct NdAttributeDataset {
    /// Attribute name — used as both the `DataKey` and the HDF dataset leaf.
    pub name: String,
    /// numpy dtype string of the attribute values, e.g. `"<f8"`.
    pub dtype_numpy: String,
}

impl NdAttributeDataset {
    /// Build a spec for attribute `name` with numpy dtype `dtype_numpy`.
    pub fn new(name: impl Into<String>, dtype_numpy: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            dtype_numpy: dtype_numpy.into(),
        }
    }
}

/// Compose a `StreamResource` for an HDF dataset in the IOC's file.
fn ad_stream_resource(uid: String, data_key: String, uri: &str, dataset: &str) -> StreamResource {
    let mut parameters = HashMap::new();
    parameters.insert(
        "dataset".to_string(),
        serde_json::Value::String(dataset.to_string()),
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
fn ad_stream_datum(resource_uid: String, descriptor: &str, start: u64, stop: u64) -> StreamDatum {
    StreamDatum {
        uid: uuid::Uuid::new_v4().to_string(),
        stream_resource: resource_uid,
        descriptor: descriptor.to_string(),
        indices: StreamRange { start, stop },
        seq_nums: StreamRange {
            start: start + 1,
            stop: stop + 1,
        },
    }
}

/// Emit state for [`AdHdfWriter`]: the once-emitted `StreamResource` UIDs (main
/// image + one per NDAttribute) and the frame cursor (`StreamDatum`s cover
/// `[last_emitted, up_to)`).
#[derive(Default)]
struct AdEmitState {
    main_resource_uid: Option<String>,
    attr_resource_uids: HashMap<String, String>,
    last_emitted: u64,
}

/// `DetectorWriter` for an areaDetector NDFileHDF5 plugin. The IOC writes the
/// actual `.h5`; this writer only arms the plugin and emits the
/// `StreamResource`/`StreamDatum` documents that point a downstream consumer
/// (e.g. a Tiled-writing process) at the IOC's file. Port of ophyd-async's
/// `ADHDFWriter` data-logic (`epics/adcore/_data_logic.py`).
pub struct AdHdfWriter {
    name: String,
    io: Arc<dyn AdFileIo>,
    path_provider: StaticPathProvider,
    /// Per-frame NDAttribute datasets the IOC also writes into the file; each
    /// gets its own `StreamResource`/`StreamDatum`. Empty = main image only.
    ndattributes: Vec<NdAttributeDataset>,
    /// Guards single-`StreamResource` emission and the datum cursor.
    emit: tokio::sync::Mutex<AdEmitState>,
}

impl AdHdfWriter {
    /// Build a writer over `io`, writing files per `path_provider`.
    pub fn new(
        name: impl Into<String>,
        io: Arc<dyn AdFileIo>,
        path_provider: StaticPathProvider,
    ) -> Self {
        Self {
            name: name.into(),
            io,
            path_provider,
            ndattributes: Vec::new(),
            emit: tokio::sync::Mutex::new(AdEmitState::default()),
        }
    }

    /// Declare the per-frame NDAttribute datasets the IOC writes into the file,
    /// so the writer emits a `StreamResource`/`StreamDatum` for each in addition
    /// to the main image.
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
}

#[async_trait::async_trait]
impl DetectorWriter for AdHdfWriter {
    async fn open(&self, _multiplier: u32) -> Result<HashMap<String, DataKey>> {
        // Configure + arm the IOC file plugin. NumCapture=0: capture until
        // close() clears Capture, matching a step scan whose frame count is
        // unknown until the plan ends.
        self.io
            .configure(
                &self.path_provider.directory,
                &self.path_provider.filename,
                AD_HDF_TEMPLATE,
                0,
            )
            .await?;
        self.io.start_capture().await?;
        // Reset emit state for this staging.
        {
            let mut st = self.emit.lock().await;
            st.main_resource_uid = None;
            st.attr_resource_uids.clear();
            st.last_emitted = 0;
        }
        // Discover the frame shape + dtype from the IOC (ArraySize*/DataType
        // RBVs). Requires a primed detector — the shape is `[]` until the first
        // frame flows (e.g. after `AreaDetectorCam::warmup`).
        let (shape, dtype_numpy) = self.io.frame_info().await?;
        let mut out = HashMap::new();
        out.insert(
            self.data_key_name(),
            DataKey {
                source: format!("ca://{}", self.data_key_name()),
                dtype: Dtype::Array,
                shape: shape.into_iter().map(Some).collect(),
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
        // One scalar external data key per declared NDAttribute dataset.
        for a in &self.ndattributes {
            out.insert(
                a.name.clone(),
                DataKey {
                    source: format!("ca://{}", a.name),
                    dtype: Dtype::Number,
                    shape: Vec::new(),
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
        self.io.observe_num_captured()
    }
    async fn indices_written(&self) -> u64 {
        // Flush first so the count reflects frames actually on disk (SWMR),
        // mirroring ophyd-async setting flush_signal before reading the count.
        // Best-effort: a plugin without SWMR flush support just yields the
        // unflushed count.
        let _ = self.io.flush().await;
        self.io.num_captured().await.unwrap_or(0)
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
            // Emit each StreamResource once. The main image and every
            // NDAttribute live in the same .h5, so they share one URI resolved
            // from FullFileName_RBV — read it only when a resource still needs
            // emitting.
            let need_main = st.main_resource_uid.is_none();
            let need_attr = self
                .ndattributes
                .iter()
                .any(|a| !st.attr_resource_uids.contains_key(&a.name));
            if need_main || need_attr {
                let path = self.io.full_file_name().await.unwrap_or_default();
                let uri = if path.is_empty() {
                    String::new()
                } else {
                    format!("file://{path}")
                };
                if need_main {
                    let uid = uuid::Uuid::new_v4().to_string();
                    st.main_resource_uid = Some(uid.clone());
                    docs.push(StreamAsset::Resource(ad_stream_resource(
                        uid,
                        data_key.clone(),
                        &uri,
                        AD_HDF_DATASET,
                    )));
                }
                for a in &self.ndattributes {
                    if !st.attr_resource_uids.contains_key(&a.name) {
                        let uid = uuid::Uuid::new_v4().to_string();
                        st.attr_resource_uids.insert(a.name.clone(), uid.clone());
                        let dataset = format!("/entry/instrument/NDAttributes/{}", a.name);
                        docs.push(StreamAsset::Resource(ad_stream_resource(
                            uid,
                            a.name.clone(),
                            &uri,
                            &dataset,
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
                for a in &self.ndattributes {
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
        // Start acquisition in the background and resolve the Status when the
        // Acquire put-callback fires (acquisition complete). Returning promptly
        // lets a fly scan's complete() watch the write index concurrently.
        let (status, setter) = Status::new();
        let acquire = self.acquire.clone();
        let prefix = self.prefix.clone();
        tokio::spawn(async move {
            match await_put(
                SignalBackend::<bool>::put(acquire.as_ref(), Some(true)),
                "arm: Acquire",
            )
            .await
            {
                Ok(()) => setter.success(),
                Err(e) => setter.fail(StatusError::Failed(format!("{prefix}arm: {e}"))),
            }
        });
        status
    }
    async fn wait_for_idle(&self) -> Result<()> {
        // Inherent `wait_for_idle(timeout)` takes priority in method resolution.
        self.wait_for_idle(WARMUP_IDLE_TIMEOUT).await
    }
    async fn disarm(&self) -> Result<()> {
        await_put(
            SignalBackend::<bool>::put(self.acquire.as_ref(), Some(false)),
            "disarm: Acquire=0",
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
    let io: Arc<dyn AdFileIo> = Arc::new(NdFileIo::new(Arc::new(NdFile::new(hdf_prefix))));
    let writer = AdHdfWriter::new(name.clone(), io, path_provider);
    StandardDetector::new(name, cam, writer)
}

/// Connect both halves of an [`AreaDetectorHdf`] (cam driver + HDF plugin IO).
pub async fn connect_area_detector_hdf(det: &AreaDetectorHdf, timeout: Duration) -> Result<()> {
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

    /// In-memory `AdFileIo` so the document-composition logic is testable with
    /// no live IOC (mirrors ophyd-async's IO-vs-writer split).
    struct FakeAdFileIo {
        captured: AtomicU64,
        full_name: String,
        index_tx: Arc<watch::Sender<u64>>,
        index_rx: watch::Receiver<u64>,
        configured: std::sync::Mutex<Option<(String, String, String, i64)>>,
        capturing: AtomicBool,
        flushes: AtomicU64,
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
        async fn configure(&self, d: &str, f: &str, t: &str, n: i64) -> Result<()> {
            *self.configured.lock().unwrap() = Some((d.into(), f.into(), t.into(), n));
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
        async fn frame_info(&self) -> Result<(Vec<u64>, String)> {
            Ok((vec![20, 10], "<u2".to_string()))
        }
        async fn flush(&self) -> Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn observe_num_captured(&self) -> watch::Receiver<u64> {
            self.index_rx.clone()
        }
    }

    fn writer_with(io: Arc<FakeAdFileIo>) -> AdHdfWriter {
        AdHdfWriter::new("det", io, StaticPathProvider::new("/data/scans/", "scan"))
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
        assert!(io.capturing.load(Ordering::SeqCst), "capture started");
        let dk = keys.get("det_image").expect("det_image data key");
        assert_eq!(dk.external.as_deref(), Some("STREAM:"));
        assert_eq!(dk.dtype, Dtype::Array);
        // Shape + dtype discovered from the IOC (ArraySize*/DataType RBVs).
        assert_eq!(dk.shape, vec![Some(20), Some(10)]);
        assert_eq!(
            dk.dtype_numpy,
            Some(crate::event_model::DtypeNumpy::Scalar("<u2".to_string()))
        );
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
        let w = writer_with(io.clone());
        w.open(1).await.unwrap();
        io.set_captured(1);
        let up_to = w.indices_written().await;
        assert_eq!(up_to, 1);
        let docs: Vec<StreamAsset> = w.collect_stream_docs(up_to, "desc-uid-1").collect().await;
        assert_eq!(docs.len(), 2, "resource + datum");
        let resource_uid = match &docs[0] {
            StreamAsset::Resource(r) => {
                assert_eq!(r.uri, "file:///data/scans/scan.h5");
                assert_eq!(r.mimetype, "application/x-hdf5");
                assert_eq!(r.data_key, "det_image");
                assert_eq!(
                    r.parameters.get("dataset").and_then(|v| v.as_str()),
                    Some("/entry/data/data")
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
                assert_eq!(d.seq_nums.start, 1);
                assert_eq!(d.seq_nums.stop, 2);
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
        let w = AdHdfWriter::new(
            "det",
            io.clone(),
            StaticPathProvider::new("/data/scans/", "scan"),
        )
        .with_ndattributes(vec![
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
            assert!(dk.shape.is_empty(), "attribute is scalar");
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
}
