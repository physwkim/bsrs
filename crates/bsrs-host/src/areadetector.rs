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

use bsrs_backend_epics_ca::EpicsCaBackend;
use bsrs_core::error::{BsrsError, Result};
use bsrs_core::status::StatusError;
use bsrs_protocols_async::SignalBackend;
use std::sync::Arc;
use std::time::Duration;

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
            prefix,
        }
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
            plugin,
        }
    }

    /// Connect plugin base + every file-specific channel.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        let p = self.plugin.connect(timeout);
        let (a, b, c, d, e, f, g) = tokio::join!(
            p,
            SignalBackend::<String>::connect(self.file_path.as_ref(), timeout),
            SignalBackend::<String>::connect(self.file_name.as_ref(), timeout),
            SignalBackend::<String>::connect(self.file_template.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.auto_increment.as_ref(), timeout),
            SignalBackend::<i64>::connect(self.file_write_mode.as_ref(), timeout),
            SignalBackend::<bool>::connect(self.capture.as_ref(), timeout),
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

/// Route `file` to consume frames from `source_port` and enable its
/// callbacks. Every sibling in `siblings` is disabled (i.e. its
/// `EnableCallbacks` set to false). Useful when an IOC carries
/// multiple save plugins (HDF5 / TIFF / JPEG) but only one should be
/// active per scan.
pub async fn select_save_plugin(
    file: &NdFile,
    source_port: &str,
    siblings: &[&NdFile],
) -> Result<()> {
    file.plugin.set_source_port(source_port).await?;
    file.plugin.set_enabled(true).await?;
    for s in siblings {
        s.plugin.set_enabled(false).await?;
    }
    Ok(())
}

/// Enable the first `n` ROIs in `rois`, disable the rest. Out-of-range
/// `n` is clamped to `[0, rois.len()]` and reported as an error so
/// the caller can react if the index was a typo.
pub async fn num_rois(rois: &[&NdRoi], n: usize) -> Result<()> {
    if n > rois.len() {
        return Err(BsrsError::Status(StatusError::Failed(format!(
            "num_rois: requested {n} but only {} ROIs available",
            rois.len()
        ))));
    }
    for (i, roi) in rois.iter().enumerate() {
        let on = i < n;
        roi.plugin.set_enabled(on).await?;
    }
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
    // Live-IOC smoke tests against `epics-rs/examples/sim-detector`.
    // Marked `#[ignore]` so they only run with `--ignored`.
    //
    // Setup:
    //   cd ~/codes/epics-rs/examples/sim-detector
    //   cargo run --bin sim_ioc --features ioc -- ioc/st.cmd
    //
    // Then in another shell:
    //   cargo test -p bsrs-host --features ca \
    //       areadetector::tests:: -- --ignored --nocapture
    //
    // Override the default `SIM1:` prefix with `BSRS_AD_PREFIX=<your:>`.
    // -------------------------------------------------------------

    fn ad_prefix() -> String {
        std::env::var("BSRS_AD_PREFIX").unwrap_or_else(|_| "SIM1:".to_string())
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

    /// Smoke: `select_save_plugin(hdf1, "SIM1", [jpeg1, magick1,
    /// nexus1])` must (a) set `HDF1:NDArrayPort = "SIM1"`,
    /// (b) enable `HDF1:EnableCallbacks`, (c) disable
    /// `EnableCallbacks` on every sibling. Plus long-string
    /// round-trip on `HDF1:FilePath` to exercise the
    /// `CaStringKind::Long` get/put path.
    ///
    /// Side effect: leaves HDF1 enabled and JPEG1/Magick1/Nexus1
    /// disabled after the test. Re-enable manually via PYDM/MEDM if
    /// you need them on for another run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn ad_select_save_plugin_against_sim_detector() {
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

        // (1) Long-string round-trip on FilePath. Use a path that
        // exists on every Unix box so the IOC accepts it.
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

        // (2) Route HDF1 to consume from "SIM1" (the cam's asyn
        // port — see sim-detector's st.cmd PORT=SIM1), and disable
        // the three sibling file plugins.
        let siblings = [&jpeg1, &magick1, &nexus1];
        select_save_plugin(&hdf1, "SIM1", &siblings)
            .await
            .expect("select_save_plugin");

        let hdf1_port = SignalBackend::<String>::get_value(hdf1.plugin.nd_array_port.as_ref())
            .await
            .expect("read HDF1.NDArrayPort");
        let hdf1_en = SignalBackend::<bool>::get_value(hdf1.plugin.enable_callbacks.as_ref())
            .await
            .expect("read HDF1.EnableCallbacks");
        let jpeg1_en = SignalBackend::<bool>::get_value(jpeg1.plugin.enable_callbacks.as_ref())
            .await
            .expect("read JPEG1.EnableCallbacks");
        let magick1_en = SignalBackend::<bool>::get_value(magick1.plugin.enable_callbacks.as_ref())
            .await
            .expect("read Magick1.EnableCallbacks");
        let nexus1_en = SignalBackend::<bool>::get_value(nexus1.plugin.enable_callbacks.as_ref())
            .await
            .expect("read Nexus1.EnableCallbacks");

        eprintln!(
            "select_save_plugin: HDF1.port={hdf1_port:?}, \
             enables HDF1={hdf1_en} JPEG1={jpeg1_en} Magick1={magick1_en} Nexus1={nexus1_en}"
        );
        // NDArrayPort comes back via DBR_STRING — short form, NUL-padded
        // to 40 bytes, but our decoder strips at NUL so we just compare
        // directly.
        assert_eq!(hdf1_port, "SIM1", "HDF1.NDArrayPort not routed to SIM1");
        assert!(hdf1_en, "HDF1 was not enabled");
        assert!(!jpeg1_en, "JPEG1 still enabled after select_save_plugin");
        assert!(
            !magick1_en,
            "Magick1 still enabled after select_save_plugin"
        );
        assert!(!nexus1_en, "Nexus1 still enabled after select_save_plugin");
    }
}
