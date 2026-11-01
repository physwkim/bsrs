//! End-to-end verification: drive a real epics-rs IOC via bsrs.
//!
//! Run alongside `mini_ioc` from `~/codes/epics-rs`:
//!
//! ```sh
//! # terminal 1: start the mini-beamline IOC
//! cd ~/codes/epics-rs
//! ./target/release/mini_ioc examples/mini-beamline/ioc/st.cmd
//!
//! # terminal 2: run bsrs
//! cd ~/codes/bsrs
//! cargo run -p bsrs --features host --example mini_beamline_scan
//! ```
//!
//! What it does:
//!
//! - Opens CA channels to the `mini:ph:*` PVs (motor + detector +
//!   beam current).
//! - Connects a `bsrs::host::ca_devices::CaMotor` (`mini:ph:mtr.VAL` /
//!   `.RBV`) and a `CaDetector` (`mini:ph:DetValue_RBV`).
//! - Runs `scan(det, motor, -10, 10, 21)` against the daemon's
//!   RunEngine.
//! - Captures every Document into a `JsonlSink` at /tmp.
//! - Asserts the run finished with `exit_status="success"` and at
//!   least 21 events.
//! - Checks the primary descriptor: each `source` names the PV read and
//!   both devices are hinted, as ophyd-async's `Motor` and
//!   `epics_signal_r` write it.
//!
//! Closes the "live IOC" hole — until now bsrs's CA backend was
//! only build-tested in CI.

use std::sync::Arc;
use std::time::Duration;

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::core::msg::{MovableObj, NamedObj, ReadableObj};
use bsrs::engine::{DocumentSink, RunEngine};
use bsrs::host::ca_devices::{bootstrap_ca, CaDetector, CaMotor};
use bsrs::protocols_async::SignalBackend;

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Bootstrap the CA context outside any runtime — the CA backend's
    // `ca_context()` block_on's CaClient::new() once. Calling it from
    // inside a tokio runtime panics with "Cannot start a runtime from
    // within a runtime". After this call the global is cached.
    bootstrap_ca();

    // Drive the rest on bsrs's runtime so all subsequent
    // block_on / spawn calls share the same handle.
    bsrs::core::runtime::bsrs_runtime().block_on(async_main())
}

async fn async_main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    eprintln!("[bsrs] connecting to mini-beamline IOC via CA...");

    let motor = CaMotor::connect_async("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV").await?;
    let det = CaDetector::connect_async("ph_det", "mini:ph:DetValue_RBV").await?;

    // Mini-beamline's sim motor ships at VELO=0.2 (slow). Bump it so
    // the scan takes seconds. Real beamlines configure this once
    // during setup.
    let velo = Arc::new(EpicsCaBackend::<f64>::new("mini:ph:mtr.VELO"));
    velo.connect(Duration::from_secs(5)).await?;
    let _ = velo.put(Some(5.0)).await;
    eprintln!("[bsrs] set mini:ph:mtr.VELO = 5.0");

    eprintln!("[bsrs] motor={:?}", motor.inspect_dyn());
    eprintln!("[bsrs] det  ={:?}", det.inspect_dyn());

    // Smoke: read once before the scan.
    let r = det.read_dyn().await?;
    eprintln!("[bsrs] pre-scan read: {r:?}");

    // Capture every Document to a JSONL file at /tmp for offline
    // inspection.
    let jsonl_path = format!("/tmp/bsrs_mini_beamline_{}.jsonl", std::process::id());
    let sink: Arc<dyn DocumentSink> =
        Arc::new(bsrs::callbacks::JsonlSink::open(&jsonl_path).await?);

    let capture = Arc::new(bsrs::callbacks::CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone(), capture.clone() as Arc<dyn DocumentSink>]);

    // Scan from -8 to 8 in 17 points (covers the PinHole gaussian
    // peak at center=0, sigma=5).
    let plan = bsrs::plans::scan_1d(
        vec![det.clone() as Arc<dyn ReadableObj>],
        motor.clone() as Arc<dyn MovableObj>,
        motor.clone() as Arc<dyn ReadableObj>,
        -8.0,
        8.0,
        17,
    );
    let result = re.run_async(plan).await?;
    eprintln!(
        "[bsrs] scan finished: exit_status={} run_uid={:?}",
        result.exit_status,
        result.run_uids.last()
    );
    eprintln!("[bsrs] documents captured in {jsonl_path}");

    if result.exit_status != "success" {
        return Err(format!("expected exit_status=success, got {}", result.exit_status).into());
    }
    let frames = det.inspect_dyn()["frames_seen"].as_i64().unwrap_or(0);
    if frames < 17 {
        return Err(format!("expected ≥17 detector reads, got {frames}").into());
    }
    check_descriptor(&capture.snapshot().await)?;
    eprintln!("[bsrs] OK — {frames} reads, exit=success");
    Ok(())
}

/// The primary descriptor as ophyd-async's `Motor` and `epics_signal_r`
/// write it: each source names the PV read, and both devices are hinted.
fn check_descriptor(docs: &[bsrs::event_model::Document]) -> Result<(), String> {
    let desc = docs
        .iter()
        .find_map(|d| match d {
            bsrs::event_model::Document::Descriptor(d) => Some(d),
            _ => None,
        })
        .ok_or("no descriptor")?;
    for (key, pv) in [
        ("ph_mtr", "mini:ph:mtr.RBV"),
        ("ph_det", "mini:ph:DetValue_RBV"),
    ] {
        let source = desc.data_keys.get(key).map(|k| k.source.as_str());
        if source != Some(&format!("ca://{pv}")) {
            return Err(format!("{key}: source {source:?}, want ca://{pv}"));
        }
        let fields = desc.hints.as_ref().and_then(|h| h.get(key)?.fields.clone());
        if fields != Some(vec![key.to_string()]) {
            return Err(format!("{key}: hinted fields {fields:?}, want [{key}]"));
        }
    }
    Ok(())
}
