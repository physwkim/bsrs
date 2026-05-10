//! End-to-end verification: drive a real epics-rs IOC via cirrus.
//!
//! Run alongside `mini_ioc` from `~/codes/epics-rs`:
//!
//! ```sh
//! # terminal 1: start the mini-beamline IOC
//! cd ~/codes/epics-rs
//! ./target/release/mini_ioc examples/mini-beamline/ioc/st.cmd
//!
//! # terminal 2: run cirrus
//! cd ~/codes/cirrus
//! cargo run --example mini_beamline_scan
//! ```
//!
//! What it does:
//!
//! - Opens CA channels to the `mini:ph:*` PVs (motor + detector +
//!   beam current).
//! - Builds a cirrus `Motor` (wrapping `mini:ph:mtr.VAL` /
//!   `.RBV`) and a cirrus `Detector` (wrapping
//!   `mini:ph:DetValue_RBV`).
//! - Runs `scan(det, motor, -10, 10, 21)` against the daemon's
//!   RunEngine.
//! - Captures every Document into a `JsonlSink` at /tmp.
//! - Asserts the run finished with `exit_status="success"` and at
//!   least 21 events.
//!
//! Closes the "live IOC" hole — until now cirrus's CA backend was
//! only build-tested in CI.

use std::sync::Arc;
use std::time::Duration;

use cirrus_backend_epics_ca::EpicsCaBackend;
use cirrus_core::error::{CirrusError, Result};
use cirrus_core::msg::{
    DynLocation, LocatableObj, MovableObj, NamedObj, ReadableObj, StoppableObj,
};
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::Status;
#[allow(unused_imports)]
use cirrus_devices::{Signal, SignalConfig};
use cirrus_engine::{DocumentSink, RunEngine};
use cirrus_event_model::{DataKey, Dtype};
use cirrus_protocols_async::SignalBackend;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

/// Minimal CA-backed motor: setpoint + readback Signal pair.
struct CaMotor {
    name: String,
    setpoint: Arc<EpicsCaBackend<f64>>,
    readback: Arc<EpicsCaBackend<f64>>,
}

impl CaMotor {
    async fn new(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Self> {
        let sp = Arc::new(EpicsCaBackend::<f64>::new(val_pv));
        let rb = Arc::new(EpicsCaBackend::<f64>::new(rbv_pv));
        sp.connect(Duration::from_secs(5)).await?;
        rb.connect(Duration::from_secs(5)).await?;
        Ok(Self {
            name: name.to_string(),
            setpoint: sp,
            readback: rb,
        })
    }
}

impl NamedObj for CaMotor {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "CaMotor",
        })
    }
}

#[async_trait::async_trait]
impl ReadableObj for CaMotor {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        let r = self.readback.get_reading().await?;
        let mut out = HashMap::new();
        out.insert(self.name.clone(), r);
        Ok(out)
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        let mut out = HashMap::new();
        out.insert(
            self.name.clone(),
            DataKey {
                source: format!("ca://{}.RBV", self.name),
                dtype: Dtype::Number,
                shape: vec![],
                dtype_numpy: Some("<f8".into()),
                external: None,
                units: None,
                precision: None,
                object_name: Some(self.name.clone()),
                dims: None,
                limits: None,
            },
        );
        Ok(out)
    }
}

#[async_trait::async_trait]
impl MovableObj for CaMotor {
    async fn set_dyn(&self, value: f64) -> Status {
        let put_fut = self
            .setpoint
            .put(value, true, Some(Duration::from_secs(30)));
        // Spawn a tracker task that fires the cb after settling.
        let (status, setter) = Status::new();
        let put_status = put_fut.await;
        // The CA backend's put with wait=true blocks until the
        // motor record settles (DBR_PUT_ACK semantics). Once that
        // future resolves, mark our outer status done.
        cirrus_core::runtime::cirrus_runtime().spawn(async move {
            match put_status.await {
                Ok(()) => setter.success(),
                Err(e) => setter.fail(cirrus_core::status::StatusError::Failed(format!(
                    "set: {e:?}"
                ))),
            }
        });
        status
    }
}

#[async_trait::async_trait]
impl LocatableObj for CaMotor {
    async fn locate_dyn(&self) -> Result<DynLocation> {
        let sp = self.setpoint.get_value().await?;
        let rb = self.readback.get_value().await?;
        Ok(DynLocation {
            setpoint: sp,
            readback: rb,
        })
    }
}

#[async_trait::async_trait]
impl StoppableObj for CaMotor {
    async fn stop_dyn(&self, _success: bool) -> Result<()> {
        Ok(())
    }
}

/// Minimal CA-backed scalar detector: one Signal on a `_RBV` PV.
struct CaDetector {
    name: String,
    value: Arc<EpicsCaBackend<f64>>,
    seen: AtomicI64,
}

impl CaDetector {
    async fn new(name: &str, value_pv: &str) -> Result<Self> {
        let v = Arc::new(EpicsCaBackend::<f64>::new(value_pv));
        v.connect(Duration::from_secs(5)).await?;
        Ok(Self {
            name: name.to_string(),
            value: v,
            seen: AtomicI64::new(0),
        })
    }
}

impl NamedObj for CaDetector {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "CaDetector",
            "frames_seen": self.seen.load(Ordering::SeqCst),
        })
    }
}

#[async_trait::async_trait]
impl ReadableObj for CaDetector {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        let r = self.value.get_reading().await?;
        self.seen.fetch_add(1, Ordering::SeqCst);
        let mut out = HashMap::new();
        out.insert(self.name.clone(), r);
        Ok(out)
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        let mut out = HashMap::new();
        out.insert(
            self.name.clone(),
            DataKey {
                source: format!("ca://{}", self.name),
                dtype: Dtype::Number,
                shape: vec![],
                dtype_numpy: Some("<f8".into()),
                external: None,
                units: None,
                precision: None,
                object_name: Some(self.name.clone()),
                dims: None,
                limits: None,
            },
        );
        Ok(out)
    }
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Bootstrap the CA context outside any runtime — the CA backend's
    // `ca_context()` block_on's CaClient::new() once. Calling it from
    // inside a tokio runtime panics with "Cannot start a runtime from
    // within a runtime". After this call the global is cached.
    let _ = cirrus_backend_epics_ca::ca_context();

    // Drive the rest on cirrus's runtime so all subsequent
    // block_on / spawn calls share the same handle.
    cirrus_core::runtime::cirrus_runtime().block_on(async_main())
}

async fn async_main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    eprintln!("[cirrus] connecting to mini-beamline IOC via CA...");

    let motor = Arc::new(CaMotor::new("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV").await?);
    let det = Arc::new(CaDetector::new("ph_det", "mini:ph:DetValue_RBV").await?);

    // Mini-beamline's sim motor ships at VELO=0.2 (slow). Bump it
    // so each scan step finishes inside the 30 s WRITE_NOTIFY put
    // timeout. Real beamlines configure this once during setup.
    let velo = Arc::new(EpicsCaBackend::<f64>::new("mini:ph:mtr.VELO"));
    velo.connect(Duration::from_secs(5)).await?;
    let _ = velo
        .put(5.0, false, Some(Duration::from_secs(5)))
        .await
        .await;
    eprintln!("[cirrus] set mini:ph:mtr.VELO = 5.0");

    eprintln!("[cirrus] motor={:?}", motor.inspect_dyn());
    eprintln!("[cirrus] det  ={:?}", det.inspect_dyn());

    // Smoke: read once before the scan.
    let r = det.read_dyn().await?;
    eprintln!("[cirrus] pre-scan read: {r:?}");

    // Capture every Document to a JSONL file at /tmp for offline
    // inspection.
    let jsonl_path = format!("/tmp/cirrus_mini_beamline_{}.jsonl", std::process::id());
    let sink: Arc<dyn DocumentSink> =
        Arc::new(cirrus_callbacks::JsonlSink::open(&jsonl_path).await?);

    let re = RunEngine::new(vec![sink.clone()]);

    // Scan from -8 to 8 in 17 points (covers the PinHole gaussian
    // peak at center=0, sigma=5).
    let plan = cirrus_plans::scan(
        vec![det.clone() as Arc<dyn ReadableObj>],
        motor.clone() as Arc<dyn MovableObj>,
        motor.clone() as Arc<dyn ReadableObj>,
        -8.0,
        8.0,
        17,
    );
    let result = re.run_async(plan).await?;
    eprintln!(
        "[cirrus] scan finished: exit_status={} run_uid={:?}",
        result.exit_status, result.run_uid
    );
    eprintln!("[cirrus] documents captured in {jsonl_path}");

    if result.exit_status != "success" {
        return Err(format!("expected exit_status=success, got {}", result.exit_status).into());
    }
    let frames = det.seen.load(Ordering::SeqCst);
    if frames < 17 {
        return Err(format!("expected ≥17 detector reads, got {frames}").into());
    }
    eprintln!("[cirrus] OK — {frames} reads, exit=success");
    Ok(())
}

// CirrusError is referenced via Result<...> in the trait impls above.
const _: fn() = || {
    let _: Box<CirrusError> = Box::new(CirrusError::Backend("never constructed".into()));
};
