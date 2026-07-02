//! Minimal CA-backed devices for the Lua REPL — `ca_motor` and
//! `ca_detector` factories that connect bsrs's `EpicsCaBackend`
//! to a real EPICS IOC.
//!
//! Behind the `ca` Cargo feature so the default bsrs-cli build
//! stays free of `epics-ca-rs`. Build with:
//!
//! ```sh
//! cargo run -p bsrs-cli --features ca -- repl --script my_scan.lua
//! ```
//!
//! ## Lua surface
//!
//! ```lua
//! local m = ca_motor("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV")
//! local d = ca_detector("ph_det", "mini:ph:DetValue_RBV")
//! RE:run(scan({d}, m, -8.0, 8.0, 17))
//! ```
//!
//! Both factories block on connect (5 s timeout) before returning.

#![cfg(feature = "ca")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backends::epics_ca::EpicsCaBackend;
use crate::core::error::Result;
use crate::core::msg::{
    DynLocation, LocatableObj, MovableObj, NamedObj, ReadableObj, StoppableObj,
};
use crate::core::reading::ReadingValue;
use crate::core::status::Status;
use crate::event_model::{DataKey, Dtype};
// `SignalBackend` is the trait that provides connect/put/get on the
// CA backend; pulled in via the `bsrs-protocols-async` dep that
// the `ca` feature toggles on.
use crate::protocols_async::SignalBackend;

/// CA-backed motor: setpoint (`.VAL`) + readback (`.RBV`) Signal pair.
pub struct CaMotor {
    name: String,
    setpoint: Arc<EpicsCaBackend<f64>>,
    readback: Arc<EpicsCaBackend<f64>>,
}

impl CaMotor {
    /// Build + connect both channels. Blocks on bsrs's runtime; the
    /// caller must invoke this from a sync context (see
    /// `bootstrap_ca` for the recommended order).
    pub fn connect_blocking(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Arc<Self>> {
        let sp = Arc::new(EpicsCaBackend::<f64>::new(val_pv));
        let rb = Arc::new(EpicsCaBackend::<f64>::new(rbv_pv));
        let sp_for_async = sp.clone();
        let rb_for_async = rb.clone();
        crate::core::runtime::bsrs_runtime().block_on(async move {
            sp_for_async.connect(Duration::from_secs(5)).await?;
            rb_for_async.connect(Duration::from_secs(5)).await
        })?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            setpoint: sp,
            readback: rb,
        }))
    }

    /// Async equivalent — call from inside an existing tokio runtime
    /// (e.g. `bsrs qs-manager`'s `async fn run`). Same outcome as
    /// `connect_blocking` but doesn't trigger a nested-runtime panic.
    pub async fn connect_async(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Arc<Self>> {
        let sp = Arc::new(EpicsCaBackend::<f64>::new(val_pv));
        let rb = Arc::new(EpicsCaBackend::<f64>::new(rbv_pv));
        sp.connect(Duration::from_secs(5)).await?;
        rb.connect(Duration::from_secs(5)).await?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            setpoint: sp,
            readback: rb,
        }))
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
                choices: None,
            },
        );
        Ok(out)
    }
}

#[async_trait::async_trait]
impl MovableObj for CaMotor {
    async fn set_dyn(&self, value: f64) -> Status {
        // The backend `put` awaits the motor settle (WRITE_NOTIFY). The
        // 30 s move-timeout lives here, at the device layer (CP-11), not
        // in the backend's channel-ensure step.
        match tokio::time::timeout(Duration::from_secs(30), self.setpoint.put(Some(value))).await {
            Ok(Ok(())) => Status::done(),
            Ok(Err(e)) => Status::fail(crate::core::status::StatusError::Failed(format!(
                "ca_motor set: {e}"
            ))),
            Err(_) => Status::fail(crate::core::status::StatusError::Failed(
                "ca_motor set: timed out after 30s".into(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl LocatableObj for CaMotor {
    async fn locate_dyn(&self) -> Result<DynLocation> {
        let setpoint = self.setpoint.get_value().await?;
        let readback = self.readback.get_value().await?;
        Ok(DynLocation { setpoint, readback })
    }
}

#[async_trait::async_trait]
impl StoppableObj for CaMotor {
    async fn stop_dyn(&self, _success: bool) -> Result<()> {
        Ok(())
    }
}

/// CA-backed scalar detector: one Signal on a `_RBV` PV.
pub struct CaDetector {
    name: String,
    value: Arc<EpicsCaBackend<f64>>,
    seen: AtomicI64,
}

impl CaDetector {
    /// Build + connect. Blocks; see `CaMotor::connect_blocking`.
    pub fn connect_blocking(name: &str, value_pv: &str) -> Result<Arc<Self>> {
        let v = Arc::new(EpicsCaBackend::<f64>::new(value_pv));
        let v_for_async = v.clone();
        crate::core::runtime::bsrs_runtime()
            .block_on(async move { v_for_async.connect(Duration::from_secs(5)).await })?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            value: v,
            seen: AtomicI64::new(0),
        }))
    }

    /// Async equivalent of `connect_blocking` — for callers
    /// already inside a tokio runtime (qs-manager's `async fn`).
    pub async fn connect_async(name: &str, value_pv: &str) -> Result<Arc<Self>> {
        let v = Arc::new(EpicsCaBackend::<f64>::new(value_pv));
        v.connect(Duration::from_secs(5)).await?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            value: v,
            seen: AtomicI64::new(0),
        }))
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
                choices: None,
            },
        );
        Ok(out)
    }
}

/// Bootstrap the CA backend's global client. Must be called from a
/// sync context (no active tokio runtime); after this the cached
/// client is reused everywhere.
pub fn bootstrap_ca() {
    let _ = crate::backends::epics_ca::ca_context();
}
