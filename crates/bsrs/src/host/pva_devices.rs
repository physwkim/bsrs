//! Minimal PVA-backed devices for the Lua REPL — `pva_motor` and
//! `pva_detector` factories that connect bsrs's `EpicsPvaBackend`
//! to a real PV Access IOC.
//!
//! Behind the `pva` Cargo feature so the default bsrs-cli build
//! can opt into PVA without dragging in `epics-pva-rs`. Build with:
//!
//! ```sh
//! cargo run -p bsrs-cli -- repl --script my_scan.lua
//! ```
//!
//! ## Lua surface
//!
//! ```lua
//! local m = pva_motor("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV")
//! local d = pva_detector("ph_det", "mini:ph:DetValue_RBV")
//! RE:run(scan({d}, m, -8.0, 8.0, 17))
//! ```
//!
//! Both factories block on connect (5 s timeout) before returning.
//! Unlike `ca_devices`, no separate bootstrap call is needed —
//! `PvaClient::new()` is fully synchronous.

#![cfg(feature = "pva")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backends::epics_pva::EpicsPvaBackend;
use crate::core::error::Result;
use crate::core::msg::{
    DynLocation, LocatableObj, MovableObj, NamedObj, ReadableObj, StoppableObj,
};
use crate::core::reading::ReadingValue;
use crate::core::status::Status;
use crate::event_model::{DataKey, Dtype};
use crate::protocols_async::SignalBackend;

/// PVA-backed motor: setpoint + readback Signal pair.
pub struct PvaMotor {
    name: String,
    setpoint: Arc<EpicsPvaBackend<f64>>,
    readback: Arc<EpicsPvaBackend<f64>>,
}

impl PvaMotor {
    /// Build + connect both channels. Blocks on bsrs's runtime.
    pub fn connect_blocking(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Arc<Self>> {
        let sp = Arc::new(EpicsPvaBackend::<f64>::new(val_pv));
        let rb = Arc::new(EpicsPvaBackend::<f64>::new(rbv_pv));
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
}

impl NamedObj for PvaMotor {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "PvaMotor",
        })
    }
}

#[async_trait::async_trait]
impl ReadableObj for PvaMotor {
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
                source: format!("pva://{}.RBV", self.name),
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
impl MovableObj for PvaMotor {
    async fn set_dyn(&self, value: f64) -> Status {
        // The backend `put` awaits the put completion. The 30 s
        // move-timeout lives here, at the device layer (CP-11).
        match tokio::time::timeout(Duration::from_secs(30), self.setpoint.put(Some(value))).await {
            Ok(Ok(())) => Status::done(),
            Ok(Err(e)) => Status::fail(crate::core::status::StatusError::Failed(format!(
                "pva_motor set: {e}"
            ))),
            Err(_) => Status::fail(crate::core::status::StatusError::Failed(
                "pva_motor set: timed out after 30s".into(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl LocatableObj for PvaMotor {
    async fn locate_dyn(&self) -> Result<DynLocation> {
        let setpoint = self.setpoint.get_value().await?;
        let readback = self.readback.get_value().await?;
        Ok(DynLocation { setpoint, readback })
    }
}

#[async_trait::async_trait]
impl StoppableObj for PvaMotor {
    async fn stop_dyn(&self, _success: bool) -> Result<()> {
        Ok(())
    }
}

/// PVA-backed scalar detector: one Signal on a `_RBV` PV.
pub struct PvaDetector {
    name: String,
    value: Arc<EpicsPvaBackend<f64>>,
    seen: AtomicI64,
}

impl PvaDetector {
    /// Build + connect.
    pub fn connect_blocking(name: &str, value_pv: &str) -> Result<Arc<Self>> {
        let v = Arc::new(EpicsPvaBackend::<f64>::new(value_pv));
        let v_for_async = v.clone();
        crate::core::runtime::bsrs_runtime()
            .block_on(async move { v_for_async.connect(Duration::from_secs(5)).await })?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            value: v,
            seen: AtomicI64::new(0),
        }))
    }
}

impl NamedObj for PvaDetector {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "PvaDetector",
            "frames_seen": self.seen.load(Ordering::SeqCst),
        })
    }
}

#[async_trait::async_trait]
impl ReadableObj for PvaDetector {
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
                source: format!("pva://{}", self.name),
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
