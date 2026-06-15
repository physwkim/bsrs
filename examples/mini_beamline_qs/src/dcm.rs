//! `Dcm` — composite device for the mini-beamline Kohzu DCM.
//!
//! Children:
//! - `energy` — Bragg-energy setpoint/readback pair (`mini:BraggEAO` /
//!   `mini:BraggERdbkAO`), driven through the Kohzu controller's
//!   energy-↔-angle state machine.
//! - `theta_rbv` — read-only Bragg-angle readback
//!   (`mini:BraggThetaRdbkAO`).
//!
//! Surface:
//! - `NamedObj` — `dcm` global in the daemon Lua state.
//! - `ReadableObj` — `dcm:read()` returns both `dcm_energy` (keV) and
//!   `dcm_theta_rbv` (deg) in one Reading.
//! - `MovableObj` / `LocatableObj` — `dcm:set(e)` / `dcm:locate()`
//!   drive the energy axis (theta follows via the Kohzu controller).
//!   Lets `scan({det}, dcm, 6, 12, 7)` work in plain bluesky style.
//! - `#[lua_methods]` extras:
//!   - `dcm:move_energy_keV(e)` — explicit alias for `set` + wait.
//!   - `dcm:theta_now()` — read theta readback only.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bsrs_core::error::Result;
use bsrs_core::msg::{DynLocation, LocatableObj, MovableObj, NamedObj, ReadableObj};
use bsrs_core::reading::ReadingValue;
use bsrs_core::status::Status;
use bsrs_event_model::DataKey;
use bsrs_host::ca_devices::{CaDetector, CaMotor};

/// Composite DCM: energy axis + theta readback.
pub struct Dcm {
    name: String,
    energy: Arc<CaMotor>,
    theta_rbv: Arc<CaDetector>,
}

impl Dcm {
    /// Connect both children. CA bootstrap must have run already.
    pub async fn connect(
        name: &str,
        energy_val_pv: &str,
        energy_rbv_pv: &str,
        theta_rbv_pv: &str,
    ) -> Result<Arc<Self>> {
        let energy = CaMotor::connect_async("dcm_energy", energy_val_pv, energy_rbv_pv).await?;
        let theta_rbv = CaDetector::connect_async("dcm_theta_rbv", theta_rbv_pv).await?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            energy,
            theta_rbv,
        }))
    }
}

impl NamedObj for Dcm {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "Dcm",
            "children": ["dcm_energy", "dcm_theta_rbv"],
        })
    }
}

#[async_trait]
impl ReadableObj for Dcm {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        let (e, t) = tokio::join!(self.energy.read_dyn(), self.theta_rbv.read_dyn());
        let mut out = e?;
        out.extend(t?);
        Ok(out)
    }

    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        let (e, t) = tokio::join!(self.energy.describe_dyn(), self.theta_rbv.describe_dyn());
        let mut out = e?;
        out.extend(t?);
        Ok(out)
    }
}

#[async_trait]
impl MovableObj for Dcm {
    async fn set_dyn(&self, value: f64) -> Status {
        self.energy.set_dyn(value).await
    }
}

#[async_trait]
impl LocatableObj for Dcm {
    async fn locate_dyn(&self) -> Result<DynLocation> {
        self.energy.locate_dyn().await
    }
}

#[bsrs_derive::lua_methods]
impl Dcm {
    /// Move energy to `keV` and wait for completion. Equivalent to
    /// `dcm:set(keV):wait()` but spelled out.
    #[lua_method]
    #[allow(non_snake_case)]
    pub async fn move_energy_keV(&self, kev: f64) -> Result<(), String> {
        let status = self.energy.set_dyn(kev).await;
        status.await.map_err(|e| format!("{e:?}"))
    }

    /// Read only the theta readback. Returns the angle in degrees.
    #[lua_method]
    pub async fn theta_now(&self) -> Result<f64, String> {
        let r = self.theta_rbv.read_dyn().await.map_err(|e| e.to_string())?;
        let (_, v) = r
            .into_iter()
            .next()
            .ok_or_else(|| "theta_rbv: no reading".to_string())?;
        v.value
            .as_f64()
            .ok_or_else(|| format!("theta_rbv: expected number, got {:?}", v.value))
    }
}
