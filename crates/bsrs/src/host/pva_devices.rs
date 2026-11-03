//! Minimal PVA-backed devices for the Lua REPL — `pva_motor` and
//! `pva_detector` factories that connect bsrs's `EpicsPvaBackend`
//! to a real PV Access IOC.
//!
//! Behind the `pva` Cargo feature (on by default) so a
//! `--no-default-features` build stays free of `epics-pva-rs`. Run
//! with:
//!
//! ```sh
//! cargo run -p bsrs --features cli -- repl --script my_scan.lua
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backends::epics_pva::EpicsPvaBackend;
use crate::core::error::Result;
use crate::core::msg::{
    DynLocation, LocatableObj, MonitorableObj, MovableObj, NamedObj, ReadableObj, StoppableObj,
};
use crate::core::reading::ReadingValue;
use crate::core::status::Status;
use crate::core::subscription::Subscription;
use crate::devices::SignalCache;
use crate::event_model::DataKey;
use crate::host::motor_move::{describe_pv, MotorChannels};
use crate::protocols_async::SignalBackend;

/// PVA-backed motor: setpoint + readback Signal pair.
///
/// Also a [`MonitorableObj`] over the readback: every PVA update becomes
/// an Event, through one shared [`SignalCache`] (CP-08 / K2).
pub struct PvaMotor {
    name: String,
    channels: MotorChannels<EpicsPvaBackend<f64>>,
    rbv_pv: String,
}

impl PvaMotor {
    /// Build + connect the channels. Blocks on bsrs's runtime.
    pub fn connect_blocking(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Arc<Self>> {
        let connect = MotorChannels::connect("pva_motor", name, val_pv, rbv_pv);
        let channels: MotorChannels<EpicsPvaBackend<f64>> =
            crate::core::runtime::bsrs_runtime().block_on(connect)?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            channels,
            rbv_pv: rbv_pv.to_string(),
        }))
    }

    /// ophyd `EpicsMotor.set_current_position`: redefine the user position
    /// as `pos` without moving the motor, past the limits if need be.
    pub async fn set_current_position(&self, pos: f64) -> Result<()> {
        self.channels
            .set_current_position("pva_motor", &self.name, pos)
            .await
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
        let r = self.channels.readback.backend.get_reading().await?;
        let mut out = HashMap::new();
        out.insert(self.name.clone(), r);
        Ok(out)
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        Ok(HashMap::from([(
            self.name.clone(),
            describe_pv(
                self.channels.readback.backend.as_ref(),
                &self.rbv_pv,
                &self.name,
            )
            .await?,
        )]))
    }
    /// The readback is the hinted field, as in ophyd's `EpicsMotor`.
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![self.name.clone()])
    }
}

#[async_trait::async_trait]
impl MovableObj for PvaMotor {
    async fn set_dyn(&self, value: f64) -> Status {
        self.channels.set("pva_motor", &self.name, value).await
    }
    async fn stop_on_pause(&self, success: bool) -> Result<()> {
        StoppableObj::stop_dyn(self, success).await
    }
}

#[async_trait::async_trait]
impl LocatableObj for PvaMotor {
    async fn locate_dyn(&self) -> Result<DynLocation> {
        let setpoint = self.channels.setpoint.get_value().await?;
        let readback = self.channels.readback.backend.get_value().await?;
        Ok(DynLocation { setpoint, readback })
    }
}

#[async_trait::async_trait]
impl StoppableObj for PvaMotor {
    /// ophyd-async `Motor.stop`: press `.STOP` while the motor moves, then
    /// settle the move, failed unless `success`.
    async fn stop_dyn(&self, success: bool) -> Result<()> {
        self.channels.stop(success).await
    }
}

#[async_trait::async_trait]
impl MonitorableObj for PvaMotor {
    async fn subscribe_dyn(&self) -> Result<Subscription> {
        let (rx, token) = self.channels.readback.cache.add_listener();
        Ok(Subscription::new(rx, token, self.name.clone()))
    }
}

/// PVA-backed scalar detector: one Signal on a `_RBV` PV.
///
/// Also a [`MonitorableObj`]: every PVA update of the PV becomes an Event,
/// through one shared [`SignalCache`] (CP-08 / K2).
pub struct PvaDetector {
    name: String,
    value: Arc<EpicsPvaBackend<f64>>,
    value_pv: String,
    cache: Arc<SignalCache<f64, EpicsPvaBackend<f64>>>,
    seen: AtomicI64,
}

impl PvaDetector {
    /// Build + connect.
    pub fn connect_blocking(name: &str, value_pv: &str) -> Result<Arc<Self>> {
        let v = Arc::new(EpicsPvaBackend::<f64>::new_inferred(value_pv));
        let v_for_async = v.clone();
        crate::core::runtime::bsrs_runtime()
            .block_on(async move { v_for_async.connect(Duration::from_secs(5)).await })?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            cache: SignalCache::new(v.clone(), value_pv),
            value: v,
            value_pv: value_pv.to_string(),
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
        Ok(HashMap::from([(
            self.name.clone(),
            describe_pv(self.value.as_ref(), &self.value_pv, &self.name).await?,
        )]))
    }
    /// The value is the hinted field, as in an ophyd `EpicsSignalRO`.
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![self.name.clone()])
    }
}

#[async_trait::async_trait]
impl MonitorableObj for PvaDetector {
    async fn subscribe_dyn(&self) -> Result<Subscription> {
        let (rx, token) = self.cache.add_listener();
        Ok(Subscription::new(rx, token, self.name.clone()))
    }
}
