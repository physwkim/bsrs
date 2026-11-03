//! Minimal CA-backed devices for the Lua REPL — `ca_motor` and
//! `ca_detector` factories that connect bsrs's `EpicsCaBackend`
//! to a real EPICS IOC.
//!
//! Behind the `ca` Cargo feature (on by default) so a
//! `--no-default-features` build stays free of `epics-ca-rs`. Run
//! with:
//!
//! ```sh
//! cargo run -p bsrs --features cli -- repl --script my_scan.lua
//! ```
//!
//! ## Lua surface
//!
//! ```lua
//! local m = ca_motor("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV")
//! local d = ca_detector("ph_det", "mini:ph:DetValue_RBV")
//! RE:run(scan({d}, m, -8.0, 8.0, 17))
//! -- ca_detector is monitorable: every CA update of the PV becomes an
//! -- Event in a "<name>_monitor" stream for the run's duration.
//! RE:run(bpp.monitor_during(scan({d}, m, -8.0, 8.0, 17), {d}))
//! ```
//!
//! Both factories block on connect (5 s timeout) before returning.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backends::epics_ca::EpicsCaBackend;
use crate::core::error::Result;
use crate::core::msg::{
    DynLocation, LocatableObj, MonitorableObj, MovableObj, NamedObj, ReadableObj, StoppableObj,
};
use crate::core::reading::ReadingValue;
use crate::core::status::Status;
use crate::core::subscription::Subscription;
use crate::devices::{SignalR, SignalRW};
use crate::event_model::DataKey;
use crate::host::motor_move::{
    describe_device, move_status, pv_signal, Axis, ControlRule, Halt, Limits, MotorChannels,
};
use crate::protocols_async::{AsyncSubscribable, PutCompletion};

/// CA-backed motor: setpoint (`.VAL`) + readback (`.RBV`) Signal pair.
///
/// Also a [`MonitorableObj`] over the readback: every CA update of `.RBV`
/// becomes an Event, through the readback's one shared monitor (CP-08 / K2).
pub struct CaMotor {
    name: String,
    channels: MotorChannels<EpicsCaBackend<f64>>,
}

impl CaMotor {
    /// Build + connect the channels. Blocks on bsrs's runtime; the
    /// caller must invoke this from a sync context (see
    /// `bootstrap_ca` for the recommended order).
    pub fn connect_blocking(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Arc<Self>> {
        crate::core::runtime::bsrs_runtime().block_on(Self::connect(name, val_pv, rbv_pv))
    }

    /// Async equivalent — call from inside an existing tokio runtime
    /// (e.g. `bsrs qs-manager`'s `async fn run`). Same outcome as
    /// `connect_blocking` but doesn't trigger a nested-runtime panic.
    pub async fn connect_async(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Arc<Self>> {
        Self::connect(name, val_pv, rbv_pv).await
    }

    async fn connect(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Arc<Self>> {
        let channels: MotorChannels<EpicsCaBackend<f64>> =
            MotorChannels::connect("ca_motor", name, val_pv, rbv_pv).await?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            channels,
        }))
    }

    /// ophyd `EpicsMotor.set_current_position`: redefine the user position
    /// as `pos` without moving the motor, past the limits if need be.
    pub async fn set_current_position(&self, pos: f64) -> Result<()> {
        self.channels.set_current_position("ca_motor", pos).await
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
        self.channels.readback.read_cached(Some(false)).await
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        describe_device(&self.channels.readback, &self.name).await
    }
    /// The readback is the hinted field, as in ophyd's `EpicsMotor`.
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![self.name.clone()])
    }
}

#[async_trait::async_trait]
impl MovableObj for CaMotor {
    async fn set_dyn(&self, value: f64) -> Status {
        self.channels.set("ca_motor", value).await
    }
    async fn stop_on_pause(&self, success: bool) -> Result<()> {
        StoppableObj::stop_dyn(self, success).await
    }
}

#[async_trait::async_trait]
impl LocatableObj for CaMotor {
    async fn locate_dyn(&self) -> Result<DynLocation> {
        let setpoint = self.channels.setpoint.get().await?;
        let readback = self.channels.readback.get().await?;
        Ok(DynLocation { setpoint, readback })
    }
}

#[async_trait::async_trait]
impl StoppableObj for CaMotor {
    /// ophyd-async `Motor.stop`: press `.STOP` while the motor moves, then
    /// settle the move, failed unless `success`.
    async fn stop_dyn(&self, success: bool) -> Result<()> {
        self.channels.stop(success).await
    }
}

#[async_trait::async_trait]
impl MonitorableObj for CaMotor {
    async fn subscribe_dyn(&self) -> Result<Subscription> {
        AsyncSubscribable::subscribe(&self.channels.readback).await
    }
}

/// CA-backed positioner with an explicit done signal — the bsrs
/// equivalent of ophyd's `PVPositioner`. For axes whose setpoint
/// record completes processing immediately while an external
/// controller (SNL sequencer, coordinated-motion box) moves the
/// hardware afterwards; the Kohzu DCM energy axis (`BraggEAO` +
/// `KohzuMoving`) is the canonical case. A plain [`CaMotor`] on such
/// an axis reports the move done at WRITE_NOTIFY completion and every
/// scan point reads a stale readback.
///
/// `set` puts the setpoint, then polls the done PV until it reads
/// `done_value`.
///
/// Database requirement: processing the setpoint record must drive the
/// done PV to not-done *within the same put chain* (kohzuSeq.db FLNKs
/// `BraggEAO` → `KohzuPutMoving` → `KohzuMoving`=Busy before the
/// WRITE_NOTIFY completes). That closes the stale-done race — the
/// first post-put poll can never observe the pre-move done state.
pub struct CaPositioner {
    name: String,
    setpoint: SignalRW<f64, EpicsCaBackend<f64>>,
    /// Named after the device.
    readback: SignalR<f64, EpicsCaBackend<f64>>,
    done: Arc<SignalR<f64, EpicsCaBackend<f64>>>,
    done_pv: String,
    done_value: f64,
    halt: Halt,
}

impl CaPositioner {
    /// Build + connect all three channels. Blocks; see
    /// `CaMotor::connect_blocking`.
    pub fn connect_blocking(
        name: &str,
        val_pv: &str,
        rbv_pv: &str,
        done_pv: &str,
        done_value: f64,
    ) -> Result<Arc<Self>> {
        crate::core::runtime::bsrs_runtime().block_on(Self::connect_async(
            name, val_pv, rbv_pv, done_pv, done_value,
        ))
    }

    /// Async equivalent — call from inside an existing tokio runtime.
    pub async fn connect_async(
        name: &str,
        val_pv: &str,
        rbv_pv: &str,
        done_pv: &str,
        done_value: f64,
    ) -> Result<Arc<Self>> {
        let this = Self {
            name: name.to_string(),
            setpoint: pv_signal(
                EpicsCaBackend::new(val_pv),
                val_pv,
                format!("{name}-setpoint"),
            ),
            readback: pv_signal(EpicsCaBackend::new(rbv_pv), rbv_pv, name.to_string()),
            done: Arc::new(pv_signal(
                EpicsCaBackend::new_inferred(done_pv),
                done_pv,
                format!("{name}-done"),
            )),
            done_pv: done_pv.to_string(),
            done_value,
            halt: Halt::new(),
        };
        this.setpoint.connect(Duration::from_secs(5)).await?;
        this.readback.connect(Duration::from_secs(5)).await?;
        this.done.connect(Duration::from_secs(5)).await?;
        Ok(Arc::new(this))
    }
}

impl NamedObj for CaPositioner {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "CaPositioner",
            "done_pv": self.done_pv,
            "done_value": self.done_value,
        })
    }
}

#[async_trait::async_trait]
impl ReadableObj for CaPositioner {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        self.readback.read_cached(Some(false)).await
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        describe_device(&self.readback, &self.name).await
    }
    /// The readback is the hinted field, as in ophyd's `PVPositioner`.
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![self.name.clone()])
    }
}

#[async_trait::async_trait]
impl MovableObj for CaPositioner {
    async fn set_dyn(&self, value: f64) -> Status {
        // No deadline: the done PV follows a controller whose move length
        // this device cannot know, as ophyd's `PVPositioner` waits without
        // one by default. Done values come from int-ish records
        // (bi/bo/mbbi), so exact f64 equality is the intended comparison.
        let done = self.done.clone();
        let done_value = self.done_value;
        let finish = move |put_done: PutCompletion| async move {
            put_done
                .await
                .map_err(|e| format!("ca_positioner set: {e}"))?;
            loop {
                let v = done
                    .get()
                    .await
                    .map_err(|e| format!("ca_positioner done read: {e}"))?;
                if v == done_value {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        let what = "ca_positioner set";
        let axis = Axis {
            setpoint: &self.setpoint,
            readback: &self.readback,
            limits: Limits::Control(ControlRule::Positioner),
            halt: &self.halt,
        };
        move_status(axis, value, what, finish).await
    }
    async fn stop_on_pause(&self, success: bool) -> Result<()> {
        StoppableObj::stop_dyn(self, success).await
    }
}

#[async_trait::async_trait]
impl LocatableObj for CaPositioner {
    async fn locate_dyn(&self) -> Result<DynLocation> {
        let setpoint = self.setpoint.get().await?;
        let readback = self.readback.get().await?;
        Ok(DynLocation { setpoint, readback })
    }
}

#[async_trait::async_trait]
impl StoppableObj for CaPositioner {
    /// ophyd `PositionerBase.stop`: settle the move, failed unless
    /// `success`. With no stop PV the motion carries on, as for a
    /// `PVPositioner` without `stop_signal`.
    async fn stop_dyn(&self, success: bool) -> Result<()> {
        self.halt.stop(success);
        Ok(())
    }
}

#[async_trait::async_trait]
impl MonitorableObj for CaPositioner {
    async fn subscribe_dyn(&self) -> Result<Subscription> {
        AsyncSubscribable::subscribe(&self.readback).await
    }
}

/// CA-backed scalar detector: one Signal on a `_RBV` PV, named after the
/// detector.
///
/// Also a [`MonitorableObj`]: `Msg::Monitor` / `monitor_during_wrapper`
/// stream every CA update of the PV as an Event. Subscriptions go through
/// the signal's one shared monitor (CP-08), so N monitors of the same
/// detector open one CA monitor, released when the last subscription drops
/// (K2).
pub struct CaDetector {
    name: String,
    value: SignalR<f64, EpicsCaBackend<f64>>,
    seen: AtomicI64,
}

impl CaDetector {
    /// Build + connect. Blocks; see `CaMotor::connect_blocking`.
    pub fn connect_blocking(name: &str, value_pv: &str) -> Result<Arc<Self>> {
        crate::core::runtime::bsrs_runtime().block_on(Self::connect_async(name, value_pv))
    }

    /// Async equivalent of `connect_blocking` — for callers
    /// already inside a tokio runtime (qs-manager's `async fn`).
    pub async fn connect_async(name: &str, value_pv: &str) -> Result<Arc<Self>> {
        let backend = EpicsCaBackend::new_inferred(value_pv);
        let value = pv_signal(backend, value_pv, name.to_string());
        value.connect(Duration::from_secs(5)).await?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            value,
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
        let r = self.value.read_cached(Some(false)).await?;
        self.seen.fetch_add(1, Ordering::SeqCst);
        Ok(r)
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        describe_device(&self.value, &self.name).await
    }
    /// The value is the hinted field, as in an ophyd `EpicsSignalRO`.
    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![self.name.clone()])
    }
}

#[async_trait::async_trait]
impl MonitorableObj for CaDetector {
    async fn subscribe_dyn(&self) -> Result<Subscription> {
        AsyncSubscribable::subscribe(&self.value).await
    }
}

/// Bootstrap the CA backend's global client. Must be called from a
/// sync context (no active tokio runtime); after this the cached
/// client is reused everywhere.
pub fn bootstrap_ca() {
    let _ = crate::backends::epics_ca::ca_context();
}
