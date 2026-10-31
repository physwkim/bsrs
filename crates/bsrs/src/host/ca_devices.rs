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
use crate::core::status::{Status, StatusError};
use crate::core::subscription::Subscription;
use crate::devices::SignalCache;
use crate::event_model::DataKey;
// `SignalBackend` is the trait that provides connect/put/get on the
// CA backend; pulled in via the `bsrs-protocols-async` dep that
// the `ca` feature toggles on.
use crate::protocols_async::{PutCompletion, SignalBackend};

/// CA-backed motor: setpoint (`.VAL`) + readback (`.RBV`) Signal pair.
///
/// Also a [`MonitorableObj`] over the readback: every CA update of `.RBV`
/// becomes an Event, through one shared [`SignalCache`] (CP-08 / K2).
pub struct CaMotor {
    name: String,
    setpoint: Arc<EpicsCaBackend<f64>>,
    readback: Arc<EpicsCaBackend<f64>>,
    rbv_pv: String,
    cache: Arc<SignalCache<f64, EpicsCaBackend<f64>>>,
    /// `None` when the setpoint's record has no `.VELO`/`.ACCL` (not a
    /// motor record); its moves then have no deadline.
    timing: Option<MoveTiming>,
}

/// The motor record's `.VELO` and `.ACCL`, read at each move for its
/// deadline.
struct MoveTiming {
    velo: Arc<EpicsCaBackend<f64>>,
    accl: Arc<EpicsCaBackend<f64>>,
}

/// ophyd-async `DEFAULT_TIMEOUT`, the fixed term of
/// `Motor.calculate_timeout`.
const MOVE_TIMEOUT_MARGIN_SECS: f64 = 10.0;

/// ophyd-async `Motor.calculate_timeout`, in seconds: the travel time at
/// `velo`, plus the acceleration time on both ends, plus the margin. `None`
/// for zero velocity, where ophyd-async raises instead of moving.
fn move_timeout_secs(old: f64, new: f64, velo: f64, accl: f64) -> Option<f64> {
    (velo != 0.0).then(|| ((new - old) / velo).abs() + 2.0 * accl + MOVE_TIMEOUT_MARGIN_SECS)
}

/// The channels of a [`CaMotor`] before connect: the setpoint, the readback
/// and the `.VELO`/`.ACCL` of `val_pv`'s record.
struct MotorChannels {
    setpoint: Arc<EpicsCaBackend<f64>>,
    readback: Arc<EpicsCaBackend<f64>>,
    timing: MoveTiming,
}

impl MotorChannels {
    fn new(val_pv: &str, rbv_pv: &str) -> Self {
        let record = val_pv.split_once('.').map_or(val_pv, |(r, _)| r);
        Self {
            setpoint: Arc::new(EpicsCaBackend::new(val_pv)),
            readback: Arc::new(EpicsCaBackend::new(rbv_pv)),
            timing: MoveTiming {
                velo: Arc::new(EpicsCaBackend::new(format!("{record}.VELO"))),
                accl: Arc::new(EpicsCaBackend::new(format!("{record}.ACCL"))),
            },
        }
    }

    /// Connect all four channels into a [`CaMotor`]. A `.VELO`/`.ACCL` that
    /// does not connect (no motor record behind `val_pv`) leaves its moves
    /// without a deadline.
    async fn connect(self, name: &str, rbv_pv: &str) -> Result<Arc<CaMotor>> {
        let t = Duration::from_secs(5);
        let (sp, rb, velo, accl) = tokio::join!(
            self.setpoint.connect(t),
            self.readback.connect(t),
            self.timing.velo.connect(t),
            self.timing.accl.connect(t),
        );
        sp?;
        rb?;
        let timing = match velo.and(accl) {
            Ok(()) => Some(self.timing),
            Err(e) => {
                tracing::warn!("ca_motor {name}: moves have no deadline: {e}");
                None
            }
        };
        Ok(Arc::new(CaMotor {
            name: name.to_string(),
            setpoint: self.setpoint,
            cache: SignalCache::new(self.readback.clone()),
            readback: self.readback,
            rbv_pv: rbv_pv.to_string(),
            timing,
        }))
    }
}

impl CaMotor {
    /// Build + connect the channels. Blocks on bsrs's runtime; the
    /// caller must invoke this from a sync context (see
    /// `bootstrap_ca` for the recommended order).
    pub fn connect_blocking(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Arc<Self>> {
        let ch = MotorChannels::new(val_pv, rbv_pv);
        crate::core::runtime::bsrs_runtime().block_on(ch.connect(name, rbv_pv))
    }

    /// Async equivalent — call from inside an existing tokio runtime
    /// (e.g. `bsrs qs-manager`'s `async fn run`). Same outcome as
    /// `connect_blocking` but doesn't trigger a nested-runtime panic.
    pub async fn connect_async(name: &str, val_pv: &str, rbv_pv: &str) -> Result<Arc<Self>> {
        MotorChannels::new(val_pv, rbv_pv)
            .connect(name, rbv_pv)
            .await
    }

    /// The deadline of a move to `target`, from the current `.RBV`, `.VELO`
    /// and `.ACCL`. `Ok(None)` when there is no timing record or a read
    /// fails; `Err` for zero velocity, refused before the put as
    /// ophyd-async does.
    async fn move_deadline(&self, target: f64) -> std::result::Result<Option<Duration>, String> {
        let Some(timing) = &self.timing else {
            return Ok(None);
        };
        let read = tokio::try_join!(
            self.readback.get_value(),
            timing.velo.get_value(),
            timing.accl.get_value(),
        );
        let (old, velo, accl) = match read {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("ca_motor {}: move has no deadline: {e}", self.name);
                return Ok(None);
            }
        };
        match move_timeout_secs(old, target, velo, accl) {
            // NaN or a travel time past `Duration::MAX` has no deadline.
            Some(secs) => Ok(Duration::try_from_secs_f64(secs).ok()),
            None => Err(format!(
                "ca_motor set: motor {} has zero velocity",
                self.name
            )),
        }
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
        Ok(HashMap::from([(
            self.name.clone(),
            describe_pv(self.readback.as_ref(), &self.rbv_pv, &self.name).await?,
        )]))
    }
}

#[async_trait::async_trait]
impl MovableObj for CaMotor {
    async fn set_dyn(&self, value: f64) -> Status {
        // The put completion is the motor settle (WRITE_NOTIFY), bounded by
        // the move's own deadline as ophyd-async's `Motor.set` bounds it.
        let deadline = match self.move_deadline(value).await {
            Ok(d) => d,
            Err(e) => return Status::fail(StatusError::Failed(e)),
        };
        move_status(
            self.setpoint.as_ref(),
            value,
            "ca_motor set",
            move |done| async move {
                let Some(d) = deadline else {
                    return done.await.map_err(|e| format!("ca_motor set: {e}"));
                };
                match tokio::time::timeout(d, done).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(format!("ca_motor set: {e}")),
                    Err(_) => Err(format!(
                        "ca_motor set: timed out after {:.3}s",
                        d.as_secs_f64()
                    )),
                }
            },
        )
        .await
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

#[async_trait::async_trait]
impl MonitorableObj for CaMotor {
    async fn subscribe_dyn(&self) -> Result<Subscription> {
        let (rx, token) = self.cache.add_listener();
        Ok(Subscription::new(rx, token, self.name.clone()))
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
    setpoint: Arc<EpicsCaBackend<f64>>,
    readback: Arc<EpicsCaBackend<f64>>,
    /// Monitor fan-out over `readback` (CP-08 / K2).
    cache: Arc<SignalCache<f64, EpicsCaBackend<f64>>>,
    done: Arc<EpicsCaBackend<f64>>,
    done_pv: String,
    rbv_pv: String,
    done_value: f64,
}

/// The three channels of a [`CaPositioner`], already connected.
struct PositionerChannels {
    setpoint: Arc<EpicsCaBackend<f64>>,
    readback: Arc<EpicsCaBackend<f64>>,
    done: Arc<EpicsCaBackend<f64>>,
}

impl CaPositioner {
    fn from_connected(
        name: &str,
        ch: PositionerChannels,
        done_pv: &str,
        rbv_pv: &str,
        done_value: f64,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_string(),
            setpoint: ch.setpoint,
            cache: SignalCache::new(ch.readback.clone()),
            readback: ch.readback,
            done: ch.done,
            done_pv: done_pv.to_string(),
            rbv_pv: rbv_pv.to_string(),
            done_value,
        })
    }

    /// Build + connect all three channels. Blocks; see
    /// `CaMotor::connect_blocking`.
    pub fn connect_blocking(
        name: &str,
        val_pv: &str,
        rbv_pv: &str,
        done_pv: &str,
        done_value: f64,
    ) -> Result<Arc<Self>> {
        let sp = Arc::new(EpicsCaBackend::<f64>::new(val_pv));
        let rb = Arc::new(EpicsCaBackend::<f64>::new(rbv_pv));
        let dn = Arc::new(EpicsCaBackend::<f64>::new(done_pv));
        let (sp_a, rb_a, dn_a) = (sp.clone(), rb.clone(), dn.clone());
        crate::core::runtime::bsrs_runtime().block_on(async move {
            sp_a.connect(Duration::from_secs(5)).await?;
            rb_a.connect(Duration::from_secs(5)).await?;
            dn_a.connect(Duration::from_secs(5)).await
        })?;
        let ch = PositionerChannels {
            setpoint: sp,
            readback: rb,
            done: dn,
        };
        Ok(Self::from_connected(name, ch, done_pv, rbv_pv, done_value))
    }

    /// Async equivalent — call from inside an existing tokio runtime.
    pub async fn connect_async(
        name: &str,
        val_pv: &str,
        rbv_pv: &str,
        done_pv: &str,
        done_value: f64,
    ) -> Result<Arc<Self>> {
        let sp = Arc::new(EpicsCaBackend::<f64>::new(val_pv));
        let rb = Arc::new(EpicsCaBackend::<f64>::new(rbv_pv));
        let dn = Arc::new(EpicsCaBackend::<f64>::new(done_pv));
        sp.connect(Duration::from_secs(5)).await?;
        rb.connect(Duration::from_secs(5)).await?;
        dn.connect(Duration::from_secs(5)).await?;
        let ch = PositionerChannels {
            setpoint: sp,
            readback: rb,
            done: dn,
        };
        Ok(Self::from_connected(name, ch, done_pv, rbv_pv, done_value))
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
        let r = self.readback.get_reading().await?;
        let mut out = HashMap::new();
        out.insert(self.name.clone(), r);
        Ok(out)
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        Ok(HashMap::from([(
            self.name.clone(),
            describe_pv(self.readback.as_ref(), &self.rbv_pv, &self.name).await?,
        )]))
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
                    .get_value()
                    .await
                    .map_err(|e| format!("ca_positioner done read: {e}"))?;
                if v == done_value {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        move_status(self.setpoint.as_ref(), value, "ca_positioner set", finish).await
    }
}

#[async_trait::async_trait]
impl LocatableObj for CaPositioner {
    async fn locate_dyn(&self) -> Result<DynLocation> {
        let setpoint = self.setpoint.get_value().await?;
        let readback = self.readback.get_value().await?;
        Ok(DynLocation { setpoint, readback })
    }
}

#[async_trait::async_trait]
impl StoppableObj for CaPositioner {
    async fn stop_dyn(&self, _success: bool) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl MonitorableObj for CaPositioner {
    async fn subscribe_dyn(&self) -> Result<Subscription> {
        let (rx, token) = self.cache.add_listener();
        Ok(Subscription::new(rx, token, self.name.clone()))
    }
}

/// CA-backed scalar detector: one Signal on a `_RBV` PV.
///
/// Also a [`MonitorableObj`]: `Msg::Monitor` / `monitor_during_wrapper`
/// stream every CA update of the PV as an Event. Subscriptions go through
/// one shared [`SignalCache`] (CP-08), so N monitors of the same detector
/// open one CA monitor, released when the last subscription drops (K2).
pub struct CaDetector {
    name: String,
    value: Arc<EpicsCaBackend<f64>>,
    value_pv: String,
    cache: Arc<SignalCache<f64, EpicsCaBackend<f64>>>,
    seen: AtomicI64,
}

impl CaDetector {
    fn from_connected(name: &str, value: Arc<EpicsCaBackend<f64>>, value_pv: &str) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_string(),
            cache: SignalCache::new(value.clone()),
            value,
            value_pv: value_pv.to_string(),
            seen: AtomicI64::new(0),
        })
    }

    /// Build + connect. Blocks; see `CaMotor::connect_blocking`.
    pub fn connect_blocking(name: &str, value_pv: &str) -> Result<Arc<Self>> {
        let v = Arc::new(EpicsCaBackend::<f64>::new(value_pv));
        let v_for_async = v.clone();
        crate::core::runtime::bsrs_runtime()
            .block_on(async move { v_for_async.connect(Duration::from_secs(5)).await })?;
        Ok(Self::from_connected(name, v, value_pv))
    }

    /// Async equivalent of `connect_blocking` — for callers
    /// already inside a tokio runtime (qs-manager's `async fn`).
    pub async fn connect_async(name: &str, value_pv: &str) -> Result<Arc<Self>> {
        let v = Arc::new(EpicsCaBackend::<f64>::new(value_pv));
        v.connect(Duration::from_secs(5)).await?;
        Ok(Self::from_connected(name, v, value_pv))
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
        Ok(HashMap::from([(
            self.name.clone(),
            describe_pv(self.value.as_ref(), &self.value_pv, &self.name).await?,
        )]))
    }
}

#[async_trait::async_trait]
impl MonitorableObj for CaDetector {
    async fn subscribe_dyn(&self) -> Result<Subscription> {
        let (rx, token) = self.cache.add_listener();
        Ok(Subscription::new(rx, token, self.name.clone()))
    }
}

/// The data key of one `f64` PV as `object_name` reports it: the backend's
/// `get_datakey` reads DBR_CTRL for `units`, `precision` and the limit
/// ranges — what ophyd's `EpicsSignal.describe()` carries — and sources it
/// as `ca://<pv>`.
async fn describe_pv(
    backend: &EpicsCaBackend<f64>,
    pv: &str,
    object_name: &str,
) -> Result<DataKey> {
    let mut key = backend.get_datakey(pv).await?;
    key.object_name = Some(object_name.to_string());
    Ok(key)
}

/// Send `value` to `setpoint`, then return a pending [`Status`] that `finish`
/// settles from the put's completion. The engine's `Set` handler awaits
/// `set_dyn` before it moves on to the next message, so a `set_dyn` that
/// awaited the move itself would serialize a multi-axis `mv`; bluesky's
/// `set()` returns its status immediately and the motion completes it. Only
/// the wait is spawned: a spawned send lets `set(a); set(b)` reach the IOC as
/// b, a, and the IOC then finishes at `a`.
async fn move_status<B, F, Fut>(setpoint: &B, value: f64, what: &str, finish: F) -> Status
where
    B: SignalBackend<f64>,
    F: FnOnce(PutCompletion) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
{
    let (status, setter) = Status::new();
    match setpoint.put_start(Some(value)).await {
        Ok(done) => {
            let mv = finish(done);
            tokio::spawn(async move {
                match mv.await {
                    Ok(()) => setter.success(),
                    Err(e) => setter.fail(StatusError::Failed(e)),
                }
            });
        }
        Err(e) => setter.fail(StatusError::Failed(format!("{what}: {e}"))),
    }
    status
}

/// Bootstrap the CA backend's global client. Must be called from a
/// sync context (no active tokio runtime); after this the cached
/// client is reused everywhere.
pub fn bootstrap_ca() {
    let _ = crate::backends::epics_ca::ca_context();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::mock::MockSignalBackend;
    use crate::core::Dtype;

    #[test]
    fn move_timeout_is_travel_plus_two_accelerations_plus_margin() {
        assert_eq!(
            move_timeout_secs(1.0, -3.0, 2.0, 0.5),
            Some(2.0 + 1.0 + 10.0)
        );
        assert_eq!(move_timeout_secs(1.0, 1.0, 2.0, 0.0), Some(10.0));
        assert_eq!(move_timeout_secs(0.0, 4.0, -2.0, 0.0), Some(12.0));
        assert_eq!(move_timeout_secs(0.0, 4.0, 0.0, 0.5), None);
        assert_eq!(move_timeout_secs(0.0, 0.0, -0.0, 0.5), None);
    }

    #[tokio::test]
    async fn move_status_sends_puts_in_call_order() {
        let sp = MockSignalBackend::with_value(0.0_f64, Dtype::Number);
        let blocked = sp.mock_puts_blocked();
        let finish = |done: PutCompletion| async move { done.await.map_err(|e| e.to_string()) };

        // No yield between the two calls: the puts must already be sent when
        // `move_status` returns, not left to spawned tasks.
        let a = move_status(&sp, 1.0, "t", finish).await;
        assert_eq!(sp.put_calls(), vec![Some(1.0)]);
        let b = move_status(&sp, 2.0, "t", finish).await;
        assert_eq!(sp.put_calls(), vec![Some(1.0), Some(2.0)]);
        assert!(!a.done_state() && !b.done_state());

        drop(blocked);
        let both = async { (a.await, b.await) };
        let (ra, rb) = tokio::time::timeout(Duration::from_secs(1), both)
            .await
            .unwrap();
        ra.unwrap();
        rb.unwrap();
    }
}
