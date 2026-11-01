//! The moves of the CA and PVA motors: their channels, their deadline, their
//! stop and the [`Status`] a move returns; and the data key of a host
//! device's PV.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::core::error::Result;
use crate::core::status::{Status, StatusError};
use crate::devices::BackendFromPv;
use crate::event_model::{DataKey, LimitsRange};
use crate::protocols_async::{PutCompletion, SignalBackend};

/// ophyd-async `DEFAULT_TIMEOUT`, the fixed term of
/// `Motor.calculate_timeout`.
const MOVE_TIMEOUT_MARGIN_SECS: f64 = 10.0;

/// ophyd-async `Motor.calculate_timeout`, in seconds: the travel time at
/// `velo`, plus the acceleration time on both ends, plus the margin. `None`
/// for zero velocity, where ophyd-async raises instead of moving.
fn move_timeout_secs(old: f64, new: f64, velo: f64, accl: f64) -> Option<f64> {
    (velo != 0.0).then(|| ((new - old) / velo).abs() + 2.0 * accl + MOVE_TIMEOUT_MARGIN_SECS)
}

/// The stop of a device's moves: [`Halt::stop`] settles the move in flight
/// at once, done for `success` and failed otherwise, as ophyd-async's
/// `StandardMovable.stop` cancels its move task. A move started after the
/// stop is not ended by it.
pub(crate) struct Halt(watch::Sender<bool>);

impl Halt {
    pub(crate) fn new() -> Self {
        Self(watch::channel(false).0)
    }

    /// Settle the move in flight, if any.
    pub(crate) fn stop(&self, success: bool) {
        self.0.send_replace(success);
    }
}

/// A motor record's `.STOP`, pressed with a plain put: a put-callback to the
/// record queues behind the move's own (dbNotify) and would press it only
/// once the move is over.
#[async_trait::async_trait]
pub(crate) trait StopField: Send + Sync + Sized {
    fn from_pv(pv: &str) -> Self;
    async fn connect(&self, timeout: Duration) -> Result<()>;
    /// Write 1 without waiting for the record to process.
    async fn press(&self) -> Result<()>;
}

/// A backend for the motor record's numeric fields, with its kind of
/// [`StopField`].
pub(crate) trait MotorBackend: SignalBackend<f64> + BackendFromPv {
    type Stop: StopField;
}

#[cfg(feature = "ca")]
#[async_trait::async_trait]
impl StopField for crate::backends::epics_ca::EpicsCaBackend<bool> {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
    async fn connect(&self, timeout: Duration) -> Result<()> {
        SignalBackend::connect(self, timeout).await
    }
    async fn press(&self) -> Result<()> {
        self.put_nowait(true).await
    }
}

#[cfg(feature = "ca")]
impl MotorBackend for crate::backends::epics_ca::EpicsCaBackend<f64> {
    type Stop = crate::backends::epics_ca::EpicsCaBackend<bool>;
}

/// A PVA `.STOP`: a put with the default pvRequest, which QSRV answers
/// without `block`.
#[cfg(feature = "pva")]
pub(crate) struct PvaStop(String);

#[cfg(feature = "pva")]
#[async_trait::async_trait]
impl StopField for PvaStop {
    fn from_pv(pv: &str) -> Self {
        Self(pv.to_string())
    }
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        crate::backends::epics_pva::pva_context()
            .pvconnect(&self.0)
            .await
            .map(|_| ())
            .map_err(|e| {
                crate::core::error::BsrsError::Backend(format!("pva connect {}: {e}", self.0))
            })
    }
    async fn press(&self) -> Result<()> {
        crate::backends::epics_pva::pva_context()
            .pvput(&self.0, "1")
            .await
            .map_err(|e| crate::core::error::BsrsError::Backend(format!("pva put {}: {e}", self.0)))
    }
}

#[cfg(feature = "pva")]
impl MotorBackend for crate::backends::epics_pva::EpicsPvaBackend<f64> {
    type Stop = PvaStop;
}

/// The motor record's `.VELO` and `.ACCL`, read at each move for its
/// deadline.
struct MoveTiming<B> {
    velo: Arc<B>,
    accl: Arc<B>,
}

/// The motor record's `.DMOV` and `.STOP`.
struct StopButton<B: MotorBackend> {
    dmov: B,
    stop: B::Stop,
}

impl<B: MotorBackend> StopButton<B> {
    /// ophyd-async `Motor.stop`: press `.STOP` only while the motor moves.
    async fn press_if_moving(&self) -> Result<()> {
        if self.dmov.get_value().await? == 0.0 {
            self.stop.press().await?;
        }
        Ok(())
    }
}

/// A motor's setpoint and readback, the fields of the setpoint's record that
/// its moves and stop use, connected, and the [`Halt`] of its moves.
pub(crate) struct MotorChannels<B: MotorBackend> {
    pub(crate) setpoint: Arc<B>,
    pub(crate) readback: Arc<B>,
    /// `None` when the setpoint's record has no `.VELO`/`.ACCL` (not a
    /// motor record); its moves then have no deadline.
    timing: Option<MoveTiming<B>>,
    /// `None` when the setpoint's record has no `.DMOV`/`.STOP`; a stop then
    /// only settles the move.
    button: Option<StopButton<B>>,
    halt: Halt,
}

impl<B: MotorBackend> MotorChannels<B> {
    /// Connect `val_pv`, `rbv_pv` and the `.VELO`/`.ACCL`/`.DMOV`/`.STOP` of
    /// `val_pv`'s record. A `.VELO`/`.ACCL` that does not connect (no motor
    /// record behind `val_pv`) leaves the moves without a deadline; a
    /// `.DMOV`/`.STOP` that does not, the stop without a press. `kind` and
    /// `name` label the motor in messages.
    pub(crate) async fn connect(
        kind: &str,
        name: &str,
        val_pv: &str,
        rbv_pv: &str,
    ) -> Result<Self> {
        let record = val_pv.split_once('.').map_or(val_pv, |(r, _)| r);
        let setpoint = Arc::new(B::from_pv(val_pv));
        let readback = Arc::new(B::from_pv(rbv_pv));
        let timing = MoveTiming {
            velo: Arc::new(B::from_pv(&format!("{record}.VELO"))),
            accl: Arc::new(B::from_pv(&format!("{record}.ACCL"))),
        };
        let button = StopButton {
            dmov: B::from_pv(&format!("{record}.DMOV")),
            stop: <B::Stop as StopField>::from_pv(&format!("{record}.STOP")),
        };
        let t = Duration::from_secs(5);
        let (sp, rb, velo, accl, dmov, stop) = tokio::join!(
            setpoint.connect(t),
            readback.connect(t),
            timing.velo.connect(t),
            timing.accl.connect(t),
            button.dmov.connect(t),
            button.stop.connect(t),
        );
        sp?;
        rb?;
        let timing = match velo.and(accl) {
            Ok(()) => Some(timing),
            Err(e) => {
                tracing::warn!("{kind} {name}: moves have no deadline: {e}");
                None
            }
        };
        let button = match dmov.and(stop) {
            Ok(()) => Some(button),
            Err(e) => {
                tracing::warn!("{kind} {name}: stop presses no .STOP: {e}");
                None
            }
        };
        Ok(Self {
            setpoint,
            readback,
            timing,
            button,
            halt: Halt::new(),
        })
    }

    /// Move to `target`. The put completion is the motor settle, bounded by
    /// the move's own deadline as ophyd-async's `Motor.set` bounds it.
    pub(crate) async fn set(&self, kind: &str, name: &str, target: f64) -> Status {
        let what = format!("{kind} set");
        let deadline = match self.deadline(kind, name, target).await {
            Ok(d) => d,
            Err(e) => return Status::fail(StatusError::Failed(format!("{what}: {e}"))),
        };
        let label = what.clone();
        let finish = |done: PutCompletion| async move {
            let Some(d) = deadline else {
                return done.await.map_err(|e| format!("{label}: {e}"));
            };
            match tokio::time::timeout(d, done).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(format!("{label}: {e}")),
                Err(_) => Err(format!("{label}: timed out after {:.3}s", d.as_secs_f64())),
            }
        };
        move_status(self.setpoint.as_ref(), target, &what, &self.halt, finish).await
    }

    /// ophyd-async `Motor.stop`, then `StandardMovable.stop`: press `.STOP`
    /// while the motor moves, then settle the move in flight. The move is
    /// settled even when the press fails.
    pub(crate) async fn stop(&self, success: bool) -> Result<()> {
        let pressed = match &self.button {
            Some(button) => button.press_if_moving().await,
            None => Ok(()),
        };
        self.halt.stop(success);
        pressed
    }

    /// The deadline of a move to `target`, from the current readback,
    /// `.VELO` and `.ACCL`. `Ok(None)` when there is no timing record or a
    /// read fails; `Err` for zero velocity, refused before the put as
    /// ophyd-async does.
    async fn deadline(
        &self,
        kind: &str,
        name: &str,
        target: f64,
    ) -> std::result::Result<Option<Duration>, String> {
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
                tracing::warn!("{kind} {name}: move has no deadline: {e}");
                return Ok(None);
            }
        };
        match move_timeout_secs(old, target, velo, accl) {
            // NaN or a travel time past `Duration::MAX` has no deadline.
            Some(secs) => Ok(Duration::try_from_secs_f64(secs).ok()),
            None => Err(format!("motor {name} has zero velocity")),
        }
    }
}

/// Send `value` to `setpoint`, then return a pending [`Status`] that `finish`
/// settles from the put's completion. The engine's `Set` handler awaits
/// `set_dyn` before it moves on to the next message, so a `set_dyn` that
/// awaited the move itself would serialize a multi-axis `mv`; bluesky's
/// `set()` returns its status immediately and the motion completes it. Only
/// the wait is spawned: a spawned send lets `set(a); set(b)` reach the IOC as
/// b, a, and the IOC then finishes at `a`. A `value` outside the setpoint's
/// control limits fails the status with nothing sent. A [`Halt::stop`] once
/// this is called settles the status; halting the motion is the caller's.
pub(crate) async fn move_status<B, F, Fut>(
    setpoint: &B,
    value: f64,
    what: &str,
    halt: &Halt,
    finish: F,
) -> Status
where
    B: SignalBackend<f64>,
    F: FnOnce(PutCompletion) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
{
    let (status, setter) = Status::new();
    let mut halted = halt.0.subscribe();
    let started = match within_limits(setpoint, value).await {
        Ok(()) => setpoint
            .put_start(Some(value))
            .await
            .map_err(|e| e.to_string()),
        Err(e) => Err(e),
    };
    match started {
        Ok(done) => {
            let mv = finish(done);
            let stopped = format!("{what}: stopped");
            tokio::spawn(async move {
                let end = tokio::select! {
                    end = mv => end,
                    Ok(()) = halted.changed() => {
                        if *halted.borrow() { Ok(()) } else { Err(stopped) }
                    }
                };
                match end {
                    Ok(()) => setter.success(),
                    Err(e) => setter.fail(StatusError::Failed(e)),
                }
            });
        }
        Err(e) => setter.fail(StatusError::Failed(format!("{what}: {e}"))),
    }
    status
}

/// ophyd `EpicsSignal.check_value`: `target` against the control limits of
/// the setpoint, read at each move as ophyd-async's `Motor.check_move` reads
/// its limits.
async fn within_limits<B: SignalBackend<f64>>(
    setpoint: &B,
    target: f64,
) -> std::result::Result<(), String> {
    let key = setpoint
        .get_datakey("")
        .await
        .map_err(|e| format!("limits: {e}"))?;
    check_range(key.limits.and_then(|l| l.control).as_ref(), target)
}

/// A range with `low >= high` is no limit, as in ophyd (a motor record's dial
/// limits 0, 0); an absent bound is open.
fn check_range(range: Option<&LimitsRange>, target: f64) -> std::result::Result<(), String> {
    let Some(range) = range else {
        return Ok(());
    };
    let low = range.low.unwrap_or(f64::NEG_INFINITY);
    let high = range.high.unwrap_or(f64::INFINITY);
    if low >= high || (low..=high).contains(&target) {
        return Ok(());
    }
    Err(format!("{target} is outside the limits [{low}, {high}]"))
}

/// The data key of one `f64` PV as `object_name` reports it: the backend's
/// `get_datakey` reads the PV's units, precision and limit ranges — what
/// ophyd's `EpicsSignal.describe()` carries — and sources it from the PV.
pub(crate) async fn describe_pv<B: SignalBackend<f64>>(
    backend: &B,
    pv: &str,
    object_name: &str,
) -> Result<DataKey> {
    let mut key = backend.get_datakey(pv).await?;
    key.object_name = Some(object_name.to_string());
    Ok(key)
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

    #[test]
    fn check_range_refuses_only_a_target_outside_an_ordered_range() {
        let r = |low, high| Some(LimitsRange { low, high });
        assert!(check_range(None, 1e9).is_ok());
        assert!(check_range(r(Some(-1.0), Some(1.0)).as_ref(), -1.0).is_ok());
        assert!(check_range(r(Some(-1.0), Some(1.0)).as_ref(), 1.0).is_ok());
        assert!(check_range(r(Some(-1.0), Some(1.0)).as_ref(), 1.5).is_err());
        assert!(check_range(r(Some(-1.0), Some(1.0)).as_ref(), -1.5).is_err());
        assert!(check_range(r(Some(-1.0), Some(1.0)).as_ref(), f64::NAN).is_err());
        assert!(check_range(r(Some(3.0), Some(3.0)).as_ref(), 9.0).is_ok());
        assert!(check_range(r(Some(1.0), Some(-1.0)).as_ref(), 9.0).is_ok());
        assert!(check_range(r(None, Some(1.0)).as_ref(), -1e9).is_ok());
        assert!(check_range(r(None, Some(1.0)).as_ref(), 2.0).is_err());
        assert!(check_range(r(Some(-1.0), None).as_ref(), -2.0).is_err());
    }

    #[tokio::test]
    async fn move_status_sends_puts_in_call_order() {
        let sp = MockSignalBackend::with_value(0.0_f64, Dtype::Number);
        let blocked = sp.mock_puts_blocked();
        let finish = |done: PutCompletion| async move { done.await.map_err(|e| e.to_string()) };

        // No yield between the two calls: the puts must already be sent when
        // `move_status` returns, not left to spawned tasks.
        let halt = Halt::new();
        let a = move_status(&sp, 1.0, "t", &halt, finish).await;
        assert_eq!(sp.put_calls(), vec![Some(1.0)]);
        let b = move_status(&sp, 2.0, "t", &halt, finish).await;
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

    #[tokio::test]
    async fn halt_settles_only_the_move_in_flight() {
        let sp = MockSignalBackend::with_value(0.0_f64, Dtype::Number);
        let _blocked = sp.mock_puts_blocked();
        let finish = |done: PutCompletion| async move { done.await.map_err(|e| e.to_string()) };
        let halt = Halt::new();
        let settled = |st: Status| async move {
            tokio::time::timeout(Duration::from_secs(1), st)
                .await
                .expect("the stop settles the move")
        };

        let st = move_status(&sp, 1.0, "t", &halt, finish).await;
        halt.stop(false);
        let err = settled(st).await.expect_err("stop(false) fails the move");
        assert!(err.to_string().contains("t: stopped"), "{err}");

        let st = move_status(&sp, 2.0, "t", &halt, finish).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!st.done_state(), "an earlier stop settled a later move");
        halt.stop(true);
        settled(st).await.expect("stop(true) completes the move");
    }
}
