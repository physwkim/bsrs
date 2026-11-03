//! The moves of the CA and PVA motors: their channels, their deadline, their
//! stop and the [`Status`] a move returns, with its watcher updates; and the
//! data key of a host device's PV.

use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use serde::Serialize;
use tokio::sync::watch;

use crate::core::error::{BsrsError, Result};
use crate::core::status::{Status, StatusError, WatcherUpdate};
use crate::devices::{
    wait_for_connection, Access, BackendFromPv, Readable, Signal, SignalConfig, SignalR, SignalRW,
    SignalW,
};
use crate::event_model::{DataKey, LimitsRange};
use crate::protocols_async::{AsyncSubscribable, PutCompletion, SignalBackend};

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

/// A host device's signal on `pv`, named `name` as the derive names a
/// device's children; a readback is named after its device, as
/// ophyd-async's `Motor.set_name` names `user_readback`.
pub(crate) fn pv_signal<T, B, A>(backend: B, pv: &str, name: String) -> Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Access,
{
    let config = SignalConfig {
        source: pv.to_string(),
        name,
        ..Default::default()
    };
    Signal::new(Arc::new(backend), config)
}

/// What [`move_status`] moves and reports on. The watcher updates carry the
/// readback's name, the device's.
pub(crate) struct Axis<'a, B: SignalBackend<f64>> {
    pub(crate) setpoint: &'a SignalRW<f64, B>,
    pub(crate) readback: &'a SignalR<f64, B>,
    pub(crate) limits: Limits<'a, B>,
    pub(crate) halt: &'a Halt,
}

/// The check a move passes before [`move_status`] sends it, as the Python
/// class the device ports makes it.
pub(crate) enum Limits<'a, B: SignalBackend<f64>> {
    /// ophyd-async `Motor.check_move`, on the motor record's fields.
    MotorRecord(&'a MotorLimits<B>),
    /// The setpoint's control limits.
    Control(ControlRule),
}

impl<B: SignalBackend<f64> + 'static> Limits<'_, B> {
    /// Check a move from `from` to `target`, on limits read now.
    async fn check(
        &self,
        setpoint: &SignalRW<f64, B>,
        from: f64,
        target: f64,
    ) -> std::result::Result<(), String> {
        match self {
            Self::MotorRecord(fields) => fields.check(from, target).await,
            Self::Control(rule) => {
                let key = describe_one(setpoint)
                    .await
                    .map_err(|e| format!("limits: {e}"))?;
                rule.check(key.limits.and_then(|l| l.control).as_ref(), target)
            }
        }
    }
}

/// ophyd's rules for a target against a setpoint's control limits, which
/// differ in which ranges are no limit. An absent bound is open.
#[derive(Clone, Copy)]
pub(crate) enum ControlRule {
    /// ophyd `EpicsSignal.check_value`, to which `EpicsMotor.check_value`
    /// delegates: none when `low >= high`.
    Signal,
    /// ophyd `PVPositioner.check_value`: none when `low == high`.
    Positioner,
}

impl ControlRule {
    fn check(self, range: Option<&LimitsRange>, target: f64) -> std::result::Result<(), String> {
        let Some(range) = range else {
            return Ok(());
        };
        let low = range.low.unwrap_or(f64::NEG_INFINITY);
        let high = range.high.unwrap_or(f64::INFINITY);
        let none = match self {
            Self::Signal => low >= high,
            Self::Positioner => low == high,
        };
        if none || (low..=high).contains(&target) {
            return Ok(());
        }
        Err(format!("{target} is outside the limits [{low}, {high}]"))
    }
}

/// The motor record's `.LLM`, `.HLM`, `.DLLM` and `.DHLM`.
pub(crate) struct MotorLimits<B: SignalBackend<f64>> {
    llm: SignalRW<f64, B>,
    hlm: SignalRW<f64, B>,
    dllm: SignalRW<f64, B>,
    dhlm: SignalRW<f64, B>,
}

impl<B: SignalBackend<f64>> MotorLimits<B> {
    async fn check(&self, from: f64, target: f64) -> std::result::Result<(), String> {
        let (llm, hlm, dllm, dhlm) = tokio::try_join!(
            self.llm.get(),
            self.hlm.get(),
            self.dllm.get(),
            self.dhlm.get(),
        )
        .map_err(|e| format!("limits: {e}"))?;
        check_motor_move([llm, hlm], [dllm, dhlm], from, target)
    }
}

/// ophyd-async `Motor.check_move`: dial limits 0, 0 are no limits; otherwise
/// both where the motor stands and `target` lie in `[LLM, HLM]`.
fn check_motor_move(
    [llm, hlm]: [f64; 2],
    dial: [f64; 2],
    from: f64,
    target: f64,
) -> std::result::Result<(), String> {
    let within = |x: f64| hlm >= x && x >= llm;
    if dial == [0.0, 0.0] || (within(from) && within(target)) {
        return Ok(());
    }
    Err(format!(
        "a move from {from} to {target} is outside the limits [{llm}, {hlm}]"
    ))
}

/// A backend for the motor record's numeric fields, with its kinds of
/// boolean field (`.SET`, a two-choice enum) and integer field (`.DMOV` and
/// `.STOP`, SHORTs).
pub(crate) trait MotorBackend: SignalBackend<f64> + BackendFromPv + 'static {
    type Bool: SignalBackend<bool> + BackendFromPv + 'static;
    type Int: SignalBackend<i64> + BackendFromPv + 'static;
    /// `.STOP`, ophyd-async's `epics_signal_w(int, wait=False)`: a put that
    /// waited would queue behind the move's own and press it only once the
    /// move is over.
    fn stop_field(pv: &str) -> Self::Int;
}

#[cfg(feature = "ca")]
impl MotorBackend for crate::backends::epics_ca::EpicsCaBackend<f64> {
    type Bool = crate::backends::epics_ca::EpicsCaBackend<bool>;
    type Int = crate::backends::epics_ca::EpicsCaBackend<i64>;
    fn stop_field(pv: &str) -> Self::Int {
        Self::Int::new(pv).with_wait(|_| false)
    }
}

#[cfg(feature = "pva")]
impl MotorBackend for crate::backends::epics_pva::EpicsPvaBackend<f64> {
    type Bool = crate::backends::epics_pva::EpicsPvaBackend<bool>;
    type Int = crate::backends::epics_pva::EpicsPvaBackend<i64>;
    fn stop_field(pv: &str) -> Self::Int {
        Self::Int::new(pv).with_wait(|_| false)
    }
}

/// The motor record's `.VELO` and `.ACCL`, read at each move for its
/// deadline.
struct MoveTiming<B: SignalBackend<f64>> {
    velo: SignalRW<f64, B>,
    accl: SignalRW<f64, B>,
}

/// The motor record's `.DMOV` and `.STOP`.
struct StopButton<B: MotorBackend> {
    dmov: SignalR<i64, B::Int>,
    stop: SignalW<i64, B::Int>,
}

impl<B: MotorBackend> StopButton<B> {
    /// ophyd-async `Motor.stop`: press `.STOP` only while the motor moves.
    async fn press_if_moving(&self) -> Result<()> {
        if self.dmov.get().await? == 0 {
            self.stop.put(1).await.await?;
        }
        Ok(())
    }
}

/// A motor's setpoint and readback, the fields of the setpoint's record that
/// its moves and stop use, connected, and the [`Halt`] of its moves.
pub(crate) struct MotorChannels<B: MotorBackend> {
    pub(crate) setpoint: Arc<SignalRW<f64, B>>,
    /// Named after the device.
    pub(crate) readback: SignalR<f64, B>,
    /// `None` when the setpoint's record has no `.VELO`/`.ACCL` (not a
    /// motor record); its moves then have no deadline.
    timing: Option<MoveTiming<B>>,
    /// `None` when the setpoint's record has no `.DMOV`/`.STOP`; a stop then
    /// only settles the move.
    button: Option<StopButton<B>>,
    /// `None` when the setpoint's record has no `.LLM`/`.HLM`/`.DLLM`/
    /// `.DHLM`; its moves are then checked as ophyd's `EpicsMotor` checks
    /// them, by [`ControlRule::Signal`].
    limits: Option<MotorLimits<B>>,
    /// The setpoint record's `.SET`; `None` when it has none.
    set_use: Option<Arc<SignalRW<bool, B::Bool>>>,
    halt: Halt,
}

impl<B: MotorBackend> MotorChannels<B> {
    /// Connect `val_pv`, `rbv_pv` and the `.VELO`/`.ACCL`/`.DMOV`/`.STOP`/
    /// `.SET`/`.LLM`/`.HLM`/`.DLLM`/`.DHLM` of `val_pv`'s record. A `.VELO`/
    /// `.ACCL` that does not connect (no motor record behind `val_pv`)
    /// leaves the moves without a deadline; a `.DMOV`/`.STOP` that does not,
    /// the stop without a press; a `.SET` that does not, no
    /// `set_current_position`; a limit that does not, the moves checked on
    /// the setpoint's control limits in place of ophyd-async's
    /// `Motor.check_move`. `kind` labels the motor in messages; its signals
    /// are named after ophyd-async `Motor`'s, under `name`.
    pub(crate) async fn connect(
        kind: &str,
        name: &str,
        val_pv: &str,
        rbv_pv: &str,
    ) -> Result<Self> {
        let record = val_pv.split_once('.').map_or(val_pv, |(r, _)| r);
        let field = |f: &str| format!("{record}.{f}");
        let child = |c: &str| format!("{name}-{c}");
        let setpoint = Arc::new(pv_signal(
            B::from_pv(val_pv),
            val_pv,
            child("user_setpoint"),
        ));
        let readback = pv_signal(B::from_pv(rbv_pv), rbv_pv, name.to_string());
        let f64_field = |f: &str, c: &str| pv_signal(B::from_pv(&field(f)), &field(f), child(c));
        let timing = MoveTiming {
            velo: f64_field("VELO", "velocity"),
            accl: f64_field("ACCL", "acceleration_time"),
        };
        let button = StopButton::<B> {
            dmov: pv_signal(
                B::Int::from_pv(&field("DMOV")),
                &field("DMOV"),
                child("motor_done_move"),
            ),
            stop: pv_signal(
                B::stop_field(&field("STOP")),
                &field("STOP"),
                child("motor_stop"),
            ),
        };
        let set_use: SignalRW<bool, B::Bool> = pv_signal(
            B::Bool::from_pv(&field("SET")),
            &field("SET"),
            child("set_use_switch"),
        );
        let limits = MotorLimits {
            llm: f64_field("LLM", "low_limit_travel"),
            hlm: f64_field("HLM", "high_limit_travel"),
            dllm: f64_field("DLLM", "dial_low_limit_travel"),
            dhlm: f64_field("DHLM", "dial_high_limit_travel"),
        };
        let t = Duration::from_secs(5);
        let (required, velo, accl, dmov, stop, set, llm, hlm, dllm, dhlm) = tokio::join!(
            wait_for_connection([
                ("setpoint", setpoint.connect(t).boxed()),
                ("readback", readback.connect(t).boxed()),
            ]),
            timing.velo.connect(t),
            timing.accl.connect(t),
            button.dmov.connect(t),
            button.stop.connect(t),
            set_use.connect(t),
            limits.llm.connect(t),
            limits.hlm.connect(t),
            limits.dllm.connect(t),
            limits.dhlm.connect(t),
        );
        required?;
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
        let set_use = match set {
            Ok(()) => Some(Arc::new(set_use)),
            Err(e) => {
                tracing::warn!("{kind} {name}: no set_current_position: {e}");
                None
            }
        };
        let limits = match llm.and(hlm).and(dllm).and(dhlm) {
            Ok(()) => Some(limits),
            Err(e) => {
                tracing::warn!("{kind} {name}: moves are checked on the control limits: {e}");
                None
            }
        };
        Ok(Self {
            setpoint,
            readback,
            timing,
            button,
            limits,
            set_use,
            halt: Halt::new(),
        })
    }

    /// Move to `target`. The put completion is the motor settle, bounded by
    /// the move's own deadline as ophyd-async's `Motor.set` bounds it.
    pub(crate) async fn set(&self, kind: &str, target: f64) -> Status {
        let what = format!("{kind} set");
        let deadline = match self.deadline(kind, target).await {
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
        let axis = Axis {
            setpoint: &self.setpoint,
            readback: &self.readback,
            limits: self
                .limits
                .as_ref()
                .map_or(Limits::Control(ControlRule::Signal), Limits::MotorRecord),
            halt: &self.halt,
        };
        move_status(axis, target, &what, finish).await
    }

    /// ophyd `EpicsMotor.set_current_position`: `.SET` to Set, `pos` to the
    /// setpoint with no limits check (`force=True`), then `.SET` back to Use
    /// whatever that put did. Spawned so that a dropped caller still restores
    /// `.SET`: a motor left in Set recalibrates on its next move.
    pub(crate) async fn set_current_position(&self, kind: &str, pos: f64) -> Result<()> {
        let name = self.readback.name();
        let Some(switch) = self.set_use.clone() else {
            return Err(BsrsError::Backend(format!(
                "{kind} {name}: set_current_position: no .SET"
            )));
        };
        let setpoint = self.setpoint.clone();
        tokio::spawn(async move {
            switch.put(true).await.await?;
            let put = setpoint.put(pos).await.await;
            let restored = switch.put(false).await.await;
            Ok(put.and(restored)?)
        })
        .await
        .map_err(|e| BsrsError::Backend(format!("{kind} {name}: set_current_position: {e}")))?
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
        target: f64,
    ) -> std::result::Result<Option<Duration>, String> {
        let name = self.readback.name();
        let Some(timing) = &self.timing else {
            return Ok(None);
        };
        let read = tokio::try_join!(self.readback.get(), timing.velo.get(), timing.accl.get());
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

/// Send `value` to the axis's setpoint, then return a pending [`Status`] that
/// `finish` settles from the put's completion. The engine's `Set` handler
/// awaits `set_dyn` before it moves on to the next message, so a `set_dyn`
/// that awaited the move itself would serialize a multi-axis `mv`; bluesky's
/// `set()` returns its status immediately and the motion completes it. Only
/// the wait is spawned: a spawned send lets `set(a); set(b)` reach the IOC as
/// b, a, and the IOC then finishes at `a`. A move the axis's [`Limits`]
/// refuses fails the status with nothing sent. A [`Halt::stop`] once
/// this is called settles the status; halting the motion is the caller's.
///
/// Until it settles, the status carries a [`WatcherUpdate`] for each readback
/// its monitor delivers, the first being where the axis stands, as
/// ophyd-async's `StandardMovable.set` yields them from `observe_value`;
/// `initial`, `unit` and `precision` are read from the readback before the
/// put, and there is no `fraction`.
pub(crate) async fn move_status<B, F, Fut>(
    axis: Axis<'_, B>,
    value: f64,
    what: &str,
    finish: F,
) -> Status
where
    B: SignalBackend<f64> + 'static,
    F: FnOnce(PutCompletion) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
{
    let (status, setter) = Status::new();
    let mut halted = axis.halt.0.subscribe();
    let started = async {
        let (initial, key) = tokio::try_join!(axis.readback.get(), describe_one(axis.readback))
            .map_err(|e| e.to_string())?;
        axis.limits.check(axis.setpoint, initial, value).await?;
        let monitor = AsyncSubscribable::subscribe(axis.readback)
            .await
            .map_err(|e| e.to_string())?;
        let done = axis
            .setpoint
            .put_start(value)
            .await
            .map_err(|e| e.to_string())?;
        let update = WatcherUpdate {
            name: Some(axis.readback.name().to_string()),
            unit: key.units,
            precision: key.precision.and_then(|p| i32::try_from(p).ok()),
            ..WatcherUpdate::new(initial, initial, value)
        };
        Ok::<_, String>((done, monitor, update))
    }
    .await;
    match started {
        Ok((done, mut monitor, update)) => {
            let mv = finish(done);
            let stopped = format!("{what}: stopped");
            tokio::spawn(async move {
                let readings = monitor.rx_mut();
                tokio::pin!(mv);
                let end = loop {
                    tokio::select! {
                        biased;
                        Ok(()) = halted.changed() => {
                            break if *halted.borrow() { Ok(()) } else { Err(stopped) };
                        }
                        Ok(()) = readings.changed() => {
                            let current = readings.borrow_and_update().value.as_f64();
                            if let Some(current) = current {
                                setter.update_watcher(WatcherUpdate { current, ..update.clone() });
                            }
                        }
                        end = &mut mv => break end,
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

/// The one data key of `signal`'s description.
pub(crate) async fn describe_one<T, B, A>(signal: &Signal<T, B, A>) -> Result<DataKey>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable,
{
    signal
        .describe()
        .await?
        .into_values()
        .next()
        .ok_or_else(|| BsrsError::Backend(format!("{}: no data key", signal.name())))
}

/// A host device's description: its one signal's data key, with the
/// device as its `object_name`, as ophyd's `EpicsSignal.describe()` carries
/// the PV's units, precision and limit ranges.
pub(crate) async fn describe_device<T, B, A>(
    signal: &Signal<T, B, A>,
    object_name: &str,
) -> Result<std::collections::HashMap<String, DataKey>>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable,
{
    let mut keys = signal.describe().await?;
    for key in keys.values_mut() {
        key.object_name = Some(object_name.to_string());
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::mock::MockSignalBackend;

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

    /// ophyd `EpicsSignal.check_value` and `PVPositioner.check_value`: a
    /// target outside an ordered range is refused, a range with `low ==
    /// high` is none; `low > high` is none to the first and refuses all to
    /// the second.
    #[test]
    fn control_rules_refuse_a_target_outside_the_range() {
        let r = |low, high| Some(LimitsRange { low, high });
        for rule in [ControlRule::Signal, ControlRule::Positioner] {
            assert!(rule.check(None, 1e9).is_ok());
            assert!(rule.check(r(Some(-1.0), Some(1.0)).as_ref(), -1.0).is_ok());
            assert!(rule.check(r(Some(-1.0), Some(1.0)).as_ref(), 1.0).is_ok());
            assert!(rule.check(r(Some(-1.0), Some(1.0)).as_ref(), 1.5).is_err());
            assert!(rule.check(r(Some(-1.0), Some(1.0)).as_ref(), -1.5).is_err());
            assert!(rule
                .check(r(Some(-1.0), Some(1.0)).as_ref(), f64::NAN)
                .is_err());
            assert!(rule.check(r(Some(3.0), Some(3.0)).as_ref(), 9.0).is_ok());
            assert!(rule.check(r(None, Some(1.0)).as_ref(), -1e9).is_ok());
            assert!(rule.check(r(None, Some(1.0)).as_ref(), 2.0).is_err());
            assert!(rule.check(r(Some(-1.0), None).as_ref(), -2.0).is_err());
        }
        let inverted = r(Some(1.0), Some(-1.0));
        assert!(ControlRule::Signal.check(inverted.as_ref(), 9.0).is_ok());
        for target in [0.0, 1.0, -1.0, 9.0] {
            assert!(ControlRule::Positioner
                .check(inverted.as_ref(), target)
                .is_err());
        }
    }

    /// ophyd-async `test_move_outside_motor_limits_causes_error`,
    /// `test_given_limits_of_0_0_then_move_causes_no_error` and
    /// `Motor.check_move`: both ends of the move lie in `[LLM, HLM]` unless
    /// the dial limits are 0, 0; NaN and an inverted range refuse.
    #[test]
    fn check_motor_move_refuses_a_move_that_starts_or_ends_past_a_limit() {
        let lim = [-1.0, 1.0];
        let dial = [-1.0, 1.0];
        assert!(check_motor_move(lim, dial, 0.0, 1.0).is_ok());
        assert!(check_motor_move(lim, dial, -1.0, 0.0).is_ok());
        assert!(check_motor_move(lim, dial, 0.0, 1.5).is_err());
        assert!(check_motor_move(lim, dial, 1.5, 0.0).is_err());
        assert!(check_motor_move(lim, dial, -1.5, 0.0).is_err());
        assert!(check_motor_move(lim, dial, f64::NAN, 0.0).is_err());
        assert!(check_motor_move(lim, dial, 0.0, f64::NAN).is_err());
        assert!(check_motor_move([1.0, -1.0], dial, 0.0, 0.0).is_err());
        assert!(check_motor_move([-0.001, 0.001], [0.0, 0.0], 0.0, 100.0).is_ok());
        assert!(check_motor_move([0.0, 0.0], [0.0, 0.5], 0.0, 0.0).is_ok());
        assert!(check_motor_move([0.0, 0.0], [0.0, 0.5], 0.0, 0.1).is_err());
    }

    async fn put_done(done: PutCompletion) -> std::result::Result<(), String> {
        done.await.map_err(|e| e.to_string())
    }

    fn mock(value: f64) -> MockSignalBackend<f64> {
        MockSignalBackend::with_value(value)
    }

    /// A signal over a clone of `backend`, which shares its state.
    fn signal<A: Access>(
        backend: &MockSignalBackend<f64>,
        name: &str,
    ) -> Signal<f64, MockSignalBackend<f64>, A> {
        pv_signal(backend.clone(), "pv", name.to_string())
    }

    fn axis<'a, B: SignalBackend<f64>>(
        setpoint: &'a SignalRW<f64, B>,
        readback: &'a SignalR<f64, B>,
        halt: &'a Halt,
    ) -> Axis<'a, B> {
        Axis {
            setpoint,
            readback,
            limits: Limits::Control(ControlRule::Signal),
            halt,
        }
    }

    #[tokio::test]
    async fn move_status_sends_puts_in_call_order() {
        let sp_mock = mock(0.0);
        let (sp, rb) = (signal(&sp_mock, "sp"), signal(&mock(0.0), "motor"));
        let blocked = sp_mock.mock_puts_blocked();

        // No yield between the two calls: the puts must already be sent when
        // `move_status` returns, not left to spawned tasks.
        let halt = Halt::new();
        let a = move_status(axis(&sp, &rb, &halt), 1.0, "t", put_done).await;
        assert_eq!(sp_mock.put_calls(), vec![Some(1.0)]);
        let b = move_status(axis(&sp, &rb, &halt), 2.0, "t", put_done).await;
        assert_eq!(sp_mock.put_calls(), vec![Some(1.0), Some(2.0)]);
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
        let sp_mock = mock(0.0);
        let (sp, rb) = (signal(&sp_mock, "sp"), signal(&mock(0.0), "motor"));
        let _blocked = sp_mock.mock_puts_blocked();
        let halt = Halt::new();
        let settled = |st: Status| async move {
            tokio::time::timeout(Duration::from_secs(1), st)
                .await
                .expect("the stop settles the move")
        };

        let st = move_status(axis(&sp, &rb, &halt), 1.0, "t", put_done).await;
        halt.stop(false);
        let err = settled(st).await.expect_err("stop(false) fails the move");
        assert!(err.to_string().contains("t: stopped"), "{err}");

        let st = move_status(axis(&sp, &rb, &halt), 2.0, "t", put_done).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!st.done_state(), "an earlier stop settled a later move");
        halt.stop(true);
        settled(st).await.expect("stop(true) completes the move");
    }

    /// The next watcher update of `rx` within a second.
    async fn next_update(rx: &mut watch::Receiver<Option<WatcherUpdate>>) -> WatcherUpdate {
        tokio::time::timeout(Duration::from_secs(1), rx.changed())
            .await
            .expect("a watcher update")
            .unwrap();
        rx.borrow_and_update().clone().unwrap()
    }

    /// ophyd-async `test_motor_moving_well`: the move reports where it
    /// stands, then each readback, with the name, the readback's units and
    /// the time elapsed, while the put is pending.
    #[tokio::test]
    async fn a_move_reports_each_readback_until_it_settles() {
        let sp_mock = mock(0.0);
        let rb_mock = MockSignalBackend::new(
            crate::backends::soft::SoftSignalBackend::new(0.0).with_units("mm"),
        );
        let (sp, rb) = (signal(&sp_mock, "sp"), signal(&rb_mock, "motor"));
        // A CA or PVA monitor delivers the current value as it opens; the
        // soft backend does not, so give the cache one through a listener.
        let _listener = AsyncSubscribable::subscribe(&rb).await.unwrap();
        rb_mock.set_value(0.0);
        let halt = Halt::new();
        let blocked = sp_mock.mock_puts_blocked();

        let st = move_status(axis(&sp, &rb, &halt), 0.55, "t", put_done).await;
        let mut rx = st.watch_updates();
        let expected = |current| WatcherUpdate {
            name: Some("motor".into()),
            unit: Some("mm".into()),
            ..WatcherUpdate::new(current, 0.0, 0.55)
        };
        let first = next_update(&mut rx).await;
        assert!(first.time_elapsed.unwrap() < 0.2, "{first:?}");
        assert_eq!(
            WatcherUpdate {
                time_elapsed: None,
                ..first
            },
            expected(0.0)
        );
        assert_eq!(sp_mock.put_calls(), vec![Some(0.55)]);
        assert!(!st.done_state());

        tokio::time::sleep(Duration::from_millis(200)).await;
        rb_mock.set_value(0.1);
        let next = next_update(&mut rx).await;
        let elapsed = next.time_elapsed.unwrap();
        assert!((0.2..0.4).contains(&elapsed), "{next:?}");
        assert_eq!(
            WatcherUpdate {
                time_elapsed: None,
                ..next
            },
            expected(0.1)
        );

        drop(blocked);
        tokio::time::timeout(Duration::from_secs(1), st)
            .await
            .unwrap()
            .unwrap();
    }
}
