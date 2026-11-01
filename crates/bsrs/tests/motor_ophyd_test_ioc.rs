//! `CaMotor` and `PvaMotor` on the motor records ophyd-test-ioc serves, held
//! to what ophyd's `EpicsMotor` (test_epicsmotor.py) and ophyd-async's
//! `Motor` (epics/test_motor.py) do with them. Every record has VELO 1,
//! ACCL 0.5, MRES 0.001, PREC 4, soft limits ±100 and no EGU.
//!
//! Under `cargo test` the tests share one IOC, so each owns its motors and
//! none assumes where its motor starts.
#![cfg(all(feature = "ca", feature = "pva", feature = "host"))]

mod example_ioc;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::core::msg::{LocatableObj, MonitorableObj, MovableObj, ReadableObj, StoppableObj};
use bsrs::core::runtime::bsrs_runtime;
use bsrs::core::subscription::Subscription;
use bsrs::core::Status;
use bsrs::event_model::LimitsRange;
use bsrs::host::ca_devices::{CaDetector, CaMotor};
use bsrs::host::pva_devices::{PvaDetector, PvaMotor};
use bsrs::protocols_async::SignalBackend;

const BIN: &str = "ophyd-test-ioc";
const MRES: f64 = 0.001;

const X1: &str = "XF:31IDA-OP{Tbl-Ax:X1}Mtr";
const X2: &str = "XF:31IDA-OP{Tbl-Ax:X2}Mtr";
const X3: &str = "XF:31IDA-OP{Tbl-Ax:X3}Mtr";
const X4: &str = "XF:31IDA-OP{Tbl-Ax:X4}Mtr";
const X5: &str = "XF:31IDA-OP{Tbl-Ax:X5}Mtr";
const X6: &str = "XF:31IDA-OP{Tbl-Ax:X6}Mtr";
const FAKE: &str = "XF:31IDA-OP{Tbl-Ax:FakeMtr}";
const SIM1: &str = "sim:mtr1";
const SIM2: &str = "sim:mtr2";

fn field(record: &str, f: &str) -> String {
    format!("{record}.{f}")
}

async fn ca_motor(name: &str, record: &str) -> Arc<CaMotor> {
    CaMotor::connect_async(name, &field(record, "VAL"), &field(record, "RBV"))
        .await
        .unwrap()
}

fn pva_motor(name: &str, record: &str) -> Arc<PvaMotor> {
    PvaMotor::connect_blocking(name, &field(record, "VAL"), &field(record, "RBV")).unwrap()
}

async fn ca<T>(record: &str, f: &str) -> EpicsCaBackend<T>
where
    T: Clone + Send + Sync + 'static,
    EpicsCaBackend<T>: SignalBackend<T>,
{
    let b = EpicsCaBackend::new(field(record, f));
    b.connect(Duration::from_secs(10)).await.unwrap();
    b
}

/// `d` away from `from`, toward the middle of the ±100 travel.
fn inward(from: f64, d: f64) -> f64 {
    if from > 0.0 {
        from - d
    } else {
        from + d
    }
}

/// Wait until the subscribed readback satisfies `pred`.
async fn readback_until(sub: &Subscription, pred: impl Fn(f64) -> bool) {
    let mut rx = sub.clone_rx();
    tokio::time::timeout(
        Duration::from_secs(10),
        rx.wait_for(|r| r.value.as_f64().is_some_and(&pred)),
    )
    .await
    .expect("the readback gets there")
    .unwrap();
}

/// Wait until `.DMOV` reads done, from its monitor.
async fn done_moving(record: &str) {
    let dmov = CaDetector::connect_async("dmov", &field(record, "DMOV"))
        .await
        .unwrap();
    let sub = dmov.subscribe_dyn().await.unwrap();
    readback_until(&sub, |v| v == 1.0).await;
}

/// test_epicsmotor.py `test_move`, `test_read_in_motor_callback`; ophyd-async
/// `test_motor_moving_well`: the status is pending while the motor travels,
/// settles once it is done, and the readback passes through the travel to
/// land within MRES of the target. A move to where it stands settles at once.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn a_move_settles_once_the_motor_is_done() {
    example_ioc::boot(BIN, BIN);
    let m = ca_motor("x1", X1).await;
    let dmov = ca::<f64>(X1, "DMOV").await;
    let start = m.locate_dyn().await.unwrap().readback;
    let target = inward(start, 1.5);
    let sub = m.subscribe_dyn().await.unwrap();
    let mut rx = sub.clone_rx();
    rx.borrow_and_update();
    let seen = tokio::spawn(async move {
        let mut seen = Vec::new();
        while rx.changed().await.is_ok() {
            let v = rx.borrow_and_update().value.as_f64().unwrap();
            seen.push(v);
            if (v - target).abs() <= MRES {
                break;
            }
        }
        seen
    });

    let started = Instant::now();
    let st = m.set_dyn(target).await;
    assert!(!st.done_state(), "settled before the motor moved");
    tokio::time::timeout(Duration::from_secs(20), st)
        .await
        .expect("within the deadline")
        .expect("the move succeeds");
    let took = started.elapsed();

    // 1.5 s of travel at VELO 1.
    assert!(took >= Duration::from_millis(1_400), "{took:?}");
    assert_eq!(dmov.get_value().await.unwrap(), 1.0);
    let at = m.locate_dyn().await.unwrap();
    assert!((at.setpoint - target).abs() < 1e-9, "{at:?}");
    assert!((at.readback - target).abs() <= MRES + 1e-9, "{at:?}");
    let seen = tokio::time::timeout(Duration::from_secs(5), seen)
        .await
        .expect("the readback reached the target")
        .unwrap();
    let (lo, hi) = (start.min(target), start.max(target));
    assert!(
        seen.iter().any(|&v| v > lo + MRES && v < hi - MRES),
        "no readback between {start} and {target}: {seen:?}"
    );

    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), m.set_dyn(target).await)
        .await
        .expect("the null move settles")
        .expect("the null move succeeds");
    assert!(started.elapsed() < Duration::from_secs(1));
}

/// Start a long move and wait until the motor is underway.
async fn underway(m: &CaMotor) -> (Status, f64, Subscription) {
    let start = m.locate_dyn().await.unwrap().readback;
    let target = inward(start, 8.0);
    let sub = m.subscribe_dyn().await.unwrap();
    let st = m.set_dyn(target).await;
    readback_until(&sub, |v| (v - start).abs() >= 0.5).await;
    (st, target, sub)
}

/// The motor halted short of `target` and its move is settled.
async fn halted_short(m: &CaMotor, record: &str, target: f64) {
    tokio::time::timeout(Duration::from_secs(3), done_moving(record))
        .await
        .expect("the motor halts once stopped");
    let rb = m.locate_dyn().await.unwrap().readback;
    assert!((rb - target).abs() > 4.0, "the motor ran on to {rb}");
}

/// test_epicsmotor.py `test_move` (`motor.stop()`); ophyd-async
/// `test_motor_moving_stopped`: `stop(success=False)` writes `.STOP`, the
/// motor halts, and the move fails.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn stop_halts_the_motor_and_fails_its_move() {
    example_ioc::boot(BIN, BIN);
    let m = ca_motor("x2", X2).await;
    let (st, target, _sub) = underway(&m).await;
    m.stop_dyn(false).await.unwrap();
    let err = tokio::time::timeout(Duration::from_secs(3), st)
        .await
        .expect("the move settles once stopped")
        .expect_err("a move stopped with success=false fails");
    assert!(err.to_string().contains("stopped"), "{err}");
    halted_short(&m, X2, target).await;
}

/// ophyd-async `StandardMovable.stop(success=True)`, which bluesky sends on
/// pause and at the end of a run (`stop_on_pause`): the motor halts and its
/// move completes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn stop_with_success_halts_the_motor_and_completes_its_move() {
    example_ioc::boot(BIN, BIN);
    let m = ca_motor("x3", X3).await;

    let (st, target, _sub) = underway(&m).await;
    m.stop_dyn(true).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), st)
        .await
        .expect("the move settles once stopped")
        .expect("a move stopped with success=true completes");
    halted_short(&m, X3, target).await;

    let (st, target, _sub) = underway(&m).await;
    MovableObj::stop_on_pause(m.as_ref(), true).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), st)
        .await
        .expect("the move settles once the engine stops it")
        .expect("the engine's stop completes the move");
    halted_short(&m, X3, target).await;
}

/// test_epicsmotor.py `test_limits`/`check_value`; ophyd-async
/// `test_move_outside_motor_limits_causes_error`: a target past a soft limit
/// is refused and the setpoint stays. ophyd-async
/// `test_given_limits_of_0_0_then_move_causes_no_error`: dial limits 0, 0
/// mean no limits.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn a_move_past_a_soft_limit_is_refused() {
    example_ioc::boot(BIN, BIN);
    let m = ca_motor("x4", X4).await;
    let (hlm, llm) = (ca::<f64>(X4, "HLM").await, ca::<f64>(X4, "LLM").await);
    let (hlm, llm) = (
        hlm.get_value().await.unwrap(),
        llm.get_value().await.unwrap(),
    );
    let before = m.locate_dyn().await.unwrap().setpoint;

    for target in [hlm + 1.0, llm - 1.0] {
        let res = tokio::time::timeout(Duration::from_secs(5), m.set_dyn(target).await)
            .await
            .expect("refused at once");
        assert!(
            res.is_err(),
            "a move to {target} past the limits [{llm}, {hlm}] succeeded"
        );
        assert_eq!(m.locate_dyn().await.unwrap().setpoint, before);
    }

    for f in ["DHLM", "DLLM"] {
        ca::<f64>(X4, f).await.put(Some(0.0)).await.unwrap();
    }
    let start = m.locate_dyn().await.unwrap().readback;
    let target = inward(start, 0.5);
    tokio::time::timeout(Duration::from_secs(15), m.set_dyn(target).await)
        .await
        .expect("within the deadline")
        .expect("no limits, no refusal");
    let rb = m.locate_dyn().await.unwrap().readback;
    assert!((rb - target).abs() <= MRES + 1e-9, "{rb}");
}

/// test_epicsmotor.py `test_hints`; ophyd-async `Motor` (`user_readback` is
/// HINTED) and `test_read_motor` (`motor_egu`): the readback is the motor's
/// hinted field, described with the record's EGU, PREC and soft limits.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn describe_carries_the_records_egu_precision_limits_and_hint() {
    example_ioc::boot(BIN, BIN);
    ca::<String>(X5, "EGU")
        .await
        .put(Some("mm".into()))
        .await
        .unwrap();
    let (hlm, llm) = (ca::<f64>(X5, "HLM").await, ca::<f64>(X5, "LLM").await);
    let (hlm, llm) = (
        hlm.get_value().await.unwrap(),
        llm.get_value().await.unwrap(),
    );
    let m = ca_motor("x5", X5).await;

    let dk = m.describe_dyn().await.unwrap().remove("x5").unwrap();
    assert_eq!(dk.source, format!("ca://{X5}.RBV"));
    assert_eq!(dk.units.as_deref(), Some("mm"));
    assert_eq!(dk.precision, Some(4));
    let limits = dk.limits.expect("the record's limits");
    let travel = Some(LimitsRange {
        low: Some(llm),
        high: Some(hlm),
    });
    assert_eq!(limits.control, travel);
    assert_eq!(limits.display, travel);
    assert_eq!(m.hint_fields(), Some(vec!["x5".to_string()]));
}

/// test_epicsmotor.py `test_calibration`: with `.SET` at Set, a move
/// redefines the user position without motion (`set_current_position`), the
/// offset takes the difference, and later moves land in the new user
/// coordinates.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn a_move_in_set_mode_recalibrates_the_user_position() {
    example_ioc::boot(BIN, BIN);
    let m = ca_motor("x6", X6).await;
    let set = ca::<i64>(X6, "SET").await;
    let off = ca::<f64>(X6, "OFF").await;
    let drbv = ca::<f64>(X6, "DRBV").await;
    let old = m.locate_dyn().await.unwrap().readback;
    let (off0, dial) = (
        off.get_value().await.unwrap(),
        drbv.get_value().await.unwrap(),
    );
    let pos = inward(old, 10.0);

    set.put(Some(1)).await.unwrap();
    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), m.set_dyn(pos).await)
        .await
        .expect("settles without motion")
        .expect("the calibration succeeds");
    assert!(started.elapsed() < Duration::from_secs(1));
    set.put(Some(0)).await.unwrap();

    let at = m.locate_dyn().await.unwrap();
    assert!((at.setpoint - pos).abs() < 1e-9, "{at:?}");
    assert!((at.readback - pos).abs() <= MRES + 1e-9, "{at:?}");
    let shift = off.get_value().await.unwrap() - off0;
    assert!(
        (shift - (pos - old)).abs() <= MRES + 1e-9,
        "offset moved {shift}"
    );
    assert_eq!(drbv.get_value().await.unwrap(), dial);

    let next = inward(pos, 0.5);
    tokio::time::timeout(Duration::from_secs(15), m.set_dyn(next).await)
        .await
        .expect("within the deadline")
        .expect("the move succeeds");
    assert!((m.locate_dyn().await.unwrap().readback - next).abs() <= MRES + 1e-9);
    let travelled = drbv.get_value().await.unwrap() - dial;
    assert!(
        (travelled - (next - pos)).abs() <= MRES + 1e-9,
        "{travelled}"
    );
}

/// ophyd-async `Motor.calculate_timeout` (`test_motor_move_timeout`): a move
/// is bounded by `|Δ/VELO| + 2·ACCL + 10 s` from the record's own VELO and
/// ACCL. SPMG at Pause holds the put-callback the motor would answer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn a_held_move_times_out_on_the_records_velocity_and_acceleration() {
    example_ioc::boot(BIN, BIN);
    let m = ca_motor("fake", FAKE).await;
    let spmg = ca::<i64>(FAKE, "SPMG").await;
    ca::<f64>(FAKE, "VELO").await.put(Some(2.0)).await.unwrap();
    let start = m.locate_dyn().await.unwrap().readback;
    spmg.put(Some(1)).await.unwrap();

    let started = Instant::now();
    let res = tokio::time::timeout(Duration::from_secs(20), m.set_dyn(inward(start, 0.5)).await)
        .await
        .expect("the move's own deadline ends it");
    let took = started.elapsed();
    // The held VAL put-callback owns the record, so a put-callback to SPMG
    // would queue behind it (dbNotify): Go goes as a plain put.
    let client = epics_ca_rs::client::CaClient::new().await.unwrap();
    let go = client.create_channel(field(FAKE, "SPMG"));
    go.wait_connected(Duration::from_secs(5)).await.unwrap();
    go.put_nowait(&epics_ca_rs::EpicsValue::Enum(3))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), done_moving(FAKE))
        .await
        .expect("Go ends the held move");

    // 0.5 / 2 + 2 × 0.5 + 10.
    let err = res.expect_err("a held move times out");
    assert!(err.to_string().contains("timed out after 11.250s"), "{err}");
    assert!(took >= Duration::from_millis(11_200), "{took:?}");
    assert!(took < Duration::from_secs(13), "{took:?}");
}

/// test_epicsmotor.py `test_move` and `motor.stop()` over PVA: the
/// `block=true` put settles once the motor is done, and `stop` halts the
/// motor and fails its move.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_pva_move_settles_once_done_and_stop_halts_it() {
    example_ioc::boot(BIN, BIN);
    let m = pva_motor("sim_mtr1", SIM1);
    bsrs_runtime().block_on(async {
        let start = m.locate_dyn().await.unwrap().readback;
        let target = inward(start, 1.0);
        let started = Instant::now();
        let st = m.set_dyn(target).await;
        assert!(!st.done_state(), "settled before the motor moved");
        tokio::time::timeout(Duration::from_secs(20), st)
            .await
            .expect("within the deadline")
            .expect("the move succeeds");
        assert!(started.elapsed() >= Duration::from_millis(900));
        let rb = m.locate_dyn().await.unwrap().readback;
        assert!((rb - target).abs() <= MRES + 1e-9, "{rb}");

        let start = rb;
        let target = inward(start, 8.0);
        let sub = m.subscribe_dyn().await.unwrap();
        let st = m.set_dyn(target).await;
        readback_until(&sub, |v| (v - start).abs() >= 0.5).await;
        m.stop_dyn(false).await.unwrap();
        let err = tokio::time::timeout(Duration::from_secs(3), st)
            .await
            .expect("the move settles once stopped")
            .expect_err("a move stopped with success=false fails");
        assert!(err.to_string().contains("stopped"), "{err}");
        tokio::time::timeout(Duration::from_secs(3), done_moving(SIM1))
            .await
            .expect("the motor halts once stopped");
        let rb = m.locate_dyn().await.unwrap().readback;
        assert!((rb - target).abs() > 4.0, "the motor ran on to {rb}");
    });
}

/// test_epicsmotor.py `test_hints` and ophyd-async `test_read_motor` over
/// PVA: the readback is described from its own PV, with the record's EGU,
/// PREC and soft limits, and is the hinted field; a detector's value is
/// described from its PV too.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_describe_carries_the_records_egu_precision_limits_and_hint() {
    example_ioc::boot(BIN, BIN);
    let (hlm, llm) = bsrs_runtime().block_on(async {
        ca::<String>(SIM2, "EGU")
            .await
            .put(Some("mm".into()))
            .await
            .unwrap();
        let (hlm, llm) = (ca::<f64>(SIM2, "HLM").await, ca::<f64>(SIM2, "LLM").await);
        (
            hlm.get_value().await.unwrap(),
            llm.get_value().await.unwrap(),
        )
    });
    let m = pva_motor("sim_mtr2", SIM2);

    let dk = bsrs_runtime()
        .block_on(m.describe_dyn())
        .unwrap()
        .remove("sim_mtr2")
        .unwrap();
    assert_eq!(dk.source, format!("pva://{SIM2}.RBV"));
    assert_eq!(dk.units.as_deref(), Some("mm"));
    assert_eq!(dk.precision, Some(4));
    let limits = dk.limits.expect("the record's limits");
    let travel = Some(LimitsRange {
        low: Some(llm),
        high: Some(hlm),
    });
    assert_eq!(limits.control, travel);
    assert_eq!(limits.display, travel);
    assert_eq!(m.hint_fields(), Some(vec!["sim_mtr2".to_string()]));

    let det = PvaDetector::connect_blocking("sim_mtr2_rbv", &field(SIM2, "RBV")).unwrap();
    let dk = bsrs_runtime()
        .block_on(det.describe_dyn())
        .unwrap()
        .remove("sim_mtr2_rbv")
        .unwrap();
    assert_eq!(dk.source, format!("pva://{SIM2}.RBV"));
    assert_eq!(dk.units.as_deref(), Some("mm"));
}
