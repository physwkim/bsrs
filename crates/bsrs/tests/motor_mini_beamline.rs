//! Motor plans and `CaPositioner` on the mini-beamline's motor records, held
//! to what bluesky plans do with ophyd's `EpicsMotor` (test_sim.py), and to
//! ophyd's `PVPositionerPC` and `PositionerBase.stop`
//! (test_pvpositioner.py).
//!
//! Autosave restores the motors' last positions, so no test assumes where a
//! motor starts; under `cargo test` each test owns its motors.
#![cfg(all(feature = "ca", feature = "pva", feature = "host"))]

mod example_ioc;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::callbacks::CapturingSink;
use bsrs::core::msg::{LocatableObj, MovableObj, ReadableObj, StoppableObj};
use bsrs::core::{Watcher, WatcherUpdate};
use bsrs::devices::BackendFromPv;
use bsrs::event_model::PerObjectHint;
use bsrs::host::ca_devices::{CaDetector, CaMotor, CaPositioner};
use bsrs::prelude::*;
use bsrs::protocols_async::SignalBackend;

const BIN: &str = "mini-ioc";
const EXAMPLE: &str = "mini-beamline";
const MRES: f64 = 0.001;

fn field(record: &str, f: &str) -> String {
    format!("{record}.{f}")
}

async fn ca_motor(name: &str, record: &str) -> Arc<CaMotor> {
    CaMotor::connect_async(name, &field(record, "VAL"), &field(record, "RBV"))
        .await
        .unwrap()
}

async fn ca<T>(pv: &str) -> EpicsCaBackend<T>
where
    T: Clone + Send + Sync + 'static,
    EpicsCaBackend<T>: SignalBackend<T> + BackendFromPv,
{
    let b = EpicsCaBackend::from_pv(pv);
    b.connect(Duration::from_secs(10)).await.unwrap();
    b
}

fn events(docs: &[Document]) -> Vec<HashMap<String, serde_json::Value>> {
    docs.iter()
        .filter_map(|d| match d {
            Document::Event(e) => Some(e.data.clone()),
            _ => None,
        })
        .collect()
}

fn num(data: &HashMap<String, serde_json::Value>, key: &str) -> f64 {
    data[key]
        .as_f64()
        .unwrap_or_else(|| panic!("{key}: {data:?}"))
}

/// test_sim.py `test_synaxis_describe` (`bp.scan([], motor1, ...)`) and
/// test_epicsmotor.py `test_hints`, on a real motor: each point reads the
/// motor where the scan put it, the descriptor hints the motor and the
/// detector, and `rel_scan` returns the motor to where it started.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn rel_scan_reads_the_hinted_motor_and_returns_it() {
    example_ioc::boot(BIN, EXAMPLE);
    let m = ca_motor("ph", "mini:ph:mtr").await;
    let det = CaDetector::connect_async("ph_det", "mini:ph:DetValue_RBV")
        .await
        .unwrap();
    let here = m.locate_dyn().await.unwrap().readback;
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let plan = bsrs::plans::rel_scan(
        vec![det as Arc<dyn ReadableObj>],
        m.clone() as Arc<dyn MovableObj>,
        m.clone() as Arc<dyn ReadableObj>,
        here,
        -0.5,
        0.5,
        3,
    );
    tokio::time::timeout(Duration::from_secs(60), re.run_async(plan))
        .await
        .expect("the scan ends")
        .unwrap();
    let docs = sink.snapshot().await;

    let evs = events(&docs);
    assert_eq!(evs.len(), 3);
    for (ev, d) in evs.iter().zip([-0.5, 0.0, 0.5]) {
        let at = num(ev, "ph");
        assert!((at - (here + d)).abs() <= MRES + 1e-9, "{at} for {d}");
        num(ev, "ph_det");
    }
    let hints = docs
        .iter()
        .find_map(|d| match d {
            Document::Descriptor(d) => d.hints.clone(),
            _ => None,
        })
        .expect("the descriptor carries hints");
    for name in ["ph", "ph_det"] {
        assert_eq!(
            hints.get(name),
            Some(&PerObjectHint {
                fields: Some(vec![name.to_string()]),
                NX_class: None,
            }),
            "{hints:?}"
        );
    }
    let back = m.locate_dyn().await.unwrap().readback;
    assert!((back - here).abs() <= MRES + 1e-9, "{back} != {here}");
}

/// ophyd-async test_sim_motor.py `test_move_sim_in_plan`: `spiral_square`
/// over a 4 × 4 square of 10 × 10 points leaves both motors at the corner
/// two below the center.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn spiral_square_leaves_both_motors_at_the_low_corner() {
    example_ioc::boot(BIN, EXAMPLE);
    let x = ca_motor("dot_x", "mini:dot:mtrx").await;
    let y = ca_motor("dot_y", "mini:dot:mtry").await;
    // Clear of the ±500 soft limits by more than the half-range.
    let cx = x.locate_dyn().await.unwrap().readback.clamp(-490.0, 490.0);
    let cy = y.locate_dyn().await.unwrap().readback.clamp(-490.0, 490.0);
    let sink = Arc::new(CapturingSink::new());
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let plan = bsrs::plans::spiral_square(
        vec![],
        x.clone() as Arc<dyn MovableObj>,
        x.clone() as Arc<dyn ReadableObj>,
        y.clone() as Arc<dyn MovableObj>,
        y.clone() as Arc<dyn ReadableObj>,
        cx,
        cy,
        4.0,
        4.0,
        10,
        10,
    );
    tokio::time::timeout(Duration::from_secs(300), re.run_async(plan))
        .await
        .expect("the scan ends")
        .unwrap();

    assert_eq!(events(&sink.snapshot().await).len(), 100);
    let (xe, ye) = (
        x.locate_dyn().await.unwrap().readback,
        y.locate_dyn().await.unwrap().readback,
    );
    assert!(
        (xe - (cx - 2.0)).abs() <= MRES + 1e-9,
        "x at {xe}, center {cx}"
    );
    assert!(
        (ye - (cy - 2.0)).abs() <= MRES + 1e-9,
        "y at {ye}, center {cy}"
    );
}

/// `d` away from `from`, toward the middle of the ±100 travel.
fn inward(from: f64, d: f64) -> f64 {
    if from > 0.0 {
        from - d
    } else {
        from + d
    }
}

/// Every watcher update of `st` until it settles, as ophyd-async's
/// `StatusWatcher` collects them.
fn watched(st: &Status) -> tokio::task::JoinHandle<Vec<WatcherUpdate>> {
    struct Collect(Vec<WatcherUpdate>);
    impl Watcher for Collect {
        fn watch(&mut self, update: &WatcherUpdate) {
            self.0.push(update.clone());
        }
    }
    let st = st.clone();
    tokio::spawn(async move {
        let mut c = Collect(Vec::new());
        st.observe_watcher(&mut c).await;
        c.0
    })
}

async fn slit_positioner() -> Arc<CaPositioner> {
    CaPositioner::connect_async(
        "slit",
        "mini:slit:mtr.VAL",
        "mini:slit:mtr.RBV",
        "mini:slit:mtr.MOVN",
        0.0,
    )
    .await
    .unwrap()
}

/// test_pvpositioner.py `test_put_complete_setpoint_readback_done`: a
/// positioner over VAL, RBV and MOVN (done at 0) refuses a target past the
/// setpoint's high limit, and every target while its limits are inverted,
/// but not a move from past a limit (`PVPositioner.check_value`); a move
/// settles once the put completes and MOVN reads done, with the readback at
/// the target; its watchers follow the readback meanwhile.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn a_positioner_refuses_a_target_past_the_limit_and_settles_on_done() {
    example_ioc::boot(BIN, EXAMPLE);
    let p = slit_positioner().await;
    let hlm = ca::<f64>("mini:slit:mtr.HLM")
        .await
        .get_value()
        .await
        .unwrap();
    let before = p.locate_dyn().await.unwrap().setpoint;

    let res = tokio::time::timeout(Duration::from_secs(5), p.set_dyn(hlm + 1.0).await)
        .await
        .expect("refused at once");
    assert!(res.is_err(), "a move past HLM {hlm} succeeded");
    assert_eq!(p.locate_dyn().await.unwrap().setpoint, before);

    let llm = ca::<f64>("mini:slit:mtr.LLM")
        .await
        .get_value()
        .await
        .unwrap();
    let hlm_pv = ca::<f64>("mini:slit:mtr.HLM").await;
    hlm_pv.put(Some(llm - 1.0)).await.unwrap();
    let inverted = hlm_pv.get_value().await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(5), p.set_dyn(before).await)
        .await
        .expect("refused at once");
    hlm_pv.put(Some(hlm)).await.unwrap();
    assert!(inverted < llm, "HLM {inverted}, LLM {llm}");
    assert!(
        res.is_err(),
        "a move with HLM {inverted} < LLM {llm} succeeded"
    );

    let pos = p.locate_dyn().await.unwrap().readback;
    let (f, inside) = if pos > 0.0 {
        ("HLM", pos - 0.2)
    } else {
        ("LLM", pos + 0.2)
    };
    let pulled = ca::<f64>(&format!("mini:slit:mtr.{f}")).await;
    let limit = pulled.get_value().await.unwrap();
    pulled.put(Some(inward(pos, 0.1))).await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(25), p.set_dyn(inside).await)
        .await
        .expect("within ophyd's 25 s");
    pulled.put(Some(limit)).await.unwrap();
    res.unwrap_or_else(|e| panic!("a move from {pos} past .{f} back inside it: {e}"));

    let start = p.locate_dyn().await.unwrap().readback;
    let target = inward(start, 1.0);
    let key = p.describe_dyn().await.unwrap().remove("slit").unwrap();
    let st = p.set_dyn(target).await;
    let updates = watched(&st);
    assert!(!st.done_state(), "settled before the motor moved");
    tokio::time::timeout(Duration::from_secs(25), st)
        .await
        .expect("within ophyd's 25 s")
        .expect("the move succeeds");
    // ophyd `MoveStatus` watchers: the positioner's name, where the move
    // started and is going, and its readback as the motor travels.
    let updates = updates.await.unwrap();
    assert!(updates.len() > 1, "{updates:?}");
    assert!(
        (updates[0].current - start).abs() <= MRES + 1e-9,
        "{updates:?}"
    );
    for u in &updates {
        assert_eq!(u.name.as_deref(), Some("slit"), "{u:?}");
        assert_eq!(u.unit, key.units, "{u:?}");
        assert_eq!((u.initial, u.target), (start, target), "{u:?}");
    }
    let movn = ca::<i64>("mini:slit:mtr.MOVN")
        .await
        .get_value()
        .await
        .unwrap();
    assert_eq!(movn, 0);
    let rb = p.locate_dyn().await.unwrap().readback;
    assert!((rb - target).abs() <= MRES + 1e-9, "{rb}");
}

/// test_pvpositioner.py `test_hints`: the positioner hints its readback,
/// under the positioner's own name, and every hinted field is described.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn a_positioner_hints_its_described_readback() {
    example_ioc::boot(BIN, EXAMPLE);
    let p = slit_positioner().await;
    let desc = p.describe_dyn().await.unwrap();
    let hints = p.hint_fields().unwrap();
    assert_eq!(hints, ["slit"]);
    for k in &hints {
        assert!(desc.contains_key(k), "{k} not in {:?}", desc.keys());
    }
}

/// ophyd `PositionerBase.stop` (test_pvpositioner.py `test_pvpos`
/// `m.stop()`): a positioner with no stop PV ends its move on `stop`, failed
/// for `success=False`, done for `success=True`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn stop_ends_a_positioners_move() {
    example_ioc::boot(BIN, EXAMPLE);
    let p = CaPositioner::connect_async(
        "edge",
        "mini:edge:mtr.VAL",
        "mini:edge:mtr.RBV",
        "mini:edge:mtr.MOVN",
        0.0,
    )
    .await
    .unwrap();

    for success in [false, true] {
        let start = p.locate_dyn().await.unwrap().readback;
        let st = p.set_dyn(inward(start, 3.0)).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        p.stop_dyn(success).await.unwrap();
        let res = tokio::time::timeout(Duration::from_secs(1), st)
            .await
            .expect("stop ends the move");
        assert_eq!(res.is_ok(), success, "{res:?}");
        // The record carries on to the target; let it get there.
        let done = ca::<i64>("mini:edge:mtr.DMOV").await;
        tokio::time::timeout(Duration::from_secs(15), async {
            while done.get_value().await.unwrap() != 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the motor finishes");
    }
}
