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

use bsrs::callbacks::CapturingSink;
use bsrs::core::msg::{LocatableObj, MovableObj, ReadableObj};
use bsrs::event_model::PerObjectHint;
use bsrs::host::ca_devices::{CaDetector, CaMotor};
use bsrs::prelude::*;

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
