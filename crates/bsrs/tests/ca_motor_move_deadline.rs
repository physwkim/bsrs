//! `CaMotor::set` bounds a move as ophyd-async's `Motor.calculate_timeout`
//! does: `|Δ/VELO| + 2·ACCL + 10 s`, from the readback at the time of the
//! set. Each setpoint's FLNK is a calcout with an 11 s ODLY, so its
//! put-callback lasts 11 s; `.VELO`/`.ACCL` are simple PVs named as the
//! motor record's fields.
//!
//! The only test in this binary: the CA client is resolved once per
//! process, after the environment below is set.
#![cfg(all(feature = "ca", feature = "host"))]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::core::msg::MovableObj;
use bsrs::host::ca_devices::CaMotor;
use bsrs::protocols_async::SignalBackend;
use epics_ca_rs::server::CaServer;
use epics_ca_rs::EpicsValue;

const DB: &str = r#"
record(ao, "SHORT") { field(FLNK, "SHORT_D") }
record(calcout, "SHORT_D") { field(ODLY, "11") field(CALC, "1") }
record(ao, "LONG") { field(FLNK, "LONG_D") }
record(calcout, "LONG_D") { field(ODLY, "11") field(CALC, "1") }
record(ao, "STILL") {}
record(ao, "PLAIN") {}
"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_move_is_bounded_by_its_velocity_and_acceleration() {
    let pvs = [
        ("SHORT.VELO", 1.0),
        ("SHORT.ACCL", 0.0),
        ("SHORT_RBV", 0.0),
        ("LONG.VELO", 1.0),
        ("LONG.ACCL", 0.5),
        ("LONG_RBV", 0.0),
        ("STILL.VELO", 0.0),
        ("STILL.ACCL", 0.0),
        ("STILL_RBV", 0.0),
        ("PLAIN_RBV", 0.0),
    ];
    let mut builder = CaServer::builder()
        .port(0)
        .db_string(DB, &HashMap::new())
        .unwrap();
    for (name, v) in pvs {
        builder = builder.pv(name, EpicsValue::Double(v));
    }
    let server = builder.build().await.expect("CA server");
    let port = server.tcp_port();
    tokio::spawn(async move { server.run().await });
    // SAFETY: no other thread reads the environment yet.
    unsafe {
        std::env::set_var("EPICS_CA_NAME_SERVERS", format!("127.0.0.1:{port}"));
        std::env::set_var("EPICS_CA_ADDR_LIST", "");
        std::env::set_var("EPICS_CA_AUTO_ADDR_LIST", "NO");
    }

    // 0.5 s of travel: a 10.5 s deadline, short of the 11 s callback.
    let short = async {
        let m = CaMotor::connect_async("short", "SHORT.VAL", "SHORT_RBV")
            .await
            .unwrap();
        let started = Instant::now();
        let err = m.set_dyn(0.5).await.await.expect_err("past the deadline");
        (err.to_string(), started.elapsed())
    };
    // 1 s of travel and 2 × 0.5 s of acceleration: a 12 s deadline.
    let long = async {
        let m = CaMotor::connect_async("long", "LONG.VAL", "LONG_RBV")
            .await
            .unwrap();
        let started = Instant::now();
        m.set_dyn(1.0).await.await.expect("within the deadline");
        started.elapsed()
    };
    let still = async {
        let m = CaMotor::connect_async("still", "STILL.VAL", "STILL_RBV")
            .await
            .unwrap();
        let err = m.set_dyn(3.0).await.await.expect_err("zero velocity");
        let sp = EpicsCaBackend::<f64>::new("STILL");
        sp.connect(Duration::from_secs(5)).await.unwrap();
        (err.to_string(), sp.get_value().await.unwrap())
    };
    // An ao has no .VELO/.ACCL: the motor connects and moves undeadlined.
    let plain = async {
        let m = CaMotor::connect_async("plain", "PLAIN", "PLAIN_RBV")
            .await
            .unwrap();
        m.set_dyn(2.0).await.await
    };
    let ((short_err, short_t), long_t, (still_err, still_sp), plain) =
        tokio::join!(short, long, still, plain);

    assert!(short_err.contains("timed out after 10.500s"), "{short_err}");
    assert!(
        short_t >= Duration::from_millis(10_400) && short_t < Duration::from_millis(10_900),
        "{short_t:?}"
    );
    assert!(long_t >= Duration::from_millis(10_900), "{long_t:?}");
    assert!(still_err.contains("has zero velocity"), "{still_err}");
    assert_eq!(still_sp, 0.0, "a zero-velocity move must not be put");
    plain.unwrap();
}
