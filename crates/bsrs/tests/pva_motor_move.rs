//! `PvaMotor::set` returns its status once the put is sent, and the settle
//! the put waits for (`record[block=true]`) completes it, bounded as
//! ophyd-async's `Motor.calculate_timeout` bounds it: `|Δ/VELO| + 2·ACCL +
//! 10 s`, from the readback at the time of the set. `.VELO`/`.ACCL` are
//! simple PVs named as the motor record's fields.
//!
//! M's FLNK is a calcout with a 2 s ODLY whose output is "On Change", so a
//! move takes 2 s and a move to where M stands completes in the same
//! processing, as a motor record's null move does; M has no `.VELO`, so its
//! moves have no deadline. SHORT's and LONG's FLNK is a calcout with an
//! 11 s ODLY, a put that outlasts the client's 5 s op timeout.
//!
//! The only test in this binary: the PVA client is resolved once per
//! process, after the environment below is set.
#![cfg(all(feature = "pva", feature = "host"))]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::core::msg::MovableObj;
use bsrs::core::runtime::bsrs_runtime;
use bsrs::host::pva_devices::PvaMotor;
use bsrs::protocols_async::SignalBackend;
use epics_pva_rs::server::PvaServer;
use epics_pva_rs::EpicsValue;
use tokio::sync::watch;

const DB: &str = r#"
record(ao, "M") { field(FLNK, "M_D") }
record(calcout, "M_D") {
    field(INPA, "M") field(CALC, "A") field(OOPT, "On Change") field(ODLY, "2")
}
record(ao, "M_RBV") {}
record(ao, "SHORT") { field(FLNK, "SHORT_D") }
record(calcout, "SHORT_D") { field(ODLY, "11") field(CALC, "1") }
record(ao, "LONG") { field(FLNK, "LONG_D") }
record(calcout, "LONG_D") { field(ODLY, "11") field(CALC, "1") }
record(ao, "STILL") {}
"#;

/// Serve `DB` on an ephemeral port and point the PVA client at it.
async fn serve() {
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
    ];
    // SAFETY: no other thread reads the environment yet.
    unsafe {
        std::env::set_var("EPICS_PVAS_INTF_ADDR_LIST", "127.0.0.1");
        std::env::set_var("EPICS_PVAS_BROADCAST_PORT", "0");
    }
    let mut builder = PvaServer::builder()
        .port(0)
        .db_string(DB, &HashMap::new())
        .unwrap();
    for (name, v) in pvs {
        builder = builder.pv(name, EpicsValue::Double(v));
    }
    let server = builder.build().await.expect("PVA server");
    let (tx, mut rx) = watch::channel(None);
    tokio::spawn(async move { server.run_reporting(Some(tx)).await });
    let port = loop {
        if let Some(h) = rx.borrow_and_update().clone() {
            break h.bound_tcp_port();
        }
        rx.changed().await.expect("server reports its port");
    };
    // SAFETY: as above; the client has not been built yet.
    unsafe {
        std::env::set_var("EPICS_PVA_NAME_SERVERS", format!("127.0.0.1:{port}"));
        std::env::set_var("EPICS_PVA_ADDR_LIST", "");
        std::env::set_var("EPICS_PVA_AUTO_ADDR_LIST", "NO");
    }
}

#[test]
fn a_move_settles_at_its_put_completion_within_its_deadline() {
    let rt = bsrs_runtime();
    rt.block_on(serve());
    let [m, short, long, still] = std::thread::scope(|s| {
        [
            ("m", "M", "M_RBV"),
            ("short", "SHORT.VAL", "SHORT_RBV"),
            ("long", "LONG.VAL", "LONG_RBV"),
            ("still", "STILL.VAL", "STILL_RBV"),
        ]
        .map(|(name, val, rbv)| s.spawn(move || PvaMotor::connect_blocking(name, val, rbv)))
        .map(|h| h.join().unwrap().unwrap())
    });

    let plain = async {
        let started = Instant::now();
        let moving = m.set_dyn(1.0).await;
        let returned = started.elapsed();
        let pending = !moving.done_state();
        moving.await.expect("the move settles");
        let moved = started.elapsed();

        let started = Instant::now();
        m.set_dyn(1.0).await.await.expect("the null move settles");
        (returned, pending, moved, started.elapsed())
    };
    // 0.5 s of travel: a 10.5 s deadline, short of the 11 s settle.
    let short = async {
        let started = Instant::now();
        let err = short
            .set_dyn(0.5)
            .await
            .await
            .expect_err("past the deadline");
        (err.to_string(), started.elapsed())
    };
    // 1 s of travel and 2 × 0.5 s of acceleration: a 12 s deadline.
    let long = async {
        let started = Instant::now();
        long.set_dyn(1.0).await.await.expect("within the deadline");
        started.elapsed()
    };
    let still = async {
        let err = still.set_dyn(3.0).await.await.expect_err("zero velocity");
        let sp = EpicsPvaBackend::<f64>::new("STILL");
        sp.connect(Duration::from_secs(5)).await.unwrap();
        (err.to_string(), sp.get_value().await.unwrap())
    };
    let ((returned, pending, moved, stayed), (short_err, short_t), long_t, (still_err, still_sp)) =
        rt.block_on(async { tokio::join!(plain, short, long, still) });

    assert!(returned < Duration::from_millis(500), "{returned:?}");
    assert!(pending, "set returned a settled status before the move");
    assert!(moved >= Duration::from_millis(1_900), "{moved:?}");
    assert!(stayed < Duration::from_millis(500), "{stayed:?}");
    assert!(short_err.contains("timed out after 10.500s"), "{short_err}");
    assert!(
        short_t >= Duration::from_millis(10_400) && short_t < Duration::from_millis(10_900),
        "{short_t:?}"
    );
    assert!(long_t >= Duration::from_millis(10_900), "{long_t:?}");
    assert!(still_err.contains("has zero velocity"), "{still_err}");
    assert_eq!(still_sp, 0.0, "a zero-velocity move must not be put");
}
