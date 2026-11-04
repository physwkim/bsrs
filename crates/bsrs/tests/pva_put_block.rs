//! A PVA put completes when the record has processed, not when the value is
//! written, and that completion outlasts the client's 5 s op timeout. SLOW is
//! a calcout with a 6 s ODLY whose output is "On Change": a put that changes
//! it waits the delay out, as a motor's put waits out the move, and a put of
//! the value it holds processes at once, as a move to where the motor stands.
//! LOCKED is disabled (DISP=1), so a put to it fails, naming the PV.
//!
//! The only test in this binary: the PVA client is resolved once per
//! process, after the environment below is set.
#![cfg(feature = "pva")]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::protocols_async::SignalBackend;
use epics_pva_rs::server::PvaServer;
use tokio::sync::watch;

const DB: &str = r#"
record(calcout, "SLOW") { field(ODLY, "6") field(CALC, "A") field(OOPT, "On Change") }
record(ao, "LOCKED") { field(DISP, "1") }
"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_put_completes_when_the_record_has_processed() {
    // SAFETY: no other thread reads the environment yet.
    unsafe {
        std::env::set_var("EPICS_PVAS_INTF_ADDR_LIST", "127.0.0.1");
        std::env::set_var("EPICS_PVAS_BROADCAST_PORT", "0");
    }
    let server = PvaServer::builder()
        .port(0)
        .db_string(DB, &HashMap::new())
        .unwrap()
        .build()
        .await
        .expect("PVA server");
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
    let slow = EpicsPvaBackend::<f64>::new("SLOW.A");
    slow.connect(Duration::from_secs(10)).await.unwrap();

    let started = Instant::now();
    let moving = slow.put_start(Some(1.0)).await.unwrap();
    let sent = started.elapsed();
    let moved = moving.await;
    let moved_t = started.elapsed();

    let started = Instant::now();
    slow.put(Some(1.0)).await.unwrap();
    let still_t = started.elapsed();

    assert!(sent < Duration::from_secs(1), "{sent:?}");
    moved.unwrap();
    assert!(moved_t >= Duration::from_millis(5_900), "{moved_t:?}");
    assert!(still_t < Duration::from_secs(1), "{still_t:?}");
    assert_eq!(slow.get_value().await.unwrap(), 1.0);

    let locked = EpicsPvaBackend::<f64>::new("LOCKED");
    locked.connect(Duration::from_secs(10)).await.unwrap();
    let err = locked.put(Some(1.0)).await.expect_err("DISP refuses a put");
    assert!(
        err.to_string()
            .starts_with("backend error: pva put LOCKED: "),
        "{err}"
    );
}
