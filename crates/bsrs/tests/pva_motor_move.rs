//! `PvaMotor::set` returns its status once the put is sent, and the settle
//! the put waits for (`record[block=true]`) completes it. M's FLNK is a
//! calcout with a 2 s ODLY whose output is "On Change", so a move takes 2 s
//! and a move to where M stands completes in the same processing, as a
//! motor record's null move does.
//!
//! The only test in this binary: the PVA client is resolved once per
//! process, after the environment below is set.
#![cfg(all(feature = "pva", feature = "host"))]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bsrs::core::msg::MovableObj;
use bsrs::core::runtime::bsrs_runtime;
use bsrs::host::pva_devices::PvaMotor;
use epics_pva_rs::server::PvaServer;
use tokio::sync::watch;

const DB: &str = r#"
record(ao, "M") { field(FLNK, "M_D") }
record(calcout, "M_D") {
    field(INPA, "M") field(CALC, "A") field(OOPT, "On Change") field(ODLY, "2")
}
record(ao, "M_RBV") {}
"#;

/// Serve `DB` on an ephemeral port and point the PVA client at it.
async fn serve() {
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
}

#[test]
fn a_set_returns_at_once_and_its_status_completes_at_the_settle() {
    let rt = bsrs_runtime();
    rt.block_on(serve());
    let m = PvaMotor::connect_blocking("m", "M", "M_RBV").unwrap();
    rt.block_on(async {
        let started = Instant::now();
        let moving = m.set_dyn(1.0).await;
        let returned = started.elapsed();
        let pending = !moving.done_state();
        moving.await.expect("the move settles");
        let moved = started.elapsed();

        let started = Instant::now();
        m.set_dyn(1.0).await.await.expect("the null move settles");
        let stayed = started.elapsed();

        assert!(returned < Duration::from_millis(500), "{returned:?}");
        assert!(pending, "set returned a settled status before the move");
        assert!(moved >= Duration::from_millis(1_900), "{moved:?}");
        assert!(stayed < Duration::from_millis(500), "{stayed:?}");
    });
}
