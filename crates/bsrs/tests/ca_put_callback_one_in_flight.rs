//! A channel has one put-callback in flight: rsrv holds a second
//! WRITE_NOTIFY on the channel, and the circuit behind it, until the first is
//! answered. A put made meanwhile is written plain at once and completes with
//! one trailing put-callback of the latest value. SLOW is a calcout with a
//! 2 s ODLY, so a plain put while it is busy reprocesses it (RPRO) and the
//! trailing put-callback waits for that; ARM's FLNK is such a calcout.
//!
//! The only test in this binary: the CA client is resolved once per
//! process, after the environment below is set.
#![cfg(feature = "ca")]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::protocols_async::SignalBackend;
use epics_ca_rs::server::CaServer;

const DB: &str = r#"
record(calcout, "SLOW") { field(ODLY, "2") field(CALC, "A") }
record(bo, "ARM") { field(FLNK, "ARM_D") }
record(calcout, "ARM_D") { field(ODLY, "2") field(CALC, "1") }
record(ao, "OTHER") {}
"#;

#[tokio::test(flavor = "multi_thread")]
async fn puts_during_a_put_callback_neither_stall_the_circuit_nor_reorder() {
    let server = CaServer::builder()
        .port(0)
        .db_string(DB, &HashMap::new())
        .unwrap()
        .build()
        .await
        .expect("CA server");
    let port = server.tcp_port();
    tokio::spawn(async move { server.run().await });
    // SAFETY: no other thread reads the environment yet.
    unsafe {
        std::env::set_var("EPICS_CA_NAME_SERVERS", format!("127.0.0.1:{port}"));
        std::env::set_var("EPICS_CA_ADDR_LIST", "");
        std::env::set_var("EPICS_CA_AUTO_ADDR_LIST", "NO");
    }
    let slow = EpicsCaBackend::<f64>::new("SLOW.A");
    let arm = EpicsCaBackend::<bool>::new("ARM").with_wait(|on| *on);
    let other = EpicsCaBackend::<f64>::new("OTHER");
    slow.connect(Duration::from_secs(10)).await.unwrap();
    arm.connect(Duration::from_secs(10)).await.unwrap();
    other.connect(Duration::from_secs(10)).await.unwrap();

    let started = Instant::now();
    let first = slow.put_start(Some(1.0)).await.unwrap();
    let armed = arm.put_start(Some(true)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = slow.put_start(Some(2.0)).await.unwrap();
    let third = slow.put_start(Some(3.0)).await.unwrap();
    let rearmed = arm.put_start(Some(true)).await.unwrap();
    // A stop after a re-arm that is still to be sent as the trailing
    // put-callback: that put-callback must carry the stop.
    arm.put(Some(false)).await.unwrap();

    let read = Instant::now();
    other.get_value().await.unwrap();
    let read = read.elapsed();
    let latest = slow.get_value().await.unwrap();

    let ((first, first_t), (second, third, trailing_t), (armed, rearmed)) = tokio::join!(
        async { (first.await, started.elapsed()) },
        async { (second.await, third.await, started.elapsed()) },
        async { (armed.await, rearmed.await) },
    );

    assert!(read < Duration::from_millis(500), "{read:?}");
    assert_eq!(latest, 3.0);
    first.unwrap();
    second.unwrap();
    third.unwrap();
    assert!(first_t >= Duration::from_millis(1_900), "{first_t:?}");
    assert!(
        trailing_t >= first_t + Duration::from_millis(1_900),
        "{trailing_t:?}"
    );
    assert_eq!(slow.get_value().await.unwrap(), 3.0);
    armed.unwrap();
    rearmed.unwrap();
    assert!(!arm.get_value().await.unwrap(), "the stop was rewritten");
}
