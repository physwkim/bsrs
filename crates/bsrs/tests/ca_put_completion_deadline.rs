//! A CA put-callback's completion is bounded by whoever awaits it: `put`
//! applies `EPICS_CA_PUT_TIMEOUT`, while `put_start`'s completion lasts as
//! long as the record's processing, as an areaDetector `arm` needs for its
//! busy Acquire. T's FLNK is a calcout with a 1 s ODLY, so its callback
//! outlives the 0.2 s `EPICS_CA_PUT_TIMEOUT` set here. Either failure names
//! the PV: that timeout, and LOCKED's refusal, as it is disabled (DISP=1).
//!
//! The only test in this binary: the CA client and `EPICS_CA_PUT_TIMEOUT`
//! are resolved once per process, after the environment below is set.
#![cfg(feature = "ca")]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::protocols_async::SignalBackend;
use epics_ca_rs::server::CaServer;

const DB: &str = r#"
record(ao, "T") { field(FLNK, "D") }
record(calcout, "D") { field(ODLY, "1") field(CALC, "1") }
record(ao, "LOCKED") { field(DISP, "1") }
"#;

#[tokio::test(flavor = "multi_thread")]
async fn put_is_bounded_and_a_put_start_completion_is_not() {
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
        std::env::set_var("EPICS_CA_PUT_TIMEOUT", "0.2");
    }
    let t = EpicsCaBackend::<f64>::new("T");
    t.connect(Duration::from_secs(10)).await.unwrap();

    let started = Instant::now();
    let done = t.put_start(Some(1.0)).await.unwrap();
    done.await.expect("a put_start completion has no deadline");
    assert!(started.elapsed() >= Duration::from_millis(900));

    let started = Instant::now();
    let err = t.put(Some(2.0)).await.expect_err("put is bounded");
    assert!(started.elapsed() < Duration::from_millis(900), "{err}");
    assert_eq!(
        err.to_string(),
        "backend error: ca put T: timeout waiting for response"
    );

    let locked = EpicsCaBackend::<f64>::new("LOCKED");
    locked.connect(Duration::from_secs(10)).await.unwrap();
    let err = locked.put(Some(1.0)).await.expect_err("DISP refuses a put");
    assert!(
        err.to_string()
            .starts_with("backend error: ca put LOCKED: "),
        "{err}"
    );
}
