//! CA and PVA signal backends across a restart of the epics-rs `qsrv-ioc`
//! example, as aioca and p4p behave under ophyd-async: a monitor resumes,
//! and a get waits for the reconnect. DEMO:AO starts at 25 on every boot.
//! The tests share the IOC and run one at a time.
#![cfg(all(feature = "ca", feature = "pva"))]

mod example_ioc;
#[path = "example_ioc/outage.rs"]
mod outage;

use std::future::Future;
use std::time::{Duration, Instant};

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::core::bsrs_runtime;
use bsrs::core::SubToken;
use bsrs::protocols_async::SignalBackend;
use tokio::sync::mpsc;

const CONNECT: Duration = Duration::from_secs(10);
const UPDATE: Duration = Duration::from_secs(5);
/// Time for a client to find the restarted server: about 30 s for
/// epics-ca-rs through `EPICS_CA_NAME_SERVERS`, 10 s for epics-pva-rs.
const RECONNECT: Duration = Duration::from_secs(60);
/// Below this a failed call did not wait for the connection.
const WAITED: Duration = Duration::from_secs(1);
/// An outage longer than any bound a backend puts on a connection.
const OUTAGE: Duration = Duration::from_secs(5);
const PV: &str = "DEMO:AO";
const BOOT_VALUE: f64 = 25.0;

fn run(test: impl Future<Output = ()>) {
    let _serial = outage::serial();
    example_ioc::boot("qsrv-ioc", "qsrv-ioc");
    bsrs_runtime().block_on(test);
}

fn subscribe<T: Clone + Send + Sync + 'static>(
    b: &impl SignalBackend<T>,
) -> (SubToken, mpsc::UnboundedReceiver<T>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let token = b.set_callback(Some(Box::new(move |v: &T, _, _| {
        let _ = tx.send(v.clone());
    })));
    (token, rx)
}

async fn next_eq<T: PartialEq + std::fmt::Debug + std::fmt::Display>(
    rx: &mut mpsc::UnboundedReceiver<T>,
    want: T,
    within: Duration,
) {
    let seen = tokio::time::timeout(within, async {
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            if v == want {
                return Ok(());
            }
            seen.push(v);
        }
        Err(seen)
    })
    .await;
    match seen {
        Ok(Ok(())) => {}
        Ok(Err(seen)) => panic!("the monitor ended before {want}, after {seen:?}"),
        Err(_) => panic!("no {want} in {within:?}"),
    }
}

/// aioca's `test_channel_connected` and p4p's monitor: the subscription
/// outlives the server, and the restarted record's value arrives on it.
async fn assert_monitor_resumes(b: &impl SignalBackend<f64>) {
    b.connect(CONNECT).await.unwrap();
    b.put(Some(7.0)).await.unwrap();
    let (_token, mut rx) = subscribe(b);
    next_eq(&mut rx, 7.0, UPDATE).await;
    outage::restart();
    next_eq(&mut rx, BOOT_VALUE, RECONNECT).await;
}

/// aioca's camonitor and p4p's monitor on a channel that is down wait for
/// it: the restarted record's value is the subscription's first.
async fn assert_monitor_made_during_the_outage_starts(b: &impl SignalBackend<f64>) {
    b.connect(CONNECT).await.unwrap();
    b.put(Some(7.0)).await.unwrap();
    outage::stop();
    // Fails once the client has seen the circuit close.
    b.get_value().await.expect_err("the IOC is down");
    let (_token, mut rx) = subscribe(b);
    outage::start();
    next_eq(&mut rx, BOOT_VALUE, RECONNECT).await;
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_monitor_made_during_the_outage_starts_on_reconnect() {
    run(async {
        assert_monitor_made_during_the_outage_starts(&EpicsCaBackend::<f64>::new(PV)).await
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_monitor_made_during_the_outage_starts_on_reconnect() {
    run(async {
        assert_monitor_made_during_the_outage_starts(&EpicsPvaBackend::<f64>::new(PV)).await
    });
}

/// A CA String monitor reads the channel's native type before it
/// subscribes; made with no `connect` while the IOC is down, it waits for
/// the channel as camonitor does, however long the outage.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_string_monitor_made_during_the_outage_starts_on_reconnect() {
    run(async {
        outage::stop();
        let b = EpicsCaBackend::<String>::new("DEMO:BI");
        let (_token, mut rx) = subscribe(&b);
        tokio::time::sleep(OUTAGE).await;
        outage::start();
        next_eq(&mut rx, "Off".to_string(), RECONNECT).await;
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_monitor_resumes_after_the_ioc_restarts() {
    run(async { assert_monitor_resumes(&EpicsCaBackend::<f64>::new(PV)).await });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_monitor_resumes_after_the_ioc_restarts() {
    run(async { assert_monitor_resumes(&EpicsPvaBackend::<f64>::new(PV)).await });
}

/// aioca's `caget` and p4p's `get` wait for the channel to reconnect. A
/// bsrs get bounds that wait, so each get made during the outage fails only
/// once it has waited, and one reaches the restarted server.
async fn assert_get_and_put_reach_the_restarted_ioc(b: &impl SignalBackend<f64>) {
    b.connect(CONNECT).await.unwrap();
    b.put(Some(7.0)).await.unwrap();
    outage::restart();
    let outage = Instant::now();
    let value = loop {
        let call = Instant::now();
        match b.get_value().await {
            Ok(v) => break v,
            Err(e) => {
                let waited = call.elapsed();
                assert!(waited >= WAITED, "failed after {waited:?}: {e}");
                assert!(outage.elapsed() < RECONNECT, "no reconnect: {e}");
            }
        }
    };
    assert_eq!(value, BOOT_VALUE);
    b.put(Some(4.0)).await.unwrap();
    assert_eq!(b.get_value().await.unwrap(), 4.0);
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_get_and_put_reach_the_restarted_ioc() {
    run(async {
        assert_get_and_put_reach_the_restarted_ioc(&EpicsCaBackend::<f64>::new(PV)).await
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_get_and_put_reach_the_restarted_ioc() {
    run(async {
        assert_get_and_put_reach_the_restarted_ioc(&EpicsPvaBackend::<f64>::new(PV)).await
    });
}
