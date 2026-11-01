//! CA and PVA signal backends across a restart of the epics-rs `qsrv-ioc`
//! example, as aioca and p4p behave under ophyd-async: a get waits for the
//! reconnect. DEMO:AO starts at 25 on every boot.
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
use bsrs::protocols_async::SignalBackend;

const CONNECT: Duration = Duration::from_secs(10);
/// Time for a client to find the restarted server: about 30 s for
/// epics-ca-rs through `EPICS_CA_NAME_SERVERS`, 10 s for epics-pva-rs.
const RECONNECT: Duration = Duration::from_secs(60);
/// Below this a failed call did not wait for the connection.
const WAITED: Duration = Duration::from_secs(1);
const PV: &str = "DEMO:AO";
const BOOT_VALUE: f64 = 25.0;

fn run(test: impl Future<Output = ()>) {
    let _serial = outage::serial();
    example_ioc::boot("qsrv-ioc", "qsrv-ioc");
    bsrs_runtime().block_on(test);
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
