//! The example-IOC harness: both clients reach the IOC it boots, and a port
//! the IOC could not bind is read as taken rather than as a failure.
#![cfg(all(feature = "ca", feature = "pva"))]

mod example_ioc;

use std::net::{TcpListener, UdpSocket};
use std::time::Duration;

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::protocols_async::SignalBackend;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
async fn ca_and_pva_reach_the_booted_ioc() {
    example_ioc::boot("qsrv-ioc", "qsrv-ioc");
    // DEMO:AO holds its VAL; the simulator writes only DEMO:AI and DEMO:BI.
    let ca = EpicsCaBackend::<f64>::new("DEMO:AO");
    ca.connect(Duration::from_secs(10)).await.unwrap();
    assert_eq!(ca.get_value().await.unwrap(), 25.0);
    let pva = EpicsPvaBackend::<f64>::new("DEMO:AO");
    pva.connect(Duration::from_secs(10)).await.unwrap();
    assert_eq!(pva.get_value().await.unwrap(), 25.0);
}

fn free_tcp() -> u16 {
    let l = TcpListener::bind("0.0.0.0:0").unwrap();
    l.local_addr().unwrap().port()
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_ca_port_held_on_udp_is_read_as_taken() {
    let held = UdpSocket::bind("0.0.0.0:0").unwrap();
    let ca = held.local_addr().unwrap().port();
    assert!(example_ioc::try_start("qsrv-ioc", "qsrv-ioc", ca, free_tcp()).is_none());
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_ca_port_held_on_tcp_is_read_as_taken() {
    // Free on UDP, so the IOC binds its search responder and falls back on TCP.
    let (_held, ca) = loop {
        let held = TcpListener::bind("0.0.0.0:0").unwrap();
        let ca = held.local_addr().unwrap().port();
        if UdpSocket::bind(("0.0.0.0", ca)).is_ok() {
            break (held, ca);
        }
    };
    assert!(example_ioc::try_start("qsrv-ioc", "qsrv-ioc", ca, free_tcp()).is_none());
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_pva_port_held_on_tcp_is_read_as_taken() {
    let held = TcpListener::bind("0.0.0.0:0").unwrap();
    let pva = held.local_addr().unwrap().port();
    let ca = loop {
        let p = free_tcp();
        if p != pva && UdpSocket::bind(("0.0.0.0", p)).is_ok() {
            break p;
        }
    };
    assert!(example_ioc::try_start("qsrv-ioc", "qsrv-ioc", ca, pva).is_none());
}
