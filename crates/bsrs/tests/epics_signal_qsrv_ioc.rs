//! CA and PVA signal backends against the epics-rs `qsrv-ioc` example:
//! connect and its timeout, get/put, monitor, describe and timestamps on
//! DEMO:AI (simulated every 500 ms), DEMO:AO and DEMO:BI.
#![cfg(all(feature = "ca", feature = "pva"))]

mod example_ioc;

use std::future::Future;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bsrs::backends::epics_ca::{CaEnumBackend, EpicsCaBackend};
use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::core::bsrs_runtime;
use bsrs::core::SubToken;
use bsrs::event_model::{DataKey, Dtype, DtypeNumpy, Limits, LimitsRange};
use bsrs::protocols_async::SignalBackend;
use tokio::sync::mpsc;

const CONNECT: Duration = Duration::from_secs(10);
const UPDATE: Duration = Duration::from_secs(5);

fn run(test: impl Future<Output = ()>) {
    example_ioc::boot("qsrv-ioc", "qsrv-ioc");
    bsrs_runtime().block_on(test);
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

type Update<T> = (T, f64, Option<i32>);

fn subscribe<T: Clone + Send + Sync + 'static>(
    b: &impl SignalBackend<T>,
) -> (SubToken, mpsc::UnboundedReceiver<Update<T>>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let token = b.set_callback(Some(Box::new(move |v: &T, ts, sev| {
        let _ = tx.send((v.clone(), ts, sev));
    })));
    (token, rx)
}

async fn next<T>(rx: &mut mpsc::UnboundedReceiver<Update<T>>) -> Update<T> {
    tokio::time::timeout(UPDATE, rx.recv())
        .await
        .expect("an update in time")
        .expect("still subscribed")
}

/// Dropping the token ends the subscription, and with it the callback that
/// holds the sender.
async fn assert_unsubscribed<T>(token: SubToken, mut rx: mpsc::UnboundedReceiver<Update<T>>) {
    drop(token);
    tokio::time::timeout(UPDATE, async { while rx.recv().await.is_some() {} })
        .await
        .expect("the callback is dropped with its token");
}

fn range(low: f64, high: f64) -> Option<LimitsRange> {
    Some(LimitsRange {
        low: Some(low),
        high: Some(high),
    })
}

/// DEMO:AI: EGU degC, PREC 2, HOPR/LOPR 100/0. An ai has no DRVH/DRVL, so
/// its control limits are HOPR/LOPR; its HIHI..LOLO have NO_ALARM
/// severities, which the IOC serves as NaN limits and the datakey drops.
fn assert_demo_ai_datakey(dk: &DataKey) {
    assert_eq!(dk.dtype, Dtype::Number);
    assert_eq!(dk.dtype_numpy, Some(DtypeNumpy::Scalar("<f8".into())));
    assert!(dk.shape.is_empty(), "{:?}", dk.shape);
    assert_eq!(dk.units.as_deref(), Some("degC"));
    assert_eq!(dk.precision, Some(2));
    let limits = dk.limits.as_ref().expect("limits");
    assert_eq!(
        *limits,
        Limits {
            control: range(0.0, 100.0),
            display: range(0.0, 100.0),
            ..Limits::default()
        }
    );
}

/// `connect` to a PV no server has fails once `timeout` has passed, not
/// sooner or later, and names the PV.
async fn assert_missing_pv_connect_times_out<T: Clone + Send + Sync + 'static>(
    b: &impl SignalBackend<T>,
    pv: &str,
    timeout: Duration,
) {
    let started = Instant::now();
    let err = b.connect(timeout).await.expect_err("no server has the PV");
    let elapsed = started.elapsed();
    assert!(elapsed >= timeout, "gave up after {elapsed:?}: {err}");
    assert!(
        elapsed < timeout + Duration::from_secs(1),
        "connect({timeout:?}) took {elapsed:?}: {err}"
    );
    assert!(err.to_string().contains(pv), "{err}");
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_connect_to_a_missing_pv_fails_within_its_timeout() {
    run(async {
        let pv = "BSRS:QSRV:MISSING:CA";
        let b = EpicsCaBackend::<f64>::new(pv);
        assert_missing_pv_connect_times_out(&b, pv, Duration::from_millis(300)).await;
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_connect_to_a_missing_pv_fails_within_its_timeout() {
    run(async {
        let pv = "BSRS:QSRV:MISSING:PVA";
        let b = EpicsPvaBackend::<f64>::new(pv);
        assert_missing_pv_connect_times_out(&b, pv, Duration::from_millis(300)).await;
    });
}

/// Past the 5 s the PVA client gives each of its own operations.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_connect_to_a_missing_pv_waits_out_a_long_timeout() {
    run(async {
        let pv = "BSRS:QSRV:MISSING:PVA:LONG";
        let b = EpicsPvaBackend::<f64>::new(pv);
        assert_missing_pv_connect_times_out(&b, pv, Duration::from_secs(6)).await;
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_get_and_put_to_a_missing_pv_fail() {
    run(async {
        let b = EpicsCaBackend::<f64>::new("BSRS:QSRV:MISSING:CA:IO");
        b.get_value().await.expect_err("get of a missing PV");
        b.put(Some(1.0)).await.expect_err("put to a missing PV");
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_get_and_put_to_a_missing_pv_fail() {
    run(async {
        let b = EpicsPvaBackend::<f64>::new("BSRS:QSRV:MISSING:PVA:IO");
        b.get_value().await.expect_err("get of a missing PV");
        b.put(Some(1.0)).await.expect_err("put to a missing PV");
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_describes_an_ai() {
    run(async {
        let b = EpicsCaBackend::<f64>::new("DEMO:AI");
        b.connect(CONNECT).await.unwrap();
        let dk = b.get_datakey("DEMO:AI").await.unwrap();
        assert_eq!(dk.source, "ca://DEMO:AI");
        assert_demo_ai_datakey(&dk);
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_describes_an_ai() {
    run(async {
        let b = EpicsPvaBackend::<f64>::new("DEMO:AI");
        b.connect(CONNECT).await.unwrap();
        let dk = b.get_datakey("DEMO:AI").await.unwrap();
        assert_eq!(dk.source, "pva://DEMO:AI");
        assert_demo_ai_datakey(&dk);
    });
}

/// Three DEMO:AI updates, each with a server timestamp later than the last
/// and close to now, and no alarm; then the subscription ends with its token.
async fn assert_ticks(b: &impl SignalBackend<f64>) {
    b.connect(CONNECT).await.unwrap();
    let (token, mut rx) = subscribe(b);
    let mut last = 0.0;
    for _ in 0..3 {
        let (v, ts, sev) = next(&mut rx).await;
        assert!((21.0..=23.0).contains(&v), "{v}");
        assert!(ts > last, "{ts} after {last}");
        assert!((now() - ts).abs() < 2.0, "{ts} is not now");
        assert_eq!(sev, Some(0));
        last = ts;
    }
    assert_unsubscribed(token, rx).await;
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_monitor_delivers_each_tick_and_ends_with_its_token() {
    run(async { assert_ticks(&EpicsCaBackend::<f64>::new("DEMO:AI")).await });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_monitor_delivers_each_tick_and_ends_with_its_token() {
    run(async { assert_ticks(&EpicsPvaBackend::<f64>::new("DEMO:AI")).await });
}

/// A put to DEMO:AO reads back over both protocols, and the reading after it
/// carries the put's processing time.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_put_reads_back_over_ca_and_pva_with_a_new_timestamp() {
    run(async {
        let ca = EpicsCaBackend::<f64>::new("DEMO:AO");
        let pva = EpicsPvaBackend::<f64>::new("DEMO:AO");
        ca.connect(CONNECT).await.unwrap();
        pva.connect(CONNECT).await.unwrap();

        let before = ca.get_reading().await.unwrap();
        ca.put(Some(42.5)).await.unwrap();
        let after = ca.get_reading().await.unwrap();
        assert_eq!(after.value, 42.5);
        assert!(after.timestamp > before.timestamp);
        assert!((now() - after.timestamp).abs() < 0.5, "{}", after.timestamp);
        assert_eq!(pva.get_value().await.unwrap(), 42.5);

        let before = pva.get_reading().await.unwrap();
        pva.put(Some(43.5)).await.unwrap();
        let after = pva.get_reading().await.unwrap();
        assert_eq!(after.value, 43.5);
        assert!(after.timestamp > before.timestamp);
        assert!((now() - after.timestamp).abs() < 0.5, "{}", after.timestamp);
        assert_eq!(ca.get_value().await.unwrap(), 43.5);
        assert_eq!(ca.get_setpoint().await.unwrap(), 43.5);
        assert_eq!(pva.get_setpoint().await.unwrap(), 43.5);
    });
}

/// DEMO:BI toggles Off/On every few seconds: read the label between two bool
/// reads that agree.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_reads_a_bi_as_its_label_and_as_a_bool() {
    run(async {
        let label = CaEnumBackend::new("DEMO:BI");
        let flag = EpicsCaBackend::<bool>::new("DEMO:BI");
        label.connect(CONNECT).await.unwrap();
        flag.connect(CONNECT).await.unwrap();

        let dk = label.get_datakey("DEMO:BI").await.unwrap();
        assert_eq!(dk.dtype, Dtype::String);
        assert_eq!(dk.dtype_numpy, Some(DtypeNumpy::Scalar("|S40".into())));
        assert!(dk.shape.is_empty());
        assert_eq!(dk.choices, Some(vec!["Off".to_string(), "On".to_string()]));
        let dk = flag.get_datakey("DEMO:BI").await.unwrap();
        assert_eq!(dk.dtype, Dtype::Boolean);
        assert_eq!(dk.dtype_numpy, Some(DtypeNumpy::Scalar("|b1".into())));

        for _ in 0..3 {
            let first = flag.get_value().await.unwrap();
            let text = label.get_value().await.unwrap();
            if flag.get_value().await.unwrap() == first {
                assert_eq!(text, if first { "On" } else { "Off" });
                return;
            }
        }
        panic!("DEMO:BI toggled under every read");
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_reads_a_bi_as_its_label_and_as_a_bool() {
    run(async {
        let label = EpicsPvaBackend::<String>::new("DEMO:BI");
        let flag = EpicsPvaBackend::<bool>::new("DEMO:BI");
        label.connect(CONNECT).await.unwrap();
        flag.connect(CONNECT).await.unwrap();
        let r = label.get_reading().await.unwrap();
        assert_eq!(r.alarm_severity, Some(0));
        assert!((now() - r.timestamp).abs() < 30.0, "{}", r.timestamp);

        for _ in 0..3 {
            let first = flag.get_value().await.unwrap();
            let text = label.get_value().await.unwrap();
            if flag.get_value().await.unwrap() == first {
                assert_eq!(text, if first { "On" } else { "Off" });
                return;
            }
        }
        panic!("DEMO:BI toggled under every read");
    });
}
