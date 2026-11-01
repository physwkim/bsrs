//! CA and PVA signal backends against the epics-rs `ophyd-test-ioc`
//! example: the soft sensors `XF:31IDA-BI{Dev:N}E-I` (ai, EGU mA, PREC 3,
//! HOPR/LOPR ±1000), the simDetector's string, long-string, enum, integer
//! and array records under its two prefixes, and the sim motors.
//! Each test writes only records no other test here touches.
#![cfg(all(feature = "ca", feature = "pva"))]

mod example_ioc;

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use bsrs::backends::epics_ca::{CaEnumBackend, EpicsCaBackend};
use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::core::bsrs_runtime;
use bsrs::core::SubToken;
use bsrs::event_model::{DataKey, Dtype, DtypeNumpy, Limits, LimitsRange};
use bsrs::protocols_async::SignalBackend;
use tokio::sync::mpsc;

const CONNECT: Duration = Duration::from_secs(10);
const UPDATE: Duration = Duration::from_secs(5);
const CAM: &str = "XF:31IDA-BI{Cam:Tbl}:";

fn run(test: impl Future<Output = ()>) {
    example_ioc::boot("ophyd-test-ioc", "ophyd-test-ioc");
    bsrs_runtime().block_on(test);
}

fn sensor(n: u32) -> String {
    format!("XF:31IDA-BI{{Dev:{n}}}E-I")
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

/// The first update for which `pred` holds.
async fn next_where<T: Debug>(
    rx: &mut mpsc::UnboundedReceiver<Update<T>>,
    pred: impl Fn(&T) -> bool,
) -> Update<T> {
    loop {
        let u = next(rx).await;
        if pred(&u.0) {
            return u;
        }
    }
}

/// Dropping the token ends the subscription, and with it the callback that
/// holds the sender.
async fn assert_unsubscribed<T>(token: SubToken, mut rx: mpsc::UnboundedReceiver<Update<T>>) {
    drop(token);
    tokio::time::timeout(UPDATE, async { while rx.recv().await.is_some() {} })
        .await
        .expect("the callback is dropped with its token");
}

/// ophyd-async's `assert_signal_lifecycle`: the current value arrives on
/// subscribe, a put arrives on the monitor and reads back as value and
/// setpoint, the describe is the same after the put as before it, and the
/// subscription ends with its token.
async fn assert_lifecycle<T>(b: &impl SignalBackend<T>, source: &str, put: T) -> DataKey
where
    T: Clone + PartialEq + Debug + Send + Sync + 'static,
{
    b.connect(CONNECT).await.unwrap();
    let before = b.get_datakey(source).await.unwrap();
    let initial = b.get_value().await.unwrap();
    assert_ne!(initial, put, "the put must change the value");
    let (token, mut rx) = subscribe(b);
    assert_eq!(next(&mut rx).await.0, initial);
    b.put(Some(put.clone())).await.unwrap();
    next_where(&mut rx, |v| *v == put).await;
    assert_eq!(b.get_value().await.unwrap(), put);
    assert_eq!(b.get_setpoint().await.unwrap(), put);
    assert_eq!(b.get_datakey(source).await.unwrap(), before);
    assert_unsubscribed(token, rx).await;
    before
}

fn range(low: f64, high: f64) -> Option<LimitsRange> {
    Some(LimitsRange {
        low: Some(low),
        high: Some(high),
    })
}

fn numpy(s: &str) -> Option<DtypeNumpy> {
    Some(DtypeNumpy::Scalar(s.into()))
}

/// A sensor: EGU mA, PREC 3, HOPR/LOPR ±1000, which an ai also serves as
/// its control limits; no alarm severities, so no alarm or warning limits.
fn assert_sensor_datakey(dk: &DataKey) {
    assert_eq!(dk.dtype, Dtype::Number);
    assert_eq!(dk.dtype_numpy, numpy("<f8"));
    assert!(dk.shape.is_empty(), "{:?}", dk.shape);
    assert_eq!(dk.units.as_deref(), Some("mA"));
    assert_eq!(dk.precision, Some(3));
    assert_eq!(
        dk.limits,
        Some(Limits {
            control: range(-1000.0, 1000.0),
            display: range(-1000.0, 1000.0),
            ..Limits::default()
        })
    );
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_sensor_lifecycle() {
    run(async {
        let pv = sensor(1);
        let b = EpicsCaBackend::<f64>::new(&pv);
        let dk = assert_lifecycle(&b, &pv, 12.5).await;
        assert_eq!(dk.source, format!("ca://{pv}"));
        assert_sensor_datakey(&dk);
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_sensor_lifecycle() {
    run(async {
        let pv = sensor(2);
        let b = EpicsPvaBackend::<f64>::new(&pv);
        let dk = assert_lifecycle(&b, &pv, 12.5).await;
        assert_eq!(dk.source, format!("pva://{pv}"));
        assert_sensor_datakey(&dk);
    });
}

/// Give sensor `pv` a MINOR band above 10, MAJOR above 20 and INVALID below
/// -20, over CA.
async fn set_alarm_bands(pv: &str) {
    for (field, v) in [("HIGH", 10.0), ("HIHI", 20.0), ("LOLO", -20.0)] {
        let b = EpicsCaBackend::<f64>::new(format!("{pv}.{field}"));
        b.connect(CONNECT).await.unwrap();
        b.put(Some(v)).await.unwrap();
    }
    for (field, sev) in [("HSV", "MINOR"), ("HHSV", "MAJOR"), ("LLSV", "INVALID")] {
        let b = CaEnumBackend::new(format!("{pv}.{field}"));
        b.connect(CONNECT).await.unwrap();
        b.put(Some(sev.into())).await.unwrap();
    }
}

/// Each put lands in its alarm band: the monitor, and a reading after it,
/// carry NO_ALARM 0, MINOR 1, MAJOR 2 and INVALID -1, and the datakey now
/// carries the bands as limits. The sensor is scanned, so a put writes VAL
/// without processing and the alarm comes with the next scan, as in C.
async fn assert_alarm_severity(b: &impl SignalBackend<f64>, pv: &str) {
    set_alarm_bands(pv).await;
    b.connect(CONNECT).await.unwrap();
    let (token, mut rx) = subscribe(b);
    next(&mut rx).await;
    for (v, sev) in [(15.0, 1), (25.0, 2), (-25.0, -1), (0.0, 0)] {
        b.put(Some(v)).await.unwrap();
        assert_eq!(next_where(&mut rx, |x| *x == v).await.2, Some(sev), "{v}");
        assert_eq!(
            b.get_reading().await.unwrap().alarm_severity,
            Some(sev),
            "{v}"
        );
    }
    assert_unsubscribed(token, rx).await;
    let limits = b.get_datakey(pv).await.unwrap().limits.expect("limits");
    assert_eq!(
        limits.alarm,
        Some(LimitsRange {
            low: Some(-20.0),
            high: Some(20.0)
        })
    );
    assert_eq!(
        limits.warning,
        Some(LimitsRange {
            low: None,
            high: Some(10.0)
        })
    );
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_alarm_severity() {
    run(async {
        let pv = sensor(3);
        assert_alarm_severity(&EpicsCaBackend::<f64>::new(&pv), &pv).await;
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_alarm_severity() {
    run(async {
        let pv = sensor(4);
        assert_alarm_severity(&EpicsPvaBackend::<f64>::new(&pv), &pv).await;
    });
}

fn motor(n: u32) -> String {
    format!("XF:31IDA-OP{{Tbl-Ax:X{n}}}Mtr")
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-6
}

fn f64_of(v: &serde_json::Value) -> f64 {
    v.as_f64().unwrap_or_else(|| panic!("a number: {v}"))
}

/// ophyd's `test_timestamps`: after a setpoint put that moves the motor,
/// the setpoint and the readback each read back, by get and by monitor,
/// at the new position with a later timestamp than before the put.
async fn assert_move_timestamps(sp: &impl SignalBackend<f64>, rbv: &impl SignalBackend<f64>) {
    sp.connect(CONNECT).await.unwrap();
    rbv.connect(CONNECT).await.unwrap();
    let sp0 = sp.get_reading().await.unwrap();
    let rbv0 = rbv.get_reading().await.unwrap();
    let target = f64_of(&sp0.value) + 0.1;
    let (sp_token, mut sp_rx) = subscribe(sp);
    let (rbv_token, mut rbv_rx) = subscribe(rbv);
    next(&mut sp_rx).await;
    next(&mut rbv_rx).await;
    sp.put(Some(target)).await.unwrap();

    let sp1 = sp.get_reading().await.unwrap();
    assert!(close(f64_of(&sp1.value), target), "{}", sp1.value);
    assert!(sp1.timestamp > sp0.timestamp);
    let rbv1 = rbv.get_reading().await.unwrap();
    assert!(close(f64_of(&rbv1.value), target), "{}", rbv1.value);
    assert!(rbv1.timestamp > rbv0.timestamp);

    let (_, ts, _) = next_where(&mut sp_rx, |v| close(*v, target)).await;
    assert!(ts > sp0.timestamp);
    let (_, ts, _) = next_where(&mut rbv_rx, |v| close(*v, target)).await;
    assert!(ts > rbv0.timestamp);
    assert_unsubscribed(sp_token, sp_rx).await;
    assert_unsubscribed(rbv_token, rbv_rx).await;
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_move_timestamps() {
    run(async {
        let m = motor(5);
        let sp = EpicsCaBackend::<f64>::new(&m);
        let rbv = EpicsCaBackend::<f64>::new(format!("{m}.RBV"));
        assert_move_timestamps(&sp, &rbv).await;
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_move_timestamps() {
    run(async {
        let m = motor(6);
        let sp = EpicsPvaBackend::<f64>::new(&m);
        let rbv = EpicsPvaBackend::<f64>::new(format!("{m}.RBV"));
        assert_move_timestamps(&sp, &rbv).await;
    });
}

fn assert_string_datakey(dk: &DataKey) {
    assert_eq!(dk.dtype, Dtype::String);
    assert!(dk.shape.is_empty(), "{:?}", dk.shape);
    assert_eq!(dk.units, None);
    assert_eq!(dk.precision, None);
    assert_eq!(dk.limits, None);
}

/// A stringout (DBR_STRING).
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_string_lifecycle() {
    run(async {
        let pv = format!("{CAM}TIFF1:TempSuffix");
        let b = EpicsCaBackend::<String>::new(&pv);
        assert_string_datakey(&assert_lifecycle(&b, &pv, "_ca.tmp".to_string()).await);
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_string_lifecycle() {
    run(async {
        let pv = format!("{CAM}HDF1:TempSuffix");
        let b = EpicsPvaBackend::<String>::new(&pv);
        assert_string_datakey(&assert_lifecycle(&b, &pv, "_pva.tmp".to_string()).await);
    });
}

/// A CHAR waveform (NELM 256) read and written as one string longer than a
/// DBR_STRING's 40 bytes.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_long_string_lifecycle() {
    run(async {
        let pv = format!("{CAM}TIFF1:FileName");
        let b = EpicsCaBackend::<String>::new_long_string(&pv);
        let long = format!("bsrs_{}", "x".repeat(100));
        let dk = assert_lifecycle(&b, &pv, long).await;
        assert_eq!(dk.dtype, Dtype::String);
    });
}

/// The same CHAR waveform over PVA, as a string.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_long_string_lifecycle() {
    run(async {
        let pv = format!("{CAM}HDF1:FileName");
        let b = EpicsPvaBackend::<String>::new(&pv);
        let long = format!("bsrs_{}", "y".repeat(100));
        let dk = assert_lifecycle(&b, &pv, long).await;
        assert_eq!(dk.dtype, Dtype::String);
    });
}

/// An mbbo: its labels are the datakey's choices, a label puts and reads
/// back, and a label it does not have is refused with the ones it has.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_enum_lifecycle() {
    run(async {
        let pv = format!("{CAM}cam1:ImageMode");
        let b = CaEnumBackend::new(&pv);
        let dk = assert_lifecycle(&b, &pv, "Multiple".to_string()).await;
        assert_eq!(dk.dtype, Dtype::String);
        assert!(dk.shape.is_empty());
        let choices = ["Single", "Multiple", "Continuous"]
            .map(String::from)
            .to_vec();
        assert_eq!(dk.choices, Some(choices));
        let err = b.put(Some("Bogus".into())).await.expect_err("not a label");
        assert!(err.to_string().contains("Bogus"), "{err}");
        assert!(err.to_string().contains("Continuous"), "{err}");
        assert_eq!(b.get_value().await.unwrap(), "Multiple");
    });
}

/// An mbbo over PVA (NTEnum) as a string: a label puts and reads back, an
/// index string puts as that index, as p4p assigns into an `enum_t`.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_enum_lifecycle() {
    run(async {
        let pv = "ADSIM:cam1:ImageMode";
        let b = EpicsPvaBackend::<String>::new(pv);
        let dk = assert_lifecycle(&b, pv, "Multiple".to_string()).await;
        assert_eq!(dk.dtype, Dtype::String);
        assert!(dk.shape.is_empty());
        b.put(Some("2".into())).await.unwrap();
        assert_eq!(b.get_value().await.unwrap(), "Continuous");
        b.put(Some("Bogus".into())).await.expect_err("not a label");
        assert_eq!(b.get_value().await.unwrap(), "Continuous");
    });
}

/// A longout without EGU: an integer whose units are the empty EGU, as
/// ophyd-async keeps them for any number, and with no precision.
async fn assert_integer_lifecycle(b: &impl SignalBackend<i64>, pv: &str) {
    let dk = assert_lifecycle(b, pv, 5).await;
    assert_eq!(dk.dtype, Dtype::Integer);
    assert_eq!(dk.dtype_numpy, numpy("<i8"));
    assert!(dk.shape.is_empty());
    assert_eq!(dk.units.as_deref(), Some(""));
    assert_eq!(dk.precision, None);
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_integer_lifecycle() {
    run(async {
        let pv = format!("{CAM}cam1:NumImages");
        assert_integer_lifecycle(&EpicsCaBackend::<i64>::new(&pv), &pv).await;
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_integer_lifecycle() {
    run(async {
        let pv = "ADSIM:cam1:NumImages";
        assert_integer_lifecycle(&EpicsPvaBackend::<i64>::new(pv), pv).await;
    });
}

/// A LONG waveform (NELM 10) written and read as `Vec<f64>`.
/// A put reads back at its own length, one element included: the reads ask
/// for the current element count, as aioca's and p4p's do.
async fn assert_array_put_reads_back(b: &impl SignalBackend<Vec<f64>>, pv: &str) {
    b.connect(CONNECT).await.unwrap();
    let (token, mut rx) = subscribe(b);
    next(&mut rx).await;
    for put in [vec![3.0, 4.0, 5.0], vec![7.0]] {
        b.put(Some(put.clone())).await.unwrap();
        next_where(&mut rx, |v| *v == put).await;
        assert_eq!(b.get_value().await.unwrap(), put);
        let reading = b.get_reading().await.unwrap();
        assert_eq!(reading.value, serde_json::json!(put));
    }
    assert_unsubscribed(token, rx).await;
    let dk = b.get_datakey(pv).await.unwrap();
    assert_eq!(dk.dtype_numpy, numpy("<f8"));
    assert_eq!(dk.shape.len(), 1, "{:?}", dk.shape);
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_array_put_reads_back() {
    run(async {
        let pv = format!("{CAM}cam1:Dimensions");
        assert_array_put_reads_back(&EpicsCaBackend::<Vec<f64>>::new(&pv), &pv).await;
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_array_put_reads_back() {
    run(async {
        let pv = "ADSIM:cam1:Dimensions";
        assert_array_put_reads_back(&EpicsPvaBackend::<Vec<f64>>::new(pv), pv).await;
    });
}

/// NAME is SPC_NOMOD: the server refuses the put and the field keeps its
/// value. The record names fit a DBR_STRING's 39 characters.
async fn assert_put_refused(b: &impl SignalBackend<String>, record: &str) {
    b.connect(CONNECT).await.unwrap();
    b.put(Some("renamed".into()))
        .await
        .expect_err("a put to a read-only field");
    assert_eq!(b.get_value().await.unwrap(), record);
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_put_to_a_read_only_field_is_refused() {
    run(async {
        let record = "ADSIM:cam1:Model_RBV";
        let b = EpicsCaBackend::<String>::new(format!("{record}.NAME"));
        assert_put_refused(&b, record).await;
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_put_to_a_read_only_field_is_refused() {
    run(async {
        let record = "ADSIM:cam1:Manufacturer_RBV";
        let b = EpicsPvaBackend::<String>::new(format!("{record}.NAME"));
        assert_put_refused(&b, record).await;
    });
}

/// ophyd's `test_epicssignal_pv_reuse`: a hundred signals on one PV all
/// connect and read.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_many_signals_on_one_pv_connect_and_read() {
    run(async {
        let signals: Vec<_> = (0..100)
            .map(|_| EpicsCaBackend::<f64>::new("sim:mtr1.RBV"))
            .collect();
        let reads = signals.iter().map(|s| async move {
            s.connect(CONNECT).await?;
            s.get_value().await
        });
        for r in futures::future::join_all(reads).await {
            r.unwrap();
        }
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_many_signals_on_one_pv_connect_and_read() {
    run(async {
        let signals: Vec<_> = (0..100)
            .map(|_| EpicsPvaBackend::<f64>::new("sim:mtr2.RBV"))
            .collect();
        let reads = signals.iter().map(|s| async move {
            s.connect(CONNECT).await?;
            s.get_value().await
        });
        for r in futures::future::join_all(reads).await {
            r.unwrap();
        }
    });
}
