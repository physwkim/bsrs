//! Signal behaviour checked against ophyd / ophyd-async: soft and mock
//! backends, the shared monitor, and the observe / wait combinators.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bsrs::backends::mock::{MockBackend, MockSignalBackend};
use bsrs::backends::soft::SoftSignalBackend;
use bsrs::core::error::BsrsError;
use bsrs::core::msg::{LocatableObj, MovableObj, ReadableObj};
use bsrs::core::status::SubToken;
use bsrs::core::{Kind, NdArray, ReadingValue, SignalDatatype, Table};
use bsrs::devices::{
    observe_signals_value, observe_value, wait_for_value, BackendFromPv, Readable, Signal,
    SignalConfig, SignalR, SignalRW, SignalW,
};
use bsrs::event_model::{make_datakey, DataKey, Dtype, DtypeNumpy, SignalMetadata};
use bsrs::protocols_async::{
    AsyncMovable, AsyncSubscribable, Locatable, PutCompletion, ReadingValueCallback, SignalBackend,
};
use futures::StreamExt;
use serde::Serialize;
use serde_json::json;

fn cfg(name: &str) -> SignalConfig {
    SignalConfig {
        source: name.into(),
        kind: Kind::Normal,
        name: name.into(),
    }
}

fn soft<T: SignalDatatype + Clone + Send + Sync + Serialize + 'static>(
    initial: T,
) -> Arc<SoftSignalBackend<T>> {
    Arc::new(SoftSignalBackend::new(initial))
}

/// `descriptor` is ophyd-async's `integer_d`, `waveform_d`, ...: a value's
/// dtype and shape.
async fn lifecycle<T>(
    initial: T,
    put: T,
    descriptor: impl Fn(&T) -> (Dtype, Vec<Option<u64>>),
    dtype_numpy: DtypeNumpy,
) where
    T: SignalDatatype + Clone + Send + Sync + Serialize + PartialEq + Debug + 'static,
{
    let datakey = |v: &T| {
        let (dtype, shape) = descriptor(v);
        let meta = SignalMetadata::default();
        make_datakey("soft://test", dtype, shape, Some(dtype_numpy.clone()), meta)
    };
    let sig: SignalRW<T, _> = Signal::new(soft(initial.clone()), cfg("test"));
    sig.connect(Duration::from_secs(1)).await.unwrap();
    assert_eq!(sig.describe().await.unwrap()["test"], datakey(&initial));

    let mut values = Box::pin(observe_value(
        AsyncSubscribable::subscribe(&sig).await.unwrap(),
    ));
    assert_eq!(values.next().await.unwrap().value, json!(initial));
    assert_eq!(sig.get().await.unwrap(), initial);
    let at = sig.locate().await.unwrap();
    assert_eq!((at.setpoint, at.readback), (initial.clone(), initial));

    assert!(sig.set(put.clone()).await.success());
    assert_eq!(values.next().await.unwrap().value, json!(put));
    assert_eq!(sig.get().await.unwrap(), put);
    let at = sig.locate().await.unwrap();
    assert_eq!((at.setpoint, at.readback), (put.clone(), put.clone()));
    assert_eq!(
        sig.describe().await.unwrap()["test"],
        datakey(&put),
        "the DataKey is the current value's"
    );
}

// ophyd-async test_soft_signal_backend_get_put_monitor, test_signal_lifecycle,
// test_soft_signal_backend_with_numpy_typing; ophyd test_set_method,
// test_array_into_softsignal.
#[tokio::test]
async fn soft_signal_monitor_starts_at_initial_and_follows_puts() {
    let np = |s: &str| DtypeNumpy::from(s);
    lifecycle(0_i64, 43, |_| (Dtype::Integer, vec![]), np("<i8")).await;
    lifecycle(0.0_f64, 43.5, |_| (Dtype::Number, vec![]), np("<f8")).await;
    let goodbye = "goodbye".to_string();
    lifecycle(
        String::new(),
        goodbye,
        |_| (Dtype::String, vec![]),
        np("|S40"),
    )
    .await;
    lifecycle(true, false, |_| (Dtype::Boolean, vec![]), np("|b1")).await;

    fn waveform<E>(v: &[E]) -> (Dtype, Vec<Option<u64>>) {
        (Dtype::Array, vec![Some(v.len() as u64)])
    }
    lifecycle(vec![], vec![-8_i8, 3, 44], |v| waveform(v), np("|i1")).await;
    lifecycle(vec![], vec![218_u8], |v| waveform(v), np("|u1")).await;
    lifecycle(vec![], vec![-855_i16], |v| waveform(v), np("<i2")).await;
    lifecycle(vec![], vec![5666_u16], |v| waveform(v), np("<u2")).await;
    lifecycle(vec![], vec![-2_i32], |v| waveform(v), np("<i4")).await;
    lifecycle(vec![], vec![1022233_u32], |v| waveform(v), np("<u4")).await;
    lifecycle(vec![], vec![-3_i64], |v| waveform(v), np("<i8")).await;
    lifecycle(vec![], vec![995444_u64], |v| waveform(v), np("<u8")).await;
    lifecycle(vec![], vec![1.0_f32], |v| waveform(v), np("<f4")).await;
    lifecycle(vec![], vec![0.2_f64], |v| waveform(v), np("<f8")).await;
    let strs = vec!["nine".to_string(), "ten".into()];
    lifecycle(vec![], strs, |v| waveform(v), np("|S40")).await;
}

// ophyd-async test_soft_signal_ndarray_can_change_dtype: an `np.ndarray`
// signal starts empty of float64 and takes each value in its own dtype,
// which its DataKey then describes.
#[tokio::test]
async fn soft_ndarray_signal_takes_each_value_in_its_own_dtype() {
    let sig: SignalRW<NdArray, _> = Signal::new(soft(NdArray::default()), cfg("test"));
    assert_eq!(sig.get().await.unwrap().data().dtype_numpy(), "<f8");
    for (v, np) in [
        (
            NdArray::new(vec![2, 2], (0..4).collect::<Vec<i32>>()).unwrap(),
            "<i4",
        ),
        (NdArray::from(vec![0.0_f64, 1.0, 2.0, 3.0]), "<f8"),
    ] {
        assert!(sig.set(v.clone()).await.success());
        assert_eq!(sig.get().await.unwrap(), v);
        let dk = &sig.describe().await.unwrap()["test"];
        let shape: Vec<Option<u64>> = v.shape().iter().map(|&n| Some(n as u64)).collect();
        assert_eq!(
            (dk.dtype, &dk.shape, &dk.dtype_numpy),
            (Dtype::Array, &shape, &Some(np.into()))
        );
    }
}

/// ophyd-async's ExampleTable.
#[derive(Clone, Debug, Default, PartialEq, Table)]
struct ExampleTable {
    a_bool: Vec<bool>,
    a_int: Vec<i32>,
    a_float: Vec<f64>,
    a_str: Vec<String>,
    a_enum: Vec<String>,
}

// ophyd-async test_soft_signal_backend_get_put_monitor's ExampleTable case.
#[tokio::test]
async fn soft_table_signal_monitor_starts_at_initial_and_follows_puts() {
    let put = ExampleTable {
        a_bool: vec![true],
        a_int: vec![525],
        a_float: vec![3.25],
        a_str: vec!["pi".into()],
        a_enum: vec!["Aaa".into()],
    };
    let descr = [
        ("a_bool", "|b1"),
        ("a_int", "<i4"),
        ("a_float", "<f8"),
        ("a_str", "|S40"),
        ("a_enum", "|S40"),
    ];
    let descr = descr.map(|(n, d)| (n.to_string(), d.to_string())).to_vec();
    let table_d = |t: &ExampleTable| (Dtype::Array, vec![Some(t.a_bool.len() as u64)]);
    lifecycle(
        ExampleTable::default(),
        put,
        table_d,
        DtypeNumpy::Structured(descr),
    )
    .await;
}

// ophyd-async SignalW is Movable and SignalRW Locatable, so plans move a
// signal like a motor (bps.mv / mvr).
#[tokio::test]
async fn signals_move_and_locate_through_the_plan_traits() {
    let wo = soft(0.0_f64);
    let w: SignalW<f64, _> = Signal::new(wo.clone(), cfg("w"));
    assert!(MovableObj::set_dyn(&w, 2.0).await.success());
    assert_eq!(wo.current_value(), 2.0);

    let rw: Arc<dyn LocatableObj> = Arc::new(SignalRW::<f64, _>::new(
        MockSignalBackend::with_value(1.0).into(),
        cfg("rw"),
    ));
    assert!(rw.set_dyn(5.0).await.success());
    let at = rw.locate_dyn().await.unwrap();
    assert_eq!((at.setpoint, at.readback), (5.0, 5.0));
}

// bluesky stages anything with `stage()`, and ophyd-async `SignalR.stage`
// holds the signal's monitor; a signal handed over as a plain readable
// (a detector, or a `StandardReadable` child) must still be stageable.
#[tokio::test]
async fn a_readable_signal_stages_through_its_readable_handle() {
    let backend = soft(1.0_f64);
    let sig: Arc<dyn ReadableObj> = Arc::new(SignalR::new(backend.clone(), cfg("x")));
    let stageable = sig.as_stageable().expect("a readable Signal is stageable");
    stageable.stage_dyn().await.unwrap();
    assert_eq!(backend.subscriber_count(), 1, "stage holds the monitor");
    stageable.unstage_dyn().await.unwrap();
    assert_eq!(backend.subscriber_count(), 0);
}

/// A device value behind a getter that counts its reads and fails while
/// the value is `None`.
#[derive(Clone)]
struct Device {
    value: Arc<Mutex<Option<f64>>>,
    reads: Arc<AtomicUsize>,
}

impl Device {
    fn new(start: f64) -> Self {
        Self {
            value: Arc::new(Mutex::new(Some(start))),
            reads: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn set(&self, value: Option<f64>) {
        *self.value.lock().unwrap() = value;
    }
    fn reads(&self) -> usize {
        self.reads.load(SeqCst)
    }
    fn getter(
        &self,
    ) -> impl Fn() -> futures::future::Ready<bsrs::core::error::Result<f64>> + Send + Sync + 'static
    {
        let d = self.clone();
        move || {
            d.reads.fetch_add(1, SeqCst);
            let value = *d.value.lock().unwrap();
            futures::future::ready(value.ok_or(BsrsError::Backend("offline".into())))
        }
    }
}

// ophyd-async test_soft_signal_backend_{getter,async_getter},
// _getter_updates_reading, _getter_does_not_affect_setpoint.
#[tokio::test]
async fn soft_getter_supplies_value_and_reading_but_not_setpoint() {
    let dev = Device::new(42.0);
    let b = SoftSignalBackend::new(0.0).with_getter(dev.getter(), None);
    assert_eq!(b.get_value().await.unwrap(), 42.0);
    dev.set(Some(2.0));
    assert_eq!(b.get_reading().await.unwrap().value, json!(2.0));

    b.put(Some(5.0)).await.unwrap();
    dev.set(Some(99.0));
    assert_eq!(b.get_setpoint().await.unwrap(), 5.0);
    assert_eq!(b.get_value().await.unwrap(), 99.0);
    assert_eq!(b.get_setpoint().await.unwrap(), 5.0);
}

// ophyd-async test_soft_signal_backend_{setter_homogeneous_types,async_setter},
// _setter_returns_settled_value, _setter_none_with_getter_refreshes,
// _setter_none_without_getter_stores_write_value.
#[tokio::test]
async fn soft_setter_runs_on_put_and_settles_the_readback() {
    let clamp = SoftSignalBackend::new(0.0)
        .with_setter(|v: Option<f64>| async move { Ok(v.map(|v| v.clamp(0.0, 10.0))) });
    clamp.put(Some(50.0)).await.unwrap();
    assert_eq!(clamp.get_value().await.unwrap(), 10.0);
    assert_eq!(clamp.get_setpoint().await.unwrap(), 50.0);

    let calls = Arc::new(Mutex::new(Vec::new()));
    let log = calls.clone();
    let setter = move |v: Option<f64>| {
        log.lock().unwrap().push(v);
        async { Ok(None) }
    };
    let plain = SoftSignalBackend::new(0.0).with_setter(setter.clone());
    plain.put(Some(6.0)).await.unwrap();
    assert_eq!(plain.current_value(), 6.0);

    let read_back = SoftSignalBackend::new(0.0)
        .with_setter(setter)
        .with_getter(Device::new(3.0).getter(), None);
    read_back.put(Some(7.0)).await.unwrap();
    assert_eq!(read_back.current_value(), 3.0, "taken from the getter");
    assert_eq!(*calls.lock().unwrap(), vec![Some(6.0), Some(7.0)]);
}

// ophyd-async test_soft_signal_backend_poll_period_updates_via_callback,
// _poll_task_starts_and_stops, _no_poll_without_poll_period, and _poll
// skipping a getter that raises.
#[tokio::test(start_paused = true)]
async fn soft_getter_is_polled_only_while_subscribed_with_a_poll_period() {
    let period = Duration::from_millis(50);
    let dev = Device::new(0.0);
    let b = SoftSignalBackend::new(0.0).with_getter(dev.getter(), Some(period));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let token = b.set_callback(Some(Box::new(move |v: &f64, _, _| {
        log.lock().unwrap().push(*v)
    })));

    dev.set(Some(5.0));
    tokio::time::sleep(Duration::from_millis(60)).await;
    dev.set(None);
    tokio::time::sleep(period).await;
    dev.set(Some(7.0));
    tokio::time::sleep(period).await;
    assert_eq!(*seen.lock().unwrap(), vec![0.0, 5.0, 7.0]);
    assert_eq!(dev.reads(), 3);

    drop(token);
    tokio::time::sleep(4 * period).await;
    assert_eq!(dev.reads(), 3, "unsubscribed: no more polls");

    let dev = Device::new(0.0);
    let unpolled = SoftSignalBackend::new(0.0).with_getter(dev.getter(), None);
    let _token = unpolled.set_callback(Some(Box::new(|_: &f64, _, _| {})));
    tokio::time::sleep(4 * period).await;
    assert_eq!(dev.reads(), 0, "no poll period: no polls");
}

/// Soft state behind a connect that counts its calls, takes a while, and
/// fails while `fail` is set.
struct CountedConnect {
    soft: SoftSignalBackend<i64>,
    connects: AtomicUsize,
    fail: AtomicBool,
}

#[async_trait::async_trait]
impl SignalBackend<i64> for CountedConnect {
    async fn connect(&self, _: Duration) -> bsrs::core::error::Result<()> {
        self.connects.fetch_add(1, SeqCst);
        tokio::time::sleep(Duration::from_millis(10)).await;
        match self.fail.load(SeqCst) {
            true => Err(BsrsError::Backend("no IOC".into())),
            false => Ok(()),
        }
    }
    async fn put_start(&self, v: Option<i64>) -> bsrs::core::error::Result<PutCompletion> {
        self.soft.put_start(v).await
    }
    async fn get_datakey(&self, source: &str) -> bsrs::core::error::Result<DataKey> {
        self.soft.get_datakey(source).await
    }
    async fn get_reading(&self) -> bsrs::core::error::Result<ReadingValue> {
        self.soft.get_reading().await
    }
    async fn get_value(&self) -> bsrs::core::error::Result<i64> {
        self.soft.get_value().await
    }
    async fn get_setpoint(&self) -> bsrs::core::error::Result<i64> {
        self.soft.get_setpoint().await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<i64>>) -> SubToken {
        self.soft.set_callback(cb)
    }
    fn source(&self, name: &str, read: bool) -> String {
        self.soft.source(name, read)
    }
}

// ophyd-async test_signal_connects_to_previous_backend,
// test_signal_connects_with_force_reconnect, and Device.connect retrying a
// connect that failed.
#[tokio::test(start_paused = true)]
async fn connect_joins_a_running_or_successful_connect_and_retries_a_failed_one() {
    let backend = Arc::new(CountedConnect {
        soft: SoftSignalBackend::new(0),
        connects: AtomicUsize::new(0),
        fail: AtomicBool::new(true),
    });
    let sig: SignalRW<i64, _> = Signal::new(backend.clone(), cfg("x"));
    let connects = || backend.connects.load(SeqCst);
    let t = Duration::from_secs(1);

    let (a, b) = tokio::join!(sig.connect(t), sig.connect(t));
    assert_eq!(connects(), 1, "concurrent connects share one");
    assert_eq!(a.unwrap_err().to_string(), "backend error: no IOC");
    assert_eq!(b.unwrap_err().to_string(), "backend error: no IOC");

    backend.fail.store(false, SeqCst);
    sig.connect(t).await.unwrap();
    assert_eq!(connects(), 2, "a failed connect is retried");
    let (a, b) = tokio::join!(sig.connect(t), sig.connect(t));
    a.and(b).unwrap();
    assert_eq!(connects(), 2, "a successful connect is kept");

    sig.reconnect(t).await.unwrap();
    assert_eq!(connects(), 3, "reconnect forces a new one");
}

async fn source_and_initial<A: Readable>(sig: Signal<String, SoftSignalBackend<String>, A>) {
    assert_eq!(sig.source(), "soft://TEST-PREFIX:SIGNAL");
    sig.connect(Duration::from_secs(1)).await.unwrap();
    assert_eq!(sig.get().await.unwrap(), "INITIAL");
}

// ophyd-async test_create_soft_signal (SignalR and SignalRW cases).
#[tokio::test]
async fn soft_signal_reports_soft_source_and_initial_value() {
    let r: SignalR<String, _> = Signal::new(soft("INITIAL".to_string()), cfg("TEST-PREFIX:SIGNAL"));
    source_and_initial(r).await;
    let rw: SignalRW<String, _> =
        Signal::new(soft("INITIAL".to_string()), cfg("TEST-PREFIX:SIGNAL"));
    source_and_initial(rw).await;
}

// ophyd test_signal_default_type.
#[tokio::test]
async fn default_soft_signal_reads_float_zero() {
    let sig: SignalR<f64, _> = Signal::new(
        Arc::new(SoftSignalBackend::<f64>::from_pv("aardvark")),
        cfg("aardvark"),
    );
    let value = &sig.read().await.unwrap()["aardvark"].value;
    assert!(value.is_f64(), "{value:?} is not a float");
    assert_eq!(value, &json!(0.0));
}

// ophyd-async test_soft_signal_descriptor_with_metadata,
// test_soft_signal_descriptor_with_no_metadata_not_passed, and
// set_mock_units / set_mock_precision.
#[tokio::test]
async fn describe_carries_units_and_precision_only_when_given() {
    async fn meta<B: SignalBackend<i64> + 'static>(b: Arc<B>) -> (Option<String>, Option<i64>) {
        let sig: SignalRW<i64, _> = Signal::new(b, cfg(""));
        let dk = sig.describe().await.unwrap().remove("").unwrap();
        (dk.units, dk.precision)
    }
    let plain = || SoftSignalBackend::new(0_i64);
    assert_eq!(
        meta(Arc::new(plain().with_units("mm").with_precision(0))).await,
        (Some("mm".into()), Some(0))
    );
    assert_eq!(
        meta(Arc::new(plain().with_units(""))).await,
        (Some("".into()), None)
    );
    assert_eq!(meta(Arc::new(plain())).await, (None, None));

    let mock = Arc::new(MockSignalBackend::new(plain()));
    mock.set_units("mm");
    mock.set_precision(3);
    assert_eq!(meta(mock).await, (Some("mm".into()), Some(3)));
}

fn describe_of(source: &str) -> DataKey {
    make_datakey(
        source,
        Dtype::Number,
        vec![],
        Some("<f8".into()),
        SignalMetadata {
            units: Some("mm".into()),
            precision: Some(2),
            ..Default::default()
        },
    )
}

// ophyd-async test_mock_signal_of_soft_signal_backend_receives_metadata,
// test_mock_signal_of_soft_signal_backend_receives_intial_value.
#[tokio::test]
async fn mock_over_soft_keeps_initial_and_metadata_under_a_mock_source() {
    let backend = || {
        SoftSignalBackend::new(10.0_f64)
            .with_units("mm")
            .with_precision(2)
    };
    let mocked: SignalRW<f64, _> = Signal::new(
        Arc::new(MockSignalBackend::new(backend())),
        cfg("mocked_device-my_signal"),
    );
    let plain: SignalRW<f64, _> = Signal::new(Arc::new(backend()), cfg("soft_device-my_signal"));

    assert_eq!(mocked.get().await.unwrap(), 10.0);
    assert_eq!(plain.get().await.unwrap(), 10.0);
    assert_eq!(
        mocked.describe().await.unwrap(),
        HashMap::from([(
            "mocked_device-my_signal".to_string(),
            describe_of("mock+soft://mocked_device-my_signal"),
        )])
    );
    assert_eq!(
        plain.describe().await.unwrap(),
        HashMap::from([(
            "soft_device-my_signal".to_string(),
            describe_of("soft://soft_device-my_signal"),
        )])
    );
}

// ophyd-async test_when_callback_on_mock_put_returns_none_then_readback_is_the_setpoint;
// a callback returning a value is pinned by mock_signal's own tests.
#[tokio::test]
async fn mock_put_callback_returning_none_keeps_the_put_value() {
    let backend = Arc::new(MockSignalBackend::with_value(0.0_f64));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    backend.set_put_callback(Some(Box::new(move |v| {
        log.lock().unwrap().push(v);
        None
    })));
    let sig: SignalRW<f64, _> = Signal::new(backend, cfg("m"));

    assert!(sig.put(10.0).await.success());
    assert_eq!(*seen.lock().unwrap(), vec![Some(10.0)]);
    assert_eq!(sig.get().await.unwrap(), 10.0);
}

// ophyd-async test_async_callback_on_mock_put,
// test_when_async_callback_returns_{a_value_the_readback_is_set,
// none_then_readback_is_the_setpoint}.
#[tokio::test(start_paused = true)]
async fn async_mock_put_callback_is_awaited_before_the_readback() {
    let backend = Arc::new(MockSignalBackend::with_value(0_i64));
    let sig: Arc<SignalRW<i64, _>> = Arc::new(Signal::new(backend.clone(), cfg("m")));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let log = calls.clone();
    backend.set_put_callback_async(move |v| {
        log.lock().unwrap().push(v);
        async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Some(2)
        }
    });
    let put = tokio::spawn({
        let sig = sig.clone();
        async move { sig.put(5).await.success() }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(sig.get().await.unwrap(), 0, "callback still running");
    assert!(put.await.unwrap());
    assert_eq!(sig.get().await.unwrap(), 2);
    assert_eq!(*calls.lock().unwrap(), vec![Some(5)]);

    backend.set_put_callback_async(|_| async { None });
    assert!(sig.put(7).await.success());
    assert_eq!(sig.get().await.unwrap(), 7);
}

// ophyd-async test_reset_mock_put_calls.
#[tokio::test]
async fn reset_put_calls_forgets_earlier_puts() {
    let backend = Arc::new(MockSignalBackend::with_value(0.0_f64));
    let sig: SignalRW<f64, _> = Signal::new(backend.clone(), cfg("m"));
    assert!(sig.put(1.0).await.success());
    backend.reset_put_calls();
    assert_eq!(backend.put_count(), 0);
    assert!(sig.put(2.0).await.success());
    assert_eq!(backend.put_calls(), vec![Some(2.0)]);
}

// ophyd-async test_set_mock_values{,_exhausted_passes,_exhausted_fails}.
#[tokio::test]
async fn set_values_sets_each_value_as_it_is_taken() {
    let backend = MockSignalBackend::with_value("first".to_string());
    let values = || vec!["second".to_string(), "third".to_string()];

    let mut it = backend.set_values(values(), false);
    assert_eq!(backend.get_value().await.unwrap(), "first");
    assert_eq!(it.next().as_deref(), Some("second"));
    assert_eq!(backend.get_value().await.unwrap(), "second");
    drop(it);

    for v in backend.set_values(values(), true) {
        assert_eq!(backend.get_value().await.unwrap(), v);
    }
    let endless = backend.set_values(values().into_iter().cycle(), false);
    assert_eq!(endless.take(6).count(), 6);

    let mut it = backend.set_values(values(), true);
    it.next();
    let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(it))).unwrap_err();
    assert_eq!(
        err.downcast_ref::<String>().map(String::as_str),
        Some(r#"["second"] were consumed but ["third"] were not consumed"#)
    );
}

// ophyd-async test_can_unsubscribe_from_subscribe_callback; ophyd
// test_signal_base (readback callback on put).
#[tokio::test]
async fn subscribe_callback_gets_current_then_each_put_until_dropped() {
    let sig: SignalRW<f64, _> = Signal::new(soft(0.0_f64), cfg("x"));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let token = sig.subscribe(Box::new(move |v, ts, sev| {
        log.lock().unwrap().push((*v, ts, sev))
    }));

    sig.put(1.0).await;
    sig.put(2.0).await;
    let got: Vec<_> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|(v, _, s)| (*v, *s))
        .collect();
    assert_eq!(got, [(0.0, Some(0)), (1.0, Some(0)), (2.0, Some(0))]);
    let last_ts = seen.lock().unwrap().last().unwrap().1;
    let read = &sig.read().await.unwrap()["x"];
    assert_eq!((read.timestamp, read.alarm_severity), (last_ts, Some(0)));

    drop(token);
    sig.put(3.0).await;
    assert_eq!(
        seen.lock().unwrap().len(),
        3,
        "no callback after unsubscribe"
    );
}

fn less_than_42(r: &ReadingValue) -> bool {
    r.value.as_f64().is_some_and(|v| v < 42.0)
}

// ophyd-async test_wait_for_value_with_value
#[tokio::test(start_paused = true)]
async fn wait_for_value_matches_a_value() {
    let backend = Arc::new(MockSignalBackend::with_value(String::new()));
    let sig: SignalRW<String, _> = Signal::new(backend.clone(), cfg("signal"));
    assert!(sig.set("blah".to_string()).await.success());
    let sub = || async { AsyncSubscribable::subscribe(&sig).await.unwrap() };
    let timeout = Some(Duration::from_millis(100));

    let err = wait_for_value(sub().await, "something", timeout)
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "signal didn't match 'something' in 0.1s, last value 'blah'"
    );
    let current = wait_for_value(sub().await, "blah", timeout).await;
    assert_eq!(current.unwrap().value, json!("blah"));

    let waiter = tokio::spawn(wait_for_value(sub().await, "something else", None));
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    backend.set_value("something else".to_string());
    assert_eq!(
        waiter.await.unwrap().unwrap().value,
        json!("something else")
    );
}

// ophyd-async test_wait_for_value_with_function; the combinator's own
// timeout and wake-up are pinned in devices::observe.
#[tokio::test(start_paused = true)]
async fn wait_for_value_matches_a_function() {
    let backend = Arc::new(MockSignalBackend::with_value(0.0_f64));
    let sig: SignalRW<f64, _> = Signal::new(backend.clone(), cfg("signal"));
    backend.set_value(45.8);
    let sub = || async { AsyncSubscribable::subscribe(&sig).await.unwrap() };
    let timeout = Some(Duration::from_millis(100));

    let err = wait_for_value(sub().await, less_than_42, timeout)
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "signal didn't match less_than_42 in 0.1s, last value 45.8"
    );

    let waiter = tokio::spawn(wait_for_value(sub().await, less_than_42, None));
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    backend.set_value(41.0);
    assert_eq!(waiter.await.unwrap().unwrap().value, json!(41.0));
}

// ophyd-async test_observes_signals_values_working_correctly, and the
// values test_observe_signals_value_timeout_message receives; merging and
// tagging are pinned on raw channels in devices::observe.
#[tokio::test]
async fn observe_signals_value_tags_each_signal_from_its_current_value() {
    let (b1, b2) = (soft(0.0_f64), soft(0.0_f64));
    let s1: SignalR<f64, _> = Signal::new(b1.clone(), cfg("s1"));
    let s2: SignalR<f64, _> = Signal::new(b2.clone(), cfg("s2"));
    let mut merged = Box::pin(observe_signals_value([
        AsyncSubscribable::subscribe(&s1).await.unwrap(),
        AsyncSubscribable::subscribe(&s2).await.unwrap(),
    ]));
    let mut recv: [Vec<serde_json::Value>; 2] = Default::default();
    for step in 0..4 {
        match step {
            2 => b1.write_now(1.0),
            3 => b2.write_now(10.0),
            _ => {}
        }
        let (i, r) = merged.next().await.unwrap();
        recv[i].push(r.value);
    }
    assert_eq!(recv[0], vec![json!(0.0), json!(1.0)]);
    assert_eq!(recv[1], vec![json!(0.0), json!(10.0)]);
}

// bsrs's fixed-value MockBackend has no ophyd counterpart; its monitor still
// starts at the current value like every other backend's.
#[tokio::test]
async fn fixed_mock_backend_monitor_yields_its_value() {
    let sig: SignalR<f64, _> = Signal::new(Arc::new(MockBackend::new(2.5)), cfg("fixed"));
    let mut values = Box::pin(observe_value(
        AsyncSubscribable::subscribe(&sig).await.unwrap(),
    ));
    assert_eq!(values.next().await.unwrap().value, json!(2.5));
}
