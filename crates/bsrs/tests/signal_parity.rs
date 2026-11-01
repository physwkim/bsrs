//! Signal behaviour checked against ophyd / ophyd-async: soft and mock
//! backends, the shared monitor, and the observe / wait combinators.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bsrs::backends::mock::{MockBackend, MockSignalBackend};
use bsrs::backends::soft::SoftSignalBackend;
use bsrs::core::msg::{LocatableObj, MovableObj, ReadableObj};
use bsrs::core::{Kind, ReadingValue};
use bsrs::devices::{
    observe_signals_value, observe_value, wait_for_value, BackendFromPv, Readable, Signal,
    SignalConfig, SignalR, SignalRW, SignalW,
};
use bsrs::event_model::{make_datakey, DataKey, Dtype, SignalMetadata};
use bsrs::protocols_async::{AsyncMovable, AsyncSubscribable, Locatable};
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

fn soft<T: Clone + Send + Sync + Serialize + 'static>(
    initial: T,
    dtype: Dtype,
) -> Arc<SoftSignalBackend<T>> {
    Arc::new(SoftSignalBackend::new(initial, dtype))
}

async fn lifecycle<T>(initial: T, put: T, dtype: Dtype)
where
    T: Clone + Send + Sync + Serialize + PartialEq + Debug + 'static,
{
    let sig: SignalRW<T, _> = Signal::new(soft(initial.clone(), dtype), cfg("test"));
    sig.connect(Duration::from_secs(1)).await.unwrap();
    let before = sig.describe().await.unwrap();
    assert_eq!(before["test"].source, "soft://test");
    assert_eq!(before["test"].dtype, dtype);

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
    assert_eq!((at.setpoint, at.readback), (put.clone(), put));

    assert_eq!(
        sig.describe().await.unwrap(),
        before,
        "a put changes no DataKey"
    );
}

// ophyd-async test_soft_signal_backend_get_put_monitor, test_signal_lifecycle,
// test_soft_signal_backend_with_numpy_typing; ophyd test_set_method,
// test_array_into_softsignal.
#[tokio::test]
async fn soft_signal_monitor_starts_at_initial_and_follows_puts() {
    lifecycle(0_i64, 43, Dtype::Integer).await;
    lifecycle(0.0_f64, 43.5, Dtype::Number).await;
    lifecycle(String::new(), "goodbye".into(), Dtype::String).await;
    lifecycle(true, false, Dtype::Boolean).await;
    lifecycle(Vec::<f64>::new(), vec![1.0, 2.0], Dtype::Array).await;
}

// ophyd-async SignalW is Movable and SignalRW Locatable, so plans move a
// signal like a motor (bps.mv / mvr).
#[tokio::test]
async fn signals_move_and_locate_through_the_plan_traits() {
    let wo = soft(0.0_f64, Dtype::Number);
    let w: SignalW<f64, _> = Signal::new(wo.clone(), cfg("w"));
    assert!(MovableObj::set_dyn(&w, 2.0).await.success());
    assert_eq!(wo.current_value(), 2.0);

    let rw: Arc<dyn LocatableObj> = Arc::new(SignalRW::<f64, _>::new(
        MockSignalBackend::with_value(1.0, Dtype::Number).into(),
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
    let backend = soft(1.0_f64, Dtype::Number);
    let sig: Arc<dyn ReadableObj> = Arc::new(SignalR::new(backend.clone(), cfg("x")));
    let stageable = sig.as_stageable().expect("a readable Signal is stageable");
    stageable.stage_dyn().await.unwrap();
    assert_eq!(backend.subscriber_count(), 1, "stage holds the monitor");
    stageable.unstage_dyn().await.unwrap();
    assert_eq!(backend.subscriber_count(), 0);
}

async fn source_and_initial<A: Readable>(sig: Signal<String, SoftSignalBackend<String>, A>) {
    assert_eq!(sig.source(), "soft://TEST-PREFIX:SIGNAL");
    sig.connect(Duration::from_secs(1)).await.unwrap();
    assert_eq!(sig.get().await.unwrap(), "INITIAL");
}

// ophyd-async test_create_soft_signal (SignalR and SignalRW cases).
#[tokio::test]
async fn soft_signal_reports_soft_source_and_initial_value() {
    let r: SignalR<String, _> = Signal::new(
        soft("INITIAL".to_string(), Dtype::String),
        cfg("TEST-PREFIX:SIGNAL"),
    );
    source_and_initial(r).await;
    let rw: SignalRW<String, _> = Signal::new(
        soft("INITIAL".to_string(), Dtype::String),
        cfg("TEST-PREFIX:SIGNAL"),
    );
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

// ophyd-async test_soft_signal_descriptor_with_metadata (units),
// test_soft_signal_descriptor_with_no_metadata_not_passed.
#[tokio::test]
async fn soft_describe_carries_units_only_when_given() {
    async fn units(b: SoftSignalBackend<i64>) -> Option<String> {
        let sig: SignalRW<i64, _> = Signal::new(Arc::new(b), cfg(""));
        sig.describe().await.unwrap()[""].units.clone()
    }
    let plain = || SoftSignalBackend::new(0_i64, Dtype::Integer);
    assert_eq!(units(plain().with_units("mm")).await.as_deref(), Some("mm"));
    assert_eq!(units(plain().with_units("")).await.as_deref(), Some(""));
    assert_eq!(units(plain()).await, None);
}

fn describe_of(source: &str) -> DataKey {
    make_datakey(
        source,
        Dtype::Number,
        vec![],
        Some("<f8".into()),
        SignalMetadata {
            units: Some("mm".into()),
            ..Default::default()
        },
    )
}

// ophyd-async test_mock_signal_of_soft_signal_backend_receives_metadata,
// test_mock_signal_of_soft_signal_backend_receives_intial_value.
#[tokio::test]
async fn mock_over_soft_keeps_initial_and_metadata_under_a_mock_source() {
    let backend = || {
        SoftSignalBackend::new(10.0_f64, Dtype::Number)
            .with_units("mm")
            .with_dtype_numpy("<f8")
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
    let backend = Arc::new(MockSignalBackend::with_value(0.0_f64, Dtype::Number));
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

// ophyd-async test_can_unsubscribe_from_subscribe_callback; ophyd
// test_signal_base (readback callback on put).
#[tokio::test]
async fn subscribe_callback_gets_current_then_each_put_until_dropped() {
    let sig: SignalRW<f64, _> = Signal::new(soft(0.0_f64, Dtype::Number), cfg("x"));
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

// ophyd-async test_wait_for_value_with_value, test_wait_for_value_with_function;
// the combinator's own timeout and wake-up are pinned in devices::observe.
#[tokio::test(start_paused = true)]
async fn wait_for_value_on_a_signal_sees_its_current_value_and_mock_sets() {
    let backend = Arc::new(MockSignalBackend::with_value(0.0_f64, Dtype::Number));
    let sig: SignalRW<f64, _> = Signal::new(backend.clone(), cfg("signal"));
    backend.set_value(45.8);
    let mut sub = AsyncSubscribable::subscribe(&sig).await.unwrap();
    let timeout = Some(Duration::from_millis(100));

    let current = wait_for_value(&mut sub, |r| r.value == json!(45.8), timeout).await;
    assert_eq!(current.unwrap().value, json!(45.8));

    let err = wait_for_value(&mut sub, less_than_42, timeout)
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "signal didn't match less_than_42 in 0.1s, last value 45.8"
    );
    let err = wait_for_value(&mut sub, |r| r.value == json!("blah"), timeout)
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "signal didn't match <closure> in 0.1s, last value 45.8"
    );

    let waiter = tokio::spawn(async move { wait_for_value(&mut sub, less_than_42, None).await });
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
    let (b1, b2) = (soft(0.0_f64, Dtype::Number), soft(0.0_f64, Dtype::Number));
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
