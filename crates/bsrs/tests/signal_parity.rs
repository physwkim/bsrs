//! Signal behaviour checked against ophyd / ophyd-async: soft and mock
//! backends, the shared monitor, and the observe / wait combinators.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bsrs::backends::mock::MockSignalBackend;
use bsrs::backends::soft::SoftSignalBackend;
use bsrs::core::Kind;
use bsrs::devices::{BackendFromPv, Readable, Signal, SignalConfig, SignalR, SignalRW};
use bsrs::event_model::Dtype;
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
