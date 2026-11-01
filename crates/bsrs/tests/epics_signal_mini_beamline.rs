//! CA and PVA signal backends against the epics-rs `mini-beamline` example:
//! the 10000-element DOUBLE waveforms `mini:wfN`, refreshed with random
//! values in [0, 1) every second, read past CA's default 16 kB array limit.
#![cfg(all(feature = "ca", feature = "pva"))]

mod example_ioc;

use std::future::Future;
use std::time::Duration;

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::core::bsrs_runtime;
use bsrs::event_model::DtypeNumpy;
use bsrs::protocols_async::SignalBackend;
use tokio::sync::mpsc;

const CONNECT: Duration = Duration::from_secs(10);
const UPDATE: Duration = Duration::from_secs(5);
const NELM: usize = 10000;

fn run(test: impl Future<Output = ()>) {
    example_ioc::boot("mini-ioc", "mini-beamline");
    bsrs_runtime().block_on(test);
}

fn assert_random(v: &[f64]) {
    assert_eq!(v.len(), NELM);
    assert!(v.iter().all(|x| (0.0..1.0).contains(x)), "{:?}", &v[..8]);
}

/// A get and two monitor updates each carry the whole waveform; the updates
/// come a scan apart with fresh values, the subscription ends with its
/// token, and the datakey has the waveform's length.
async fn assert_waveform(b: &impl SignalBackend<Vec<f64>>, pv: &str) {
    b.connect(CONNECT).await.unwrap();
    assert_random(&b.get_value().await.unwrap());

    let (tx, mut rx) = mpsc::unbounded_channel();
    let token = b.set_callback(Some(Box::new(move |v: &Vec<f64>, ts, sev| {
        let _ = tx.send((v.clone(), ts, sev));
    })));
    let mut next = async || {
        tokio::time::timeout(UPDATE, rx.recv())
            .await
            .expect("an update in time")
            .expect("still subscribed")
    };
    let (first, t1, sev) = next().await;
    assert_eq!(sev, Some(0));
    let (second, t2, _) = next().await;
    drop(token);
    tokio::time::timeout(UPDATE, async { while rx.recv().await.is_some() {} })
        .await
        .expect("the callback is dropped with its token");
    assert_random(&first);
    assert_random(&second);
    assert_ne!(first, second);
    assert!((0.5..1.5).contains(&(t2 - t1)), "{t1} then {t2}");

    let dk = b.get_datakey(pv).await.unwrap();
    assert_eq!(dk.dtype_numpy, Some(DtypeNumpy::Scalar("<f8".into())));
    assert_eq!(dk.shape, vec![Some(NELM as u64)]);
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ca_waveform_get_monitor_and_describe() {
    run(async { assert_waveform(&EpicsCaBackend::<Vec<f64>>::new("mini:wf1"), "mini:wf1").await });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn pva_waveform_get_monitor_and_describe() {
    run(async { assert_waveform(&EpicsPvaBackend::<Vec<f64>>::new("mini:wf2"), "mini:wf2").await });
}
