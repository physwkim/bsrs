//! NTNDArray PVs against the epics-rs `qsrv-ioc` example, whose
//! `db/nt_groups.db` is ophyd-async's `_epics_test_pva_records.db` table and
//! ntndarray records under `DEMO:`.
#![cfg(feature = "pva")]

mod example_ioc;

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::core::error::BsrsError;
use bsrs::core::{bsrs_runtime, NdArray, NdData, SubToken};
use bsrs::event_model::{DataKey, Dtype};
use bsrs::protocols_async::SignalBackend;
use tokio::sync::mpsc;

const CONNECT: Duration = Duration::from_secs(10);
const UPDATE: Duration = Duration::from_secs(5);
const NTNDARRAY: &str = "DEMO:ntndarray";

fn run(test: impl Future<Output = ()>) {
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

async fn next<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(UPDATE, rx.recv())
        .await
        .expect("an update in time")
        .expect("still subscribed")
}

/// ophyd-async `assert_monitor_then_put`: the monitor starts at `initial`
/// and next brings `put`, which `set` writes; the describe is shaped as
/// `initial`.
async fn assert_monitor_then_put<T, F>(
    b: &impl SignalBackend<T>,
    initial: T,
    put: T,
    set: impl Fn(T) -> F,
) -> DataKey
where
    T: Clone + PartialEq + Debug + Send + Sync + 'static,
    F: Future<Output = ()>,
{
    b.connect(CONNECT).await.unwrap();
    let (_token, mut rx) = subscribe(b);
    assert_eq!(next(&mut rx).await, initial);
    let dk = b.get_datakey(NTNDARRAY).await.unwrap();
    set(put.clone()).await;
    assert_eq!(next(&mut rx).await, put);
    dk
}

fn int64s(shape: Vec<usize>, data: Vec<i64>) -> NdArray {
    NdArray::new(shape, data).unwrap()
}

/// test_ntndarray_lifecycle: QSRV cannot write an NTNDArray, so the value
/// goes in through the `:data` waveform behind it.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn ntndarray_lifecycle() {
    run(async {
        let signal = EpicsPvaBackend::<NdArray>::new(NTNDARRAY);
        let raw = EpicsPvaBackend::<Vec<i64>>::new(format!("{NTNDARRAY}:data"));
        raw.connect(CONNECT).await.unwrap();
        let set = |v: NdArray| {
            let NdData::I64(flat) = v.data().clone() else {
                panic!("not int64: {v:?}");
            };
            let raw = &raw;
            async move { raw.put(Some(flat)).await.unwrap() }
        };
        let initial = int64s(vec![2, 3], vec![0; 6]);
        let put = int64s(vec![2, 3], (0..6).collect());
        for (from, to) in [(&initial, &put), (&put, &initial)] {
            let dk = assert_monitor_then_put(&signal, from.clone(), to.clone(), set).await;
            assert_eq!(dk.source, format!("pva://{NTNDARRAY}"));
            assert_eq!(dk.dtype, Dtype::Array);
            assert_eq!(dk.shape, vec![Some(2), Some(3)]);
            assert_eq!(dk.dtype_numpy, Some("<i8".into()));
        }
    });
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn writing_to_ndarray_raises_typeerror() {
    run(async {
        let signal = EpicsPvaBackend::<NdArray>::new(NTNDARRAY);
        signal.connect(CONNECT).await.unwrap();
        match signal.put(Some(NdArray::from(vec![0_i64; 6]))).await {
            Err(BsrsError::Datatype(why)) => assert_eq!(why, "Writing to NDArray not supported"),
            other => panic!("written: {other:?}"),
        }
    });
}
