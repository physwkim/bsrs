//! NTTable and NTNDArray PVs against the epics-rs `qsrv-ioc` example, whose
//! `db/nt_groups.db` is ophyd-async's `_epics_test_pva_records.db` table and
//! ntndarray records under `DEMO:`.
#![cfg(feature = "pva")]

mod example_ioc;

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::core::error::BsrsError;
use bsrs::core::{bsrs_runtime, Column, NdArray, NdData, SubToken, Table};
use bsrs::event_model::{DataKey, Dtype, DtypeNumpy};
use bsrs::protocols_async::SignalBackend;
use tokio::sync::mpsc;

const CONNECT: Duration = Duration::from_secs(10);
const UPDATE: Duration = Duration::from_secs(5);
const NTNDARRAY: &str = "DEMO:ntndarray";
const TABLE: &str = "DEMO:table";

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
    source: &str,
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
    let dk = b.get_datakey(source).await.unwrap();
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
            let dk =
                assert_monitor_then_put(&signal, NTNDARRAY, from.clone(), to.clone(), set).await;
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

/// ophyd-async's EpicsTestTable.
#[derive(Clone, Debug, PartialEq, bsrs::core::Table)]
struct EpicsTestTable {
    a_bool: Vec<bool>,
    a_int: Vec<i32>,
    a_float: Vec<f64>,
    a_str: Vec<String>,
    a_enum: Vec<String>,
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// The datakey's `dtype_numpy` of the record's columns, `a_bool` as `bool`.
fn table_numpy(bool: &str) -> Option<DtypeNumpy> {
    let descr = [
        ("a_bool", bool),
        ("a_int", "<i4"),
        ("a_float", "<f8"),
        ("a_str", "|S40"),
        ("a_enum", "|S40"),
    ];
    let descr = descr.map(|(n, d)| (n.to_string(), d.to_string()));
    Some(DtypeNumpy::Structured(descr.to_vec()))
}

/// test_table_lifecycle: an EpicsTestTable and then a plain Table, whose
/// `a_bool` stays the record's uint8, both write the NTTable.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn table_lifecycle() {
    run(async {
        let initial = EpicsTestTable {
            a_bool: vec![false, false, true, true],
            a_int: vec![1, 8, -9, 32],
            a_float: vec![1.8, 8.2, -6.0, 32.9887],
            a_str: strs(&["Hello", "World", "Foo", "Bar"]),
            a_enum: strs(&["Aaa", "Bbb", "Aaa", "Ccc"]),
        };
        let put = EpicsTestTable {
            a_bool: vec![true, false],
            a_int: vec![-5, 32],
            a_float: vec![8.5, -6.97],
            a_str: strs(&["Hello", "Bat"]),
            a_enum: strs(&["Ccc", "Bbb"]),
        };
        let signal = EpicsPvaBackend::<EpicsTestTable>::new(TABLE);
        let set = |v| {
            let signal = &signal;
            async move { signal.put(Some(v)).await.unwrap() }
        };
        let dk = assert_monitor_then_put(&signal, TABLE, initial.clone(), put.clone(), set).await;
        assert_eq!(dk.source, format!("pva://{TABLE}"));
        assert_eq!(dk.dtype, Dtype::Array);
        assert_eq!(dk.shape, vec![Some(4)]);
        assert_eq!(dk.dtype_numpy, table_numpy("|b1"));

        // The record's own column types: `a_bool` a uint8.
        let plain = |t: EpicsTestTable| {
            let a_bool: Vec<u8> = t.a_bool.into_iter().map(u8::from).collect();
            let columns: [(&str, Column); 5] = [
                ("a_bool", a_bool.into()),
                ("a_int", t.a_int.into()),
                ("a_float", t.a_float.into()),
                ("a_str", t.a_str.into()),
                ("a_enum", t.a_enum.into()),
            ];
            Table::new(columns).unwrap()
        };
        let signal = EpicsPvaBackend::<Table>::new(TABLE);
        let set = |v| {
            let signal = &signal;
            async move { signal.put(Some(v)).await.unwrap() }
        };
        let dk = assert_monitor_then_put(&signal, TABLE, plain(put), plain(initial), set).await;
        assert_eq!(dk.shape, vec![Some(2)]);
        assert_eq!(dk.dtype_numpy, table_numpy("|u1"));
    });
}
