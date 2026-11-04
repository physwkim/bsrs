//! ophyd-async `test_mbb_direct_bit_access`: a `.B0` field reads and writes
//! one bit of an mbbiDirect/mbboDirect record as a bool, over CA and PVA.
//! The records are this crate's own, `tests/ioc/mbb_direct`, served by the
//! epics-rs `qsrv-ioc`.
#![cfg(all(feature = "ca", feature = "pva"))]

mod example_ioc;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bsrs::backends::epics_ca::EpicsCaBackend;
use bsrs::backends::epics_pva::EpicsPvaBackend;
use bsrs::core::bsrs_runtime;
use bsrs::devices::{Access, BackendFromPv, Signal, SignalConfig, SignalR, SignalRW};
use bsrs::protocols_async::SignalBackend;

const CONNECT: Duration = Duration::from_secs(10);

fn run(test: impl Future<Output = ()>) {
    example_ioc::boot(
        "qsrv-ioc",
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/ioc/mbb_direct"),
    );
    bsrs_runtime().block_on(test);
}

fn signal<B: BackendFromPv + SignalBackend<bool>, A: Access>(pv: &str) -> Signal<bool, B, A> {
    let config = SignalConfig {
        source: pv.into(),
        name: pv.into(),
        ..Default::default()
    };
    Signal::new(Arc::new(B::from_pv(pv)), config)
}

/// `mbb_direct_bit_r` and `mbb_direct_bit`: bits of two records, not a
/// readback and its setpoint.
async fn assert_bit_access<B: BackendFromPv + SignalBackend<bool> + 'static>() {
    let bit_r: SignalR<bool, B> = signal("BSRS:mbb_direct.B0");
    let bit: SignalRW<bool, B> = signal("BSRS:mbb_direct_rw.B0");
    bit_r.connect(CONNECT).await.unwrap();
    bit.connect(CONNECT).await.unwrap();
    bit_r.get().await.unwrap();

    let initial = bit.get().await.unwrap();
    bit.put(!initial).await.await.unwrap();
    assert_eq!(bit.get().await.unwrap(), !initial, "{}", bit.source());
    bit.put(initial).await.await.unwrap();
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_bit_of_an_mbb_direct_record_is_a_bool_over_ca() {
    run(assert_bit_access::<EpicsCaBackend<bool>>());
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_bit_of_an_mbb_direct_record_is_a_bool_over_pva() {
    run(assert_bit_access::<EpicsPvaBackend<bool>>());
}
