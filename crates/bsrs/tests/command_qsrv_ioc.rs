//! CA and PVA command backends against the epics-rs `qsrv-ioc` example: the
//! PV types `connect` accepts and refuses, and a trigger that processes
//! DEMO:AO through its PROC field.
#![cfg(all(feature = "ca", feature = "pva"))]

mod example_ioc;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bsrs::backends::epics_ca::{CaCommandBackend, EpicsCaBackend};
use bsrs::backends::epics_pva::PvaCommandBackend;
use bsrs::core::bsrs_runtime;
use bsrs::core::error::BsrsError;
use bsrs::devices::{Command, CommandBackend, Device};
use bsrs::protocols_async::SignalBackend;

const CONNECT: Duration = Duration::from_secs(10);

fn run(test: impl Future<Output = ()>) {
    example_ioc::boot("qsrv-ioc", "qsrv-ioc");
    bsrs_runtime().block_on(test);
}

fn refusal(e: BsrsError) -> String {
    match e {
        BsrsError::Datatype(why) => why,
        e => panic!("not a datatype refusal: {e}"),
    }
}

/// DEMO:BI is a bi, an enum over CA and an NTEnum over PVA.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn an_enum_pv_is_a_command() {
    run(async {
        CaCommandBackend::new("DEMO:BI")
            .connect(CONNECT)
            .await
            .unwrap();
        PvaCommandBackend::new("DEMO:BI")
            .connect(CONNECT)
            .await
            .unwrap();
    });
}

/// DEMO:AO is an ao, a double: ophyd-async's `TypeError`s, word for word.
#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_float_pv_is_not_a_command() {
    run(async {
        let ca = CaCommandBackend::new("DEMO:AO").connect(CONNECT).await;
        assert_eq!(
            refusal(ca.unwrap_err()),
            "ca://DEMO:AO is not a numeric PV (dbr=6); \
             CaCommandBackend requires a scalar numeric PV"
        );
        let pva = PvaCommandBackend::new("DEMO:AO").connect(CONNECT).await;
        assert_eq!(
            refusal(pva.unwrap_err()),
            "pva://DEMO:AO is not a scalar boolean, scalar numeric, or enum PV \
             (typeid='epics:nt/NTScalar:1.0', specifier='d'); PvaCommandBackend \
             requires a scalar boolean, scalar numeric, or enum PV"
        );
    });
}

/// Each trigger of DEMO:AO.PROC processes the record, which stamps it anew.
async fn assert_trigger_processes<B>(cmd: &Command<B>)
where
    B: CommandBackend<Args = (), Output = ()>,
{
    let ao = EpicsCaBackend::<f64>::new("DEMO:AO");
    ao.connect(CONNECT).await.unwrap();
    cmd.connect(CONNECT).await.unwrap();
    let before = ao.get_reading().await.unwrap();
    cmd.trigger().await.await.unwrap();
    let after = ao.get_reading().await.unwrap();
    assert!(after.timestamp > before.timestamp, "{}", cmd.source());
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_trigger_processes_the_record_over_ca_and_pva() {
    run(async {
        let ca = CaCommandBackend::new("DEMO:AO.PROC");
        assert_eq!(ca.write_pv(), "DEMO:AO.PROC");
        let ca = Command::new(Arc::new(ca), "proc");
        assert_eq!(ca.source(), "ca://DEMO:AO.PROC");
        assert_trigger_processes(&ca).await;
        let pva = Command::new(Arc::new(PvaCommandBackend::new("DEMO:AO.PROC")), "proc");
        assert_eq!(pva.source(), "pva://DEMO:AO.PROC");
        assert_trigger_processes(&pva).await;
    });
}

#[derive(bsrs::devices::Device)]
struct Demo {
    name: String,
    #[command("{prefix}AO.PROC")]
    process: Command<CaCommandBackend>,
}

#[test]
#[ignore = "needs BSRS_EPICS_RS; see tests/example_ioc"]
fn a_derived_command_connects_with_its_device() {
    run(async {
        let demo = Demo::new_named("DEMO:", "demo");
        assert_eq!(demo.process.name(), "demo-process");
        Device::connect_all_boxed(&*demo, CONNECT).await.unwrap();
        assert_trigger_processes(&demo.process).await;
    });
}
