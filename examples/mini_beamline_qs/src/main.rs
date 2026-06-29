//! `mini-beamline-qs` — example downstream bsrs-qs daemon that
//! registers composite `Dcm` + `Table` devices alongside the plain
//! per-axis CA motors of the epics-rs mini-beamline IOC.
//!
//! Demonstrates the `bsrs-host` library API: how a beamline author
//! writes their own ophyd-style hierarchical devices in Rust and stands
//! up a queue server that exposes them through both the JSON-RPC queue
//! path *and* the daemon-side Lua REPL.
//!
//! ## Usage
//!
//! ```sh
//! # terminal 1 — the IOC (from physwkim/epics-rs)
//! ./mini_ioc examples/mini-beamline/ioc/st.cmd
//!
//! # terminal 2 — bump motor velocities (default 0.2 → 30s timeout
//! # on a 1-unit step is too slow for scans)
//! for pv in mini:ph:mtr.VELO mini:dot:mtrx.VELO mini:dot:mtry.VELO; do
//!     caput "$pv" 5
//! done
//!
//! # terminal 3 — the daemon
//! cargo run -p mini-beamline-qs --release
//!
//! # terminal 4 — attach a REPL
//! cargo run -p bsrs-cli --release -- qs \
//!     --address tcp://localhost:60615 repl
//! bsrs> dcm:move_energy_keV(8.0)
//! bsrs> dcm:locate()
//! bsrs> RE:run(scan({ph_det}, dcm, 6, 12, 7))
//! bsrs> table:move_to_xy(1.0, 2.0)
//! bsrs> table:at_xy()
//! bsrs> RE:run(count({table, ph_det}, 5))
//! ```

#![deny(missing_docs)]

mod dcm;
mod table;

use std::sync::Arc;

use bsrs::core::msg::{LocatableObj, MovableObj, ReadableObj};
use bsrs::host::checkpoint_store::{default_path as default_ckpt_path, JsonlCheckpointStore};
use bsrs::host::manager_lua::ManagerLuaState;
use bsrs::qs::{Registry, Server};
use clap::Parser;
use tokio::sync::Mutex as TMutex;

/// CLI arguments. Mirror `bsrs qs-manager` but with mini-beamline
/// devices hardcoded — no `--ca-motor` flag is needed since the PVs
/// are fixed for this example.
#[derive(Parser, Debug)]
#[command(name = "mini-beamline-qs", version, about, long_about = None)]
struct Args {
    /// Control REP socket address.
    #[arg(long, default_value = "tcp://*:60615")]
    control: String,

    /// Document PUB socket address.
    #[arg(long, default_value = "tcp://*:60625")]
    documents: String,

    /// Optional checkpoint JSONL file. Defaults to
    /// `~/.bsrs/checkpoints.jsonl`.
    #[arg(long)]
    checkpoints: Option<std::path::PathBuf>,
}

fn main() {
    // CA bootstrap must run BEFORE the tokio runtime is built (rule:
    // `ca_context()` calls `block_on` once and panics from inside an
    // active runtime). Same pattern as `bsrs qs-manager`.
    bsrs::host::ca_devices::bootstrap_ca();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    std::process::exit(rt.block_on(run(Args::parse())));
}

async fn run(args: Args) -> i32 {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .compact()
        .init();

    let mut reg = Registry::new();
    reg.register_plan_count("count");

    // ---- single-axis CA devices (so plain plans still work) ----
    let ph_mtr = match bsrs::host::ca_devices::CaMotor::connect_async(
        "ph_mtr",
        "mini:ph:mtr.VAL",
        "mini:ph:mtr.RBV",
    )
    .await
    {
        Ok(m) => m,
        Err(e) => {
            eprintln!("mini-beamline-qs: ph_mtr: {e}");
            return 2;
        }
    };
    reg.register_readable("ph_mtr", ph_mtr.clone() as Arc<dyn ReadableObj>);
    reg.register_movable("ph_mtr", ph_mtr.clone() as Arc<dyn MovableObj>);
    reg.register_locatable("ph_mtr", ph_mtr as Arc<dyn LocatableObj>);
    tracing::info!(target: "mini-beamline-qs", "registered ph_mtr");

    let ph_det =
        match bsrs::host::ca_devices::CaDetector::connect_async("ph_det", "mini:ph:DetValue_RBV")
            .await
        {
            Ok(d) => d,
            Err(e) => {
                eprintln!("mini-beamline-qs: ph_det: {e}");
                return 2;
            }
        };
    reg.register_readable("ph_det", ph_det as Arc<dyn ReadableObj>);
    tracing::info!(target: "mini-beamline-qs", "registered ph_det");

    // ---- composite Dcm: energy + theta_rbv ----
    let dcm = match dcm::Dcm::connect(
        "dcm",
        "mini:BraggEAO",
        "mini:BraggERdbkAO",
        "mini:BraggThetaRdbkAO",
    )
    .await
    {
        Ok(d) => d,
        Err(e) => {
            eprintln!("mini-beamline-qs: dcm: {e}");
            return 2;
        }
    };
    reg.register_readable("dcm", dcm.clone() as Arc<dyn ReadableObj>);
    reg.register_movable("dcm", dcm.clone() as Arc<dyn MovableObj>);
    reg.register_locatable("dcm", dcm.clone() as Arc<dyn LocatableObj>);
    reg.register_lua_methods("dcm", dcm);
    tracing::info!(target: "mini-beamline-qs", "registered composite dcm");

    // ---- composite Table: x + y ----
    let table = match table::Table::connect(
        "table",
        "mini:dot:mtrx.VAL",
        "mini:dot:mtrx.RBV",
        "mini:dot:mtry.VAL",
        "mini:dot:mtry.RBV",
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            eprintln!("mini-beamline-qs: table: {e}");
            return 2;
        }
    };
    reg.register_readable("table", table.clone() as Arc<dyn ReadableObj>);
    reg.register_lua_methods("table", table);
    tracing::info!(target: "mini-beamline-qs", "registered composite table");

    // ---- daemon-side Lua bridge + checkpoint store ----
    let engine_slot = Arc::new(TMutex::new(None));
    let registry_for_lua = Arc::new(reg.clone());
    let evaluator: Arc<dyn bsrs::qs::LuaEvaluator> =
        Arc::new(ManagerLuaState::new(engine_slot.clone(), registry_for_lua));

    let ckpt_path = args.checkpoints.clone().unwrap_or_else(default_ckpt_path);
    if let Some(prev) = JsonlCheckpointStore::latest(&ckpt_path) {
        tracing::info!(
            target: "mini-beamline-qs",
            "checkpoint store: previous record at ts={} run={:?}",
            prev.timestamp_ns,
            prev.run_uid,
        );
    }
    let ckpt_store = Arc::new(JsonlCheckpointStore::new(ckpt_path.clone()));
    let ckpt_hook = ckpt_store.into_hook();

    let server = match Server::builder()
        .control_address(&args.control)
        .document_address(&args.documents)
        .registry(reg)
        .engine_slot(engine_slot)
        .lua_evaluator(evaluator)
        .checkpoint_hook(ckpt_hook)
        .build()
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("mini-beamline-qs: bind failed: {e}");
            return 2;
        }
    };
    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\nmini-beamline-qs: shutting down");
            shutdown.shutdown();
        }
    });

    println!(
        "mini-beamline-qs listening:\n  control:   {}\n  documents: {}",
        args.control, args.documents
    );
    match server.run_async().await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("mini-beamline-qs: {e}");
            1
        }
    }
}
