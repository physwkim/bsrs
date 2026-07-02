//! `bsrs qs-manager` — start a bsrs-qs server.

use std::sync::Arc;

use bsrs::backends::soft::{SoftDetector, SoftMotor};
use bsrs::core::msg::{LocatableObj, MovableObj, ReadableObj};
use bsrs::qs::{Registry, Server};
use clap::Args;
use tokio::sync::Mutex as TMutex;

use bsrs::host::checkpoint_store::{default_path as default_ckpt_path, JsonlCheckpointStore};
use bsrs::host::manager_lua::ManagerLuaState;

/// Arguments for `bsrs qs-manager`.
#[derive(Args, Debug)]
pub struct ManagerArgs {
    /// Control REP socket address. Plans and engine commands flow here.
    #[arg(long, default_value = "tcp://*:60615")]
    control: String,

    /// Document PUB socket address. Bluesky `RemoteDispatcher` connects
    /// here to receive `RunStart` / `EventDescriptor` / `Event` /
    /// `RunStop` documents.
    #[arg(long, default_value = "tcp://*:60625")]
    documents: String,

    /// Register `n` `SoftDetector` instances named `det1`, `det2`, …
    /// Useful for trying out the queueserver workflow without bringing
    /// a real registry. Set to 0 to register none.
    #[arg(long, default_value_t = 1)]
    soft_detectors: usize,

    /// Register `n` `SoftMotor` instances named `m1`, `m2`, … Set to 0
    /// to skip.
    #[arg(long, default_value_t = 1)]
    soft_motors: usize,

    /// Optional Prometheus `/metrics` HTTP listener address (e.g.
    /// `127.0.0.1:9090`). Requires bsrs-qs built with the
    /// `metrics` feature; otherwise this flag logs a warning and
    /// is ignored.
    #[arg(long)]
    metrics: Option<String>,

    /// Optional path to a permissions.toml file that gates JSON-RPC
    /// methods by user group. Without this flag, the server runs
    /// permissive — every method is allowed for every caller.
    /// Callers identify themselves by `params.api_key`.
    #[arg(long)]
    permissions: Option<std::path::PathBuf>,

    /// Optional path to the checkpoint JSONL file. Each
    /// `Msg::Checkpoint` the engine emits is appended as one record
    /// (timestamp + run_uid + bsrs version). On startup, if the
    /// file already exists, the latest record is logged so an
    /// operator can answer "where was the engine when the daemon
    /// went down?". Default: `~/.bsrs/checkpoints.jsonl`.
    #[arg(long)]
    checkpoints: Option<std::path::PathBuf>,

    /// Register a CA-backed motor. Repeatable. Format:
    /// `name=val_pv,rbv_pv` — e.g.
    /// `--ca-motor ph_mtr=mini:ph:mtr.VAL,mini:ph:mtr.RBV`.
    /// (Comma rather than colon between PVs because EPICS PV names
    /// embed `:` already.) Requires the `ca` feature (default).
    #[cfg(feature = "ca")]
    #[arg(long = "ca-motor", value_name = "NAME=VAL_PV,RBV_PV")]
    ca_motor: Vec<String>,

    /// Register a CA-backed scalar detector. Repeatable. Format:
    /// `name=value_pv` — e.g.
    /// `--ca-detector ph_det=mini:ph:DetValue_RBV`.
    #[cfg(feature = "ca")]
    #[arg(long = "ca-detector", value_name = "NAME=PV")]
    ca_detector: Vec<String>,
}

/// Entry point — returns a process exit code.
pub async fn run(args: ManagerArgs) -> i32 {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .compact()
        .init();

    let mut reg = Registry::new();
    for i in 1..=args.soft_detectors {
        let name = format!("det{i}");
        let det = SoftDetector::new(&name);
        reg.register_readable(&name, det as Arc<dyn ReadableObj>);
    }
    for i in 1..=args.soft_motors {
        let name = format!("m{i}");
        let motor = Arc::new(SoftMotor::new(&name, Some(0.0)));
        reg.register_readable(&name, motor.clone() as Arc<dyn ReadableObj>);
        reg.register_movable(&name, motor as Arc<dyn MovableObj>);
    }
    reg.register_plan_count("count");

    // Register CA-backed devices supplied via flags. Bootstrap CA
    // first (sync, off-runtime). Each `--ca-motor name=val:rbv`
    // and `--ca-detector name=pv` line spawns one CaMotor /
    // CaDetector and inserts it into both the device registry
    // (so the queue worker / lua_eval see them) and triggers a
    // single connect.
    #[cfg(feature = "ca")]
    {
        // CA bootstrap was already done by `main.rs` before the
        // tokio runtime started — calling `ca_context()` here from
        // inside `async fn run` would trigger nested-runtime panic.
        for spec in &args.ca_motor {
            let (name, pvs) = match spec.split_once('=') {
                Some(p) => p,
                None => {
                    eprintln!(
                        "bsrs qs-manager: --ca-motor expects 'name=val_pv:rbv_pv', got {spec:?}"
                    );
                    return 2;
                }
            };
            let (val_pv, rbv_pv) = match pvs.split_once(',') {
                Some(p) => p,
                None => {
                    eprintln!("bsrs qs-manager: --ca-motor PVs must be 'val,rbv', got {pvs:?}");
                    return 2;
                }
            };
            let m = match bsrs::host::ca_devices::CaMotor::connect_async(name, val_pv, rbv_pv).await
            {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("bsrs qs-manager: ca_motor {name}: {e}");
                    return 2;
                }
            };
            reg.register_readable(name, m.clone() as Arc<dyn ReadableObj>);
            reg.register_movable(name, m.clone() as Arc<dyn MovableObj>);
            reg.register_locatable(name, m as Arc<dyn LocatableObj>);
            tracing::info!(target: "bsrs-qs", "registered ca_motor {name} → {val_pv} / {rbv_pv}");
        }
        for spec in &args.ca_detector {
            let (name, pv) = match spec.split_once('=') {
                Some(p) => p,
                None => {
                    eprintln!("bsrs qs-manager: --ca-detector expects 'name=pv', got {spec:?}");
                    return 2;
                }
            };
            let d = match bsrs::host::ca_devices::CaDetector::connect_async(name, pv).await {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("bsrs qs-manager: ca_detector {name}: {e}");
                    return 2;
                }
            };
            reg.register_readable(name, d as Arc<dyn ReadableObj>);
            tracing::info!(target: "bsrs-qs", "registered ca_detector {name} → {pv}");
        }
    }

    // Share the engine slot + registry between Server and the
    // daemon-side Lua bridge so `lua_eval` resolves the same `RE`
    // and the same registered devices the queue worker sees.
    let engine_slot = Arc::new(TMutex::new(None));
    let registry_for_lua = Arc::new(reg.clone());
    let evaluator: Arc<dyn bsrs::qs::LuaEvaluator> =
        Arc::new(ManagerLuaState::new(engine_slot.clone(), registry_for_lua));

    // Crash-recovery audit trail. On startup, log the most recent
    // record (if any) so the operator can pinpoint where the engine
    // left off; install the JSONL hook on the engine the moment
    // `environment_open` populates the slot.
    let ckpt_path = args.checkpoints.clone().unwrap_or_else(default_ckpt_path);
    if let Some(prev) = JsonlCheckpointStore::latest(&ckpt_path) {
        tracing::info!(
            target: "bsrs-qs",
            "checkpoint store: previous record at ts={} run={:?} from {}",
            prev.timestamp_ns,
            prev.run_uid,
            prev.bsrs_version,
        );
    }
    // Loud warning if the prior daemon left an unfinished run. The
    // operator needs to decide whether to re-issue the plan; bsrs
    // does not auto-replay because that requires plan serialization.
    if let Some(unfinished) = JsonlCheckpointStore::unfinished_run(&ckpt_path) {
        tracing::warn!(
            target: "bsrs-qs",
            "checkpoint store: unfinished run detected — run_uid={:?} \
             reached a checkpoint at ts={} but never closed. \
             The previous daemon likely terminated mid-plan; re-issue \
             the plan manually if recovery is desired.",
            unfinished.run_uid,
            unfinished.timestamp_ns,
        );
    }
    let ckpt_store = Arc::new(JsonlCheckpointStore::new(ckpt_path.clone()));
    let ckpt_hook = ckpt_store.clone().into_hook();
    tracing::info!(
        target: "bsrs-qs",
        "checkpoint hook will append to {} on environment_open",
        ckpt_path.display(),
    );

    let mut sb = Server::builder()
        .control_address(&args.control)
        .document_address(&args.documents)
        .registry(reg)
        .engine_slot(engine_slot)
        .lua_evaluator(evaluator)
        .checkpoint_hook(ckpt_hook);
    if let Some(addr) = &args.metrics {
        sb = sb.metrics_address(addr);
    }
    if let Some(path) = &args.permissions {
        sb = sb.permissions_path(path.clone());
    }
    let server = match sb.build() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("bsrs qs-manager: bind failed: {e}");
            return 2;
        }
    };
    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\nbsrs qs-manager: shutting down");
            shutdown.shutdown();
        }
    });

    println!(
        "bsrs qs-manager listening:\n  control:   {}\n  documents: {}",
        args.control, args.documents
    );
    match server.run_async().await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("bsrs qs-manager: {e}");
            1
        }
    }
}
