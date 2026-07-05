//! `bsrs console` — a fused local Lua REPL **and** a bsrs-qs server sharing
//! one `RunEngine`.
//!
//! One process presents two faces onto a single engine + queue + device
//! registry:
//!
//! - a local, interactive Lua prompt (the `bsrs repl` surface), driven on the
//!   main thread;
//! - the bluesky-queueserver control (REP) + document (PUB) sockets, so remote
//!   `bsrs qs …` clients — and the `lua_eval` RPC — drive the *same* engine.
//!
//! ## What is shared, what is not
//!
//! - **Shared:** the `Arc<RunEngine>` (via the `engine_slot` the server and the
//!   daemon-side `ManagerLuaState` already share), the plan queue, the device
//!   registry, and the document stream (the engine owns the PUB sink, so both a
//!   local `RE:run` and a remote `queue_start` publish to the same subscribers).
//! - **Not shared:** the Lua *state*. `mlua::Lua` is `!Send`, so the local
//!   prompt (main thread) and the remote `lua_eval` (tokio blocking pool) each
//!   run their own Lua state built by [`build_shared_lua`]. A global created at
//!   the local prompt is not visible to `lua_eval`, and vice versa — but they
//!   resolve the same engine and the same device instances.
//!
//! ## Concurrency
//!
//! The engine runs at most one plan at a time and enforces it itself
//! (`RunEngine::run_async` rejects a concurrent call). So a local `RE:run`
//! fired while the queue worker is mid-plan — or a `queue_start` fired while a
//! local plan runs — is rejected with a clear error rather than corrupting the
//! shared run loop. No extra coordination is layered on top.
//!
//! ## Environment lifecycle
//!
//! The console **pre-opens** the environment at startup: it builds the engine,
//! seeds the shared slot, and marks the qs state `idle`, so the prompt can
//! drive `RE` immediately and remote `status` reports the environment open. A
//! remote `environment_open` therefore reports already-open. Remote
//! `environment_close` is out of scope for this prototype — the console owns the
//! environment for its lifetime.

use std::path::PathBuf;
use std::sync::Arc;

use bsrs::backends::soft::{SoftDetector, SoftMotor};
use bsrs::callbacks::ZmqDocumentSink;
use bsrs::core::msg::{MovableObj, ReadableObj};
use bsrs::core::runtime::bsrs_runtime;
use bsrs::engine::{DocumentSink, RunEngine};
use bsrs::host::checkpoint_store::{default_path as default_ckpt_path, JsonlCheckpointStore};
use bsrs::host::manager_lua::{build_shared_lua, ManagerLuaState};
use bsrs::qs::{EState, Registry, Server};
use clap::Args;
use tokio::sync::Mutex as TMutex;

/// Arguments for `bsrs console`.
#[derive(Args, Debug)]
pub struct ConsoleArgs {
    /// Control REP socket address (bluesky-queueserver control port).
    #[arg(long, default_value = "tcp://*:60615")]
    control: String,

    /// Document PUB socket address. A bluesky `RemoteDispatcher` connects here
    /// to receive documents from plans run at the local prompt *or* from the
    /// queue. Owned by the pre-opened engine (the server does not bind it).
    #[arg(long, default_value = "tcp://*:60625")]
    documents: String,

    /// Register `n` `SoftDetector`s named `det1`, `det2`, … (0 to skip).
    #[arg(long, default_value_t = 1)]
    soft_detectors: usize,

    /// Register `n` `SoftMotor`s named `m1`, `m2`, … (0 to skip).
    #[arg(long, default_value_t = 1)]
    soft_motors: usize,

    /// Optional permissions.toml path gating JSON-RPC methods by user group.
    #[arg(long)]
    permissions: Option<PathBuf>,

    /// Optional checkpoint JSONL path (default `~/.bsrs/checkpoints.jsonl`).
    #[arg(long)]
    checkpoints: Option<PathBuf>,

    /// Optional Lua file executed before the prompt opens (a `~/.bsrsrc.lua`).
    #[arg(long)]
    init: Option<PathBuf>,
}

/// Entry point — returns a process exit code. Runs from a sync context (like
/// `repl::run`) so the blocking rustyline loop can `block_on` the bsrs runtime
/// for `RE:run`, while the qs server runs concurrently on that same runtime.
pub fn run(args: ConsoleArgs) -> i32 {
    // Logs to stderr so they do not corrupt the rustyline prompt on stdout.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .compact()
        .try_init();

    // Bootstrap CA before any runtime is entered (mirrors repl/manager), so a
    // later `ca_*` factory does not trip the nested-runtime check.
    #[cfg(feature = "ca")]
    bsrs::host::ca_devices::bootstrap_ca();

    // 1. Device registry — shared by the queue worker, remote lua_eval, and the
    //    local prompt. Soft devices only in this prototype (CA is a follow-up).
    let mut registry = Registry::new();
    for i in 1..=args.soft_detectors {
        let name = format!("det{i}");
        let det = SoftDetector::new(&name);
        registry.register_readable(&name, det as Arc<dyn ReadableObj>);
    }
    for i in 1..=args.soft_motors {
        let name = format!("m{i}");
        let motor = Arc::new(SoftMotor::new(&name, Some(0.0)));
        registry.register_readable(&name, motor.clone() as Arc<dyn ReadableObj>);
        registry.register_movable(&name, motor as Arc<dyn MovableObj>);
    }
    registry.register_plan_count("count");

    // 2. Document PUB sink → the pre-opened engine owns it (so every plan, local
    //    or queued, publishes to the same subscribers).
    let sink = match ZmqDocumentSink::bind(&args.documents) {
        Ok(s) => Arc::new(s) as Arc<dyn DocumentSink>,
        Err(e) => {
            eprintln!(
                "bsrs console: failed to bind document PUB {}: {e}",
                args.documents
            );
            return 2;
        }
    };
    let re = Arc::new(RunEngine::new(vec![sink]));

    // Crash-recovery audit trail — install the checkpoint hook directly (the
    // server would install it on `environment_open`, which the console
    // pre-empts by pre-opening).
    let ckpt_path = args.checkpoints.clone().unwrap_or_else(default_ckpt_path);
    let ckpt_store = Arc::new(JsonlCheckpointStore::new(ckpt_path.clone()));
    re.set_checkpoint_hook(ckpt_store.into_hook());

    // 3. Pre-seed the shared engine slot. The server and the daemon-side Lua
    //    bridge both read this slot, so remote lua_eval resolves the same engine.
    let engine_slot = Arc::new(TMutex::new(Some(re.clone())));
    let evaluator: Arc<dyn bsrs::qs::LuaEvaluator> = Arc::new(ManagerLuaState::new(
        engine_slot.clone(),
        Arc::new(registry.clone()),
    ));

    // 4. Build the server sharing the slot + registry + evaluator. It must NOT
    //    bind its own PUB socket — the engine already owns `args.documents`.
    let mut sb = Server::builder()
        .control_address(&args.control)
        .without_document_socket()
        .registry(registry.clone())
        .engine_slot(engine_slot.clone())
        .lua_evaluator(evaluator);
    if let Some(path) = &args.permissions {
        sb = sb.permissions_path(path.clone());
    }
    let server = match sb.build() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("bsrs console: server bind failed: {e}");
            return 2;
        }
    };

    // Mark the pre-opened environment idle so remote `status` reports it open
    // (`worker_environment_exists` is derived from the seeded slot).
    server.state_arc().lock().unwrap().state = Some(EState::Idle);

    let control_endpoint = server.control_endpoint().to_string();
    let shutdown = server.shutdown_handle();
    // Run the server concurrently on the bsrs runtime; the REPL owns the main
    // thread. `RunEngine`'s single-plan guard keeps the two faces from
    // executing plans at the same time.
    let _server_task = bsrs_runtime().spawn(async move {
        if let Err(e) = server.run_async().await {
            tracing::error!(target: "bsrs-console", "qs server exited with error: {e}");
        }
    });

    eprintln!(
        "bsrs console: qs server listening\n  control:   {control_endpoint}\n  documents: {}",
        args.documents
    );

    // 5. Local Lua state on the SAME engine + registry devices. Built on the
    //    main thread (mlua is !Send); independent from the remote lua_eval state.
    let lua = match build_shared_lua(re, &registry) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bsrs console: failed to build Lua state: {e}");
            shutdown.shutdown();
            return 2;
        }
    };

    if let Some(path) = &args.init {
        if let Err(e) = crate::repl::run_file(&lua, path) {
            eprintln!("bsrs console: --init failed: {e}");
            shutdown.shutdown();
            return 1;
        }
    }

    let code = crate::repl::interactive_loop(&lua);
    // Leaving the prompt tears down the server.
    shutdown.shutdown();
    code
}
