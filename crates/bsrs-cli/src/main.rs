//! `bsrs` CLI — bluesky-queueserver-compatible workflow without
//! requiring the Python `bluesky-queueserver` package to be installed.
//!
//! Two top-level subcommands:
//!
//! - `bsrs qs-manager` — start a `bsrs-qs` server that speaks the
//!   bluesky-queueserver JSON-RPC-over-0MQ protocol on the control port
//!   and emits Documents on the document port.
//! - `bsrs qs <command>` — REQ-side client. Mirrors the most common
//!   `qserver` subcommands: `ping`, `status`, `environment open/close`,
//!   `queue add/get/remove/start`, `re pause/resume/abort/halt`,
//!   `allowed plans/devices`.

#![deny(missing_docs)]

mod client;
mod doctor;
mod frame_source;
mod manager;
mod migrate;
mod repl;

use clap::{Parser, Subcommand};

/// Top-level CLI.
#[derive(Parser, Debug)]
#[command(name = "bsrs", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: TopCmd,
}

#[derive(Subcommand, Debug)]
enum TopCmd {
    /// Start a bsrs-qs server (replacement for `start-re-manager`).
    QsManager(manager::ManagerArgs),
    /// REQ-side client (replacement for `qserver`).
    Qs(client::ClientArgs),
    /// Interactive Lua REPL with bsrs types pre-registered. Drives an
    /// in-process `RunEngine`; no qs-manager required. IPython-like
    /// development surface for plans.
    Repl(repl::ReplArgs),
    /// Validate the local environment for running bsrs: tokio,
    /// EPICS env vars, optional Tiled / Kafka reachability.
    Doctor(doctor::DoctorArgs),
    /// Inspect / migrate bsrs's on-disk state directory between
    /// versions.
    Migrate(migrate::MigrateArgs),
    /// Run a frame-source process: D21 multi-process IPC. The frame
    /// data plane stays local (writes to disk); only Document-plane
    /// messages cross to the RunEngine via ZMQ PUB.
    FrameSource(frame_source::FrameSourceArgs),
}

fn main() {
    let cli = Cli::parse();
    let exit = match cli.command {
        // REPL runs from a sync context so RE:run can `block_on` the
        // bsrs runtime to drive plans.
        TopCmd::Repl(a) => repl::run(a),
        // Server / client paths build their own multi-thread tokio
        // runtime — neither needs the caller's runtime.
        TopCmd::QsManager(a) => {
            // Bootstrap the CA backend's global client BEFORE the
            // tokio runtime starts. `ca_context()` block_on's
            // `CaClient::new()` once and panics if invoked from
            // inside an active runtime — calling it here from sync
            // main pre-warms the cache so subsequent
            // `CaMotor::connect_async` / `CaDetector::connect_async`
            // calls in the daemon flow stay fully async.
            #[cfg(feature = "ca")]
            bsrs_host::ca_devices::bootstrap_ca();
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(manager::run(a))
        }
        TopCmd::Qs(a) => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(client::run(a))
        }
        TopCmd::Doctor(a) => doctor::run(a),
        TopCmd::Migrate(a) => migrate::run(a),
        TopCmd::FrameSource(a) => frame_source::run(a),
    };
    std::process::exit(exit);
}
