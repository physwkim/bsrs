//! `cirrus` CLI — bluesky-queueserver-compatible workflow without
//! requiring the Python `bluesky-queueserver` package to be installed.
//!
//! Two top-level subcommands:
//!
//! - `cirrus qs-manager` — start a `cirrus-qs` server that speaks the
//!   bluesky-queueserver JSON-RPC-over-0MQ protocol on the control port
//!   and emits Documents on the document port.
//! - `cirrus qs <command>` — REQ-side client. Mirrors the most common
//!   `qserver` subcommands: `ping`, `status`, `environment open/close`,
//!   `queue add/get/remove/start`, `re pause/resume/abort/halt`,
//!   `allowed plans/devices`.

#![deny(missing_docs)]

mod client;
mod lua_env;
#[cfg(feature = "tiled")]
mod lua_tiled;
mod manager;
mod repl;

use clap::{Parser, Subcommand};

/// Top-level CLI.
#[derive(Parser, Debug)]
#[command(name = "cirrus", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: TopCmd,
}

#[derive(Subcommand, Debug)]
enum TopCmd {
    /// Start a cirrus-qs server (replacement for `start-re-manager`).
    QsManager(manager::ManagerArgs),
    /// REQ-side client (replacement for `qserver`).
    Qs(client::ClientArgs),
    /// Interactive Lua REPL with cirrus types pre-registered. Drives an
    /// in-process `RunEngine`; no qs-manager required. IPython-like
    /// development surface for plans.
    Repl(repl::ReplArgs),
}

fn main() {
    let cli = Cli::parse();
    let exit = match cli.command {
        // REPL runs from a sync context so RE:run can `block_on` the
        // cirrus runtime to drive plans.
        TopCmd::Repl(a) => repl::run(a),
        // Server / client paths build their own multi-thread tokio
        // runtime — neither needs the caller's runtime.
        TopCmd::QsManager(a) => {
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
    };
    std::process::exit(exit);
}
