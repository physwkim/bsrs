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
mod manager;

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
}

fn main() {
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let exit = rt.block_on(async {
        match cli.command {
            TopCmd::QsManager(a) => manager::run(a).await,
            TopCmd::Qs(a) => client::run(a).await,
        }
    });
    std::process::exit(exit);
}
