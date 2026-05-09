//! `cirrus frame-source` — D21 multi-process scaffold.
//!
//! Doc 08 D21 establishes the deployment model: the frame data plane
//! lives in a separate OS process from the RunEngine, talking to the
//! engine only through the Document plane. This subcommand is the
//! **frame-source binary** half of that split.
//!
//! In a typical deployment:
//!
//! ```text
//! ┌──────────────────────────┐  ZMQ Documents  ┌────────────────────┐
//! │  cirrus frame-source     │ ──────────────► │ cirrus repl / qs   │
//! │  (PVA / rogue / ... )    │                 │ (RunEngine + plan) │
//! │  writes frames to disk   │                 │                    │
//! │  emits Resource / Datum  │                 │                    │
//! └──────────────────────────┘                 └────────────────────┘
//! ```
//!
//! Bulk frame bytes never cross the process boundary — the
//! `BinaryFrameSink` / `Hdf5FrameSink` opens the output file and
//! streams payloads locally. The Document stream tells the engine
//! "here's where the data went" via `StreamResource` /
//! `StreamDatum`.
//!
//! ## Status
//!
//! This subcommand is **scaffolding** — the wire format
//! (`ZmqDocumentSource` / `Sink`) is fully implemented and
//! round-trip tested in cirrus-callbacks. The frame-acquisition
//! backends (PVA, rogue) are feature-gated and out of scope for
//! the scaffold; this CLI presently:
//!
//! 1. accepts `--doc-pub-address` (where it would publish)
//! 2. accepts `--source` (which backend to spin up)
//! 3. validates the args and prints the ZMQ envelope it WOULD use
//!
//! Real backends are wired in via the same trait surface the
//! `cirrus-stream::PvaMonitorSource` already uses; the frame-source
//! subcommand drains them into a local sink + a `ZmqDocumentSink`.

use clap::Args;

/// CLI arguments for `cirrus frame-source`.
#[derive(Args, Debug)]
pub struct FrameSourceArgs {
    /// Backend identifier. Currently a placeholder — accepted values
    /// are `pva` (NTNDArray monitor) and `rogue` (DMA source). Both
    /// require the corresponding feature build at compile time;
    /// neither is wired in this scaffold.
    #[arg(long, default_value = "pva")]
    pub source: String,

    /// Output file path for the local frame writer (HDF5 or
    /// length-prefixed binary, decided by extension).
    #[arg(long)]
    pub output: std::path::PathBuf,

    /// ZMQ PUB endpoint where this source publishes Document-plane
    /// messages (e.g. `ipc:///tmp/cirrus-frames.sock` or
    /// `tcp://*:5577`). RunEngine processes connect here via
    /// `ZmqDocumentSource`.
    #[arg(long, default_value = "tcp://*:5577")]
    pub doc_pub_address: String,

    /// PUB envelope prefix for fan-out routing. Empty by default so
    /// any subscriber sees every Document.
    #[arg(long, default_value = "")]
    pub doc_prefix: String,

    /// PVA / rogue source URI. For PVA = NTNDArray PV name; for rogue
    /// = device path.
    #[arg(long)]
    pub source_uri: Option<String>,
}

/// Entry point. Returns process exit code.
pub fn run(args: FrameSourceArgs) -> i32 {
    println!("cirrus frame-source — D21 multi-process scaffold");
    println!("  backend           = {}", args.source);
    println!("  output            = {}", args.output.display());
    println!("  doc-pub-address   = {}", args.doc_pub_address);
    println!("  doc-prefix        = {:?}", args.doc_prefix);
    println!(
        "  source-uri        = {}",
        args.source_uri.as_deref().unwrap_or("<unset>")
    );

    match args.source.as_str() {
        "pva" => {
            eprintln!(
                "\nthe PVA backend is feature-gated; build cirrus-cli with the matching \
                 feature and a future commit will wire `cirrus-stream::PvaMonitorSource` + \
                 `Hdf5FrameSink` into this subcommand. For now the wire format and the \
                 envelope shape are validated by the `ZmqDocumentSource`/`Sink` round-trip \
                 tests in cirrus-callbacks."
            );
            0
        }
        "rogue" => {
            eprintln!(
                "\nthe rogue backend is Phase-2 (doc 07 P2-A/P2-B). Until that ships, \
                 this subcommand validates only the Document-plane half of the IPC."
            );
            0
        }
        other => {
            eprintln!("\nunknown --source {other:?}; expected `pva` or `rogue`");
            1
        }
    }
}
