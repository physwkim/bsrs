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
//! ## Build
//!
//! Without `--features frame-source`, this subcommand is a stub that
//! validates args. With the feature on, it wires
//! `cirrus-stream::PvaMonitorSource` → `Hdf5FrameSink` →
//! `ZmqDocumentSink`.

use clap::Args;

/// CLI arguments for `cirrus frame-source`.
#[derive(Args, Debug)]
pub struct FrameSourceArgs {
    /// Backend identifier. `pva` = NTNDArray monitor (requires
    /// `--features frame-source`). `rogue` is reserved for the
    /// Phase-2 milestone and currently exits with a notice.
    #[arg(long, default_value = "pva")]
    pub source: String,

    /// Output file path for the local frame writer. Extension
    /// `.h5` selects the HDF5 sink (NeXus layout); anything else
    /// falls back to the length-prefixed binary sink.
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

    /// PVA NTNDArray PV name (required when `--source pva`).
    #[arg(long)]
    pub source_uri: Option<String>,

    /// Logical detector name embedded in StreamResource docs and
    /// (for HDF5) the NXdetector group path.
    #[arg(long, default_value = "det")]
    pub name: String,

    /// Bytes-per-frame hint for the DataKey shape (0 = generic
    /// 1-D byte stream).
    #[arg(long, default_value_t = 0)]
    pub payload_size: u64,
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

    #[cfg(not(feature = "frame-source"))]
    {
        eprintln!(
            "\nbuild without `--features frame-source`: this subcommand validates args \
             and exits. Wire format is exercised in cirrus-callbacks (ZmqDocumentSource \
             ↔ Sink round-trip). Rebuild with `--features frame-source` to actually \
             acquire frames."
        );
        let _ = args;
        0
    }

    #[cfg(feature = "frame-source")]
    {
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("tokio runtime: {e}");
                return 1;
            }
        };
        rt.block_on(run_wired(args))
    }
}

#[cfg(feature = "frame-source")]
async fn run_wired(args: FrameSourceArgs) -> i32 {
    use cirrus_callbacks::{Serializer, ZmqDocumentSink};
    use cirrus_event_model::Document;
    use cirrus_protocols_async::{DetectorWriter, FrameSink, FrameSource, StreamAsset};
    use cirrus_stream::sinks::Hdf5FrameSink;
    use cirrus_stream::sources::PvaMonitorSource;
    use cirrus_stream::FramePipe;
    use epics_pva_rs::client::PvaClient;
    use futures::StreamExt;
    use std::sync::Arc;

    if args.source != "pva" {
        match args.source.as_str() {
            "rogue" => {
                eprintln!(
                    "\nthe rogue backend is Phase-2 (doc 07 P2-A/P2-B). Until that \
                     ships, the wired frame-source supports `--source pva` only."
                );
                return 1;
            }
            other => {
                eprintln!("\nunknown --source {other:?}; expected `pva`");
                return 1;
            }
        }
    }
    let pv = match args.source_uri.as_deref() {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            eprintln!("\n--source pva requires --source-uri <NTNDArray-PV-name>");
            return 1;
        }
    };

    let writer: Arc<Hdf5FrameSink> = Arc::new(Hdf5FrameSink::new(
        args.name.clone(),
        args.output.clone(),
        args.payload_size,
    ));
    let pipe = match FramePipe::builder()
        .primary(writer.clone() as Arc<dyn FrameSink>)
        .start()
    {
        Ok(p) => Arc::new(p),
        Err(e) => {
            eprintln!("frame pipe: {e}");
            return 1;
        }
    };

    let client = match PvaClient::new() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("pva client: {e}");
            return 1;
        }
    };
    let source = Arc::new(PvaMonitorSource::new(client, pv.clone()));
    if let Err(e) = source.start().await {
        eprintln!("pva start: {e}");
        return 1;
    }

    let doc_sink: Arc<ZmqDocumentSink> = match ZmqDocumentSink::bind(&args.doc_pub_address) {
        Ok(s) => match s.with_prefix(args.doc_prefix.as_bytes().to_vec()) {
            Ok(s) => Arc::new(s.with_serializer(Serializer::Msgpack)),
            Err(e) => {
                eprintln!("zmq prefix: {e}");
                return 1;
            }
        },
        Err(e) => {
            eprintln!("zmq bind {}: {e}", args.doc_pub_address);
            return 1;
        }
    };

    // Drain source frames into the pipe.
    let pipe_clone = pipe.clone();
    let mut frames = source.frames();
    let frame_loop = tokio::spawn(async move {
        while let Some(f) = frames.next().await {
            pipe_clone.send(f).await;
        }
    });

    // Watch indices_written; on each change, collect the new
    // StreamAsset docs and publish via ZMQ.
    let writer_clone = writer.clone();
    let doc_sink_clone = doc_sink.clone();
    let doc_loop = tokio::spawn(async move {
        let mut rx = writer_clone.observe_indices_written();
        let mut last: u64 = 0;
        loop {
            if rx.changed().await.is_err() {
                break;
            }
            let n = *rx.borrow();
            if n == last {
                continue;
            }
            last = n;
            // Raw PV→file→ZMQ streaming with no open run: there is no
            // EventDescriptor to reference, so the descriptor is left empty.
            let mut stream = writer_clone.collect_stream_docs(n, "");
            while let Some(asset) = stream.next().await {
                let doc = match asset {
                    StreamAsset::Resource(r) => Document::StreamResource(r),
                    StreamAsset::Datum(d) => Document::StreamDatum(d),
                };
                if let Err(e) = cirrus_engine::DocumentSink::dispatch(&*doc_sink_clone, &doc).await
                {
                    tracing::warn!("zmq dispatch: {e}");
                }
            }
        }
    });

    println!(
        "\nfrom PV {pv:?} → {output} → ZMQ PUB {addr}\nCtrl-C to stop.",
        output = args.output.display(),
        addr = args.doc_pub_address
    );

    // Block on Ctrl-C.
    if let Err(e) = tokio::signal::ctrl_c().await {
        eprintln!("ctrl_c handler: {e}");
    }
    println!("\nshutting down...");

    let _ = source.stop().await;
    frame_loop.abort();
    doc_loop.abort();
    if let Err(e) = writer.close().await {
        eprintln!("writer close: {e}");
    }
    0
}
