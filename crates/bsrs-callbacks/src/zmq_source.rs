//! `ZmqDocumentSource` — subscribe to a 0MQ PUB stream of Documents
//! emitted by a [`crate::ZmqDocumentSink`] and re-broadcast them
//! through a `RunEngine`'s subscriber chain. Forms the consume side
//! of bsrs's Document-plane IPC (doc 08 D21).
//!
//! Wire format matches `ZmqDocumentSink`:
//! `b"<prefix> <name> <serialized_doc>"`, three space-separated
//! fields. JSON or msgpack body.
//!
//! ## Typical use
//!
//! `bsrs-frame-source` (or any other process that emits a
//! Document stream — e.g. a hardware writer that publishes
//! `StreamResource` / `StreamDatum`) PUBs to an IPC or TCP
//! endpoint. The RunEngine process subscribes:
//!
//! ```ignore
//! use std::sync::Arc;
//! use bsrs_callbacks::{Serializer, ZmqDocumentSource};
//! use bsrs_engine::RunEngine;
//!
//! # async fn ex(re: Arc<RunEngine>) -> bsrs_core::Result<()> {
//! let mut src = ZmqDocumentSource::connect("tcp://localhost:5577")?
//!     .with_serializer(Serializer::Msgpack)
//!     .with_subscribe_prefix(b"");
//! src.run_into_engine(re).await?;
//! # Ok(()) }
//! ```
//!
//! Each Document received on the wire is forwarded via
//! `RunEngine::subscribe`'d callbacks AND any `DocumentSink`s
//! attached to the engine — same path as engine-internal Documents.

#![cfg(feature = "zmq")]

use bsrs_core::error::{BsrsError, Result};
use bsrs_engine::RunEngine;
use bsrs_event_model::Document;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio_util::sync::CancellationToken;

pub use crate::zmq_sink::Serializer;

/// 0MQ SUB-side document receiver.
///
/// The socket is wrapped in `Arc<StdMutex<...>>` so the blocking
/// `recv_bytes` can be dispatched to `tokio::task::spawn_blocking`
/// — without that, the 250ms `set_rcvtimeo` would pin a tokio worker
/// thread and starve other tasks on a single-thread runtime.
pub struct ZmqDocumentSource {
    socket: Arc<StdMutex<zmq::Socket>>,
    serializer: Serializer,
    cancel: CancellationToken,
}

impl ZmqDocumentSource {
    /// Open a SUB socket and `connect` it to the given endpoint
    /// (`ipc:///tmp/...` or `tcp://host:port`). The socket starts
    /// with no subscription filter — call [`with_subscribe_prefix`]
    /// to register one. Default serializer = msgpack.
    pub fn connect(endpoint: &str) -> Result<Self> {
        let ctx = zmq::Context::new();
        let sock = ctx
            .socket(zmq::SUB)
            .map_err(|e| BsrsError::Backend(format!("zmq sub: {e}")))?;
        sock.connect(endpoint)
            .map_err(|e| BsrsError::Backend(format!("zmq connect {endpoint}: {e}")))?;
        // Subscribe-all by default; users can narrow with prefix.
        sock.set_subscribe(b"")
            .map_err(|e| BsrsError::Backend(format!("zmq subscribe: {e}")))?;
        Ok(Self {
            socket: Arc::new(StdMutex::new(sock)),
            serializer: Serializer::Msgpack,
            cancel: CancellationToken::new(),
        })
    }

    /// Override the body serializer (must match the publisher's).
    pub fn with_serializer(mut self, s: Serializer) -> Self {
        self.serializer = s;
        self
    }

    /// Replace the connect-time all-match subscription with `prefix`.
    ///
    /// ZMQ SUB subscriptions are *additive*: `connect` subscribes `b""` (match
    /// all), so merely *adding* `prefix` would leave `b""` in place and every
    /// message would keep matching — the filter would be a silent no-op.
    /// Unsubscribe the `b""` default first, then subscribe `prefix`, so only
    /// prefix-matching messages arrive. Intended to be called once after
    /// `connect` to replace the default filter.
    pub fn with_subscribe_prefix(self, prefix: &[u8]) -> Self {
        {
            let s = self.socket.lock().unwrap();
            // Remove the all-match default before adding the narrower filter;
            // otherwise the additive `b""` subscription defeats `prefix`.
            let _ = s.set_unsubscribe(b"");
            let _ = s.set_subscribe(prefix);
        }
        self
    }

    /// Hand the source a [`CancellationToken`]; cancelling it ends
    /// the [`run_into_engine`] loop on the next message boundary.
    pub fn with_cancel(mut self, t: CancellationToken) -> Self {
        self.cancel = t;
        self
    }

    /// Drive the source until cancelled or the socket errors. Each
    /// received Document is broadcast into `engine`'s subscriber
    /// chain (same path as engine-internal Documents) via
    /// [`RunEngine::inject_document`].
    ///
    /// The blocking `recv_bytes` is dispatched to
    /// `tokio::task::spawn_blocking` so the 250ms wait does not pin
    /// the tokio worker; a single-thread runtime stays responsive
    /// while waiting on the SUB socket.
    pub async fn run_into_engine(&self, engine: Arc<RunEngine>) -> Result<()> {
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            let socket = self.socket.clone();
            let recv = tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
                let s = socket.lock().unwrap();
                // Set a short receive timeout so the outer loop can
                // re-check the cancel token periodically.
                let _ = s.set_rcvtimeo(250);
                match s.recv_bytes(0) {
                    Ok(b) => Ok(Some(b)),
                    Err(zmq::Error::EAGAIN) => Ok(None),
                    Err(e) => Err(BsrsError::Backend(format!("zmq recv: {e}"))),
                }
            })
            .await
            .map_err(|e| BsrsError::Backend(format!("zmq spawn_blocking join: {e}")))??;
            let envelope = match recv {
                Some(b) => b,
                None => continue,
            };
            let doc = match decode_envelope(self.serializer, &envelope) {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("zmq doc source: drop malformed envelope: {e}");
                    continue;
                }
            };
            if let Err(e) = engine.inject_document(&doc).await {
                tracing::warn!("zmq doc source: inject failed: {e}");
            }
        }
    }

    /// One-shot recv (non-blocking). Returns `Ok(Some(doc))` when a
    /// document was decoded, `Ok(None)` if no message was available,
    /// `Err` on socket / decode failure. Useful for tests.
    pub fn try_recv(&self) -> Result<Option<Document>> {
        let envelope = {
            let s = self.socket.lock().unwrap();
            let _ = s.set_rcvtimeo(0);
            match s.recv_bytes(0) {
                Ok(b) => b,
                Err(zmq::Error::EAGAIN) => return Ok(None),
                Err(e) => {
                    return Err(BsrsError::Backend(format!("zmq recv: {e}")));
                }
            }
        };
        Ok(Some(decode_envelope(self.serializer, &envelope)?))
    }
}

/// Decode a `<prefix> <name> <body>` envelope into a `Document`.
///
/// The body is the *inner* variant (e.g. `RunStop` not the tagged
/// `Document::Stop` enum) — same shape as bluesky's
/// event-model dicts. We use the `name` field from the envelope to
/// pick which variant to deserialize into.
fn decode_envelope(serializer: Serializer, raw: &[u8]) -> Result<Document> {
    let p1 = raw
        .iter()
        .position(|&b| b == b' ')
        .ok_or_else(|| BsrsError::Backend("zmq envelope: no prefix separator".into()))?;
    let after_prefix = &raw[p1 + 1..];
    let p2 = after_prefix
        .iter()
        .position(|&b| b == b' ')
        .ok_or_else(|| BsrsError::Backend("zmq envelope: no name separator".into()))?;
    let name_bytes = &after_prefix[..p2];
    let body = &after_prefix[p2 + 1..];
    let name = std::str::from_utf8(name_bytes)
        .map_err(|e| BsrsError::Backend(format!("zmq envelope name not utf8: {e}")))?;

    let value: serde_json::Value = match serializer {
        Serializer::Json => serde_json::from_slice(body)
            .map_err(|e| BsrsError::Backend(format!("zmq json decode: {e}")))?,
        Serializer::Msgpack => rmp_serde::from_slice(body)
            .map_err(|e| BsrsError::Backend(format!("zmq msgpack decode: {e}")))?,
    };
    let map_err = |e: serde_json::Error| BsrsError::Backend(format!("zmq decode {name}: {e}"));

    use bsrs_event_model as em;
    let doc = match name {
        "start" => Document::Start(serde_json::from_value::<em::RunStart>(value).map_err(map_err)?),
        "descriptor" => Document::Descriptor(
            serde_json::from_value::<em::EventDescriptor>(value).map_err(map_err)?,
        ),
        "event" => Document::Event(serde_json::from_value::<em::Event>(value).map_err(map_err)?),
        "event_page" => {
            Document::EventPage(serde_json::from_value::<em::EventPage>(value).map_err(map_err)?)
        }
        "resource" => {
            Document::Resource(serde_json::from_value::<em::Resource>(value).map_err(map_err)?)
        }
        "datum" => Document::Datum(serde_json::from_value::<em::Datum>(value).map_err(map_err)?),
        "datum_page" => {
            Document::DatumPage(serde_json::from_value::<em::DatumPage>(value).map_err(map_err)?)
        }
        "stream_resource" => Document::StreamResource(
            serde_json::from_value::<em::StreamResource>(value).map_err(map_err)?,
        ),
        "stream_datum" => Document::StreamDatum(
            serde_json::from_value::<em::StreamDatum>(value).map_err(map_err)?,
        ),
        "stop" => Document::Stop(serde_json::from_value::<em::RunStop>(value).map_err(map_err)?),
        other => {
            return Err(BsrsError::Backend(format!(
                "zmq envelope: unknown doc name {other:?}"
            )));
        }
    };
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ZmqDocumentSink;
    use bsrs_engine::DocumentSink;
    use bsrs_event_model::{ExitStatus, RunStop};

    fn fake_stop() -> Document {
        Document::Stop(RunStop {
            uid: "stop-1".into(),
            run_start: "run-1".into(),
            time: 1.0,
            exit_status: ExitStatus::Success,
            reason: None,
            num_events: std::collections::HashMap::new(),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn pub_sub_round_trip_msgpack() {
        let addr = format!(
            "ipc:///tmp/bsrs-zmq-source-test-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        );
        let sink = ZmqDocumentSink::bind(&addr)
            .expect("bind sink")
            .with_serializer(Serializer::Msgpack);
        let src = ZmqDocumentSource::connect(&addr)
            .expect("connect source")
            .with_serializer(Serializer::Msgpack);
        // PUB/SUB slow-joiner: publish until the SUB sees something.
        let stop = fake_stop();
        let mut got = None;
        for _ in 0..40 {
            sink.dispatch(&stop).await.unwrap();
            match src.try_recv() {
                Ok(Some(d)) => {
                    got = Some(d);
                    break;
                }
                Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
                Err(e) => panic!("recv error: {e}"),
            }
        }
        let got = got.expect("never received via SUB");
        match got {
            Document::Stop(s) => assert_eq!(s.exit_status, ExitStatus::Success),
            _ => panic!("wrong doc kind"),
        }
    }

    // The connect-time `b""` (match-all) subscription must be removed when a
    // prefix filter is set, or it keeps matching every message and the filter
    // is a silent no-op. Observe the SUB's real subscribe/unsubscribe frames via
    // an XPUB peer: a subscribe arrives as `\x01<prefix>`, an unsubscribe as
    // `\x00<prefix>`. First wait for the empty-prefix subscribe to confirm the
    // connection is up — so the later unsubscribe is genuinely transmitted and
    // not coalesced away — then assert the empty-prefix UNSUBSCRIBE arrives.
    // Neutering the fix (dropping `set_unsubscribe`) never emits that frame.
    #[test]
    fn subscribe_prefix_drops_the_match_all_default() {
        let addr = format!(
            "ipc:///tmp/bsrs-zmq-xpub-test-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        );
        let ctx = zmq::Context::new();
        let xpub = ctx.socket(zmq::XPUB).expect("xpub socket");
        xpub.bind(&addr).expect("xpub bind");
        xpub.set_rcvtimeo(100).unwrap();

        // connect() subscribes the match-all default b"".
        let src = ZmqDocumentSource::connect(&addr).expect("connect source");

        // Wait for the match-all subscribe (one 0x01 byte) so the connection is
        // established before we narrow — otherwise the unsubscribe below could
        // cancel the not-yet-sent subscribe and never reach the XPUB.
        let recv_frame = |sock: &zmq::Socket, want: [u8; 1]| -> bool {
            for _ in 0..50 {
                match sock.recv_bytes(0) {
                    Ok(f) if f == want => return true,
                    Ok(_) => {}
                    Err(zmq::Error::EAGAIN) => {}
                    Err(e) => panic!("xpub recv: {e}"),
                }
            }
            false
        };
        assert!(
            recv_frame(&xpub, [0x01]),
            "XPUB never saw the connect-time match-all subscribe"
        );

        // Narrow to a prefix; the fix unsubscribes b"" first.
        let _src = src.with_subscribe_prefix(b"app1.");

        assert!(
            recv_frame(&xpub, [0x00]),
            "with_subscribe_prefix must unsubscribe the match-all default \
             (no empty-prefix unsubscribe frame reached the XPUB)"
        );
    }

    // Regression for the run_into_engine injection path. Pre-fix the
    // received Document was decoded but dropped; the subscriber chain
    // never saw it. With `RunEngine::inject_document` wired, a
    // subscribe()'d callback on the engine observes every published
    // doc. Uses the default current-thread runtime — the receive loop
    // now dispatches the blocking `recv_bytes` to
    // `tokio::task::spawn_blocking`, so the 250ms wait no longer pins
    // the worker and the publisher makes progress concurrently.
    #[tokio::test]
    async fn run_into_engine_forwards_to_subscribers() {
        use bsrs_engine::RunEngine;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let addr = format!(
            "ipc:///tmp/bsrs-zmq-inject-test-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        );
        let sink = ZmqDocumentSink::bind(&addr)
            .expect("bind sink")
            .with_serializer(Serializer::Msgpack);
        let cancel = CancellationToken::new();
        let src = Arc::new(
            ZmqDocumentSource::connect(&addr)
                .expect("connect source")
                .with_serializer(Serializer::Msgpack)
                .with_cancel(cancel.clone()),
        );

        let engine = Arc::new(RunEngine::new(Vec::new()));
        let count = Arc::new(AtomicUsize::new(0));
        let count_cb = count.clone();
        engine.subscribe(Arc::new(move |_doc: &Document| {
            count_cb.fetch_add(1, Ordering::SeqCst);
        }));

        let src_loop = src.clone();
        let engine_loop = engine.clone();
        let driver = tokio::spawn(async move {
            let _ = src_loop.run_into_engine(engine_loop).await;
        });

        let stop = fake_stop();
        // PUB/SUB slow-joiner — publish repeatedly until the
        // subscriber sees something.
        for _ in 0..40 {
            sink.dispatch(&stop).await.unwrap();
            if count.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        cancel.cancel();
        let _ = driver.await;

        assert!(
            count.load(Ordering::SeqCst) > 0,
            "engine subscriber never fired"
        );
    }
}
