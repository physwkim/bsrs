//! `ZmqDocumentSink` — emit Document stream over a 0MQ PUB socket using the
//! bluesky `bluesky.callbacks.zmq.Publisher` envelope.
//!
//! Wire format: `b"<prefix> <name> <serialized_doc>"` — three fields separated
//! by ASCII space (`b' '`). `prefix` may be empty (no leading space). The
//! Python side is `bluesky.callbacks.zmq.RemoteDispatcher` configured with the
//! matching deserializer (default bsrs serializer is `msgpack`).
//!
//! ```python
//! # Python receiver — works unchanged with bsrs emitting:
//! import msgpack
//! from bluesky.callbacks.zmq import RemoteDispatcher
//! from bluesky.callbacks.best_effort import BestEffortCallback
//! disp = RemoteDispatcher("tcp://localhost:5577", deserializer=msgpack.unpackb)
//! disp.subscribe(BestEffortCallback())
//! disp.start()
//! ```

use async_trait::async_trait;
use bsrs_core::error::{BsrsError, Result};
use bsrs_engine::DocumentSink;
use bsrs_event_model::Document;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use crate::doc_name::document_name;

/// Wire-level serializer used to encode the document body.
#[derive(Copy, Clone, Debug)]
pub enum Serializer {
    /// `msgpack` (rmp-serde) — recommended default. Cross-language,
    /// fast, supported by Python `msgpack.unpackb` on the receiving side.
    Msgpack,
    /// JSON. Slower and larger but trivially diff-friendly. Receiver:
    /// `RemoteDispatcher(..., deserializer=lambda b: json.loads(b.decode()))`.
    Json,
}

/// Document sink that publishes over a 0MQ PUB socket. Bluesky-Publisher
/// compatible envelope.
pub struct ZmqDocumentSink {
    socket: Arc<StdMutex<zmq::Socket>>,
    prefix: Vec<u8>,
    serializer: Serializer,
}

impl ZmqDocumentSink {
    /// Build sink that **binds** the given address (server side — clients connect to it).
    pub fn bind(address: &str) -> Result<Self> {
        let ctx = zmq::Context::new();
        let socket = ctx
            .socket(zmq::PUB)
            .map_err(|e| BsrsError::Backend(format!("zmq socket: {e}")))?;
        socket
            .bind(address)
            .map_err(|e| BsrsError::Backend(format!("zmq bind {address}: {e}")))?;
        Ok(Self {
            socket: Arc::new(StdMutex::new(socket)),
            prefix: Vec::new(),
            serializer: Serializer::Msgpack,
        })
    }

    /// Build sink that **connects** to a 0MQ proxy (e.g. a `bluesky.callbacks.zmq.Proxy`).
    pub fn connect(address: &str) -> Result<Self> {
        let ctx = zmq::Context::new();
        let socket = ctx
            .socket(zmq::PUB)
            .map_err(|e| BsrsError::Backend(format!("zmq socket: {e}")))?;
        socket
            .connect(address)
            .map_err(|e| BsrsError::Backend(format!("zmq connect {address}: {e}")))?;
        Ok(Self {
            socket: Arc::new(StdMutex::new(socket)),
            prefix: Vec::new(),
            serializer: Serializer::Msgpack,
        })
    }

    /// Override the prefix bytes. Must not contain `b' '`.
    pub fn with_prefix(mut self, prefix: impl Into<Vec<u8>>) -> Result<Self> {
        let p = prefix.into();
        if p.contains(&b' ') {
            return Err(BsrsError::InvalidValue(
                "ZmqDocumentSink prefix must not contain b' '".into(),
            ));
        }
        self.prefix = p;
        Ok(self)
    }

    /// Override the body serializer.
    pub fn with_serializer(mut self, s: Serializer) -> Self {
        self.serializer = s;
        self
    }

    fn encode_body(&self, doc: &Document) -> Result<Vec<u8>> {
        // The wire format expects the *body only* (without the discriminator
        // tag), so we serialize the inner variant rather than the tagged
        // `Document` enum. This matches Python event-model dicts. The
        // per-variant match lives in `doc_encode` so every sink shares one
        // owner (CBEM-01).
        match self.serializer {
            Serializer::Msgpack => crate::doc_encode::encode_inner_msgpack(doc)
                .map_err(|e| BsrsError::Backend(format!("msgpack: {e}"))),
            Serializer::Json => crate::doc_encode::encode_inner_json(doc)
                .map_err(|e| BsrsError::Backend(format!("json: {e}"))),
        }
    }

    /// Send raw envelope bytes (test-only — used for slow-joiner priming).
    #[doc(hidden)]
    pub fn send_raw_for_test(&self, bytes: &[u8]) -> Result<()> {
        let s = self.socket.lock().unwrap();
        s.send(bytes, 0)
            .map_err(|e| BsrsError::Backend(format!("zmq send: {e}")))
    }

    fn build_envelope(&self, doc: &Document) -> Result<Vec<u8>> {
        let body = self.encode_body(doc)?;
        let name = document_name(doc).as_bytes();
        // Wire format: `<prefix> <name> <body>` with literal space
        // separators — matches bluesky's `Publisher.__call__`:
        //     b" ".join([self._prefix, name.encode(), self._serializer(doc)])
        // When prefix is empty bytes, the message starts with a
        // leading space (` <name> <body>`). bluesky's
        // `RemoteDispatcher` splits on the first two spaces, so the
        // empty first element is fine.
        let mut buf = Vec::with_capacity(self.prefix.len() + 1 + name.len() + 1 + body.len());
        buf.extend_from_slice(&self.prefix);
        buf.push(b' ');
        buf.extend_from_slice(name);
        buf.push(b' ');
        buf.extend_from_slice(&body);
        Ok(buf)
    }
}

#[async_trait]
impl DocumentSink for ZmqDocumentSink {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        let envelope = self.build_envelope(doc)?;
        let socket = self.socket.clone();
        // PUB::send is non-blocking; we still hand it off to spawn_blocking so
        // tokio's reactor doesn't park on the libzmq write.
        tokio::task::spawn_blocking(move || -> Result<()> {
            let s = socket.lock().unwrap();
            s.send(&envelope, 0)
                .map_err(|e| BsrsError::Backend(format!("zmq send: {e}")))
        })
        .await
        .map_err(|e| BsrsError::Backend(format!("zmq join: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsrs_event_model::{Hints, RunStart};

    fn fake_start() -> Document {
        Document::Start(RunStart {
            uid: "abc".into(),
            time: 1234.5,
            scan_id: Some(1),
            hints: Some(Hints::default()),
            ..Default::default()
        })
    }

    #[test]
    fn envelope_layout_no_prefix() {
        // We can't easily exercise the full zmq path without a context, so
        // test the envelope builder via a sink built without binding.
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PUB).unwrap();
        let sink = ZmqDocumentSink {
            socket: Arc::new(StdMutex::new(socket)),
            prefix: Vec::new(),
            serializer: Serializer::Msgpack,
        };
        let env = sink.build_envelope(&fake_start()).unwrap();
        // " start <body>"
        assert_eq!(env[0], b' ');
        let rest = &env[1..];
        let space = rest.iter().position(|&b| b == b' ').unwrap();
        assert_eq!(&rest[..space], b"start");
    }

    #[test]
    fn prefix_with_space_rejected() {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PUB).unwrap();
        let sink = ZmqDocumentSink {
            socket: Arc::new(StdMutex::new(socket)),
            prefix: Vec::new(),
            serializer: Serializer::Msgpack,
        };
        let res = sink.with_prefix(b"bad prefix".to_vec());
        assert!(res.is_err());
    }
}
