//! `KafkaDocumentSink` — publish bluesky-shaped Documents to a Kafka
//! topic. Behind the `kafka` Cargo feature.
//!
//! Uses the pure-Rust [`kafka`](https://crates.io/crates/kafka) crate;
//! no librdkafka native dep.
//!
//! ## Wire format
//!
//! Each `dispatch(doc)` produces one Kafka message on the configured
//! topic:
//!
//! - **key** = bluesky doc kind (`b"start" | "descriptor" | "event" |
//!   "event_page" | "resource" | "datum" | "datum_page" |
//!   "stream_resource" | "stream_datum" | "stop"`).
//! - **value** = serialized doc body — JSON by default, msgpack when
//!   `Serializer::Msgpack` is selected.
//!
//! Downstream consumers can dispatch by key without parsing the body
//! first, matching the bluesky-kafka envelope used by NSLS-II /
//! BNL ingestion services.
//!
//! ## Threading
//!
//! `kafka::producer::Producer` is sync and blocking. The sink wraps
//! it in a `Mutex` and offloads each `send` to `spawn_blocking` so
//! the tokio reactor isn't parked on the libnetwork I/O.

#![cfg(feature = "kafka")]

use crate::core::error::{BsrsError, Result};
use crate::engine::DocumentSink;
use crate::event_model::Document;
use async_trait::async_trait;
use kafka::producer::{Producer, Record, RequiredAcks};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::callbacks::doc_name::document_name;

/// Body serialization format for [`KafkaDocumentSink`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Serializer {
    /// JSON-encoded value bytes (default).
    Json,
    /// MessagePack-encoded value bytes.
    Msgpack,
}

/// Document sink that publishes to a Kafka topic.
pub struct KafkaDocumentSink {
    /// Wrapped Kafka producer (sync, !Send across await points; we
    /// hold it in a `Mutex` and call `send` from `spawn_blocking`).
    producer: Arc<Mutex<Producer>>,
    /// Topic name.
    topic: String,
    /// Body serializer.
    serializer: Serializer,
}

impl KafkaDocumentSink {
    /// Build with a list of broker addresses (e.g.
    /// `vec!["localhost:9092"]`) and a topic name. Uses
    /// `RequiredAcks::One` (leader-ack) and a 5-second ack timeout —
    /// reasonable for a beamline writer that wants durable but
    /// not-too-slow publishes.
    pub fn new(brokers: Vec<String>, topic: impl Into<String>) -> Result<Self> {
        let producer = Producer::from_hosts(brokers)
            .with_ack_timeout(Duration::from_secs(5))
            .with_required_acks(RequiredAcks::One)
            .create()
            .map_err(|e| BsrsError::Backend(format!("kafka producer: {e}")))?;
        Ok(Self {
            producer: Arc::new(Mutex::new(producer)),
            topic: topic.into(),
            serializer: Serializer::Json,
        })
    }

    /// Override the body serializer.
    pub fn with_serializer(mut self, s: Serializer) -> Self {
        self.serializer = s;
        self
    }

    fn encode_body(&self, doc: &Document) -> Result<Vec<u8>> {
        encode_body(self.serializer, doc)
    }
}

/// Free-function form of `encode_body` so unit tests can exercise the
/// serialization without spinning up a Kafka producer.
fn encode_body(serializer: Serializer, doc: &Document) -> Result<Vec<u8>> {
    // Serialize the raw document dict (inner variant), not the adjacently
    // tagged `Document` wrapper — matches the bluesky-kafka envelope where the
    // doc kind travels in the message key, not the body (CBEM-01).
    match serializer {
        Serializer::Json => crate::callbacks::doc_encode::encode_inner_json(doc)
            .map_err(|e| BsrsError::Backend(format!("kafka json encode: {e}"))),
        Serializer::Msgpack => crate::callbacks::doc_encode::encode_inner_msgpack(doc)
            .map_err(|e| BsrsError::Backend(format!("kafka msgpack encode: {e}"))),
    }
}

#[async_trait]
impl DocumentSink for KafkaDocumentSink {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        let body = self.encode_body(doc)?;
        let key = document_name(doc).as_bytes().to_vec();
        let topic = self.topic.clone();
        let producer = self.producer.clone();
        // Kafka producer is blocking; isolate from the reactor.
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut p = producer.blocking_lock();
            let rec = Record::from_key_value(&topic, &key[..], &body[..]);
            p.send(&rec)
                .map_err(|e| BsrsError::Backend(format!("kafka send: {e}")))
        })
        .await
        .map_err(|e| BsrsError::Backend(format!("kafka join: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_model::{ExitStatus, RunStop};
    use std::collections::HashMap;

    fn fake_stop() -> Document {
        Document::Stop(RunStop {
            uid: "stop-1".into(),
            run_start: "run-1".into(),
            time: 1.0,
            exit_status: ExitStatus::Success,
            reason: None,
            num_events: HashMap::new(),
            ..Default::default()
        })
    }

    /// Encoding does not require a broker — verify the JSON / msgpack
    /// branches via the free `encode_body` function. Live
    /// `dispatch()` testing needs a running Kafka broker (integration
    /// test concern, not unit).
    #[test]
    fn encode_body_json_round_trips() {
        let body = encode_body(Serializer::Json, &fake_stop()).expect("encode");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
        // CBEM-01: the body is the raw event-model dict (doc kind lives in the
        // Kafka key), not the adjacently-tagged {"name":..,"doc":..} wrapper.
        assert_eq!(v["exit_status"], "success");
        assert_eq!(v["run_start"], "run-1");
        assert!(
            v.get("name").is_none() && v.get("doc").is_none(),
            "kafka body must be the raw doc dict, not the Document wrapper: {v}"
        );
    }

    #[test]
    fn encode_body_msgpack_starts_with_named_struct_marker() {
        let body = encode_body(Serializer::Msgpack, &fake_stop()).expect("encode");
        assert!(
            (body[0] & 0xf0) == 0x80 || body[0] == 0xde || body[0] == 0xdf,
            "expected msgpack map header, got first byte = 0x{:02x}",
            body[0]
        );
    }
}
