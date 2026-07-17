//! `KafkaDocumentSink` — publish bluesky-shaped Documents to a Kafka
//! topic. Behind the `kafka` Cargo feature.
//!
//! Uses the pure-Rust async [`rskafka`](https://crates.io/crates/rskafka)
//! client; no librdkafka native dep. rskafka speaks the modern wire
//! protocol (record batch v2, `ApiVersions` negotiation), which Kafka 4.x
//! brokers require: KIP-896 removed the pre-2.1 client API versions the
//! previous `kafka` (kafka-rust) client produced with, so it cannot talk
//! to a 4.x broker at all (verified live: every produce fails and the
//! broker drops the connection).
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
//! ## Delivery
//!
//! Documents go to partition 0 of the topic. rskafka is a per-partition
//! client by design, and one partition is what preserves the event
//! model's total document order (start before descriptor before events)
//! — the reason bluesky-kafka deployments run single-partition topics.
//! Produces are acknowledged with `acks=all` (rskafka's fixed setting;
//! the previous client asked for leader-ack only — identical on the
//! replication-factor-1 topics this sink targets, stricter on replicated
//! ones), and every client operation is bounded by a 5-second retry
//! deadline, matching the old producer's 5-second ack timeout. The
//! topic must already exist: a missing topic fails [`KafkaDocumentSink::new`]
//! fast instead of retrying forever.

use crate::core::error::{BsrsError, Result};
use crate::engine::DocumentSink;
use crate::event_model::Document;
use async_trait::async_trait;
use rskafka::chrono::{DateTime, Utc};
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use rskafka::BackoffConfig;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    /// Client for partition 0 of the configured topic; `produce` takes
    /// `&self`, so `dispatch` needs no lock and no `spawn_blocking`.
    partition: PartitionClient,
    /// Body serializer.
    serializer: Serializer,
}

impl KafkaDocumentSink {
    /// Connect to `brokers` (e.g. `vec!["localhost:9092"]`) and bind
    /// partition 0 of `topic`.
    ///
    /// Async because the client bootstrap (metadata + `ApiVersions`
    /// negotiation) is a network exchange. Fails when no broker is
    /// reachable or the topic does not exist.
    pub async fn new(brokers: Vec<String>, topic: impl Into<String>) -> Result<Self> {
        let topic = topic.into();
        let client = ClientBuilder::new(brokers)
            .backoff_config(BackoffConfig {
                deadline: Some(Duration::from_secs(5)),
                ..Default::default()
            })
            .build()
            .await
            .map_err(|e| BsrsError::Backend(format!("kafka client: {e}")))?;
        let partition = client
            .partition_client(&topic, 0, UnknownTopicHandling::Error)
            .await
            .map_err(|e| BsrsError::Backend(format!("kafka topic {topic:?}: {e}")))?;
        Ok(Self {
            partition,
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
/// serialization without spinning up a Kafka client.
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
        // Producer wall-clock timestamp, built via `from_timestamp` — the
        // chrono `clock` feature is not in this crate's dependency graph.
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let timestamp =
            DateTime::from_timestamp(since_epoch.as_secs() as i64, since_epoch.subsec_nanos())
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        let record = Record {
            key: Some(document_name(doc).as_bytes().to_vec()),
            value: Some(self.encode_body(doc)?),
            headers: BTreeMap::new(),
            timestamp,
        };
        self.partition
            .produce(vec![record], Compression::NoCompression)
            .await
            .map_err(|e| BsrsError::Backend(format!("kafka produce: {e}")))?;
        Ok(())
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
