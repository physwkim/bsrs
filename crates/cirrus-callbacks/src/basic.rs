//! Always-built sinks: JsonlSink, CapturingSink, StderrTraceSink.

use async_trait::async_trait;
use cirrus_core::error::Result;
use cirrus_engine::DocumentSink;
use cirrus_event_model::Document;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Append every document as one JSON line to a file.
pub struct JsonlSink {
    file: Mutex<tokio::fs::File>,
}

impl JsonlSink {
    /// Build by opening (or creating) `path` for append.
    pub async fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| cirrus_core::error::CirrusError::Backend(format!("jsonl open: {e}")))?;
        Ok(Self {
            file: Mutex::new(f),
        })
    }
}

#[async_trait]
impl DocumentSink for JsonlSink {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        // JSONL has no out-of-band channel for the document kind (unlike ZMQ's
        // multipart `<name>` frame or Kafka's message key), so each line must be
        // the tagged `{"name": <kind>, "doc": <dict>}` form — matching bluesky's
        // `JSONLinesWriter` (callbacks/json_writer.py:62). The `Document` enum's
        // `#[serde(tag = "name", content = "doc", rename_all = "snake_case")]`
        // serializes to exactly that wrapper, so we serialize the whole enum.
        let mut line = serde_json::to_vec(doc)
            .map_err(|e| cirrus_core::error::CirrusError::Backend(format!("jsonl encode: {e}")))?;
        line.push(b'\n');
        let mut f = self.file.lock().await;
        f.write_all(&line)
            .await
            .map_err(|e| cirrus_core::error::CirrusError::Backend(format!("jsonl write: {e}")))?;
        Ok(())
    }
}

/// Collects every document into an in-memory vector. Useful for tests.
pub struct CapturingSink {
    /// Captured documents.
    pub docs: tokio::sync::Mutex<Vec<Document>>,
}

impl CapturingSink {
    /// Build with an empty vec.
    pub fn new() -> Self {
        Self {
            docs: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    /// Snapshot the captured documents.
    pub async fn snapshot(&self) -> Vec<Document> {
        self.docs.lock().await.clone()
    }
}

impl Default for CapturingSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DocumentSink for CapturingSink {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        self.docs.lock().await.push(doc.clone());
        Ok(())
    }
}

/// Print to stderr (one line per doc, name only).
pub struct StderrTraceSink;

#[async_trait]
impl DocumentSink for StderrTraceSink {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        eprintln!("[cirrus] {}", crate::doc_name::document_name(doc));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cirrus_event_model::{Document, RunStop};

    fn stop_doc() -> Document {
        Document::Stop(RunStop {
            uid: "u".into(),
            run_start: "r".into(),
            time: 0.0,
            exit_status: "success".into(),
            reason: None,
            num_events: Default::default(),
            ..Default::default()
        })
    }

    /// JSONL has no out-of-band channel for the document kind, so each line
    /// must be the tagged `{"name","doc"}` wrapper bluesky's `JSONLinesWriter`
    /// emits — not the bare inner dict (which is unrecoverable on read).
    #[tokio::test]
    async fn jsonl_line_is_tagged_name_doc_wrapper() {
        let path = std::env::temp_dir().join(format!("cirrus_jsonl_{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let sink = JsonlSink::open(&path).await.expect("open");
            sink.dispatch(&stop_doc()).await.expect("dispatch");
        }
        let contents = std::fs::read_to_string(&path).expect("read");
        let line = contents.lines().next().expect("one line");
        let v: serde_json::Value = serde_json::from_str(line).expect("parse");
        assert_eq!(v["name"], "stop", "line must carry the document kind: {v}");
        assert_eq!(v["doc"]["exit_status"], "success");
        assert!(
            v.get("exit_status").is_none(),
            "fields must live under `doc`, not at top level: {v}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
