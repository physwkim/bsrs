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
        // Serialize the raw document dict (not the tagged `Document` wrapper),
        // matching Python event-model JSONL — see `doc_encode` (CBEM-01).
        let mut line = crate::doc_encode::encode_inner_json(doc)
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
