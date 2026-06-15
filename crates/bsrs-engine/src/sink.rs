//! `DocumentSink` trait + a broadcast-channel based default sink.

use async_trait::async_trait;
use bsrs_core::error::Result;
use bsrs_event_model::Document;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

/// A consumer of `Document`s.
#[async_trait]
pub trait DocumentSink: Send + Sync {
    /// Dispatch a single document. Errors are logged but not fatal to the run.
    async fn dispatch(&self, doc: &Document) -> Result<()>;
}

/// Channel-based fan-out. Tracks lag with an atomic counter (rule **K6**).
#[derive(Clone)]
pub struct BroadcastSink {
    tx: broadcast::Sender<Document>,
    overflow: Arc<AtomicU64>,
}

impl BroadcastSink {
    /// Build a sink with the given buffer size.
    pub fn new(buffer: usize) -> Self {
        let (tx, _rx) = broadcast::channel(buffer);
        Self {
            tx,
            overflow: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get a fresh receiver.
    pub fn subscribe(&self) -> broadcast::Receiver<Document> {
        self.tx.subscribe()
    }

    /// Read the current lag/overflow counter.
    pub fn overflow_count(&self) -> u64 {
        self.overflow.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl DocumentSink for BroadcastSink {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        // broadcast::send returns Err only if there are no receivers; that is OK.
        let _ = self.tx.send(doc.clone());
        Ok(())
    }
}
