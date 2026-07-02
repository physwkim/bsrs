//! In-memory frame-counter sink.

use crate::core::error::Result;
use crate::protocols_async::{Frame, FrameSink};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Counts frames for tests.
pub struct CountingSink {
    /// Total frames received.
    pub count: Arc<AtomicU64>,
}

impl CountingSink {
    /// Build with a fresh counter.
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for CountingSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FrameSink for CountingSink {
    async fn accept(&self, _frame: Frame) -> Result<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
