//! `FramePipe` — primary + secondaries with rogue-style ordering and overflow counter.

use crate::core::error::Result;
use crate::protocols_async::{Frame, FrameSink};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// A live frame pipe — primary slave + secondaries.
pub struct FramePipe {
    primary: Arc<dyn FrameSink>,
    secondaries: Vec<Arc<dyn FrameSink>>,
    overflow: Arc<AtomicU64>,
    cancel: CancellationToken,
}

impl FramePipe {
    /// Construct a builder.
    pub fn builder() -> FramePipeBuilder {
        FramePipeBuilder::default()
    }

    /// Send a frame to all sinks. Secondaries first, primary last (rogue order).
    pub async fn send(&self, frame: Frame) {
        for s in &self.secondaries {
            if s.accept(frame.clone()).await.is_err() {
                self.overflow.fetch_add(1, Ordering::Relaxed);
            }
        }
        let _ = self.primary.accept(frame).await;
    }

    /// Cancel the pipe (engine drop).
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Read overflow count.
    pub fn overflow(&self) -> u64 {
        self.overflow.load(Ordering::Relaxed)
    }
}

/// Builder. Background tasks spawn only on `start()` (rule **K9**).
#[derive(Default)]
pub struct FramePipeBuilder {
    primary: Option<Arc<dyn FrameSink>>,
    secondaries: Vec<Arc<dyn FrameSink>>,
}

impl FramePipeBuilder {
    /// Set the primary sink (also the allocator).
    pub fn primary(mut self, p: Arc<dyn FrameSink>) -> Self {
        self.primary = Some(p);
        self
    }
    /// Add a secondary sink.
    pub fn secondary(mut self, s: Arc<dyn FrameSink>) -> Self {
        self.secondaries.push(s);
        self
    }
    /// Commit the build. Returns a live `FramePipe`.
    pub fn start(self) -> Result<FramePipe> {
        let primary = self
            .primary
            .ok_or_else(|| crate::core::error::BsrsError::State("missing primary sink".into()))?;
        Ok(FramePipe {
            primary,
            secondaries: self.secondaries,
            overflow: Arc::new(AtomicU64::new(0)),
            cancel: CancellationToken::new(),
        })
    }
}
