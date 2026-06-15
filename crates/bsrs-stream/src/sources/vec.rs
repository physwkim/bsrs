//! Source that yields a fixed sequence of frames synchronously.

use async_trait::async_trait;
use bsrs_core::error::Result;
use bsrs_protocols_async::{Frame, FrameSource};
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default()
}

/// Source that yields a fixed sequence of frames synchronously.
pub struct VecFrameSource {
    frames: Mutex<Vec<Frame>>,
    /// Monotonic seq counter — kept for telemetry.
    pub seq: Arc<AtomicU64>,
}

impl VecFrameSource {
    /// Build with explicit payloads. Each payload becomes one frame.
    pub fn new(payloads: Vec<Bytes>) -> Self {
        let seq = Arc::new(AtomicU64::new(0));
        let frames: Vec<Frame> = payloads
            .into_iter()
            .map(|p| {
                let s = seq.fetch_add(1, Ordering::SeqCst);
                Frame {
                    payload: p,
                    ts_ns: now_ns(),
                    channel: 0,
                    flags: 0,
                    seq: s,
                }
            })
            .collect();
        Self {
            frames: Mutex::new(frames),
            seq,
        }
    }
}

#[async_trait]
impl FrameSource for VecFrameSource {
    fn frames(&self) -> BoxStream<'static, Frame> {
        let frames = std::mem::take(&mut *self.frames.blocking_lock());
        stream::iter(frames).boxed()
    }
    async fn start(&self) -> Result<()> {
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}
