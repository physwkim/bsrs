//! Source that yields a fixed sequence of frames synchronously.

use crate::core::error::Result;
use crate::protocols_async::{Frame, FrameSource};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default()
}

/// Source that yields a fixed sequence of frames synchronously.
pub struct VecFrameSource {
    /// `frames()` is a sync trait method that may be called from inside a
    /// tokio runtime, so this must be a std mutex (no await under the lock).
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
        let frames = std::mem::take(&mut *self.frames.lock().unwrap());
        stream::iter(frames).boxed()
    }
    async fn start(&self) -> Result<()> {
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `frames()` must be callable from inside a tokio runtime — the
    /// frame-source binary calls it on a runtime thread. With a tokio
    /// mutex + `blocking_lock` this panicked ("Cannot block the current
    /// thread from within a runtime").
    #[tokio::test]
    async fn frames_callable_inside_runtime() {
        let src = VecFrameSource::new(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
        let got: Vec<Frame> = src.frames().collect().await;
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].payload.as_ref(), b"a");
        // Second call: the vec was drained, stream is empty.
        let empty: Vec<Frame> = src.frames().collect().await;
        assert!(empty.is_empty());
    }
}
