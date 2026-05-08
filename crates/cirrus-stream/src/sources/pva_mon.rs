//! `PvaMonitorSource` — produces `Frame`s by subscribing to a PVA PV (e.g. an
//! NTNDArray feed). Active only with the `pva` feature.

use async_trait::async_trait;
use bytes::Bytes;
use cirrus_core::error::Result;
use cirrus_protocols_async::{Frame, FrameSource};
use epics_pva_rs::client::PvaClient;
use epics_pva_rs::PvField;
use futures::stream::{BoxStream, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default()
}

/// Source backed by a PVA monitor on `pv`. Each monitor event becomes one
/// `Frame`. The payload is the PvField encoded as JSON bytes — a minimal,
/// type-agnostic stand-in for a true zero-copy NTNDArray fast-path.
pub struct PvaMonitorSource {
    pv: String,
    client: Arc<PvaClient>,
    seq: Arc<AtomicU64>,
    cancel: CancellationToken,
    queue: tokio::sync::Mutex<Option<mpsc::Receiver<Frame>>>,
}

impl PvaMonitorSource {
    /// Build attached to an existing `PvaClient`.
    pub fn new(client: Arc<PvaClient>, pv: impl Into<String>) -> Self {
        Self {
            pv: pv.into(),
            client,
            seq: Arc::new(AtomicU64::new(0)),
            cancel: CancellationToken::new(),
            queue: tokio::sync::Mutex::new(None),
        }
    }

    fn pv_to_payload(field: &PvField) -> Bytes {
        // Best-effort encoding: serialize via Display for now. The high-rate
        // path will swap in NTNDArray decode → `Bytes::from_owner`.
        Bytes::from(field.to_string().into_bytes())
    }
}

#[async_trait]
impl FrameSource for PvaMonitorSource {
    fn frames(&self) -> BoxStream<'static, Frame> {
        // The receiver is created lazily inside `start()`. If `start` was not
        // called, `frames()` yields an empty stream.
        let mut g = self.queue.blocking_lock();
        if let Some(rx) = g.take() {
            return tokio_stream::wrappers::ReceiverStream::new(rx).boxed();
        }
        futures::stream::empty().boxed()
    }
    async fn start(&self) -> Result<()> {
        let (tx, rx) = mpsc::channel::<Frame>(64);
        *self.queue.lock().await = Some(rx);
        let pv = self.pv.clone();
        let client = self.client.clone();
        let seq = self.seq.clone();
        let cancel = self.cancel.clone();
        // K1: spawn returns a JoinHandle that we throw away — the
        // CancellationToken is the abort signal. Cancel from `stop()` cleans up.
        let _ = tokio::spawn(async move {
            // pvmonitor_typed wants a typed NT, but we want raw PvField.
            // Use a simple poll loop until a generic monitor surface lands.
            let mut last: Option<String> = None;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    res = client.pvget(&pv) => {
                        match res {
                            Ok(field) => {
                                let s = field.to_string();
                                if Some(&s) != last.as_ref() {
                                    last = Some(s.clone());
                                    let payload = PvaMonitorSource::pv_to_payload(&field);
                                    let n = seq.fetch_add(1, Ordering::SeqCst);
                                    let f = Frame {
                                        payload,
                                        ts_ns: now_ns(),
                                        channel: 0,
                                        flags: 0,
                                        seq: n,
                                    };
                                    if tx.send(f).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(_) => {
                                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            }
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        });
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        self.cancel.cancel();
        Ok(())
    }
}
