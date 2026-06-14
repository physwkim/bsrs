//! `BinaryFrameSink` — append frames to a length-prefixed binary file
//! (CIRBIN1 magic + (u32 LE length, payload bytes) repeated).
//!
//! Implements both `FrameSink` (for the FramePipe data plane) and
//! `DetectorWriter` (for `WritesStreamAssets` in step / fly scans).
//! The mimetype is `application/x-cirbin1`; the URI is the absolute file path
//! prefixed with `file://`.

use async_trait::async_trait;
use cirrus_core::error::{CirrusError, Result};
use cirrus_event_model::{DataKey, Dtype, StreamDatum, StreamRange, StreamResource};
use cirrus_protocols_async::{DetectorWriter, Frame, FrameSink, StreamAsset};
use futures::stream::{self, BoxStream, StreamExt};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{watch, Mutex};

const MAGIC: &[u8] = b"CIRBIN1\n";

/// File-backed binary sink. Each `accept()` appends one length-prefixed frame.
pub struct BinaryFrameSink {
    name: String,
    path: PathBuf,
    file: Mutex<Option<tokio::fs::File>>,
    indices_tx: watch::Sender<u64>,
    indices_rx: watch::Receiver<u64>,
    counter: AtomicU64,
    last_emitted: AtomicU64,
    resource_uid: StdMutex<Option<String>>,
    /// Bytes-per-frame hint exposed in the DataKey shape (unused if 0).
    payload_size: u64,
}

impl BinaryFrameSink {
    /// Build for `path` with a logical `name`. Does not open the file until
    /// the first `open()` (DetectorWriter contract) or first `accept()` call.
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>, payload_size: u64) -> Self {
        let (tx, rx) = watch::channel(0_u64);
        Self {
            name: name.into(),
            path: path.into(),
            file: Mutex::new(None),
            indices_tx: tx,
            indices_rx: rx,
            counter: AtomicU64::new(0),
            last_emitted: AtomicU64::new(0),
            resource_uid: StdMutex::new(None),
            payload_size,
        }
    }

    async fn ensure_open(&self) -> Result<()> {
        let mut guard = self.file.lock().await;
        if guard.is_none() {
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&self.path)
                .await
                .map_err(|e| CirrusError::Backend(format!("binary sink open: {e}")))?;
            f.write_all(MAGIC)
                .await
                .map_err(|e| CirrusError::Backend(format!("binary sink write magic: {e}")))?;
            *guard = Some(f);
        }
        Ok(())
    }
}

#[async_trait]
impl FrameSink for BinaryFrameSink {
    async fn accept(&self, frame: Frame) -> Result<()> {
        self.ensure_open().await?;
        let mut guard = self.file.lock().await;
        let f = guard
            .as_mut()
            .ok_or_else(|| CirrusError::State("binary sink not open".into()))?;
        // CIRBIN1's per-frame length prefix is u32 LE — reject frames
        // larger than 4 GiB explicitly rather than silently truncating
        // (which would leave the file with a wrong length and cascade
        // into reader corruption).
        let len_u32: u32 = frame.payload.len().try_into().map_err(|_| {
            CirrusError::Backend(format!(
                "binary sink: frame too large for u32 length prefix ({} bytes; CIRBIN1 max = {})",
                frame.payload.len(),
                u32::MAX
            ))
        })?;
        let len = len_u32.to_le_bytes();
        f.write_all(&len)
            .await
            .map_err(|e| CirrusError::Backend(format!("binary sink write len: {e}")))?;
        f.write_all(&frame.payload)
            .await
            .map_err(|e| CirrusError::Backend(format!("binary sink write payload: {e}")))?;
        let next = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.indices_tx.send(next);
        Ok(())
    }
}

#[async_trait]
impl DetectorWriter for BinaryFrameSink {
    async fn open(&self, _multiplier: u32) -> Result<HashMap<String, DataKey>> {
        self.ensure_open().await?;
        let mut out = HashMap::new();
        out.insert(
            format!("{}_image", self.name),
            DataKey {
                source: format!("file://{}", self.path.display()),
                dtype: Dtype::Number,
                shape: if self.payload_size > 0 {
                    vec![Some(self.payload_size)]
                } else {
                    vec![]
                },
                dtype_numpy: Some("|u1".into()),
                external: Some("STREAM:".into()),
                units: None,
                precision: None,
                object_name: Some(self.name.clone()),
                dims: Some(vec!["byte".into()]),
                limits: None,
                choices: None,
            },
        );
        Ok(out)
    }
    fn observe_indices_written(&self) -> watch::Receiver<u64> {
        self.indices_rx.clone()
    }
    async fn indices_written(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
    fn collect_stream_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset> {
        let mut docs: Vec<StreamAsset> = Vec::new();
        let resource_uid = {
            let mut g = self.resource_uid.lock().unwrap();
            if let Some(u) = g.clone() {
                u
            } else {
                let new_uid = uuid::Uuid::new_v4().to_string();
                *g = Some(new_uid.clone());
                docs.push(StreamAsset::Resource(StreamResource {
                    uid: new_uid.clone(),
                    data_key: format!("{}_image", self.name),
                    mimetype: "application/x-cirbin1".into(),
                    uri: format!("file://{}", self.path.display()),
                    parameters: Default::default(),
                    run_start: None,
                }));
                new_uid
            }
        };
        let last = self.last_emitted.load(Ordering::SeqCst);
        if up_to > last {
            docs.push(StreamAsset::Datum(StreamDatum {
                uid: uuid::Uuid::new_v4().to_string(),
                stream_resource: resource_uid,
                descriptor: descriptor.to_string(),
                indices: StreamRange {
                    start: last,
                    stop: up_to,
                },
                seq_nums: StreamRange {
                    start: last + 1,
                    stop: up_to + 1,
                },
            }));
            self.last_emitted.store(up_to, Ordering::SeqCst);
        }
        stream::iter(docs).boxed()
    }
    async fn close(&self) -> Result<()> {
        let mut guard = self.file.lock().await;
        if let Some(mut f) = guard.take() {
            f.flush()
                .await
                .map_err(|e| CirrusError::Backend(format!("binary sink flush: {e}")))?;
        }
        Ok(())
    }
}
