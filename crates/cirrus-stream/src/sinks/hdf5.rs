//! `Hdf5FrameSink` — append frames to a chunked dataset inside a
//! NeXus-flavored HDF5 file. Behind the `hdf5` Cargo feature.
//!
//! Uses [`rust-hdf5`](https://crates.io/crates/rust-hdf5) — pure-Rust,
//! no system libhdf5 needed.
//!
//! ## Layout
//!
//! ```text
//! /                              NX_class=NXroot
//! /entry                         NX_class=NXentry
//! /entry/instrument              NX_class=NXinstrument
//! /entry/instrument/<name>       NX_class=NXdetector
//! /entry/instrument/<name>/data  u8 1-D extensible
//! ```
//!
//! ## Threading
//!
//! `rust-hdf5` handles use `Rc<RefCell>` (not `Send`), so all HDF5
//! ops are confined to a dedicated `std::thread`. Frames cross the
//! boundary via a `tokio::sync::mpsc` channel.

#![cfg(feature = "hdf5")]

use async_trait::async_trait;
use cirrus_core::error::{CirrusError, Result};
use cirrus_event_model::{DataKey, Dtype, StreamDatum, StreamRange, StreamResource};
use cirrus_protocols_async::{DetectorWriter, Frame, FrameSink, StreamAsset};
use futures::stream::{self, BoxStream, StreamExt};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot, watch};

/// Command sent to the dedicated HDF5-owning thread.
enum Cmd {
    Open(oneshot::Sender<Result<()>>),
    Append {
        payload: bytes::Bytes,
        reply: oneshot::Sender<Result<()>>,
    },
}

/// Configuration for [`Hdf5FrameSink`].
#[derive(Clone, Debug)]
pub struct Hdf5SinkConfig {
    /// Logical name (becomes the NXdetector group name).
    pub name: String,
    /// File path to create/truncate.
    pub path: PathBuf,
    /// Bytes-per-frame hint exposed in the DataKey shape (0 = generic).
    pub payload_size: u64,
    /// Chunk size in bytes for the data dataset (default 1 MiB).
    pub chunk_size_bytes: usize,
    /// If true, dataset chunks are gzip-deflated at level 4.
    pub compress: bool,
}

impl Hdf5SinkConfig {
    /// Build a config with defaults: 1 MiB chunks, no compression.
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>, payload_size: u64) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            payload_size,
            chunk_size_bytes: 1 << 20,
            compress: false,
        }
    }
    /// Override the chunk size.
    pub fn chunk_size_bytes(mut self, n: usize) -> Self {
        self.chunk_size_bytes = n.max(1);
        self
    }
    /// Toggle gzip compression.
    pub fn compression(mut self, on: bool) -> Self {
        self.compress = on;
        self
    }
}

/// Sink writing detector frames to a NeXus-flavored HDF5 file.
pub struct Hdf5FrameSink {
    name: String,
    path: PathBuf,
    payload_size: u64,
    /// Channel to the HDF5 worker thread. `None` after Drop.
    tx: StdMutex<Option<mpsc::UnboundedSender<Cmd>>>,
    /// Worker thread handle (joined on Drop).
    worker: StdMutex<Option<JoinHandle<()>>>,
    /// Tracks whether `Open` has been issued already.
    opened: OnceLock<()>,
    indices_tx: watch::Sender<u64>,
    indices_rx: watch::Receiver<u64>,
    counter: AtomicU64,
    last_emitted: AtomicU64,
    resource_uid: StdMutex<Option<String>>,
}

impl Hdf5FrameSink {
    /// Build with default config (1 MiB chunks, no compression).
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>, payload_size: u64) -> Self {
        Self::with_config(Hdf5SinkConfig::new(name, path, payload_size))
    }

    /// Build with an explicit config.
    pub fn with_config(cfg: Hdf5SinkConfig) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Cmd>();
        let (indices_tx, indices_rx) = watch::channel(0_u64);
        let path = cfg.path.clone();
        let name = cfg.name.clone();
        let chunk_size = cfg.chunk_size_bytes;
        let compress = cfg.compress;
        let worker = std::thread::Builder::new()
            .name(format!("hdf5-sink-{name}"))
            .spawn(move || {
                worker_loop(&mut rx, &path, &name, chunk_size, compress);
            })
            .expect("spawn hdf5 worker");
        Self {
            name: cfg.name,
            path: cfg.path,
            payload_size: cfg.payload_size,
            tx: StdMutex::new(Some(tx)),
            worker: StdMutex::new(Some(worker)),
            opened: OnceLock::new(),
            indices_tx,
            indices_rx,
            counter: AtomicU64::new(0),
            last_emitted: AtomicU64::new(0),
            resource_uid: StdMutex::new(None),
        }
    }

    fn send(&self, cmd: Cmd) -> Result<()> {
        let g = self.tx.lock().unwrap();
        let tx = g
            .as_ref()
            .ok_or_else(|| CirrusError::Backend("hdf5 worker shut down".into()))?;
        tx.send(cmd)
            .map_err(|_| CirrusError::Backend("hdf5 worker dropped".into()))
    }

    async fn ensure_open(&self) -> Result<()> {
        if self.opened.get().is_some() {
            return Ok(());
        }
        let (rep_tx, rep_rx) = oneshot::channel();
        self.send(Cmd::Open(rep_tx))?;
        let res = rep_rx
            .await
            .map_err(|_| CirrusError::Backend("hdf5 open: worker dropped reply".into()))?;
        if res.is_ok() {
            let _ = self.opened.set(());
        }
        res
    }
}

impl Drop for Hdf5FrameSink {
    fn drop(&mut self) {
        // Close tx → worker recv() returns None, loop exits, file
        // closes via Drop.
        self.tx.lock().unwrap().take();
        if let Some(h) = self.worker.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

#[async_trait]
impl FrameSink for Hdf5FrameSink {
    async fn accept(&self, frame: Frame) -> Result<()> {
        self.ensure_open().await?;
        let (rep_tx, rep_rx) = oneshot::channel();
        self.send(Cmd::Append {
            payload: frame.payload,
            reply: rep_tx,
        })?;
        rep_rx
            .await
            .map_err(|_| CirrusError::Backend("hdf5 append: worker dropped reply".into()))??;
        let next = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.indices_tx.send(next);
        Ok(())
    }
}

#[async_trait]
impl DetectorWriter for Hdf5FrameSink {
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
    async fn close(&self) -> Result<()> {
        // Drop the worker channel — Drop impl joins the thread which
        // closes the file. Idempotent: subsequent close()'s are no-ops
        // because tx is already None.
        self.tx.lock().unwrap().take();
        Ok(())
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
                let mut params: HashMap<String, serde_json::Value> = HashMap::new();
                params.insert(
                    "path".into(),
                    serde_json::Value::String(format!("/entry/instrument/{}/data", self.name)),
                );
                docs.push(StreamAsset::Resource(StreamResource {
                    uid: new_uid.clone(),
                    data_key: format!("{}_image", self.name),
                    mimetype: "application/x-hdf5".into(),
                    uri: format!("file://{}", self.path.display()),
                    parameters: params,
                    run_start: None,
                }));
                new_uid
            }
        };
        let prev = self.last_emitted.swap(up_to, Ordering::SeqCst);
        if up_to > prev {
            docs.push(StreamAsset::Datum(StreamDatum {
                uid: uuid::Uuid::new_v4().to_string(),
                stream_resource: resource_uid,
                descriptor: descriptor.to_string(),
                indices: StreamRange {
                    start: prev,
                    stop: up_to,
                },
                seq_nums: StreamRange {
                    start: prev + 1,
                    stop: up_to + 1,
                },
            }));
        }
        stream::iter(docs).boxed()
    }
}

fn worker_loop(
    rx: &mut mpsc::UnboundedReceiver<Cmd>,
    path: &std::path::Path,
    name: &str,
    chunk_size_bytes: usize,
    compress: bool,
) {
    let mut state: Option<(rust_hdf5::H5File, rust_hdf5::H5Dataset)> = None;
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            Cmd::Open(reply) => match open_file(path, name, chunk_size_bytes, compress) {
                Ok((f, ds)) => {
                    state = Some((f, ds));
                    let _ = reply.send(Ok(()));
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            },
            Cmd::Append { payload, reply } => match state.as_ref() {
                Some((_, ds)) => {
                    let res = ds
                        .append::<u8>(&payload)
                        .map_err(|e| CirrusError::Backend(format!("hdf5 append: {e}")));
                    let _ = reply.send(res);
                }
                None => {
                    let _ = reply.send(Err(CirrusError::State("hdf5 sink not open".into())));
                }
            },
        }
    }
}

fn open_file(
    path: &std::path::Path,
    name: &str,
    chunk_size_bytes: usize,
    compress: bool,
) -> Result<(rust_hdf5::H5File, rust_hdf5::H5Dataset)> {
    let file = rust_hdf5::H5File::create(path)
        .map_err(|e| CirrusError::Backend(format!("hdf5 sink create {}: {e}", path.display())))?;
    // rust-hdf5 0.2 only supports attributes on the root file; per-group
    // NX_class attrs would require a newer version. Mark the root with
    // a hint and rely on the path layout for the rest.
    file.set_attr_string("NX_class", "NXroot").ok();
    file.set_attr_string("default", "entry").ok();
    let entry = file
        .create_group("entry")
        .map_err(|e| CirrusError::Backend(format!("hdf5 sink: create entry: {e}")))?;
    let instr = entry
        .create_group("instrument")
        .map_err(|e| CirrusError::Backend(format!("hdf5 sink: create instrument: {e}")))?;
    let _det = instr
        .create_group(name)
        .map_err(|e| CirrusError::Backend(format!("hdf5 sink: create {name}: {e}")))?;
    let dataset_path = format!("entry/instrument/{name}/data");
    let mut builder = file
        .new_dataset::<u8>()
        .shape([0])
        .max_shape(&[None])
        .chunk(&[chunk_size_bytes])
        .resizable();
    if compress {
        builder = builder.deflate(4);
    }
    let dataset = builder.create(&dataset_path).map_err(|e| {
        CirrusError::Backend(format!("hdf5 sink: create dataset {dataset_path}: {e}"))
    })?;
    Ok((file, dataset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn test_frame(bytes: &'static [u8], seq: u64) -> Frame {
        Frame {
            payload: Bytes::from_static(bytes),
            ts_ns: 0,
            channel: 0,
            flags: 0,
            seq,
        }
    }

    #[tokio::test]
    async fn append_and_read_back_via_dataset() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.h5");
        let sink = Hdf5FrameSink::new("det1", &path, 0);
        sink.accept(test_frame(&[1, 2, 3, 4], 1)).await.unwrap();
        sink.accept(test_frame(&[5, 6, 7, 8], 2)).await.unwrap();
        drop(sink);
        let f = rust_hdf5::H5File::open(&path).unwrap();
        let ds = f.dataset("entry/instrument/det1/data").unwrap();
        assert_eq!(ds.shape(), vec![8], "two 4-byte appends → 8 bytes total");
    }

    #[tokio::test]
    async fn root_carries_nx_class_attribute() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nx.h5");
        let sink = Hdf5FrameSink::new("dx", &path, 0);
        sink.accept(test_frame(&[42], 1)).await.unwrap();
        drop(sink);
        let f = rust_hdf5::H5File::open(&path).unwrap();
        assert_eq!(f.attr_string("NX_class").unwrap(), "NXroot");
        assert_eq!(f.attr_string("default").unwrap(), "entry");
    }
}
