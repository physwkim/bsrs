//! `PvaMonitorSource` — drives a PVA monitor on a single PV and emits one
//! `Frame` per monitor event.
//!
//! - **Generic path**: `PvaClient::pvmonitor` (decoded `PvField`). Works for
//!   any PV type. `pv_field_to_bytes` walks the structure and extracts a
//!   payload.
//! - **NTNDArray fast path**: when the field is `Structure` whose `value`
//!   union carries a `ScalarArrayTyped`, we emit the underlying `Arc<[T]>` as
//!   `Bytes` via `Bytes::from_owner` — **zero-copy** (the bytes share the
//!   refcount with the PVA decode path).
//! - **NTScalar path**: scalar `value` field LE-encoded into 8 bytes.

use crate::core::error::Result;
use crate::protocols_async::{Frame, FrameSource};
use async_trait::async_trait;
use bytes::Bytes;
use epics_pva_rs::client::PvaClient;
use epics_pva_rs::pvdata::TypedScalarArray;
use epics_pva_rs::{PvField, PvStructure, ScalarValue};
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

/// PVA monitor source. One instance = one PV.
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
}

impl Drop for PvaMonitorSource {
    fn drop(&mut self) {
        // K1: dropping the source must terminate the spawned monitor task.
        // The token is shared with the task via clone(); cancel() signals
        // both clones.
        self.cancel.cancel();
    }
}

#[async_trait]
impl FrameSource for PvaMonitorSource {
    fn frames(&self) -> BoxStream<'static, Frame> {
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
        tokio::spawn(async move {
            let cb = move |field: &PvField| {
                if let Some(payload) = pv_field_to_bytes(field) {
                    let n = seq.fetch_add(1, Ordering::SeqCst);
                    let f = Frame {
                        payload,
                        ts_ns: now_ns(),
                        channel: 0,
                        flags: 0,
                        seq: n,
                    };
                    // Bounded queue: drop on full. Overflow is observable via
                    // FramePipe::overflow when the source is wired through
                    // a pipe.
                    let _ = tx.try_send(f);
                }
            };
            let monitor = client.pvmonitor(&pv, cb);
            tokio::select! {
                _ = cancel.cancelled() => {}
                r = monitor => {
                    if let Err(e) = r {
                        tracing::error!("pvmonitor on {pv}: {e}");
                    }
                }
            }
        });
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        self.cancel.cancel();
        Ok(())
    }
}

// -- field → bytes ----------------------------------------------------------

/// Extract bytes from a decoded PVA field. Returns `None` if the shape is not
/// supported (caller skips such events).
pub fn pv_field_to_bytes(field: &PvField) -> Option<Bytes> {
    match field {
        // Top-level scalar.
        PvField::Scalar(v) => Some(scalar_to_bytes(v)),
        PvField::ScalarArrayTyped(arr) => Some(typed_array_to_bytes(arr)),
        PvField::ScalarArray(values) => {
            // Encode a generic scalar array by re-grouping into the dominant
            // scalar type (rare path; pvxs prefers ScalarArrayTyped). Fall
            // back to byte-level concatenation of LE-encoded scalars.
            let mut buf = Vec::with_capacity(values.len() * 8);
            for v in values {
                buf.extend_from_slice(scalar_to_bytes(v).as_ref());
            }
            Some(Bytes::from(buf))
        }
        // NTScalar / NTNDArray / NTArray live in a Structure with .value.
        PvField::Structure(s) => structure_to_bytes(s),
        PvField::Union { value, .. } => pv_field_to_bytes(value),
        _ => None,
    }
}

fn scalar_to_bytes(v: &ScalarValue) -> Bytes {
    match v {
        ScalarValue::Boolean(b) => Bytes::from(vec![*b as u8]),
        ScalarValue::Byte(x) => Bytes::copy_from_slice(&x.to_le_bytes()),
        ScalarValue::UByte(x) => Bytes::from(vec![*x]),
        ScalarValue::Short(x) => Bytes::copy_from_slice(&x.to_le_bytes()),
        ScalarValue::UShort(x) => Bytes::copy_from_slice(&x.to_le_bytes()),
        ScalarValue::Int(x) => Bytes::copy_from_slice(&x.to_le_bytes()),
        ScalarValue::UInt(x) => Bytes::copy_from_slice(&x.to_le_bytes()),
        ScalarValue::Long(x) => Bytes::copy_from_slice(&x.to_le_bytes()),
        ScalarValue::ULong(x) => Bytes::copy_from_slice(&x.to_le_bytes()),
        ScalarValue::Float(x) => Bytes::copy_from_slice(&x.to_le_bytes()),
        ScalarValue::Double(x) => Bytes::copy_from_slice(&x.to_le_bytes()),
        ScalarValue::String(s) => Bytes::copy_from_slice(s.as_bytes()),
    }
}

/// Zero-copy: `Arc<[T]>` is wrapped in a `Bytes::from_owner` adapter that
/// reinterprets the storage as `&[u8]`. Refcount is shared with the original
/// PvField, so when the last consumer drops, the buffer is freed exactly once.
fn typed_array_to_bytes(arr: &TypedScalarArray) -> Bytes {
    match arr {
        TypedScalarArray::Boolean(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::Byte(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::UByte(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::Short(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::UShort(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::Int(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::UInt(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::Long(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::ULong(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::Float(a) => arc_pod_to_bytes(a.clone()),
        TypedScalarArray::Double(a) => arc_pod_to_bytes(a.clone()),
        // String arrays don't have a stable byte layout — punt.
        TypedScalarArray::String(_) => Bytes::new(),
    }
}

/// `Arc<[T]>` → `Bytes` for plain-old-data `T`, zero-copy.
fn arc_pod_to_bytes<T: Copy + Send + Sync + 'static>(arc: Arc<[T]>) -> Bytes {
    struct PodArc<T: Copy + Send + Sync + 'static>(Arc<[T]>);
    impl<T: Copy + Send + Sync + 'static> AsRef<[u8]> for PodArc<T> {
        fn as_ref(&self) -> &[u8] {
            // SAFETY: T is Copy (POD-like — we only call this with primitive
            // numeric and bool types). Reinterpreting `&[T]` as `&[u8]` is
            // sound because primitives have no uninitialized bytes and no
            // Drop. The lifetime of the returned slice is tied to `&self`,
            // and the `Arc` keeps the storage alive while `self` lives.
            unsafe {
                std::slice::from_raw_parts(
                    self.0.as_ptr() as *const u8,
                    std::mem::size_of_val(&*self.0),
                )
            }
        }
    }
    Bytes::from_owner(PodArc(arc))
}

fn structure_to_bytes(s: &PvStructure) -> Option<Bytes> {
    // Look for the canonical NT layout: a `value` field at the top level.
    let value_field = s
        .fields
        .iter()
        .find_map(|(name, f)| if name == "value" { Some(f) } else { None })?;
    pv_field_to_bytes(value_field)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_double_array_zero_copy_round_trip() {
        let v: Arc<[f64]> = Arc::from(vec![1.0, 2.5, -3.25]);
        let bytes = typed_array_to_bytes(&TypedScalarArray::Double(v.clone()));
        assert_eq!(bytes.len(), 3 * std::mem::size_of::<f64>());
        // Re-decode via le_bytes.
        let mut out = Vec::new();
        for chunk in bytes.chunks_exact(8) {
            let arr: [u8; 8] = chunk.try_into().unwrap();
            out.push(f64::from_le_bytes(arr));
        }
        assert_eq!(out, &[1.0, 2.5, -3.25]);
    }

    #[test]
    fn ntscalar_double_field_extracts_value() {
        let inner = PvField::Scalar(ScalarValue::Double(7.5));
        let s = PvStructure {
            struct_id: "epics:nt/NTScalar:1.0".into(),
            fields: vec![("value".into(), inner)],
        };
        let bytes = pv_field_to_bytes(&PvField::Structure(s)).unwrap();
        assert_eq!(bytes.len(), 8);
        let arr: [u8; 8] = bytes.as_ref().try_into().unwrap();
        assert_eq!(f64::from_le_bytes(arr), 7.5);
    }

    #[test]
    fn ntndarray_value_union_zero_copy() {
        let arr: Arc<[f32]> = Arc::from(vec![1.0_f32, 2.0, 3.0, 4.0]);
        let value = PvField::Union {
            selector: 9,
            variant_name: "floatValue".into(),
            value: Box::new(PvField::ScalarArrayTyped(TypedScalarArray::Float(
                arr.clone(),
            ))),
        };
        let s = PvStructure {
            struct_id: "epics:nt/NTNDArray:1.0".into(),
            fields: vec![("value".into(), value)],
        };
        let bytes = pv_field_to_bytes(&PvField::Structure(s)).unwrap();
        assert_eq!(bytes.len(), 4 * std::mem::size_of::<f32>());
        // Verify round-trip values.
        let mut out: Vec<f32> = Vec::new();
        for chunk in bytes.chunks_exact(4) {
            let a: [u8; 4] = chunk.try_into().unwrap();
            out.push(f32::from_le_bytes(a));
        }
        assert_eq!(out, &[1.0, 2.0, 3.0, 4.0]);
    }
}
