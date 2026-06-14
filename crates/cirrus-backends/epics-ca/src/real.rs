//! Real EPICS Channel Access backend, wired to `epics-ca-rs`.
//!
//! Architecture:
//!
//! - One process-wide `CaClient`, lazily initialized via `OnceCell`.
//! - Channel registry sharded across 64 mutexes (rule **K3**) — `connect()`
//!   does the slow path (`wait_connected`) outside the shard lock.
//! - In-flight de-dup via `pending: HashMap<PvName, Arc<Notify>>` (rule **K4**).
//! - `set_callback` spawns a forwarder task per channel, returning a `SubToken`
//!   whose Drop aborts the task; dropping the underlying `MonitorHandle` then
//!   unsubscribes on the wire (rule **K2**).

use async_trait::async_trait;
use cirrus_core::error::{CirrusError, Result};
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::SubToken;
use cirrus_event_model::{make_datakey, DataKey, Dtype, SignalMetadata};
use cirrus_protocols_async::{ReadingValueCallback, SignalBackend};
use epics_ca_rs::client::{CaChannel, CaClient};
use epics_ca_rs::{DbFieldType, EpicsValue};
use std::array;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

const SHARDS: usize = 64;

fn shard_for(pv: &str) -> usize {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write(pv.as_bytes());
    (h.finish() as usize) % SHARDS
}

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

/// Process-wide CA context.
pub struct CaContext {
    client: Arc<CaClient>,
    shards: [Mutex<HashMap<String, Arc<CaChannel>>>; SHARDS],
    pending: [Mutex<HashMap<String, Arc<Notify>>>; SHARDS],
}

impl CaContext {
    fn new(client: CaClient) -> Self {
        Self {
            client: Arc::new(client),
            shards: array::from_fn(|_| Mutex::new(HashMap::new())),
            pending: array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    async fn get_or_open(&self, pv: &str, timeout: Duration) -> Result<Arc<CaChannel>> {
        let s = shard_for(pv);
        // Fast path
        if let Some(ch) = self.shards[s].lock().unwrap().get(pv).cloned() {
            return Ok(ch);
        }
        // K4: in-flight dedup
        let notify = {
            let mut p = self.pending[s].lock().unwrap();
            if let Some(n) = p.get(pv).cloned() {
                Some(n)
            } else {
                let n = Arc::new(Notify::new());
                p.insert(pv.to_string(), n.clone());
                None
            }
        };
        if let Some(n) = notify {
            n.notified().await;
            // Either the in-flight winner inserted, or it failed — either way
            // re-check the cache.
            if let Some(ch) = self.shards[s].lock().unwrap().get(pv).cloned() {
                return Ok(ch);
            }
            return Err(CirrusError::Backend(format!(
                "ca: peer connect for {pv} failed"
            )));
        }

        // K3: do the I/O (wait_connected) outside the shard lock.
        let ch = self.client.create_channel(pv);
        let res: epics_ca_rs::CaResult<()> = ch.wait_connected(timeout).await;

        // Commit and notify waiters either way.
        let arc = Arc::new(ch);
        let mut p = self.pending[s].lock().unwrap();
        let n = p.remove(pv);
        if res.is_ok() {
            self.shards[s]
                .lock()
                .unwrap()
                .insert(pv.to_string(), arc.clone());
        }
        if let Some(n) = n {
            n.notify_waiters();
        }
        res.map_err(|e| CirrusError::Backend(format!("ca connect {pv}: {e}")))?;
        Ok(arc)
    }
}

static CTX: OnceLock<Arc<CaContext>> = OnceLock::new();

/// Get the shared CA context. Initializes a `CaClient` on first call.
///
/// `CaClient::new` is async; when invoked from a sync caller that is
/// itself already inside a tokio runtime (e.g. a tokio task that
/// constructs `EpicsCaBackend::new(pv)` lazily) the naive
/// `cirrus_runtime().block_on(...)` panics with "Cannot start a runtime
/// from within a runtime". We detect that case via
/// `Handle::try_current()` and bridge through a dedicated OS thread
/// whose context is free of any runtime, then `block_on` on the cirrus
/// process-singleton runtime there.
pub fn ca_context() -> Arc<CaContext> {
    if let Some(c) = CTX.get() {
        return c.clone();
    }
    let client_res = if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|s| {
            s.spawn(|| cirrus_core::runtime::cirrus_runtime().block_on(CaClient::new()))
                .join()
                .expect("ca_context: bootstrap thread panicked")
        })
    } else {
        cirrus_core::runtime::cirrus_runtime().block_on(CaClient::new())
    };
    let client = client_res.expect("CaClient::new failed");
    let ctx = Arc::new(CaContext::new(client));
    let _ = CTX.set(ctx.clone());
    ctx
}

/// How `SignalBackend<String>` maps to CA wire types.
///
/// - `Short` (default): DBR_STRING. Single 40-byte NUL-padded value;
///   strings longer than 39 bytes are truncated. Matches `caput PV
///   "value"`.
/// - `Long`: DBR_CHAR waveform carrying a NUL-terminated string.
///   Matches `caput -S PV "long/path/value"` and ophyd-async's
///   `long_string=True`. Required for areaDetector `FilePath` /
///   `FileName` / `FileTemplate` which are char waveforms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaStringKind {
    /// DBR_STRING — 40-byte cap.
    Short,
    /// DBR_CHAR waveform — long-string convention.
    Long,
}

/// CA backend for a single PV.
pub struct EpicsCaBackend<T: Clone + Send + Sync + 'static> {
    pv: String,
    ctx: Arc<CaContext>,
    channel: tokio::sync::OnceCell<Arc<CaChannel>>,
    /// Consulted only by `SignalBackend<String>`; ignored for other `T`.
    string_kind: CaStringKind,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Clone + Send + Sync + 'static> EpicsCaBackend<T> {
    /// Build with a PV name.
    pub fn new(pv: impl Into<String>) -> Self {
        Self {
            pv: pv.into(),
            ctx: ca_context(),
            channel: tokio::sync::OnceCell::new(),
            string_kind: CaStringKind::Short,
            _marker: std::marker::PhantomData,
        }
    }
}

impl EpicsCaBackend<String> {
    /// Build a String backend that uses the DBR_CHAR-waveform long-string
    /// convention. Required for areaDetector `FilePath` / `FileName` /
    /// `FileTemplate` PVs whose record type is `waveform` of `CHAR`.
    pub fn new_long_string(pv: impl Into<String>) -> Self {
        let mut s = Self::new(pv);
        s.string_kind = CaStringKind::Long;
        s
    }
}

impl<T: Clone + Send + Sync + 'static> cirrus_devices::BackendFromPv for EpicsCaBackend<T> {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
}

impl<T: Clone + Send + Sync + 'static> EpicsCaBackend<T> {
    async fn ensure_channel(&self, timeout: Duration) -> Result<Arc<CaChannel>> {
        self.channel
            .get_or_try_init(|| self.ctx.get_or_open(&self.pv, timeout))
            .await
            .cloned()
    }
}

/// Look up the channel's `native_type` (cheap — `info()` returns
/// already-cached snapshot state).
async fn channel_native_type(ch: &CaChannel) -> Result<DbFieldType> {
    ch.info()
        .await
        .map(|i| i.native_type)
        .map_err(|e| CirrusError::Backend(format!("ca info: {e}")))
}

/// Encode an `f64` payload as the `EpicsValue` variant that matches
/// the channel's `native_type`. Required because
/// `CaChannel::put` writes with `native_type` on the wire (see
/// `epics-ca-rs/src/client/mod.rs:1228`) and a mismatched payload
/// width is read by the server as garbage (e.g. `EpicsValue::Double`
/// sent to a `longout` produces an 8-byte f64-BE payload but the
/// server reads 4 bytes as DBR_LONG, yielding `0x3FF00000`).
fn f64_to_wire(t: DbFieldType, v: f64) -> EpicsValue {
    match t {
        DbFieldType::Double => EpicsValue::Double(v),
        DbFieldType::Float => EpicsValue::Float(v as f32),
        DbFieldType::Long => EpicsValue::Long(v as i32),
        DbFieldType::Int64 => EpicsValue::Int64(v as i64),
        DbFieldType::Short => EpicsValue::Short(v as i16),
        DbFieldType::Char => EpicsValue::Char(v as u8),
        DbFieldType::Enum => EpicsValue::Enum(v as u16),
        DbFieldType::String => EpicsValue::String(format!("{v}")),
    }
}

/// Encode an `i64` payload matching the channel's `native_type`.
fn i64_to_wire(t: DbFieldType, v: i64) -> EpicsValue {
    match t {
        DbFieldType::Int64 => EpicsValue::Int64(v),
        DbFieldType::Double => EpicsValue::Double(v as f64),
        DbFieldType::Float => EpicsValue::Float(v as f32),
        DbFieldType::Long => EpicsValue::Long(v as i32),
        DbFieldType::Short => EpicsValue::Short(v as i16),
        DbFieldType::Char => EpicsValue::Char(v as u8),
        DbFieldType::Enum => EpicsValue::Enum(v as u16),
        DbFieldType::String => EpicsValue::String(format!("{v}")),
    }
}

/// Encode a `bool` payload matching the channel's `native_type`.
fn bool_to_wire(t: DbFieldType, v: bool) -> EpicsValue {
    i64_to_wire(t, if v { 1 } else { 0 })
}

fn epics_to_f64(v: &EpicsValue) -> Option<f64> {
    match v {
        EpicsValue::Double(d) => Some(*d),
        EpicsValue::Float(f) => Some(*f as f64),
        EpicsValue::Long(l) => Some(*l as f64),
        EpicsValue::Short(s) => Some(*s as f64),
        EpicsValue::Char(c) => Some(*c as f64),
        EpicsValue::Int64(i) => Some(*i as f64),
        EpicsValue::Enum(e) => Some(*e as f64),
        _ => None,
    }
}

fn epics_to_i64(v: &EpicsValue) -> Option<i64> {
    match v {
        EpicsValue::Int64(i) => Some(*i),
        EpicsValue::Long(l) => Some(*l as i64),
        EpicsValue::Short(s) => Some(*s as i64),
        EpicsValue::Char(c) => Some(*c as i64),
        EpicsValue::Enum(e) => Some(*e as i64),
        EpicsValue::Float(f) => Some(*f as i64),
        EpicsValue::Double(d) => Some(*d as i64),
        _ => None,
    }
}

fn epics_to_bool(v: &EpicsValue) -> Option<bool> {
    epics_to_i64(v).map(|i| i != 0)
}

/// Decode a String value out of an `EpicsValue` according to the
/// backend's `CaStringKind`. For `Long` we also accept a stray
/// `EpicsValue::String` (some servers reply with DBR_STRING even when
/// the field is a char waveform of length ≤ 39) so we degrade
/// gracefully on get.
fn epics_to_string(v: &EpicsValue, kind: CaStringKind) -> Option<String> {
    match (kind, v) {
        (CaStringKind::Short, EpicsValue::String(s)) => Some(s.clone()),
        (CaStringKind::Long, EpicsValue::CharArray(bytes)) => {
            let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
            Some(String::from_utf8_lossy(&bytes[..end]).into_owned())
        }
        (CaStringKind::Long, EpicsValue::String(s)) => Some(s.clone()),
        (CaStringKind::Short, EpicsValue::CharArray(bytes)) => {
            let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
            Some(String::from_utf8_lossy(&bytes[..end]).into_owned())
        }
        _ => None,
    }
}

/// Build the wire `EpicsValue` for a String put according to `kind`.
/// Long-string puts append a NUL terminator (areaDetector convention).
fn string_to_epics(s: &str, kind: CaStringKind) -> EpicsValue {
    match kind {
        CaStringKind::Short => EpicsValue::String(s.to_string()),
        CaStringKind::Long => {
            let mut bytes = s.as_bytes().to_vec();
            bytes.push(0);
            EpicsValue::CharArray(bytes)
        }
    }
}

#[async_trait]
impl SignalBackend<f64> for EpicsCaBackend<f64> {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.ensure_channel(timeout).await.map(|_| ())
    }
    async fn put(&self, value: Option<f64>) -> Result<()> {
        let value = value.unwrap_or_default();
        let ch = self
            .ensure_channel(Duration::from_secs(2))
            .await
            .map_err(|e| CirrusError::Backend(format!("{e}")))?;
        let native = channel_native_type(&ch)
            .await
            .map_err(|e| CirrusError::Backend(format!("{e}")))?;
        let v = f64_to_wire(native, value);
        ch.put(&v)
            .await
            .map_err(|e| CirrusError::Backend(format!("ca put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let info = ch
            .info()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca info: {e}")))?;
        Ok(make_datakey(
            format!("ca://{source}"),
            Dtype::Number,
            if info.element_count > 1 {
                vec![Some(info.element_count as u64)]
            } else {
                vec![]
            },
            Some("<f8".into()),
            SignalMetadata::default(),
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        let f = epics_to_f64(&v)
            .ok_or_else(|| CirrusError::Backend(format!("ca: not numeric: {v:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(f),
            timestamp: now_ts(),
            alarm_severity: None,
            message: None,
        })
    }
    async fn get_value(&self) -> Result<f64> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        epics_to_f64(&v).ok_or_else(|| CirrusError::Backend(format!("ca: not numeric: {v:?}")))
    }
    async fn get_setpoint(&self) -> Result<f64> {
        // CA channels expose only one read path; we treat readback as the
        // best-effort setpoint as well.
        SignalBackend::<f64>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<f64>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let ctx = self.ctx.clone();
        let pv = self.pv.clone();
        // Spawn a forwarder. Held tokio::JoinHandle is aborted on Drop (K1).
        let handle = tokio::spawn(async move {
            let ch = match ctx.get_or_open(&pv, Duration::from_secs(2)).await {
                Ok(c) => c,
                Err(_) => return,
            };
            let mut sub = match ch.subscribe().await {
                Ok(s) => s,
                Err(_) => return,
            };
            while let Some(Ok(snap)) = sub.recv().await {
                if let Some(f) = epics_to_f64(&snap.value) {
                    let ts = snap
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or_default();
                    cb(&f, ts);
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("ca://{name}")
    }
}

#[async_trait]
impl SignalBackend<String> for EpicsCaBackend<String> {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.ensure_channel(timeout).await.map(|_| ())
    }
    async fn put(&self, value: Option<String>) -> Result<()> {
        let value = value.unwrap_or_default();
        let ch = self
            .ensure_channel(Duration::from_secs(2))
            .await
            .map_err(|e| CirrusError::Backend(format!("{e}")))?;
        let v = string_to_epics(&value, self.string_kind);
        ch.put(&v)
            .await
            .map_err(|e| CirrusError::Backend(format!("ca put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let info = ch
            .info()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca info: {e}")))?;
        let shape = match self.string_kind {
            CaStringKind::Long if info.element_count > 1 => vec![Some(info.element_count as u64)],
            _ => vec![],
        };
        Ok(make_datakey(
            format!("ca://{source}"),
            Dtype::String,
            shape,
            Some("|S".into()),
            SignalMetadata::default(),
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        let s = epics_to_string(&v, self.string_kind)
            .ok_or_else(|| CirrusError::Backend(format!("ca: not stringable: {v:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(s),
            timestamp: now_ts(),
            alarm_severity: None,
            message: None,
        })
    }
    async fn get_value(&self) -> Result<String> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        epics_to_string(&v, self.string_kind)
            .ok_or_else(|| CirrusError::Backend(format!("ca: not stringable: {v:?}")))
    }
    async fn get_setpoint(&self) -> Result<String> {
        SignalBackend::<String>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<String>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let ctx = self.ctx.clone();
        let pv = self.pv.clone();
        let kind = self.string_kind;
        let handle = tokio::spawn(async move {
            let ch = match ctx.get_or_open(&pv, Duration::from_secs(2)).await {
                Ok(c) => c,
                Err(_) => return,
            };
            let mut sub = match ch.subscribe().await {
                Ok(s) => s,
                Err(_) => return,
            };
            while let Some(Ok(snap)) = sub.recv().await {
                if let Some(s) = epics_to_string(&snap.value, kind) {
                    let ts = snap
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or_default();
                    cb(&s, ts);
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("ca://{name}")
    }
}

#[async_trait]
impl SignalBackend<i64> for EpicsCaBackend<i64> {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.ensure_channel(timeout).await.map(|_| ())
    }
    async fn put(&self, value: Option<i64>) -> Result<()> {
        let value = value.unwrap_or_default();
        let ch = self
            .ensure_channel(Duration::from_secs(2))
            .await
            .map_err(|e| CirrusError::Backend(format!("{e}")))?;
        let native = channel_native_type(&ch)
            .await
            .map_err(|e| CirrusError::Backend(format!("{e}")))?;
        let v = i64_to_wire(native, value);
        ch.put(&v)
            .await
            .map_err(|e| CirrusError::Backend(format!("ca put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let info = ch
            .info()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca info: {e}")))?;
        Ok(make_datakey(
            format!("ca://{source}"),
            Dtype::Integer,
            if info.element_count > 1 {
                vec![Some(info.element_count as u64)]
            } else {
                vec![]
            },
            Some("<i8".into()),
            SignalMetadata::default(),
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        let i =
            epics_to_i64(&v).ok_or_else(|| CirrusError::Backend(format!("ca: not int: {v:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(i),
            timestamp: now_ts(),
            alarm_severity: None,
            message: None,
        })
    }
    async fn get_value(&self) -> Result<i64> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        epics_to_i64(&v).ok_or_else(|| CirrusError::Backend(format!("ca: not int: {v:?}")))
    }
    async fn get_setpoint(&self) -> Result<i64> {
        SignalBackend::<i64>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<i64>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let ctx = self.ctx.clone();
        let pv = self.pv.clone();
        let handle = tokio::spawn(async move {
            let ch = match ctx.get_or_open(&pv, Duration::from_secs(2)).await {
                Ok(c) => c,
                Err(_) => return,
            };
            let mut sub = match ch.subscribe().await {
                Ok(s) => s,
                Err(_) => return,
            };
            while let Some(Ok(snap)) = sub.recv().await {
                if let Some(i) = epics_to_i64(&snap.value) {
                    let ts = snap
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or_default();
                    cb(&i, ts);
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("ca://{name}")
    }
}

#[async_trait]
impl SignalBackend<bool> for EpicsCaBackend<bool> {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.ensure_channel(timeout).await.map(|_| ())
    }
    async fn put(&self, value: Option<bool>) -> Result<()> {
        let value = value.unwrap_or_default();
        let ch = self
            .ensure_channel(Duration::from_secs(2))
            .await
            .map_err(|e| CirrusError::Backend(format!("{e}")))?;
        let native = channel_native_type(&ch)
            .await
            .map_err(|e| CirrusError::Backend(format!("{e}")))?;
        let v = bool_to_wire(native, value);
        ch.put(&v)
            .await
            .map_err(|e| CirrusError::Backend(format!("ca put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        Ok(make_datakey(
            format!("ca://{source}"),
            Dtype::Boolean,
            vec![],
            Some("|b1".into()),
            SignalMetadata::default(),
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        let b = epics_to_bool(&v)
            .ok_or_else(|| CirrusError::Backend(format!("ca: not bool: {v:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(b),
            timestamp: now_ts(),
            alarm_severity: None,
            message: None,
        })
    }
    async fn get_value(&self) -> Result<bool> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        epics_to_bool(&v).ok_or_else(|| CirrusError::Backend(format!("ca: not bool: {v:?}")))
    }
    async fn get_setpoint(&self) -> Result<bool> {
        SignalBackend::<bool>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<bool>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let ctx = self.ctx.clone();
        let pv = self.pv.clone();
        let handle = tokio::spawn(async move {
            let ch = match ctx.get_or_open(&pv, Duration::from_secs(2)).await {
                Ok(c) => c,
                Err(_) => return,
            };
            let mut sub = match ch.subscribe().await {
                Ok(s) => s,
                Err(_) => return,
            };
            while let Some(Ok(snap)) = sub.recv().await {
                if let Some(b) = epics_to_bool(&snap.value) {
                    let ts = snap
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or_default();
                    cb(&b, ts);
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("ca://{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression for the bootstrap bug documented at doc/10-roadmap.md
    // Tier 1.1: calling `ca_context()` from inside a tokio runtime
    // previously panicked with "Cannot start a runtime from within a
    // runtime". The fix routes through a dedicated OS thread when a
    // current runtime is detected.
    #[tokio::test(flavor = "multi_thread")]
    async fn ca_context_initializes_from_inside_runtime() {
        // Must not panic. Returns an Arc; we don't dereference into
        // any I/O so the IOC need not be present.
        let _ = ca_context();
    }

    #[test]
    fn long_string_round_trips_via_char_array() {
        let path = "/data/scan42/run0001.h5";
        let v = string_to_epics(path, CaStringKind::Long);
        match &v {
            EpicsValue::CharArray(bytes) => {
                assert_eq!(*bytes.last().unwrap(), 0);
                assert_eq!(&bytes[..path.len()], path.as_bytes());
            }
            _ => panic!("expected CharArray, got {v:?}"),
        }
        let back =
            epics_to_string(&v, CaStringKind::Long).expect("CharArray decodes as Long string");
        assert_eq!(back, path);
    }

    #[test]
    fn short_string_round_trips_via_dbr_string() {
        let v = string_to_epics("13SIM1", CaStringKind::Short);
        match &v {
            EpicsValue::String(s) => assert_eq!(s, "13SIM1"),
            _ => panic!("expected String, got {v:?}"),
        }
        let back = epics_to_string(&v, CaStringKind::Short).unwrap();
        assert_eq!(back, "13SIM1");
    }

    #[test]
    fn long_string_decode_strips_at_first_nul() {
        let bytes = b"/data/scan\0/tail/ignored".to_vec();
        let s =
            epics_to_string(&EpicsValue::CharArray(bytes), CaStringKind::Long).expect("CharArray");
        assert_eq!(s, "/data/scan");
    }

    #[test]
    fn long_string_constructor_flips_kind() {
        let long = EpicsCaBackend::<String>::new_long_string("foo:FilePath");
        assert_eq!(long.string_kind, CaStringKind::Long);
        let short = EpicsCaBackend::<String>::new("bar:Port");
        assert_eq!(short.string_kind, CaStringKind::Short);
    }

    #[test]
    fn native_type_encoding_matches_wire_widths() {
        // Each branch must produce a payload whose `to_bytes().len()`
        // matches the wire size for that DbFieldType. Otherwise
        // `CaChannel::send_write_notify_fast` (which writes with
        // `native_type` on the wire) will send a mismatched payload
        // and the server reads garbage.
        let cases: &[(DbFieldType, usize)] = &[
            (DbFieldType::Double, 8),
            (DbFieldType::Float, 4),
            (DbFieldType::Int64, 8),
            (DbFieldType::Long, 4),
            (DbFieldType::Short, 2),
            (DbFieldType::Enum, 2),
            (DbFieldType::Char, 1),
        ];
        for (t, want) in cases {
            assert_eq!(
                i64_to_wire(*t, 1).to_bytes().len(),
                *want,
                "i64_to_wire {t:?} bytes != native width {want}"
            );
            assert_eq!(
                f64_to_wire(*t, 1.0).to_bytes().len(),
                *want,
                "f64_to_wire {t:?} bytes != native width {want}"
            );
            assert_eq!(
                bool_to_wire(*t, true).to_bytes().len(),
                *want,
                "bool_to_wire {t:?} bytes != native width {want}"
            );
        }
        // String wire is 40-byte NUL-padded DBR_STRING.
        assert_eq!(i64_to_wire(DbFieldType::String, 5).to_bytes().len(), 40);
    }

    #[test]
    fn bool_decodes_from_numeric_variants() {
        assert_eq!(epics_to_bool(&EpicsValue::Long(0)), Some(false));
        assert_eq!(epics_to_bool(&EpicsValue::Long(1)), Some(true));
        assert_eq!(epics_to_bool(&EpicsValue::Enum(1)), Some(true));
        assert_eq!(epics_to_bool(&EpicsValue::Char(0)), Some(false));
        assert_eq!(epics_to_bool(&EpicsValue::String("x".into())), None);
    }
}
