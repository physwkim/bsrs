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

use crate::core::error::{BsrsError, Result};
use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use crate::event_model::{make_datakey, DataKey, Dtype, Limits, LimitsRange, SignalMetadata};
use crate::protocols_async::{ReadingValueCallback, SignalBackend};
use async_trait::async_trait;
use epics_base_rs::server::snapshot::{ControlInfo, DbrClass, DisplayInfo};
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

/// Convert a CA snapshot's `SystemTime` to epoch seconds. A `DbrClass::Time`
/// read carries the server's processing time in `snap.timestamp`, which is the
/// reading timestamp ophyd-async reports (`epics/core/_aioca.py:305-310`,
/// `FORMAT_TIME`); the monitor path already stamps from the same field.
fn ts_to_f64(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH)
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
            return Err(BsrsError::Backend(format!(
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
        res.map_err(|e| BsrsError::Backend(format!("ca connect {pv}: {e}")))?;
        Ok(arc)
    }
}

static CTX: OnceLock<Arc<CaContext>> = OnceLock::new();

/// Get the shared CA context. Initializes a `CaClient` on first call.
///
/// `CaClient::new` is async; when invoked from a sync caller that is
/// itself already inside a tokio runtime (e.g. a tokio task that
/// constructs `EpicsCaBackend::new(pv)` lazily) the naive
/// `bsrs_runtime().block_on(...)` panics with "Cannot start a runtime
/// from within a runtime". We detect that case via
/// `Handle::try_current()` and bridge through a dedicated OS thread
/// whose context is free of any runtime, then `block_on` on the bsrs
/// process-singleton runtime there.
pub fn ca_context() -> Arc<CaContext> {
    if let Some(c) = CTX.get() {
        return c.clone();
    }
    let client_res = if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|s| {
            s.spawn(|| crate::core::runtime::bsrs_runtime().block_on(CaClient::new()))
                .join()
                .expect("ca_context: bootstrap thread panicked")
        })
    } else {
        crate::core::runtime::bsrs_runtime().block_on(CaClient::new())
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

impl<T: Clone + Send + Sync + 'static> crate::devices::BackendFromPv for EpicsCaBackend<T> {
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
        .map_err(|e| BsrsError::Backend(format!("ca info: {e}")))
}

/// Map a CA wire alarm severity (`DBR_STS_*` `severity`, a `u16`) to the
/// bsrs/ophyd-async convention: NO_ALARM(0)/MINOR(1)/MAJOR(2) pass through,
/// INVALID(3) and any higher code collapse to -1
/// (`_aioca._make_reading`: `-1 if value.severity > 2 else value.severity`).
fn alarm_severity(raw: u16) -> i32 {
    if raw > 2 {
        -1
    } else {
        raw as i32
    }
}

/// One DBR_CTRL limit range with ophyd-async's inclusion rule
/// (`_aioca._limits_from_augmented_value`): the range is dropped (`None`)
/// when both bounds are NaN or both are exactly zero; otherwise a NaN bound
/// collapses to an open (`None`) side.
fn limits_range(low: f64, high: f64) -> Option<LimitsRange> {
    if (low.is_nan() && high.is_nan()) || (high == low && low == 0.0) {
        return None;
    }
    Some(LimitsRange {
        low: if low.is_nan() { None } else { Some(low) },
        high: if high.is_nan() { None } else { Some(high) },
    })
}

/// Assemble the alarm/control/display/warning ranges from DBR_CTRL
/// display + control info into a `Limits`, each range filtered by
/// [`limits_range`]. Field mapping mirrors ophyd-async's `"alarm"`/`"ctrl"`/
/// `"disp"`/`"warning"` → `alarm`/`control`/`display`/`warning`.
fn ctrl_limits(display: Option<&DisplayInfo>, control: Option<&ControlInfo>) -> Limits {
    let mut limits = Limits::default();
    if let Some(d) = display {
        limits.alarm = limits_range(d.lower_alarm_limit, d.upper_alarm_limit);
        limits.warning = limits_range(d.lower_warning_limit, d.upper_warning_limit);
        limits.display = limits_range(d.lower_disp_limit, d.upper_disp_limit);
    }
    if let Some(c) = control {
        limits.control = limits_range(c.lower_ctrl_limit, c.upper_ctrl_limit);
    }
    limits
}

/// Fetch DBR_CTRL metadata (units, precision, and the four limit ranges) for
/// a numeric channel and shape it into a [`SignalMetadata`], applying
/// ophyd-async's per-field inclusion rules (`_aioca._metadata_from_augmented_value`):
///   - `units` only when the datatype is not string/bool (`want_units`),
///   - `precision` only for floats — ints have no fractional digits
///     (`want_precision`),
///   - `limits` for any numeric, each range filtered by [`limits_range`].
///
/// A field stays `None` when the server published no display/control block.
async fn ctrl_metadata(
    ch: &CaChannel,
    want_units: bool,
    want_precision: bool,
) -> Result<SignalMetadata> {
    let snap = ch
        .get_with_metadata(DbrClass::Ctrl)
        .await
        .map_err(|e| BsrsError::Backend(format!("ca ctrl get: {e}")))?;
    let mut meta = SignalMetadata::default();
    if let Some(d) = snap.display.as_ref() {
        if want_units && !d.units.is_empty() {
            meta.units = Some(d.units.clone());
        }
        if want_precision {
            meta.precision = Some(d.precision as i64);
        }
    }
    let limits = ctrl_limits(snap.display.as_ref(), snap.control.as_ref());
    if limits != Limits::default() {
        meta.limits = Some(limits);
    }
    Ok(meta)
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

/// Decode a numeric waveform `EpicsValue` array into `Vec<f64>`, widening every
/// element type the way [`epics_to_f64`] does for scalars. `CharArray` /
/// `EnumArray` / `StringArray` are intentionally excluded: they carry the
/// long-string and enum-choice meanings owned by the `String` / enum backends,
/// not numeric waveform data.
fn epics_to_vec_f64(v: &EpicsValue) -> Option<Vec<f64>> {
    match v {
        EpicsValue::DoubleArray(a) => Some(a.clone()),
        EpicsValue::FloatArray(a) => Some(a.iter().map(|x| *x as f64).collect()),
        EpicsValue::LongArray(a) => Some(a.iter().map(|x| *x as f64).collect()),
        EpicsValue::ShortArray(a) => Some(a.iter().map(|x| *x as f64).collect()),
        EpicsValue::Int64Array(a) => Some(a.iter().map(|x| *x as f64).collect()),
        _ => None,
    }
}

/// Encode a `Vec<f64>` waveform payload as the array `EpicsValue` variant that
/// matches the channel's `native_type`, mirroring [`f64_to_wire`] for arrays
/// (`CaChannel::put` writes with `native_type`, so a width mismatch corrupts
/// the wire payload). A `String`-native channel has no numeric-array form, so
/// the closest `DoubleArray` is used — a `Vec<f64>` backend on a DBR_STRING PV
/// is a misuse the type system can't forbid here.
fn f64s_to_wire(t: DbFieldType, v: Vec<f64>) -> EpicsValue {
    match t {
        DbFieldType::Double | DbFieldType::String => EpicsValue::DoubleArray(v),
        DbFieldType::Float => EpicsValue::FloatArray(v.iter().map(|x| *x as f32).collect()),
        DbFieldType::Long => EpicsValue::LongArray(v.iter().map(|x| *x as i32).collect()),
        DbFieldType::Int64 => EpicsValue::Int64Array(v.iter().map(|x| *x as i64).collect()),
        DbFieldType::Short => EpicsValue::ShortArray(v.iter().map(|x| *x as i16).collect()),
        DbFieldType::Char => EpicsValue::CharArray(v.iter().map(|x| *x as u8).collect()),
        DbFieldType::Enum => EpicsValue::EnumArray(v.iter().map(|x| *x as u16).collect()),
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
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let native = channel_native_type(&ch)
            .await
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let v = f64_to_wire(native, value);
        ch.put(&v)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let info = ch
            .info()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca info: {e}")))?;
        // Floats carry units, precision, and limits (mirrors ophyd-async
        // `_metadata_from_augmented_value`).
        let meta = ctrl_metadata(&ch, true, true).await?;
        Ok(make_datakey(
            format!("ca://{source}"),
            Dtype::Number,
            if info.element_count > 1 {
                vec![Some(info.element_count as u64)]
            } else {
                vec![]
            },
            Some("<f8".into()),
            meta,
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let snap = ch
            .get_with_metadata(DbrClass::Time)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        let f = epics_to_f64(&snap.value)
            .ok_or_else(|| BsrsError::Backend(format!("ca: not numeric: {:?}", snap.value)))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(f),
            timestamp: ts_to_f64(snap.timestamp),
            alarm_severity: Some(alarm_severity(snap.alarm.severity)),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<f64> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        epics_to_f64(&v).ok_or_else(|| BsrsError::Backend(format!("ca: not numeric: {v:?}")))
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
                    cb(&f, ts, Some(alarm_severity(snap.alarm.severity)));
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str, _read: bool) -> String {
        format!("ca://{name}")
    }
}

#[async_trait]
impl SignalBackend<Vec<f64>> for EpicsCaBackend<Vec<f64>> {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.ensure_channel(timeout).await.map(|_| ())
    }
    async fn put(&self, value: Option<Vec<f64>>) -> Result<()> {
        let value = value.unwrap_or_default();
        let ch = self
            .ensure_channel(Duration::from_secs(2))
            .await
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let native = channel_native_type(&ch)
            .await
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let v = f64s_to_wire(native, value);
        ch.put(&v)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let info = ch
            .info()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca info: {e}")))?;
        // A waveform's `element_count` is its NELM (max capacity) → 1-D shape.
        // Float-element waveforms carry units/precision/limits like scalars.
        let meta = ctrl_metadata(&ch, true, true).await?;
        Ok(make_datakey(
            format!("ca://{source}"),
            Dtype::Number,
            vec![Some(info.element_count as u64)],
            Some("<f8".into()),
            meta,
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let snap = ch
            .get_with_metadata(DbrClass::Time)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        let v = epics_to_vec_f64(&snap.value).ok_or_else(|| {
            BsrsError::Backend(format!("ca: not a numeric array: {:?}", snap.value))
        })?;
        Ok(ReadingValue {
            value: serde_json::Value::from(v),
            timestamp: ts_to_f64(snap.timestamp),
            alarm_severity: Some(alarm_severity(snap.alarm.severity)),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<Vec<f64>> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        epics_to_vec_f64(&v)
            .ok_or_else(|| BsrsError::Backend(format!("ca: not a numeric array: {v:?}")))
    }
    async fn get_setpoint(&self) -> Result<Vec<f64>> {
        SignalBackend::<Vec<f64>>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<Vec<f64>>>) -> SubToken {
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
                if let Some(v) = epics_to_vec_f64(&snap.value) {
                    let ts = snap
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or_default();
                    cb(&v, ts, Some(alarm_severity(snap.alarm.severity)));
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str, _read: bool) -> String {
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
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let v = string_to_epics(&value, self.string_kind);
        ch.put(&v)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let info = ch
            .info()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca info: {e}")))?;
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
        let snap = ch
            .get_with_metadata(DbrClass::Time)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        let s = epics_to_string(&snap.value, self.string_kind)
            .ok_or_else(|| BsrsError::Backend(format!("ca: not stringable: {:?}", snap.value)))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(s),
            timestamp: ts_to_f64(snap.timestamp),
            alarm_severity: Some(alarm_severity(snap.alarm.severity)),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<String> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        epics_to_string(&v, self.string_kind)
            .ok_or_else(|| BsrsError::Backend(format!("ca: not stringable: {v:?}")))
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
                    cb(&s, ts, Some(alarm_severity(snap.alarm.severity)));
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str, _read: bool) -> String {
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
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let native = channel_native_type(&ch)
            .await
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let v = i64_to_wire(native, value);
        ch.put(&v)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let info = ch
            .info()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca info: {e}")))?;
        // Ints carry units and limits, but no precision — there are no
        // fractional digits (ophyd-async excludes precision for `int`).
        let meta = ctrl_metadata(&ch, true, false).await?;
        Ok(make_datakey(
            format!("ca://{source}"),
            Dtype::Integer,
            if info.element_count > 1 {
                vec![Some(info.element_count as u64)]
            } else {
                vec![]
            },
            Some("<i8".into()),
            meta,
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let snap = ch
            .get_with_metadata(DbrClass::Time)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        let i = epics_to_i64(&snap.value)
            .ok_or_else(|| BsrsError::Backend(format!("ca: not int: {:?}", snap.value)))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(i),
            timestamp: ts_to_f64(snap.timestamp),
            alarm_severity: Some(alarm_severity(snap.alarm.severity)),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<i64> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        epics_to_i64(&v).ok_or_else(|| BsrsError::Backend(format!("ca: not int: {v:?}")))
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
                    cb(&i, ts, Some(alarm_severity(snap.alarm.severity)));
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str, _read: bool) -> String {
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
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let native = channel_native_type(&ch)
            .await
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let v = bool_to_wire(native, value);
        ch.put(&v)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca put: {e}")))
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
        let snap = ch
            .get_with_metadata(DbrClass::Time)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        let b = epics_to_bool(&snap.value)
            .ok_or_else(|| BsrsError::Backend(format!("ca: not bool: {:?}", snap.value)))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(b),
            timestamp: ts_to_f64(snap.timestamp),
            alarm_severity: Some(alarm_severity(snap.alarm.severity)),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<bool> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        epics_to_bool(&v).ok_or_else(|| BsrsError::Backend(format!("ca: not bool: {v:?}")))
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
                    cb(&b, ts, Some(alarm_severity(snap.alarm.severity)));
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str, _read: bool) -> String {
        format!("ca://{name}")
    }
}

// -- DBR_ENUM backend (mbbi/mbbo, areaDetector ImageMode/TriggerMode/...) -----

/// Map a `DBR_ENUM` index to its choice label, falling back to the decimal
/// index when the index is out of range or the choice list is unknown — a
/// monitor must never silently drop a value.
fn enum_index_to_label(idx: u16, choices: &[String]) -> String {
    choices
        .get(idx as usize)
        .cloned()
        .unwrap_or_else(|| idx.to_string())
}

/// Map a put request — a choice label or a decimal index string — to its
/// `DBR_ENUM` index. Errors when a non-numeric label is not among the choices,
/// or a numeric index is out of range of a known (non-empty) choice list.
fn enum_label_to_index(req: &str, choices: &[String]) -> Result<u16> {
    if let Some(i) = choices.iter().position(|c| c == req) {
        return Ok(i as u16);
    }
    if let Ok(i) = req.parse::<u16>() {
        if choices.is_empty() || (i as usize) < choices.len() {
            return Ok(i);
        }
    }
    Err(BsrsError::Backend(format!(
        "ca enum: '{req}' is not a valid choice (choices: {choices:?})"
    )))
}

/// Decode an `EpicsValue` read from an enum PV into its label. Accepts a raw
/// `Enum` index (mapped via `choices`), an integer index, or a `String` the
/// server already resolved to a label.
fn epics_to_enum_label(v: &EpicsValue, choices: &[String]) -> Option<String> {
    match v {
        EpicsValue::Enum(idx) => Some(enum_index_to_label(*idx, choices)),
        EpicsValue::Short(i) => Some(enum_index_to_label(*i as u16, choices)),
        EpicsValue::Long(i) => Some(enum_index_to_label(*i as u16, choices)),
        EpicsValue::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// CA backend for `DBR_ENUM` PVs (`mbbi`/`mbbo`, areaDetector `ImageMode`,
/// `TriggerMode`, `FileWriteMode`, `DetectorState_RBV`, …). Presents the enum
/// as its label `String`, mirroring ophyd-async `CaEnumConverter`: get and
/// subscribe map the wire index to the choice label; put maps a label (or a
/// decimal index string) back to the `DBR_ENUM` index. The choice list is
/// fetched once at connect via `DbrClass::Ctrl`, cached, and surfaced in
/// `DataKey.choices` so plans validate against the choice set.
pub struct CaEnumBackend {
    pv: String,
    ctx: Arc<CaContext>,
    channel: tokio::sync::OnceCell<Arc<CaChannel>>,
    choices: OnceLock<Vec<String>>,
}

impl CaEnumBackend {
    /// Build with a PV name.
    pub fn new(pv: impl Into<String>) -> Self {
        Self {
            pv: pv.into(),
            ctx: ca_context(),
            channel: tokio::sync::OnceCell::new(),
            choices: OnceLock::new(),
        }
    }

    async fn ensure_channel(&self, timeout: Duration) -> Result<Arc<CaChannel>> {
        self.channel
            .get_or_try_init(|| self.ctx.get_or_open(&self.pv, timeout))
            .await
            .cloned()
    }

    /// Fetch the enum choice labels via a `DbrClass::Ctrl` read and cache them.
    /// Returns the cached list on subsequent calls (no further network I/O).
    async fn ensure_choices(&self, ch: &CaChannel) -> Result<Vec<String>> {
        if let Some(c) = self.choices.get() {
            return Ok(c.clone());
        }
        let snap = ch
            .get_with_metadata(DbrClass::Ctrl)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca enum ctrl get {}: {e}", self.pv)))?;
        let choices = snap.enums.map(|e| e.strings).unwrap_or_default();
        // First writer wins; a concurrent fetch resolves to the same labels.
        let _ = self.choices.set(choices);
        Ok(self.choices.get().cloned().unwrap_or_default())
    }

    /// Read the current value and resolve it to its choice label.
    async fn read_label(&self) -> Result<String> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let choices = self.ensure_choices(&ch).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| BsrsError::Backend(format!("ca enum get: {e}")))?;
        epics_to_enum_label(&v, &choices)
            .ok_or_else(|| BsrsError::Backend(format!("ca enum: not enum-decodable: {v:?}")))
    }
}

impl crate::devices::BackendFromPv for CaEnumBackend {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
}

#[async_trait]
impl SignalBackend<String> for CaEnumBackend {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        let ch = self.ensure_channel(timeout).await?;
        // Warm the choice cache so describe()/get() never block on it later.
        self.ensure_choices(&ch).await?;
        Ok(())
    }
    async fn put(&self, value: Option<String>) -> Result<()> {
        let value = value.unwrap_or_default();
        let ch = self
            .ensure_channel(Duration::from_secs(2))
            .await
            .map_err(|e| BsrsError::Backend(format!("{e}")))?;
        let choices = self.ensure_choices(&ch).await?;
        let idx = enum_label_to_index(&value, &choices)?;
        // Enum PVs are DBR_ENUM on the wire; put writes with native_type, so the
        // payload must be the 2-byte index, not a 40-byte DBR_STRING.
        ch.put(&EpicsValue::Enum(idx))
            .await
            .map_err(|e| BsrsError::Backend(format!("ca enum put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let choices = self.ensure_choices(&ch).await?;
        Ok(make_datakey(
            format!("ca://{source}"),
            Dtype::String,
            vec![],
            Some("|S".into()),
            SignalMetadata {
                choices: Some(choices),
                ..SignalMetadata::default()
            },
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        // A Reading carries the alarm severity and server timestamp that
        // `read_label` discards, so inline a `Time` metadata read (value +
        // severity + timestamp) and decode the label against the cached choices
        // rather than going through `read_label`.
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let choices = self.ensure_choices(&ch).await?;
        let snap = ch
            .get_with_metadata(DbrClass::Time)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca enum get: {e}")))?;
        let label = epics_to_enum_label(&snap.value, &choices).ok_or_else(|| {
            BsrsError::Backend(format!("ca enum: not enum-decodable: {:?}", snap.value))
        })?;
        Ok(ReadingValue {
            value: serde_json::Value::from(label),
            timestamp: ts_to_f64(snap.timestamp),
            alarm_severity: Some(alarm_severity(snap.alarm.severity)),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<String> {
        self.read_label().await
    }
    async fn get_setpoint(&self) -> Result<String> {
        self.read_label().await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<String>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let ctx = self.ctx.clone();
        let pv = self.pv.clone();
        let cached = self.choices.get().cloned();
        let handle = tokio::spawn(async move {
            let ch = match ctx.get_or_open(&pv, Duration::from_secs(2)).await {
                Ok(c) => c,
                Err(_) => return,
            };
            // Resolve the choice list once (the cached value if connect warmed
            // it, else a one-shot Ctrl read) so each update maps the wire index
            // to its label.
            let choices = match cached {
                Some(c) => c,
                None => ch
                    .get_with_metadata(DbrClass::Ctrl)
                    .await
                    .ok()
                    .and_then(|s| s.enums.map(|e| e.strings))
                    .unwrap_or_default(),
            };
            let mut sub = match ch.subscribe().await {
                Ok(s) => s,
                Err(_) => return,
            };
            while let Some(Ok(snap)) = sub.recv().await {
                if let Some(label) = epics_to_enum_label(&snap.value, &choices) {
                    let ts = snap
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or_default();
                    cb(&label, ts, Some(alarm_severity(snap.alarm.severity)));
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str, _read: bool) -> String {
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
    fn alarm_severity_maps_invalid_and_above_to_negative_one() {
        // NO_ALARM / MINOR / MAJOR pass through unchanged.
        assert_eq!(alarm_severity(0), 0);
        assert_eq!(alarm_severity(1), 1);
        assert_eq!(alarm_severity(2), 2);
        // INVALID(3) and any higher code collapse to -1, matching
        // ophyd-async `_aioca` (`-1 if value.severity > 2 else value.severity`).
        assert_eq!(alarm_severity(3), -1);
        assert_eq!(alarm_severity(4), -1);
        assert_eq!(alarm_severity(u16::MAX), -1);
    }

    #[test]
    fn limits_range_inclusion_rules() {
        // Both NaN → dropped (limit field absent for this record).
        assert_eq!(limits_range(f64::NAN, f64::NAN), None);
        // Both exactly zero → dropped (unset DBR_CTRL limit pair).
        assert_eq!(limits_range(0.0, 0.0), None);
        // One-sided NaN → open on that side.
        assert_eq!(
            limits_range(f64::NAN, 10.0),
            Some(LimitsRange {
                low: None,
                high: Some(10.0)
            })
        );
        assert_eq!(
            limits_range(-5.0, f64::NAN),
            Some(LimitsRange {
                low: Some(-5.0),
                high: None
            })
        );
        // Ordinary pair retained.
        assert_eq!(
            limits_range(-5.0, 5.0),
            Some(LimitsRange {
                low: Some(-5.0),
                high: Some(5.0)
            })
        );
        // Equal nonzero bounds retained (only 0==0 is dropped).
        assert_eq!(
            limits_range(2.0, 2.0),
            Some(LimitsRange {
                low: Some(2.0),
                high: Some(2.0)
            })
        );
    }

    #[test]
    fn ctrl_limits_maps_alarm_warning_display_control() {
        let display = DisplayInfo {
            lower_alarm_limit: -10.0,
            upper_alarm_limit: 10.0,
            lower_warning_limit: -5.0,
            upper_warning_limit: 5.0,
            lower_disp_limit: -100.0,
            upper_disp_limit: 100.0,
            ..Default::default()
        };
        let control = ControlInfo {
            lower_ctrl_limit: -50.0,
            upper_ctrl_limit: 50.0,
        };
        let limits = ctrl_limits(Some(&display), Some(&control));
        assert_eq!(
            limits.alarm,
            Some(LimitsRange {
                low: Some(-10.0),
                high: Some(10.0)
            })
        );
        assert_eq!(
            limits.warning,
            Some(LimitsRange {
                low: Some(-5.0),
                high: Some(5.0)
            })
        );
        assert_eq!(
            limits.display,
            Some(LimitsRange {
                low: Some(-100.0),
                high: Some(100.0)
            })
        );
        assert_eq!(
            limits.control,
            Some(LimitsRange {
                low: Some(-50.0),
                high: Some(50.0)
            })
        );
    }

    #[test]
    fn ctrl_limits_default_or_absent_info_yields_no_ranges() {
        // A record with all-zero limits (DBR_CTRL default) → every range dropped.
        assert_eq!(
            ctrl_limits(Some(&DisplayInfo::default()), Some(&ControlInfo::default())),
            Limits::default()
        );
        // Absent display/control blocks → also empty.
        assert_eq!(ctrl_limits(None, None), Limits::default());
    }

    #[test]
    fn epics_to_vec_f64_widens_numeric_arrays_and_rejects_non_numeric() {
        assert_eq!(
            epics_to_vec_f64(&EpicsValue::DoubleArray(vec![1.0, 2.0])),
            Some(vec![1.0, 2.0])
        );
        assert_eq!(
            epics_to_vec_f64(&EpicsValue::FloatArray(vec![1.5, 2.5])),
            Some(vec![1.5, 2.5])
        );
        assert_eq!(
            epics_to_vec_f64(&EpicsValue::LongArray(vec![3, 4])),
            Some(vec![3.0, 4.0])
        );
        assert_eq!(
            epics_to_vec_f64(&EpicsValue::ShortArray(vec![5, 6])),
            Some(vec![5.0, 6.0])
        );
        assert_eq!(
            epics_to_vec_f64(&EpicsValue::Int64Array(vec![7, 8])),
            Some(vec![7.0, 8.0])
        );
        // Char/Enum/String arrays carry non-numeric-waveform meanings; excluded.
        assert_eq!(epics_to_vec_f64(&EpicsValue::CharArray(vec![1, 2])), None);
        assert_eq!(
            epics_to_vec_f64(&EpicsValue::StringArray(vec!["a".into()])),
            None
        );
        // A scalar is not a waveform.
        assert_eq!(epics_to_vec_f64(&EpicsValue::Double(1.0)), None);
    }

    #[test]
    fn f64s_to_wire_matches_native_array_type() {
        match f64s_to_wire(DbFieldType::Double, vec![1.0, 2.0]) {
            EpicsValue::DoubleArray(a) => assert_eq!(a, vec![1.0, 2.0]),
            other => panic!("expected DoubleArray, got {other:?}"),
        }
        match f64s_to_wire(DbFieldType::Float, vec![1.0, 2.0]) {
            EpicsValue::FloatArray(a) => assert_eq!(a, vec![1.0f32, 2.0]),
            other => panic!("expected FloatArray, got {other:?}"),
        }
        match f64s_to_wire(DbFieldType::Long, vec![3.9, 4.1]) {
            // f64 → i32 truncates toward zero.
            EpicsValue::LongArray(a) => assert_eq!(a, vec![3, 4]),
            other => panic!("expected LongArray, got {other:?}"),
        }
        match f64s_to_wire(DbFieldType::Short, vec![5.0]) {
            EpicsValue::ShortArray(a) => assert_eq!(a, vec![5i16]),
            other => panic!("expected ShortArray, got {other:?}"),
        }
        match f64s_to_wire(DbFieldType::Char, vec![65.0]) {
            EpicsValue::CharArray(a) => assert_eq!(a, vec![65u8]),
            other => panic!("expected CharArray, got {other:?}"),
        }
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

    // -- DB-05: DBR_ENUM label/index mapping --------------------------------

    fn image_mode_choices() -> Vec<String> {
        vec![
            "Single".to_string(),
            "Multiple".to_string(),
            "Continuous".to_string(),
        ]
    }

    #[test]
    fn enum_label_round_trips_via_choices() {
        let choices = image_mode_choices();
        assert_eq!(enum_index_to_label(1, &choices), "Multiple");
        assert_eq!(enum_label_to_index("Continuous", &choices).unwrap(), 2);
        // A decimal index string is accepted in range.
        assert_eq!(enum_label_to_index("0", &choices).unwrap(), 0);
        // An unknown label is rejected.
        assert!(enum_label_to_index("Bogus", &choices).is_err());
        // A numeric index outside a known choice list is rejected.
        assert!(enum_label_to_index("9", &choices).is_err());
    }

    #[test]
    fn enum_index_out_of_range_falls_back_to_decimal() {
        let choices = image_mode_choices();
        // Out-of-range index never panics or drops — decimal fallback.
        assert_eq!(enum_index_to_label(7, &choices), "7");
        // With no choice list (server gave none) the index passes through, and
        // a numeric put is still accepted.
        assert_eq!(enum_index_to_label(3, &[]), "3");
        assert_eq!(enum_label_to_index("3", &[]).unwrap(), 3);
    }

    #[test]
    fn epics_value_decodes_to_enum_label() {
        let choices = image_mode_choices();
        assert_eq!(
            epics_to_enum_label(&EpicsValue::Enum(2), &choices).as_deref(),
            Some("Continuous")
        );
        // Servers that hand back the index as a plain integer still map.
        assert_eq!(
            epics_to_enum_label(&EpicsValue::Long(0), &choices).as_deref(),
            Some("Single")
        );
        // An already-resolved label string passes through.
        assert_eq!(
            epics_to_enum_label(&EpicsValue::String("Multiple".into()), &choices).as_deref(),
            Some("Multiple")
        );
        // A non-enum-shaped value is not decodable.
        assert_eq!(
            epics_to_enum_label(&EpicsValue::Double(1.5), &choices),
            None
        );
    }
}
