//! Real PVA backend wired to `epics-pva-rs::PvaClient`.

use crate::core::error::{BsrsError, Result};
use crate::core::ndarray::{NdArray, NdData};
use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use crate::core::table::{Column, Table, TypedTable};
use crate::event_model::{make_datakey, DataKey, Dtype, Limits, LimitsRange, SignalMetadata};
use crate::protocols_async::{PutCompletion, ReadingValueCallback, SignalBackend};
use async_trait::async_trait;
use epics_pva_rs::client::PvaClient;
use epics_pva_rs::client_native::ops_v2::{effective_typed_put_path, PutLeaf};
use epics_pva_rs::pv_request::{PvRequestBuilder, PvRequestExpr};
use epics_pva_rs::pvdata::{FieldDesc, PvStructure, ScalarType, TypedScalarArray};
use epics_pva_rs::{PvField, PvaError, ScalarValue};
use futures::FutureExt;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

/// Deadline of every client op but a put's completion, which is the caller's.
const OP_TIMEOUT: Duration = Duration::from_secs(5);

static CTX: OnceLock<Arc<PvaClient>> = OnceLock::new();

/// Process-wide PVA client.
pub fn pva_context() -> Arc<PvaClient> {
    CTX.get_or_init(|| Arc::new(PvaClient::builder().timeout(OP_TIMEOUT).build()))
        .clone()
}

/// PVA backend for one PV.
pub struct EpicsPvaBackend<T: Clone + Send + Sync + 'static> {
    pv: String,
    client: Arc<PvaClient>,
    /// What the PV is read as: `T`'s datatype, or [`Datatype::Number`].
    want: Datatype,
    /// The converter the latest `connect`, or before one the first call,
    /// made for the PV.
    converter: Mutex<Option<Arc<PvaConverter>>>,
    /// ophyd-async's `initial_values`: what `connect` read, which
    /// `put(None)` writes.
    initial: Mutex<Option<T>>,
    /// ophyd-async's `EpicsOptions.wait`: whether a put of a value waits
    /// for the record to process.
    wait: fn(&T) -> bool,
}

impl<T: PvaType> EpicsPvaBackend<T> {
    /// Build with a PV name. `connect` refuses a PV that cannot be read as
    /// `T`, as ophyd-async's `make_converter`: `f64` is a float NTScalar,
    /// `i64` an integer one (or a float one at precision 0), `bool` a
    /// two-choice NTEnum or a bool, int8, uint8, int32 or uint32 NTScalar,
    /// `String` a string NTScalar, an NTEnum's labels or an int8 array's
    /// text, and a `Vec` an NTScalarArray of its element's type: `bool`,
    /// `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `i64`, `u64`, `f32`, `f64` or
    /// `String`, an [`NdArray`] an NTNDArray, which is never written, and a
    /// [`Table`] or a [`TypedTable`] an NTTable.
    pub fn new(pv: impl Into<String>) -> Self {
        Self::reading_as(pv.into(), T::DATATYPE)
    }

    fn reading_as(pv: String, want: Datatype) -> Self {
        Self {
            pv,
            client: pva_context(),
            want,
            converter: Mutex::new(None),
            initial: Mutex::new(None),
            wait: |_| true,
        }
    }

    /// Put the values `wait` refuses with `record[block=false]`, as
    /// ophyd-async's `EpicsOptions(wait=...)`: a put to `.STOP` that blocks
    /// would queue behind the move it is meant to stop.
    pub fn with_wait(self, wait: fn(&T) -> bool) -> Self {
        Self { wait, ..self }
    }

    /// `connect`: make the converter from a [`connect_read`] whose value is
    /// the one `put(None)` writes, as ophyd-async's connect-time `pvget` and
    /// `make_converter`.
    async fn connect_reading(&self, timeout: Duration) -> Result<()> {
        let top = connect_read(&self.client, &self.pv, timeout).await?;
        let conv = make_converter(&self.pv, self.want, &top)?;
        let v = T::decode(&conv, &top).ok_or_else(|| self.undecodable(&top))?;
        *self.converter.lock().unwrap() = Some(Arc::new(conv));
        *self.initial.lock().unwrap() = Some(v);
        Ok(())
    }

    /// A full read, with the converter `connect` made, or if none has, one
    /// made from this read.
    async fn read(&self) -> Result<(Arc<PvaConverter>, PvField)> {
        let top = pvget(&self.client, &self.pv).await?;
        let cached = self.converter.lock().unwrap().clone();
        let conv = match cached {
            Some(conv) => conv,
            None => {
                let conv = Arc::new(make_converter(&self.pv, self.want, &top)?);
                *self.converter.lock().unwrap() = Some(conv.clone());
                conv
            }
        };
        Ok((conv, top))
    }

    async fn value(&self) -> Result<T> {
        let (conv, top) = self.read().await?;
        T::decode(&conv, &top).ok_or_else(|| self.undecodable(&top))
    }

    fn undecodable(&self, top: &PvField) -> BsrsError {
        undecodable(&format!("pva://{}", self.pv), top)
    }

    /// `value`, or for `None` the value `connect` read, read now if no
    /// `connect` has.
    async fn or_initial(&self, value: Option<T>) -> Result<T> {
        if let Some(v) = value.or_else(|| self.initial.lock().unwrap().clone()) {
            return Ok(v);
        }
        let v = self.value().await?;
        *self.initial.lock().unwrap() = Some(v.clone());
        Ok(v)
    }

    async fn put_value(&self, value: Option<T>) -> Result<PutCompletion> {
        let value = self.or_initial(value).await?;
        let cached = self.converter.lock().unwrap().clone();
        let conv = match cached {
            Some(conv) => conv,
            None => self.read().await?.0,
        };
        let block = (self.wait)(&value);
        put_block(&self.client, &self.pv, T::encode(&conv, value)?, block).await
    }
}

impl EpicsPvaBackend<f64> {
    /// ophyd-async's `datatype=None` read as a number: any numeric or bool
    /// NTScalar, an NTEnum as its index, for the devices whose PV is the
    /// user's to name (`pva_detector`).
    #[cfg(feature = "host")]
    pub(crate) fn new_inferred(pv: impl Into<String>) -> Self {
        Self::reading_as(pv.into(), Datatype::Number)
    }
}

impl<T: PvaType> crate::devices::BackendFromPv for EpicsPvaBackend<T> {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
}

/// A full read of `pv`.
async fn pvget(client: &PvaClient, pv: &str) -> Result<PvField> {
    client.pvget(pv).await.map_err(|e| pva_failed("get", pv, e))
}

/// A read whose value is not of the type the converter was made for: the PV
/// changed type since the connect.
fn undecodable(source: &str, top: &PvField) -> BsrsError {
    BsrsError::Backend(format!(
        "{source}: {top:?} is not of the PV's connect-time type"
    ))
}

/// PVA's `op` on `pv` failed with `e`, naming the PV as `ca_failed` does.
fn pva_failed(op: &str, pv: &str, e: impl std::fmt::Display) -> BsrsError {
    BsrsError::Backend(format!("pva {op} {pv}: {e}"))
}

/// Wait up to `timeout` for `pv` to connect. `pvconnect` is bounded by the
/// client's `OP_TIMEOUT` instead; this watcher searches until the caller's
/// deadline, and dropping its handle stops the watch, not the channel. Only
/// the timeout is not-connected, as p4p's `TimeoutError` at connect.
async fn pva_connect(client: &PvaClient, pv: &str, timeout: Duration) -> Result<()> {
    let connected = Arc::new(tokio::sync::Notify::new());
    let on_connect = connected.clone();
    let _watch = client
        .connect(pv)
        .on_connect(move || on_connect.notify_one())
        .exec()
        .await
        .map_err(|e| pva_failed("connect", pv, e))?;
    tokio::time::timeout(timeout, connected.notified())
        .await
        .map_err(|_| {
            let why = format_args!("no channel in {timeout:?}");
            BsrsError::not_connected(format!("pva://{pv}"), why)
        })
}

/// Wait for `pv`'s channel, then read it in full, both within `timeout`, as
/// ophyd-async's connect-time `pvget_with_timeout`.
async fn connect_read(client: &PvaClient, pv: &str, timeout: Duration) -> Result<PvField> {
    let deadline = tokio::time::Instant::now() + timeout;
    pva_connect(client, pv, timeout).await?;
    tokio::time::timeout_at(deadline, pvget(client, pv))
        .await
        .map_err(|_| {
            let why = format_args!("no value in {timeout:?}");
            BsrsError::not_connected(format!("pva://{pv}"), why)
        })?
}

/// PVA put of `f` with `record[block=...]`. Blocking, QSRV answers it once
/// the record has processed (`dbProcessNotify`), as rsrv does a
/// put-callback, so the completion is that of the move or acquisition the
/// put starts, with no deadline of its own. Returns once DATA is queued on
/// the circuit, so puts are sent in call order.
async fn put_block(client: &PvaClient, pv: &str, f: PvField, block: bool) -> Result<PutCompletion> {
    let put_err = |e: PvaError| pva_failed("put", pv, e);
    let req = PvRequestBuilder::new()
        .field("value")
        .record("block", block)
        .build();
    // A string may name an enum choice, which only the readback lists.
    let fetch_present = matches!(f, PvField::Scalar(ScalarValue::String(_)));
    let begin = client.pvput_begin_unbounded(pv, Some(&req), fetch_present);
    let mut op = tokio::time::timeout(OP_TIMEOUT, begin)
        .await
        .unwrap_or(Err(PvaError::Timeout))
        .map_err(put_err)?;
    let present = op.take_present().map(|(v, _)| v);
    let (path, leaf) = put_leaf(op.introspection(), present.as_ref(), f);
    let sent = op
        .send_fields_typed(&[(path, PutLeaf::Typed(leaf))])
        .await
        .map_err(put_err)?;
    let pv = pv.to_string();
    Ok(async move { sent.wait().await.map_err(|e| pva_failed("put", &pv, e)) }.boxed())
}

/// Where a put of `f` lands in a PV of shape `intro`, and what it writes
/// there, as p4p assigns into the remote type (`pvxs_value.cpp:319-352`).
/// The path is pvxs `linkBuildPut`'s, which ends at an `enum_t`'s `index`;
/// a string naming one of the readback's `choices` writes its place, and
/// anything else is left to the leaf's own coercion.
fn put_leaf(intro: &FieldDesc, present: Option<&PvField>, f: PvField) -> (String, PvField) {
    let path = effective_typed_put_path(intro, "");
    let choice = match (&f, present) {
        (PvField::Scalar(ScalarValue::String(s)), Some(PvField::Structure(p))) => {
            sub_fields(&p.fields, "value")
                .and_then(|v| field(v, "choices"))
                .and_then(pv_string_array)
                .and_then(|c| c.iter().position(|c| *c == s.as_str_lossy()))
        }
        _ => None,
    };
    match choice {
        Some(i) => (path, PvField::Scalar(ScalarValue::Int(i as i32))),
        None => (path, f),
    }
}

// Coerce one scalar to f64, as Python `float()` does any numeric value.
fn scalar_to_f64(s: &ScalarValue) -> Option<f64> {
    match s {
        ScalarValue::Double(d) => Some(*d),
        ScalarValue::Float(f) => Some(*f as f64),
        ScalarValue::Int(i) => Some(*i as f64),
        ScalarValue::Long(l) => Some(*l as f64),
        ScalarValue::Short(s) => Some(*s as f64),
        ScalarValue::Byte(b) => Some(*b as f64),
        ScalarValue::UByte(b) => Some(*b as f64),
        ScalarValue::UShort(s) => Some(*s as f64),
        ScalarValue::UInt(u) => Some(*u as f64),
        ScalarValue::ULong(u) => Some(*u as f64),
        _ => None,
    }
}

fn pv_field_to_f64(p: &PvField) -> Option<f64> {
    match p {
        PvField::Scalar(s) => scalar_to_f64(s),
        PvField::Structure(s) => {
            // NTScalar shape: { value: scalar, ... }. Try `.value` first.
            s.fields
                .iter()
                .find(|(name, _)| name == "value")
                .and_then(|(_, f)| pv_field_to_f64(f))
        }
        _ => None,
    }
}

fn pv_field_to_i64(p: &PvField) -> Option<i64> {
    match p {
        PvField::Scalar(s) => match s {
            ScalarValue::Long(l) => Some(*l),
            ScalarValue::Int(i) => Some(*i as i64),
            ScalarValue::Short(s) => Some(*s as i64),
            ScalarValue::Byte(b) => Some(*b as i64),
            ScalarValue::UByte(b) => Some(*b as i64),
            ScalarValue::UShort(s) => Some(*s as i64),
            ScalarValue::UInt(u) => Some(*u as i64),
            ScalarValue::ULong(u) => Some(*u as i64),
            ScalarValue::Boolean(b) => Some(*b as i64),
            ScalarValue::Float(f) => Some(*f as i64),
            ScalarValue::Double(d) => Some(*d as i64),
            _ => None,
        },
        PvField::Structure(s) => s
            .fields
            .iter()
            .find(|(name, _)| name == "value")
            .and_then(|(_, f)| pv_field_to_i64(f)),
        _ => None,
    }
}

// Extract an array of strings (e.g. an NTEnum's `choices`) regardless of
// whether the client decoded it as the legacy `ScalarArray` or the typed
// `ScalarArrayTyped` fast-path. Returns None if any element is non-string.
fn pv_string_array(p: &PvField) -> Option<Vec<String>> {
    let values = match p {
        PvField::ScalarArray(items) => items.clone(),
        PvField::ScalarArrayTyped(arr) => arr.to_scalar_values(),
        _ => return None,
    };
    values
        .iter()
        .map(|v| match v {
            ScalarValue::String(s) => Some(s.as_str_lossy().into_owned()),
            _ => None,
        })
        .collect()
}

// Decode an NTEnum value substructure `{ index: int, choices: [string] }` to
// its selected label `choices[index]`, mirroring ophyd-async's
// `PvaEnumConverter` (`value["choices"][value["index"]]`, _p4p.py:168-178). An
// index outside the choices range falls back to its decimal string (matching
// the CA enum backend's out-of-range behaviour). Returns None when `fields` is
// not an NTEnum value (missing `index` or `choices`).
fn pv_enum_to_label(fields: &[(String, PvField)]) -> Option<String> {
    let index = fields
        .iter()
        .find(|(n, _)| n == "index")
        .and_then(|(_, f)| pv_field_to_i64(f))?;
    let choices = fields
        .iter()
        .find(|(n, _)| n == "choices")
        .and_then(|(_, f)| pv_string_array(f))?;
    Some(
        usize::try_from(index)
            .ok()
            .and_then(|i| choices.get(i).cloned())
            .unwrap_or_else(|| index.to_string()),
    )
}

// NTScalar carries an optional `timeStamp` substructure with
// `secondsPastEpoch` (Long) and `nanoseconds` (Int). Return the
// composed `f64` epoch timestamp when both are present; None otherwise.
fn pv_field_to_ts(p: &PvField) -> Option<f64> {
    let PvField::Structure(s) = p else {
        return None;
    };
    let ts = s.fields.iter().find(|(n, _)| n == "timeStamp")?;
    let PvField::Structure(t) = &ts.1 else {
        return None;
    };
    let secs = t
        .fields
        .iter()
        .find(|(n, _)| n == "secondsPastEpoch")
        .and_then(|(_, f)| match f {
            PvField::Scalar(ScalarValue::Long(l)) => Some(*l as f64),
            PvField::Scalar(ScalarValue::ULong(u)) => Some(*u as f64),
            _ => None,
        })?;
    let nanos = t
        .fields
        .iter()
        .find(|(n, _)| n == "nanoseconds")
        .and_then(|(_, f)| match f {
            PvField::Scalar(ScalarValue::Int(i)) => Some(*i as f64),
            PvField::Scalar(ScalarValue::UInt(u)) => Some(*u as f64),
            _ => None,
        })
        .unwrap_or(0.0);
    Some(secs + nanos / 1.0e9)
}

// EPICS alarm severity from an NTScalar/NTEnum `alarm` substructure
// `{ severity: int, ... }`, mapped to the bsrs/ophyd-async convention:
// 0/1/2 pass through (NO_ALARM/MINOR/MAJOR), 3+ (INVALID) -> -1
// (`_aioca._make_reading`: `-1 if severity > 2 else severity`). None when no
// `alarm` substructure is present (e.g. a server publishing a bare scalar).
fn pv_field_to_alarm_severity(p: &PvField) -> Option<i32> {
    let PvField::Structure(s) = p else {
        return None;
    };
    let alarm = s.fields.iter().find(|(n, _)| n == "alarm")?;
    let PvField::Structure(a) = &alarm.1 else {
        return None;
    };
    let sev = a
        .fields
        .iter()
        .find(|(n, _)| n == "severity")
        .and_then(|(_, f)| pv_field_to_i64(f))?;
    Some(if sev > 2 { -1 } else { sev as i32 })
}

// Borrow a structure's named field, if present.
fn field<'a>(fields: &'a [(String, PvField)], name: &str) -> Option<&'a PvField> {
    fields.iter().find(|(n, _)| n == name).map(|(_, f)| f)
}

// Borrow a named substructure's field list.
fn sub_fields<'a>(fields: &'a [(String, PvField)], name: &str) -> Option<&'a [(String, PvField)]> {
    match field(fields, name) {
        Some(PvField::Structure(s)) => Some(&s.fields),
        _ => None,
    }
}

// A numeric leaf field as f64, treating NaN (PVA's "unset" fill) as absent.
fn leaf_f64(fields: &[(String, PvField)], name: &str) -> Option<f64> {
    field(fields, name)
        .and_then(pv_field_to_f64)
        .filter(|v| !v.is_nan())
}

// Build a LimitsRange from a low/high pair, dropping it when both bounds are
// absent (NaN) or both exactly zero (an unset PVA limit pair) — ophyd-async
// `_limits_from_value.get_limits` (epics/core/_p4p.py:49-53).
fn limits_range(low: Option<f64>, high: Option<f64>) -> Option<LimitsRange> {
    if low.is_none() && high.is_none() {
        return None;
    }
    if low.unwrap_or(f64::NAN) == 0.0 && high.unwrap_or(f64::NAN) == 0.0 {
        return None;
    }
    Some(LimitsRange { low, high })
}

// Extract the four limit ranges from an NTScalar's top-level substructures,
// mirroring ophyd-async `_limits_from_value` (epics/core/_p4p.py:42-65): alarm
// and warning from `valueAlarm`, control and display from their own
// `limitLow`/`limitHigh`.
fn pv_limits(top: &[(String, PvField)]) -> Limits {
    let mut limits = Limits::default();
    if let Some(va) = sub_fields(top, "valueAlarm") {
        limits.alarm = limits_range(
            leaf_f64(va, "lowAlarmLimit"),
            leaf_f64(va, "highAlarmLimit"),
        );
        limits.warning = limits_range(
            leaf_f64(va, "lowWarningLimit"),
            leaf_f64(va, "highWarningLimit"),
        );
    }
    if let Some(c) = sub_fields(top, "control") {
        limits.control = limits_range(leaf_f64(c, "limitLow"), leaf_f64(c, "limitHigh"));
    }
    if let Some(d) = sub_fields(top, "display") {
        limits.display = limits_range(leaf_f64(d, "limitLow"), leaf_f64(d, "limitHigh"));
    }
    limits
}

/// What a backend reads its PV as: ophyd-async's `datatype`, named as its
/// `format_datatype` names it in a refusal. `pub` only for the sealed
/// [`PvaType`], as is [`PvaConverter`]; neither is reachable outside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Datatype {
    Float,
    Int,
    Bool,
    Str,
    /// The `Array1D` of this element type; `Sequence[str]` for
    /// [`ScalarType::String`].
    Array(ScalarType),
    /// `datatype=None`, read as a number: whatever numeric or bool scalar
    /// the PV is, an NTEnum as its index (ophyd `EpicsSignalRO`).
    Number,
    /// `np.ndarray`.
    NdArray,
    /// `Table` or the named subclass of it.
    Table(&'static str),
}

impl Datatype {
    fn name(self) -> &'static str {
        use ScalarType as S;
        match self {
            Self::Float | Self::Number => "float",
            Self::Int => "int",
            Self::Bool => "bool",
            Self::Str => "str",
            Self::Array(S::Boolean) => "Array1D[np.bool]",
            Self::Array(S::Byte) => "Array1D[np.int8]",
            Self::Array(S::UByte) => "Array1D[np.uint8]",
            Self::Array(S::Short) => "Array1D[np.int16]",
            Self::Array(S::UShort) => "Array1D[np.uint16]",
            Self::Array(S::Int) => "Array1D[np.int32]",
            Self::Array(S::UInt) => "Array1D[np.uint32]",
            Self::Array(S::Long) => "Array1D[np.int64]",
            Self::Array(S::ULong) => "Array1D[np.uint64]",
            Self::Array(S::Float) => "Array1D[np.float32]",
            Self::Array(S::Double) => "Array1D[np.float64]",
            Self::Array(S::String) => "Sequence[str]",
            Self::NdArray => "ndarray",
            Self::Table(name) => name,
        }
    }
}

/// How a backend's values come from its PV's `value`, as [`make_converter`]
/// picked it at connect.
#[derive(Debug, PartialEq)]
pub enum PvaConverter {
    /// The scalar or array itself (`PvaConverter`, `PvaIntConverter`,
    /// `PvaScalarBoolConverter`).
    Value,
    /// An NTEnum's `index` and `choices` (`PvaEnumConverter`,
    /// `PvaEnumBoolConverter`).
    Enum,
    /// A `str` on an int8 array, NUL-terminated (`PvaLongStringConverter`).
    LongStr,
    /// An NTNDArray's `value` shaped by its `dimension`
    /// (`PvaNDArrayConverter`).
    NdArray,
    /// An NTTable's `value` columns (`PvaTableConverter`).
    Table,
}

impl PvaConverter {
    /// The fields a monitor asks for: the converter's `value_fields`, then
    /// `alarm` and `timeStamp`.
    fn monitor_request(&self) -> &'static str {
        match self {
            Self::NdArray => "field(value,dimension,alarm,timeStamp)",
            _ => "field(value,alarm,timeStamp)",
        }
    }
}

const NT_SCALAR: &str = "epics:nt/NTScalar:1.0";
const NT_SCALAR_ARRAY: &str = "epics:nt/NTScalarArray:1.0";
const NT_ENUM: &str = "epics:nt/NTEnum:1.0";

/// A full read's fields; none for a bare value.
fn top_fields(top: &PvField) -> &[(String, PvField)] {
    match top {
        PvField::Structure(s) => &s.fields,
        _ => &[],
    }
}

/// The p4p type code of a `value` field: ophyd-async's `_get_specifier`.
fn specifier(value: Option<&PvField>) -> String {
    fn code(t: ScalarType) -> char {
        use ScalarType::*;
        match t {
            Boolean => '?',
            Byte => 'b',
            UByte => 'B',
            Short => 'h',
            UShort => 'H',
            Int => 'i',
            UInt => 'I',
            Long => 'l',
            ULong => 'L',
            Float => 'f',
            Double => 'd',
            String => 's',
        }
    }
    match value {
        Some(PvField::Scalar(x)) => code(x.scalar_type()).to_string(),
        Some(PvField::ScalarArrayTyped(a)) => format!("a{}", code(a.scalar_type())),
        // The legacy form, which no decoder makes, is typed by its elements.
        Some(PvField::ScalarArray(items)) => match items.first() {
            Some(x) => format!("a{}", code(x.scalar_type())),
            None => "a".into(),
        },
        Some(PvField::Structure(_)) => "S".into(),
        Some(PvField::StructureArray(_)) => "aS".into(),
        Some(PvField::Union { .. }) => "U".into(),
        Some(PvField::UnionArray(_)) => "aU".into(),
        Some(PvField::Variant(_)) => "v".into(),
        Some(PvField::VariantArray(_)) => "av".into(),
        Some(PvField::Null) | None => String::new(),
    }
}

/// The datatype ophyd-async infers from a PV's typeid and specifier
/// (`_datatype_converter_from_typeid`).
fn inferred(typeid: &str, spec: &str) -> Option<Datatype> {
    use Datatype as D;
    use ScalarType as S;
    Some(match (typeid, spec) {
        (NT_SCALAR, "?") => D::Bool,
        (NT_SCALAR, "b" | "B" | "h" | "H" | "i" | "I" | "l" | "L") => D::Int,
        (NT_SCALAR, "f" | "d") => D::Float,
        (NT_SCALAR, "s") | (NT_ENUM, "S") => D::Str,
        (NT_SCALAR_ARRAY, "a?") => D::Array(S::Boolean),
        (NT_SCALAR_ARRAY, "ab") => D::Array(S::Byte),
        (NT_SCALAR_ARRAY, "aB") => D::Array(S::UByte),
        (NT_SCALAR_ARRAY, "ah") => D::Array(S::Short),
        (NT_SCALAR_ARRAY, "aH") => D::Array(S::UShort),
        (NT_SCALAR_ARRAY, "ai") => D::Array(S::Int),
        (NT_SCALAR_ARRAY, "aI") => D::Array(S::UInt),
        (NT_SCALAR_ARRAY, "al") => D::Array(S::Long),
        (NT_SCALAR_ARRAY, "aL") => D::Array(S::ULong),
        (NT_SCALAR_ARRAY, "af") => D::Array(S::Float),
        (NT_SCALAR_ARRAY, "ad") => D::Array(S::Double),
        (NT_SCALAR_ARRAY, "as") => D::Array(S::String),
        ("epics:nt/NTTable:1.0", "S") => D::Table("Table"),
        ("epics:nt/NTNDArray:1.0", "v" | "U") => D::NdArray,
        _ => return None,
    })
}

/// ophyd-async `make_converter`: the converter that reads `pv`, whose full
/// read is `top`, as `want`, or the `TypeError` that refuses it.
fn make_converter(pv: &str, want: Datatype, top: &PvField) -> Result<PvaConverter> {
    use Datatype as D;
    let typeid = match top {
        PvField::Structure(s) => s.struct_id.as_str(),
        _ => "",
    };
    let spec = specifier(field(top_fields(top), "value"));
    let Some(inferred) = inferred(typeid, &spec) else {
        // ophyd-async's lookup raises a bare `KeyError` here.
        return Err(BsrsError::Datatype(format!(
            "{pv} has no datatype for typeid='{typeid}' specifier='{spec}'"
        )));
    };
    let is_enum = typeid == NT_ENUM;
    let conv = match want {
        // A database bool is often a two-choice enum.
        D::Bool if is_enum => {
            let n = sub_fields(top_fields(top), "value")
                .and_then(|e| field(e, "choices"))
                .and_then(pv_string_array)
                .map_or(0, |c| c.len());
            if n != 2 {
                return Err(BsrsError::Datatype(format!(
                    "{pv} has {n} choices, can't map to bool"
                )));
            }
            Some(PvaConverter::Enum)
        }
        D::Bool if typeid == NT_SCALAR && matches!(spec.as_str(), "?" | "b" | "B" | "i" | "I") => {
            Some(PvaConverter::Value)
        }
        _ if is_enum => matches!(want, D::Str | D::Number).then_some(PvaConverter::Enum),
        // An int on a float record whose precision shows no fraction.
        D::Int
            if inferred == D::Float
                && sub_fields(top_fields(top), "display")
                    .and_then(|d| field(d, "precision"))
                    .and_then(pv_field_to_i64)
                    == Some(0) =>
        {
            Some(PvaConverter::Value)
        }
        D::Number if matches!(inferred, D::Float | D::Int | D::Bool) => Some(PvaConverter::Value),
        D::NdArray if inferred == D::NdArray => Some(PvaConverter::NdArray),
        D::Table(_) if matches!(inferred, D::Table(_)) => Some(PvaConverter::Table),
        _ if inferred == want => Some(PvaConverter::Value),
        D::Str if inferred == D::Array(ScalarType::Byte) => Some(PvaConverter::LongStr),
        _ => None,
    };
    conv.ok_or_else(|| {
        BsrsError::Datatype(format!(
            "{pv} with inferred datatype {} from typeid='{typeid}' \
             specifier='{spec}' cannot be coerced to {}",
            inferred.name(),
            want.name()
        ))
    })
}

// The element type of a `value` leaf: ophyd-async's specifier.
fn scalar_type(v: &PvField) -> Option<ScalarType> {
    match v {
        PvField::Scalar(x) => Some(x.scalar_type()),
        PvField::ScalarArrayTyped(a) => Some(a.scalar_type()),
        PvField::ScalarArray(items) => items.first().map(ScalarValue::scalar_type),
        _ => None,
    }
}

// Build SignalMetadata from a full GET as ophyd-async `_metadata_from_value`
// (epics/core/_p4p.py:69-95): units and limits of a numeric value, units not
// when read as str, precision of a float value not read as int, and choices
// from `display` or the `enum_t` value when read as str.
fn pv_field_to_metadata(p: &PvField, datatype: Datatype) -> SignalMetadata {
    use ScalarType::*;
    let mut meta = SignalMetadata::default();
    let PvField::Structure(s) = p else {
        return meta;
    };
    let top = &s.fields;
    let spec = field(top, "value").and_then(scalar_type);
    let float = matches!(spec, Some(Float | Double));
    let number = float
        || matches!(
            spec,
            Some(Byte | Short | Int | Long | UByte | UShort | UInt | ULong)
        );
    let display = sub_fields(top, "display");
    if let Some(display) = display {
        if number && datatype != Datatype::Str {
            if let Some(PvField::Scalar(ScalarValue::String(u))) = field(display, "units") {
                meta.units = Some(u.as_str_lossy().into_owned());
            }
        }
        if float && datatype != Datatype::Int {
            if let Some(prec) = field(display, "precision").and_then(pv_field_to_i64) {
                meta.precision = Some(prec);
            }
        }
    }
    let limits = pv_limits(top);
    if number && limits != Limits::default() {
        meta.limits = Some(limits);
    }
    if datatype == Datatype::Str {
        meta.choices = display
            .and_then(|d| field(d, "choices"))
            .or_else(|| sub_fields(top, "value").and_then(|v| field(v, "choices")))
            .and_then(pv_string_array);
    }
    meta
}

/// A datatype ophyd-async's `make_datakey` gives shape `[]`, with its
/// `dtype` and `dtype_numpy` there (`core/_signal_backend.py:128-195`).
trait Primitive {
    const DTYPE: Dtype;
    const DTYPE_NUMPY: &'static str;
}

impl Primitive for f64 {
    const DTYPE: Dtype = Dtype::Number;
    const DTYPE_NUMPY: &'static str = "<f8";
}

impl Primitive for i64 {
    const DTYPE: Dtype = Dtype::Integer;
    const DTYPE_NUMPY: &'static str = "<i8";
}

impl Primitive for bool {
    const DTYPE: Dtype = Dtype::Boolean;
    const DTYPE_NUMPY: &'static str = "|b1";
}

/// Enum labels included: ophyd-async describes both as `S40`.
impl Primitive for String {
    const DTYPE: Dtype = Dtype::String;
    const DTYPE_NUMPY: &'static str = "|S40";
}

fn primitive_datakey<T: Primitive>(source: String, meta: SignalMetadata) -> DataKey {
    make_datakey(source, T::DTYPE, vec![], Some(T::DTYPE_NUMPY.into()), meta)
}

/// An array's datakey: shaped as the `len` elements read.
fn array_datakey(source: String, len: usize, dtype_numpy: &str, meta: SignalMetadata) -> DataKey {
    let shape = vec![Some(len as u64)];
    make_datakey(source, Dtype::Array, shape, Some(dtype_numpy.into()), meta)
}

mod sealed {
    use super::{Datatype, PvaConverter};
    use crate::core::error::Result;
    use crate::event_model::DataKey;
    use epics_pva_rs::pvdata::{ScalarType, TypedScalarArray};
    use epics_pva_rs::{PvField, ScalarValue};

    /// A type [`EpicsPvaBackend`](super::EpicsPvaBackend) reads a PV as: its
    /// ophyd-async datatype, and its values through the converter
    /// [`make_converter`](super::make_converter) picked for it.
    pub trait PvaType: Into<serde_json::Value> + Clone + Send + Sync + 'static {
        #[doc(hidden)]
        const DATATYPE: Datatype;
        /// `None` when `top` is not a read of the PV `conv` was made for.
        #[doc(hidden)]
        fn decode(conv: &PvaConverter, top: &PvField) -> Option<Self>;
        /// The `TypeError` of a type the PV cannot be written as.
        #[doc(hidden)]
        fn encode(conv: &PvaConverter, v: Self) -> Result<PvField>;
        /// ophyd-async `get_datakey` from a full read `top`.
        #[doc(hidden)]
        fn datakey(source: String, conv: &PvaConverter, top: &PvField) -> Result<DataKey>;
    }

    /// An element of the `Vec` [`EpicsPvaBackend`](super::EpicsPvaBackend)
    /// reads an NTScalarArray of its type as: ophyd-async's `Array1D`
    /// dtype, or for `String` its `Sequence[str]`.
    pub trait PvaElement: Into<serde_json::Value> + Clone + Send + Sync + 'static {
        #[doc(hidden)]
        const SCALAR_TYPE: ScalarType;
        #[doc(hidden)]
        const DTYPE_NUMPY: &'static str;
        /// `None` when `a` is not an array of `SCALAR_TYPE`.
        #[doc(hidden)]
        fn from_array(a: &TypedScalarArray) -> Option<Vec<Self>>;
        /// An element of the legacy untyped array.
        #[doc(hidden)]
        fn from_scalar(x: &ScalarValue) -> Option<Self>;
        #[doc(hidden)]
        fn to_array(v: Vec<Self>) -> TypedScalarArray;
    }
}
use sealed::{PvaElement, PvaType};

/// An NTEnum's `value.index`.
fn enum_index(top: &PvField) -> Option<i64> {
    sub_fields(top_fields(top), "value")
        .and_then(|e| field(e, "index"))
        .and_then(pv_field_to_i64)
}

/// An int8 array's text, its trailing NULs stripped, as
/// `PvaLongStringConverter` reads it.
fn long_string(chars: &[i8]) -> String {
    let bytes: Vec<u8> = chars.iter().map(|c| *c as u8).collect();
    let end = bytes.iter().rposition(|b| *b != 0).map_or(0, |i| i + 1);
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

impl PvaType for f64 {
    const DATATYPE: Datatype = Datatype::Float;
    fn decode(conv: &PvaConverter, top: &PvField) -> Option<Self> {
        match (conv, field(top_fields(top), "value")?) {
            (PvaConverter::Enum, _) => enum_index(top).map(|i| i as f64),
            (_, PvField::Scalar(ScalarValue::Boolean(b))) => Some(f64::from(u8::from(*b))),
            (_, PvField::Scalar(s)) => scalar_to_f64(s),
            _ => None,
        }
    }
    fn encode(_: &PvaConverter, v: Self) -> Result<PvField> {
        Ok(PvField::Scalar(ScalarValue::Double(v)))
    }
    fn datakey(source: String, _: &PvaConverter, top: &PvField) -> Result<DataKey> {
        let meta = pv_field_to_metadata(top, Datatype::Float);
        Ok(primitive_datakey::<f64>(source, meta))
    }
}

impl PvaType for i64 {
    const DATATYPE: Datatype = Datatype::Int;
    fn decode(_: &PvaConverter, top: &PvField) -> Option<Self> {
        field(top_fields(top), "value").and_then(pv_field_to_i64)
    }
    fn encode(_: &PvaConverter, v: Self) -> Result<PvField> {
        Ok(PvField::Scalar(ScalarValue::Long(v)))
    }
    fn datakey(source: String, _: &PvaConverter, top: &PvField) -> Result<DataKey> {
        let meta = pv_field_to_metadata(top, Datatype::Int);
        Ok(primitive_datakey::<i64>(source, meta))
    }
}

impl PvaType for bool {
    const DATATYPE: Datatype = Datatype::Bool;
    fn decode(conv: &PvaConverter, top: &PvField) -> Option<Self> {
        match conv {
            PvaConverter::Enum => enum_index(top),
            _ => field(top_fields(top), "value").and_then(pv_field_to_i64),
        }
        .map(|i| i != 0)
    }
    fn encode(_: &PvaConverter, v: Self) -> Result<PvField> {
        Ok(PvField::Scalar(ScalarValue::Boolean(v)))
    }
    fn datakey(source: String, _: &PvaConverter, top: &PvField) -> Result<DataKey> {
        let meta = pv_field_to_metadata(top, Datatype::Bool);
        Ok(primitive_datakey::<bool>(source, meta))
    }
}

impl PvaType for String {
    const DATATYPE: Datatype = Datatype::Str;
    fn decode(conv: &PvaConverter, top: &PvField) -> Option<Self> {
        match (conv, field(top_fields(top), "value")?) {
            (PvaConverter::Value, PvField::Scalar(ScalarValue::String(s))) => {
                Some(s.as_str_lossy().into_owned())
            }
            (PvaConverter::Enum, PvField::Structure(e)) => pv_enum_to_label(&e.fields),
            (PvaConverter::LongStr, PvField::ScalarArrayTyped(TypedScalarArray::Byte(a))) => {
                Some(long_string(a))
            }
            _ => None,
        }
    }
    fn encode(conv: &PvaConverter, v: Self) -> Result<PvField> {
        Ok(match conv {
            // NUL-terminated, as `PvaLongStringConverter` writes it.
            PvaConverter::LongStr => {
                let mut chars: Vec<i8> = v.bytes().map(|b| b as i8).collect();
                chars.push(0);
                PvField::ScalarArrayTyped(TypedScalarArray::Byte(chars.into()))
            }
            _ => PvField::Scalar(ScalarValue::String(v.into())),
        })
    }
    fn datakey(source: String, _: &PvaConverter, top: &PvField) -> Result<DataKey> {
        let meta = pv_field_to_metadata(top, Datatype::Str);
        Ok(primitive_datakey::<String>(source, meta))
    }
}

impl<E: PvaElement> PvaType for Vec<E> {
    const DATATYPE: Datatype = Datatype::Array(E::SCALAR_TYPE);
    fn decode(_: &PvaConverter, top: &PvField) -> Option<Self> {
        match field(top_fields(top), "value")? {
            PvField::ScalarArrayTyped(a) => E::from_array(a),
            PvField::ScalarArray(items) => items.iter().map(E::from_scalar).collect(),
            _ => None,
        }
    }
    fn encode(_: &PvaConverter, v: Self) -> Result<PvField> {
        Ok(PvField::ScalarArrayTyped(E::to_array(v)))
    }
    fn datakey(source: String, conv: &PvaConverter, top: &PvField) -> Result<DataKey> {
        let v = Self::decode(conv, top).ok_or_else(|| undecodable(&source, top))?;
        let meta = pv_field_to_metadata(top, Self::DATATYPE);
        Ok(array_datakey(source, v.len(), E::DTYPE_NUMPY, meta))
    }
}

/// `$t` as the element of an NTScalarArray of `$v`.
macro_rules! pva_element {
    ($t:ty, $v:ident, $numpy:literal) => {
        impl PvaElement for $t {
            const SCALAR_TYPE: ScalarType = ScalarType::$v;
            const DTYPE_NUMPY: &'static str = $numpy;
            fn from_array(a: &TypedScalarArray) -> Option<Vec<Self>> {
                match a {
                    TypedScalarArray::$v(a) => Some(a.to_vec()),
                    _ => None,
                }
            }
            fn from_scalar(x: &ScalarValue) -> Option<Self> {
                match x {
                    ScalarValue::$v(x) => Some(*x),
                    _ => None,
                }
            }
            fn to_array(v: Vec<Self>) -> TypedScalarArray {
                TypedScalarArray::$v(v.into())
            }
        }
    };
}

pva_element!(bool, Boolean, "|b1");
pva_element!(i8, Byte, "|i1");
pva_element!(u8, UByte, "|u1");
pva_element!(i16, Short, "<i2");
pva_element!(u16, UShort, "<u2");
pva_element!(i32, Int, "<i4");
pva_element!(u32, UInt, "<u4");
pva_element!(i64, Long, "<i8");
pva_element!(u64, ULong, "<u8");
pva_element!(f32, Float, "<f4");
pva_element!(f64, Double, "<f8");

/// `Sequence[str]`.
impl PvaElement for String {
    const SCALAR_TYPE: ScalarType = ScalarType::String;
    const DTYPE_NUMPY: &'static str = "|S40";
    fn from_array(a: &TypedScalarArray) -> Option<Vec<Self>> {
        match a {
            TypedScalarArray::String(a) => {
                Some(a.iter().map(|s| s.as_str_lossy().into_owned()).collect())
            }
            _ => None,
        }
    }
    fn from_scalar(x: &ScalarValue) -> Option<Self> {
        match x {
            ScalarValue::String(s) => Some(s.as_str_lossy().into_owned()),
            _ => None,
        }
    }
    fn to_array(v: Vec<Self>) -> TypedScalarArray {
        TypedScalarArray::String(v.into_iter().map(Into::into).collect())
    }
}

/// The elements of an NTNDArray's `value`, an `any` or a union of arrays;
/// none for a string array, which numpy holds as no numeric dtype.
fn nd_data(value: &PvField) -> Option<NdData> {
    let a = match value {
        PvField::Variant(v) => &v.value,
        PvField::Union { value, .. } => value,
        _ => return None,
    };
    let PvField::ScalarArrayTyped(a) = a else {
        return None;
    };
    typed_nd_data(a)
}

/// A numeric or bool array's elements; none for a string array.
fn typed_nd_data(a: &TypedScalarArray) -> Option<NdData> {
    Some(match a {
        TypedScalarArray::Boolean(a) => a.to_vec().into(),
        TypedScalarArray::Byte(a) => a.to_vec().into(),
        TypedScalarArray::UByte(a) => a.to_vec().into(),
        TypedScalarArray::Short(a) => a.to_vec().into(),
        TypedScalarArray::UShort(a) => a.to_vec().into(),
        TypedScalarArray::Int(a) => a.to_vec().into(),
        TypedScalarArray::UInt(a) => a.to_vec().into(),
        TypedScalarArray::Long(a) => a.to_vec().into(),
        TypedScalarArray::ULong(a) => a.to_vec().into(),
        TypedScalarArray::Float(a) => a.to_vec().into(),
        TypedScalarArray::Double(a) => a.to_vec().into(),
        TypedScalarArray::String(_) => return None,
    })
}

/// `np.ndarray`: `value` reshaped to the `dimension` sizes, reversed from
/// NTNDArray's first-fastest order to C order.
impl PvaType for NdArray {
    const DATATYPE: Datatype = Datatype::NdArray;
    fn decode(_: &PvaConverter, top: &PvField) -> Option<Self> {
        let fields = top_fields(top);
        let PvField::StructureArray(dims) = field(fields, "dimension")? else {
            return None;
        };
        let mut shape = dims
            .iter()
            .map(|d| {
                let size = field(&d.as_ref()?.fields, "size").and_then(pv_field_to_i64)?;
                usize::try_from(size).ok()
            })
            .collect::<Option<Vec<_>>>()?;
        shape.reverse();
        NdArray::new(shape, nd_data(field(fields, "value")?)?).ok()
    }
    fn encode(_: &PvaConverter, _: Self) -> Result<PvField> {
        Err(BsrsError::Datatype(
            "Writing to NDArray not supported".into(),
        ))
    }
    fn datakey(source: String, conv: &PvaConverter, top: &PvField) -> Result<DataKey> {
        let v = Self::decode(conv, top).ok_or_else(|| undecodable(&source, top))?;
        let shape = v.shape().iter().map(|&n| Some(n as u64)).collect();
        let dtype_numpy = Some(v.data().dtype_numpy().into());
        let meta = pv_field_to_metadata(top, Self::DATATYPE);
        Ok(make_datakey(source, Dtype::Array, shape, dtype_numpy, meta))
    }
}

/// An NTTable's `value` columns, in order.
fn table(top: &PvField) -> Option<Table> {
    let columns = sub_fields(top_fields(top), "value")?
        .iter()
        .map(|(name, c)| {
            let PvField::ScalarArrayTyped(a) = c else {
                return None;
            };
            let column = match a {
                TypedScalarArray::String(_) => Column::Str(String::from_array(a)?),
                _ => Column::Array(typed_nd_data(a)?),
            };
            Some((name.as_str(), column))
        })
        .collect::<Option<Vec<_>>>()?;
    Table::new(columns).ok()
}

/// `t` as an NTTable's `value`, each column an array the put coerces to
/// the PV's element type.
fn table_value(t: Table) -> PvField {
    let mut value = PvStructure::new("");
    value.fields = t
        .into_columns()
        .into_iter()
        .map(|(name, c)| {
            let a = match c {
                Column::Str(v) => String::to_array(v),
                Column::Array(NdData::Bool(v)) => bool::to_array(v),
                Column::Array(NdData::I8(v)) => i8::to_array(v),
                Column::Array(NdData::U8(v)) => u8::to_array(v),
                Column::Array(NdData::I16(v)) => i16::to_array(v),
                Column::Array(NdData::U16(v)) => u16::to_array(v),
                Column::Array(NdData::I32(v)) => i32::to_array(v),
                Column::Array(NdData::U32(v)) => u32::to_array(v),
                Column::Array(NdData::I64(v)) => i64::to_array(v),
                Column::Array(NdData::U64(v)) => u64::to_array(v),
                Column::Array(NdData::F32(v)) => f32::to_array(v),
                Column::Array(NdData::F64(v)) => f64::to_array(v),
            };
            (name, PvField::ScalarArrayTyped(a))
        })
        .collect();
    PvField::Structure(value)
}

/// `Table` or a subclass: the NTTable's columns, cast to the subclass's as
/// its validation casts them.
impl<T: TypedTable> PvaType for T {
    const DATATYPE: Datatype = Datatype::Table(T::NAME);
    fn decode(_: &PvaConverter, top: &PvField) -> Option<Self> {
        T::from_table(table(top)?).ok()
    }
    fn encode(_: &PvaConverter, v: Self) -> Result<PvField> {
        Ok(table_value(v.into_table()?))
    }
    fn datakey(source: String, conv: &PvaConverter, top: &PvField) -> Result<DataKey> {
        let t = Self::decode(conv, top)
            .ok_or_else(|| undecodable(&source, top))?
            .into_table()?;
        let shape = vec![Some(t.len() as u64)];
        let meta = pv_field_to_metadata(top, Self::DATATYPE);
        Ok(make_datakey(
            source,
            Dtype::Array,
            shape,
            Some(t.dtype_numpy()),
            meta,
        ))
    }
}

/// Feed `cb` each update of `pv`'s monitor that `decode` accepts. Only
/// `conv`'s fields are asked for; a server that publishes no `timeStamp` is
/// stamped by the local clock, warned of once.
async fn forward_monitor<T>(
    client: &PvaClient,
    pv: &str,
    conv: &PvaConverter,
    decode: impl Fn(&PvField) -> Option<T> + Send,
    cb: ReadingValueCallback<T>,
) {
    let request = PvRequestExpr::parse(conv.monitor_request()).unwrap_or_default();
    let mut warned = false;
    let res = client
        .pvmonitor_with_request(pv, &request, move |f: &PvField| {
            let Some(v) = decode(f) else {
                return;
            };
            let ts = pv_field_to_ts(f).unwrap_or_else(|| {
                if !std::mem::replace(&mut warned, true) {
                    tracing::warn!(
                        target: "bsrs_backend_epics_pva",
                        "pva {pv}: monitor frame has no server timeStamp; \
                         falling back to local clock for this PV (one-shot)",
                    );
                }
                now_ts()
            });
            cb(&v, ts, pv_field_to_alarm_severity(f));
        })
        .await;
    if let Err(e) = res {
        tracing::error!("pva monitor on {pv}: {e}");
    }
}

#[async_trait]
impl<T: PvaType> SignalBackend<T> for EpicsPvaBackend<T> {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.connect_reading(timeout).await
    }
    async fn put_start(&self, value: Option<T>) -> Result<PutCompletion> {
        self.put_value(value).await
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        // The full read carries display/control/valueAlarm: units,
        // precision and limits.
        let (conv, top) = self.read().await?;
        T::datakey(format!("pva://{source}"), &conv, &top)
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let (conv, top) = self.read().await?;
        let v = T::decode(&conv, &top).ok_or_else(|| self.undecodable(&top))?;
        Ok(ReadingValue {
            value: v.into(),
            timestamp: pv_field_to_ts(&top).unwrap_or_else(now_ts),
            alarm_severity: pv_field_to_alarm_severity(&top),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<T> {
        self.value().await
    }
    async fn get_setpoint(&self) -> Result<T> {
        self.value().await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<T>>) -> SubToken {
        let Some(cb) = cb else {
            return SubToken::noop();
        };
        let client = self.client.clone();
        let cached = self.converter.lock().unwrap().clone();
        let (pv, want) = (self.pv.clone(), self.want);
        let handle = tokio::spawn(async move {
            let conv = match cached {
                Some(conv) => conv,
                None => {
                    // No connect made the converter: wait for the channel as
                    // long as the token lives, as the monitor itself would.
                    if pva_connect(&client, &pv, Duration::MAX).await.is_err() {
                        return;
                    }
                    let made = pvget(&client, &pv)
                        .await
                        .and_then(|top| make_converter(&pv, want, &top));
                    match made {
                        Ok(conv) => Arc::new(conv),
                        Err(e) => {
                            tracing::warn!("pva monitor {pv}: {e}");
                            return;
                        }
                    }
                }
            };
            forward_monitor(&client, &pv, &conv, |f| T::decode(&conv, f), cb).await;
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str, _read: bool) -> String {
        format!("pva://{name}")
    }
}

/// [`Command`](crate::devices::Command) backend over PVA: ophyd-async
/// `PvaCommandBackend`. Executing puts `execute_value` to the PV's `value`
/// with `record[block=true]`, so a `.PROC` field's command completes when the
/// record has processed.
pub struct PvaCommandBackend {
    pv: String,
    execute_value: i64,
    client: Arc<PvaClient>,
}

impl PvaCommandBackend {
    /// Put 1 to `write_pv` on each execute.
    pub fn new(write_pv: impl Into<String>) -> Self {
        Self {
            pv: write_pv.into(),
            execute_value: 1,
            client: pva_context(),
        }
    }

    /// Put `execute_value` instead of 1.
    pub fn with_execute_value(mut self, execute_value: i64) -> Self {
        self.execute_value = execute_value;
        self
    }

    /// The PV written.
    pub fn write_pv(&self) -> &str {
        &self.pv
    }
}

impl crate::devices::BackendFromPv for PvaCommandBackend {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
}

#[async_trait]
impl crate::devices::CommandBackend for PvaCommandBackend {
    type Args = ();
    type Output = ();
    fn source(&self, _name: &str) -> String {
        format!("pva://{}", self.pv)
    }
    /// Refuses all but a boolean or integer NTScalar and an NTEnum.
    async fn connect(&self, timeout: Duration) -> Result<()> {
        let top = connect_read(&self.client, &self.pv, timeout).await?;
        let typeid = match &top {
            PvField::Structure(s) => s.struct_id.as_str(),
            _ => "",
        };
        let spec = specifier(field(top_fields(&top), "value"));
        let accepted = match typeid {
            NT_SCALAR => matches!(
                spec.as_str(),
                "?" | "b" | "B" | "h" | "H" | "i" | "I" | "l" | "L"
            ),
            NT_ENUM => true,
            _ => false,
        };
        if !accepted {
            return Err(BsrsError::Datatype(format!(
                "pva://{} is not a scalar boolean, scalar numeric, or enum PV \
                 (typeid='{typeid}', specifier='{spec}'); PvaCommandBackend requires a \
                 scalar boolean, scalar numeric, or enum PV",
                self.pv
            )));
        }
        Ok(())
    }
    async fn execute(&self, _args: ()) -> Result<()> {
        let v = PvField::Scalar(ScalarValue::Long(self.execute_value));
        put_block(&self.client, &self.pv, v, true).await?.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use epics_pva_rs::pvdata::{PvStructure, VariantValue};

    fn ntscalar_with_ts(value: f64, secs: i64, nanos: i32) -> PvField {
        let mut ts = PvStructure::new("time_t");
        ts.fields.push((
            "secondsPastEpoch".into(),
            PvField::Scalar(ScalarValue::Long(secs)),
        ));
        ts.fields.push((
            "nanoseconds".into(),
            PvField::Scalar(ScalarValue::Int(nanos)),
        ));
        let mut nt = PvStructure::new("epics:nt/NTScalar:1.0");
        nt.fields
            .push(("value".into(), PvField::Scalar(ScalarValue::Double(value))));
        nt.fields.push(("timeStamp".into(), PvField::Structure(ts)));
        PvField::Structure(nt)
    }

    // Build an NTEnum `{ value: { index, choices }, ... }`. `typed` selects
    // the real-wire `ScalarArrayTyped(String)` representation vs the legacy
    // `ScalarArray(Vec<ScalarValue::String>)` form.
    fn ntenum(index: i32, choices: &[&str], typed: bool) -> PvField {
        let choices_field = if typed {
            PvField::ScalarArrayTyped(epics_pva_rs::pvdata::TypedScalarArray::String(
                choices
                    .iter()
                    .map(|s| s.to_string().into())
                    .collect::<Vec<_>>()
                    .into(),
            ))
        } else {
            PvField::ScalarArray(
                choices
                    .iter()
                    .map(|s| ScalarValue::String(s.to_string().into()))
                    .collect(),
            )
        };
        let mut ev = PvStructure::new("enum_t");
        ev.fields
            .push(("index".into(), PvField::Scalar(ScalarValue::Int(index))));
        ev.fields.push(("choices".into(), choices_field));
        let mut nt = PvStructure::new("epics:nt/NTEnum:1.0");
        nt.fields.push(("value".into(), PvField::Structure(ev)));
        PvField::Structure(nt)
    }

    fn nt(value: FieldDesc) -> FieldDesc {
        FieldDesc::Structure {
            struct_id: "epics:nt/NTScalar:1.0".into(),
            fields: vec![("value".to_string(), value)].into(),
        }
    }

    fn enum_t() -> FieldDesc {
        FieldDesc::Structure {
            struct_id: "enum_t".into(),
            fields: vec![
                ("index".to_string(), FieldDesc::Scalar(ScalarType::Int)),
                (
                    "choices".to_string(),
                    FieldDesc::ScalarArray(ScalarType::String),
                ),
            ]
            .into(),
        }
    }

    fn string(s: &str) -> PvField {
        PvField::Scalar(ScalarValue::String(s.into()))
    }

    fn at(path: &str, f: PvField) -> (String, PvField) {
        (path.to_string(), f)
    }

    /// A put into an `enum_t` sets its index: a label by its place in the
    /// readback's choices, anything else as the value the index coerces.
    #[test]
    fn put_leaf_assigns_an_enum_through_its_index() {
        let present = ntenum(0, &["Single", "Multiple", "Continuous"], true);
        let to = |f| put_leaf(&nt(enum_t()), Some(&present), f);
        assert_eq!(
            to(string("Continuous")),
            at("value.index", PvField::Scalar(ScalarValue::Int(2)))
        );
        assert_eq!(to(string("1")), at("value.index", string("1")));
        let on = PvField::Scalar(ScalarValue::Boolean(true));
        assert_eq!(to(on.clone()), at("value.index", on));
        assert_eq!(
            put_leaf(&nt(enum_t()), None, string("Multiple")),
            at("value.index", string("Multiple"))
        );
    }

    #[test]
    fn put_leaf_assigns_value_or_a_bare_root() {
        let x = PvField::Scalar(ScalarValue::Double(1.5));
        let scalar = nt(FieldDesc::Scalar(ScalarType::Double));
        assert_eq!(put_leaf(&scalar, None, x.clone()), at("value", x.clone()));
        let bare = FieldDesc::Scalar(ScalarType::Double);
        assert_eq!(put_leaf(&bare, None, x.clone()), at("", x));
    }

    #[test]
    fn nt_enum_decodes_typed_choices_to_label() {
        // Real-wire shape: choices arrive as ScalarArrayTyped(String).
        let f = ntenum(1, &["OFF", "ON"], true);
        assert_eq!(String::decode(&PvaConverter::Enum, &f), Some("ON".into()));
    }

    #[test]
    fn nt_enum_out_of_range_index_falls_back_to_decimal() {
        // Legacy ScalarArray choices + index past the end → decimal fallback.
        let f = ntenum(5, &["OFF"], false);
        assert_eq!(String::decode(&PvaConverter::Enum, &f), Some("5".into()));
        let f = ntscalar(ScalarValue::String("hi".into()));
        assert_eq!(String::decode(&PvaConverter::Value, &f), Some("hi".into()));
    }

    // Build an NTScalar `{ value, alarm: { severity, status } }`.
    fn ntscalar_with_alarm(value: f64, severity: i32) -> PvField {
        let mut alarm = PvStructure::new("alarm_t");
        alarm.fields.push((
            "severity".into(),
            PvField::Scalar(ScalarValue::Int(severity)),
        ));
        alarm
            .fields
            .push(("status".into(), PvField::Scalar(ScalarValue::Int(0))));
        let mut nt = PvStructure::new("epics:nt/NTScalar:1.0");
        nt.fields
            .push(("value".into(), PvField::Scalar(ScalarValue::Double(value))));
        nt.fields.push(("alarm".into(), PvField::Structure(alarm)));
        PvField::Structure(nt)
    }

    #[test]
    fn alarm_severity_decoded_from_ntscalar_alarm() {
        // MINOR(1) and MAJOR(2) pass through.
        assert_eq!(
            pv_field_to_alarm_severity(&ntscalar_with_alarm(1.0, 1)),
            Some(1)
        );
        assert_eq!(
            pv_field_to_alarm_severity(&ntscalar_with_alarm(1.0, 2)),
            Some(2)
        );
        // NO_ALARM(0) is still Some(0) — ophyd-async always reports it.
        assert_eq!(
            pv_field_to_alarm_severity(&ntscalar_with_alarm(1.0, 0)),
            Some(0)
        );
        // INVALID(3) and any higher code collapse to -1
        // (`_aioca`: `-1 if severity > 2 else severity`).
        assert_eq!(
            pv_field_to_alarm_severity(&ntscalar_with_alarm(1.0, 3)),
            Some(-1)
        );
        assert_eq!(
            pv_field_to_alarm_severity(&ntscalar_with_alarm(1.0, 4)),
            Some(-1)
        );
    }

    #[test]
    fn alarm_severity_none_when_no_alarm_substructure() {
        // Bare scalar and an NTScalar lacking `alarm` both yield None.
        assert!(pv_field_to_alarm_severity(&PvField::Scalar(ScalarValue::Double(1.0))).is_none());
        let f = ntscalar_with_ts(42.0, 1, 0); // has timeStamp but no alarm
        assert!(pv_field_to_alarm_severity(&f).is_none());
    }

    // Build an NTScalarArray `{ value: <array> }`. `typed` selects the
    // real-wire `ScalarArrayTyped(Double)` vs the legacy `ScalarArray`.
    fn ntscalararray_doubles(values: &[f64], typed: bool) -> PvField {
        let value = if typed {
            PvField::ScalarArrayTyped(TypedScalarArray::Double(values.to_vec().into()))
        } else {
            PvField::ScalarArray(values.iter().map(|x| ScalarValue::Double(*x)).collect())
        };
        let mut nt = PvStructure::new("epics:nt/NTScalarArray:1.0");
        nt.fields.push(("value".into(), value));
        PvField::Structure(nt)
    }

    #[test]
    fn ntscalararray_decodes_typed_and_legacy_doubles() {
        let decode = |f| Vec::<f64>::decode(&PvaConverter::Value, &f);
        let typed = ntscalararray_doubles(&[1.0, 2.5, -3.0], true);
        assert_eq!(decode(typed), Some(vec![1.0, 2.5, -3.0]));
        let legacy = ntscalararray_doubles(&[4.0, 5.0], false);
        assert_eq!(decode(legacy), Some(vec![4.0, 5.0]));
        assert_eq!(decode(ntscalar_with_ts(1.0, 0, 0)), None);
    }

    fn ntscalararray(value: PvField) -> PvField {
        let mut nt = PvStructure::new(NT_SCALAR_ARRAY);
        nt.fields.push(("value".into(), value));
        PvField::Structure(nt)
    }

    /// Each element type is the datatype ophyd-async infers from its own
    /// NTScalarArray, and reads back what it writes.
    #[test]
    fn arrays_round_trip_their_own_scalar_array() {
        fn round_trip<E: PvaElement + PartialEq + std::fmt::Debug>(v: Vec<E>) {
            let top = ntscalararray(Vec::<E>::encode(&PvaConverter::Value, v.clone()).unwrap());
            let conv = make_converter("WF", Vec::<E>::DATATYPE, &top).unwrap();
            assert_eq!(Vec::<E>::decode(&conv, &top), Some(v));
        }
        round_trip(vec![true, false]);
        round_trip(vec![-1i8, 2]);
        round_trip(vec![1u8, 255]);
        round_trip(vec![-1i16, 2]);
        round_trip(vec![1u16, 65535]);
        round_trip(vec![-1i32, 2]);
        round_trip(vec![1u32, u32::MAX]);
        round_trip(vec![-1i64, 2]);
        round_trip(vec![1u64, u64::MAX]);
        round_trip(vec![1.5f32, -2.0]);
        round_trip(vec![1.5f64, -2.0]);
        round_trip(vec!["a".to_string(), String::new()]);
        round_trip(Vec::<i32>::new());
    }

    /// An array is its own element type only, as ophyd-async infers
    /// `Array1D[np.int32]` from an int one and coerces it to nothing else.
    #[test]
    fn array_refuses_another_element_type() {
        let ints = ntscalararray(PvField::ScalarArrayTyped(TypedScalarArray::Int(
            vec![7, 8, 9].into(),
        )));
        let err = |want: Datatype| make_converter("WF", want, &ints).unwrap_err().to_string();
        assert_eq!(
            err(Vec::<f64>::DATATYPE),
            "WF with inferred datatype Array1D[np.int32] from \
             typeid='epics:nt/NTScalarArray:1.0' specifier='ai' cannot be \
             coerced to Array1D[np.float64]"
        );
        assert_eq!(
            err(Vec::<i64>::DATATYPE),
            "WF with inferred datatype Array1D[np.int32] from \
             typeid='epics:nt/NTScalarArray:1.0' specifier='ai' cannot be \
             coerced to Array1D[np.int64]"
        );
        assert!(Vec::<u32>::decode(&PvaConverter::Value, &ints).is_none());
        let bytes = ntscalararray(PvField::ScalarArrayTyped(TypedScalarArray::Byte(
            vec![104, 105, 0].into(),
        )));
        // An int8 array is `Array1D[np.int8]`, and still a long string.
        assert!(make_converter("WF", Vec::<i8>::DATATYPE, &bytes).is_ok());
        assert_eq!(
            make_converter("WF", Datatype::Str, &bytes).unwrap(),
            PvaConverter::LongStr
        );
        assert_eq!(
            make_converter("WF", Vec::<String>::DATATYPE, &bytes)
                .unwrap_err()
                .to_string(),
            "WF with inferred datatype Array1D[np.int8] from \
             typeid='epics:nt/NTScalarArray:1.0' specifier='ab' cannot be \
             coerced to Sequence[str]"
        );
    }

    /// An array's datakey carries its element's numpy dtype, and a float
    /// array's precision only.
    #[test]
    fn array_datakeys_carry_their_element_dtype() {
        let with_display = |value: PvField| {
            let mut display = PvStructure::new("display_t");
            display.fields.push((
                "units".into(),
                PvField::Scalar(ScalarValue::String("mm".into())),
            ));
            display
                .fields
                .push(("precision".into(), PvField::Scalar(ScalarValue::Int(3))));
            let PvField::Structure(mut nt) = ntscalararray(value) else {
                unreachable!()
            };
            nt.fields
                .push(("display".into(), PvField::Structure(display)));
            PvField::Structure(nt)
        };
        let ints = with_display(Vec::<i32>::encode(&PvaConverter::Value, vec![1, 2, 3]).unwrap());
        let dk = Vec::<i32>::datakey("pva://WF".into(), &PvaConverter::Value, &ints).unwrap();
        assert_eq!(dk.dtype, Dtype::Array);
        assert_eq!(dk.shape, vec![Some(3)]);
        assert_eq!(dk.dtype_numpy, Some("<i4".into()));
        assert_eq!(dk.units.as_deref(), Some("mm"));
        assert_eq!(dk.precision, None);
        let floats = with_display(Vec::<f32>::encode(&PvaConverter::Value, vec![1.0]).unwrap());
        let dk = Vec::<f32>::datakey("pva://WF".into(), &PvaConverter::Value, &floats).unwrap();
        assert_eq!(dk.dtype_numpy, Some("<f4".into()));
        assert_eq!(dk.precision, Some(3));
        let strs = with_display(
            Vec::<String>::encode(&PvaConverter::Value, vec!["a".into(), "b".into()]).unwrap(),
        );
        let dk = Vec::<String>::datakey("pva://WF".into(), &PvaConverter::Value, &strs).unwrap();
        assert_eq!(dk.shape, vec![Some(2)]);
        assert_eq!(dk.dtype_numpy, Some("|S40".into()));
        assert_eq!(dk.units, None);
    }

    #[test]
    fn timestamp_extracted_from_ntscalar() {
        let f = ntscalar_with_ts(42.0, 1_700_000_000, 250_000_000);
        let ts = pv_field_to_ts(&f).expect("ntscalar timestamp");
        assert!((ts - 1_700_000_000.25).abs() < 1e-6);
        let v = pv_field_to_f64(&f).expect("ntscalar value");
        assert_eq!(v, 42.0);
    }

    #[test]
    fn timestamp_none_for_bare_scalar() {
        // Server publishes a raw scalar (no NTScalar wrapper) — no
        // server timestamp is available. `pv_field_to_ts` returns
        // None so the monitor closure can fall through to `now_ts`.
        let bare = PvField::Scalar(ScalarValue::Double(2.5));
        assert!(pv_field_to_ts(&bare).is_none());
        // Value still extractable.
        assert_eq!(pv_field_to_f64(&bare), Some(2.5));
    }

    // Live-IOC monitor smoke test. Marked #[ignore] because it
    // requires the mini-beamline mini_ioc to be running and reachable.
    // Run manually with:
    //   cargo test -p bsrs --features pva \
    //       --lib pva_monitor_live_mini_current -- --ignored --nocapture
    // PV `mini:current` is a 1Hz oscillating beam-current readback —
    // we should get multiple callback invocations within 3 seconds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn pva_monitor_live_mini_current() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let backend: EpicsPvaBackend<f64> = EpicsPvaBackend::new("mini:current");
        // Bootstrap the channel so the monitor finds it quickly.
        backend
            .connect(Duration::from_secs(3))
            .await
            .expect("pvconnect mini:current");

        let count = Arc::new(AtomicUsize::new(0));
        let last_ts: Arc<std::sync::Mutex<f64>> = Arc::new(std::sync::Mutex::new(0.0));
        let count_cb = count.clone();
        let last_ts_cb = last_ts.clone();
        let cb: ReadingValueCallback<f64> =
            Box::new(move |_v: &f64, ts: f64, _sev: Option<i32>| {
                count_cb.fetch_add(1, Ordering::SeqCst);
                *last_ts_cb.lock().unwrap() = ts;
            });
        let _tok = backend.set_callback(Some(cb));

        tokio::time::sleep(Duration::from_secs(3)).await;

        let got = count.load(Ordering::SeqCst);
        let ts = *last_ts.lock().unwrap();
        eprintln!("pva_monitor_live_mini_current: {got} callbacks, last ts={ts}");
        assert!(got > 0, "no monitor callbacks received in 3s");
        // mini_ioc publishes NTScalar with server timeStamp; the
        // extracted ts should be within ~5 minutes of now() — confirms
        // the timeStamp substructure path actually fired.
        let now = now_ts();
        assert!(
            (now - ts).abs() < 300.0,
            "last timestamp {ts} is not close to now {now} \
             (server timestamp may not be extracted)"
        );
    }

    #[test]
    fn timestamp_none_when_substructure_missing_seconds() {
        // NTScalar-shaped but `secondsPastEpoch` is absent — treat as
        // no usable server timestamp rather than fabricating a partial
        // one.
        let mut ts = PvStructure::new("time_t");
        ts.fields
            .push(("nanoseconds".into(), PvField::Scalar(ScalarValue::Int(0))));
        let mut nt = PvStructure::new("epics:nt/NTScalar:1.0");
        nt.fields
            .push(("value".into(), PvField::Scalar(ScalarValue::Double(1.0))));
        nt.fields.push(("timeStamp".into(), PvField::Structure(ts)));
        let f = PvField::Structure(nt);
        assert!(pv_field_to_ts(&f).is_none());
    }

    // Build an NTScalar carrying display/control/valueAlarm metadata.
    fn ntscalar_with_meta() -> PvField {
        fn dbl(v: f64) -> PvField {
            PvField::Scalar(ScalarValue::Double(v))
        }
        let mut display = PvStructure::new("display_t");
        display.fields.push((
            "units".into(),
            PvField::Scalar(ScalarValue::String("mm".into())),
        ));
        display
            .fields
            .push(("precision".into(), PvField::Scalar(ScalarValue::Int(3))));
        display.fields.push(("limitLow".into(), dbl(-100.0)));
        display.fields.push(("limitHigh".into(), dbl(100.0)));

        let mut control = PvStructure::new("control_t");
        control.fields.push(("limitLow".into(), dbl(-50.0)));
        control.fields.push(("limitHigh".into(), dbl(50.0)));

        let mut va = PvStructure::new("valueAlarm_t");
        va.fields.push(("lowAlarmLimit".into(), dbl(-10.0)));
        va.fields.push(("highAlarmLimit".into(), dbl(10.0)));
        va.fields.push(("lowWarningLimit".into(), dbl(-5.0)));
        va.fields.push(("highWarningLimit".into(), dbl(5.0)));

        let mut nt = PvStructure::new("epics:nt/NTScalar:1.0");
        nt.fields.push(("value".into(), dbl(1.0)));
        nt.fields
            .push(("display".into(), PvField::Structure(display)));
        nt.fields
            .push(("control".into(), PvField::Structure(control)));
        nt.fields
            .push(("valueAlarm".into(), PvField::Structure(va)));
        PvField::Structure(nt)
    }

    #[test]
    fn metadata_extracts_units_precision_and_four_limit_ranges() {
        let meta = pv_field_to_metadata(&ntscalar_with_meta(), Datatype::Float);
        assert_eq!(meta.units.as_deref(), Some("mm"));
        assert_eq!(meta.precision, Some(3));
        let limits = meta.limits.expect("limits present");
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
            limits.control,
            Some(LimitsRange {
                low: Some(-50.0),
                high: Some(50.0)
            })
        );
        assert_eq!(
            limits.display,
            Some(LimitsRange {
                low: Some(-100.0),
                high: Some(100.0)
            })
        );
    }

    #[test]
    fn metadata_for_int_omits_precision_keeps_units() {
        // ophyd-async restricts precision to floats; integer datakeys keep units.
        let meta = pv_field_to_metadata(&ntscalar_with_meta(), Datatype::Int);
        assert_eq!(meta.units.as_deref(), Some("mm"));
        assert_eq!(meta.precision, None);
    }

    #[test]
    fn bool_reads_an_enum_index_and_a_scalar() {
        let enum_bool = |i| bool::decode(&PvaConverter::Enum, &ntenum(i, &["No", "Yes"], true));
        assert_eq!((enum_bool(1), enum_bool(0)), (Some(true), Some(false)));
        let f = ntscalar(ScalarValue::Int(2));
        assert_eq!(bool::decode(&PvaConverter::Value, &f), Some(true));
    }

    fn ntscalar(value: ScalarValue) -> PvField {
        let mut nt = PvStructure::new(NT_SCALAR);
        nt.fields.push(("value".into(), PvField::Scalar(value)));
        PvField::Structure(nt)
    }

    fn converter(want: Datatype, top: &PvField) -> Result<PvaConverter> {
        make_converter("PV", want, top)
    }

    /// A bool is a two-choice enum, or an NTScalar whose type p4p calls bool,
    /// int8, uint8, int32 or uint32: not int16, as ophyd-async lists them.
    #[test]
    fn bool_takes_two_choices_or_a_listed_integer() {
        let two = ntenum(1, &["No", "Yes"], true);
        assert_eq!(converter(Datatype::Bool, &two).unwrap(), PvaConverter::Enum);
        let three = ntenum(1, &["A", "B", "C"], true);
        assert_eq!(
            converter(Datatype::Bool, &three).unwrap_err().to_string(),
            "PV has 3 choices, can't map to bool"
        );
        let int = ntscalar(ScalarValue::Int(1));
        assert_eq!(
            converter(Datatype::Bool, &int).unwrap(),
            PvaConverter::Value
        );
        let short = ntscalar(ScalarValue::Short(1));
        assert_eq!(
            converter(Datatype::Bool, &short).unwrap_err().to_string(),
            "PV with inferred datatype int from typeid='epics:nt/NTScalar:1.0' \
             specifier='h' cannot be coerced to bool"
        );
    }

    /// An int reads a float record only at precision 0.
    #[test]
    fn int_takes_a_float_at_precision_zero() {
        let at_precision = |p| {
            let PvField::Structure(mut nt) = ntscalar_with_meta() else {
                unreachable!()
            };
            let Some((_, PvField::Structure(display))) =
                nt.fields.iter_mut().find(|(n, _)| n == "display")
            else {
                unreachable!()
            };
            display.fields[1].1 = PvField::Scalar(ScalarValue::Int(p));
            converter(Datatype::Int, &PvField::Structure(nt))
        };
        assert_eq!(at_precision(0).unwrap(), PvaConverter::Value);
        assert!(matches!(at_precision(3), Err(BsrsError::Datatype(_))));
    }

    /// An enum is read as its labels, or by an inferred number as its
    /// index, and as nothing else.
    #[test]
    fn enum_is_read_as_str_or_an_inferred_number() {
        let e = ntenum(2, &["A", "B", "C"], true);
        assert_eq!(converter(Datatype::Str, &e).unwrap(), PvaConverter::Enum);
        assert_eq!(converter(Datatype::Number, &e).unwrap(), PvaConverter::Enum);
        assert_eq!(f64::decode(&PvaConverter::Enum, &e), Some(2.0));
        for want in [Datatype::Int, Datatype::Float] {
            assert!(matches!(converter(want, &e), Err(BsrsError::Datatype(_))));
        }
    }

    /// An inferred number is any numeric or bool scalar, not a string.
    #[test]
    fn number_takes_any_numeric_scalar() {
        for v in [
            ScalarValue::Double(1.5),
            ScalarValue::Short(3),
            ScalarValue::Boolean(true),
        ] {
            let f = ntscalar(v);
            assert_eq!(
                converter(Datatype::Number, &f).unwrap(),
                PvaConverter::Value
            );
            assert!(f64::decode(&PvaConverter::Value, &f).is_some());
        }
        let s = ntscalar(ScalarValue::String("x".into()));
        assert!(matches!(
            converter(Datatype::Number, &s),
            Err(BsrsError::Datatype(_))
        ));
    }

    /// A str reads a `FTVL=CHAR` waveform as its text, and writes it back
    /// NUL-terminated.
    #[test]
    fn str_takes_an_int8_array_as_long_string() {
        let mut nt = PvStructure::new(NT_SCALAR_ARRAY);
        let chars = TypedScalarArray::Byte(vec![104, 105, 0, 0].into());
        nt.fields
            .push(("value".into(), PvField::ScalarArrayTyped(chars)));
        let top = PvField::Structure(nt);
        let conv = converter(Datatype::Str, &top).unwrap();
        assert_eq!(conv, PvaConverter::LongStr);
        assert_eq!(String::decode(&conv, &top), Some("hi".into()));
        let PvField::ScalarArrayTyped(TypedScalarArray::Byte(written)) =
            String::encode(&conv, "hi".into()).unwrap()
        else {
            unreachable!()
        };
        assert_eq!(&written[..], &[104, 105, 0]);
    }

    /// A PV that is no normative type has no datatype to read it as.
    #[test]
    fn a_bare_scalar_has_no_datatype() {
        let bare = PvField::Scalar(ScalarValue::Double(1.0));
        assert_eq!(
            converter(Datatype::Float, &bare).unwrap_err().to_string(),
            "PV has no datatype for typeid='' specifier=''"
        );
    }

    /// Read as str, an enum reports its choices and nothing numeric.
    #[test]
    fn metadata_for_str_is_the_enum_choices() {
        let meta = pv_field_to_metadata(&ntenum(1, &["Off", "On"], true), Datatype::Str);
        let choices = Some(vec!["Off".to_string(), "On".to_string()]);
        assert_eq!(
            meta,
            SignalMetadata {
                choices,
                ..Default::default()
            }
        );
    }

    /// A bool read off an integer keeps its units and limits, but an integer
    /// has no precision; a float read as str keeps all but its units.
    #[test]
    fn metadata_follows_the_value_type() {
        let PvField::Structure(mut nt) = ntscalar_with_meta() else {
            unreachable!()
        };
        nt.fields[0].1 = PvField::Scalar(ScalarValue::Int(1));
        let meta = pv_field_to_metadata(&PvField::Structure(nt), Datatype::Bool);
        assert_eq!(meta.units.as_deref(), Some("mm"));
        assert_eq!(meta.precision, None);
        assert!(meta.limits.is_some());
        let meta = pv_field_to_metadata(&ntscalar_with_meta(), Datatype::Str);
        assert_eq!((meta.units, meta.precision), (None, Some(3)));
        assert!(meta.limits.is_some());
    }

    /// An empty EGU is a unit like any other, as ophyd-async keeps it.
    #[test]
    fn metadata_keeps_empty_units() {
        let PvField::Structure(mut nt) = ntscalar_with_meta() else {
            unreachable!()
        };
        let Some((_, PvField::Structure(display))) =
            nt.fields.iter_mut().find(|(n, _)| n == "display")
        else {
            unreachable!()
        };
        display.fields[0].1 = PvField::Scalar(ScalarValue::String("".into()));
        let meta = pv_field_to_metadata(&PvField::Structure(nt), Datatype::Int);
        assert_eq!(meta.units.as_deref(), Some(""));
    }

    #[test]
    fn metadata_drops_zero_pair_and_handles_bare_scalar() {
        // A display limit pair of exactly (0, 0) is PVA's "unset" → dropped.
        let mut display = PvStructure::new("display_t");
        display
            .fields
            .push(("limitLow".into(), PvField::Scalar(ScalarValue::Double(0.0))));
        display.fields.push((
            "limitHigh".into(),
            PvField::Scalar(ScalarValue::Double(0.0)),
        ));
        let mut nt = PvStructure::new("epics:nt/NTScalar:1.0");
        nt.fields
            .push(("value".into(), PvField::Scalar(ScalarValue::Double(1.0))));
        nt.fields
            .push(("display".into(), PvField::Structure(display)));
        let meta = pv_field_to_metadata(&PvField::Structure(nt), Datatype::Float);
        assert_eq!(meta.limits, None);

        // A bare (non-Normative) scalar has no metadata substructures.
        let bare = PvField::Scalar(ScalarValue::Double(2.5));
        assert_eq!(
            pv_field_to_metadata(&bare, Datatype::Float),
            SignalMetadata::default()
        );
    }

    /// An NTNDArray of `value` whose `dimension` sizes are `dims`, first
    /// fastest, as QSRV and areaDetector publish one.
    fn ntndarray(value: PvField, dims: &[i32]) -> PvField {
        let dims = dims
            .iter()
            .map(|&n| {
                let mut d = PvStructure::new("dimension_t");
                d.fields
                    .push(("size".into(), PvField::Scalar(ScalarValue::Int(n))));
                Some(d)
            })
            .collect();
        let mut nt = PvStructure::new("epics:nt/NTNDArray:1.0");
        nt.fields.push(("value".into(), value));
        nt.fields
            .push(("dimension".into(), PvField::StructureArray(dims)));
        PvField::Structure(nt)
    }

    /// QSRV's `+type:"any"` value.
    fn any(a: TypedScalarArray) -> PvField {
        let value = PvField::ScalarArrayTyped(a);
        PvField::Variant(Box::new(VariantValue { desc: None, value }))
    }

    fn longs(n: i64) -> TypedScalarArray {
        TypedScalarArray::Long((0..n).collect::<Vec<_>>().into())
    }

    /// ophyd-async `PvaNDArrayConverter`: three columns and two rows are
    /// `dimension` sizes [3, 2], shape (2, 3).
    #[test]
    fn an_ntndarray_reads_reshaped_to_its_dimensions_reversed() {
        let top = ntndarray(any(longs(6)), &[3, 2]);
        let conv = make_converter("ND", Datatype::NdArray, &top).unwrap();
        assert_eq!(conv, PvaConverter::NdArray);
        let want = NdArray::new(vec![2, 3], (0..6_i64).collect::<Vec<_>>()).unwrap();
        assert_eq!(NdArray::decode(&conv, &top), Some(want));

        let floats = TypedScalarArray::Float(vec![1.0_f32, 2.0].into());
        let union = PvField::Union {
            selector: 9,
            variant_name: "floatValue".into(),
            value: Box::new(PvField::ScalarArrayTyped(floats)),
        };
        let top = ntndarray(union, &[2]);
        assert_eq!(
            make_converter("ND", Datatype::NdArray, &top).unwrap(),
            PvaConverter::NdArray
        );
        let want = NdArray::from(vec![1.0_f32, 2.0]);
        assert_eq!(NdArray::decode(&conv, &top), Some(want));

        let short = ntndarray(any(longs(5)), &[3, 2]);
        assert_eq!(NdArray::decode(&conv, &short), None);
    }

    /// test_writing_to_ndarray_raises_typeerror, and an NTNDArray read as
    /// anything else is ophyd-async's `TypeError`.
    #[test]
    fn an_ntndarray_is_read_only_an_ndarray() {
        let top = ntndarray(any(longs(6)), &[3, 2]);
        let conv = make_converter("ND", Datatype::NdArray, &top).unwrap();
        let v = NdArray::decode(&conv, &top).unwrap();
        match NdArray::encode(&conv, v) {
            Err(BsrsError::Datatype(why)) => assert_eq!(why, "Writing to NDArray not supported"),
            other => panic!("written: {other:?}"),
        }
        assert_eq!(
            make_converter("ND", Datatype::Float, &top)
                .unwrap_err()
                .to_string(),
            "ND with inferred datatype ndarray from typeid='epics:nt/NTNDArray:1.0' \
             specifier='v' cannot be coerced to float"
        );
        let wf = ntscalararray(PvField::ScalarArrayTyped(longs(6)));
        assert_eq!(
            make_converter("WF", Datatype::NdArray, &wf)
                .unwrap_err()
                .to_string(),
            "WF with inferred datatype Array1D[np.int64] from \
             typeid='epics:nt/NTScalarArray:1.0' specifier='al' cannot be coerced to ndarray"
        );
    }

    /// test_ntndarray_lifecycle's describe: the reshaped value's shape and
    /// dtype, and no metadata.
    #[test]
    fn an_ntndarray_datakey_is_its_shape_and_dtype() {
        let top = ntndarray(any(longs(6)), &[3, 2]);
        let dk = NdArray::datakey("pva://ND".into(), &PvaConverter::NdArray, &top).unwrap();
        assert_eq!(dk.dtype, Dtype::Array);
        assert_eq!(dk.shape, vec![Some(2), Some(3)]);
        assert_eq!(dk.dtype_numpy, Some("<i8".into()));
        assert_eq!(dk.units, None);
        assert_eq!(dk.precision, None);
        assert_eq!(dk.limits, None);
        assert_eq!(dk.choices, None);
    }

    /// `PvaNDArrayConverter.value_fields` ask for `dimension` too.
    #[test]
    fn an_ndarray_monitor_asks_for_its_dimensions() {
        assert_eq!(
            PvaConverter::NdArray.monitor_request(),
            "field(value,dimension,alarm,timeStamp)"
        );
        assert_eq!(
            PvaConverter::Value.monitor_request(),
            "field(value,alarm,timeStamp)"
        );
    }

    /// ophyd-async's EpicsTestTable.
    #[derive(Clone, Debug, PartialEq, crate::core::Table)]
    struct EpicsTestTable {
        a_bool: Vec<bool>,
        a_int: Vec<i32>,
        a_float: Vec<f64>,
        a_str: Vec<String>,
        a_enum: Vec<String>,
    }

    fn strings(v: &[&str]) -> TypedScalarArray {
        TypedScalarArray::String(v.iter().map(|s| (*s).into()).collect())
    }

    /// QSRV's NTTable of `_epics_test_pva_records.db`, `a_bool` a uint8.
    fn nttable() -> PvField {
        let mut value = PvStructure::new("");
        for (name, a) in [
            ("a_bool", TypedScalarArray::UByte(vec![0, 1].into())),
            ("a_int", TypedScalarArray::Int(vec![1, -9].into())),
            ("a_float", TypedScalarArray::Double(vec![1.8, -6.0].into())),
            ("a_str", strings(&["Hello", "World"])),
            ("a_enum", strings(&["Aaa", "Ccc"])),
        ] {
            value
                .fields
                .push((name.into(), PvField::ScalarArrayTyped(a)));
        }
        let mut nt = PvStructure::new("epics:nt/NTTable:1.0");
        nt.fields
            .push(("labels".into(), PvField::ScalarArrayTyped(strings(&[]))));
        nt.fields.push(("value".into(), PvField::Structure(value)));
        PvField::Structure(nt)
    }

    /// `PvaTableConverter`: a plain Table keeps each column's PV dtype, a
    /// subclass casts it to its field's.
    #[test]
    fn an_nttable_reads_as_a_table_or_a_typed_one() {
        let top = nttable();
        let conv = make_converter("T", Datatype::Table("Table"), &top).unwrap();
        assert_eq!(conv, PvaConverter::Table);
        let plain = Table::decode(&conv, &top).unwrap();
        assert_eq!(
            plain
                .columns()
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>(),
            ["a_bool", "a_int", "a_float", "a_str", "a_enum"]
        );
        assert_eq!(plain.column("a_bool"), Some(&Column::from(vec![0_u8, 1])));
        assert_eq!(
            plain.column("a_enum"),
            Some(&Column::from(vec!["Aaa".to_string(), "Ccc".into()]))
        );

        let conv = make_converter("T", EpicsTestTable::DATATYPE, &top).unwrap();
        let want = EpicsTestTable {
            a_bool: vec![false, true],
            a_int: vec![1, -9],
            a_float: vec![1.8, -6.0],
            a_str: vec!["Hello".into(), "World".into()],
            a_enum: vec!["Aaa".into(), "Ccc".into()],
        };
        assert_eq!(EpicsTestTable::decode(&conv, &top), Some(want.clone()));

        let PvField::Structure(value) = EpicsTestTable::encode(&conv, want).unwrap() else {
            panic!("not a structure");
        };
        assert_eq!(
            value.fields[0],
            (
                "a_bool".into(),
                PvField::ScalarArrayTyped(TypedScalarArray::Boolean(vec![false, true].into()))
            )
        );
        assert_eq!(
            value.fields[4],
            (
                "a_enum".into(),
                PvField::ScalarArrayTyped(strings(&["Aaa", "Ccc"]))
            )
        );
    }

    /// test_table_lifecycle's describe: one row per element, the columns'
    /// structured dtype, as each type casts them.
    #[test]
    fn an_nttable_datakey_is_its_length_and_columns() {
        let top = nttable();
        let dk = EpicsTestTable::datakey("pva://T".into(), &PvaConverter::Table, &top).unwrap();
        assert_eq!(dk.dtype, Dtype::Array);
        assert_eq!(dk.shape, vec![Some(2)]);
        let numpy = |b: &str| {
            crate::event_model::DtypeNumpy::Structured(
                [
                    ("a_bool", b),
                    ("a_int", "<i4"),
                    ("a_float", "<f8"),
                    ("a_str", "|S40"),
                    ("a_enum", "|S40"),
                ]
                .map(|(n, d)| (n.to_string(), d.to_string()))
                .to_vec(),
            )
        };
        assert_eq!(dk.dtype_numpy, Some(numpy("|b1")));
        let dk = Table::datakey("pva://T".into(), &PvaConverter::Table, &top).unwrap();
        assert_eq!(dk.dtype_numpy, Some(numpy("|u1")));
    }

    /// A table is only an NTTable, named in a refusal by its type's name.
    #[test]
    fn an_nttable_is_read_only_as_a_table() {
        assert_eq!(
            make_converter("T", Datatype::Float, &nttable())
                .unwrap_err()
                .to_string(),
            "T with inferred datatype Table from typeid='epics:nt/NTTable:1.0' \
             specifier='S' cannot be coerced to float"
        );
        let wf = ntscalararray(PvField::ScalarArrayTyped(longs(6)));
        assert_eq!(
            make_converter("WF", EpicsTestTable::DATATYPE, &wf).unwrap_err().to_string(),
            "WF with inferred datatype Array1D[np.int64] from \
             typeid='epics:nt/NTScalarArray:1.0' specifier='al' cannot be coerced to EpicsTestTable"
        );
    }

    /// A column the subclass cannot cast is a read it does not decode.
    #[test]
    fn a_typed_table_does_not_decode_a_column_it_cannot_cast() {
        let PvField::Structure(mut nt) = nttable() else {
            unreachable!()
        };
        let PvField::Structure(value) = &mut nt.fields[1].1 else {
            unreachable!()
        };
        value.fields[0].1 = PvField::ScalarArrayTyped(TypedScalarArray::UByte(vec![0, 2].into()));
        let top = PvField::Structure(nt);
        assert_eq!(EpicsTestTable::decode(&PvaConverter::Table, &top), None);
        assert!(Table::decode(&PvaConverter::Table, &top).is_some());
    }
}
