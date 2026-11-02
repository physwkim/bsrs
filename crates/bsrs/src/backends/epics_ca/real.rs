//! Real EPICS Channel Access backend, wired to `epics-ca-rs`.
//!
//! Architecture:
//!
//! - One process-wide `CaClient`, lazily initialized via `OnceCell`.
//! - Channel registry sharded across 64 mutexes (rule **K3**), holding each
//!   PV's one channel from its creation (rule **K4**). Every call waits for
//!   the connection itself, outside the shard lock, with its own timeout.
//! - `set_callback` spawns a forwarder task per channel, returning a `SubToken`
//!   whose Drop aborts the task; dropping the underlying `MonitorHandle` then
//!   unsubscribes on the wire (rule **K2**).

use super::channel::SharedChannel;
use crate::core::error::{BsrsError, Result};
use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use crate::event_model::{make_datakey, DataKey, Dtype, Limits, LimitsRange, SignalMetadata};
use crate::protocols_async::{PutCompletion, ReadingValueCallback, SignalBackend};
use async_trait::async_trait;
use epics_base_rs::server::snapshot::{ControlInfo, DbrClass, DisplayInfo, Snapshot};
use epics_base_rs::types::WallTime;
use epics_ca_rs::client::CaClient;
use epics_ca_rs::protocol::ECA_DISCONN;
use epics_ca_rs::{estdlib, CaError, DbFieldType, EpicsValue, PvString};
use std::array;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const SHARDS: usize = 64;

/// Bound on each CA read: the 30 s epics-ca-rs applied inside `get` and
/// `get_with_metadata` before 0.33 made it explicit.
const CA_IO_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on `put`'s wait for its completion: `EPICS_CA_PUT_TIMEOUT` seconds,
/// 30 s when unset or unparseable, resolved once. epics-ca-rs read this
/// inside `put` until 0.33 left the deadline to the caller.
fn ca_put_timeout() -> Duration {
    static RESOLVED: OnceLock<Duration> = OnceLock::new();
    *RESOLVED.get_or_init(|| put_timeout_from(estdlib::env_double("EPICS_CA_PUT_TIMEOUT")))
}

fn put_timeout_from(secs: std::result::Result<f64, estdlib::EnvDoubleError>) -> Duration {
    secs.map(estdlib::duration_from_secs)
        .unwrap_or(Duration::from_secs(30))
}

/// `DefaultHasher::new` hashes alike on every call; a fresh `RandomState`
/// is seeded anew each time, scattering one PV over the shards.
fn shard_for(pv: &str) -> usize {
    use std::hash::Hasher;
    let mut h = std::hash::DefaultHasher::new();
    h.write(pv.as_bytes());
    (h.finish() as usize) % SHARDS
}

/// Convert a CA snapshot's `WallTime` to epoch seconds. A `DbrClass::Time`
/// read carries the server's processing time in `snap.timestamp`, which is the
/// reading timestamp ophyd-async reports (`epics/core/_aioca.py:305-310`,
/// `FORMAT_TIME`); the monitor path already stamps from the same field.
fn ts_to_f64(t: WallTime) -> f64 {
    t.since_unix_epoch().as_secs_f64()
}

/// Process-wide CA context.
pub struct CaContext {
    client: Arc<CaClient>,
    shards: [Mutex<HashMap<String, Arc<SharedChannel>>>; SHARDS],
}

impl CaContext {
    fn new(client: CaClient) -> Self {
        Self {
            client: Arc::new(client),
            shards: array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    /// The PV's one channel, created by its first caller and kept, searching
    /// while unconnected, as aioca's channel cache keeps it. No caller's
    /// timeout or failure is another's.
    fn channel(&self, pv: &str) -> Arc<SharedChannel> {
        let mut shard = self.shards[shard_for(pv)].lock().unwrap();
        if let Some(ch) = shard.get(pv) {
            return ch.clone();
        }
        let ch = Arc::new(SharedChannel::new(self.client.create_channel(pv)));
        shard.insert(pv.to_string(), ch.clone());
        ch
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

/// What a backend reads its PV as: ophyd-async's `datatype`, named as its
/// `format_datatype` names it in a refusal. `pub` only for the sealed
/// [`CaType`], as is [`CaConverter`]; neither is reachable outside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Datatype {
    Float,
    Int,
    Bool,
    Str,
    FloatArray,
    /// `datatype=None`, read as a number: whatever numeric or enum scalar
    /// the PV is, an enum as its index (ophyd `EpicsSignalRO`).
    Number,
}

impl Datatype {
    fn name(self) -> &'static str {
        match self {
            Self::Float | Self::Number => "float",
            Self::Int => "int",
            Self::Bool => "bool",
            Self::Str => "str",
            Self::FloatArray => "Array1D[np.float64]",
        }
    }
}

/// How a backend's values go to and from its PV, as [`make_converter`]
/// picked it at connect.
#[derive(Debug, PartialEq)]
pub enum CaConverter {
    /// The PV's own DBR type, read and written as it.
    Native(DbFieldType),
    /// A `str` on a `DBR_CHAR` waveform, NUL-terminated (`CaLongStrConverter`).
    LongStr,
    /// A `str` on a `DBR_ENUM`: its labels, by the record's choices.
    Enum(Vec<String>),
}

impl CaConverter {
    /// The DBR type a put is written as.
    fn dbr(&self) -> DbFieldType {
        match self {
            Self::Native(t) => *t,
            Self::LongStr => DbFieldType::Char,
            Self::Enum(_) => DbFieldType::Enum,
        }
    }
}

/// ophyd-async `make_converter`: the converter that reads `pv`, of `native`
/// type and `element_count` elements, as `want`, from its DBR_CTRL read
/// `snap`, or the `TypeError` that refuses it.
fn make_converter(
    pv: &str,
    want: Datatype,
    native: DbFieldType,
    element_count: u32,
    snap: &Snapshot,
) -> Result<CaConverter> {
    use Datatype as D;
    use DbFieldType as F;
    let is_array = element_count > 1;
    match (want, native, is_array) {
        (D::Str, F::Char, true) => Ok(CaConverter::LongStr),
        (D::Bool, F::Enum, false) => {
            let n = snap.enums.as_ref().map_or(0, |e| e.strings.len());
            if n != 2 {
                return Err(BsrsError::Datatype(format!(
                    "{pv} has {n} choices, can't map to bool"
                )));
            }
            Ok(CaConverter::Native(native))
        }
        (D::Str, F::Enum, false) => Ok(CaConverter::Enum(
            snap.enums
                .as_ref()
                .map(|e| enum_labels_to_strings(&e.strings))
                .unwrap_or_default(),
        )),
        // An int on a float record whose precision shows no fraction.
        (D::Int, F::Float | F::Double, false)
            if snap.display.as_ref().is_some_and(|d| d.precision == 0) =>
        {
            Ok(CaConverter::Native(native))
        }
        (D::Bool, F::Char | F::Short, false)
        | (D::Number, F::Short | F::Char | F::Long | F::Float | F::Double | F::Enum, false)
        | (D::Float, F::Float | F::Double, false)
        | (D::Int, F::Short | F::Char | F::Long, false)
        | (D::Str, F::String, false)
        | (D::FloatArray, F::Double, true) => Ok(CaConverter::Native(native)),
        _ => Err(BsrsError::Datatype(format!(
            "{pv} with inferred datatype {} cannot be coerced to {}",
            inferred_name(native, is_array),
            want.name()
        ))),
    }
}

/// The datatype ophyd-async infers for a PV (`_datatype_converter_from_dbr`),
/// as its refusal names it.
fn inferred_name(native: DbFieldType, is_array: bool) -> String {
    use DbFieldType as F;
    match (native, is_array) {
        (F::Enum, _) => "str | SubsetEnum | StrictEnum",
        (F::String, false) => "str",
        (F::Short | F::Char | F::Long, false) => "int",
        (F::Float | F::Double, false) => "float",
        (F::String, true) => "Sequence[str]",
        (F::Short, true) => "Array1D[np.int16]",
        (F::Float, true) => "Array1D[np.float32]",
        (F::Char, true) => "Array1D[np.uint8]",
        (F::Long, true) => "Array1D[np.int32]",
        (F::Double, true) => "Array1D[np.float64]",
        // Not a CA wire type: `DbFieldType::from_u16` yields none of these.
        (other, _) => return format!("{other:?}"),
    }
    .to_string()
}

/// The converter for reading `pv` as `want` on the connected `ch`, made from
/// one DBR_CTRL read, which it returns too: ophyd-async's connect-time
/// `caget(FORMAT_CTRL)`.
async fn converter_for(
    pv: &str,
    want: Datatype,
    ch: &SharedChannel,
) -> Result<(CaConverter, Snapshot)> {
    let snap = ctrl_get(ch).await?;
    let info = ch
        .info()
        .await
        .map_err(|e| BsrsError::Backend(format!("ca info {pv}: {e}")))?;
    let conv = make_converter(pv, want, info.native_type, info.element_count, &snap)?;
    Ok((conv, snap))
}

mod sealed {
    use super::{CaConverter, Datatype, Snapshot};
    use crate::core::error::Result;
    use crate::event_model::DataKey;
    use epics_ca_rs::EpicsValue;

    /// A type [`EpicsCaBackend`](super::EpicsCaBackend) reads a PV as: its
    /// ophyd-async datatype, and its values through the converter
    /// [`make_converter`](super::make_converter) picked for it.
    pub trait CaType: Into<serde_json::Value> + Clone + Send + Sync + 'static {
        #[doc(hidden)]
        const DATATYPE: Datatype;
        /// `None` when `v` is not a value of the PV `conv` was made for.
        #[doc(hidden)]
        fn decode(conv: &CaConverter, v: &EpicsValue) -> Option<Self>;
        #[doc(hidden)]
        fn encode(conv: &CaConverter, v: Self) -> Result<EpicsValue>;
        /// ophyd-async `get_datakey` from a DBR_CTRL read `snap`.
        #[doc(hidden)]
        fn datakey(source: String, conv: &CaConverter, snap: &Snapshot) -> Result<DataKey>;
    }
}
use sealed::CaType;

impl CaType for f64 {
    const DATATYPE: Datatype = Datatype::Float;
    fn decode(_: &CaConverter, v: &EpicsValue) -> Option<Self> {
        epics_to_f64(v)
    }
    fn encode(conv: &CaConverter, v: Self) -> Result<EpicsValue> {
        Ok(f64_to_wire(conv.dbr(), v))
    }
    fn datakey(source: String, conv: &CaConverter, snap: &Snapshot) -> Result<DataKey> {
        // Only a float record has a precision (`DBR_CTRL_LONG` has none).
        let float = matches!(conv.dbr(), DbFieldType::Float | DbFieldType::Double);
        let meta = ctrl_metadata(snap, true, float);
        Ok(primitive_datakey::<f64>(source, meta))
    }
}

impl CaType for i64 {
    const DATATYPE: Datatype = Datatype::Int;
    fn decode(_: &CaConverter, v: &EpicsValue) -> Option<Self> {
        epics_to_i64(v)
    }
    fn encode(conv: &CaConverter, v: Self) -> Result<EpicsValue> {
        Ok(i64_to_wire(conv.dbr(), v))
    }
    fn datakey(source: String, _: &CaConverter, snap: &Snapshot) -> Result<DataKey> {
        // Ints carry units and limits, but no precision — there are no
        // fractional digits (ophyd-async excludes precision for `int`).
        let meta = ctrl_metadata(snap, true, false);
        Ok(primitive_datakey::<i64>(source, meta))
    }
}

impl CaType for bool {
    const DATATYPE: Datatype = Datatype::Bool;
    fn decode(_: &CaConverter, v: &EpicsValue) -> Option<Self> {
        epics_to_bool(v)
    }
    fn encode(conv: &CaConverter, v: Self) -> Result<EpicsValue> {
        Ok(bool_to_wire(conv.dbr(), v))
    }
    fn datakey(source: String, _: &CaConverter, _: &Snapshot) -> Result<DataKey> {
        Ok(primitive_datakey::<bool>(source, SignalMetadata::default()))
    }
}

impl CaType for String {
    const DATATYPE: Datatype = Datatype::Str;
    fn decode(conv: &CaConverter, v: &EpicsValue) -> Option<Self> {
        match (conv, v) {
            (CaConverter::Native(_), EpicsValue::String(s)) => Some(s.as_str_lossy().into_owned()),
            (CaConverter::LongStr, EpicsValue::UCharArray(bytes)) => Some(nul_terminated(bytes)),
            // A one-element char waveform: the lone NUL of an empty string.
            (CaConverter::LongStr, EpicsValue::UChar(c)) => Some(nul_terminated(&[*c])),
            (CaConverter::Enum(choices), v) => epics_to_enum_label(v, choices),
            _ => None,
        }
    }
    fn encode(conv: &CaConverter, v: Self) -> Result<EpicsValue> {
        match conv {
            CaConverter::Native(_) => Ok(EpicsValue::String(v.as_str().into())),
            // NUL-terminated, as `caput -S` writes it.
            CaConverter::LongStr => {
                let mut bytes = v.into_bytes();
                bytes.push(0);
                Ok(EpicsValue::CharArray(bytes.into()))
            }
            CaConverter::Enum(choices) => enum_label_to_index(&v, choices).map(EpicsValue::Enum),
        }
    }
    fn datakey(source: String, conv: &CaConverter, _: &Snapshot) -> Result<DataKey> {
        let choices = match conv {
            CaConverter::Enum(choices) => Some(choices.clone()),
            _ => None,
        };
        let meta = SignalMetadata {
            choices,
            ..SignalMetadata::default()
        };
        Ok(primitive_datakey::<String>(source, meta))
    }
}

impl CaType for Vec<f64> {
    const DATATYPE: Datatype = Datatype::FloatArray;
    fn decode(_: &CaConverter, v: &EpicsValue) -> Option<Self> {
        epics_to_vec_f64(v)
    }
    fn encode(_: &CaConverter, v: Self) -> Result<EpicsValue> {
        Ok(EpicsValue::DoubleArray(v.into()))
    }
    fn datakey(source: String, conv: &CaConverter, snap: &Snapshot) -> Result<DataKey> {
        let v = Self::decode(conv, &snap.value).ok_or_else(|| undecodable(&snap.value))?;
        Ok(array_datakey(source, &v, ctrl_metadata(snap, true, true)))
    }
}

/// A read whose value is not of the type the converter was made for: the PV
/// changed type since the connect.
fn undecodable(v: &EpicsValue) -> BsrsError {
    BsrsError::Backend(format!("ca: {v:?} is not of the PV's connect-time type"))
}

/// CA backend for a single PV.
pub struct EpicsCaBackend<T: Clone + Send + Sync + 'static> {
    pv: String,
    ctx: Arc<CaContext>,
    channel: OnceLock<Arc<SharedChannel>>,
    /// What the PV is read as: `T`'s datatype, or [`Datatype::Number`].
    want: Datatype,
    /// The converter the latest `connect`, or before one the first call,
    /// made for the PV.
    converter: Mutex<Option<Arc<CaConverter>>>,
    /// ophyd-async's `initial_values`: what `connect` read, which
    /// `put(None)` writes.
    initial: Mutex<Option<T>>,
}

impl<T: CaType> EpicsCaBackend<T> {
    /// Build with a PV name. `connect` refuses a PV that cannot be read as
    /// `T`, as ophyd-async's `make_converter`: `f64` is a float record,
    /// `i64` an integer one (or a float one at precision 0), `bool` a
    /// two-choice enum or a char/short, `String` a string, an enum's labels
    /// or a char waveform's text, `Vec<f64>` a double waveform.
    pub fn new(pv: impl Into<String>) -> Self {
        Self::reading_as(pv.into(), T::DATATYPE)
    }

    fn reading_as(pv: String, want: Datatype) -> Self {
        Self {
            pv,
            ctx: ca_context(),
            channel: OnceLock::new(),
            want,
            converter: Mutex::new(None),
            initial: Mutex::new(None),
        }
    }
}

impl EpicsCaBackend<f64> {
    /// ophyd-async's `datatype=None` read as a number: any numeric or enum
    /// scalar PV, an enum as its index, for the devices whose PV is the
    /// user's to name (`ca_value`, a suspender, a positioner's done PV).
    pub(crate) fn new_inferred(pv: impl Into<String>) -> Self {
        Self::reading_as(pv.into(), Datatype::Number)
    }
}

impl<T: CaType> crate::devices::BackendFromPv for EpicsCaBackend<T> {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
}

impl<T: CaType> EpicsCaBackend<T> {
    fn channel(&self) -> &Arc<SharedChannel> {
        self.channel.get_or_init(|| self.ctx.channel(&self.pv))
    }

    /// The channel, once connected within `timeout`. Asked on every call, as
    /// aioca's `channel.wait()`: a call during an outage waits for the
    /// reconnect.
    async fn ensure_channel(&self, timeout: Duration) -> Result<Arc<SharedChannel>> {
        let ch = self.channel();
        ch.wait_connected(timeout)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca connect {}: {e}", self.pv)))?;
        Ok(ch.clone())
    }

    /// `connect`: open the channel, then make the converter from a DBR_CTRL
    /// read whose value is the one `put(None)` writes, all within `timeout`,
    /// as ophyd-async's connect-time `caget` and `make_converter`.
    async fn connect_reading(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let ch = self.ensure_channel(timeout).await?;
        let (conv, snap) =
            tokio::time::timeout_at(deadline, converter_for(&self.pv, self.want, &ch))
                .await
                .map_err(|_| {
                    BsrsError::Backend(format!("ca connect {}: no value in {timeout:?}", self.pv))
                })??;
        let v = T::decode(&conv, &snap.value).ok_or_else(|| undecodable(&snap.value))?;
        *self.converter.lock().unwrap() = Some(Arc::new(conv));
        *self.initial.lock().unwrap() = Some(v);
        Ok(())
    }

    /// The converter `connect` made, or one made now if none has.
    async fn converter(&self, ch: &SharedChannel) -> Result<Arc<CaConverter>> {
        if let Some(conv) = self.converter.lock().unwrap().clone() {
            return Ok(conv);
        }
        let conv = Arc::new(converter_for(&self.pv, self.want, ch).await?.0);
        *self.converter.lock().unwrap() = Some(conv.clone());
        Ok(conv)
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
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let v = T::encode(&*self.converter(&ch).await?, value)?;
        put_callback(&ch, &v, "ca put")
    }

    async fn value(&self) -> Result<T> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let conv = self.converter(&ch).await?;
        let (_ty, v) = ch
            .get_with_timeout(CA_IO_TIMEOUT)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        T::decode(&conv, &v).ok_or_else(|| undecodable(&v))
    }
}

/// [`SharedChannel::put_callback`] of `v`: the request is queued on the
/// circuit, in call order, when this returns. The completion has no deadline
/// of its own: a busy record's callback lasts as long as its operation, so
/// whoever awaits it bounds it.
fn put_callback(
    ch: &Arc<SharedChannel>,
    v: &EpicsValue,
    what: &'static str,
) -> Result<PutCompletion> {
    let done = ch
        .put_callback(v)
        .map_err(|e| BsrsError::Backend(format!("{what}: {e}")))?;
    Ok(Box::pin(async move {
        done.await
            .unwrap_or_else(|_| Err("put-callback driver gone".into()))
            .map_err(|e| BsrsError::Backend(format!("{what}: {e}")))
    }))
}

/// `put`'s wait on `done`, bounded by [`ca_put_timeout`]: a plain put has no
/// caller deadline to defer to.
async fn await_put(done: PutCompletion, what: &'static str) -> Result<()> {
    tokio::time::timeout(ca_put_timeout(), done)
        .await
        .unwrap_or_else(|_| Err(BsrsError::Backend(format!("{what}: {}", CaError::Timeout))))
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

/// Feed `cb` each update of `ch`'s monitor that `decode` accepts. As in
/// aioca's camonitor, a disconnect does not end it, for the subscription
/// resumes on reconnect; any other error does (`_catools.py` closes it).
async fn forward_monitor<T>(
    ch: &SharedChannel,
    decode: impl Fn(&EpicsValue) -> Option<T>,
    cb: &ReadingValueCallback<T>,
) {
    let Ok(mut sub) = ch.subscribe().await else {
        return;
    };
    loop {
        match sub.recv().await {
            Some(Ok(snap)) => {
                if let Some(v) = decode(&snap.value) {
                    let ts = ts_to_f64(snap.timestamp);
                    cb(&v, ts, Some(alarm_severity(snap.alarm.severity)));
                }
            }
            Some(Err(CaError::ServerError(ECA_DISCONN))) => {}
            Some(Err(_)) | None => return,
        }
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

/// An `Array1D[np.float64]`'s datakey: shaped as the array read.
fn array_datakey(source: String, value: &[f64], meta: SignalMetadata) -> DataKey {
    let shape = vec![Some(value.len() as u64)];
    make_datakey(source, Dtype::Array, shape, Some("<f8".into()), meta)
}

/// The DBR_CTRL read ophyd-async's `get_datakey` describes a PV from.
async fn ctrl_get(ch: &SharedChannel) -> Result<Snapshot> {
    ch.get_with_metadata(DbrClass::Ctrl, CA_IO_TIMEOUT)
        .await
        .map_err(|e| BsrsError::Backend(format!("ca ctrl get: {e}")))
}

/// Shape a DBR_CTRL read's units, precision and four limit ranges into a
/// [`SignalMetadata`], applying ophyd-async's per-field inclusion rules
/// (`_aioca._metadata_from_augmented_value`):
///   - `units`, empty included, only when the datatype is not string/bool
///     (`want_units`),
///   - `precision` only for floats — ints have no fractional digits
///     (`want_precision`),
///   - `limits` for any numeric, each range filtered by [`limits_range`].
///
/// A field stays `None` when the server published no display/control block.
fn ctrl_metadata(snap: &Snapshot, want_units: bool, want_precision: bool) -> SignalMetadata {
    let mut meta = SignalMetadata::default();
    if let Some(d) = snap.display.as_ref() {
        if want_units {
            meta.units = Some(d.units.as_str_lossy().into_owned());
        }
        if want_precision {
            meta.precision = Some(d.precision as i64);
        }
    }
    let limits = ctrl_limits(snap.display.as_ref(), snap.control.as_ref());
    if limits != Limits::default() {
        meta.limits = Some(limits);
    }
    meta
}

/// Encode an `f64` payload as the `EpicsValue` variant that matches
/// the channel's `native_type`. Required because
/// `CaChannel::put_callback` writes with `native_type` on the wire (see
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
        DbFieldType::UInt64 => EpicsValue::UInt64(v as u64),
        DbFieldType::UShort => EpicsValue::UShort(v as u16),
        DbFieldType::ULong => EpicsValue::ULong(v as u32),
        DbFieldType::UChar => EpicsValue::UChar(v as u8),
        DbFieldType::String => EpicsValue::String(format!("{v}").into()),
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
        DbFieldType::UInt64 => EpicsValue::UInt64(v as u64),
        DbFieldType::UShort => EpicsValue::UShort(v as u16),
        DbFieldType::ULong => EpicsValue::ULong(v as u32),
        DbFieldType::UChar => EpicsValue::UChar(v as u8),
        DbFieldType::String => EpicsValue::String(format!("{v}").into()),
    }
}

/// Encode a `bool` payload matching the channel's `native_type`.
fn bool_to_wire(t: DbFieldType, v: bool) -> EpicsValue {
    i64_to_wire(t, if v { 1 } else { 0 })
}

/// The decoders below read values off the CA wire, where a `DBR_CHAR`
/// arrives as its unsigned carrier `UChar`/`UCharArray`
/// (`DbFieldType::wire_carrier`), never as `Char`/`CharArray`.
fn epics_to_f64(v: &EpicsValue) -> Option<f64> {
    match v {
        EpicsValue::Double(d) => Some(*d),
        EpicsValue::Float(f) => Some(*f as f64),
        EpicsValue::Long(l) => Some(*l as f64),
        EpicsValue::Short(s) => Some(*s as f64),
        EpicsValue::UChar(c) => Some(*c as f64),
        EpicsValue::Int64(i) => Some(*i as f64),
        EpicsValue::Enum(e) => Some(*e as f64),
        _ => None,
    }
}

/// Decode a `DBR_DOUBLE` waveform, the one array `Vec<f64>` is made for. A
/// one-element answer arrives as the scalar (`EpicsValue::from_bytes_array`).
fn epics_to_vec_f64(v: &EpicsValue) -> Option<Vec<f64>> {
    match v {
        EpicsValue::DoubleArray(a) => Some(a.to_vec()),
        EpicsValue::Double(d) => Some(vec![*d]),
        _ => None,
    }
}

fn epics_to_i64(v: &EpicsValue) -> Option<i64> {
    match v {
        EpicsValue::Int64(i) => Some(*i),
        EpicsValue::Long(l) => Some(*l as i64),
        EpicsValue::Short(s) => Some(*s as i64),
        EpicsValue::UChar(c) => Some(*c as i64),
        EpicsValue::Enum(e) => Some(*e as i64),
        EpicsValue::Float(f) => Some(*f as i64),
        EpicsValue::Double(d) => Some(*d as i64),
        _ => None,
    }
}

fn epics_to_bool(v: &EpicsValue) -> Option<bool> {
    epics_to_i64(v).map(|i| i != 0)
}

fn nul_terminated(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

#[async_trait]
impl<T: CaType> SignalBackend<T> for EpicsCaBackend<T> {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.connect_reading(timeout).await
    }
    async fn put_start(&self, value: Option<T>) -> Result<PutCompletion> {
        self.put_value(value).await
    }
    async fn put(&self, value: Option<T>) -> Result<()> {
        await_put(self.put_value(value).await?, "ca put").await
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let conv = self.converter(&ch).await?;
        T::datakey(format!("ca://{source}"), &conv, &ctrl_get(&ch).await?)
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let conv = self.converter(&ch).await?;
        let snap = ch
            .get_with_metadata(DbrClass::Time, CA_IO_TIMEOUT)
            .await
            .map_err(|e| BsrsError::Backend(format!("ca get: {e}")))?;
        let v = T::decode(&conv, &snap.value).ok_or_else(|| undecodable(&snap.value))?;
        Ok(ReadingValue {
            value: v.into(),
            timestamp: ts_to_f64(snap.timestamp),
            alarm_severity: Some(alarm_severity(snap.alarm.severity)),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<T> {
        self.value().await
    }
    async fn get_setpoint(&self) -> Result<T> {
        // CA channels expose only one read path; we treat readback as the
        // best-effort setpoint as well.
        self.value().await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<T>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => cb,
        };
        let ch = self.channel().clone();
        let cached = self.converter.lock().unwrap().clone();
        let (pv, want) = (self.pv.clone(), self.want);
        // Spawn a forwarder. Held tokio::JoinHandle is aborted on Drop (K1).
        let handle = tokio::spawn(async move {
            let conv = match cached {
                Some(conv) => conv,
                None => {
                    // No connect made the converter: wait for the channel as
                    // long as the token lives, as camonitor does.
                    if ch.wait_connected(Duration::MAX).await.is_err() {
                        return;
                    }
                    match converter_for(&pv, want, &ch).await {
                        Ok((conv, _)) => Arc::new(conv),
                        Err(e) => {
                            tracing::warn!("ca monitor {pv}: {e}");
                            return;
                        }
                    }
                }
            };
            forward_monitor(&ch, |v| T::decode(&conv, v), &cb).await;
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str, _read: bool) -> String {
        format!("ca://{name}")
    }
}

impl EpicsCaBackend<bool> {
    /// Fire-and-forget put (`CA_PROTO_WRITE`, no put-callback). For busy
    /// records (areaDetector `Acquire`/`Capture`) a put-callback completes
    /// only when the *operation* ends — and a `0` put's callback can
    /// deadlock outright — so callers that just need the value written use
    /// this and watch the readback instead (the ophyd-async
    /// `EpicsOptions(wait=non_zero)` convention).
    pub async fn put_nowait(&self, value: bool) -> Result<()> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let v = bool::encode(&*self.converter(&ch).await?, value)?;
        ch.put_no_callback(&v)
            .map_err(|e| BsrsError::Backend(format!("ca put_nowait: {e}")))
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
        EpicsValue::String(s) => Some(s.as_str_lossy().into_owned()),
        _ => None,
    }
}

/// EPICS enum choice labels arrive as `PvString` (raw wire bytes); bsrs
/// models choices as UTF-8 `String`, so convert lossily at this boundary.
fn enum_labels_to_strings(labels: &[PvString]) -> Vec<String> {
    labels
        .iter()
        .map(|s| s.as_str_lossy().into_owned())
        .collect()
}

/// CA backend for `DBR_ENUM` PVs (`mbbi`/`mbbo`, areaDetector `ImageMode`,
/// `TriggerMode`, `FileWriteMode`, `DetectorState_RBV`, …) as their labels:
/// the `String` backend, which reads any `DBR_ENUM` channel so.
pub type CaEnumBackend = EpicsCaBackend<String>;

#[cfg(test)]
mod tests {
    use super::*;
    use epics_base_rs::server::snapshot::EnumInfo;

    #[test]
    fn put_timeout_from_env_double() {
        use estdlib::EnvDoubleError;
        assert_eq!(put_timeout_from(Ok(2.5)), Duration::from_millis(2500));
        assert_eq!(
            put_timeout_from(Err(EnvDoubleError::Unset)),
            Duration::from_secs(30)
        );
        let bad = estdlib::epics_parse_double("abc").unwrap_err();
        assert_eq!(
            put_timeout_from(Err(EnvDoubleError::Invalid(bad))),
            Duration::from_secs(30)
        );
    }

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
    fn epics_to_vec_f64_reads_double_waveforms_only() {
        assert_eq!(
            epics_to_vec_f64(&EpicsValue::DoubleArray(vec![1.0, 2.0].into())),
            Some(vec![1.0, 2.0])
        );
        assert_eq!(epics_to_vec_f64(&EpicsValue::Double(3.0)), Some(vec![3.0]));
        assert_eq!(
            epics_to_vec_f64(&EpicsValue::LongArray(vec![3, 4].into())),
            None
        );
    }

    /// A read of the current element count decodes 0, 1 and n elements: the
    /// one-element answer arrives as the scalar.
    #[test]
    fn autosized_reads_decode_at_every_count() {
        let double = CaConverter::Native(DbFieldType::Double);
        for n in [0, 1, 3] {
            let put: Vec<f64> = (0..n).map(|i| i as f64 + 0.5).collect();
            let wire = Vec::<f64>::encode(&double, put.clone()).unwrap().to_bytes();
            let v = EpicsValue::from_bytes_array(DbFieldType::Double, &wire, n).unwrap();
            assert_eq!(Vec::<f64>::decode(&double, &v), Some(put), "{n} elements");
        }
        for s in ["", "a", "/data/scan42"] {
            let v = String::encode(&CaConverter::LongStr, s.into()).unwrap();
            let back = String::decode(&CaConverter::LongStr, &off_the_wire(&v, s.len() + 1));
            assert_eq!(back.as_deref(), Some(s), "{s:?}");
        }
    }

    /// `v` as a CA read hands it back: its DBR_CHAR bytes decoded by the
    /// carrier the wire gives them.
    fn off_the_wire(v: &EpicsValue, count: usize) -> EpicsValue {
        EpicsValue::from_bytes_array(DbFieldType::Char.wire_carrier(), &v.to_bytes(), count)
            .unwrap()
    }

    /// A DBR_CHAR read is unsigned: byte 200 is 200, as `dbr_char_t` is.
    #[test]
    fn dbr_char_reads_decode() {
        let byte = off_the_wire(&EpicsValue::Char(200), 1);
        assert_eq!(epics_to_f64(&byte), Some(200.0));
        assert_eq!(epics_to_i64(&byte), Some(200));
        assert_eq!(epics_to_bool(&byte), Some(true));
        let text = off_the_wire(&EpicsValue::CharArray(b"ab\0".to_vec().into()), 3);
        assert_eq!(
            String::decode(&CaConverter::LongStr, &text).as_deref(),
            Some("ab")
        );
    }

    #[test]
    fn long_string_round_trips_via_char_array() {
        let path = "/data/scan42/run0001.h5";
        let v = String::encode(&CaConverter::LongStr, path.into()).unwrap();
        match &v {
            EpicsValue::CharArray(bytes) => {
                assert_eq!(*bytes.last().unwrap(), 0);
                assert_eq!(&bytes[..path.len()], path.as_bytes());
            }
            _ => panic!("expected CharArray, got {v:?}"),
        }
        let back = String::decode(&CaConverter::LongStr, &off_the_wire(&v, path.len() + 1))
            .expect("a DBR_CHAR array decodes as a long string");
        assert_eq!(back, path);
    }

    #[test]
    fn short_string_round_trips_via_dbr_string() {
        let conv = CaConverter::Native(DbFieldType::String);
        let v = String::encode(&conv, "13SIM1".into()).unwrap();
        match &v {
            EpicsValue::String(s) => assert_eq!(s, "13SIM1"),
            _ => panic!("expected String, got {v:?}"),
        }
        assert_eq!(String::decode(&conv, &v).as_deref(), Some("13SIM1"));
    }

    #[test]
    fn long_string_decode_strips_at_first_nul() {
        let bytes = b"/data/scan\0/tail/ignored".to_vec();
        let s = String::decode(&CaConverter::LongStr, &EpicsValue::UCharArray(bytes.into()))
            .expect("UCharArray");
        assert_eq!(s, "/data/scan");
    }

    /// A DBR_CTRL read with `labels` as its enum strings and `precision` as
    /// its display precision.
    fn ctrl(labels: &[&str], precision: Option<i16>) -> Snapshot {
        let mut snap = Snapshot::new(EpicsValue::Double(0.0), 0, 0, std::time::UNIX_EPOCH);
        if !labels.is_empty() {
            snap.enums = Some(EnumInfo::new(labels.iter().map(|s| (*s).into()).collect()));
        }
        snap.display = precision.map(|precision| DisplayInfo {
            precision,
            ..Default::default()
        });
        snap
    }

    fn converter(want: Datatype, native: DbFieldType, n: u32, snap: &Snapshot) -> CaConverter {
        make_converter("P", want, native, n, snap).unwrap()
    }

    fn refusal(want: Datatype, native: DbFieldType, n: u32, snap: &Snapshot) -> String {
        match make_converter("P", want, native, n, snap) {
            Err(BsrsError::Datatype(msg)) => msg,
            other => panic!("{want:?} on {native:?}[{n}]: {other:?}"),
        }
    }

    #[test]
    fn bool_on_enum_needs_two_choices() {
        let off_on = ctrl(&["Off", "On"], None);
        assert_eq!(
            converter(Datatype::Bool, DbFieldType::Enum, 1, &off_on),
            CaConverter::Native(DbFieldType::Enum)
        );
        assert_eq!(
            refusal(
                Datatype::Bool,
                DbFieldType::Enum,
                1,
                &ctrl(&["A", "B", "C"], None)
            ),
            "P has 3 choices, can't map to bool"
        );
    }

    #[test]
    fn bool_on_char_or_short_but_not_long() {
        let none = ctrl(&[], None);
        assert_eq!(
            converter(Datatype::Bool, DbFieldType::Short, 1, &none),
            CaConverter::Native(DbFieldType::Short)
        );
        assert_eq!(
            converter(Datatype::Bool, DbFieldType::Char, 1, &none),
            CaConverter::Native(DbFieldType::Char)
        );
        assert_eq!(
            refusal(Datatype::Bool, DbFieldType::Long, 1, &none),
            "P with inferred datatype int cannot be coerced to bool"
        );
    }

    #[test]
    fn int_on_float_needs_precision_zero() {
        assert_eq!(
            converter(Datatype::Int, DbFieldType::Double, 1, &ctrl(&[], Some(0))),
            CaConverter::Native(DbFieldType::Double)
        );
        assert_eq!(
            refusal(Datatype::Int, DbFieldType::Double, 1, &ctrl(&[], Some(3))),
            "P with inferred datatype float cannot be coerced to int"
        );
    }

    #[test]
    fn float_on_int_record_is_refused() {
        assert_eq!(
            refusal(Datatype::Float, DbFieldType::Long, 1, &ctrl(&[], Some(0))),
            "P with inferred datatype int cannot be coerced to float"
        );
    }

    #[test]
    fn str_on_char_waveform_is_long_string() {
        let none = ctrl(&[], None);
        assert_eq!(
            converter(Datatype::Str, DbFieldType::Char, 256, &none),
            CaConverter::LongStr
        );
        assert_eq!(
            refusal(Datatype::Str, DbFieldType::Char, 1, &none),
            "P with inferred datatype int cannot be coerced to str"
        );
    }

    #[test]
    fn float_array_on_double_waveform_only() {
        let none = ctrl(&[], None);
        assert_eq!(
            converter(Datatype::FloatArray, DbFieldType::Double, 10, &none),
            CaConverter::Native(DbFieldType::Double)
        );
        assert_eq!(
            refusal(Datatype::FloatArray, DbFieldType::Long, 10, &none),
            "P with inferred datatype Array1D[np.int32] cannot be coerced to Array1D[np.float64]"
        );
        assert_eq!(
            refusal(Datatype::Float, DbFieldType::Double, 10, &none),
            "P with inferred datatype Array1D[np.float64] cannot be coerced to float"
        );
    }

    #[test]
    fn int_on_string_is_refused() {
        assert_eq!(
            refusal(Datatype::Int, DbFieldType::String, 1, &ctrl(&[], None)),
            "P with inferred datatype str cannot be coerced to int"
        );
    }

    #[test]
    fn enum_is_its_labels_and_no_number() {
        let modes = ctrl(&["Single", "Multiple", "Continuous"], None);
        assert_eq!(
            converter(Datatype::Str, DbFieldType::Enum, 1, &modes),
            CaConverter::Enum(image_mode_choices())
        );
        assert_eq!(
            refusal(Datatype::Int, DbFieldType::Enum, 1, &modes),
            "P with inferred datatype str | SubsetEnum | StrictEnum cannot be coerced to int"
        );
    }

    #[test]
    fn number_reads_any_numeric_or_enum_scalar() {
        let modes = ctrl(&["Single", "Multiple", "Continuous"], None);
        for native in [
            DbFieldType::Enum,
            DbFieldType::Short,
            DbFieldType::Char,
            DbFieldType::Long,
            DbFieldType::Float,
            DbFieldType::Double,
        ] {
            assert_eq!(
                converter(Datatype::Number, native, 1, &modes),
                CaConverter::Native(native)
            );
        }
        assert_eq!(
            f64::decode(
                &CaConverter::Native(DbFieldType::Enum),
                &EpicsValue::Enum(2)
            ),
            Some(2.0)
        );
        assert_eq!(
            refusal(Datatype::Number, DbFieldType::String, 1, &modes),
            "P with inferred datatype str cannot be coerced to float"
        );
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
            // DBF_UCHAR promotes to DBR_CHAR on the wire: 1 byte, like Char.
            (DbFieldType::UChar, 1),
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
        assert_eq!(epics_to_bool(&EpicsValue::UChar(0)), Some(false));
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

    /// Every backend on a PV must find the one registry slot its channel is in.
    #[test]
    fn shard_for_is_the_same_on_every_call() {
        let first = shard_for("XF:31IDA-BI{Dev:1}E-I");
        assert!((0..100).all(|_| shard_for("XF:31IDA-BI{Dev:1}E-I") == first));
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
