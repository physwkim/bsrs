//! Real PVA backend wired to `epics-pva-rs::PvaClient`.

use async_trait::async_trait;
use cirrus_core::error::{CirrusError, Result};
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::SubToken;
use cirrus_event_model::{make_datakey, DataKey, Dtype, SignalMetadata};
use cirrus_protocols_async::{ReadingValueCallback, SignalBackend};
use epics_pva_rs::client::PvaClient;
use epics_pva_rs::pv_request::PvRequestExpr;
use epics_pva_rs::pvdata::TypedScalarArray;
use epics_pva_rs::{PvField, ScalarValue};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

static CTX: OnceLock<Arc<PvaClient>> = OnceLock::new();

/// Process-wide PVA client.
pub fn pva_context() -> Arc<PvaClient> {
    CTX.get_or_init(|| Arc::new(PvaClient::new().expect("PvaClient::new")))
        .clone()
}

/// PVA backend for one PV. Currently scalar-Double oriented (M5 minimum).
pub struct EpicsPvaBackend<T: Clone + Send + Sync + 'static> {
    pv: String,
    client: Arc<PvaClient>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Clone + Send + Sync + 'static> EpicsPvaBackend<T> {
    /// Build with a PV name.
    pub fn new(pv: impl Into<String>) -> Self {
        Self {
            pv: pv.into(),
            client: pva_context(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> cirrus_devices::BackendFromPv for EpicsPvaBackend<T> {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
}

// Coerce one scalar to f64. The single source of truth for numeric→f64
// widening, shared by the scalar `pv_field_to_f64` and the array
// `pv_field_to_vec_f64` so both accept the same element types.
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

// Decode a numeric NTScalarArray (`{ value: [...] }`) or a bare scalar array
// into `Vec<f64>`, coercing every element through [`scalar_to_f64`] so any
// numeric element type (Double/Float/Int/Long/…), in either the legacy
// `ScalarArray` or the typed `ScalarArrayTyped` representation, reads
// uniformly. Returns None if the field is not an array or any element is
// non-numeric.
fn pv_field_to_vec_f64(p: &PvField) -> Option<Vec<f64>> {
    match p {
        PvField::ScalarArray(items) => items.iter().map(scalar_to_f64).collect(),
        PvField::ScalarArrayTyped(arr) => {
            arr.to_scalar_values().iter().map(scalar_to_f64).collect()
        }
        PvField::Structure(s) => s
            .fields
            .iter()
            .find(|(name, _)| name == "value")
            .and_then(|(_, f)| pv_field_to_vec_f64(f)),
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

fn pv_field_to_bool(p: &PvField) -> Option<bool> {
    match p {
        PvField::Scalar(ScalarValue::Boolean(b)) => Some(*b),
        _ => pv_field_to_i64(p).map(|i| i != 0),
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
            ScalarValue::String(s) => Some(s.clone()),
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

fn pv_field_to_string(p: &PvField) -> Option<String> {
    match p {
        PvField::Scalar(ScalarValue::String(s)) => Some(s.clone()),
        PvField::Structure(s) => {
            // NTEnum value substructure `{ index, choices }`: decode the
            // selected label. Checked before the NTScalar `value` recursion
            // because an NTEnum's `value` is itself this substructure.
            if let Some(label) = pv_enum_to_label(&s.fields) {
                return Some(label);
            }
            s.fields
                .iter()
                .find(|(name, _)| name == "value")
                .and_then(|(_, f)| pv_field_to_string(f))
        }
        _ => None,
    }
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
// `{ severity: int, ... }`, mapped to the cirrus/ophyd-async convention:
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

#[async_trait]
impl SignalBackend<f64> for EpicsPvaBackend<f64> {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        // PvaClient connects lazily; the search system handles re-tries.
        // pvconnect is the explicit handshake.
        self.client
            .pvconnect(&self.pv)
            .await
            .map(|_| ())
            .map_err(|e| CirrusError::Backend(format!("pva connect {}: {e}", self.pv)))
    }
    async fn put(&self, value: Option<f64>) -> Result<()> {
        let f = PvField::Scalar(ScalarValue::Double(value.unwrap_or_default()));
        self.client
            .pvput_pv_field(&self.pv, &f)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        Ok(make_datakey(
            format!("pva://{source}"),
            Dtype::Number,
            vec![],
            Some("<f8".into()),
            SignalMetadata::default(),
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        let v = pv_field_to_f64(&f)
            .ok_or_else(|| CirrusError::Backend(format!("pva: not numeric: {f:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(v),
            timestamp: now_ts(),
            alarm_severity: pv_field_to_alarm_severity(&f),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<f64> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        pv_field_to_f64(&f).ok_or_else(|| CirrusError::Backend(format!("pva: not numeric: {f:?}")))
    }
    async fn get_setpoint(&self) -> Result<f64> {
        SignalBackend::<f64>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<f64>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let client = self.client.clone();
        let pv = self.pv.clone();
        // Server timestamps live in NTScalar's `timeStamp` substructure and
        // alarm state in `alarm`. Project both explicitly (skipping the larger
        // `display`/`control` fields) so monitor readings carry server time +
        // alarm severity, matching ophyd-async's reading format.
        // Servers publishing bare (non-Normative) scalars simply have no
        // `timeStamp` to send; we detect that on first frame and emit a
        // one-shot WARN per PV so operators can see that local-clock
        // timestamps are being substituted (the fallback is otherwise
        // invisible).
        let request = PvRequestExpr::parse("field(value,alarm,timeStamp)").unwrap_or_default();
        let warned_local_clock = Arc::new(AtomicBool::new(false));
        let pv_for_cb = pv.clone();
        let warned_for_cb = warned_local_clock.clone();
        let handle = tokio::spawn(async move {
            let res = client
                .pvmonitor_with_request(&pv, &request, move |field: &PvField| {
                    if let Some(f) = pv_field_to_f64(field) {
                        let ts = match pv_field_to_ts(field) {
                            Some(t) => t,
                            None => {
                                if !warned_for_cb.swap(true, Ordering::SeqCst) {
                                    tracing::warn!(
                                        target: "cirrus_backend_epics_pva",
                                        "pva {}: monitor frame has no server timeStamp; \
                                         falling back to local clock for this PV (one-shot)",
                                        pv_for_cb,
                                    );
                                }
                                now_ts()
                            }
                        };
                        cb(&f, ts, pv_field_to_alarm_severity(field));
                    }
                })
                .await;
            if let Err(e) = res {
                tracing::error!("pva monitor on {pv}: {e}");
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("pva://{name}")
    }
}

#[async_trait]
impl SignalBackend<Vec<f64>> for EpicsPvaBackend<Vec<f64>> {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        self.client
            .pvconnect(&self.pv)
            .await
            .map(|_| ())
            .map_err(|e| CirrusError::Backend(format!("pva connect {}: {e}", self.pv)))
    }
    async fn put(&self, value: Option<Vec<f64>>) -> Result<()> {
        // Written as a typed Double array; an EPICS waveform record coerces to
        // its FTVL element type server-side.
        let f =
            PvField::ScalarArrayTyped(TypedScalarArray::Double(value.unwrap_or_default().into()));
        self.client
            .pvput_pv_field(&self.pv, &f)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        // The descriptor shape needs the element count; pvget once (describe is
        // rare). Reports the waveform's current length.
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        let len = pv_field_to_vec_f64(&f)
            .ok_or_else(|| CirrusError::Backend(format!("pva: not a numeric array: {f:?}")))?
            .len();
        Ok(make_datakey(
            format!("pva://{source}"),
            Dtype::Number,
            vec![Some(len as u64)],
            Some("<f8".into()),
            SignalMetadata::default(),
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        let v = pv_field_to_vec_f64(&f)
            .ok_or_else(|| CirrusError::Backend(format!("pva: not a numeric array: {f:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(v),
            timestamp: now_ts(),
            alarm_severity: pv_field_to_alarm_severity(&f),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<Vec<f64>> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        pv_field_to_vec_f64(&f)
            .ok_or_else(|| CirrusError::Backend(format!("pva: not a numeric array: {f:?}")))
    }
    async fn get_setpoint(&self) -> Result<Vec<f64>> {
        SignalBackend::<Vec<f64>>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<Vec<f64>>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let client = self.client.clone();
        let pv = self.pv.clone();
        let request = PvRequestExpr::parse("field(value,alarm,timeStamp)").unwrap_or_default();
        let warned_local_clock = Arc::new(AtomicBool::new(false));
        let pv_for_cb = pv.clone();
        let warned_for_cb = warned_local_clock.clone();
        let handle = tokio::spawn(async move {
            let res = client
                .pvmonitor_with_request(&pv, &request, move |field: &PvField| {
                    if let Some(v) = pv_field_to_vec_f64(field) {
                        let ts = match pv_field_to_ts(field) {
                            Some(t) => t,
                            None => {
                                if !warned_for_cb.swap(true, Ordering::SeqCst) {
                                    tracing::warn!(
                                        target: "cirrus_backend_epics_pva",
                                        "pva {}: monitor frame has no server timeStamp; \
                                         falling back to local clock for this PV (one-shot)",
                                        pv_for_cb,
                                    );
                                }
                                now_ts()
                            }
                        };
                        cb(&v, ts, pv_field_to_alarm_severity(field));
                    }
                })
                .await;
            if let Err(e) = res {
                tracing::error!("pva monitor on {pv}: {e}");
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("pva://{name}")
    }
}

#[async_trait]
impl SignalBackend<String> for EpicsPvaBackend<String> {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        self.client
            .pvconnect(&self.pv)
            .await
            .map(|_| ())
            .map_err(|e| CirrusError::Backend(format!("pva connect {}: {e}", self.pv)))
    }
    async fn put(&self, value: Option<String>) -> Result<()> {
        let f = PvField::Scalar(ScalarValue::String(value.unwrap_or_default()));
        self.client
            .pvput_pv_field(&self.pv, &f)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        Ok(make_datakey(
            format!("pva://{source}"),
            Dtype::String,
            vec![],
            Some("|S".into()),
            SignalMetadata::default(),
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        let s = pv_field_to_string(&f)
            .ok_or_else(|| CirrusError::Backend(format!("pva: not string: {f:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(s),
            timestamp: now_ts(),
            alarm_severity: pv_field_to_alarm_severity(&f),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<String> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        pv_field_to_string(&f)
            .ok_or_else(|| CirrusError::Backend(format!("pva: not string: {f:?}")))
    }
    async fn get_setpoint(&self) -> Result<String> {
        SignalBackend::<String>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<String>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let client = self.client.clone();
        let pv = self.pv.clone();
        let request = PvRequestExpr::parse("field(value,alarm,timeStamp)").unwrap_or_default();
        let warned_local_clock = Arc::new(AtomicBool::new(false));
        let pv_for_cb = pv.clone();
        let warned_for_cb = warned_local_clock.clone();
        let handle = tokio::spawn(async move {
            let res = client
                .pvmonitor_with_request(&pv, &request, move |field: &PvField| {
                    if let Some(s) = pv_field_to_string(field) {
                        let ts = pv_field_to_ts(field).unwrap_or_else(|| {
                            if !warned_for_cb.swap(true, Ordering::SeqCst) {
                                tracing::warn!(
                                    target: "cirrus_backend_epics_pva",
                                    "pva {}: monitor frame has no server timeStamp; \
                                     falling back to local clock for this PV (one-shot)",
                                    pv_for_cb,
                                );
                            }
                            now_ts()
                        });
                        cb(&s, ts, pv_field_to_alarm_severity(field));
                    }
                })
                .await;
            if let Err(e) = res {
                tracing::error!("pva monitor on {pv}: {e}");
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("pva://{name}")
    }
}

#[async_trait]
impl SignalBackend<i64> for EpicsPvaBackend<i64> {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        self.client
            .pvconnect(&self.pv)
            .await
            .map(|_| ())
            .map_err(|e| CirrusError::Backend(format!("pva connect {}: {e}", self.pv)))
    }
    async fn put(&self, value: Option<i64>) -> Result<()> {
        let f = PvField::Scalar(ScalarValue::Long(value.unwrap_or_default()));
        self.client
            .pvput_pv_field(&self.pv, &f)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        Ok(make_datakey(
            format!("pva://{source}"),
            Dtype::Integer,
            vec![],
            Some("<i8".into()),
            SignalMetadata::default(),
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        let i = pv_field_to_i64(&f)
            .ok_or_else(|| CirrusError::Backend(format!("pva: not int: {f:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(i),
            timestamp: now_ts(),
            alarm_severity: pv_field_to_alarm_severity(&f),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<i64> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        pv_field_to_i64(&f).ok_or_else(|| CirrusError::Backend(format!("pva: not int: {f:?}")))
    }
    async fn get_setpoint(&self) -> Result<i64> {
        SignalBackend::<i64>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<i64>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let client = self.client.clone();
        let pv = self.pv.clone();
        let request = PvRequestExpr::parse("field(value,alarm,timeStamp)").unwrap_or_default();
        let warned_local_clock = Arc::new(AtomicBool::new(false));
        let pv_for_cb = pv.clone();
        let warned_for_cb = warned_local_clock.clone();
        let handle = tokio::spawn(async move {
            let res = client
                .pvmonitor_with_request(&pv, &request, move |field: &PvField| {
                    if let Some(i) = pv_field_to_i64(field) {
                        let ts = pv_field_to_ts(field).unwrap_or_else(|| {
                            if !warned_for_cb.swap(true, Ordering::SeqCst) {
                                tracing::warn!(
                                    target: "cirrus_backend_epics_pva",
                                    "pva {}: monitor frame has no server timeStamp; \
                                     falling back to local clock for this PV (one-shot)",
                                    pv_for_cb,
                                );
                            }
                            now_ts()
                        });
                        cb(&i, ts, pv_field_to_alarm_severity(field));
                    }
                })
                .await;
            if let Err(e) = res {
                tracing::error!("pva monitor on {pv}: {e}");
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("pva://{name}")
    }
}

#[async_trait]
impl SignalBackend<bool> for EpicsPvaBackend<bool> {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        self.client
            .pvconnect(&self.pv)
            .await
            .map(|_| ())
            .map_err(|e| CirrusError::Backend(format!("pva connect {}: {e}", self.pv)))
    }
    async fn put(&self, value: Option<bool>) -> Result<()> {
        let f = PvField::Scalar(ScalarValue::Boolean(value.unwrap_or_default()));
        self.client
            .pvput_pv_field(&self.pv, &f)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva put: {e}")))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        Ok(make_datakey(
            format!("pva://{source}"),
            Dtype::Boolean,
            vec![],
            Some("|b1".into()),
            SignalMetadata::default(),
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        let b = pv_field_to_bool(&f)
            .ok_or_else(|| CirrusError::Backend(format!("pva: not bool: {f:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(b),
            timestamp: now_ts(),
            alarm_severity: pv_field_to_alarm_severity(&f),
            message: None,
        })
    }
    async fn get_value(&self) -> Result<bool> {
        let f = self
            .client
            .pvget(&self.pv)
            .await
            .map_err(|e| CirrusError::Backend(format!("pva get: {e}")))?;
        pv_field_to_bool(&f).ok_or_else(|| CirrusError::Backend(format!("pva: not bool: {f:?}")))
    }
    async fn get_setpoint(&self) -> Result<bool> {
        SignalBackend::<bool>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<bool>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let client = self.client.clone();
        let pv = self.pv.clone();
        let request = PvRequestExpr::parse("field(value,alarm,timeStamp)").unwrap_or_default();
        let warned_local_clock = Arc::new(AtomicBool::new(false));
        let pv_for_cb = pv.clone();
        let warned_for_cb = warned_local_clock.clone();
        let handle = tokio::spawn(async move {
            let res = client
                .pvmonitor_with_request(&pv, &request, move |field: &PvField| {
                    if let Some(b) = pv_field_to_bool(field) {
                        let ts = pv_field_to_ts(field).unwrap_or_else(|| {
                            if !warned_for_cb.swap(true, Ordering::SeqCst) {
                                tracing::warn!(
                                    target: "cirrus_backend_epics_pva",
                                    "pva {}: monitor frame has no server timeStamp; \
                                     falling back to local clock for this PV (one-shot)",
                                    pv_for_cb,
                                );
                            }
                            now_ts()
                        });
                        cb(&b, ts, pv_field_to_alarm_severity(field));
                    }
                })
                .await;
            if let Err(e) = res {
                tracing::error!("pva monitor on {pv}: {e}");
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("pva://{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use epics_pva_rs::pvdata::PvStructure;

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
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .into(),
            ))
        } else {
            PvField::ScalarArray(
                choices
                    .iter()
                    .map(|s| ScalarValue::String(s.to_string()))
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

    #[test]
    fn nt_enum_decodes_typed_choices_to_label() {
        // Real-wire shape: choices arrive as ScalarArrayTyped(String).
        let f = ntenum(1, &["OFF", "ON"], true);
        assert_eq!(pv_field_to_string(&f), Some("ON".into()));
    }

    #[test]
    fn nt_enum_out_of_range_index_falls_back_to_decimal() {
        // Legacy ScalarArray choices + index past the end → decimal fallback.
        let f = ntenum(5, &["OFF"], false);
        assert_eq!(pv_field_to_string(&f), Some("5".into()));
        // A plain NTScalar string is unaffected by the new enum arm.
        let mut nt = PvStructure::new("epics:nt/NTScalar:1.0");
        nt.fields.push((
            "value".into(),
            PvField::Scalar(ScalarValue::String("hi".into())),
        ));
        assert_eq!(
            pv_field_to_string(&PvField::Structure(nt)),
            Some("hi".into())
        );
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
        let typed = ntscalararray_doubles(&[1.0, 2.5, -3.0], true);
        assert_eq!(pv_field_to_vec_f64(&typed), Some(vec![1.0, 2.5, -3.0]));
        let legacy = ntscalararray_doubles(&[4.0, 5.0], false);
        assert_eq!(pv_field_to_vec_f64(&legacy), Some(vec![4.0, 5.0]));
    }

    #[test]
    fn ntscalararray_coerces_integer_elements_and_rejects_scalar() {
        // A legacy mixed-integer array coerces element-wise to f64.
        let mut nt = PvStructure::new("epics:nt/NTScalarArray:1.0");
        nt.fields.push((
            "value".into(),
            PvField::ScalarArray(vec![
                ScalarValue::Int(7),
                ScalarValue::Long(8),
                ScalarValue::Short(9),
            ]),
        ));
        assert_eq!(
            pv_field_to_vec_f64(&PvField::Structure(nt)),
            Some(vec![7.0, 8.0, 9.0])
        );
        // A plain scalar NTScalar is not a numeric array.
        assert!(pv_field_to_vec_f64(&ntscalar_with_ts(1.0, 0, 0)).is_none());
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
    //   cargo test -p cirrus-backend-epics-pva --features real \
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
}
