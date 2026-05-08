//! Real PVA backend wired to `epics-pva-rs::PvaClient`.

use async_trait::async_trait;
use cirrus_core::error::{CirrusError, Result};
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::{Status, StatusError, SubToken};
use cirrus_event_model::{DataKey, Dtype};
use cirrus_protocols_async::{ReadingValueCallback, SignalBackend};
use epics_pva_rs::client::PvaClient;
use epics_pva_rs::{PvField, ScalarValue};
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

fn pv_field_to_f64(p: &PvField) -> Option<f64> {
    match p {
        PvField::Scalar(s) => match s {
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
        },
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
    async fn put(&self, value: f64, _wait: bool, _timeout: Option<Duration>) -> Status {
        let f = PvField::Scalar(ScalarValue::Double(value));
        match self.client.pvput_pv_field(&self.pv, &f).await {
            Ok(()) => Status::done(),
            Err(e) => Status::fail(StatusError::Failed(format!("pva put: {e}"))),
        }
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        Ok(DataKey {
            source: format!("pva://{source}"),
            dtype: Dtype::Number,
            shape: vec![],
            dtype_numpy: Some("<f8".into()),
            external: None,
            units: None,
            precision: None,
            object_name: None,
            dims: None,
            limits: None,
        })
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
            alarm_severity: None,
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
    fn set_callback(&self, _cb: Option<ReadingValueCallback<f64>>) -> SubToken {
        // Full PVA monitor wiring lives in cirrus-stream::sources::pva_mon
        // for the bulk-data path. Scalar monitor here is left as a TODO so
        // the backend stays focused on point-in-time get/put.
        SubToken::noop()
    }
    fn source(&self, name: &str) -> String {
        format!("pva://{name}")
    }
}
