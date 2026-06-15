//! `SoftMotor` — single-signal soft device implementing `AsyncMovable`.

use async_trait::async_trait;
use bsrs_core::error::Result;
use bsrs_core::msg::{DynLocation, LocatableObj, MovableObj, NamedObj, ReadableObj, StoppableObj};
use bsrs_core::reading::ReadingValue;
use bsrs_core::status::{Status, StatusError};
use bsrs_core::Kind;
use bsrs_event_model::{DataKey, Dtype};
use bsrs_protocols_async::{AsyncMovable, AsyncReadable, Locatable, Location, SignalBackend};
use std::collections::HashMap;
use std::sync::Arc;

use crate::signal::SoftSignalBackend;

/// Single-signal motor backed by a `SoftSignalBackend<f64>`.
pub struct SoftMotor {
    name: String,
    backend: Arc<SoftSignalBackend<f64>>,
    units: Option<String>,
    kind: Kind,
}

impl SoftMotor {
    /// Build a soft motor with `initial_pos` at `0.0` if `None`.
    pub fn new(name: impl Into<String>, initial_pos: Option<f64>) -> Self {
        Self {
            name: name.into(),
            backend: Arc::new(
                SoftSignalBackend::new(initial_pos.unwrap_or(0.0), Dtype::Number)
                    .with_dtype_numpy("<f8")
                    .with_units("mm"),
            ),
            units: Some("mm".into()),
            kind: Kind::Hinted,
        }
    }

    /// Read the current readback.
    pub async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        let r = self
            .backend
            .get_reading()
            .await
            .map_err(|_| bsrs_core::error::BsrsError::Backend("soft read".into()))?;
        let mut out = HashMap::new();
        out.insert(self.name.clone(), r);
        Ok(out)
    }

    /// Describe.
    pub async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        let mut dk = self.backend.get_datakey(&self.name).await?;
        dk.units = self.units.clone();
        let mut out = HashMap::new();
        out.insert(self.name.clone(), dk);
        Ok(out)
    }
}

#[async_trait]
impl NamedObj for SoftMotor {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "SoftMotor",
            "setpoint": self.backend.current_setpoint(),
            "readback": self.backend.current_value(),
            "units": self.units,
            "kind": format!("{:?}", self.kind),
            "subscribers": self.backend.subscriber_count(),
            "connected": true,
        })
    }
}

#[async_trait]
impl AsyncReadable for SoftMotor {
    fn name(&self) -> &str {
        &self.name
    }
    async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        self.read().await
    }
    async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        self.describe().await
    }
}

#[async_trait]
impl AsyncMovable<f64> for SoftMotor {
    fn name(&self) -> &str {
        &self.name
    }
    async fn set(&self, value: f64) -> Status {
        match self.backend.put(Some(value)).await {
            Ok(()) => Status::done(),
            Err(e) => Status::fail(StatusError::Failed(e.to_string())),
        }
    }
}

#[async_trait]
impl Locatable<f64> for SoftMotor {
    async fn locate(&self) -> Result<Location<f64>> {
        Ok(Location {
            setpoint: bsrs_protocols_async::SignalBackend::get_setpoint(self.backend.as_ref())
                .await?,
            readback: bsrs_protocols_async::SignalBackend::get_value(self.backend.as_ref()).await?,
        })
    }
}

#[async_trait]
impl ReadableObj for SoftMotor {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        self.read().await
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        self.describe().await
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        if matches!(self.kind, Kind::Hinted) {
            Some(vec![self.name.clone()])
        } else {
            None
        }
    }
}

#[async_trait]
impl MovableObj for SoftMotor {
    async fn set_dyn(&self, value: f64) -> Status {
        self.set(value).await
    }
    async fn stop_on_pause(&self, success: bool) -> Result<()> {
        StoppableObj::stop_dyn(self, success).await
    }
}

#[async_trait]
impl LocatableObj for SoftMotor {
    async fn locate_dyn(&self) -> Result<DynLocation> {
        let setpoint = SignalBackend::get_setpoint(self.backend.as_ref()).await?;
        let readback = SignalBackend::get_value(self.backend.as_ref()).await?;
        Ok(DynLocation { setpoint, readback })
    }
}

#[async_trait]
impl StoppableObj for SoftMotor {
    async fn stop_dyn(&self, _success: bool) -> Result<()> {
        // Soft motor: no motion in flight to halt; just record the call.
        // Real implementations would write to a STOP PV.
        Ok(())
    }
}
