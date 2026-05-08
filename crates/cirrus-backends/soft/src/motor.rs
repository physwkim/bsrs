//! `SoftMotor` — single-signal soft device implementing `AsyncMovable`.

use async_trait::async_trait;
use cirrus_core::error::Result;
use cirrus_core::msg::{
    DynLocation, LocatableObj, MovableObj, NamedObj, ReadableObj, StoppableObj,
};
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::Status;
use cirrus_core::Kind;
use cirrus_event_model::{DataKey, Dtype};
use cirrus_protocols_async::{AsyncMovable, AsyncReadable, Locatable, Location, SignalBackend};
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
            .map_err(|_| cirrus_core::error::CirrusError::Backend("soft read".into()))?;
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
        self.backend.put(value, true, None).await
    }
}

#[async_trait]
impl Locatable<f64> for SoftMotor {
    async fn locate(&self) -> Result<Location<f64>> {
        Ok(Location {
            setpoint: cirrus_protocols_async::SignalBackend::get_setpoint(self.backend.as_ref())
                .await?,
            readback: cirrus_protocols_async::SignalBackend::get_value(self.backend.as_ref())
                .await?,
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
