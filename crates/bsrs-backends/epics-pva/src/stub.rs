//! Stub PVA backend.

use async_trait::async_trait;
use bsrs_core::error::{BsrsError, Result};
use bsrs_core::reading::ReadingValue;
use bsrs_core::status::SubToken;
use bsrs_event_model::DataKey;
use bsrs_protocols_async::{ReadingValueCallback, SignalBackend};
use serde::Serialize;
use std::time::Duration;

const DISABLED: &str = "epics-pva backend disabled — build with --features real";

/// Stub PVA backend.
pub struct EpicsPvaBackend<T: Clone + Send + Sync + 'static> {
    pv: String,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Clone + Send + Sync + 'static> EpicsPvaBackend<T> {
    /// Build with a PV name.
    pub fn new(pv: impl Into<String>) -> Self {
        Self {
            pv: pv.into(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> bsrs_devices::BackendFromPv for EpicsPvaBackend<T> {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + Serialize + 'static> SignalBackend<T> for EpicsPvaBackend<T> {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        Err(BsrsError::Backend(DISABLED.into()))
    }
    async fn put(&self, _value: Option<T>) -> Result<()> {
        Err(BsrsError::Backend(DISABLED.into()))
    }
    async fn get_datakey(&self, _source: &str) -> Result<DataKey> {
        Err(BsrsError::Backend(DISABLED.into()))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        Err(BsrsError::Backend(DISABLED.into()))
    }
    async fn get_value(&self) -> Result<T> {
        Err(BsrsError::Backend(DISABLED.into()))
    }
    async fn get_setpoint(&self) -> Result<T> {
        Err(BsrsError::Backend(DISABLED.into()))
    }
    fn set_callback(&self, _cb: Option<ReadingValueCallback<T>>) -> SubToken {
        SubToken::noop()
    }
    fn source(&self, _name: &str, _read: bool) -> String {
        format!("pva://{}", self.pv)
    }
}
