//! Stub PVA backend.

use async_trait::async_trait;
use cirrus_core::error::{CirrusError, Result};
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::SubToken;
use cirrus_event_model::DataKey;
use cirrus_protocols_async::{ReadingValueCallback, SignalBackend};
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

impl<T: Clone + Send + Sync + 'static> cirrus_devices::BackendFromPv for EpicsPvaBackend<T> {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + Serialize + 'static> SignalBackend<T> for EpicsPvaBackend<T> {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        Err(CirrusError::Backend(DISABLED.into()))
    }
    async fn put(&self, _value: Option<T>) -> Result<()> {
        Err(CirrusError::Backend(DISABLED.into()))
    }
    async fn get_datakey(&self, _source: &str) -> Result<DataKey> {
        Err(CirrusError::Backend(DISABLED.into()))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        Err(CirrusError::Backend(DISABLED.into()))
    }
    async fn get_value(&self) -> Result<T> {
        Err(CirrusError::Backend(DISABLED.into()))
    }
    async fn get_setpoint(&self) -> Result<T> {
        Err(CirrusError::Backend(DISABLED.into()))
    }
    fn set_callback(&self, _cb: Option<ReadingValueCallback<T>>) -> SubToken {
        SubToken::noop()
    }
    fn source(&self, _name: &str) -> String {
        format!("pva://{}", self.pv)
    }
}
