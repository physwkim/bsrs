//! Stub backend used when the `real` feature is disabled.

use crate::core::error::{BsrsError, Result};
use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use crate::event_model::DataKey;
use crate::protocols_async::{ReadingValueCallback, SignalBackend};
use async_trait::async_trait;
use serde::Serialize;
use std::time::Duration;

const DISABLED: &str = "epics-ca backend disabled — build with --features real";

/// Stub backend that always errors. Replace by enabling the `real` feature.
pub struct EpicsCaBackend<T: Clone + Send + Sync + 'static> {
    pv: String,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Clone + Send + Sync + 'static> EpicsCaBackend<T> {
    /// Build with a PV name.
    pub fn new(pv: impl Into<String>) -> Self {
        Self {
            pv: pv.into(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> crate::devices::BackendFromPv for EpicsCaBackend<T> {
    fn from_pv(pv: &str) -> Self {
        Self::new(pv)
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + Serialize + 'static> SignalBackend<T> for EpicsCaBackend<T> {
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
        format!("ca://{}", self.pv)
    }
}
