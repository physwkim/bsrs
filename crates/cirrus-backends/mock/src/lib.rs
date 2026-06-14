//! Mock backend — single-fixed-value backend used in unit tests.

#![deny(missing_docs)]

use async_trait::async_trait;
use cirrus_core::error::Result;
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::SubToken;
use cirrus_event_model::{DataKey, Dtype};
use cirrus_protocols_async::{ReadingValueCallback, SignalBackend};
use serde::Serialize;
use std::time::Duration;

/// Mock backend that returns a fixed value forever.
pub struct MockBackend<T: Clone + Send + Sync + 'static> {
    value: T,
}

impl<T: Clone + Default + Send + Sync + 'static> cirrus_devices::BackendFromPv for MockBackend<T> {
    fn from_pv(_pv: &str) -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone + Send + Sync + 'static> MockBackend<T> {
    /// Build with a fixed value.
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + Serialize + 'static> SignalBackend<T> for MockBackend<T> {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        Ok(())
    }
    async fn put(&self, _value: Option<T>) -> Result<()> {
        Ok(())
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        Ok(DataKey {
            source: format!("mock://{source}"),
            dtype: Dtype::Number,
            shape: vec![],
            dtype_numpy: None,
            external: None,
            units: None,
            precision: None,
            object_name: None,
            dims: None,
            limits: None,
            choices: None,
        })
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        Ok(ReadingValue {
            value: serde_json::to_value(&self.value)?,
            timestamp: 0.0,
            alarm_severity: None,
            message: None,
        })
    }
    async fn get_value(&self) -> Result<T> {
        Ok(self.value.clone())
    }
    async fn get_setpoint(&self) -> Result<T> {
        Ok(self.value.clone())
    }
    fn set_callback(&self, _cb: Option<ReadingValueCallback<T>>) -> SubToken {
        SubToken::noop()
    }
    fn source(&self, name: &str) -> String {
        format!("mock://{name}")
    }
}
