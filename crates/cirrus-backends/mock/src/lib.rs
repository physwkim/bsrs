//! Mock backends for unit tests: the trivial fixed-value [`MockBackend`] and
//! the ophyd-async-style [`MockSignalBackend`] (records puts, put callback,
//! `put_proceeds` gate).

#![deny(missing_docs)]

pub mod mock_signal;
pub use mock_signal::{MockPutCallback, MockSignalBackend, PutsBlockedGuard};

use async_trait::async_trait;
use cirrus_core::error::Result;
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::SubToken;
use cirrus_event_model::{make_datakey, DataKey, Dtype, SignalMetadata};
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
        Ok(make_datakey(
            format!("mock://{source}"),
            Dtype::Number,
            vec![],
            None,
            SignalMetadata::default(),
        ))
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
