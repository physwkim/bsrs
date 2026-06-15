//! Typed `Reading` — a value + timestamp + optional alarm/severity.

use serde::{Deserialize, Serialize};

/// One reading of one field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TypedReading<T> {
    /// The value.
    pub value: T,
    /// Unix epoch timestamp in seconds.
    pub timestamp: f64,
    /// Optional alarm severity (0 = ok).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub alarm_severity: Option<i32>,
    /// Optional alarm message.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub message: Option<String>,
}

impl<T> TypedReading<T> {
    /// Construct a `TypedReading` with no alarm.
    pub fn new(value: T, timestamp: f64) -> Self {
        Self {
            value,
            timestamp,
            alarm_severity: None,
            message: None,
        }
    }
}

/// Type-erased reading carrying a `serde_json::Value`. The bundler stores these.
pub type ReadingValue = TypedReading<serde_json::Value>;

/// Concrete shorthand.
pub type ReadingF64 = TypedReading<f64>;

impl<T: Serialize> TypedReading<T> {
    /// Erase the type to a JSON-valued reading suitable for the bundler.
    pub fn into_value(self) -> Result<ReadingValue, serde_json::Error> {
        Ok(ReadingValue {
            value: serde_json::to_value(self.value)?,
            timestamp: self.timestamp,
            alarm_severity: self.alarm_severity,
            message: self.message,
        })
    }
}
