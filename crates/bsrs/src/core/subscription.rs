//! `Subscription` — RAII bundle of a `watch::Receiver<ReadingValue>`, the data
//! key those readings belong to, and the `SubToken` that keeps the backend
//! slot alive (rule **K2**).

use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use tokio::sync::watch;

/// Live subscription to one signal. Drop releases the backend slot.
///
/// `key` names the reading the way the device's `describe()` / `read()` do
/// (ophyd-async `subscribe_reading` delivers `{signal.name: reading}`), so a
/// consumer that turns readings into Events needs no mapping of its own.
pub struct Subscription {
    rx: watch::Receiver<ReadingValue>,
    key: String,
    _token: SubToken,
}

impl Subscription {
    /// Build from parts. `key` is the data key the readings belong to; the
    /// `token` is held until `self` is dropped.
    pub fn new(rx: watch::Receiver<ReadingValue>, token: SubToken, key: impl Into<String>) -> Self {
        Self {
            rx,
            key: key.into(),
            _token: token,
        }
    }
    /// Data key of the readings this subscription streams.
    pub fn key(&self) -> &str {
        &self.key
    }
    /// Borrow the receiver.
    pub fn rx(&self) -> &watch::Receiver<ReadingValue> {
        &self.rx
    }
    /// Borrow the receiver mutably.
    pub fn rx_mut(&mut self) -> &mut watch::Receiver<ReadingValue> {
        &mut self.rx
    }
    /// Clone the receiver. The clone observes the same channel; cancelling the
    /// original `Subscription` also stops the clone (rx returns `Err`).
    pub fn clone_rx(&self) -> watch::Receiver<ReadingValue> {
        self.rx.clone()
    }
}
