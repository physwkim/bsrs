//! `Subscription` — RAII bundle of a `watch::Receiver<ReadingValue>` and the
//! `SubToken` that keeps the backend slot alive (rule **K2**).

use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use tokio::sync::watch;

/// Live subscription. Drop releases the backend slot.
pub struct Subscription {
    rx: watch::Receiver<ReadingValue>,
    _token: SubToken,
}

impl Subscription {
    /// Build from parts. The `token` is held until `self` is dropped.
    pub fn new(rx: watch::Receiver<ReadingValue>, token: SubToken) -> Self {
        Self { rx, _token: token }
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
