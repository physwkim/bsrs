//! ophyd-style sync trait family — blanket impls over the async traits.
//!
//! Users who pull `use bsrs::ophyd::*` into scope get sync method names that
//! drive the bsrs runtime via `block_on`. **Do not call from inside an async
//! task.**

#![deny(missing_docs)]

use crate::core::{
    error::Result, reading::ReadingValue, runtime::block_on, status::Status, ConfigureArgs,
};
use crate::event_model::DataKey;
use std::collections::HashMap;

use crate::protocols_async as a;

/// Sync analog of `AsyncReadable`.
pub trait Readable: a::AsyncReadable {
    /// Read all signals (blocking).
    fn read_blocking(&self) -> Result<HashMap<String, ReadingValue>> {
        block_on(self.read())
    }
    /// Describe (blocking).
    fn describe_blocking(&self) -> Result<HashMap<String, DataKey>> {
        block_on(self.describe())
    }
}
impl<T: a::AsyncReadable + ?Sized> Readable for T {}

/// Sync analog of `AsyncMovable<T>`.
pub trait Movable<T = f64>: a::AsyncMovable<T> {
    /// Set (returns `Status` immediately; use `.wait()` for sync completion).
    fn set_blocking(&self, value: T) -> Status {
        block_on(self.set(value))
    }
}
impl<T, U: a::AsyncMovable<T> + ?Sized> Movable<T> for U {}

/// Sync analog of `Triggerable`.
pub trait TriggerableSync: a::Triggerable {
    /// Trigger (returns `Status` immediately).
    fn trigger_blocking(&self) -> Status {
        block_on(self.trigger())
    }
}
impl<T: a::Triggerable + ?Sized> TriggerableSync for T {}

/// Sync analog of `Stageable`.
pub trait StageableSync: a::Stageable {
    /// Stage (blocking).
    fn stage_blocking(&self) -> Result<()> {
        block_on(self.stage())
    }
    /// Unstage (blocking).
    fn unstage_blocking(&self) -> Result<()> {
        block_on(self.unstage())
    }
}
impl<T: a::Stageable + ?Sized> StageableSync for T {}

/// Sync analog of `Flyable`.
pub trait FlyableSync: a::Flyable {
    /// Kickoff (returns `Status`).
    fn kickoff_blocking(&self) -> Status {
        block_on(self.kickoff())
    }
    /// Complete (returns `Status`).
    fn complete_blocking(&self) -> Status {
        block_on(self.complete())
    }
}
impl<T: a::Flyable + ?Sized> FlyableSync for T {}

/// Sync analog of `AsyncConfigurable`.
pub trait Configurable: a::AsyncConfigurable {
    /// Read configuration (blocking).
    fn read_configuration_blocking(&self) -> Result<HashMap<String, ReadingValue>> {
        block_on(self.read_configuration())
    }
    /// Describe configuration (blocking).
    fn describe_configuration_blocking(&self) -> Result<HashMap<String, DataKey>> {
        block_on(self.describe_configuration())
    }
    /// Apply (blocking).
    fn configure_blocking(&self, args: ConfigureArgs) -> Result<()> {
        block_on(self.configure(args))
    }
}
impl<T: a::AsyncConfigurable + ?Sized> Configurable for T {}
