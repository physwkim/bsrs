//! Bluesky-style short-name extensions for the cirrus role traits.
//!
//! The role traits in [`crate::msg`] use `_dyn` suffixes (`read_dyn`,
//! `set_dyn`, `locate_dyn`, …) so the trait method is unambiguously the
//! object-safe version. Users writing plans against trait objects
//! (`Arc<dyn ReadableObj>`) prefer bluesky's short names. These
//! extension traits add `read`, `set`, `position`, `trigger`, etc.,
//! routed through the `_dyn` methods.
//!
//! Blanket impl for `T: <Role> + ?Sized` so `Arc<dyn Role>` AND
//! `&SomeConcreteType` both pick up the new methods. Concrete types
//! that already define the same method via another trait
//! (`AsyncReadable`, `AsyncMovable`) will need disambiguation only if
//! BOTH traits are imported in the same scope; the cirrus prelude
//! intentionally re-exports only these `Ext` traits.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::error::Result;
use crate::msg::{
    DynLocation, FlyableObj, LocatableObj, MovableObj, MonitorableObj, ReadableObj, StageableObj,
    StoppableObj, TriggerableObj,
};
use crate::reading::ReadingValue;
use crate::status::Status;
use crate::subscription::Subscription;
use cirrus_event_model::DataKey;

/// Bluesky-style methods for any `ReadableObj`.
#[async_trait]
pub trait ReadableExt: ReadableObj {
    /// Read the device. Equivalent to bluesky's `dev.read()`.
    async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        self.read_dyn().await
    }
    /// Describe the device's data keys. Equivalent to `dev.describe()`.
    async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        self.describe_dyn().await
    }
}
impl<T: ReadableObj + ?Sized> ReadableExt for T {}

/// Bluesky-style methods for any `MovableObj`.
#[async_trait]
pub trait MovableExt: MovableObj {
    /// Issue a move. Returns a `Status` you can `.await` for completion.
    /// Equivalent to bluesky's `motor.set(value)` returning a Status.
    async fn set(&self, value: f64) -> Status {
        self.set_dyn(value).await
    }
    /// Convenience: `set` + await the Status to completion.
    async fn move_to(&self, value: f64) -> Result<()> {
        let s = self.set_dyn(value).await;
        s.await.map_err(|e| {
            crate::error::CirrusError::Backend(format!("move failed: {e:?}"))
        })
    }
}
impl<T: MovableObj + ?Sized> MovableExt for T {}

/// Bluesky-style methods for any `LocatableObj`.
#[async_trait]
pub trait LocatableExt: LocatableObj {
    /// Read setpoint + readback. Equivalent to `motor.locate()`.
    async fn locate(&self) -> Result<DynLocation> {
        self.locate_dyn().await
    }
    /// Just the readback. Equivalent to bluesky's `motor.position`.
    async fn position(&self) -> Result<f64> {
        Ok(self.locate_dyn().await?.readback)
    }
    /// Just the setpoint. Bluesky has no direct counterpart, but this
    /// is symmetric with `position`.
    async fn target(&self) -> Result<f64> {
        Ok(self.locate_dyn().await?.setpoint)
    }
}
impl<T: LocatableObj + ?Sized> LocatableExt for T {}

/// Bluesky-style methods for any `TriggerableObj`.
#[async_trait]
pub trait TriggerableExt: TriggerableObj {
    /// Equivalent to bluesky's `det.trigger()`.
    async fn trigger(&self) -> Status {
        self.trigger_dyn().await
    }
}
impl<T: TriggerableObj + ?Sized> TriggerableExt for T {}

/// Bluesky-style methods for any `StoppableObj`.
#[async_trait]
pub trait StoppableExt: StoppableObj {
    /// Stop with `success=true` (planned stop). Equivalent to
    /// `dev.stop()` in bluesky.
    async fn stop(&self) -> Result<()> {
        self.stop_dyn(true).await
    }
    /// Emergency stop (`success=false`). Mirrors
    /// `bluesky.protocols.Stoppable` failure semantics.
    async fn stop_emergency(&self) -> Result<()> {
        self.stop_dyn(false).await
    }
}
impl<T: StoppableObj + ?Sized> StoppableExt for T {}

/// Bluesky-style methods for any `StageableObj`.
#[async_trait]
pub trait StageableExt: StageableObj {
    /// Stage. Equivalent to `dev.stage()`.
    async fn stage(&self) -> Result<()> {
        self.stage_dyn().await
    }
    /// Unstage. Equivalent to `dev.unstage()`.
    async fn unstage(&self) -> Result<()> {
        self.unstage_dyn().await
    }
}
impl<T: StageableObj + ?Sized> StageableExt for T {}

/// Bluesky-style methods for any `MonitorableObj`.
#[async_trait]
pub trait MonitorableExt: MonitorableObj {
    /// Subscribe to the monitor stream. Equivalent to bluesky's
    /// `dev.subscribe()`.
    async fn subscribe(&self) -> Result<Subscription> {
        self.subscribe_dyn().await
    }
}
impl<T: MonitorableObj + ?Sized> MonitorableExt for T {}

/// Bluesky-style methods for any `FlyableObj`.
#[async_trait]
pub trait FlyableExt: FlyableObj {
    /// Begin acquisition. Equivalent to bluesky's `dev.kickoff()`.
    async fn kickoff(&self) -> Status {
        self.kickoff_dyn().await
    }
    /// Wait for completion. Equivalent to bluesky's `dev.complete()`.
    async fn complete(&self) -> Status {
        self.complete_dyn().await
    }
}
impl<T: FlyableObj + ?Sized> FlyableExt for T {}

