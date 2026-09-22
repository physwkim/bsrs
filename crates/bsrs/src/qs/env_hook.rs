//! `EnvironmentHook` — what the daemon puts on the engine that
//! `environment_open` has just created.
//!
//! The engine is born in `environment_open` and dies with
//! `environment_close`, so everything installed on it — suspenders,
//! subscriptions, metadata — dies with it. A client cannot repair that
//! by re-installing after each open: it is not told the open happened
//! (a console or another client can open one), and a queued plan can
//! start before it finds out. Whatever must hold for *every* open
//! environment is therefore installed by the open itself, through this
//! hook, and the daemon that owns the beamline is the one that
//! supplies it.
//!
//! [`crate::engine::CheckpointHook`] is the same idea for the one hook
//! the engine itself owns; this is the general form.

use std::sync::Arc;

use async_trait::async_trait;

use crate::core::error::Result;
use crate::engine::RunEngine;

/// Prepares each new environment's engine.
#[async_trait]
pub trait EnvironmentHook: Send + Sync {
    /// Called with the engine `environment_open` created, before that
    /// engine becomes the open environment.
    ///
    /// An `Err` fails the `environment_open` request and leaves the
    /// environment closed — an engine that skipped the hook never
    /// becomes the environment, so what the hook installs is an
    /// invariant of every open environment and not something that may
    /// or may not have made it.
    ///
    /// The engine arrives as an argument rather than being read from
    /// the engine slot because the open holds that slot's lock across
    /// this call: a hook that locks the slot deadlocks the daemon.
    async fn environment_opened(&self, re: &Arc<RunEngine>) -> Result<()>;
}
