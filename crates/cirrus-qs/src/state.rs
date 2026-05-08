//! Engine / queue state machine.

use serde::{Deserialize, Serialize};

/// Public engine state — values match `bluesky_queueserver.manager.worker.EState`
/// where they overlap so qserver CLI displays them naturally.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EState {
    /// Engine not yet opened.
    EnvironmentClosed,
    /// Engine open, no plan running.
    Idle,
    /// A plan is executing.
    ExecutingQueue,
    /// Engine paused.
    Paused,
    /// Abort requested, plan winding down.
    Aborting,
}

impl EState {
    /// Stringified for status JSON.
    pub fn as_str(&self) -> &'static str {
        match self {
            EState::EnvironmentClosed => "environment_closed",
            EState::Idle => "idle",
            EState::ExecutingQueue => "executing_queue",
            EState::Paused => "paused",
            EState::Aborting => "aborting",
        }
    }
}

/// Snapshot of the engine state shared across handlers.
#[derive(Clone, Debug, Default)]
pub struct EngineState {
    /// Current state.
    pub state: Option<EState>,
    /// UID of the current run, if any.
    pub current_run_uid: Option<String>,
    /// Plan name currently running.
    pub current_plan_name: Option<String>,
    /// Pending queue length.
    pub queue_len: usize,
    /// Total plans run this session.
    pub plans_run: u64,
    /// Total plans failed.
    pub plans_failed: u64,
}

impl EngineState {
    /// Build the initial state (engine closed).
    pub fn initial() -> Self {
        Self {
            state: Some(EState::EnvironmentClosed),
            ..Default::default()
        }
    }
}
