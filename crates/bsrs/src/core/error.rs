//! bsrs error type.

/// Catch-all error for bsrs operations.
#[derive(Debug, thiserror::Error)]
pub enum BsrsError {
    /// I/O or backend connection error.
    #[error("backend error: {0}")]
    Backend(String),
    /// Operation timed out.
    #[error("timeout after {0:?}")]
    Timeout(std::time::Duration),
    /// Cancellation was requested.
    #[error("cancelled")]
    Cancelled,
    /// A `stop`/`abort` request the plan's contingency regions have finished
    /// unwinding: what a [`Msg::Raise`](crate::core::Msg::Raise) of a
    /// `Thrown::Interrupt` returns to the run loop, which then throws it into
    /// the next enclosing region or ends the run with it.
    #[error("interrupted by {0}")]
    Interrupted(Interrupt),
    /// A signal value was rejected by `check_value`.
    #[error("invalid value: {0}")]
    InvalidValue(String),
    /// A device was used in the wrong state (e.g. read before stage).
    #[error("device state error: {0}")]
    State(String),
    /// A plan emitted a message that the engine could not satisfy.
    #[error("plan logic error: {0}")]
    Plan(String),
    /// Wrapped status error.
    #[error("status: {0}")]
    Status(#[from] crate::core::status::StatusError),
    /// Wrapped event-model error.
    #[error("event-model: {0}")]
    EventModel(#[from] crate::event_model::EventModelError),
    /// JSON encode/decode failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Generic miscellaneous error.
    #[error("{0}")]
    Other(String),
}

/// Type alias for bsrs results.
pub type Result<T, E = BsrsError> = std::result::Result<T, E>;

/// An interrupt the engine throws into the running plan so its contingency
/// regions unwind — bluesky's `RequestStop` / `RequestAbort`. `halt` is not
/// one: it drops the plan without unwinding (`PlanHalt` is a `GeneratorExit`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interrupt {
    /// `RunEngine::stop`: the run closes `success`.
    Stop,
    /// `RunEngine::abort`: the run closes `abort`.
    Abort,
}

impl Interrupt {
    /// The `RunStop` exit status this interrupt closes a run with.
    pub fn exit_status(self) -> &'static str {
        match self {
            Interrupt::Stop => "success",
            Interrupt::Abort => "abort",
        }
    }
}

impl std::fmt::Display for Interrupt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Interrupt::Stop => "stop",
            Interrupt::Abort => "abort",
        })
    }
}

impl From<&str> for BsrsError {
    fn from(s: &str) -> Self {
        BsrsError::Other(s.to_string())
    }
}

impl From<String> for BsrsError {
    fn from(s: String) -> Self {
        BsrsError::Other(s)
    }
}
