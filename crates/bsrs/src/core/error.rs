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
    /// [`wait_for_value`](crate::devices::wait_for_value), or an areaDetector
    /// readback waited on after a put, timed out; the message is ophyd-async
    /// `wait_for_value`'s `TimeoutError` text.
    #[error(
        "{name} didn't match {matcher} in {}s, last value {}",
        .timeout.as_secs_f64(),
        crate::devices::signal::repr(.last)
    )]
    ValueTimeout {
        /// Subscription key (the signal name).
        name: String,
        /// The [`Matcher::name`](crate::devices::Matcher::name): a value's
        /// `repr`, a predicate's fn name, or `<closure>`.
        matcher: String,
        /// The timeout that elapsed.
        timeout: std::time::Duration,
        /// The last value the subscription delivered, `None` if none came
        /// (boxed to keep `BsrsError` small).
        last: Option<Box<serde_json::Value>>,
    },
    /// A signal's backend connect failed; every caller that joined that
    /// connect gets this same error, as ophyd-async callers share one
    /// `_connect_task`.
    #[error(transparent)]
    Connect(std::sync::Arc<BsrsError>),
    /// A connect failed: ophyd-async `NotConnectedError`.
    #[error("not connected:{}", NotConnectedText(.0, ""))]
    NotConnected(NotConnected),
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
    /// A PV cannot be read as the signal's datatype: ophyd-async
    /// `make_converter`'s `TypeError` at connect.
    #[error("{0}")]
    Datatype(String),
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

/// What did not connect, as ophyd-async `NotConnectedError` built from a
/// `str` or from a `dict` of `sub_errors`.
#[derive(Debug)]
pub enum NotConnected {
    /// A signal whose PV never answered, by its source (`ca://PV`).
    Source(String),
    /// Every child of a device that failed to connect, by child name, in
    /// connect order. A child that is itself a device nests its own
    /// [`Children`](NotConnected::Children).
    Children(Vec<(String, BsrsError)>),
}

impl BsrsError {
    /// The not-connected failure for `source` (`ca://PV`), with `why` logged
    /// at debug, as aioca and p4p log a connect timeout before raising
    /// `NotConnectedError(source)`: what a backend's `connect` returns when
    /// its PV did not answer.
    pub fn not_connected(source: String, why: impl std::fmt::Display) -> Self {
        tracing::debug!("signal {source} not connected: {why}");
        BsrsError::NotConnected(NotConnected::Source(source))
    }

    /// The [`NotConnected`] this error is, seen through a shared
    /// [`Connect`](BsrsError::Connect).
    pub(crate) fn as_not_connected(&self) -> Option<&NotConnected> {
        match self {
            BsrsError::NotConnected(nc) => Some(nc),
            BsrsError::Connect(inner) => inner.as_not_connected(),
            _ => None,
        }
    }
}

/// [`NotConnected`] as ophyd-async `format_error_string`: a source after a
/// space, or the children one per line at `indent`, a nested device's own
/// children four spaces further in.
struct NotConnectedText<'a>(&'a NotConnected, &'a str);

impl std::fmt::Display for NotConnectedText<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let NotConnectedText(nc, indent) = *self;
        let children = match nc {
            NotConnected::Source(source) => return write!(f, " {source}"),
            NotConnected::Children(children) => children,
        };
        for (name, err) in children {
            match err.as_not_connected() {
                Some(sub @ NotConnected::Children(_)) => {
                    write!(f, "\n{indent}{name}:")?;
                    let deeper = format!("{indent}    ");
                    NotConnectedText(sub, &deeper).fmt(f)?;
                }
                _ => write!(f, "\n{indent}{name}: {err}")?,
            }
        }
        Ok(())
    }
}

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

#[cfg(test)]
mod tests {
    use super::BsrsError::{self, InvalidValue};
    use super::NotConnected::{self, Children};

    fn child(name: &str, err: BsrsError) -> (String, BsrsError) {
        (name.to_string(), err)
    }

    fn not_connected(nc: NotConnected) -> BsrsError {
        BsrsError::NotConnected(nc)
    }

    fn timeout_signal() -> (String, BsrsError) {
        child(
            "timeout_signal",
            BsrsError::not_connected("ca://A_NON_EXISTENT_SIGNAL".into(), "test"),
        )
    }

    /// ophyd-async test_utils::test_not_connected_error_output and the
    /// `init_devices` case of test_combining_top_level_signal_and_child_device:
    /// one line per failed child, each nested device four spaces further in.
    #[test]
    fn not_connected_lists_every_failed_child_nested_by_device() {
        let err = not_connected(Children(vec![
            child(
                "timeout_child_device_ca",
                not_connected(Children(vec![timeout_signal()])),
            ),
            child(
                "dummy_device2",
                not_connected(Children(vec![
                    timeout_signal(),
                    child(
                        "sub_device",
                        not_connected(Children(vec![child(
                            "value_error_signal",
                            InvalidValue("Some ValueError text".into()),
                        )])),
                    ),
                ])),
            ),
        ]));
        assert_eq!(
            err.to_string(),
            "not connected:\n\
             timeout_child_device_ca:\n    \
             timeout_signal: not connected: ca://A_NON_EXISTENT_SIGNAL\n\
             dummy_device2:\n    \
             timeout_signal: not connected: ca://A_NON_EXISTENT_SIGNAL\n    \
             sub_device:\n        \
             value_error_signal: invalid value: Some ValueError text"
        );
    }
}
