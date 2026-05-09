//! Engine-side `Suspender` registry + reference impls.
//!
//! Reference: bluesky `run_engine.py:1132-1310` (`install_suspender`,
//! `request_suspend`, `_start_suspender`) and `bluesky/suspenders.py`.
//!
//! Two layers live here:
//!
//! - The internal [`SuspenderHandle`] used by the engine's
//!   `Msg::InstallSuspender` path. It wraps an opaque `Arc<dyn Suspender>`
//!   plus the spawned watcher task; drop aborts the task (rule **K1**).
//! - User-facing impls — [`SuspendBoolHigh`], [`SuspendBoolLow`],
//!   [`SuspendThreshold`]. These wire a `tokio::sync::watch::Receiver`
//!   to the engine's `suspend_until_with` API: when the watched signal
//!   enters the "bad" region the engine pauses; when it returns to
//!   "good" the engine auto-resumes. Each impl exposes an
//!   [`install`](SuspendBoolHigh::install) method that spawns the
//!   monitor task and returns an [`tokio::task::AbortHandle`] — drop
//!   it to detach the suspender.

pub use cirrus_core::suspender::Suspender;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

use crate::engine::RunEngine;

/// Boxed pre/post plan injection. `None` = nothing to inject.
pub type SuspendInjection = Option<crate::engine::SuspendCallback>;

/// Live registration record. Drop aborts the watcher task (rule **K1**).
pub(crate) struct SuspenderHandle {
    /// Stable id used by `RemoveSuspender` Msg.
    #[allow(dead_code)]
    pub(crate) id: u64,
    /// Underlying suspender (kept alive while the registration exists).
    #[allow(dead_code)]
    pub(crate) inner: Arc<dyn Suspender>,
    /// The watcher task — drop / abort on Drop.
    pub(crate) abort: AbortHandle,
}

impl SuspenderHandle {
    pub(crate) fn new(id: u64, inner: Arc<dyn Suspender>, handle: JoinHandle<()>) -> Self {
        let abort = handle.abort_handle();
        Self { id, inner, abort }
    }
}

impl Drop for SuspenderHandle {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

// -- User-facing reference impls --------------------------------------------

const RECONNECT_BACKOFF: Duration = Duration::from_millis(50);

/// Pause the engine when a watched `bool` signal goes **high** (`true`),
/// auto-resume when it returns to low. Mirrors bluesky's
/// `SuspendBoolHigh`.
pub struct SuspendBoolHigh {
    name: String,
    rx: watch::Receiver<bool>,
}

impl SuspendBoolHigh {
    /// Build with a stable name (used in the interruption-stream
    /// justification when `record_interruptions` is on) and a
    /// `watch::Receiver<bool>` whose published value reflects the
    /// monitored condition.
    pub fn new(name: impl Into<String>, rx: watch::Receiver<bool>) -> Self {
        Self {
            name: name.into(),
            rx,
        }
    }

    /// Spawn the watcher task. Returns the `JoinHandle` so the caller
    /// can keep / abort it. A typical caller stores the handle (or
    /// passes it to `RunEngine::register_suspender_task`); when the
    /// handle drops, `tokio` does **not** abort — call `.abort()` or
    /// wrap in `AbortOnDrop` if you want lifecycle tied to scope.
    pub fn install(self, re: Arc<RunEngine>) -> JoinHandle<()> {
        let SuspendBoolHigh { name, rx } = self;
        spawn_bool_watcher(re, name, rx, /*pause_when=*/ true)
    }
}

/// Pause the engine when a watched `bool` signal goes **low** (`false`),
/// auto-resume when it returns to high.
pub struct SuspendBoolLow {
    name: String,
    rx: watch::Receiver<bool>,
}

impl SuspendBoolLow {
    /// See [`SuspendBoolHigh::new`].
    pub fn new(name: impl Into<String>, rx: watch::Receiver<bool>) -> Self {
        Self {
            name: name.into(),
            rx,
        }
    }

    /// See [`SuspendBoolHigh::install`].
    pub fn install(self, re: Arc<RunEngine>) -> JoinHandle<()> {
        let SuspendBoolLow { name, rx } = self;
        spawn_bool_watcher(re, name, rx, /*pause_when=*/ false)
    }
}

/// Threshold variant — pauses when a watched `f64` signal is on the
/// "bad" side of a numeric threshold. Direction is configurable.
pub struct SuspendThreshold {
    name: String,
    rx: watch::Receiver<f64>,
    threshold: f64,
    /// Direction of the BAD region.
    direction: ThresholdDirection,
}

/// Which side of [`SuspendThreshold::threshold`] is the BAD (pause)
/// region.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ThresholdDirection {
    /// Pause when value < threshold (resume when ≥). Mirrors bluesky
    /// `SuspendFloor` (e.g. beam current too low → pause).
    BadIfBelow,
    /// Pause when value > threshold (resume when ≤). Mirrors bluesky
    /// `SuspendCeil` (e.g. temperature too high → pause).
    BadIfAbove,
}

impl SuspendThreshold {
    /// Build a threshold-based suspender.
    pub fn new(
        name: impl Into<String>,
        rx: watch::Receiver<f64>,
        threshold: f64,
        direction: ThresholdDirection,
    ) -> Self {
        Self {
            name: name.into(),
            rx,
            threshold,
            direction,
        }
    }

    /// Spawn the watcher; see [`SuspendBoolHigh::install`].
    pub fn install(self, re: Arc<RunEngine>) -> JoinHandle<()> {
        let SuspendThreshold {
            name,
            mut rx,
            threshold,
            direction,
        } = self;
        let bad = move |v: f64| match direction {
            ThresholdDirection::BadIfBelow => v < threshold,
            ThresholdDirection::BadIfAbove => v > threshold,
        };
        tokio::spawn(async move {
            loop {
                // Wait until the value enters the BAD region.
                while !bad(*rx.borrow_and_update()) {
                    if rx.changed().await.is_err() {
                        return;
                    }
                }
                // BAD: pause + auto-resume when value returns to GOOD.
                let resume_rx = rx.clone();
                let bad_for_fut = bad;
                let fut: futures::future::BoxFuture<'static, ()> = Box::pin(async move {
                    let mut rx = resume_rx;
                    while bad_for_fut(*rx.borrow_and_update()) {
                        if rx.changed().await.is_err() {
                            return;
                        }
                    }
                });
                re.suspend_until_with(
                    fut,
                    Some(format!(
                        "{name}: signal {} {threshold}",
                        match direction {
                            ThresholdDirection::BadIfBelow => "<",
                            ThresholdDirection::BadIfAbove => ">",
                        }
                    )),
                );
                // Wait for value to actually go GOOD before next bad-watch
                // iteration (avoid a tight loop on transient flickers).
                while bad(*rx.borrow_and_update()) {
                    if rx.changed().await.is_err() {
                        return;
                    }
                }
                tokio::time::sleep(RECONNECT_BACKOFF).await;
            }
        })
    }
}

/// Shared body for `SuspendBoolHigh` / `SuspendBoolLow`. `pause_when`
/// is the boolean value that triggers a pause.
fn spawn_bool_watcher(
    re: Arc<RunEngine>,
    name: String,
    mut rx: watch::Receiver<bool>,
    pause_when: bool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            while *rx.borrow_and_update() != pause_when {
                if rx.changed().await.is_err() {
                    return;
                }
            }
            // BAD: pause + auto-resume when value flips back.
            let mut resume_rx = rx.clone();
            let fut: futures::future::BoxFuture<'static, ()> = Box::pin(async move {
                while *resume_rx.borrow_and_update() == pause_when {
                    if resume_rx.changed().await.is_err() {
                        return;
                    }
                }
            });
            re.suspend_until_with(
                fut,
                Some(format!(
                    "{name}: signal {}",
                    if pause_when { "high" } else { "low" }
                )),
            );
            // Wait for the actual flip to GOOD before next iteration.
            while *rx.borrow_and_update() == pause_when {
                if rx.changed().await.is_err() {
                    return;
                }
            }
            tokio::time::sleep(RECONNECT_BACKOFF).await;
        }
    })
}
