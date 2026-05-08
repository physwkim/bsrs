//! `Status` and `SubToken` — completion handles with both async and sync APIs.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::sync::watch;

const PENDING: u8 = 0;
const SUCCESS: u8 = 1;
const ERROR: u8 = 2;

/// Errors that a `Status` can carry.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StatusError {
    /// The operation was cancelled before completion.
    #[error("cancelled")]
    Cancelled,
    /// The operation timed out waiting for completion.
    #[error("timed out")]
    Timeout,
    /// The operation completed with an error message.
    #[error("{0}")]
    Failed(String),
}

/// Outcome of a status. Used by the ophyd-style `add_callback` API.
#[derive(Debug, Clone)]
pub enum StatusOutcome {
    /// Successful completion.
    Success,
    /// Failure with cause.
    Failed(StatusError),
}

type StatusCallback = Box<dyn FnOnce(&StatusOutcome) + Send>;

struct Inner {
    state: AtomicU8,
    error: Mutex<Option<StatusError>>,
    progress: watch::Sender<f64>,
    callbacks: Mutex<Vec<StatusCallback>>,
    wakers: Mutex<Vec<Waker>>,
}

/// Future + sync handle representing a deferred operation.
#[derive(Clone)]
pub struct Status {
    inner: Arc<Inner>,
    progress_rx: watch::Receiver<f64>,
}

/// One-time setter side of a `Status`.
pub struct StatusSetter {
    inner: Arc<Inner>,
}

impl Status {
    /// Build a fresh pair of `(Status, setter)`.
    pub fn new() -> (Self, StatusSetter) {
        let (tx, rx) = watch::channel(0.0_f64);
        let inner = Arc::new(Inner {
            state: AtomicU8::new(PENDING),
            error: Mutex::new(None),
            progress: tx,
            callbacks: Mutex::new(Vec::new()),
            wakers: Mutex::new(Vec::new()),
        });
        (
            Status {
                inner: inner.clone(),
                progress_rx: rx,
            },
            StatusSetter { inner },
        )
    }

    /// Construct an already-done success status.
    pub fn done() -> Self {
        let (s, setter) = Self::new();
        setter.success();
        s
    }

    /// Construct an already-done failed status.
    pub fn fail(err: StatusError) -> Self {
        let (s, setter) = Self::new();
        setter.fail(err);
        s
    }

    /// Has the operation completed?
    pub fn done_state(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) != PENDING
    }

    /// Has the operation completed successfully?
    pub fn success(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == SUCCESS
    }

    /// If failed, returns the error.
    pub fn exception(&self) -> Option<StatusError> {
        if self.inner.state.load(Ordering::Acquire) == ERROR {
            self.inner.error.lock().unwrap().clone()
        } else {
            None
        }
    }

    /// ophyd-style: register a callback fired on completion. If already done,
    /// fires immediately on the calling thread.
    pub fn add_callback<F>(&self, cb: F)
    where
        F: FnOnce(&StatusOutcome) + Send + 'static,
    {
        match self.inner.state.load(Ordering::Acquire) {
            SUCCESS => cb(&StatusOutcome::Success),
            ERROR => {
                let err = self
                    .inner
                    .error
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or(StatusError::Failed("unknown".into()));
                cb(&StatusOutcome::Failed(err));
            }
            _ => self.inner.callbacks.lock().unwrap().push(Box::new(cb)),
        }
    }

    /// Sync wait — blocks (via cirrus runtime) until completion.
    pub fn wait(&self, timeout: Option<Duration>) -> Result<(), StatusError> {
        let fut = self.clone();
        let result = match timeout {
            Some(d) => crate::runtime::block_on(async move {
                tokio::time::timeout(d, fut)
                    .await
                    .map_err(|_| StatusError::Timeout)
                    .and_then(|r| r)
            }),
            None => crate::runtime::block_on(fut),
        };
        result
    }

    /// Subscribe to progress updates as a `watch::Receiver<f64>`.
    pub fn watch(&self) -> watch::Receiver<f64> {
        self.progress_rx.clone()
    }
}

impl StatusSetter {
    /// Mark the status as successful and fire callbacks/wakers.
    pub fn success(self) {
        if self
            .inner
            .state
            .compare_exchange(PENDING, SUCCESS, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.fire(StatusOutcome::Success);
        }
    }

    /// Mark the status as failed.
    pub fn fail(self, err: StatusError) {
        if self
            .inner
            .state
            .compare_exchange(PENDING, ERROR, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            *self.inner.error.lock().unwrap() = Some(err.clone());
            self.fire(StatusOutcome::Failed(err));
        }
    }

    /// Update progress (0.0 to 1.0). Best-effort — receivers see latest value.
    pub fn progress(&self, p: f64) {
        let _ = self.inner.progress.send(p);
    }

    fn fire(self, outcome: StatusOutcome) {
        let cbs: Vec<_> = std::mem::take(&mut *self.inner.callbacks.lock().unwrap());
        for cb in cbs {
            cb(&outcome);
        }
        let wakers: Vec<_> = std::mem::take(&mut *self.inner.wakers.lock().unwrap());
        for w in wakers {
            w.wake();
        }
    }
}

impl Future for Status {
    type Output = Result<(), StatusError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.state.load(Ordering::Acquire) {
            SUCCESS => Poll::Ready(Ok(())),
            ERROR => {
                let err = self
                    .inner
                    .error
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or(StatusError::Failed("unknown".into()));
                Poll::Ready(Err(err))
            }
            _ => {
                self.inner.wakers.lock().unwrap().push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl std::fmt::Debug for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Status")
            .field("done", &self.done_state())
            .field("success", &self.success())
            .finish()
    }
}

/// RAII subscription token. Drop unregisters from the backend.
pub struct SubToken {
    on_drop: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl SubToken {
    /// Construct a token whose `Drop` runs the given closure exactly once.
    pub fn new<F: FnOnce() + Send + Sync + 'static>(unsubscribe: F) -> Self {
        Self {
            on_drop: Some(Box::new(unsubscribe)),
        }
    }

    /// A no-op token (for backends that have no per-subscription state).
    pub fn noop() -> Self {
        Self { on_drop: None }
    }
}

impl Drop for SubToken {
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn status_success_via_future() {
        let (s, setter) = Status::new();
        let h = tokio::spawn(s);
        setter.success();
        assert!(h.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn status_failure_via_callback() {
        let (s, setter) = Status::new();
        let flag = Arc::new(Mutex::new(false));
        let f2 = flag.clone();
        s.add_callback(move |o| {
            if matches!(o, StatusOutcome::Failed(_)) {
                *f2.lock().unwrap() = true;
            }
        });
        setter.fail(StatusError::Failed("boom".into()));
        // give callback chain a chance to run on this thread (it's sync, already done)
        assert!(*flag.lock().unwrap());
    }
}
