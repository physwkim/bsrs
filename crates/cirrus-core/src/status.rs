//! `Status` and `SubToken` — completion handles with both async and sync APIs.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const PENDING: u8 = 0;
const SUCCESS: u8 = 1;
const ERROR: u8 = 2;
const CANCELLED: u8 = 3;

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

/// Structured progress update for a long-running operation, mirroring
/// ophyd-async's `WatcherUpdate` (`core/_utils.py:154`). Carries enough
/// context for a `LiveTable` / progress bar to render an ETA: where the value
/// started, where it is now, where it is going, plus display metadata.
///
/// Generic over the value type; defaults to `f64` — the common case (motor
/// positions, temperatures) and the type [`Status`] carries.
#[derive(Clone, Debug, PartialEq)]
pub struct WatcherUpdate<T = f64> {
    /// The current value (where it is now).
    pub current: T,
    /// The initial value (where it started).
    pub initial: T,
    /// The target value (where it will be when finished).
    pub target: T,
    /// Optional device name.
    pub name: Option<String>,
    /// Units of the value, if applicable.
    pub unit: Option<String>,
    /// Decimal places the value should be displayed to.
    pub precision: Option<i32>,
    /// Fraction of the way from `initial` to `target` (0.0–1.0).
    pub fraction: Option<f64>,
    /// Seconds elapsed since the operation started.
    pub time_elapsed: Option<f64>,
    /// Estimated seconds remaining until completion.
    pub time_remaining: Option<f64>,
}

impl<T> WatcherUpdate<T> {
    /// Construct an update from the three positions; metadata fields default
    /// to `None`. Use struct-update syntax to set `unit` / `precision` / etc.
    pub fn new(current: T, initial: T, target: T) -> Self {
        Self {
            current,
            initial,
            target,
            name: None,
            unit: None,
            precision: None,
            fraction: None,
            time_elapsed: None,
            time_remaining: None,
        }
    }
}

/// Structured progress-update sink — the cirrus equivalent of ophyd-async's
/// `Watcher` protocol (`core/_protocol.py:124`). A `LiveTable` / progress bar
/// implements this and is driven from a [`Status`] via
/// [`Status::observe_watcher`]: called immediately with the last update if one
/// already exists, then on every subsequent update until the status completes.
pub trait Watcher: Send {
    /// Receive the latest progress update.
    fn watch(&mut self, update: &WatcherUpdate);
}

type StatusCallback = Box<dyn FnOnce(&StatusOutcome) + Send>;

struct Inner {
    state: AtomicU8,
    error: Mutex<Option<StatusError>>,
    progress: watch::Sender<f64>,
    watcher: watch::Sender<Option<WatcherUpdate>>,
    callbacks: Mutex<Vec<StatusCallback>>,
    wakers: Mutex<Vec<Waker>>,
    /// Signalled by [`Status::cancel`] (consumer side) so a producer running
    /// long work can observe the request and abort. Held here, shared by every
    /// `Status`/`StatusSetter` clone of the same operation.
    cancel: CancellationToken,
}

impl Inner {
    /// Drain and run completion callbacks + wakers. Shared by the three
    /// terminal transitions (`success`, `fail`, `cancel`) so each fires exactly
    /// the same way. Takes `&self` — the single owner is whoever won the state
    /// CAS, which is enforced at the call sites, not here.
    fn fire(&self, outcome: StatusOutcome) {
        let cbs: Vec<_> = std::mem::take(&mut *self.callbacks.lock().unwrap());
        for cb in cbs {
            cb(&outcome);
        }
        let wakers: Vec<_> = std::mem::take(&mut *self.wakers.lock().unwrap());
        for w in wakers {
            w.wake();
        }
    }
}

/// Future + sync handle representing a deferred operation.
#[derive(Clone)]
pub struct Status {
    inner: Arc<Inner>,
    progress_rx: watch::Receiver<f64>,
    watcher_rx: watch::Receiver<Option<WatcherUpdate>>,
}

/// One-time setter side of a `Status`.
pub struct StatusSetter {
    inner: Arc<Inner>,
}

impl Status {
    /// Build a fresh pair of `(Status, setter)`.
    pub fn new() -> (Self, StatusSetter) {
        let (tx, rx) = watch::channel(0.0_f64);
        let (wtx, wrx) = watch::channel::<Option<WatcherUpdate>>(None);
        let inner = Arc::new(Inner {
            state: AtomicU8::new(PENDING),
            error: Mutex::new(None),
            progress: tx,
            watcher: wtx,
            callbacks: Mutex::new(Vec::new()),
            wakers: Mutex::new(Vec::new()),
            cancel: CancellationToken::new(),
        });
        (
            Status {
                inner: inner.clone(),
                progress_rx: rx,
                watcher_rx: wrx,
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

    /// If failed or cancelled, returns the error (cancellation surfaces as
    /// [`StatusError::Cancelled`]).
    pub fn exception(&self) -> Option<StatusError> {
        match self.inner.state.load(Ordering::Acquire) {
            ERROR | CANCELLED => self.inner.error.lock().unwrap().clone(),
            _ => None,
        }
    }

    /// Did the operation end in cancellation? Mirrors ophyd-async
    /// `Status.cancelled` (`_status.py:91`); a cancelled status is *not*
    /// `success()`.
    pub fn cancelled(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == CANCELLED
    }

    /// Request cancellation (consumer side). Signals the producer via the
    /// shared cancellation token so an in-flight operation (e.g. a motor move)
    /// can abort, and transitions a still-pending status to `CANCELLED` — its
    /// [`Future`] then resolves to `Err(StatusError::Cancelled)`.
    ///
    /// Idempotent and safe after completion: if the status already finished,
    /// the token is (harmlessly) re-signalled and the terminal state is left
    /// unchanged, so cancelling a status that just succeeded preserves the
    /// success. This is the cirrus analogue of asyncio task cancellation that
    /// ophyd-async drives from `async with status:` (`_status.py:113-118`).
    pub fn cancel(&self) {
        self.inner.cancel.cancel();
        if self
            .inner
            .state
            .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            *self.inner.error.lock().unwrap() = Some(StatusError::Cancelled);
            self.inner
                .fire(StatusOutcome::Failed(StatusError::Cancelled));
        }
    }

    /// Wrap this status in a [`CancelGuard`] that cancels it on scope exit —
    /// the Rust analogue of ophyd-async's `async with status:` block. The guard
    /// holds a clone, so the original handle still observes the cancellation.
    pub fn cancel_on_drop(&self) -> CancelGuard {
        CancelGuard::new(self.clone())
    }

    /// Current progress fraction (0.0–1.0). The `Future` impl does
    /// not surface this — it's exposed for inspect/debug paths.
    pub fn progress(&self) -> f64 {
        *self.progress_rx.borrow()
    }

    /// Snapshot the status as a JSON value suitable for inspect /
    /// debug output. Shape:
    ///
    /// ```json
    /// { "done": bool, "success": bool|null,
    ///   "exception": "...string..."|null, "progress": 0.0–1.0 }
    /// ```
    ///
    /// `success` is `null` while pending; once `done`, it's a bool.
    pub fn inspect(&self) -> serde_json::Value {
        let state = self.inner.state.load(Ordering::Acquire);
        let done = state != PENDING;
        let success = match state {
            SUCCESS => Some(true),
            ERROR | CANCELLED => Some(false),
            _ => None,
        };
        let exception = self.exception().map(|e| e.to_string());
        serde_json::json!({
            "done": done,
            "success": success,
            "cancelled": state == CANCELLED,
            "exception": exception,
            "progress": self.progress(),
        })
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

    /// Subscribe to structured progress updates (the cirrus equivalent of
    /// ophyd-async's `WatchableAsyncStatus.watch`). Holds `None` until the
    /// first [`StatusSetter::update_watcher`] call, then the latest update.
    pub fn watch_updates(&self) -> watch::Receiver<Option<WatcherUpdate>> {
        self.watcher_rx.clone()
    }

    /// Drive a [`Watcher`] from this status's structured updates — the cirrus
    /// equivalent of ophyd-async `WatchableAsyncStatus.watch` (`_status.py:220`).
    ///
    /// Calls `watcher.watch(&update)` immediately if an update has already been
    /// posted (via [`StatusSetter::update_watcher`]), then on every subsequent
    /// update, returning once the status completes. A final update that lands
    /// together with completion is delivered exactly once.
    pub async fn observe_watcher<W: Watcher>(&self, watcher: &mut W) {
        let mut rx = self.watch_updates();
        // "called immediately if there has already been an update".
        if let Some(update) = rx.borrow_and_update().clone() {
            watcher.watch(&update);
        }
        let mut done = self.clone();
        loop {
            tokio::select! {
                biased;
                _ = &mut done => {
                    // A final update posted together with completion is
                    // delivered here; the immediate/changed paths already
                    // cleared the "seen" flag, so this fires only on new data.
                    if rx.has_changed().unwrap_or(false) {
                        if let Some(update) = rx.borrow_and_update().clone() {
                            watcher.watch(&update);
                        }
                    }
                    return;
                }
                res = rx.changed() => {
                    if res.is_err() {
                        return;
                    }
                    if let Some(update) = rx.borrow_and_update().clone() {
                        watcher.watch(&update);
                    }
                }
            }
        }
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
            self.inner.fire(StatusOutcome::Success);
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
            self.inner.fire(StatusOutcome::Failed(err));
        }
    }

    /// Resolves when the consumer requests cancellation via [`Status::cancel`].
    /// A producer running long work `select!`s on this to abort promptly and
    /// avoid leaving the operation dangling — the cirrus equivalent of the
    /// asyncio task cancellation ophyd-async relies on. After observing it the
    /// producer should release the setter (dropping it, or recording
    /// [`StatusError::Cancelled`] via [`fail`](Self::fail), which is a no-op
    /// once [`Status::cancel`] already set the `CANCELLED` state).
    pub async fn cancelled(&self) {
        self.inner.cancel.cancelled().await
    }

    /// Non-blocking check for a pending cancellation request.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancel.is_cancelled()
    }

    /// Update progress (0.0 to 1.0). Best-effort — receivers see latest value.
    pub fn progress(&self, p: f64) {
        let _ = self.inner.progress.send(p);
    }

    /// Push a structured progress update. Receivers see the latest update via
    /// [`Status::watch_updates`]; the scalar [`Status::progress`] fraction is
    /// also updated when the update carries one. Best-effort.
    pub fn update_watcher(&self, update: WatcherUpdate) {
        if let Some(f) = update.fraction {
            let _ = self.inner.progress.send(f);
        }
        let _ = self.inner.watcher.send(Some(update));
    }
}

/// RAII guard that cancels its [`Status`] when dropped — the Rust analogue of
/// ophyd-async's `async with status:` block (`_status.py:110-120`), which
/// cancels the operation on scope exit so a loop that finishes (or errors)
/// before the operation completes does not leave it dangling.
///
/// The guard holds a clone of the status, so the original handle still observes
/// the cancellation. Because [`Status::cancel`] is idempotent and a no-op once
/// the status has completed, dropping the guard after a successful completion
/// preserves the success.
pub struct CancelGuard {
    status: Status,
}

impl CancelGuard {
    /// Wrap a status so its [`Status::cancel`] runs when this guard drops.
    pub fn new(status: Status) -> Self {
        Self { status }
    }

    /// Borrow the guarded status (to await it, read progress, etc.).
    pub fn status(&self) -> &Status {
        &self.status
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.status.cancel();
    }
}

impl Future for Status {
    type Output = Result<(), StatusError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.state.load(Ordering::Acquire) {
            SUCCESS => Poll::Ready(Ok(())),
            ERROR | CANCELLED => {
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
    async fn inspect_pending_success_failure() {
        // Pending shape.
        let (s, setter) = Status::new();
        let v = s.inspect();
        assert_eq!(v["done"], false);
        assert!(v["success"].is_null());
        assert!(v["exception"].is_null());
        assert_eq!(v["progress"], 0.0);

        // Successful resolution.
        setter.success();
        let v = s.inspect();
        assert_eq!(v["done"], true);
        assert_eq!(v["success"], true);
        assert!(v["exception"].is_null());

        // Failed resolution.
        let (s2, setter2) = Status::new();
        setter2.fail(StatusError::Failed("boom".into()));
        let v = s2.inspect();
        assert_eq!(v["done"], true);
        assert_eq!(v["success"], false);
        assert_eq!(v["exception"].as_str(), Some("boom"));
    }

    #[tokio::test]
    async fn watcher_update_propagates_to_receiver_and_fraction() {
        let (s, setter) = Status::new();
        let mut rx = s.watch_updates();
        // No update yet.
        assert!(rx.borrow_and_update().is_none());
        assert_eq!(s.progress(), 0.0);

        let update = WatcherUpdate {
            fraction: Some(0.25),
            unit: Some("mm".into()),
            ..WatcherUpdate::new(2.5, 0.0, 10.0)
        };
        setter.update_watcher(update.clone());

        // Structured update landed on the watch_updates channel ...
        assert_eq!(rx.borrow_and_update().clone(), Some(update));
        // ... and the scalar progress fraction was updated from it.
        assert_eq!(s.progress(), 0.25);
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

    // Boundary: status PENDING at cancel time → resolves Cancelled, distinct
    // from success and from a Failed error.
    #[tokio::test]
    async fn cancel_pending_resolves_to_cancelled() {
        let (s, _setter) = Status::new();
        let waiter = tokio::spawn(s.clone());
        s.cancel();
        let out = waiter.await.unwrap();
        assert!(matches!(out, Err(StatusError::Cancelled)));
        assert!(s.cancelled());
        assert!(!s.success());
        assert!(matches!(s.exception(), Some(StatusError::Cancelled)));
        let v = s.inspect();
        assert_eq!(v["done"], true);
        assert_eq!(v["success"], false);
        assert_eq!(v["cancelled"], true);
        assert_eq!(v["exception"].as_str(), Some("cancelled"));
    }

    // Boundary: status already SUCCESS at cancel time → cancel is a no-op;
    // the success outcome is preserved (matches `async with` exit after done).
    #[tokio::test]
    async fn cancel_after_success_is_noop() {
        let (s, setter) = Status::new();
        setter.success();
        s.cancel();
        assert!(s.success());
        assert!(!s.cancelled());
        assert!(s.exception().is_none());
        assert!(s.clone().await.is_ok());
    }

    // Boundary: the producer observes the cancellation request through the
    // shared token, so an in-flight operation can abort (the headline use case).
    #[tokio::test]
    async fn cancel_signals_producer_token() {
        let (s, setter) = Status::new();
        assert!(!setter.is_cancelled());
        let producer = tokio::spawn(async move {
            // Aborts as soon as cancellation is requested.
            setter.cancelled().await;
            setter.is_cancelled()
        });
        s.cancel();
        assert!(producer.await.unwrap(), "producer saw the cancel request");
    }

    // Boundary: guard dropped while PENDING → cancels; guard dropped after
    // SUCCESS → preserves success.
    #[tokio::test]
    async fn cancel_guard_cancels_on_drop_only_while_pending() {
        // Pending → drop cancels.
        let (s, _setter) = Status::new();
        {
            let g = s.cancel_on_drop();
            assert!(!g.status().done_state());
        }
        assert!(s.cancelled());
        assert!(matches!(s.clone().await, Err(StatusError::Cancelled)));

        // Already succeeded → drop is a no-op.
        let (s2, setter2) = Status::new();
        setter2.success();
        drop(s2.cancel_on_drop());
        assert!(s2.success());
        assert!(!s2.cancelled());
    }

    struct RecordingWatcher(Arc<Mutex<Vec<WatcherUpdate>>>);
    impl Watcher for RecordingWatcher {
        fn watch(&mut self, update: &WatcherUpdate) {
            self.0.lock().unwrap().push(update.clone());
        }
    }

    fn frac(f: f64) -> WatcherUpdate {
        WatcherUpdate {
            fraction: Some(f),
            ..WatcherUpdate::new(f * 10.0, 0.0, 10.0)
        }
    }

    // Boundary: an update already posted before observe → delivered immediately;
    // a later update → delivered too; completion ends the driver.
    #[tokio::test]
    async fn observe_watcher_immediate_and_subsequent() {
        let (s, setter) = Status::new();
        let rec = Arc::new(Mutex::new(Vec::<WatcherUpdate>::new()));
        setter.update_watcher(frac(0.2)); // posted BEFORE observing

        let rec2 = rec.clone();
        let sd = s.clone();
        let driver = tokio::spawn(async move {
            let mut w = RecordingWatcher(rec2);
            sd.observe_watcher(&mut w).await;
        });

        // Let the immediate delivery run before posting the next update.
        tokio::time::sleep(Duration::from_millis(10)).await;
        setter.update_watcher(frac(0.8));
        tokio::time::sleep(Duration::from_millis(10)).await;
        setter.success();
        driver.await.unwrap();

        let fracs: Vec<_> = rec.lock().unwrap().iter().map(|u| u.fraction).collect();
        assert!(fracs.contains(&Some(0.2)), "immediate update: {fracs:?}");
        assert!(fracs.contains(&Some(0.8)), "subsequent update: {fracs:?}");
    }

    // Boundary: no update ever posted → watcher never called, driver still
    // returns cleanly on completion.
    #[tokio::test]
    async fn observe_watcher_no_update_completes_cleanly() {
        let (s, setter) = Status::new();
        let rec = Arc::new(Mutex::new(Vec::<WatcherUpdate>::new()));
        let rec2 = rec.clone();
        let sd = s.clone();
        let driver = tokio::spawn(async move {
            let mut w = RecordingWatcher(rec2);
            sd.observe_watcher(&mut w).await;
        });
        setter.success();
        driver.await.unwrap();
        assert!(
            rec.lock().unwrap().is_empty(),
            "no updates → watcher unused"
        );
    }
}
