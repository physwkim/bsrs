//! `Status` and `SubToken` — completion handles with both async and sync APIs.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Errors that a `Status` can carry.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StatusError {
    /// The operation was cancelled before completion.
    #[error("cancelled")]
    Cancelled,
    /// The operation did not complete in time — ophyd's `StatusTimeoutError`.
    /// A [`Status::wait`] running out is not this: see [`WaitError::Timeout`].
    #[error("timed out")]
    Timeout,
    /// The operation completed with an error message.
    #[error("{0}")]
    Failed(String),
}

/// Why [`Status::wait`] returned without success.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WaitError {
    /// The wait ran out with the status still pending — ophyd's
    /// `WaitTimeoutError`. Not an outcome, so no setter can record it.
    #[error("status has not completed yet")]
    Timeout,
    /// The status finished without success.
    #[error(transparent)]
    Status(#[from] StatusError),
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

/// Structured progress-update sink — the bsrs equivalent of ophyd-async's
/// `Watcher` protocol (`core/_protocol.py:124`). A `LiveTable` / progress bar
/// implements this and is driven from a [`Status`] via
/// [`Status::observe_watcher`]: called immediately with the last update if one
/// already exists, then on every subsequent update until the status completes.
pub trait Watcher: Send {
    /// Receive the latest progress update.
    fn watch(&mut self, update: &WatcherUpdate);
}

type StatusCallback = Box<dyn FnOnce(&StatusOutcome) + Send>;

/// Where a status is: waiting, with whoever waits for it, or finished, with
/// its outcome. One value under one lock, so no reader sees a finished status
/// without its outcome and no waiter registers after the waiters were run.
enum Phase {
    Pending {
        callbacks: Vec<StatusCallback>,
        wakers: Vec<Waker>,
    },
    Done(End),
}

/// How a status ended. `Err(StatusError::Cancelled)` is its only cancelled
/// form, whether [`Status::cancel`] or the producer recorded it.
type End = Result<(), StatusError>;

fn outcome(end: &End) -> StatusOutcome {
    match end {
        Ok(()) => StatusOutcome::Success,
        Err(e) => StatusOutcome::Failed(e.clone()),
    }
}

struct Inner {
    phase: Mutex<Phase>,
    progress: watch::Sender<f64>,
    /// The latest update only; back-to-back updates replace each other here.
    watcher: watch::Sender<Option<WatcherUpdate>>,
    /// One queue per running [`Status::observe_watcher`], fed every update.
    /// Its lock also covers the `watcher` store, so an observer that registers
    /// gets each update once: as the latest, or through its queue.
    observers: Mutex<Vec<mpsc::UnboundedSender<WatcherUpdate>>>,
    /// Signalled by [`Status::cancel`] (consumer side) so a producer running
    /// long work can observe the request and abort. Held here, shared by every
    /// `Status`/`StatusSetter` clone of the same operation.
    cancel: CancellationToken,
    /// When the operation began, used to back-fill `WatcherUpdate.time_elapsed`
    /// for producers that omit it. Mirrors ophyd-async's
    /// `WatchableAsyncStatus._start` (`_status.py:200`).
    start: Instant,
    /// How long a reported success takes to land (ophyd `settle_time`).
    settle_time: Duration,
}

/// Runs a status timer on the calling tokio runtime, or on the bsrs runtime
/// from sync code; ophyd runs its timers on threads of their own.
fn spawn_timer<F: Future<Output = ()> + Send + 'static>(timer: F) {
    tokio::runtime::Handle::try_current()
        .unwrap_or_else(|_| crate::core::runtime::runtime_handle())
        .spawn(timer);
}

impl Inner {
    /// The only transition: the first call finishes the status and runs its
    /// waiters, after the lock is released; later calls change nothing.
    fn finish(&self, end: End) {
        let (callbacks, wakers) = {
            let mut phase = self.phase.lock().unwrap();
            let Phase::Pending { callbacks, wakers } = &mut *phase else {
                return;
            };
            let waiters = (std::mem::take(callbacks), std::mem::take(wakers));
            *phase = Phase::Done(end.clone());
            waiters
        };
        let outcome = outcome(&end);
        for cb in callbacks {
            cb(&outcome);
        }
        for w in wakers {
            w.wake();
        }
    }

    fn end(&self) -> Option<End> {
        match &*self.phase.lock().unwrap() {
            Phase::Pending { .. } => None,
            Phase::Done(end) => Some(end.clone()),
        }
    }
}

/// Future + sync handle representing a deferred operation.
#[derive(Clone)]
pub struct Status {
    inner: Arc<Inner>,
    /// The two statuses an [`and`](Status::and) status combines. Held on the
    /// handle, not in `Inner`, which the parts' callbacks keep alive.
    parts: Option<Arc<(Status, Status)>>,
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
        Self::with_timeout(None, Duration::ZERO)
    }

    /// Build a `(Status, setter)` pair with ophyd `StatusBase`'s `timeout`
    /// and `settle_time`: [`StatusSetter::success`] lands `settle_time` after
    /// it is called (a failure lands at once), and a status not finished by
    /// `timeout + settle_time` after this call fails with
    /// [`StatusError::Timeout`]. An outcome reported later changes nothing.
    pub fn with_timeout(timeout: Option<Duration>, settle_time: Duration) -> (Self, StatusSetter) {
        let (tx, rx) = watch::channel(0.0_f64);
        let (wtx, wrx) = watch::channel::<Option<WatcherUpdate>>(None);
        let inner = Arc::new(Inner {
            phase: Mutex::new(Phase::Pending {
                callbacks: Vec::new(),
                wakers: Vec::new(),
            }),
            progress: tx,
            watcher: wtx,
            observers: Mutex::new(Vec::new()),
            cancel: CancellationToken::new(),
            start: Instant::now(),
            settle_time,
        });
        let status = Status {
            inner: inner.clone(),
            parts: None,
            progress_rx: rx,
            watcher_rx: wrx,
        };
        if let Some(timeout) = timeout {
            let status = status.clone();
            spawn_timer(async move {
                let deadline = timeout.saturating_add(settle_time);
                if tokio::time::timeout(deadline, status.clone())
                    .await
                    .is_err()
                {
                    status.inner.finish(Err(StatusError::Timeout));
                }
            });
        }
        (status, StatusSetter { inner })
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

    /// ophyd's `self & other` (`AndStatus`): succeeds once both succeed, and
    /// fails with the first failure without waiting for the other.
    pub fn and(&self, other: &Status) -> Status {
        let (mut status, _setter) = Status::new();
        let pending = Arc::new(AtomicUsize::new(2));
        for part in [self, other] {
            let (inner, pending) = (status.inner.clone(), pending.clone());
            part.add_callback(move |outcome| match outcome {
                StatusOutcome::Failed(e) => inner.finish(Err(e.clone())),
                StatusOutcome::Success => {
                    if pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                        inner.finish(Ok(()));
                    }
                }
            });
        }
        status.parts = Some(Arc::new((self.clone(), other.clone())));
        status
    }

    /// Is `status` one of the statuses this [`and`](Self::and) status
    /// combines, at any depth? ophyd `AndStatus.__contains__`.
    pub fn contains(&self, status: &Status) -> bool {
        self.parts.as_deref().is_some_and(|(left, right)| {
            [left, right]
                .into_iter()
                .any(|p| Arc::ptr_eq(&p.inner, &status.inner) || p.contains(status))
        })
    }

    /// Has the operation completed?
    pub fn done_state(&self) -> bool {
        self.inner.end().is_some()
    }

    /// Has the operation completed successfully?
    pub fn success(&self) -> bool {
        matches!(self.inner.end(), Some(Ok(())))
    }

    /// If failed or cancelled, returns the error (cancellation surfaces as
    /// [`StatusError::Cancelled`]).
    pub fn exception(&self) -> Option<StatusError> {
        self.inner.end()?.err()
    }

    /// Did the operation end in cancellation, by [`Status::cancel`] or by the
    /// producer failing with [`StatusError::Cancelled`]? Mirrors ophyd-async
    /// `Status.cancelled` (`_status.py:91`), where a task that raises
    /// `CancelledError` is cancelled; a cancelled status is *not* `success()`.
    pub fn cancelled(&self) -> bool {
        matches!(self.inner.end(), Some(Err(StatusError::Cancelled)))
    }

    /// Request cancellation (consumer side). Signals the producer via the
    /// shared cancellation token so an in-flight operation (e.g. a motor move)
    /// can abort, and transitions a still-pending status to `CANCELLED` — its
    /// [`Future`] then resolves to `Err(StatusError::Cancelled)`.
    ///
    /// Idempotent and safe after completion: if the status already finished,
    /// the token is (harmlessly) re-signalled and the terminal state is left
    /// unchanged, so cancelling a status that just succeeded preserves the
    /// success. This is the bsrs analogue of asyncio task cancellation that
    /// ophyd-async drives from `async with status:` (`_status.py:113-118`).
    pub fn cancel(&self) {
        self.inner.cancel.cancel();
        self.inner.finish(Err(StatusError::Cancelled));
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
        let end = self.inner.end();
        serde_json::json!({
            "done": end.is_some(),
            "success": end.as_ref().map(Result::is_ok),
            "cancelled": matches!(end, Some(Err(StatusError::Cancelled))),
            "exception": end.and_then(Result::err).map(|e| e.to_string()),
            "progress": self.progress(),
        })
    }

    /// ophyd-style: register a callback fired on completion. If already done,
    /// fires immediately on the calling thread.
    ///
    /// A *cancelled* status counts as done — like a cancelled asyncio task in
    /// ophyd-async (`_status.py:56-60`, `if self.done: callback(self)`), so a
    /// callback registered after `cancel()` fires immediately with the stored
    /// `Cancelled` error. Registration and completion take the same lock, so a
    /// callback is either queued before completion runs the queue or run here.
    pub fn add_callback<F>(&self, cb: F)
    where
        F: FnOnce(&StatusOutcome) + Send + 'static,
    {
        let outcome = match &mut *self.inner.phase.lock().unwrap() {
            Phase::Pending { callbacks, .. } => {
                callbacks.push(Box::new(cb));
                return;
            }
            Phase::Done(end) => outcome(end),
        };
        cb(&outcome);
    }

    /// Sync wait — blocks (via bsrs runtime) until completion, or until
    /// `timeout` runs out with the status still pending.
    pub fn wait(&self, timeout: Option<Duration>) -> Result<(), WaitError> {
        let fut = self.clone();
        crate::core::runtime::block_on(async move {
            let outcome = match timeout {
                Some(d) => tokio::time::timeout(d, fut)
                    .await
                    .map_err(|_| WaitError::Timeout)?,
                None => fut.await,
            };
            outcome.map_err(WaitError::Status)
        })
    }

    /// Subscribe to progress updates as a `watch::Receiver<f64>`.
    pub fn watch(&self) -> watch::Receiver<f64> {
        self.progress_rx.clone()
    }

    /// Subscribe to structured progress updates (the bsrs equivalent of
    /// ophyd-async's `WatchableAsyncStatus.watch`). Holds `None` until the
    /// first [`StatusSetter::update_watcher`] call, then the latest update.
    pub fn watch_updates(&self) -> watch::Receiver<Option<WatcherUpdate>> {
        self.watcher_rx.clone()
    }

    /// Drive a [`Watcher`] from this status's structured updates — the bsrs
    /// equivalent of ophyd-async `WatchableAsyncStatus.watch` (`_status.py:220`).
    ///
    /// Calls `watcher.watch(&update)` immediately if an update has already been
    /// posted (via [`StatusSetter::update_watcher`]), then on every subsequent
    /// update in the order posted, returning once the status completes. Updates
    /// are never merged: one posted right after another, or together with
    /// completion, is still delivered.
    pub async fn observe_watcher<W: Watcher>(&self, watcher: &mut W) {
        let (tx, mut queue) = mpsc::unbounded_channel();
        let latest = {
            let mut observers = self.inner.observers.lock().unwrap();
            observers.push(tx);
            self.inner.watcher.borrow().clone()
        };
        // "called immediately if there has already been an update".
        if let Some(update) = latest {
            watcher.watch(&update);
        }
        let mut done = self.clone();
        loop {
            tokio::select! {
                biased;
                _ = &mut done => break,
                Some(update) = queue.recv() => watcher.watch(&update),
            }
        }
        // Updates posted before completion are already queued.
        while let Ok(update) = queue.try_recv() {
            watcher.watch(&update);
        }
    }
}

impl StatusSetter {
    /// Mark the status as successful and fire callbacks/wakers, after the
    /// settle time given to [`Status::with_timeout`], if any.
    pub fn success(self) {
        let inner = self.inner;
        if inner.settle_time.is_zero() {
            inner.finish(Ok(()));
            return;
        }
        spawn_timer(async move {
            tokio::time::sleep(inner.settle_time).await;
            inner.finish(Ok(()));
        });
    }

    /// Mark the status as failed.
    pub fn fail(self, err: StatusError) {
        self.inner.finish(Err(err));
    }

    /// Resolves when the consumer requests cancellation via [`Status::cancel`].
    /// A producer running long work `select!`s on this to abort promptly and
    /// avoid leaving the operation dangling — the bsrs equivalent of the
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

    /// Push a structured progress update. [`Status::watch_updates`] receivers
    /// see the latest update and each running [`Status::observe_watcher`] every
    /// one; the scalar [`Status::progress`] fraction is also updated when the
    /// update carries one. Best-effort.
    ///
    /// When the producer omits `time_elapsed`, it is back-filled from the
    /// operation's start instant before the update is stored and broadcast, so
    /// a progress display always has an elapsed time (and late observers see
    /// the filled value). Mirrors ophyd-async's `WatchableAsyncStatus`
    /// (`_status.py:204-210`), which back-fills the cached `_last_update`.
    pub fn update_watcher(&self, mut update: WatcherUpdate) {
        if update.time_elapsed.is_none() {
            update.time_elapsed = Some(self.inner.start.elapsed().as_secs_f64());
        }
        if let Some(f) = update.fraction {
            let _ = self.inner.progress.send(f);
        }
        let mut observers = self.inner.observers.lock().unwrap();
        observers.retain(|tx| tx.send(update.clone()).is_ok());
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
        match &mut *self.inner.phase.lock().unwrap() {
            Phase::Pending { wakers, .. } => {
                // A task re-polling in a select! loop must not add a waker per poll.
                if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
                    wakers.push(cx.waker().clone());
                }
                Poll::Pending
            }
            Phase::Done(end) => Poll::Ready(end.clone()),
        }
    }
}

impl std::fmt::Debug for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let end = self.inner.end();
        f.debug_struct("Status")
            .field("done", &end.is_some())
            .field("success", &matches!(end, Some(Ok(()))))
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
            // Explicit time_elapsed so the back-fill is a no-op and this update
            // round-trips verbatim (back-fill is covered separately below).
            time_elapsed: Some(7.0),
            ..WatcherUpdate::new(2.5, 0.0, 10.0)
        };
        setter.update_watcher(update.clone());

        // Structured update landed on the watch_updates channel ...
        assert_eq!(rx.borrow_and_update().clone(), Some(update));
        // ... and the scalar progress fraction was updated from it.
        assert_eq!(s.progress(), 0.25);
    }

    #[tokio::test]
    async fn update_watcher_backfills_time_elapsed_when_omitted() {
        let (s, setter) = Status::new();

        // Producer omits time_elapsed → back-filled from the start instant.
        setter.update_watcher(WatcherUpdate::new(2.5, 0.0, 10.0));
        let got = s.watch_updates().borrow().clone().expect("update posted");
        let te = got
            .time_elapsed
            .expect("time_elapsed back-filled when omitted");
        assert!(te >= 0.0, "elapsed must be non-negative, got {te}");

        // Producer-supplied time_elapsed is preserved verbatim.
        setter.update_watcher(WatcherUpdate {
            time_elapsed: Some(42.0),
            ..WatcherUpdate::new(5.0, 0.0, 10.0)
        });
        let got2 = s.watch_updates().borrow().clone().expect("update posted");
        assert_eq!(
            got2.time_elapsed,
            Some(42.0),
            "explicit time_elapsed must not be overwritten"
        );
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

    // Boundary: callback registered AFTER cancel fires immediately (a cancelled
    // status is done). cancel() already drained the queue, so enqueuing here
    // would lose the callback forever → consumer hang. Mirrors `_status.py:56-60`.
    #[tokio::test]
    async fn add_callback_after_cancel_fires_immediately() {
        let (s, _setter) = Status::new();
        s.cancel();
        let got = Arc::new(Mutex::new(None));
        let g2 = got.clone();
        s.add_callback(move |o| {
            *g2.lock().unwrap() = Some(o.clone());
        });
        // Fired synchronously on this thread with the stored Cancelled error,
        // not silently enqueued.
        assert!(matches!(
            *got.lock().unwrap(),
            Some(StatusOutcome::Failed(StatusError::Cancelled))
        ));
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

    // Boundary: one task re-polling a pending status (a select! loop) is
    // stored once, not once per poll, and is still woken on completion.
    #[test]
    fn repolling_a_pending_status_stores_its_waker_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Wakes(AtomicUsize);
        impl std::task::Wake for Wakes {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (s, setter) = Status::new();
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(s.clone());
        for _ in 0..100 {
            assert!(fut.as_mut().poll(&mut cx).is_pending());
        }
        let stored = match &*s.inner.phase.lock().unwrap() {
            Phase::Pending { wakers, .. } => wakers.len(),
            Phase::Done(_) => panic!("status finished early"),
        };
        assert_eq!(stored, 1);
        setter.success();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        assert!(fut.as_mut().poll(&mut cx).is_ready());
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
