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

pub use crate::core::suspender::Suspender;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

use crate::engine::run_engine::RunEngine;

/// Boxed pre/post plan injection. `None` = nothing to inject.
pub type SuspendInjection = Option<crate::engine::run_engine::SuspendCallback>;

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
    resume_delay: Option<Duration>,
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
            resume_delay: None,
        }
    }

    /// Set the resume delay (bluesky `sleep=`): after the signal returns
    /// to GOOD, wait `delay` — and stay GOOD for the whole `delay` — before
    /// resuming. A flicker back to BAD during the wait restarts it.
    pub fn with_resume_delay(mut self, delay: Duration) -> Self {
        self.resume_delay = Some(delay);
        self
    }

    /// Spawn the watcher task. Returns the `JoinHandle` so the caller
    /// can keep / abort it. A typical caller stores the handle (or
    /// passes it to `RunEngine::register_suspender_task`); when the
    /// handle drops, `tokio` does **not** abort — call `.abort()` or
    /// wrap in `AbortOnDrop` if you want lifecycle tied to scope.
    pub fn install(self, re: Arc<RunEngine>) -> JoinHandle<()> {
        let SuspendBoolHigh {
            name,
            rx,
            resume_delay,
        } = self;
        spawn_bool_watcher(re, name, rx, /*pause_when=*/ true, resume_delay)
    }
}

/// Pause the engine when a watched `bool` signal goes **low** (`false`),
/// auto-resume when it returns to high.
pub struct SuspendBoolLow {
    name: String,
    rx: watch::Receiver<bool>,
    resume_delay: Option<Duration>,
}

impl SuspendBoolLow {
    /// See [`SuspendBoolHigh::new`].
    pub fn new(name: impl Into<String>, rx: watch::Receiver<bool>) -> Self {
        Self {
            name: name.into(),
            rx,
            resume_delay: None,
        }
    }

    /// See [`SuspendBoolHigh::with_resume_delay`].
    pub fn with_resume_delay(mut self, delay: Duration) -> Self {
        self.resume_delay = Some(delay);
        self
    }

    /// See [`SuspendBoolHigh::install`].
    pub fn install(self, re: Arc<RunEngine>) -> JoinHandle<()> {
        let SuspendBoolLow {
            name,
            rx,
            resume_delay,
        } = self;
        spawn_bool_watcher(re, name, rx, /*pause_when=*/ false, resume_delay)
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
    resume_delay: Option<Duration>,
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
            resume_delay: None,
        }
    }

    /// See [`SuspendBoolHigh::with_resume_delay`].
    pub fn with_resume_delay(mut self, delay: Duration) -> Self {
        self.resume_delay = Some(delay);
        self
    }

    /// Spawn the watcher; see [`SuspendBoolHigh::install`].
    pub fn install(self, re: Arc<RunEngine>) -> JoinHandle<()> {
        let SuspendThreshold {
            name,
            mut rx,
            threshold,
            direction,
            resume_delay,
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
                // BAD: pause + auto-resume once value is GOOD-stable for the
                // resume delay (bluesky `sleep=`).
                let resume_rx = rx.clone();
                let fut: futures::future::BoxFuture<'static, ()> = Box::pin(await_good_stable(
                    resume_rx,
                    move |v: &f64| bad(*v),
                    resume_delay,
                ));
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

/// Pause while a watched `f64` is **outside** the open band
/// `(band_bottom, band_top)`; resume when it returns inside. Mirrors
/// bluesky's `SuspendWhenOutsideBand` (temperature controllers, beam
/// position). BAD ⟺ `value <= band_bottom || value >= band_top`.
pub struct SuspendOutsideBand {
    name: String,
    rx: watch::Receiver<f64>,
    band_bottom: f64,
    band_top: f64,
    resume_delay: Option<Duration>,
}

impl SuspendOutsideBand {
    /// Build with the inclusive-outside band edges. `band_bottom` must be
    /// the lower edge; values are GOOD only strictly inside the band.
    pub fn new(
        name: impl Into<String>,
        rx: watch::Receiver<f64>,
        band_bottom: f64,
        band_top: f64,
    ) -> Self {
        Self {
            name: name.into(),
            rx,
            band_bottom,
            band_top,
            resume_delay: None,
        }
    }

    /// See [`SuspendBoolHigh::with_resume_delay`].
    pub fn with_resume_delay(mut self, delay: Duration) -> Self {
        self.resume_delay = Some(delay);
        self
    }

    /// Spawn the watcher; see [`SuspendBoolHigh::install`].
    pub fn install(self, re: Arc<RunEngine>) -> JoinHandle<()> {
        let SuspendOutsideBand {
            name,
            mut rx,
            band_bottom,
            band_top,
            resume_delay,
        } = self;
        let bad = move |v: f64| v <= band_bottom || v >= band_top;
        tokio::spawn(async move {
            loop {
                // Wait until the value leaves the band (BAD).
                while !bad(*rx.borrow_and_update()) {
                    if rx.changed().await.is_err() {
                        return;
                    }
                }
                // BAD: pause + auto-resume once back inside and GOOD-stable.
                let resume_rx = rx.clone();
                let fut: futures::future::BoxFuture<'static, ()> = Box::pin(await_good_stable(
                    resume_rx,
                    move |v: &f64| bad(*v),
                    resume_delay,
                ));
                re.suspend_until_with(
                    fut,
                    Some(format!("{name}: outside ({band_bottom}, {band_top})")),
                );
                // Wait for the value to actually return inside before the next
                // bad-watch iteration (avoid a tight loop on flickers).
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

/// Pause when a watched value deviates from `expected`; resume when it
/// returns. Mirrors bluesky's `SuspendWhenChanged` (facility-mode enum
/// PVs). With `allow_resume = false` (bluesky default) the suspender is
/// one-shot: it pauses on the first deviation and the engine stays paused
/// until a **manual** [`RunEngine::resume`] — it never auto-resumes.
pub struct SuspendWhenChanged<T> {
    name: String,
    rx: watch::Receiver<T>,
    expected: T,
    allow_resume: bool,
    resume_delay: Option<Duration>,
}

impl<T> SuspendWhenChanged<T>
where
    T: Eq + Clone + Send + Sync + 'static,
{
    /// Build with the `expected` value. Defaults to `allow_resume = false`
    /// (matches bluesky): manual resume required after a deviation.
    pub fn new(name: impl Into<String>, rx: watch::Receiver<T>, expected: T) -> Self {
        Self {
            name: name.into(),
            rx,
            expected,
            allow_resume: false,
            resume_delay: None,
        }
    }

    /// Allow the suspender to auto-resume when the value returns to
    /// `expected` (bluesky `allow_resume=True`). Without this the
    /// suspender is one-shot and requires a manual resume.
    pub fn allow_resume(mut self) -> Self {
        self.allow_resume = true;
        self
    }

    /// See [`SuspendBoolHigh::with_resume_delay`]. Only meaningful with
    /// [`allow_resume`](Self::allow_resume).
    pub fn with_resume_delay(mut self, delay: Duration) -> Self {
        self.resume_delay = Some(delay);
        self
    }

    /// Spawn the watcher; see [`SuspendBoolHigh::install`].
    pub fn install(self, re: Arc<RunEngine>) -> JoinHandle<()> {
        let SuspendWhenChanged {
            name,
            mut rx,
            expected,
            allow_resume,
            resume_delay,
        } = self;
        tokio::spawn(async move {
            loop {
                // Wait until the value deviates from `expected` (BAD).
                while *rx.borrow_and_update() == expected {
                    if rx.changed().await.is_err() {
                        return;
                    }
                }
                if !allow_resume {
                    // One-shot: pause and require a manual resume. The resume
                    // future never resolves, so only `RunEngine::resume` (which
                    // un-pauses independently) lifts the suspension. The watcher
                    // is then spent — return rather than re-trip the user.
                    let fut: futures::future::BoxFuture<'static, ()> =
                        Box::pin(std::future::pending());
                    re.suspend_until_with(
                        fut,
                        Some(format!("{name}: value changed (manual resume required)")),
                    );
                    return;
                }
                // BAD: pause + auto-resume once value returns to expected.
                let resume_rx = rx.clone();
                let exp = expected.clone();
                let fut: futures::future::BoxFuture<'static, ()> = Box::pin(await_good_stable(
                    resume_rx,
                    move |v: &T| *v != exp,
                    resume_delay,
                ));
                re.suspend_until_with(fut, Some(format!("{name}: value changed from expected")));
                // Wait for the value to actually return before next iteration.
                while *rx.borrow_and_update() != expected {
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
    resume_delay: Option<Duration>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            while *rx.borrow_and_update() != pause_when {
                if rx.changed().await.is_err() {
                    return;
                }
            }
            // BAD: pause + auto-resume once value is GOOD-stable for the
            // resume delay (bluesky `sleep=`).
            let resume_rx = rx.clone();
            let fut: futures::future::BoxFuture<'static, ()> = Box::pin(await_good_stable(
                resume_rx,
                move |v: &bool| *v == pause_when,
                resume_delay,
            ));
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

/// Resolve once `rx` has been continuously GOOD (`!bad`) for `resume_delay`.
///
/// Mirrors bluesky's `sleep=` resume delay: when the watched signal returns
/// to GOOD the resume is deferred by `resume_delay`; a flicker back to BAD
/// (or any further update) during that window restarts the wait, so the
/// engine only resumes after the signal has settled. `None` resolves the
/// instant the signal is GOOD (no delay). A closed channel resolves too —
/// the source is gone, so staying suspended forever is the wrong default.
async fn await_good_stable<T>(
    mut rx: watch::Receiver<T>,
    bad: impl Fn(&T) -> bool + Send + 'static,
    resume_delay: Option<Duration>,
) where
    T: Send + Sync + 'static,
{
    loop {
        // Wait until the signal is GOOD.
        while bad(&rx.borrow_and_update()) {
            if rx.changed().await.is_err() {
                return;
            }
        }
        // GOOD. With no delay, resume now.
        let Some(delay) = resume_delay else {
            return;
        };
        // Stay GOOD for the whole delay; any update cancels and re-checks.
        tokio::select! {
            _ = tokio::time::sleep(delay) => return,
            changed = rx.changed() => {
                if changed.is_err() {
                    return;
                }
                // Value changed — loop re-evaluates GOOD/BAD and restarts
                // the delay if still GOOD, or waits for GOOD again if BAD.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    // BAD when the bool is `true` (mirrors `pause_when = true`).
    fn bad_high(v: &bool) -> bool {
        *v
    }

    #[tokio::test(start_paused = true)]
    async fn await_good_stable_no_delay_resolves_on_good() {
        let (tx, rx) = watch::channel(true); // BAD
        let mut fut = Box::pin(await_good_stable(rx, bad_high, None));
        // Still BAD: pending.
        assert!((&mut fut).now_or_never().is_none());
        tx.send(false).unwrap(); // GOOD
                                 // No delay → resolves immediately on GOOD.
        assert!((&mut fut).now_or_never().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn await_good_stable_waits_resume_delay() {
        let (tx, rx) = watch::channel(true); // BAD
        let mut fut = Box::pin(await_good_stable(
            rx,
            bad_high,
            Some(Duration::from_secs(5)),
        ));
        assert!((&mut fut).now_or_never().is_none());
        tx.send(false).unwrap(); // GOOD — arms the 5s delay
        assert!(
            (&mut fut).now_or_never().is_none(),
            "must not resume before the delay elapses"
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(
            (&mut fut).now_or_never().is_some(),
            "must resume after the delay elapses"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn await_good_stable_restarts_on_flicker_back_to_bad() {
        let (tx, rx) = watch::channel(true); // BAD
        let mut fut = Box::pin(await_good_stable(
            rx,
            bad_high,
            Some(Duration::from_secs(5)),
        ));
        tx.send(false).unwrap(); // GOOD — arms 5s
        assert!((&mut fut).now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(3)).await; // partway through
        tx.send(true).unwrap(); // flicker BAD — cancels the pending delay
        assert!((&mut fut).now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(5)).await; // old timer must NOT fire
        assert!(
            (&mut fut).now_or_never().is_none(),
            "flicker to BAD must cancel the resume"
        );
        tx.send(false).unwrap(); // GOOD again — fresh 5s
        assert!((&mut fut).now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(
            (&mut fut).now_or_never().is_some(),
            "resumes only after a full stable delay"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn await_good_stable_closed_channel_resolves() {
        let (tx, rx) = watch::channel(true); // BAD
        let mut fut = Box::pin(await_good_stable(
            rx,
            bad_high,
            Some(Duration::from_secs(5)),
        ));
        assert!((&mut fut).now_or_never().is_none());
        drop(tx); // source gone while BAD
        assert!(
            (&mut fut).now_or_never().is_some(),
            "closed channel must resolve, not hang suspended"
        );
    }
}
