//! Engine-side `Suspender` registry + reference impls.
//!
//! Reference: bluesky `run_engine.py:1132-1310` (`install_suspender`,
//! `request_suspend`, `_start_suspender`) and `bluesky/suspenders.py`.
//!
//! Two layers live here:
//!
//! - The internal [`SuspenderHandle`], the engine's registration record for
//!   one installed suspender: the suspender itself (queried at plan start for
//!   the ENG-12 gate), its watcher task and the release of a suspension it
//!   holds. Drop aborts the task (rule **K1**) and releases the suspension, as
//!   bluesky's `SuspenderBase.remove` sets the suspender's event.
//! - User-facing impls — [`SuspendBoolHigh`], [`SuspendBoolLow`],
//!   [`SuspendThreshold`], [`SuspendOutsideBand`], [`SuspendWhenChanged`].
//!   Each watches a `tokio::sync::watch::Receiver` and implements
//!   [`Suspender`] over it: the engine suspends the run while the watched
//!   value is in the impl's BAD region and resumes once it has been GOOD for
//!   the resume delay. Install one with `RunEngine::install_suspender`.

pub use crate::core::suspender::{SuspendCallback, Suspender};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;

/// Boxed pre/post plan injection. `None` = nothing to inject.
pub type SuspendInjection = Option<SuspendCallback>;

/// Live registration record. Drop aborts the watcher task (rule **K1**) and
/// releases the suspension the suspender holds, if any.
pub(crate) struct SuspenderHandle {
    /// Stable id used by `RemoveSuspender` Msg.
    #[allow(dead_code)]
    pub(crate) id: u64,
    /// Underlying suspender (kept alive while the registration exists). Also
    /// queried at plan start via [`Suspender::tripped`] for the ENG-12 gate.
    pub(crate) inner: Arc<dyn Suspender>,
    /// The watcher task — drop / abort on Drop.
    pub(crate) abort: AbortHandle,
    /// Cancelled on drop: the future of a suspension this suspender requested
    /// resolves on it, so removing the suspender lifts the suspension as
    /// bluesky's `SuspenderBase.remove` does by setting its event
    /// (`suspenders.py:74-85`) — here without the resume delay.
    pub(crate) released: CancellationToken,
}

impl SuspenderHandle {
    pub(crate) fn new(
        id: u64,
        inner: Arc<dyn Suspender>,
        handle: JoinHandle<()>,
        released: CancellationToken,
    ) -> Self {
        let abort = handle.abort_handle();
        Self {
            id,
            inner,
            abort,
            released,
        }
    }
}

impl Drop for SuspenderHandle {
    fn drop(&mut self) {
        self.abort.abort();
        self.released.cancel();
    }
}

// -- User-facing reference impls --------------------------------------------

/// The shared body of the reference suspenders: a watched signal, the
/// predicate that puts a value in the BAD region, the resume delay, the
/// justification, and the plans run around a suspension.
struct SignalSuspender<T> {
    name: String,
    rx: watch::Receiver<T>,
    bad: Arc<dyn Fn(&T) -> bool + Send + Sync>,
    resume_delay: Option<Duration>,
    /// `false`: the suspension never lifts on its own — a manual
    /// `RunEngine::resume` is required (`SuspendWhenChanged` without
    /// `allow_resume`, bluesky's default for it).
    auto_resume: bool,
    justification: String,
    pre_plan: Option<SuspendCallback>,
    post_plan: Option<SuspendCallback>,
}

impl<T> SignalSuspender<T>
where
    T: Send + Sync + 'static,
{
    fn new(
        name: String,
        rx: watch::Receiver<T>,
        bad: impl Fn(&T) -> bool + Send + Sync + 'static,
        justification: String,
    ) -> Self {
        Self {
            name,
            rx,
            bad: Arc::new(bad),
            resume_delay: None,
            auto_resume: true,
            justification,
            pre_plan: None,
            post_plan: None,
        }
    }

    fn trip(&self) -> BoxFuture<'static, ()> {
        let bad = self.bad.clone();
        Box::pin(await_bad(self.rx.clone(), move |v| bad(v)))
    }

    fn watch(&self) -> BoxFuture<'static, ()> {
        if !self.auto_resume {
            return Box::pin(std::future::pending());
        }
        let bad = self.bad.clone();
        Box::pin(await_good_stable(
            self.rx.clone(),
            move |v| bad(v),
            self.resume_delay,
        ))
    }

    fn tripped(&self) -> Option<BoxFuture<'static, ()>> {
        (self.bad)(&self.rx.borrow()).then(|| self.watch())
    }
}

/// The builders every reference suspender offers and its `Suspender` impl,
/// both forwarding to the wrapped [`SignalSuspender`].
macro_rules! signal_suspender {
    ($ty:ident $(<$g:ident>)?) => {
        impl$(<$g: Eq + Clone + Send + Sync + 'static>)? $ty$(<$g>)? {
            /// Set the resume delay (bluesky `sleep=`): after the signal returns
            /// to GOOD, wait `delay` — and stay GOOD for the whole `delay` —
            /// before resuming. A flicker back to BAD during the wait restarts
            /// it.
            pub fn with_resume_delay(mut self, delay: Duration) -> Self {
                self.0.resume_delay = Some(delay);
                self
            }

            /// Plan to run once the run is suspended, after the touched motors
            /// are stopped and before the wait (bluesky `pre_plan`).
            pub fn with_pre_plan(mut self, plan: SuspendCallback) -> Self {
                self.0.pre_plan = Some(plan);
                self
            }

            /// Plan to run when the suspension lifts, before the rewind replays
            /// (bluesky `post_plan`).
            pub fn with_post_plan(mut self, plan: SuspendCallback) -> Self {
                self.0.post_plan = Some(plan);
                self
            }
        }

        #[async_trait]
        impl$(<$g: Eq + Clone + Send + Sync + 'static>)? Suspender for $ty$(<$g>)? {
            fn name(&self) -> &str {
                &self.0.name
            }
            fn trip(&self) -> BoxFuture<'static, ()> {
                self.0.trip()
            }
            fn watch(&self) -> BoxFuture<'static, ()> {
                self.0.watch()
            }
            fn tripped(&self) -> Option<BoxFuture<'static, ()>> {
                self.0.tripped()
            }
            fn justification(&self) -> String {
                self.0.justification.clone()
            }
            fn pre_plan(&self) -> Option<SuspendCallback> {
                self.0.pre_plan.clone()
            }
            fn post_plan(&self) -> Option<SuspendCallback> {
                self.0.post_plan.clone()
            }
        }
    };
}

/// Suspend while a watched `bool` signal is **high** (`true`); resume once it
/// is low again. Mirrors bluesky's `SuspendBoolHigh`.
pub struct SuspendBoolHigh(SignalSuspender<bool>);

impl SuspendBoolHigh {
    /// Build with a stable name (used in the interruption-stream
    /// justification when `record_interruptions` is on) and a
    /// `watch::Receiver<bool>` whose published value reflects the
    /// monitored condition.
    pub fn new(name: impl Into<String>, rx: watch::Receiver<bool>) -> Self {
        let name = name.into();
        let justification = format!("{name}: signal high");
        Self(SignalSuspender::new(name, rx, |v| *v, justification))
    }
}
signal_suspender!(SuspendBoolHigh);

/// Suspend while a watched `bool` signal is **low** (`false`); resume once it
/// is high again. Mirrors bluesky's `SuspendBoolLow`.
pub struct SuspendBoolLow(SignalSuspender<bool>);

impl SuspendBoolLow {
    /// See [`SuspendBoolHigh::new`].
    pub fn new(name: impl Into<String>, rx: watch::Receiver<bool>) -> Self {
        let name = name.into();
        let justification = format!("{name}: signal low");
        Self(SignalSuspender::new(name, rx, |v| !*v, justification))
    }
}
signal_suspender!(SuspendBoolLow);

/// Threshold variant — suspends while a watched `f64` signal is on the
/// "bad" side of a numeric threshold. Direction is configurable.
pub struct SuspendThreshold(SignalSuspender<f64>);

/// Which side of the threshold is the BAD (suspend) region.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ThresholdDirection {
    /// Suspend when value < threshold (resume when ≥). Mirrors bluesky
    /// `SuspendFloor` (e.g. beam current too low → pause).
    BadIfBelow,
    /// Suspend when value > threshold (resume when ≤). Mirrors bluesky
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
        let name = name.into();
        let justification = format!(
            "{name}: signal {} {threshold}",
            match direction {
                ThresholdDirection::BadIfBelow => "<",
                ThresholdDirection::BadIfAbove => ">",
            }
        );
        let bad = move |v: &f64| match direction {
            ThresholdDirection::BadIfBelow => *v < threshold,
            ThresholdDirection::BadIfAbove => *v > threshold,
        };
        Self(SignalSuspender::new(name, rx, bad, justification))
    }
}
signal_suspender!(SuspendThreshold);

/// Suspend while a watched `f64` is **outside** the open band
/// `(band_bottom, band_top)`; resume when it returns inside. Mirrors
/// bluesky's `SuspendWhenOutsideBand` (temperature controllers, beam
/// position). BAD ⟺ `value <= band_bottom || value >= band_top`.
pub struct SuspendOutsideBand(SignalSuspender<f64>);

impl SuspendOutsideBand {
    /// Build with the inclusive-outside band edges. `band_bottom` must be
    /// the lower edge; values are GOOD only strictly inside the band.
    pub fn new(
        name: impl Into<String>,
        rx: watch::Receiver<f64>,
        band_bottom: f64,
        band_top: f64,
    ) -> Self {
        let name = name.into();
        let justification = format!("{name}: outside ({band_bottom}, {band_top})");
        let bad = move |v: &f64| *v <= band_bottom || *v >= band_top;
        Self(SignalSuspender::new(name, rx, bad, justification))
    }
}
signal_suspender!(SuspendOutsideBand);

/// Suspend when a watched value deviates from `expected`; resume when it
/// returns. Mirrors bluesky's `SuspendWhenChanged` (facility-mode enum
/// PVs). With `allow_resume = false` (bluesky default) the suspender is
/// one-shot: it suspends on the first deviation and the engine stays paused
/// until a **manual** `RunEngine::resume` — it never auto-resumes, and it
/// stays tripped for the plan-start gate until removed.
pub struct SuspendWhenChanged<T>(SignalSuspender<T>);

impl<T> SuspendWhenChanged<T>
where
    T: Eq + Clone + Send + Sync + 'static,
{
    /// Build with the `expected` value. Defaults to `allow_resume = false`
    /// (matches bluesky): manual resume required after a deviation.
    pub fn new(name: impl Into<String>, rx: watch::Receiver<T>, expected: T) -> Self {
        let name = name.into();
        let justification = format!("{name}: value changed (manual resume required)");
        let bad = move |v: &T| *v != expected;
        let mut inner = SignalSuspender::new(name, rx, bad, justification);
        inner.auto_resume = false;
        Self(inner)
    }

    /// Allow the suspender to auto-resume when the value returns to
    /// `expected` (bluesky `allow_resume=True`). Without this the
    /// suspender is one-shot and requires a manual resume.
    pub fn allow_resume(mut self) -> Self {
        self.0.auto_resume = true;
        self.0.justification = format!("{}: value changed from expected", self.0.name);
        self
    }
}
signal_suspender!(SuspendWhenChanged<T>);

/// Resolve once `rx` is BAD — at once if it already is. A closed channel never
/// resolves: with the source gone there is nothing left to suspend on.
async fn await_bad<T>(mut rx: watch::Receiver<T>, bad: impl Fn(&T) -> bool + Send + 'static)
where
    T: Send + Sync + 'static,
{
    while !bad(&rx.borrow_and_update()) {
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
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

    // BAD when the bool is `true` (mirrors `SuspendBoolHigh`).
    fn bad_high(v: &bool) -> bool {
        *v
    }

    #[tokio::test(start_paused = true)]
    async fn await_bad_resolves_at_once_when_already_bad() {
        let (_tx, rx) = watch::channel(true); // BAD
        let mut fut = Box::pin(await_bad(rx, bad_high));
        assert!((&mut fut).now_or_never().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn await_bad_waits_for_the_signal_to_go_bad() {
        let (tx, rx) = watch::channel(false); // GOOD
        let mut fut = Box::pin(await_bad(rx, bad_high));
        assert!((&mut fut).now_or_never().is_none());
        tx.send(true).unwrap(); // BAD
        assert!((&mut fut).now_or_never().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn await_bad_never_resolves_on_a_closed_channel() {
        let (tx, rx) = watch::channel(false); // GOOD
        let mut fut = Box::pin(await_bad(rx, bad_high));
        drop(tx);
        assert!(
            (&mut fut).now_or_never().is_none(),
            "a dead source must not trip a suspension"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tripped_reports_the_current_bad_state() {
        let (tx, rx) = watch::channel(false);
        let s = SuspendBoolHigh::new("shutter", rx);
        assert!(s.tripped().is_none(), "GOOD at rest: not tripped");
        tx.send(true).unwrap();
        let mut clear = s.tripped().expect("BAD at rest: tripped");
        assert!((&mut clear).now_or_never().is_none());
        tx.send(false).unwrap();
        assert!((&mut clear).now_or_never().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn one_shot_when_changed_never_lifts_but_stays_tripped() {
        let (tx, rx) = watch::channel(0_i64);
        let s = SuspendWhenChanged::new("mode", rx, 0_i64);
        tx.send(1).unwrap();
        let mut watch = s.watch();
        tx.send(0).unwrap();
        assert!(
            (&mut watch).now_or_never().is_none(),
            "manual resume only: returning to expected does not lift"
        );
        tx.send(1).unwrap();
        assert!(s.tripped().is_some(), "gates plan start while deviated");
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
