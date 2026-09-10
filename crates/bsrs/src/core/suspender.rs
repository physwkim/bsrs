//! `Suspender` trait — what the engine needs from a suspender: when to
//! suspend the run, when the suspension lifts, and what to run around it.
//!
//! Mirrors bluesky `SuspenderBase` (`bluesky/suspenders.py`): [`trip`] is the
//! `_should_suspend` edge, [`watch`] the release event `_should_resume` sets,
//! [`tripped`] is `get_futures`, and [`justification`], [`pre_plan`] and
//! [`post_plan`] keep their names. The engine owns the watcher that drives a
//! suspender (`RunEngine::install_suspender`); a suspender only describes the
//! condition.
//!
//! Lives in `core` (rather than `engine`) so plan factories and preprocessors
//! can reference the trait without pulling the engine in. The engine's
//! `Msg::InstallSuspender` carries an `Arc<dyn Any + Send + Sync>` and
//! downcasts it to `Arc<dyn Suspender>` at install time.
//!
//! [`trip`]: Suspender::trip
//! [`watch`]: Suspender::watch
//! [`tripped`]: Suspender::tripped
//! [`justification`]: Suspender::justification
//! [`pre_plan`]: Suspender::pre_plan
//! [`post_plan`]: Suspender::post_plan

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::core::plan::Plan;

/// A plan factory run around a suspension.
///
/// It is a factory (not a bare [`Plan`]) so a fresh message stream is produced
/// each time a suspension fires, mirroring bluesky's `pre_plan` / `post_plan`
/// (`run_engine.py:1199`), which are generator callables re-invoked per
/// suspend. The produced plan's messages run through the *same* handlers as the
/// main plan — so a `pre_plan` can e.g. close a shutter (real `Set`/`Wait`) and
/// emit documents before the wait, and `post_plan` can re-open it on resume.
pub type SuspendCallback = Arc<dyn Fn() -> Plan + Send + Sync>;

/// A condition the engine suspends the run on while it holds.
#[async_trait]
pub trait Suspender: Send + Sync + 'static {
    /// A short label for logs / errors.
    fn name(&self) -> &str;

    /// Resolve once the suspending condition is active — at once if it already
    /// is. While the suspender is installed the engine awaits this and, when it
    /// resolves, suspends the run until [`watch`](Self::watch) resolves; it
    /// re-arms only after that, so one bad episode requests one suspension
    /// (bluesky requests only while no release event exists yet,
    /// `suspenders.py:128-140`). A suspender that never trips on its own — one
    /// that only gates plan start through [`tripped`](Self::tripped) — returns
    /// a future that never resolves.
    fn trip(&self) -> BoxFuture<'static, ()>;

    /// Wait for the suspending condition to clear, resume delay included. Lifts
    /// the suspension [`trip`](Self::trip) started.
    fn watch(&self) -> BoxFuture<'static, ()>;

    /// If the suspending condition is **currently active** (tripped) at query
    /// time, return a future that resolves once it clears; return `None` when
    /// the condition is currently clear. The engine calls this at plan start
    /// and waits on every returned future before the first message runs, so a
    /// scan never begins its first point while a condition (e.g. beam down) is
    /// bad. Mirrors bluesky's `Suspender.get_futures()` returning an empty list
    /// when the suspender is not tripped (`run_engine.py:933-967`).
    ///
    /// Default `None`: a suspender that is never considered tripped at query
    /// time. A suspender that can be found tripped at rest overrides this to
    /// gate plan start.
    fn tripped(&self) -> Option<BoxFuture<'static, ()>> {
        None
    }

    /// Why the run is suspended. Recorded in the interruptions stream when a
    /// trip suspends the run, as bluesky's `_start_suspender` records the
    /// suspender's `_get_justification()` (`run_engine.py:1263`).
    fn justification(&self) -> String {
        format!("suspended by {}", self.name())
    }

    /// Plan to run once the run is suspended — after the touched motors are
    /// stopped, before the wait (bluesky `pre_plan`). Called per suspension.
    fn pre_plan(&self) -> Option<SuspendCallback> {
        None
    }

    /// Plan to run when the suspension lifts — before the rewind replays
    /// (bluesky `post_plan`). Called per suspension.
    fn post_plan(&self) -> Option<SuspendCallback> {
        None
    }
}
