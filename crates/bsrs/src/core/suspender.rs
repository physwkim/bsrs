//! `Suspender` trait — future-producing object the engine watches to
//! decide when a paused plan should resume.
//!
//! Lives in `bsrs-core` (rather than `bsrs-engine`) so plan factories
//! and preprocessors in `bsrs-plans` can reference the trait without
//! pulling the engine in. The engine's `Msg::InstallSuspender` carries
//! an `Arc<dyn Any + Send + Sync>` and downcasts it to `Arc<dyn
//! Suspender>` at install time.

use async_trait::async_trait;
use futures::future::BoxFuture;

/// A future-producing object the engine watches. When the future
/// resolves, the engine is signalled to resume.
#[async_trait]
pub trait Suspender: Send + Sync + 'static {
    /// A short label for logs / errors.
    fn name(&self) -> &str;
    /// Wait for the suspending condition to clear.
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
    /// time (e.g. a pure resume-gate whose only job is to lift an existing
    /// suspension). A suspender that can be found tripped at rest overrides
    /// this to gate plan start.
    fn tripped(&self) -> Option<BoxFuture<'static, ()>> {
        None
    }
}
