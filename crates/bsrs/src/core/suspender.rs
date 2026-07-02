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
}
