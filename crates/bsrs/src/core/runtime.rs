//! Process-singleton tokio runtime that powers the sync facade.
//!
//! Sync entry points (e.g. `RunEngine::run_blocking`, `Signal::get_blocking`)
//! call `bsrs_runtime().block_on(...)`. The handle is shared across threads.

use std::sync::OnceLock;
use tokio::runtime::{Builder, Handle, Runtime};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Build (lazily) and return the bsrs runtime handle.
///
/// # Panics
/// Panics if the runtime cannot be built. This should only happen if the
/// caller is trying to start the runtime from inside another runtime that
/// already exists in the same thread.
pub fn bsrs_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("bsrs-rt")
            .build()
            .expect("failed to build bsrs tokio runtime")
    })
}

/// Returns a shared `Handle` to the bsrs runtime.
pub fn runtime_handle() -> Handle {
    bsrs_runtime().handle().clone()
}

/// Block on a future using the bsrs runtime.
///
/// **Must not** be called from inside an async task — it will panic.
/// This matches Python's `asyncio.run` semantics: top-level entry only.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    if Handle::try_current().is_ok() {
        panic!(
            "bsrs_runtime::block_on called from inside an async context — \
             use `await` instead, or call from a sync entry point"
        );
    }
    bsrs_runtime().block_on(fut)
}
