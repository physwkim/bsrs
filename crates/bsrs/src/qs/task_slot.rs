//! Generation-stamped slot for the queue-worker task.
//!
//! The slot is the **synchronous claim on queue execution**: a worker may
//! be spawned only after [`QueueTaskSlot::claim`] succeeds, so two
//! back-to-back `queue_start` calls can never both spawn (the manager
//! state only flips to `ExecutingQueue` asynchronously, inside the worker,
//! and is therefore not a race-free gate). Each claim carries a generation
//! number; releases are generation-checked, so a stale worker's exit can
//! never clear a successor's claim, and a handle registered after a forced
//! [`QueueTaskSlot::take`] is aborted instead of leaking an untracked task.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use tokio::task::AbortHandle;

/// Slot state: at most one live claim, stamped with its generation. The
/// abort handle arrives via [`QueueTaskSlot::register`] after the spawn
/// (a `JoinHandle` does not exist until `tokio::spawn` returns).
pub(crate) struct QueueTaskSlot {
    inner: StdMutex<SlotInner>,
}

struct SlotInner {
    next_gen: u64,
    entry: Option<Entry>,
}

struct Entry {
    generation: u64,
    handle: Option<AbortHandle>,
}

impl QueueTaskSlot {
    pub(crate) fn new() -> Self {
        QueueTaskSlot {
            inner: StdMutex::new(SlotInner {
                next_gen: 0,
                entry: None,
            }),
        }
    }

    /// Claim the slot. Returns `None` while another claim is live — the
    /// caller must not spawn a worker. The returned guard releases the
    /// claim on drop (generation-checked, so a stale guard is inert).
    pub(crate) fn claim(self: &Arc<Self>) -> Option<SlotClaim> {
        let mut inner = self.inner.lock().unwrap();
        if inner.entry.is_some() {
            return None;
        }
        let generation = inner.next_gen;
        inner.next_gen += 1;
        inner.entry = Some(Entry {
            generation,
            handle: None,
        });
        Some(SlotClaim {
            slot: self.clone(),
            generation,
        })
    }

    /// Attach the spawned worker's abort handle to its claim. If the claim
    /// is no longer current — the worker already finished, or a forced
    /// [`take`](Self::take) revoked it between spawn and register — the
    /// handle is aborted instead (a no-op on a finished task; kills an
    /// otherwise-untracked orphan after a revoke).
    pub(crate) fn register(&self, generation: u64, handle: AbortHandle) {
        let mut inner = self.inner.lock().unwrap();
        match &mut inner.entry {
            Some(e) if e.generation == generation => e.handle = Some(handle),
            _ => handle.abort(),
        }
    }

    /// Forced revoke (environment_destroy, manager_stop, server shutdown):
    /// empty the slot and hand back the live worker's handle for aborting.
    /// The revoked claim's later drop is generation-checked and inert.
    pub(crate) fn take(&self) -> Option<AbortHandle> {
        self.inner
            .lock()
            .unwrap()
            .entry
            .take()
            .and_then(|e| e.handle)
    }

    /// True while a claim is live (worker spawned or about to be).
    pub(crate) fn is_active(&self) -> bool {
        self.inner.lock().unwrap().entry.is_some()
    }

    fn finish(&self, generation: u64) {
        let mut inner = self.inner.lock().unwrap();
        if matches!(&inner.entry, Some(e) if e.generation == generation) {
            inner.entry = None;
        }
    }
}

/// RAII claim on the queue-task slot. The worker holds it for its whole
/// run; dropping it (normal exit or abort-induced future drop) releases
/// the slot — but only if this claim is still the current generation.
pub(crate) struct SlotClaim {
    slot: Arc<QueueTaskSlot>,
    generation: u64,
}

impl SlotClaim {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

impl Drop for SlotClaim {
    fn drop(&mut self) {
        self.slot.finish(self.generation);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real AbortHandle whose task parks until aborted (or told to stop).
    async fn parked_handle() -> (tokio::task::JoinHandle<()>, AbortHandle) {
        let join = tokio::spawn(std::future::pending::<()>());
        let handle = join.abort_handle();
        (join, handle)
    }

    #[tokio::test]
    async fn claim_is_exclusive_until_released() {
        let slot = Arc::new(QueueTaskSlot::new());
        let c1 = slot.claim().expect("first claim");
        assert!(slot.claim().is_none(), "second claim while live");
        assert!(slot.is_active());
        drop(c1);
        assert!(!slot.is_active());
        assert!(slot.claim().is_some(), "claim after release");
    }

    #[tokio::test]
    async fn stale_release_does_not_clear_successor() {
        let slot = Arc::new(QueueTaskSlot::new());
        let c1 = slot.claim().expect("first claim");
        // Forced revoke (env_destroy path), then a successor claims.
        assert!(slot.take().is_none(), "no handle registered yet");
        let c2 = slot.claim().expect("claim after take");
        // The revoked worker's future is dropped later; its release
        // must not clear the successor's claim.
        drop(c1);
        assert!(slot.is_active(), "stale release cleared the successor");
        drop(c2);
        assert!(!slot.is_active());
    }

    #[tokio::test]
    async fn register_after_release_aborts_the_handle() {
        let slot = Arc::new(QueueTaskSlot::new());
        let c1 = slot.claim().expect("claim");
        let generation = c1.generation();
        // Worker finished before the spawner could register its handle.
        drop(c1);
        let (join, handle) = parked_handle().await;
        slot.register(generation, handle);
        assert!(!slot.is_active(), "released slot must stay empty");
        let err = join.await.expect_err("task must be aborted");
        assert!(err.is_cancelled());
    }

    #[tokio::test]
    async fn register_after_revoke_aborts_the_handle() {
        let slot = Arc::new(QueueTaskSlot::new());
        let c1 = slot.claim().expect("claim");
        let generation = c1.generation();
        // Forced revoke lands between spawn and register.
        assert!(slot.take().is_none());
        let (join, handle) = parked_handle().await;
        slot.register(generation, handle);
        assert!(!slot.is_active());
        let err = join.await.expect_err("task must be aborted");
        assert!(err.is_cancelled());
        drop(c1); // stale release, inert
        assert!(!slot.is_active());
    }

    #[tokio::test]
    async fn take_returns_the_registered_handle() {
        let slot = Arc::new(QueueTaskSlot::new());
        let claim = slot.claim().expect("claim");
        let (join, handle) = parked_handle().await;
        slot.register(claim.generation(), handle);
        let taken = slot.take().expect("registered handle");
        taken.abort();
        let err = join.await.expect_err("task must be aborted");
        assert!(err.is_cancelled());
        drop(claim); // stale release, inert
        assert!(!slot.is_active());
        assert!(slot.claim().is_some());
    }
}
