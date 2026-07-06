//! `stage_sigs` — a port of ophyd's `Device.stage_sigs`
//! (`ophyd/device.py` `Device.stage`/`unstage`).
//!
//! A device owns an ordered list of `(signal → desired value)` pairs. On
//! [`StageSigs::stage`] each signal's *current* value is captured and the
//! desired value written, in insertion order. On [`StageSigs::unstage`] the
//! captured values are written back in **reverse** order, returning the device
//! to the state it was in before staging. This is what makes staged
//! configuration (e.g. enabling one areaDetector file plugin for the duration
//! of a scan) revert cleanly on unstage instead of leaking into the IOC.
//!
//! Fidelity to ophyd:
//! - Originals are captured at stage time and restored on unstage.
//! - A setting's original is recorded only *after* its write succeeds, so a
//!   partial stage failure restores exactly the settings that were applied
//!   (`stage()` undoes its partial work and re-raises).
//! - `unstage()` is idempotent: after the first call the captured values are
//!   consumed, so a second `unstage()` with no intervening `stage()` is a
//!   no-op.
//! - Staging twice without an intervening unstage is rejected
//!   (ophyd's `RedundantStaging`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::error::{BsrsError, Result};
use crate::core::status::StatusError;
use crate::protocols_async::SignalBackend;

/// One staged setting. Object-safe so heterogeneously-typed settings share a
/// single list.
#[async_trait::async_trait]
pub trait StageSetting: Send + Sync {
    /// Capture the signal's current value, then write the desired value.
    async fn apply(&self) -> Result<()>;
    /// Write the captured value back and consume it. A no-op when nothing was
    /// captured (never applied, or already restored) — this is what makes
    /// [`StageSigs::unstage`] idempotent.
    async fn restore(&self) -> Result<()>;
    /// Dedup key — the target signal's identity (its PV / source name). Two
    /// settings with the same key address the same signal; the later
    /// [`StageSigs::set`] replaces the earlier, mirroring ophyd's
    /// `stage_sigs[sig] = val` dict assignment.
    fn key(&self) -> &str;
}

/// A [`StageSetting`] over a typed [`SignalBackend<T>`].
struct TypedStageSetting<T: Clone + Send + Sync + 'static> {
    signal: Arc<dyn SignalBackend<T>>,
    desired: T,
    /// The captured pre-stage value, present only between a successful `apply`
    /// and its `restore`.
    original: Mutex<Option<T>>,
    key: String,
}

#[async_trait::async_trait]
impl<T: Clone + Send + Sync + 'static> StageSetting for TypedStageSetting<T> {
    async fn apply(&self) -> Result<()> {
        // Capture BEFORE mutating; record the original only after the write
        // succeeds, so a failed apply leaves nothing for restore() to undo on
        // this signal (ophyd records into `_original_vals` post-set).
        let current = self.signal.get_value().await?;
        self.signal.put(Some(self.desired.clone())).await?;
        *self.original.lock().unwrap() = Some(current);
        Ok(())
    }

    async fn restore(&self) -> Result<()> {
        let original = self.original.lock().unwrap().take();
        if let Some(v) = original {
            self.signal.put(Some(v)).await?;
        }
        Ok(())
    }

    fn key(&self) -> &str {
        &self.key
    }
}

/// An ordered set of [`StageSetting`]s owned by a device — the port of ophyd's
/// `Device.stage_sigs`. See the module docs for the staging contract.
#[derive(Default)]
pub struct StageSigs {
    settings: Mutex<Vec<Arc<dyn StageSetting>>>,
    staged: AtomicBool,
}

impl StageSigs {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `signal → desired`, keyed by `key` (the signal's PV / source
    /// name). If a setting with the same `key` already exists it is replaced,
    /// mirroring ophyd's `stage_sigs[sig] = val`.
    pub fn set<T: Clone + Send + Sync + 'static>(
        &self,
        signal: Arc<dyn SignalBackend<T>>,
        desired: T,
        key: impl Into<String>,
    ) {
        let key = key.into();
        let setting: Arc<dyn StageSetting> = Arc::new(TypedStageSetting {
            signal,
            desired,
            original: Mutex::new(None),
            key: key.clone(),
        });
        self.push(setting);
    }

    /// Add a pre-built setting, replacing any existing setting with the same
    /// [`StageSetting::key`]. The general primitive behind [`Self::set`].
    pub fn push(&self, setting: Arc<dyn StageSetting>) {
        let mut v = self.settings.lock().unwrap();
        if let Some(slot) = v.iter_mut().find(|s| s.key() == setting.key()) {
            *slot = setting;
        } else {
            v.push(setting);
        }
    }

    /// Remove all recorded settings. (Intended for use while unstaged; it does
    /// not restore captured values.)
    pub fn clear(&self) {
        self.settings.lock().unwrap().clear();
    }

    /// Number of recorded settings.
    pub fn len(&self) -> usize {
        self.settings.lock().unwrap().len()
    }

    /// Whether any settings are recorded.
    pub fn is_empty(&self) -> bool {
        self.settings.lock().unwrap().is_empty()
    }

    /// Whether the set is currently staged.
    pub fn is_staged(&self) -> bool {
        self.staged.load(Ordering::SeqCst)
    }

    /// Apply every setting in insertion order. On the first failure, restore
    /// the settings already applied (in reverse) and return the error, so a
    /// partial stage leaves the device as it was found. Rejects a second
    /// `stage()` without an intervening [`Self::unstage`] (`RedundantStaging`).
    pub async fn stage(&self) -> Result<()> {
        if self.staged.swap(true, Ordering::SeqCst) {
            return Err(BsrsError::Status(StatusError::Failed(
                "StageSigs::stage: already staged; unstage first (RedundantStaging)".to_string(),
            )));
        }
        // Clone the Arc list so we never hold the std Mutex across an await.
        let settings = self.settings.lock().unwrap().clone();
        let mut applied: Vec<Arc<dyn StageSetting>> = Vec::new();
        for s in &settings {
            match s.apply().await {
                Ok(()) => applied.push(s.clone()),
                Err(e) => {
                    for done in applied.iter().rev() {
                        let _ = done.restore().await;
                    }
                    self.staged.store(false, Ordering::SeqCst);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Restore every setting in reverse order, returning the device to its
    /// pre-stage state. Idempotent (each setting's `restore` consumes its
    /// captured value). If several restores fail, the first error is returned
    /// after all have been attempted.
    pub async fn unstage(&self) -> Result<()> {
        let settings = self.settings.lock().unwrap().clone();
        let mut first_err: Option<BsrsError> = None;
        for s in settings.iter().rev() {
            if let Err(e) = s.restore().await {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        self.staged.store(false, Ordering::SeqCst);
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::soft::SoftSignalBackend;
    use crate::event_model::Dtype;

    fn soft(initial: f64) -> Arc<dyn SignalBackend<f64>> {
        Arc::new(SoftSignalBackend::new(initial, Dtype::Number))
    }

    #[tokio::test]
    async fn stage_applies_and_unstage_restores() {
        let sig = soft(1.0);
        let ss = StageSigs::new();
        ss.set(sig.clone(), 5.0, "pv");
        assert_eq!(sig.get_value().await.unwrap(), 1.0);

        ss.stage().await.unwrap();
        assert_eq!(sig.get_value().await.unwrap(), 5.0);
        assert!(ss.is_staged());

        ss.unstage().await.unwrap();
        assert_eq!(sig.get_value().await.unwrap(), 1.0);
        assert!(!ss.is_staged());
    }

    #[tokio::test]
    async fn restores_the_value_present_at_stage_time_not_construction() {
        // Original is captured at stage(), so a value written between set()
        // and stage() is the one restored.
        let sig = soft(1.0);
        let ss = StageSigs::new();
        ss.set(sig.clone(), 5.0, "pv");
        sig.put(Some(2.0)).await.unwrap(); // changes the "current" value

        ss.stage().await.unwrap();
        assert_eq!(sig.get_value().await.unwrap(), 5.0);
        ss.unstage().await.unwrap();
        assert_eq!(sig.get_value().await.unwrap(), 2.0); // restored to 2.0, not 1.0
    }

    #[tokio::test]
    async fn unstage_is_idempotent() {
        let sig = soft(1.0);
        let ss = StageSigs::new();
        ss.set(sig.clone(), 5.0, "pv");
        ss.stage().await.unwrap();
        ss.unstage().await.unwrap();
        // Change the value, then unstage again: with nothing captured it must
        // be a no-op, so the value is left untouched.
        sig.put(Some(9.0)).await.unwrap();
        ss.unstage().await.unwrap();
        assert_eq!(sig.get_value().await.unwrap(), 9.0);
    }

    #[tokio::test]
    async fn redundant_stage_is_rejected() {
        let sig = soft(1.0);
        let ss = StageSigs::new();
        ss.set(sig.clone(), 5.0, "pv");
        ss.stage().await.unwrap();
        assert!(ss.stage().await.is_err());
        // The rejected second stage must not have disturbed the value.
        assert_eq!(sig.get_value().await.unwrap(), 5.0);
    }

    #[tokio::test]
    async fn set_replaces_by_key() {
        let sig = soft(1.0);
        let ss = StageSigs::new();
        ss.set(sig.clone(), 5.0, "pv");
        ss.set(sig.clone(), 7.0, "pv"); // same key → replaces
        assert_eq!(ss.len(), 1);
        ss.stage().await.unwrap();
        assert_eq!(sig.get_value().await.unwrap(), 7.0);
    }

    // Records apply/restore ordering to prove reverse-order restoration and
    // partial-failure rollback, without a full SignalBackend mock.
    struct OrderRecorder {
        key: String,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl StageSetting for OrderRecorder {
        async fn apply(&self) -> Result<()> {
            self.log.lock().unwrap().push(format!("apply:{}", self.key));
            Ok(())
        }
        async fn restore(&self) -> Result<()> {
            self.log
                .lock()
                .unwrap()
                .push(format!("restore:{}", self.key));
            Ok(())
        }
        fn key(&self) -> &str {
            &self.key
        }
    }

    // A setting whose `apply` always fails, used to trigger partial-stage undo.
    struct FailingSetting {
        key: String,
    }

    #[async_trait::async_trait]
    impl StageSetting for FailingSetting {
        async fn apply(&self) -> Result<()> {
            Err(BsrsError::Status(StatusError::Failed("apply boom".into())))
        }
        async fn restore(&self) -> Result<()> {
            Ok(())
        }
        fn key(&self) -> &str {
            &self.key
        }
    }

    #[tokio::test]
    async fn applies_in_order_restores_in_reverse() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let ss = StageSigs::new();
        ss.push(Arc::new(OrderRecorder {
            key: "a".into(),
            log: log.clone(),
        }));
        ss.push(Arc::new(OrderRecorder {
            key: "b".into(),
            log: log.clone(),
        }));
        ss.stage().await.unwrap();
        ss.unstage().await.unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            vec!["apply:a", "apply:b", "restore:b", "restore:a"]
        );
    }

    #[tokio::test]
    async fn partial_stage_failure_restores_applied_settings() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let ss = StageSigs::new();
        ss.push(Arc::new(OrderRecorder {
            key: "good".into(),
            log: log.clone(),
        }));
        ss.push(Arc::new(FailingSetting { key: "bad".into() }));

        let res = ss.stage().await;
        assert!(res.is_err(), "stage must surface the failing apply");
        // The good setting, applied before the failure, must be rolled back,
        // and the set left unstaged so it can be retried.
        assert_eq!(*log.lock().unwrap(), vec!["apply:good", "restore:good"]);
        assert!(!ss.is_staged());
    }
}
