//! Disk-backed [`CheckpointHook`] for crash-recovery audit trails.
//!
//! Every `Msg::Checkpoint` the engine emits gets appended to a JSONL
//! file (default `~/.bsrs/checkpoints.jsonl`). Every `CloseRun`
//! appends a paired record with `exit_status` set. On daemon
//! restart, `manager.rs` calls [`JsonlCheckpointStore::unfinished_run`]
//! to detect runs that opened, hit at least one checkpoint, but never
//! emitted a paired close — i.e. runs that were abandoned when the
//! daemon went down.
//!
//! Full crash-recovery (resume the plan from the last checkpoint) is
//! still deferred — it requires plan-arg persistence and msg_cache
//! replay which are deeper concerns. The pieces here give an operator
//! the data needed to *detect* the unfinished run and decide whether
//! to re-issue the plan manually.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use crate::engine::{CheckpointHook, CheckpointSnapshot};
use serde::{Deserialize, Serialize};

/// One JSONL record. Stable shape — extend additively.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRecord {
    /// Wall-clock UTC nanoseconds since the unix epoch.
    pub timestamp_ns: u64,
    /// Currently-open run uid, if any.
    pub run_uid: Option<String>,
    /// Bsrs version that produced this record (for cross-version
    /// audit). Set to `CARGO_PKG_VERSION` at append time.
    pub bsrs_version: String,
    /// `None` for mid-run `Msg::Checkpoint` records. `Some(status)`
    /// for the record fired right after a `CloseRun` emitted its
    /// RunStop document (`success` / `abort` / `fail` / `halt`).
    ///
    /// Pre-existing records written before this field was introduced
    /// deserialize as `None` (`#[serde(default)]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_status: Option<String>,
}

/// Append-only checkpoint store. The file is opened lazily on the
/// first append and held for the daemon's lifetime; OS writeback
/// flushes the line buffer.
pub struct JsonlCheckpointStore {
    path: PathBuf,
    /// `None` until the first append succeeds. Behind a mutex so the
    /// `CheckpointHook` (Fn) can mutate it.
    file: StdMutex<Option<std::fs::File>>,
}

impl JsonlCheckpointStore {
    /// Build a store at `path`. The file is not opened yet.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            file: StdMutex::new(None),
        }
    }

    /// Append one record. Errors are logged via `tracing::warn!` and
    /// swallowed so a transient I/O fault doesn't crash the engine.
    pub fn append(&self, record: &CheckpointRecord) {
        let line = match serde_json::to_string(record) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("checkpoint store: serialize failed: {e}");
                return;
            }
        };
        let mut g = self.file.lock().unwrap();
        if g.is_none() {
            // Lazy open. Create parent dir if missing.
            if let Some(parent) = self.path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::warn!("checkpoint store: mkdir {}: {e}", parent.display());
                    return;
                }
            }
            match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
            {
                Ok(f) => *g = Some(f),
                Err(e) => {
                    tracing::warn!("checkpoint store: open {}: {e}", self.path.display());
                    return;
                }
            }
        }
        if let Some(f) = g.as_mut() {
            if let Err(e) = writeln!(f, "{line}") {
                tracing::warn!("checkpoint store: write: {e}");
            }
        }
    }

    /// Wrap as a [`CheckpointHook`]. The returned `Arc` can be
    /// passed straight to `RunEngine::set_checkpoint_hook`.
    pub fn into_hook(self: Arc<Self>) -> CheckpointHook {
        Arc::new(move |snap: CheckpointSnapshot| {
            self.append(&CheckpointRecord {
                timestamp_ns: snap.timestamp_ns,
                run_uid: snap.run_uid,
                bsrs_version: env!("CARGO_PKG_VERSION").to_string(),
                exit_status: snap.exit_status,
            });
        })
    }

    /// Return the most recent record from the file (last JSONL line).
    /// `None` if the file is missing or empty. Errors are logged
    /// and surfaced as `None`.
    pub fn latest(path: &Path) -> Option<CheckpointRecord> {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(_) => return None,
        };
        let last = text.lines().rev().find(|l| !l.trim().is_empty())?;
        match serde_json::from_str(last) {
            Ok(r) => Some(r),
            Err(e) => {
                tracing::warn!("checkpoint store: parse last record: {e}");
                None
            }
        }
    }

    /// Walk the file front-to-back and return the most recent record
    /// for a run-uid that hit at least one mid-run `Checkpoint`
    /// (`exit_status = None`) and has **no** subsequent close record
    /// (`exit_status = Some(...)`) for the same run-uid. That
    /// signature is what an abandoned run leaves behind: the engine
    /// reached a safe point and then never closed the run before the
    /// daemon went down.
    ///
    /// Returns `None` if the file is missing, empty, or every
    /// checkpointed run was cleanly closed.
    pub fn unfinished_run(path: &Path) -> Option<CheckpointRecord> {
        let text = std::fs::read_to_string(path).ok()?;
        // Most-recent-wins for each run_uid; close records remove the
        // entry so only abandoned runs remain at the end.
        let mut open: std::collections::HashMap<String, CheckpointRecord> =
            std::collections::HashMap::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let rec: CheckpointRecord = match serde_json::from_str(line) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("checkpoint store: parse record: {e}");
                    continue;
                }
            };
            let Some(uid) = rec.run_uid.clone() else {
                continue;
            };
            if rec.exit_status.is_some() {
                open.remove(&uid);
            } else {
                open.insert(uid, rec);
            }
        }
        // Pick the most recent unfinished entry.
        open.into_values().max_by_key(|r| r.timestamp_ns)
    }
}

/// Default path: `$XDG_STATE_HOME/bsrs/checkpoints.jsonl` if set,
/// else `$HOME/.bsrs/checkpoints.jsonl`.
pub fn default_path() -> PathBuf {
    if let Some(state) = std::env::var_os("XDG_STATE_HOME") {
        let mut p = PathBuf::from(state);
        p.push("bsrs");
        p.push("checkpoints.jsonl");
        return p;
    }
    if let Some(home) = std::env::var_os("HOME") {
        let mut p = PathBuf::from(home);
        p.push(".bsrs");
        p.push("checkpoints.jsonl");
        return p;
    }
    PathBuf::from(".bsrs_checkpoints.jsonl")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_then_latest_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let store = Arc::new(JsonlCheckpointStore::new(&path));
        let hook = store.clone().into_hook();
        hook(CheckpointSnapshot {
            timestamp_ns: 1_000,
            run_uid: Some("r1".into()),
            exit_status: None,
        });
        hook(CheckpointSnapshot {
            timestamp_ns: 2_000,
            run_uid: Some("r2".into()),
            exit_status: None,
        });
        // Force flush by dropping the file handle.
        *store.file.lock().unwrap() = None;
        let last = JsonlCheckpointStore::latest(&path).expect("latest");
        assert_eq!(last.timestamp_ns, 2_000);
        assert_eq!(last.run_uid.as_deref(), Some("r2"));
        assert_eq!(last.bsrs_version, env!("CARGO_PKG_VERSION"));
        assert!(last.exit_status.is_none());
    }

    #[test]
    fn unfinished_run_returns_abandoned_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let store = Arc::new(JsonlCheckpointStore::new(&path));
        let hook = store.clone().into_hook();
        // Run r1: opens, checkpoints, closes cleanly.
        hook(CheckpointSnapshot {
            timestamp_ns: 1_000,
            run_uid: Some("r1".into()),
            exit_status: None,
        });
        hook(CheckpointSnapshot {
            timestamp_ns: 1_500,
            run_uid: Some("r1".into()),
            exit_status: Some("success".into()),
        });
        // Run r2: opens, checkpoints — no close. Daemon went down.
        hook(CheckpointSnapshot {
            timestamp_ns: 2_000,
            run_uid: Some("r2".into()),
            exit_status: None,
        });
        *store.file.lock().unwrap() = None;
        let abandoned = JsonlCheckpointStore::unfinished_run(&path).expect("unfinished");
        assert_eq!(abandoned.run_uid.as_deref(), Some("r2"));
        assert_eq!(abandoned.timestamp_ns, 2_000);
        assert!(abandoned.exit_status.is_none());
    }

    #[test]
    fn unfinished_run_none_when_all_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let store = Arc::new(JsonlCheckpointStore::new(&path));
        let hook = store.clone().into_hook();
        hook(CheckpointSnapshot {
            timestamp_ns: 1_000,
            run_uid: Some("r1".into()),
            exit_status: None,
        });
        hook(CheckpointSnapshot {
            timestamp_ns: 1_500,
            run_uid: Some("r1".into()),
            exit_status: Some("success".into()),
        });
        *store.file.lock().unwrap() = None;
        assert!(JsonlCheckpointStore::unfinished_run(&path).is_none());
    }

    #[test]
    fn unfinished_run_parses_legacy_records_without_exit_status_field() {
        // Pre-existing JSONL files predate the `exit_status` field.
        // They must still parse — and a record without an
        // `exit_status` is interpreted as a mid-run checkpoint, which
        // means an unmatched run_uid in such a file *is* reported as
        // unfinished. Operators are responsible for distinguishing
        // pre-feature noise on first upgrade.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.jsonl");
        std::fs::write(
            &path,
            "{\"timestamp_ns\":1000,\"run_uid\":\"old-run\",\"bsrs_version\":\"0.1.0\"}\n",
        )
        .unwrap();
        let rec = JsonlCheckpointStore::unfinished_run(&path).expect("legacy unfinished");
        assert_eq!(rec.run_uid.as_deref(), Some("old-run"));
        assert!(rec.exit_status.is_none());
    }

    #[test]
    fn latest_handles_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.jsonl");
        assert!(JsonlCheckpointStore::latest(&path).is_none());
    }
}
