//! Disk-backed [`CheckpointHook`] for crash-recovery audit trails.
//!
//! `Msg::Checkpoint` records are appended to a JSONL file (default
//! `~/.bsrs/checkpoints.jsonl`); consecutive checkpoints of the same
//! run are coalesced to at most one record per second, so file growth
//! is bounded by plan wall-clock time, not event rate (a per-point
//! checkpointing plan at engine speed would otherwise write millions
//! of lines per minute). Every `CloseRun` appends a paired record with
//! `exit_status` set, always. On daemon restart, `manager.rs` calls
//! [`JsonlCheckpointStore::unfinished_run`] to detect a run that
//! opened, hit at least one checkpoint, but never emitted a paired
//! close — i.e. was abandoned when the daemon went down.
//!
//! Readers ([`JsonlCheckpointStore::latest`] / `unfinished_run`) look
//! only at the file **tail**: runs are serialized per daemon, so the
//! last record *is* the daemon's final state. Boot cost is therefore
//! independent of journal size.
//!
//! Full crash-recovery (resume the plan from the last checkpoint) is
//! still deferred — it requires plan-arg persistence and msg_cache
//! replay which are deeper concerns. The pieces here give an operator
//! the data needed to *detect* the unfinished run and decide whether
//! to re-issue the plan manually.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
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

/// Minimum spacing between two appended mid-run checkpoint records of
/// the same run. Close records (`exit_status: Some`) and the first
/// checkpoint of a run are never throttled.
const MID_RUN_APPEND_INTERVAL_NS: u64 = 1_000_000_000;

/// How far back from EOF the tail readers scan for the last complete
/// record. ~600 records at the current record size — far more than the
/// one complete line they need even with a torn final write.
const TAIL_SCAN_BYTES: u64 = 64 * 1024;

/// Append-only checkpoint store. The file is opened lazily on the
/// first append and held for the daemon's lifetime; OS writeback
/// flushes the line buffer.
pub struct JsonlCheckpointStore {
    path: PathBuf,
    /// `None` until the first append succeeds. Behind a mutex so the
    /// `CheckpointHook` (Fn) can mutate it.
    file: StdMutex<Option<std::fs::File>>,
    /// `(run_uid, timestamp_ns)` of the last appended **mid-run**
    /// checkpoint — the throttle state for coalescing.
    last_mid_run: StdMutex<Option<(Option<String>, u64)>>,
}

impl JsonlCheckpointStore {
    /// Build a store at `path`. The file is not opened yet.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            file: StdMutex::new(None),
            last_mid_run: StdMutex::new(None),
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
    ///
    /// Mid-run checkpoints of the same run are coalesced to one
    /// appended record per [`MID_RUN_APPEND_INTERVAL_NS`]; the first
    /// checkpoint of a run and every close record (`exit_status:
    /// Some`) are appended unconditionally.
    pub fn into_hook(self: Arc<Self>) -> CheckpointHook {
        Arc::new(move |snap: CheckpointSnapshot| {
            if snap.exit_status.is_none() {
                let mut last = self.last_mid_run.lock().unwrap();
                if let Some((uid, ts)) = last.as_ref() {
                    if *uid == snap.run_uid
                        && snap.timestamp_ns.saturating_sub(*ts) < MID_RUN_APPEND_INTERVAL_NS
                    {
                        return; // coalesced
                    }
                }
                *last = Some((snap.run_uid.clone(), snap.timestamp_ns));
            } else {
                // Run ended: reset so the next run's first checkpoint
                // always lands.
                *self.last_mid_run.lock().unwrap() = None;
            }
            self.append(&CheckpointRecord {
                timestamp_ns: snap.timestamp_ns,
                run_uid: snap.run_uid,
                bsrs_version: env!("CARGO_PKG_VERSION").to_string(),
                exit_status: snap.exit_status,
            });
        })
    }

    /// Return the most recent record (last complete JSONL line),
    /// reading only the file tail — cost is independent of journal
    /// size. `None` if the file is missing or empty; a torn final
    /// line (crash mid-write) falls back to the previous complete
    /// record. Errors are logged and surfaced as `None`.
    pub fn latest(path: &Path) -> Option<CheckpointRecord> {
        let tail = match read_tail(path, TAIL_SCAN_BYTES) {
            Ok(t) => t,
            Err(_) => return None,
        };
        for line in tail.lines().rev().filter(|l| !l.trim().is_empty()) {
            match serde_json::from_str(line) {
                Ok(r) => return Some(r),
                Err(e) => {
                    tracing::warn!("checkpoint store: parse tail record: {e}");
                }
            }
        }
        None
    }

    /// Return the abandoned run the journal ends in, if any: the run
    /// is unfinished iff the **last** record is a mid-run checkpoint
    /// (`exit_status = None`) with a run uid. Runs are serialized per
    /// daemon, so the journal tail is the daemon's final state — a
    /// run that was abandoned but *followed by later runs* is
    /// superseded and no longer reported (the crash was already
    /// surfaced at the boot right after it happened).
    ///
    /// Returns `None` if the file is missing, empty, or the last
    /// checkpointed run was cleanly closed.
    pub fn unfinished_run(path: &Path) -> Option<CheckpointRecord> {
        Self::latest(path).filter(|r| r.exit_status.is_none() && r.run_uid.is_some())
    }
}

/// Read at most `max_bytes` from the end of the file as (lossy) UTF-8.
fn read_tail(path: &Path, max_bytes: u64) -> std::io::Result<String> {
    let mut f = std::fs::File::open(path)?;
    let len = f.metadata()?.len();
    let start = len.saturating_sub(max_bytes);
    f.seek(SeekFrom::Start(start))?;
    let mut buf = Vec::with_capacity((len - start) as usize);
    f.read_to_end(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
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

    fn mid(uid: &str, ts: u64) -> CheckpointSnapshot {
        CheckpointSnapshot {
            timestamp_ns: ts,
            run_uid: Some(uid.into()),
            exit_status: None,
        }
    }

    fn file_lines(path: &Path) -> usize {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .count()
    }

    #[test]
    fn mid_run_checkpoints_coalesce_within_interval() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let store = Arc::new(JsonlCheckpointStore::new(&path));
        let hook = store.clone().into_hook();
        // Sub-second checkpoints of one run collapse into the first.
        hook(mid("r1", 0));
        hook(mid("r1", 100_000_000));
        hook(mid("r1", 200_000_000));
        // ≥1 s after the last APPENDED record: lands.
        hook(mid("r1", 1_100_000_000));
        // Close always lands.
        hook(CheckpointSnapshot {
            timestamp_ns: 1_200_000_000,
            run_uid: Some("r1".into()),
            exit_status: Some("success".into()),
        });
        *store.file.lock().unwrap() = None;
        assert_eq!(file_lines(&path), 3, "first + 1s-later + close");
        let last = JsonlCheckpointStore::latest(&path).unwrap();
        assert_eq!(last.exit_status.as_deref(), Some("success"));
    }

    #[test]
    fn run_change_bypasses_the_throttle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let store = Arc::new(JsonlCheckpointStore::new(&path));
        let hook = store.clone().into_hook();
        hook(mid("r1", 0));
        hook(mid("r2", 100_000_000)); // new run: not throttled
        *store.file.lock().unwrap() = None;
        assert_eq!(file_lines(&path), 2);
    }

    #[test]
    fn superseded_abandoned_run_is_not_reported() {
        // r1 was abandoned, but r2 ran and closed cleanly afterwards:
        // the journal ends in a clean close, so boot reports nothing.
        // (The whole-file semantics this replaces re-warned about r1
        // on every boot forever.)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let store = Arc::new(JsonlCheckpointStore::new(&path));
        let hook = store.clone().into_hook();
        hook(mid("r1", 1_000));
        hook(mid("r2", 2_000_000_000));
        hook(CheckpointSnapshot {
            timestamp_ns: 3_000_000_000,
            run_uid: Some("r2".into()),
            exit_status: Some("success".into()),
        });
        *store.file.lock().unwrap() = None;
        assert!(JsonlCheckpointStore::unfinished_run(&path).is_none());
    }

    #[test]
    fn tail_read_finds_last_record_in_a_journal_larger_than_the_scan_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.jsonl");
        let mut text = String::new();
        for i in 0..2_000u64 {
            text.push_str(&format!(
                "{{\"timestamp_ns\":{},\"run_uid\":\"r{}\",\"bsrs_version\":\"0.3.0\"}}\n",
                i * 1_000,
                i
            ));
        }
        assert!(
            text.len() as u64 > TAIL_SCAN_BYTES,
            "must exceed the window"
        );
        std::fs::write(&path, text).unwrap();
        let last = JsonlCheckpointStore::latest(&path).expect("latest");
        assert_eq!(last.run_uid.as_deref(), Some("r1999"));
    }

    #[test]
    fn torn_final_line_falls_back_to_previous_record() {
        // Crash mid-write leaves a partial last line with no newline.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("torn.jsonl");
        std::fs::write(
            &path,
            "{\"timestamp_ns\":1000,\"run_uid\":\"good\",\"bsrs_version\":\"0.3.0\"}\n\
             {\"timestamp_ns\":2000,\"run_",
        )
        .unwrap();
        let last = JsonlCheckpointStore::latest(&path).expect("fallback record");
        assert_eq!(last.run_uid.as_deref(), Some("good"));
    }
}
