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
//! The file is also **size-rotated**: when the current file reaches
//! `max_bytes` (default [`DEFAULT_MAX_BYTES`]) it is renamed to a
//! single `.1` backup and a fresh file is started, so on-disk size is
//! bounded by `~2 * max_bytes` forever — regardless of how long the
//! daemon runs. Rotation happens under the writer mutex inside
//! `append`, the sole writer, so no record can interleave with the
//! rename. Because rotation always precedes the triggering write, the
//! newest record is always in the current file during normal
//! operation; the readers fall back to `.1` only for the rare crash
//! that lands in the rename/write window.
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

/// Default per-file rotation threshold (4 MiB). At the coalesced
/// mid-run rate (~1 line/s while executing) this holds on the order of
/// a day of continuous running per file; total on-disk journal is
/// bounded by twice this (current file + one `.1` backup).
pub const DEFAULT_MAX_BYTES: u64 = 4 * 1024 * 1024;

/// The open write handle and its current byte count, guarded together
/// so rotation cannot leave them inconsistent.
struct Writer {
    /// `None` until the first append (or after a rotation) opens the
    /// current file.
    file: Option<std::fs::File>,
    /// Bytes in the current file. Seeded from the file's length on
    /// open and advanced by each successful write; the rotation gate.
    bytes: u64,
}

/// Size-rotated, append-only checkpoint store. The current file is
/// opened lazily on the first append and held until it reaches
/// `max_bytes`, at which point [`append`](Self::append) rotates it to
/// a single `.1` backup and starts fresh. OS writeback flushes the
/// line buffer.
pub struct JsonlCheckpointStore {
    path: PathBuf,
    /// Per-file size cap before rotation (`>= 1`).
    max_bytes: u64,
    /// Open handle + byte count. Behind a mutex so the
    /// `CheckpointHook` (Fn) can mutate it; also the rotation lock.
    writer: StdMutex<Writer>,
    /// `(run_uid, timestamp_ns)` of the last appended **mid-run**
    /// checkpoint — the throttle state for coalescing.
    last_mid_run: StdMutex<Option<(Option<String>, u64)>>,
}

impl JsonlCheckpointStore {
    /// Build a store at `path` with the default rotation threshold
    /// ([`DEFAULT_MAX_BYTES`]). The file is not opened yet.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_max_bytes(path, DEFAULT_MAX_BYTES)
    }

    /// Build a store with an explicit per-file rotation threshold.
    /// When the current file reaches `max_bytes` it is rotated to a
    /// single `.1` backup and a fresh file is started, bounding
    /// on-disk size to `~2 * max_bytes`. `max_bytes` is clamped to at
    /// least 1 so rotation always makes progress.
    pub fn with_max_bytes(path: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            path: path.into(),
            max_bytes: max_bytes.max(1),
            writer: StdMutex::new(Writer {
                file: None,
                bytes: 0,
            }),
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
        let mut w = self.writer.lock().unwrap();
        if !self.ensure_open(&mut w) {
            return;
        }
        // Rotate *before* writing so the record that crosses the
        // threshold starts the fresh file: the current file is capped
        // at max_bytes (+ the one record that first crossed), and
        // total disk (current + `.1`) stays within ~2 * max_bytes.
        if w.bytes >= self.max_bytes {
            self.rotate(&mut w);
            if !self.ensure_open(&mut w) {
                return;
            }
        }
        if let Some(f) = w.file.as_mut() {
            match writeln!(f, "{line}") {
                Ok(()) => w.bytes += line.len() as u64 + 1,
                Err(e) => tracing::warn!("checkpoint store: write: {e}"),
            }
        }
    }

    /// Ensure the writer holds an open handle to the current path,
    /// seeding the byte counter from the existing file length (so a
    /// restart appending to a partially-filled — or already
    /// oversized — file rotates correctly). Returns false (logged) if
    /// the file cannot be opened.
    fn ensure_open(&self, w: &mut Writer) -> bool {
        if w.file.is_some() {
            return true;
        }
        if let Some(parent) = self.path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::warn!("checkpoint store: mkdir {}: {e}", parent.display());
                return false;
            }
        }
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(f) => {
                w.bytes = f.metadata().map(|m| m.len()).unwrap_or(0);
                w.file = Some(f);
                true
            }
            Err(e) => {
                tracing::warn!("checkpoint store: open {}: {e}", self.path.display());
                false
            }
        }
    }

    /// Move the current file to the `.1` backup (overwriting any prior
    /// backup) and drop the handle so the next `ensure_open` starts a
    /// fresh file. Called only from `append` with the writer mutex
    /// held, so no record interleaves with the rename. A rename
    /// failure is non-fatal and logged: the next `ensure_open`
    /// re-opens the same path in append mode (keeping its records),
    /// so growth stays visible rather than silently unbounded.
    fn rotate(&self, w: &mut Writer) {
        // Drop the handle first so the rename detaches a closed file.
        w.file = None;
        w.bytes = 0;
        let backup = backup_path(&self.path);
        if let Err(e) = std::fs::rename(&self.path, &backup) {
            tracing::warn!(
                "checkpoint store: rotate {} -> {}: {e}",
                self.path.display(),
                backup.display()
            );
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
    /// size. Consults the current file first, then the `.1` backup:
    /// the backup only ever wins when the current file is
    /// missing/empty, which during normal operation happens solely in
    /// the crash-in-rotation window (rename done, fresh file not yet
    /// written), so the fallback recovers the stranded final state
    /// without ever masking a live record. A torn final line (crash
    /// mid-write) falls back to the previous complete record. `None`
    /// if neither file has a parseable record.
    pub fn latest(path: &Path) -> Option<CheckpointRecord> {
        tail_record(path).or_else(|| tail_record(&backup_path(path)))
    }

    /// Return the abandoned run the journal ends in, if any: the run
    /// is unfinished iff the **last** record ([`latest`](Self::latest),
    /// including the `.1`-backup fallback) is a mid-run checkpoint
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

/// The single rotation backup path for `path`: `<path>.1`.
fn backup_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".1");
    PathBuf::from(s)
}

/// Parse the last complete JSONL record from a file's tail. Reads at
/// most the final [`TAIL_SCAN_BYTES`]; skips a torn final line.
/// `None` if the file is missing, empty, or has no parseable record
/// in the window.
fn tail_record(path: &Path) -> Option<CheckpointRecord> {
    let tail = read_tail(path, TAIL_SCAN_BYTES).ok()?;
    for line in tail.lines().rev().filter(|l| !l.trim().is_empty()) {
        match serde_json::from_str(line) {
            Ok(r) => return Some(r),
            Err(e) => tracing::warn!("checkpoint store: parse tail record: {e}"),
        }
    }
    None
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

impl JsonlCheckpointStore {
    /// Drop the write handle (flushing it) so a test can read the file
    /// back through the OS. Real callers hold the store for the
    /// daemon's lifetime and rely on OS writeback.
    #[cfg(test)]
    fn close_for_test(&self) {
        self.writer.lock().unwrap().file = None;
    }
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
        store.close_for_test();
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
        store.close_for_test();
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
        store.close_for_test();
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
        store.close_for_test();
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
        store.close_for_test();
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
        store.close_for_test();
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

    #[test]
    fn rotation_bounds_disk_and_keeps_one_backup() {
        // Tiny cap so a handful of ~67-byte records trigger rotation.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let store = Arc::new(JsonlCheckpointStore::with_max_bytes(&path, 200));
        let hook = store.clone().into_hook();
        // 20 mid-run checkpoints spaced ≥1 s so none coalesce; far more
        // than two capped files can hold, so old records must be dropped.
        for i in 0..20u64 {
            hook(mid("r1", i * 1_000_000_000));
        }
        store.close_for_test();

        let cur = std::fs::metadata(&path).unwrap().len();
        assert!(cur <= 2 * 200, "current file exceeds the cap: {cur} bytes");
        let backup = backup_path(&path);
        assert!(backup.exists(), "rotation must leave a .1 backup");
        let backup_len = std::fs::metadata(&backup).unwrap().len();
        assert!(
            backup_len <= 2 * 200,
            "backup exceeds the cap: {backup_len} bytes"
        );
        // Old records were dropped, not merely split across more files.
        let total = file_lines(&path) + file_lines(&backup);
        assert!(total < 20, "expected bounded retention, got {total} lines");
        // The reader still finds the newest record after rotations.
        let last = JsonlCheckpointStore::latest(&path).expect("latest after rotation");
        assert_eq!(last.timestamp_ns, 19_000_000_000);
    }

    #[test]
    fn unfinished_detection_survives_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let store = Arc::new(JsonlCheckpointStore::with_max_bytes(&path, 200));
        let hook = store.clone().into_hook();
        // Many rotations, ending mid-run (no close) — daemon went down.
        for i in 0..30u64 {
            hook(mid("run-x", i * 1_000_000_000));
        }
        store.close_for_test();
        let ab = JsonlCheckpointStore::unfinished_run(&path).expect("unfinished after rotation");
        assert_eq!(ab.run_uid.as_deref(), Some("run-x"));
        assert_eq!(ab.timestamp_ns, 29_000_000_000);
    }

    #[test]
    fn oversized_existing_file_rotates_on_first_append() {
        // Pre-seed a file already over the cap (legacy giant journal, or
        // a lowered threshold): the first append must rotate it away.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let mut big = String::new();
        for i in 0..100u64 {
            big.push_str(&format!(
                "{{\"timestamp_ns\":{i},\"run_uid\":\"old\",\"bsrs_version\":\"0.1.0\"}}\n"
            ));
        }
        std::fs::write(&path, &big).unwrap();
        let over = std::fs::metadata(&path).unwrap().len();

        let store = Arc::new(JsonlCheckpointStore::with_max_bytes(&path, 200));
        let hook = store.clone().into_hook();
        hook(mid("new", 5_000_000_000));
        store.close_for_test();

        // The oversized content moved intact to `.1`; the current file
        // holds only the new record.
        let backup = backup_path(&path);
        assert_eq!(std::fs::metadata(&backup).unwrap().len(), over);
        assert_eq!(file_lines(&path), 1);
        let last = JsonlCheckpointStore::latest(&path).unwrap();
        assert_eq!(last.run_uid.as_deref(), Some("new"));
    }

    #[test]
    fn reader_falls_back_to_backup_when_current_is_empty() {
        // Crash in the rotation window: current file exists but is
        // empty (rename done, fresh file created, killed before the
        // write); the stranded final state lives in `.1`.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.jsonl");
        let backup = backup_path(&path);
        std::fs::write(
            &backup,
            "{\"timestamp_ns\":7000,\"run_uid\":\"stranded\",\"bsrs_version\":\"0.3.0\"}\n",
        )
        .unwrap();
        std::fs::write(&path, "").unwrap(); // empty current file
        let ab = JsonlCheckpointStore::unfinished_run(&path).expect("must fall back to backup");
        assert_eq!(ab.run_uid.as_deref(), Some("stranded"));
    }
}
