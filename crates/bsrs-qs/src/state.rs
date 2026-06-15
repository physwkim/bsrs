//! Engine / queue state machine.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Public engine state — values match `bluesky_queueserver.manager.MState`
/// so qserver CLI displays them naturally.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EState {
    /// Engine not yet opened.
    EnvironmentClosed,
    /// Engine open, no plan running.
    Idle,
    /// A plan is executing.
    ExecutingQueue,
    /// Engine paused.
    Paused,
    /// Abort requested, plan winding down.
    Aborting,
    /// environment_open in progress (transitional).
    CreatingEnvironment,
    /// environment_close in progress (transitional).
    ClosingEnvironment,
    /// environment_destroy in progress (transitional).
    DestroyingEnvironment,
}

impl EState {
    /// Stringified for status JSON (matches MState.value in manager.py).
    pub fn as_str(&self) -> &'static str {
        match self {
            EState::EnvironmentClosed => "environment_closed",
            EState::Idle => "idle",
            EState::ExecutingQueue => "executing_queue",
            EState::Paused => "paused",
            EState::Aborting => "aborting",
            EState::CreatingEnvironment => "creating_environment",
            EState::ClosingEnvironment => "closing_environment",
            EState::DestroyingEnvironment => "destroying_environment",
        }
    }
}

/// Lock state, mirroring bluesky's `lock_info`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LockInfo {
    /// Whether queue operations are locked.
    pub queue: bool,
    /// Whether environment operations are locked.
    pub environment: bool,
    /// User who installed the lock.
    pub user: Option<String>,
    /// Free-form note from the locker.
    pub note: Option<String>,
    /// ISO 8601 timestamp the lock was applied.
    pub time: Option<String>,
    /// Stable UID; bumps every state change.
    pub uid: String,
    /// Lock key hash (we don't echo the raw key).
    #[serde(skip)]
    pub key_hash: Option<u64>,
}

impl LockInfo {
    /// Build empty / unlocked.
    pub fn unlocked() -> Self {
        Self {
            uid: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        }
    }
    /// Apply a new lock — bumps the UID.
    pub fn lock(
        &mut self,
        environment: bool,
        queue: bool,
        user: Option<String>,
        note: Option<String>,
        key_hash: u64,
    ) {
        self.environment = environment;
        self.queue = queue;
        self.user = user;
        self.note = note;
        self.time = Some(now_iso8601());
        self.key_hash = Some(key_hash);
        self.uid = uuid::Uuid::new_v4().to_string();
    }
    /// Clear the lock — bumps the UID.
    pub fn clear(&mut self) {
        self.environment = false;
        self.queue = false;
        self.user = None;
        self.note = None;
        self.time = None;
        self.key_hash = None;
        self.uid = uuid::Uuid::new_v4().to_string();
    }
    /// True if either subsystem is locked.
    pub fn is_locked(&self) -> bool {
        self.environment || self.queue
    }
}

pub(crate) fn now_iso8601() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    // Cheap UTC ISO8601 without chrono.
    let (y, mo, d, h, mi, s) = secs_to_ymdhms(secs as i64);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, mi, s)
}

fn secs_to_ymdhms(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    // Unix epoch arithmetic; good enough for log timestamps.
    let days = secs.div_euclid(86_400);
    let r = secs.rem_euclid(86_400) as u32;
    let (h, mi, s) = (r / 3600, (r / 60) % 60, r % 60);
    // Compute date from epoch days (1970-01-01 = day 0).
    let mut y = 1970_i32;
    let mut d = days;
    loop {
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let dy = if leap { 366 } else { 365 };
        if d < dy {
            break;
        }
        d -= dy;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let mdays: [i64; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut mo = 1_u32;
    for &md in &mdays {
        if d < md {
            break;
        }
        d -= md;
        mo += 1;
    }
    (y, mo, (d as u32) + 1, h, mi, s)
}

/// Snapshot of the engine state shared across handlers.
#[derive(Clone, Debug)]
pub struct EngineState {
    /// Manager state.
    pub state: Option<EState>,
    /// UID of the current run, if any.
    pub current_run_uid: Option<String>,
    /// Plan name currently running.
    pub current_plan_name: Option<String>,
    /// Pending queue length.
    pub queue_len: usize,
    /// History length.
    pub history_len: usize,
    /// Total plans run this session.
    pub plans_run: u64,
    /// Total plans failed.
    pub plans_failed: u64,
    /// Recent runs as (uid, is_open) pairs (most-recent last).
    pub re_runs: Vec<(String, bool)>,
    /// Persistent metadata that mirrors `RE.md`. Returned by
    /// `re_metadata` and merged into RunMetadata at OpenRun.
    pub re_metadata: HashMap<String, serde_json::Value>,
    /// `queue_stop` requested but not yet honored.
    pub queue_stop_pending: bool,
    /// `re_pause` accepted but the plan has not yet reached a checkpoint.
    pub pause_pending: bool,
    /// Count of background tasks (lua_eval) currently executing.
    pub worker_background_tasks: u64,
    /// Whether `queue_autostart` is enabled.
    pub queue_autostart_enabled: bool,
    /// Queue execution mode (`{"loop": false}` etc.).
    pub queue_mode: HashMap<String, serde_json::Value>,
    /// Lock state.
    pub lock: LockInfo,
}

impl Default for EngineState {
    fn default() -> Self {
        Self {
            state: None,
            current_run_uid: None,
            current_plan_name: None,
            queue_len: 0,
            history_len: 0,
            plans_run: 0,
            plans_failed: 0,
            re_runs: Vec::new(),
            re_metadata: HashMap::new(),
            queue_stop_pending: false,
            pause_pending: false,
            worker_background_tasks: 0,
            queue_autostart_enabled: false,
            queue_mode: HashMap::from([("loop".into(), serde_json::Value::Bool(false))]),
            lock: LockInfo::unlocked(),
        }
    }
}

impl EngineState {
    /// Build the initial state (engine closed).
    pub fn initial() -> Self {
        Self {
            state: Some(EState::EnvironmentClosed),
            ..Default::default()
        }
    }
}
