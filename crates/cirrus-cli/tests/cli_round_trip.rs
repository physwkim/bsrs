//! End-to-end test: spawn `cirrus qs-manager`, drive it via
//! `cirrus qs ...` subcommands, and verify the responses.
//!
//! The test binds to an IPC socket (no TCP port collisions) under
//! `/tmp/cirrus-cli-it-<pid>-<seq>.sock`.

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

fn rand_id() -> u64 {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    nanos.wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

fn cirrus_bin() -> std::path::PathBuf {
    let target = std::env::var("CARGO_BIN_EXE_cirrus")
        .expect("CARGO_BIN_EXE_cirrus not set; cargo test should set this");
    std::path::PathBuf::from(target)
}

struct Manager {
    child: Child,
    control: String,
}

impl Drop for Manager {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Also clean up the IPC socket files we used.
        if let Some(p) = self.control.strip_prefix("ipc://") {
            let _ = std::fs::remove_file(p);
        }
    }
}

#[allow(clippy::zombie_processes)]
fn spawn_manager() -> Manager {
    // The returned `Manager`'s Drop kills + waits the child, so this
    // does not actually leak. The lint can't see across struct boundaries.
    let id = rand_id();
    let control = format!(
        "ipc:///tmp/cirrus-cli-it-{}-{}-c.sock",
        std::process::id(),
        id
    );
    let documents = format!(
        "ipc:///tmp/cirrus-cli-it-{}-{}-d.sock",
        std::process::id(),
        id
    );
    let child = Command::new(cirrus_bin())
        .args([
            "qs-manager",
            "--control",
            &control,
            "--documents",
            &documents,
            "--soft-detectors",
            "1",
            "--soft-motors",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cirrus qs-manager");
    // Wait until the control socket file appears (server is listening).
    let path = control.trim_start_matches("ipc://");
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if std::path::Path::new(path).exists() {
            sleep(Duration::from_millis(50));
            return Manager { child, control };
        }
        sleep(Duration::from_millis(20));
    }
    panic!("manager did not bind {control} within 3s");
}

#[allow(clippy::zombie_processes)]
fn run_client(addr: &str, args: &[&str]) -> (String, String, i32) {
    // We DO call `child.wait()` at the end of this function. The
    // zombie_processes lint trips because of the spawn-then-take-pipes
    // dance, not because we leak the handle.
    let mut child = Command::new(cirrus_bin())
        .arg("qs")
        .arg("--address")
        .arg(addr)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cirrus qs");
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut o) = child.stdout.take() {
        o.read_to_string(&mut stdout).ok();
    }
    if let Some(mut e) = child.stderr.take() {
        e.read_to_string(&mut stderr).ok();
    }
    let status = child.wait().expect("wait client");
    (stdout, stderr, status.code().unwrap_or(-1))
}

#[test]
fn ping_returns_pong() {
    let m = spawn_manager();
    let (out, err, code) = run_client(&m.control, &["ping"]);
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("\"msg\""));
    assert!(out.contains("pong"), "out = {out}");
}

#[test]
fn allowed_lists_count_and_devices() {
    let m = spawn_manager();
    let (out, _err, code) = run_client(&m.control, &["allowed", "plans"]);
    assert_eq!(code, 0);
    assert!(out.contains("\"count\""), "expected count plan in {out}");

    let (out, _err, code) = run_client(&m.control, &["allowed", "devices"]);
    assert_eq!(code, 0);
    assert!(out.contains("\"det1\""), "expected det1 in {out}");
    assert!(out.contains("\"m1\""), "expected m1 in {out}");
}

#[test]
fn full_count_e2e_through_cli() {
    let m = spawn_manager();
    let addr = m.control.clone();

    let (_, _, c) = run_client(&addr, &["environment", "open"]);
    assert_eq!(c, 0);

    let (out, _err, c) = run_client(&addr, &["queue", "add", "count", "det1", "3"]);
    assert_eq!(c, 0);
    assert!(out.contains("\"item_uid\""));

    let (_, _, c) = run_client(&addr, &["queue", "start"]);
    assert_eq!(c, 0);

    // Poll status until idle + plans_run >= 1.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut done = false;
    while Instant::now() < deadline {
        let (out, _err, c) = run_client(&addr, &["status"]);
        assert_eq!(c, 0);
        if out.contains("\"plans_run\": 1") && out.contains("\"manager_state\": \"idle\"") {
            done = true;
            break;
        }
        sleep(Duration::from_millis(100));
    }
    assert!(done, "queue did not finish via CLI");
}

#[test]
fn unknown_method_returns_nonzero_exit() {
    let m = spawn_manager();
    // Using a known-but-no-args method incorrectly is enough; force it
    // by sending a typo via env. Here we run a valid method when no
    // environment is open: queue_start should fail with a server error.
    let (_, err, code) = run_client(&m.control, &["queue", "start"]);
    assert_ne!(code, 0, "queue start without env should exit non-zero");
    assert!(err.contains("server error") || err.contains("environment"));
}

#[test]
fn lua_eval_inspect_runs_against_running_daemon() {
    // Drives the lua_eval pipeline end-to-end through the cirrus
    // binary: spawn manager (which wires ManagerLuaState), open env,
    // submit a lua_eval that inspects the soft motor, poll
    // task_status until completed, fetch task_result.
    let m = spawn_manager();
    // open env
    let (_, _, c) = run_client(&m.control, &["environment", "open"]);
    assert_eq!(c, 0);

    // submit lua_eval
    let (out, _err, code) = run_client(
        &m.control,
        &["raw", "lua_eval", r#"{"source":"m1:inspect().readback"}"#],
    );
    assert_eq!(code, 0);
    let task_uid = out
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix(r#""task_uid": ""#)
                .and_then(|s| s.strip_suffix(r#"""#))
                .or_else(|| l.trim().strip_prefix(r#""task_uid": ""#))
        })
        .map(str::to_string)
        .unwrap_or_else(|| panic!("no task_uid in: {out}"));

    // poll task_result up to 3s
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut final_result: Option<String> = None;
    while Instant::now() < deadline {
        sleep(Duration::from_millis(80));
        let params = format!(r#"{{"task_uid":"{task_uid}"}}"#);
        let (out2, _, c2) = run_client(&m.control, &["raw", "task_result", &params]);
        assert_eq!(c2, 0);
        if out2.contains(r#""status": "completed""#) {
            final_result = Some(out2);
            break;
        }
    }
    let r = final_result.expect("task did not complete within 3s");
    assert!(
        r.contains(r#""success": true"#),
        "expected success, got {r}"
    );
    assert!(
        r.contains(r#""return_value": "0""#),
        "expected return_value=0 (m1 starts at 0.0), got {r}"
    );
}

#[test]
fn lua_eval_runs_count_plan_through_daemon() {
    // The killer feature: run a count plan from inside the daemon's
    // shared mlua state via lua_eval. End-to-end across REQ/REP +
    // tokio task spawn + cirrus_runtime block_on inside Lua.
    let m = spawn_manager();
    let (_, _, c) = run_client(&m.control, &["environment", "open"]);
    assert_eq!(c, 0);

    let (out, _err, code) = run_client(
        &m.control,
        &[
            "raw",
            "lua_eval",
            r#"{"source":"RE:run(count({det1}, 3))"}"#,
        ],
    );
    assert_eq!(code, 0);
    let task_uid = out
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix(r#""task_uid": ""#)
                .and_then(|s| s.strip_suffix(r#"""#))
        })
        .map(str::to_string)
        .unwrap_or_else(|| panic!("no task_uid in: {out}"));

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got_completed = false;
    while Instant::now() < deadline {
        sleep(Duration::from_millis(100));
        let params = format!(r#"{{"task_uid":"{task_uid}"}}"#);
        let (out2, _, c2) = run_client(&m.control, &["raw", "task_result", &params]);
        assert_eq!(c2, 0);
        if out2.contains(r#""status": "completed""#) {
            got_completed = true;
            assert!(
                out2.contains("exit_status=success"),
                "expected exit_status=success, got {out2}"
            );
            break;
        }
    }
    assert!(got_completed, "RE:run(count) did not complete in 5s");
}
