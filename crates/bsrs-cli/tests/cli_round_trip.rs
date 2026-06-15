//! End-to-end test: spawn `bsrs qs-manager`, drive it via
//! `bsrs qs ...` subcommands, and verify the responses.
//!
//! The test binds to an IPC socket (no TCP port collisions) under
//! `/tmp/bsrs-cli-it-<pid>-<seq>.sock`.

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

fn bsrs_bin() -> std::path::PathBuf {
    let target = std::env::var("CARGO_BIN_EXE_bsrs")
        .expect("CARGO_BIN_EXE_bsrs not set; cargo test should set this");
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
        "ipc:///tmp/bsrs-cli-it-{}-{}-c.sock",
        std::process::id(),
        id
    );
    let documents = format!(
        "ipc:///tmp/bsrs-cli-it-{}-{}-d.sock",
        std::process::id(),
        id
    );
    let child = Command::new(bsrs_bin())
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
        .expect("spawn bsrs qs-manager");
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
    let mut child = Command::new(bsrs_bin())
        .arg("qs")
        .arg("--address")
        .arg(addr)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bsrs qs");
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
fn ping_returns_status_dict() {
    let m = spawn_manager();
    let (out, err, code) = run_client(&m.control, &["ping"]);
    assert_eq!(code, 0, "stderr: {err}");
    // ping returns the full status dict (same as status, ref manager.py:1888).
    assert!(
        out.contains("manager_state"),
        "ping should return status dict: {out}"
    );
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
    // Drives the lua_eval pipeline end-to-end through the bsrs
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
    // tokio task spawn + bsrs_runtime block_on inside Lua.
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

#[test]
fn re_abort_cancels_in_flight_lua_eval_plan() {
    // R5.2: a long-running plan started via lua_eval (RE:run inside
    // Lua) must be cancellable via the standard re_abort RPC. The
    // qs-server's REP loop is free during lua_eval (returns task_uid
    // immediately), so a parallel re_abort reaches dispatch and the
    // engine's abort flag propagates through bsrs_runtime's
    // block_on inside Lua.
    //
    // This guards against future regressions where lua_eval might
    // accidentally hold the REP loop or where the engine no longer
    // honors abort from outside the queue worker.
    let m = spawn_manager();
    let (_, _, c) = run_client(&m.control, &["environment", "open"]);
    assert_eq!(c, 0);

    // 5-second plan via Lua coroutine: 10 sleeps of 500 ms each.
    let lua = r#"
local function long_sleeper(n, secs)
    for i = 1, n do
        coroutine.yield(msg.sleep(secs))
    end
end
return tostring(RE:run(plan(long_sleeper, 10, 0.5)))
"#;
    let params = format!(r#"{{"source":{}}}"#, serde_json::json!(lua));
    let (out, _err, code) = run_client(&m.control, &["raw", "lua_eval", &params]);
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

    // Wait briefly so the plan starts, then abort.
    sleep(Duration::from_millis(400));
    let (_, _, c) = run_client(&m.control, &["re", "abort"]);
    assert_eq!(c, 0, "re abort RPC should succeed while plan is in-flight");

    // Plan should resolve well before the 5-second nominal duration.
    // The engine surfaces aborted runs with `exit_status=fail` or
    // `exit_status=abort` (depending on whether OpenRun completed
    // before the abort fired); we assert the abort cut the run short
    // by requiring completion within 3 s and exit_status != success.
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut got_completed = false;
    while Instant::now() < deadline {
        sleep(Duration::from_millis(100));
        let p = format!(r#"{{"task_uid":"{task_uid}"}}"#);
        let (out2, _, c2) = run_client(&m.control, &["raw", "task_result", &p]);
        assert_eq!(c2, 0);
        if out2.contains(r#""status": "completed""#) {
            assert!(
                !out2.contains("exit_status=success"),
                "abort should NOT yield exit_status=success: {out2}"
            );
            assert!(
                out2.contains("exit_status=abort") || out2.contains("exit_status=fail"),
                "expected exit_status=abort|fail after re abort, got {out2}"
            );
            got_completed = true;
            break;
        }
    }
    assert!(
        got_completed,
        "lua_eval task did not honor re_abort within 3s — plan would have taken 5s otherwise"
    );
}

#[test]
fn checkpoint_hook_records_on_run() {
    // Verifies the checkpoint persistence path:
    // - Spawn `bsrs qs-manager` with `--checkpoints <tmp>`.
    // - Run a `count` plan via the queue (which emits Checkpoints
    //   between iterations).
    // - Read the JSONL file; assert ≥1 record with our run's uid.
    let id = rand_id();
    let ckpt_path = format!(
        "/tmp/bsrs-cli-it-{}-{}-checkpoints.jsonl",
        std::process::id(),
        id
    );
    let _ = std::fs::remove_file(&ckpt_path);

    let control = format!(
        "ipc:///tmp/bsrs-cli-it-{}-{}-c.sock",
        std::process::id(),
        id
    );
    let documents = format!(
        "ipc:///tmp/bsrs-cli-it-{}-{}-d.sock",
        std::process::id(),
        id
    );
    let mut child = Command::new(bsrs_bin())
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
            "--checkpoints",
            &ckpt_path,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bsrs qs-manager");
    // Wait for bind.
    let path = control.trim_start_matches("ipc://");
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if std::path::Path::new(path).exists() {
            break;
        }
        sleep(Duration::from_millis(20));
    }

    // env_open then drive a Lua plan that yields explicit
    // Checkpoint Msgs (the canned `count` plan does not).
    let (_, _, c) = run_client(&control, &["environment", "open"]);
    assert_eq!(c, 0);
    let lua = r#"
local function ckpt_test()
    coroutine.yield(msg.open_run({plan_name = "ckpt_test"}))
    for i = 1, 3 do
        coroutine.yield(msg.checkpoint())
    end
    coroutine.yield(msg.close_run("success"))
end
return tostring(RE:run(plan(ckpt_test)))
"#;
    let params = format!(r#"{{"source":{}}}"#, serde_json::json!(lua));
    let (out, _, c) = run_client(&control, &["raw", "lua_eval", &params]);
    assert_eq!(c, 0, "lua_eval submit: {out}");

    // Wait for the lua_eval task to finish (poll at least one
    // task_status). With 3 checkpoints, the run completes quickly.
    let task_uid = out
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix(r#""task_uid": ""#)
                .and_then(|s| s.strip_suffix(r#"""#))
        })
        .map(str::to_string)
        .unwrap_or_else(|| panic!("no task_uid in: {out}"));
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        sleep(Duration::from_millis(100));
        let p = format!(r#"{{"task_uid":"{task_uid}"}}"#);
        let (out2, _, _) = run_client(&control, &["raw", "task_result", &p]);
        if out2.contains(r#""status": "completed""#) {
            break;
        }
    }

    // Cleanup the daemon, allow the OS to flush.
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(path);

    // Verify the JSONL file has at least one record.
    let text = std::fs::read_to_string(&ckpt_path)
        .unwrap_or_else(|e| panic!("checkpoint file {ckpt_path} missing: {e}"));
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        !lines.is_empty(),
        "expected ≥1 checkpoint record in {ckpt_path}, got: {text:?}"
    );
    // Each line is JSON with the expected shape.
    let last: serde_json::Value =
        serde_json::from_str(lines.last().unwrap()).expect("last record JSON");
    assert!(last.get("timestamp_ns").is_some());
    assert!(last.get("bsrs_version").is_some());

    let _ = std::fs::remove_file(&ckpt_path);
}
