//! Integration tests for `cirrus repl --script <FILE>`. Drives the binary
//! non-interactively against a temporary Lua script and verifies stdout
//! + exit code.

use std::io::{Read, Write};
use std::process::{Command, Stdio};

fn cirrus_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(
        std::env::var("CARGO_BIN_EXE_cirrus")
            .expect("CARGO_BIN_EXE_cirrus not set; cargo test should set this"),
    )
}

fn run_script(src: &str) -> (String, String, i32) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "cirrus_repl_test_{}_{}_{}.lua",
        std::process::id(),
        nanos.wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15)),
        n,
    ));
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(src.as_bytes()).unwrap();
    }
    let mut child = Command::new(cirrus_bin())
        .arg("repl")
        .arg("--script")
        .arg(&path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cirrus repl");
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut o) = child.stdout.take() {
        o.read_to_string(&mut stdout).ok();
    }
    if let Some(mut e) = child.stderr.take() {
        e.read_to_string(&mut stderr).ok();
    }
    let code = child.wait().unwrap().code().unwrap_or(-1);
    let _ = std::fs::remove_file(&path);
    (stdout, stderr, code)
}

#[test]
fn count_plan_runs_in_repl() {
    let (out, err, code) = run_script(
        r#"
local det1 = soft_detector("det1")
local p = count({det1}, 3)
print(RE:run(p))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("exit_status=success"), "out = {out}");
}

#[test]
fn metadata_round_trip() {
    let (out, _err, code) = run_script(
        r#"
RE:md_set("operator", "alice")
RE:md_set("scan_attempt", 7)
print(RE:md_get())
"#,
    );
    assert_eq!(code, 0);
    assert!(out.contains("alice"));
    assert!(out.contains("scan_attempt"));
    assert!(out.contains("7"));
}

#[test]
fn scan_with_motor() {
    let (out, err, code) = run_script(
        r#"
local det1 = soft_detector("det1")
local m1 = soft_motor("m1", 0.0)
print(RE:run(scan({det1}, m1, 0.0, 1.0, 4)))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("exit_status=success"), "out = {out}");
}

#[test]
fn unknown_global_errors_cleanly() {
    let (_out, err, code) = run_script(
        r#"
no_such_function("oops")
"#,
    );
    assert_ne!(code, 0);
    assert!(err.contains("no_such_function") || err.contains("nil"));
}

#[test]
fn sleep_and_null_plans() {
    let (out, err, code) = run_script(
        r#"
print(RE:run(null()))
print(RE:run(sleep(0.01)))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    let success_lines = out.matches("no-run").count();
    // Both null() and sleep() are no-run plans (no OpenRun).
    assert!(success_lines >= 1, "out = {out}");
}

#[test]
fn coroutine_plan_runs_to_completion() {
    let (out, err, code) = run_script(
        r#"
local det1 = soft_detector("det1")
local m1 = soft_motor("m1", 0.0)

local function my_scan(detectors, motor, n)
    coroutine.yield(msg.open_run({plan_name = "lua_coro_scan"}))
    for i = 0, n - 1 do
        local pos = i / (n - 1)
        coroutine.yield(msg.set(motor, pos, "main"))
        coroutine.yield(msg.wait("main"))
        coroutine.yield(msg.create("primary"))
        coroutine.yield(msg.read(motor))
        for _, d in ipairs(detectors) do
            coroutine.yield(msg.read(d))
        end
        coroutine.yield(msg.save())
    end
    coroutine.yield(msg.close_run("success"))
end

print(RE:run(plan(my_scan, {det1}, m1, 4)))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("exit_status=success"), "out = {out}");
}

#[test]
fn coroutine_plan_open_close_only() {
    // Smallest coroutine plan: just open / close. Verifies the bridge
    // doesn't drop the final close_run msg.
    let (out, err, code) = run_script(
        r#"
local function p()
    coroutine.yield(msg.open_run())
    coroutine.yield(msg.close_run("success"))
end
print(RE:run(plan(p)))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("exit_status=success"), "out = {out}");
}

#[test]
fn coroutine_plan_with_args() {
    let (out, err, code) = run_script(
        r#"
local function p(label, count)
    coroutine.yield(msg.open_run({plan_name = label}))
    for _ = 1, count do
        coroutine.yield(msg.null())
    end
    coroutine.yield(msg.close_run())
end
print(RE:run(plan(p, "argpassed", 3)))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("exit_status=success"), "out = {out}");
}

#[test]
fn coroutine_yield_returns_run_uid_after_open_run() {
    // The bridge surfaces the engine's just-issued run UID as the return
    // value of `coroutine.yield(msg.open_run())`.
    let (out, err, code) = run_script(
        r#"
local captured = nil
local function p()
    captured = coroutine.yield(msg.open_run({plan_name = "uid_test"}))
    coroutine.yield(msg.close_run("success"))
end
local result = RE:run(plan(p))
print("captured:", tostring(captured))
print("result:", result)
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    // captured uid is a uuid; just verify it's a non-empty string and
    // matches the run_uid in the result line.
    let mut captured_line = None;
    let mut result_line = None;
    for line in out.lines() {
        if let Some(s) = line.strip_prefix("captured: ") {
            captured_line = Some(s.to_string());
        }
        if let Some(s) = line.strip_prefix("result: ") {
            result_line = Some(s.to_string());
        }
    }
    let captured = captured_line.expect("missing captured: line");
    let result = result_line.expect("missing result: line");
    assert!(
        !captured.is_empty() && captured != "nil",
        "captured uid should be non-nil; got {captured:?}"
    );
    assert!(
        result.contains(&captured),
        "result {result:?} should reference captured uid {captured:?}"
    );
}

#[test]
fn coroutine_yield_returns_nil_for_unbridged_msgs() {
    // create / save / drop / null don't have a meaningful result; verify
    // they return nil (so users don't depend on accidental side effects).
    let (out, err, code) = run_script(
        r#"
local function p()
    coroutine.yield(msg.open_run())
    local r = coroutine.yield(msg.create("primary"))
    print("create result:", tostring(r))
    local n = coroutine.yield(msg.null())
    print("null result:", tostring(n))
    coroutine.yield(msg.close_run("success"))
end
RE:run(plan(p))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("create result: nil"), "out = {out}");
    assert!(out.contains("null result: nil"), "out = {out}");
}

#[test]
fn coroutine_set_returns_auto_group_string() {
    // msg.set without an explicit group must auto-allocate one and
    // return it as a string for use with msg.wait.
    let (out, err, code) = run_script(
        r#"
local m1 = soft_motor("m1", 0.0)
local function p()
    coroutine.yield(msg.open_run())
    local g1 = coroutine.yield(msg.set(m1, 1.0))
    print("g1:", tostring(g1), type(g1))
    coroutine.yield(msg.wait(g1))
    local g2 = coroutine.yield(msg.set(m1, 2.0, "user_group"))
    print("g2:", tostring(g2), type(g2))
    coroutine.yield(msg.wait(g2))
    coroutine.yield(msg.close_run())
end
RE:run(plan(p))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    // Auto group looks like "auto-N"; user group is what we passed.
    assert!(out.contains("g1: auto-"), "out = {out}");
    assert!(out.contains("g2: user_group"), "out = {out}");
    assert!(out.contains("string"), "yield should return a string for set");
}

#[test]
fn coroutine_locate_returns_setpoint_readback_table() {
    let (out, err, code) = run_script(
        r#"
local m1 = soft_motor("m1", 0.0)
local function p()
    coroutine.yield(msg.open_run())
    coroutine.yield(msg.set(m1, 2.5, "g"))
    coroutine.yield(msg.wait("g"))
    local loc = coroutine.yield(msg.locate(m1))
    print(string.format("loc.setpoint=%g loc.readback=%g", loc.setpoint, loc.readback))
    coroutine.yield(msg.close_run())
end
RE:run(plan(p))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(
        out.contains("loc.setpoint=2.5") && out.contains("loc.readback=2.5"),
        "out = {out}"
    );
}

#[test]
fn coroutine_read_returns_reading_table() {
    let (out, err, code) = run_script(
        r#"
local m1 = soft_motor("m1", 0.0)
local function p()
    coroutine.yield(msg.open_run())
    coroutine.yield(msg.set(m1, 1.5, "g"))
    coroutine.yield(msg.wait("g"))
    coroutine.yield(msg.create("primary"))
    local reading = coroutine.yield(msg.read(m1))
    print("type:", type(reading))
    print("m1 value:", tostring(reading.m1.value))
    coroutine.yield(msg.save())
    coroutine.yield(msg.close_run())
end
RE:run(plan(p))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("type: table"), "out = {out}");
    assert!(out.contains("m1 value: 1.5"), "out = {out}");
}

#[test]
fn coroutine_close_run_returns_exit_status() {
    let (out, err, code) = run_script(
        r#"
local function p()
    coroutine.yield(msg.open_run())
    local es = coroutine.yield(msg.close_run("success"))
    print("es:", tostring(es))
end
RE:run(plan(p))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("es: success"), "out = {out}");
}

#[test]
fn coroutine_msg_constructor_type_errors_clearly() {
    // soft_detector is not movable; msg.set on it must raise a clear
    // error inside the coroutine that the bridge surfaces to stderr.
    let (_out, err, code) = run_script(
        r#"
local det1 = soft_detector("det1")
local function p()
    coroutine.yield(msg.set(det1, 1.0))   -- should fail: not movable
    coroutine.yield(msg.close_run())
end
RE:run(plan(p))
"#,
    );
    // Script exits 0 because RE:run returns Ok(RunResult{exit_status="no-run"}).
    // What we verify is that the Lua error was surfaced to stderr so the
    // user actually sees the cause.
    assert_eq!(code, 0);
    assert!(
        err.contains("not movable"),
        "stderr should include the type error, got: {err}"
    );
}
