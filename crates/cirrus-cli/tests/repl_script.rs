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
