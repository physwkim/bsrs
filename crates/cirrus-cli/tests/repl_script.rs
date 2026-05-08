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
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "cirrus_repl_test_{}_{}.lua",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
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
