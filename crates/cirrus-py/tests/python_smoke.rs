//! Drive the compiled `cirrus_native` Python extension via a `python3`
//! subprocess. Ensures the `dylib` actually loads as a Python module
//! and round-trips RunEngine.run for `count` and `scan` plans.
//!
//! Gated behind the `python-tests` feature so default workspace
//! `cargo test` doesn't try to link a test binary that needs Python
//! ABI symbols on platforms (e.g. CI Linux) that don't provide them
//! via `dynamic_lookup`. Run explicitly with:
//!
//!     cargo test -p cirrus-py --features python-tests

#![cfg(feature = "python-tests")]

use std::process::{Command, Stdio};

fn dylib_path() -> std::path::PathBuf {
    // CARGO_TARGET_TMPDIR points at `target/<profile>/build/...`.
    // The dylib is two levels up at `target/<profile>/libcirrus_native.dylib`
    // (or .so on Linux). Locate by walking up from the test binary.
    let exe = std::env::current_exe().expect("current_exe");
    // exe lives in target/<profile>/deps/<test-binary>; walk up two.
    let mut p = exe.clone();
    p.pop(); // deps/
    p.pop(); // <profile>/
    let candidate_names = [
        "libcirrus_native.dylib",
        "libcirrus_native.so",
        "cirrus_native.dll",
    ];
    for name in candidate_names {
        let c = p.join(name);
        if c.exists() {
            return c;
        }
    }
    panic!(
        "could not locate cirrus_native dylib near {}",
        exe.display()
    );
}

fn run_python(script: &str) -> (String, String, i32) {
    let dylib = dylib_path();
    // Copy the dylib next to `cirrus_native.so` so the import name
    // matches. Python looks for `<modname>.so`/`.dylib`/`.pyd`.
    let dir = std::env::temp_dir().join(format!("cirrus_py_smoke_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let target = dir.join(if cfg!(target_os = "windows") {
        "cirrus_native.pyd"
    } else {
        // On macOS the convention is `.so` for Python extensions even
        // though the underlying file format is Mach-O dylib.
        "cirrus_native.so"
    });
    std::fs::copy(&dylib, &target).expect("copy dylib");

    let mut child = Command::new("python3")
        .arg("-c")
        .arg(format!(
            "import sys; sys.path.insert(0, {dir:?}); {script}",
            dir = dir.display().to_string(),
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3");
    let mut stdout = String::new();
    let mut stderr = String::new();
    use std::io::Read;
    if let Some(mut o) = child.stdout.take() {
        o.read_to_string(&mut stdout).ok();
    }
    if let Some(mut e) = child.stderr.take() {
        e.read_to_string(&mut stderr).ok();
    }
    let code = child.wait().expect("wait").code().unwrap_or(-1);
    let _ = std::fs::remove_dir_all(&dir);
    (stdout, stderr, code)
}

#[test]
fn count_plan_round_trips_through_python() {
    let (out, err, code) = run_python(
        "
import cirrus_native as c
re = c.RunEngine()
det = c.SoftDetector('det1')
plan = c.count([det], 5)
status, uid = re.run(plan)
print(status)
print(uid is not None)
",
    );
    assert_eq!(code, 0, "stderr: {err}");
    let lines: Vec<&str> = out.lines().collect();
    assert!(
        lines.contains(&"success"),
        "expected exit_status=success: {out}"
    );
    assert!(lines.contains(&"True"), "expected non-None run_uid: {out}");
}

#[test]
fn scan_plan_round_trips_through_python() {
    let (out, err, code) = run_python(
        "
import cirrus_native as c
re = c.RunEngine()
m = c.SoftMotor('m1', 0.0)
det = c.SoftDetector('det1')
plan = c.scan([det], m, m, 0.0, 1.0, 4)
status, _uid = re.run(plan)
print(status)
",
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("success"), "out: {out}");
}

#[test]
fn plan_double_consume_errors() {
    let (_out, err, code) = run_python(
        "
import cirrus_native as c
re = c.RunEngine()
det = c.SoftDetector('d')
plan = c.count([det], 2)
re.run(plan)
re.run(plan)   # second run must fail
",
    );
    assert_ne!(code, 0, "second run should have errored");
    assert!(
        err.contains("already consumed"),
        "stderr should mention 'already consumed': {err}"
    );
}

#[test]
fn version_is_a_string() {
    let (out, err, code) = run_python(
        "
import cirrus_native as c
v = c.version()
assert isinstance(v, str)
assert len(v) > 0
print(v)
",
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(!out.trim().is_empty());
}
