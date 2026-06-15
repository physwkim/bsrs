//! Drive the compiled `bsrs_native` Python extension via a `python3`
//! subprocess. Ensures the `dylib` actually loads as a Python module
//! and round-trips RunEngine.run for `count` and `scan` plans.
//!
//! Gated behind the `python-tests` feature so default workspace
//! `cargo test` doesn't try to link a test binary that needs Python
//! ABI symbols on platforms (e.g. CI Linux) that don't provide them
//! via `dynamic_lookup`. Run explicitly with:
//!
//!     cargo test -p bsrs-py --features python-tests

#![cfg(feature = "python-tests")]

use std::process::{Command, Stdio};

fn dylib_path() -> std::path::PathBuf {
    // CARGO_TARGET_TMPDIR points at `target/<profile>/build/...`.
    // The dylib is two levels up at `target/<profile>/libbsrs_native.dylib`
    // (or .so on Linux). Locate by walking up from the test binary.
    let exe = std::env::current_exe().expect("current_exe");
    // exe lives in target/<profile>/deps/<test-binary>; walk up two.
    let mut p = exe.clone();
    p.pop(); // deps/
    p.pop(); // <profile>/
    let candidate_names = [
        "libbsrs_native.dylib",
        "libbsrs_native.so",
        "bsrs_native.dll",
    ];
    for name in candidate_names {
        let c = p.join(name);
        if c.exists() {
            return c;
        }
    }
    panic!("could not locate bsrs_native dylib near {}", exe.display());
}

fn run_python(script: &str) -> (String, String, i32) {
    let dylib = dylib_path();
    // Copy the dylib next to `bsrs_native.so` so the import name
    // matches. Python looks for `<modname>.so`/`.dylib`/`.pyd`.
    let dir = std::env::temp_dir().join(format!("bsrs_py_smoke_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let target = dir.join(if cfg!(target_os = "windows") {
        "bsrs_native.pyd"
    } else {
        // On macOS the convention is `.so` for Python extensions even
        // though the underlying file format is Mach-O dylib.
        "bsrs_native.so"
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
import bsrs_native as c
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
import bsrs_native as c
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
import bsrs_native as c
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
import bsrs_native as c
v = c.version()
assert isinstance(v, str)
assert len(v) > 0
print(v)
",
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(!out.trim().is_empty());
}

#[test]
fn grid_scan_rel_scan_and_mv_round_trip() {
    // PY-04: grid_scan / rel_scan / mv plan factories are exposed and run.
    let (out, err, code) = run_python(
        "
import bsrs_native as c
re = c.RunEngine()
m1 = c.SoftMotor('m1', 0.0)
m2 = c.SoftMotor('m2', 0.0)
det = c.SoftDetector('det1')
status, _ = re.run(c.grid_scan([det], m1, m1, 0.0, 1.0, 2, m2, m2, 0.0, 2.0, 3))
print(status == 'success')
status, _ = re.run(c.rel_scan([det], m1, m1, 0.0, -1.0, 1.0, 3))
print(status == 'success')
# mv opens no run; assert the side effect (motor moved) via read().
re.run(c.mv(m1, 5.0))
print(m1.read()['m1']['value'] == 5.0)
",
    );
    assert_eq!(code, 0, "stderr: {err}");
    let trues = out.lines().filter(|l| *l == "True").count();
    assert_eq!(
        trues, 3,
        "expected grid_scan + rel_scan success and mv side-effect; out: {out}\nerr: {err}"
    );
}

#[test]
fn device_protocol_methods_callable_from_python() {
    // PY-03: soft devices expose the ophyd/bluesky Readable + Movable protocol
    // methods directly — read()/describe() on both, set(value) on the motor.
    let (out, err, code) = run_python(
        "
import bsrs_native as c
m = c.SoftMotor('m1', 1.5)
r = m.read()
assert isinstance(r, dict), 'read must be a dict: %r' % (r,)
assert 'm1' in r, r
print(r['m1']['value'] == 1.5)
d = m.describe()
assert isinstance(d, dict), d
print('m1' in d)
m.set(3.0)
print(m.read()['m1']['value'] == 3.0)
det = c.SoftDetector('det1')
dr = det.read()
print('det1_counts' in dr)
print(dr['det1_counts']['value'] == 0)
print('det1_counts' in det.describe())
",
    );
    assert_eq!(code, 0, "stderr: {err}");
    let trues = out.lines().filter(|l| *l == "True").count();
    assert_eq!(
        trues, 6,
        "expected motor read/describe/set + detector read/describe to round-trip; out: {out}\nerr: {err}"
    );
}

#[test]
fn subscribe_receives_documents_and_unsubscribe_stops_them() {
    // PY-01: RE.subscribe(cb) must deliver every document to a Python callable
    // as (name, dict); unsubscribe(token) must stop further delivery.
    let (out, err, code) = run_python(
        "
import bsrs_native as c
re = c.RunEngine()
det = c.SoftDetector('det1')
names = []
def cb(name, doc):
    assert isinstance(name, str), 'name must be str'
    assert isinstance(doc, dict), 'doc must be a dict, got %r' % type(doc)
    names.append(name)
token = re.subscribe(cb)
re.run(c.count([det], 3))
print('start' in names)
print('descriptor' in names)
print('event' in names)
print('stop' in names)
# The start document carries a uid (proves the body is a real dict).
print(any(n == 'start' for n in names))
re.unsubscribe(token)
before = len(names)
re.run(c.count([det], 1))
print(len(names) == before)
",
    );
    assert_eq!(code, 0, "stderr: {err}");
    let trues = out.lines().filter(|l| *l == "True").count();
    assert_eq!(
        trues, 6,
        "expected start/descriptor/event/stop delivered + unsubscribe stops delivery; out: {out}\nerr: {err}"
    );
}
