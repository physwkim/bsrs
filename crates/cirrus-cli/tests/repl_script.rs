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
    assert!(
        out.contains("string"),
        "yield should return a string for set"
    );
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
fn bp_namespace_all_compound_plans_run() {
    let (out, err, code) = run_script(
        r#"
local m1 = soft_motor("m1", 0.0)
local m2 = soft_motor("m2", 0.0)
local d1 = soft_detector("d1")

assert(string.find(RE:run(bp.count({d1}, 2)), "exit_status=success"))
assert(string.find(RE:run(bp.scan({d1}, m1, 0, 1, 3)), "exit_status=success"))
assert(string.find(RE:run(bp.list_scan({d1}, m1, {0, 0.5, 1})), "exit_status=success"))
assert(string.find(RE:run(bp.rel_scan({d1}, m1, -0.1, 0.1, 3)), "exit_status=success"))
assert(string.find(RE:run(bp.rel_list_scan({d1}, m1, {-0.05, 0.05})), "exit_status=success"))
assert(string.find(
  RE:run(bp.grid_scan({d1},
    {{motor=m1, start=0, stop=0.2, num=2}, {motor=m2, start=0, stop=0.3, num=2}})),
  "exit_status=success"))
assert(string.find(
  RE:run(bp.inner_product_scan({d1}, 3,
    {{motor=m1, start=0, stop=1}, {motor=m2, start=0, stop=2}})),
  "exit_status=success"))
assert(string.find(
  RE:run(bp.spiral_square({d1}, m1, m2, 0, 0, 0.4, 0.4, 3, 3)),
  "exit_status=success"))
assert(string.find(
  RE:run(bp.spiral_fermat({d1}, m1, m2, 0, 0, 0.5, 0.5, 0.1, 1.0)),
  "exit_status=success"))
assert(string.find(RE:run(bp.log_scan({d1}, m1, 0.01, 1.0, 5)), "exit_status=success"))
print("OK")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("OK"), "out: {out}");
}

#[test]
fn bps_namespace_stub_plans() {
    let (out, err, code) = run_script(
        r#"
local m1 = soft_motor("m1", 0.0)
local d1 = soft_detector("d1")

-- 1-Msg / small stubs
assert(string.find(RE:run(bps.null()), "exit_status="))
assert(string.find(RE:run(bps.sleep(0.005)), "exit_status="))
assert(string.find(RE:run(bps.mv(m1, 0.5)), "exit_status="))
assert(string.find(RE:run(bps.mvr(m1, 0.1)), "exit_status="))
assert(string.find(RE:run(bps.abs_set(m1, 1.0)), "exit_status="))

-- bps.read inside a properly-bracketed run
local body = bpp.pchain({bps.create("primary"), bps.read(m1), bps.save()})
assert(string.find(RE:run(bpp.run_wrapper(body)), "exit_status=success"))

-- repeater builds N sub-plans and chains them
local p = bps.repeater(3, function(i) return bp.count({d1}, 1) end)
assert(string.find(RE:run(p), "exit_status=success"))
print("OK")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("OK"), "out: {out}");
}

#[test]
fn bpt_namespace_pattern_generators() {
    let (out, err, code) = run_script(
        r#"
local pts = bpt.inner_product(3, {{0, 10}, {5, 15}})
assert(#pts == 3)
assert(pts[1][1] == 0 and pts[1][2] == 5)
assert(pts[3][1] == 10 and pts[3][2] == 15)

local grid = bpt.outer_product({{0, 1, 2}, {10, 20, 2}})
assert(#grid == 4)

local sp = bpt.spiral_fermat(0, 0, 1, 1, 0.1, 1.0)
assert(#sp > 5, "spiral_fermat should yield several points")

local sq = bpt.spiral_square(0, 0, 4, 4, 5, 5)
assert(#sq == 25)

print("OK")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("OK"), "out: {out}");
}

#[test]
fn bpp_namespace_preprocessors() {
    let (out, err, code) = run_script(
        r#"
local m1 = soft_motor("m1", 0.0)
local d1 = soft_detector("d1")

-- run_wrapper around a no-run plan opens/closes the run
assert(string.find(
  RE:run(bpp.run_wrapper(bps.null(), {plan_name="wrapped"})),
  "exit_status=success"))

-- print_summary echoes Msgs to stderr but plan still runs
assert(string.find(
  RE:run(bpp.print_summary(bp.count({d1}, 1))),
  "exit_status=success"))

-- pchain combines two plans
assert(string.find(
  RE:run(bpp.pchain({bp.count({d1}, 1), bp.count({d1}, 1)})),
  "exit_status=success"))

-- inject_md merges into RunStart extras
assert(string.find(
  RE:run(bpp.inject_md(bp.count({d1}, 1), {operator="alice"})),
  "exit_status=success"))

-- contingency_wrapper falls through to finally
assert(string.find(
  RE:run(bpp.contingency(bp.count({d1}, 1), bps.null())),
  "exit_status=success"))

-- finalize_wrapper too
assert(string.find(
  RE:run(bpp.finalize_wrapper(bp.count({d1}, 1), bps.null())),
  "exit_status=success"))

-- relative_set_wrapper + reset_positions_wrapper around a 0-span scan
assert(string.find(
  RE:run(bpp.relative_set(bps.mv(m1, 0.1), {m1})),
  "exit_status="))

-- msg_mutator: rewrite each Msg through a Lua function (identity here)
local p = bp.count({d1}, 1)
local mutated = bpp.msg_mutator(p, function(m) return m end)
assert(string.find(RE:run(mutated), "exit_status=success"))

print("OK")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("OK"), "out: {out}");
}

#[test]
fn device_method_bluesky_style_short_names() {
    // motor:position() / :target() / :locate() / :move_to() / :set()
    // det:read() / :describe() / :stop() — same shape as the Rust ext
    // traits, callable directly on a Lua device userdata.
    let (out, err, code) = run_script(
        r#"
local m = soft_motor("m1", 0.5)
local d = soft_detector("d1")

assert(m:position() == 0.5)
assert(m:target() == 0.5)
local loc = m:locate()
assert(loc.setpoint == 0.5 and loc.readback == 0.5)

m:move_to(1.5)
assert(m:position() == 1.5)

local s = m:set(2.0)
assert(s:done() == false)
s:wait()
assert(s:done() == true)
assert(m:position() == 2.0)

local r = d:read()
assert(type(r) == "table")
local seen_value
for _, v in pairs(r) do seen_value = v.value; break end
assert(type(seen_value) == "number")

local desc = d:describe()
local desc_count = 0
for _ in pairs(desc) do desc_count = desc_count + 1 end
assert(desc_count >= 1)

m:stop()
print("OK")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("OK"), "out: {out}");
}

#[test]
fn device_method_role_mismatch_errors_clearly() {
    let (_out, err, code) = run_script(
        r#"
local d = soft_detector("d1")
d:set(1.0)   -- soft detector is not movable
"#,
    );
    assert_ne!(code, 0);
    assert!(
        err.contains("not movable"),
        "stderr should mention 'not movable', got: {err}"
    );
}

#[test]
fn device_status_double_wait_errors() {
    let (_out, err, code) = run_script(
        r#"
local m = soft_motor("m1", 0.0)
local s = m:set(1.0)
s:wait()
s:wait()    -- second wait must error: Status is single-use
"#,
    );
    assert_ne!(code, 0);
    assert!(
        err.contains("already awaited"),
        "stderr should mention 'already awaited', got: {err}"
    );
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
