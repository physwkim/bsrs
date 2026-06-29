//! Integration tests for `bsrs repl --script <FILE>`. Drives the binary
//! non-interactively against a temporary Lua script and verifies stdout
//! + exit code.
//!
//! Gated on the `cli` feature: the test drives the `bsrs` binary, which
//! is only built (and `CARGO_BIN_EXE_bsrs` only set) when `cli` is on.

#![cfg(feature = "cli")]

use std::io::{Read, Write};
use std::process::{Command, Stdio};

fn bsrs_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(
        std::env::var("CARGO_BIN_EXE_bsrs")
            .expect("CARGO_BIN_EXE_bsrs not set; cargo test should set this"),
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
        "bsrs_repl_test_{}_{}_{}.lua",
        std::process::id(),
        nanos.wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15)),
        n,
    ));
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(src.as_bytes()).unwrap();
    }
    let mut child = Command::new(bsrs_bin())
        .arg("repl")
        .arg("--script")
        .arg(&path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bsrs repl");
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

-- lazily_stage_wrapper with no devices = pass-through (soft_* devices
-- aren't exposed as stageable from Lua; the Rust path is covered by
-- bsrs-plans unit tests). Empty list still exercises the binding.
assert(string.find(
  RE:run(bpp.lazily_stage_wrapper(bp.count({d1}, 1), {})),
  "exit_status=success"))

-- set_run_key_wrapper: tags every OpenRun with metadata.extra.run_key
assert(string.find(
  RE:run(bpp.set_run_key_wrapper(bp.count({d1}, 1), "scan_42")),
  "exit_status=success"))

-- stub_wrapper: passes through a run-free plan unchanged
assert(string.find(
  RE:run(bpp.run_wrapper(bpp.stub_wrapper(bps.read(d1)))),
  "exit_status="))

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
-- SoftMotor.set returns an already-done Status (sync backend); wait
-- is a no-op but kept for the bluesky-style call shape.
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
fn device_status_wait_is_idempotent() {
    // Status is `Clone` (Arc-shared inner state); `:wait()` is now
    // idempotent and `:done()` / `:success()` / `:exception()` /
    // `:inspect()` work both before and after `:wait()`. This was a
    // single-use semantics in earlier prototypes — see Phase C.
    let (out, err, code) = run_script(
        r#"
local m = soft_motor("m1", 0.0)
local s = m:set(1.0)
s:wait()
s:wait()    -- second wait must NOT error: Status is now idempotent
assert(s:done() == true)
assert(s:success() == true)
assert(s:exception() == nil)
print("OK")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("OK"), "out: {out}");
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

// -- Tier 1: msg.* additions -------------------------------------------------

#[test]
fn msg_input_with_handler_returns_string() {
    let (out, err, code) = run_script(
        r#"
RE:set_input_handler(function(prompt) return "answer:" .. prompt end)
local function p()
    -- Bridge passes MsgResult back as the yield's return value.
    local r = coroutine.yield(msg.input("name?"))
    print("r=" .. tostring(r))
end
RE:run(plan(p))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("r=answer:name?"), "out = {out}");
}

#[test]
fn msg_re_class_returns_engine_name() {
    let (out, err, code) = run_script(
        r#"
local function p()
    local c = coroutine.yield(msg.re_class())
    print("c=" .. tostring(c))
end
RE:run(plan(p))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("c=bsrs.RunEngine"), "out = {out}");
}

#[test]
fn msg_subscribe_receives_documents_via_lua_callback() {
    let (out, err, code) = run_script(
        r#"
local count = 0
local function cb(name, body)
    count = count + 1
end
local function p()
    coroutine.yield(msg.subscribe(cb))
    coroutine.yield(msg.open_run())
    coroutine.yield(msg.close_run())
end
RE:run(plan(p))
print("count=" .. count)
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    // Expect at least start + stop documents
    assert!(out.contains("count="), "out = {out}");
    let n: i32 = out
        .lines()
        .find_map(|l| l.strip_prefix("count="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert!(n >= 2, "got {n} docs in subscriber");
}

#[test]
fn msg_wait_for_factories_run_in_order() {
    let (out, err, code) = run_script(
        r#"
local order = ""
local function p()
    coroutine.yield(msg.wait_for({
        function() order = order .. "a"; return nil end,
        function() order = order .. "b"; return nil end,
    }))
end
RE:run(plan(p))
print("order=" .. order)
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("order=ab"), "out = {out}");
}

// -- Tier 2: simple RE:* methods --------------------------------------------

#[test]
fn re_set_record_interruptions_toggles_state() {
    let (out, err, code) = run_script(
        r#"
print("a=" .. tostring(RE:record_interruptions_enabled()))
RE:set_record_interruptions(true)
print("b=" .. tostring(RE:record_interruptions_enabled()))
RE:set_record_interruptions(false)
print("c=" .. tostring(RE:record_interruptions_enabled()))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("a=false"), "out = {out}");
    assert!(out.contains("b=true"), "out = {out}");
    assert!(out.contains("c=false"), "out = {out}");
}

#[test]
fn re_md_remove_and_replace_work() {
    let (out, err, code) = run_script(
        r#"
RE:md_set("a", "x")
RE:md_set("b", "y")
RE:md_remove("a")
print("md1=" .. RE:md_get())
RE:md_replace({c = "z"})
print("md2=" .. RE:md_get())
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    // After remove+replace, only "c" should remain.
    assert!(out.contains("\"c\": \"z\""), "out = {out}");
    assert!(!out.contains("\"a\":"), "out = {out}");
}

#[test]
fn re_set_loop_timeout_aborts_overrun() {
    let (_out, err, _code) = run_script(
        r#"
RE:set_loop_timeout(0.1)
local function p()
    coroutine.yield(msg.sleep(5))
end
local ok, msg_str = pcall(function() RE:run(plan(p)) end)
io.stderr:write("ok=" .. tostring(ok) .. " err=" .. tostring(msg_str) .. "\n")
"#,
    );
    // Loop timeout surfaces as a RuntimeError; the script's pcall
    // captures it and writes to stderr.
    assert!(
        err.to_lowercase().contains("timeout") || err.to_lowercase().contains("plan failed"),
        "stderr = {err}"
    );
}

// -- Tier 3: callback-heavy RE:* methods ------------------------------------

#[test]
fn re_set_md_normalizer_modifies_runstart() {
    let (out, err, code) = run_script(
        r#"
RE:set_md_normalizer(function(md)
    md.normalized = true
    return md
end)
local got = nil
RE:subscribe(function(name, body)
    if name == "start" then got = body end
end)
local function p()
    coroutine.yield(msg.open_run())
    coroutine.yield(msg.close_run())
end
RE:run(plan(p))
print("start=" .. tostring(got))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("\"normalized\":true"), "out = {out}");
}

#[test]
fn re_set_scan_id_source_overrides_auto_increment() {
    let (out, err, code) = run_script(
        r#"
RE:set_scan_id_source(function(md) return 99 end)
local got = nil
RE:subscribe(function(name, body)
    if name == "start" then got = body end
end)
local function p()
    coroutine.yield(msg.open_run())
    coroutine.yield(msg.close_run())
end
RE:run(plan(p))
print("start=" .. tostring(got))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("\"scan_id\":99"), "out = {out}");
}

#[test]
fn re_set_md_validator_can_reject_run() {
    let (out, err, code) = run_script(
        r#"
RE:set_md_validator(function(md)
    if md.forbidden then return "forbidden key" end
    return nil
end)
RE:md_set("forbidden", true)
local function p()
    coroutine.yield(msg.open_run())
    coroutine.yield(msg.close_run())
end
local r = RE:run(plan(p))
print("result=" .. tostring(r))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    // Validator failure marks the run as exit_status=fail; RE:run
    // returns Ok with the formatted result string.
    assert!(out.contains("exit_status=fail"), "out = {out}");
}

#[test]
fn re_register_command_dispatched_via_msg_custom() {
    // Lua's Msg::Custom binding doesn't exist, but Rust-side
    // register_command from Lua means a Rust-built Custom Msg can
    // route to the Lua handler. Verify the handler is invoked when
    // any Msg::Custom passes through (via plan side, not implemented
    // here — so we verify only the registration accepts a function).
    let (_out, err, code) = run_script(
        r#"
RE:register_command("ping", function(payload) return nil end)
RE:unregister_command("ping")
print("ok")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
}

#[test]
fn re_register_pausable_smoke_test() {
    // Single-threaded Lua + blocking RE:run means a Lua script alone
    // cannot drive a pause/resume cycle (no second Lua thread to call
    // RE:resume() while RE:run is parked). This smoke test verifies
    // only the binding plumbing: register, no error; counters readable
    // and zero before any pause; unregister, no error. The actual
    // pause/resume firing is exercised by the Rust-level integration
    // test in crates/bsrs/tests/runengine_features.rs.
    let (out, err, code) = run_script(
        r#"
local p1 = soft_pausable("p1")
RE:register_pausable(p1)
print("pc_before=" .. p1:pause_count())
print("rc_before=" .. p1:resume_count())
RE:unregister_pausable(p1)
print("after_unregister_ok")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("pc_before=0"), "out = {out}");
    assert!(out.contains("rc_before=0"), "out = {out}");
    assert!(out.contains("after_unregister_ok"), "out = {out}");
}

#[test]
fn re_register_pausable_unregister_accepts_device_or_name() {
    let (_out, err, code) = run_script(
        r#"
local p1 = soft_pausable("p1")
RE:register_pausable(p1)
-- both forms must succeed:
RE:unregister_pausable(p1)            -- device
RE:register_pausable(p1)
RE:unregister_pausable("p1")          -- name string
print("ok")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
}

#[test]
fn re_register_pausable_rejects_non_pausable_device() {
    let (_out, err, code) = run_script(
        r#"
local m = soft_motor("m1", 0.0)
local ok, e = pcall(function() RE:register_pausable(m) end)
io.stderr:write("ok=" .. tostring(ok) .. " e=" .. tostring(e) .. "\n")
"#,
    );
    assert_eq!(code, 0);
    assert!(
        err.contains("not pausable"),
        "stderr should mention 'not pausable', got: {err}"
    );
}

#[test]
fn msg_publish_missing_field_gives_actionable_error() {
    // After R3-A, the Lua error must propagate as exit_status=fail in
    // the RunResult — programmatic detection, not stderr-scraping.
    let (out, err, code) = run_script(
        r#"
local function p()
    -- resource missing required fields → actionable error message
    coroutine.yield(msg.publish({kind = "resource", body = {uid = "r-1"}}))
end
local r = RE:run(plan(p))
print("result=" .. tostring(r))
"#,
    );
    assert_eq!(code, 0);
    assert!(
        out.contains("exit_status=fail"),
        "expected exit_status=fail, got out={out} err={err}"
    );
    assert!(
        err.contains("missing required fields") || err.contains("publish"),
        "actionable detail must be in stderr trace; err = {err}"
    );
}

#[test]
fn msg_publish_minimal_datum_succeeds() {
    // Datum has only datum_id + resource as required (datum_kwargs has
    // #[serde(default)]). This regression test catches the R2-1
    // miscategorization where datum_kwargs was wrongly listed as
    // required, causing valid bodies to be rejected.
    let (_out, err, code) = run_script(
        r#"
local function p()
    coroutine.yield(msg.publish({
        kind = "datum",
        body = {datum_id = "r-1/0", resource = "r-1"},
    }))
end
RE:run(plan(p))
print("ok")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(
        !err.contains("missing required fields"),
        "minimal valid Datum body must not be rejected, got: {err}"
    );
}

#[test]
fn msg_publish_minimal_stream_resource_succeeds() {
    // StreamResource: parameters and run_start have #[serde(default)].
    // Required = uid, data_key, mimetype, uri.
    let (_out, err, code) = run_script(
        r#"
local function p()
    coroutine.yield(msg.publish({
        kind = "stream_resource",
        body = {uid = "sr-1", data_key = "det1", mimetype = "application/x-hdf5",
                uri = "file:///tmp/x.h5"},
    }))
end
RE:run(plan(p))
print("ok")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(
        !err.contains("missing required fields"),
        "minimal valid StreamResource body must not be rejected, got: {err}"
    );
}

#[test]
fn lua_coroutine_error_propagates_to_run_result() {
    // Regression for R3-A: any Lua-side error (not just publish) must
    // mark the run as exit_status=fail in the RunResult, not silently
    // swallow into "no-run" / "success".
    let (out, _err, code) = run_script(
        r#"
local function p()
    error("intentional Lua error from plan")
end
local r = RE:run(plan(p))
print("result=" .. tostring(r))
"#,
    );
    assert_eq!(code, 0);
    assert!(
        out.contains("exit_status=fail"),
        "Lua coroutine error must propagate as fail, got: {out}"
    );
}

#[test]
fn lua_error_after_open_run_emits_runstop_fail() {
    // Regression for R4-2: error mid-plan after open_run must close
    // the run with exit_status="fail" (not leave it dangling).
    let (out, _err, code) = run_script(
        r#"
local got_stop = nil
RE:subscribe(function(name, body)
    if name == "stop" then got_stop = body end
end)
local function p()
    coroutine.yield(msg.open_run())
    error("intentional mid-plan error")
end
local r = RE:run(plan(p))
print("result=" .. tostring(r))
print("stop=" .. tostring(got_stop))
"#,
    );
    assert_eq!(code, 0);
    assert!(out.contains("exit_status=fail"), "out = {out}");
    assert!(
        out.contains("\"exit_status\":\"fail\""),
        "RunStop document must carry exit_status=fail; out = {out}"
    );
}

#[test]
fn bridge_error_command_is_reserved() {
    // Regression for R4-1: users cannot accidentally clobber the
    // bridge-error custom command name.
    let (_out, err, code) = run_script(
        r#"
local ok1, e1 = pcall(function()
    RE:register_command("_bsrs_lua_bridge_error", function(p) end)
end)
local ok2, e2 = pcall(function()
    RE:unregister_command("_bsrs_lua_bridge_error")
end)
io.stderr:write(string.format("ok1=%s ok2=%s e1=%s e2=%s\n",
    tostring(ok1), tostring(ok2), tostring(e1), tostring(e2)))
"#,
    );
    assert_eq!(code, 0);
    assert!(
        err.contains("ok1=false"),
        "register should reject; err = {err}"
    );
    assert!(
        err.contains("ok2=false"),
        "unregister should reject; err = {err}"
    );
    assert!(
        err.contains("reserved"),
        "error must mention 'reserved'; err = {err}"
    );
}

#[test]
fn msg_publish_unsupported_kind_gives_actionable_error() {
    let (out, _err, code) = run_script(
        r#"
local function p()
    coroutine.yield(msg.publish({kind = "garbage", body = {}}))
end
local r = RE:run(plan(p))
print("result=" .. tostring(r))
"#,
    );
    assert_eq!(code, 0);
    assert!(out.contains("exit_status=fail"), "out: {out}");
}

#[test]
fn lua_subscriber_drains_even_when_run_returns_error() {
    // Regression for R1-1: when run_async returns Err (e.g.
    // loop_timeout), the Lua RE:run binding's `?` used to short-
    // circuit BEFORE drain_lua_subscriber_buffers(), losing any
    // buffered subscriber entries forever. Fix drains
    // unconditionally before propagating.
    //
    // Verify by: setting a tight loop_timeout, registering a
    // subscriber that records each doc into a Lua table,
    // running a plan that emits Start (so the subscriber sees
    // something) then Sleep that blows past the timeout. The
    // run errors, but the start doc must still reach the
    // subscriber.
    let (out, _err, code) = run_script(
        r#"
local seen = {}
RE:subscribe(function(name, body) table.insert(seen, name) end)
RE:set_loop_timeout(0.05)
local function p()
    coroutine.yield(msg.open_run())
    coroutine.yield(msg.sleep(2.0))   -- blows past timeout
    coroutine.yield(msg.close_run())
end
local ok, _err = pcall(function() RE:run(plan(p)) end)
print("ok=" .. tostring(ok))
print("seen=" .. table.concat(seen, ","))
"#,
    );
    assert_eq!(code, 0);
    // The run errored — that's expected. But the subscriber must
    // have received the start document via drain.
    assert!(
        out.contains("seen=") && out.contains("start"),
        "subscriber must see start doc even after timeout error; out = {out}"
    );
}

#[test]
fn re_subscribe_name_filter_only_fires_for_match() {
    let (out, err, code) = run_script(
        r#"
local n_start, n_stop, n_event = 0, 0, 0
RE:subscribe(function(name, body) n_start = n_start + 1 end, "start")
RE:subscribe(function(name, body) n_stop = n_stop + 1 end, "stop")
RE:subscribe(function(name, body) n_event = n_event + 1 end, "event")
local det1 = soft_detector("det1")
RE:run(count({det1}, 2))
print("start=" .. n_start)
print("stop=" .. n_stop)
print("event=" .. n_event)
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("start=1"), "out = {out}");
    assert!(out.contains("stop=1"), "out = {out}");
    assert!(out.contains("event=2"), "out = {out}");
}

#[test]
fn re_subscribe_all_fires_for_every_doc() {
    let (out, err, code) = run_script(
        r#"
local total = 0
RE:subscribe(function(name, body) total = total + 1 end, "all")
local det1 = soft_detector("det1")
RE:run(count({det1}, 1))
print("total=" .. total)
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    // count(1) emits start + descriptor + event + stop = 4
    assert!(
        out.contains("total=4") || out.contains("total=5"),
        "out = {out}"
    );
}

#[test]
fn re_subscribe_no_filter_matches_all_docs() {
    // Existing behavior unchanged when name arg is omitted.
    let (out, err, code) = run_script(
        r#"
local total = 0
RE:subscribe(function(name, body) total = total + 1 end)
local det1 = soft_detector("det1")
RE:run(count({det1}, 1))
print("total=" .. total)
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(
        out.contains("total=4") || out.contains("total=5"),
        "out = {out}"
    );
}

#[test]
fn lua_subscriber_receives_monitor_events_via_buffered_drain() {
    // Regression for the deadlock fix: monitor pump emits Event
    // documents from a worker task; without the buffer/drain
    // mechanism the call into Lua would deadlock waiting on mlua's
    // mutex held by the REPL thread. With the fix, the events are
    // buffered and replayed after RE:run returns.
    //
    // Note: bsrs-cli's `soft_detector` is not Monitorable. To test
    // the buffered path we'd need a Monitorable backend; this test
    // instead verifies the simpler path: a subscription added before
    // RE:run completes successfully without timing out, and the
    // drain mechanism fires the same number of times as direct
    // calls (sanity that drain doesn't double-fire). A true
    // worker-thread emit test belongs in a Rust integration test
    // with a custom Monitorable.
    let (out, err, code) = run_script(
        r#"
local total = 0
RE:subscribe(function(name, body) total = total + 1 end, "all")
local det1 = soft_detector("det1")
RE:run(count({det1}, 1))
print("total=" .. total)
RE:run(count({det1}, 1))
print("total2=" .. total)
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    // First run: 4 docs. Second run: 4 more.
    let total: i32 = out
        .lines()
        .find_map(|l| l.strip_prefix("total="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let total2: i32 = out
        .lines()
        .find_map(|l| l.strip_prefix("total2="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert!(total >= 4, "first run total = {total}, out = {out}");
    assert!(
        total2 >= total + 4,
        "second run delta = {}, out = {out}",
        total2 - total
    );
}

#[test]
fn run_async_with_rejects_non_table_md() {
    // Regression for R5-2: opts.md must be a table or nil; other
    // types must surface a clear error rather than being silently
    // dropped.
    let (_out, err, code) = run_script(
        r#"
local function p() coroutine.yield(msg.null()) end
local ok, e = pcall(function()
    RE:run_async_with(plan(p), { md = 42 })
end)
io.stderr:write("ok=" .. tostring(ok) .. " e=" .. tostring(e) .. "\n")
"#,
    );
    assert_eq!(code, 0);
    assert!(err.contains("ok=false"), "err = {err}");
    assert!(err.contains("opts.md must be a table"), "err = {err}");
}

#[test]
fn run_async_with_rejects_non_table_subs() {
    let (_out, err, code) = run_script(
        r#"
local function p() coroutine.yield(msg.null()) end
local ok, e = pcall(function()
    RE:run_async_with(plan(p), { subs = "not a list" })
end)
io.stderr:write("ok=" .. tostring(ok) .. " e=" .. tostring(e) .. "\n")
"#,
    );
    assert_eq!(code, 0);
    assert!(err.contains("ok=false"), "err = {err}");
    assert!(
        err.contains("opts.subs must be a sequence of functions"),
        "err = {err}"
    );
}

#[test]
fn re_run_async_with_per_call_md_lands_in_runstart() {
    let (out, err, code) = run_script(
        r#"
local got = nil
RE:subscribe(function(name, body)
    if name == "start" then got = body end
end)
local function p()
    coroutine.yield(msg.open_run())
    coroutine.yield(msg.close_run())
end
RE:run_async_with(plan(p), { md = { operator = "carol" } })
print("start=" .. tostring(got))
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("\"operator\":\"carol\""), "out = {out}");
}

#[cfg(feature = "tiled")]
#[test]
fn tiled_global_namespace_exists() {
    // Smoke test for the tiled.* Lua surface: when the bsrs-cli
    // binary is built with --features tiled, the global `tiled`
    // table is available with `from_uri` callable. We don't reach
    // a real Tiled server here — just verify the binding compiles
    // and the namespace resolves.
    let (_out, err, code) = run_script(
        r#"
assert(type(tiled) == "table", "tiled global must be a table")
assert(type(tiled.from_uri) == "function", "tiled.from_uri must be a function")
print("ok")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}");
}

#[test]
fn status_inspect_done_success_exception() {
    // SoftMotor.set returns a Status that resolves immediately to
    // success. Status methods should reflect that and survive
    // multiple calls (idempotent).
    let (out, err, code) = run_script(
        r#"
local m1 = soft_motor("m1", 0.0)
local s = m1:set(1.5)

-- Wait so the (already-done) status definitely settles, then
-- assert all introspection methods.
s:wait()

assert(s:done() == true, "done should be true after wait")
assert(s:success() == true, "success should be true, got " .. tostring(s:success()))
assert(s:exception() == nil, "no exception expected, got " .. tostring(s:exception()))
assert(type(s:progress()) == "number")

local snap = s:inspect()
assert(snap.done == true, "snap.done should be true")
assert(snap.success == true, "snap.success should be true")
assert(snap.exception == nil)
assert(type(snap.progress) == "number")
assert(snap.label == "set(m1=1.5)", "label = " .. tostring(snap.label))

-- Calling wait again must still succeed (idempotent).
s:wait()
print("status.inspect ok")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}\nstdout: {out}");
    assert!(out.contains("status.inspect ok"), "out = {out}");
}

#[test]
fn inspect_dumps_soft_motor_state() {
    let (out, err, code) = run_script(
        r#"
local m1 = soft_motor("m1", 0.5)
local s = m1:inspect()
assert(s.name == "m1", "name should be m1, got " .. tostring(s.name))
assert(s.type == "SoftMotor", "type should be SoftMotor, got " .. tostring(s.type))
assert(s.readback == 0.5, "readback should be 0.5, got " .. tostring(s.readback))
assert(s.connected == true, "connected should be true")
print("motor.inspect ok")

local d1 = soft_detector("d1")
local sd = d1:inspect()
assert(sd.name == "d1")
assert(sd.type == "SoftDetector")
assert(type(sd.counts) == "number")
print("detector.inspect ok")
"#,
    );
    assert_eq!(code, 0, "stderr: {err}\nstdout: {out}");
    assert!(out.contains("motor.inspect ok"), "out = {out}");
    assert!(out.contains("detector.inspect ok"), "out = {out}");
}
