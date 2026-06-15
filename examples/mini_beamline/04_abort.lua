-- Abort verification: start a long-running plan via a Lua
-- coroutine (sleep-yielding inside a run), schedule a re_abort
-- via a coroutine timer trick, expect the plan to finish with
-- exit_status != success.
--
-- Note: the local `bsrs repl` doesn't have a JSON-RPC dispatch
-- path, so we abort by calling `RE:abort()` directly from a
-- subscriber callback (fires from a worker thread when the first
-- Sleep msg's response comes back).
--
-- Cleaner verification of the abort path is the integration test
-- `re_abort_cancels_in_flight_lua_eval_plan` (bsrs-cli/tests/
-- cli_round_trip.rs); this script is the operator-facing
-- equivalent for a quick smoke.

local m = ca_motor("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV")

-- Subscribe early so we can fire abort from inside the run.
local fired = false
RE:subscribe(function(name, body)
    if name == "start" and not fired then
        fired = true
        -- abort the engine immediately after the run starts
        RE:abort("operator-initiated cancel")
    end
end)

-- 5 sleeps × 1 sec = 5 sec nominal duration. Abort should cut
-- it short.
local function long(secs, n)
    coroutine.yield(msg.open_run({plan_name = "abort_test"}))
    for i = 1, n do
        coroutine.yield(msg.sleep(secs))
    end
    coroutine.yield(msg.close_run("success"))
end

print("[abort] running long plan, abort scheduled inside on first 'start' doc...")
local t0 = os.clock()
local result = RE:run(plan(long, 1.0, 5))
local elapsed = os.clock() - t0

print(string.format("[abort] elapsed = %.2fs  result = %s", elapsed, result))
assert(elapsed < 4.0, "expected abort to cut the plan short (<4s); took " .. elapsed)
assert(string.find(result, "exit_status=success", 1, true) == nil,
       "expected non-success exit_status after abort: " .. result)
print("[abort] OK — abort cut the plan short")
