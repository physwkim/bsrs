-- 08_suspender.lua — CA-PV-backed Suspender, armed-but-not-tripped.
--
-- Installs a `SuspendThreshold` against the mini-beamline beam
-- current (`mini:current`, oscillates 475–525) with the threshold
-- set well below the natural minimum so the watcher never fires.
-- Verifies that arming + running + stopping the suspender
-- introduces no overhead and the engine completes plans normally.
--
-- For the trip path (suspender actually pauses the engine), see
-- 08b_suspender_trip.lua.
--
-- Usage:
--   cargo run -p cirrus-cli --bin cirrus -- repl --script \
--       examples/mini_beamline/08_suspender.lua

local ph = ca_detector("ph_det", "mini:ph:DetValue_RBV")

-- threshold=200, direction=below → BAD only when current<200, but
-- mini:current oscillates 475..525 so it never enters BAD.
ca_suspend_threshold("low_beam", "mini:current", 200.0, "below")
print("[suspender] armed: pause when mini:current < 200 (never trips)")

print(string.format("[suspender] is_paused = %s", tostring(RE:is_paused())))

print("[suspender] starting count(ph, num=5)")
RE:run(bp.count({ph}, 5))
print("[suspender] count complete; is_paused = " .. tostring(RE:is_paused()))
