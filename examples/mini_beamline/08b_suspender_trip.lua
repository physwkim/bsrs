-- 08b_suspender_trip.lua — exercise the SuspendThreshold pause path.
--
-- mini:current is a deterministic ai record updated by a
-- background thread inside the IOC: 500 + 25*sin(2*pi*t/4),
-- oscillating between 475 and 525 with period 4s. caput has no
-- lasting effect (the IOC overwrites on every tick), so we can't
-- "trip" the suspender from the outside; instead we set a
-- threshold *inside* the natural oscillation range and let the
-- IOC drive it through BAD/GOOD cycles on its own.
--
-- Threshold = 499, direction = "below" → BAD every 4 s for ~2 s.
-- A 12-step (read, sleep 0.5 s) plan would take ~6 s baseline;
-- with periodic pauses it takes substantially longer.

local ph = ca_detector("ph_det", "mini:ph:DetValue_RBV")
ca_suspend_threshold("low_beam", "mini:current", 499.0, "below")
print("[trip] armed: pause when mini:current < 499 (oscillates 475..525)")

-- 12 × (read, sleep 0.5 s) = 6 s baseline.
local function long_count(num, dt)
    coroutine.yield(msg.open_run({plan_name = "suspender_trip"}))
    coroutine.yield(msg.declare_stream("primary", {ph}))
    for i = 1, num do
        coroutine.yield(msg.create("primary"))
        coroutine.yield(msg.read(ph))
        coroutine.yield(msg.save())
        coroutine.yield(msg.sleep(dt))
    end
    coroutine.yield(msg.close_run("success"))
end

print("[trip] starting 12-iter (read, sleep 0.5) plan")
local t0_real = os.time()
local result = RE:run(plan(long_count, 12, 0.5))
local elapsed = os.difftime(os.time(), t0_real)
print(string.format("[trip] complete in %ds; final paused = %s",
    elapsed, tostring(RE:is_paused())))
if elapsed >= 9 then
    print("[trip] OK: elapsed>=9s (baseline 6s) — pause/resume engaged.")
else
    print("[trip] WARNING: elapsed<9s — suspender did not pause as expected.")
end
