-- Verify CA monitor / read-pump path: poll `mini:current` (a
-- pulsed sinusoidal beam current updated by the IOC's background
-- thread) over a few seconds, observe the variation.
--
-- This script polls read() in a loop and checks that values change.
-- pva_detector is also monitorable (`bpp.monitor_during(plan, {d})`
-- streams every update as an Event); polling keeps the check
-- independent of the engine's monitor pump.

print("[monitor] connecting to mini:current via PVA...")
local d = pva_detector("beam_current", "mini:current")

local samples = {}
for i = 1, 10 do
    local r = d:read()
    samples[#samples + 1] = r.beam_current.value
    -- 200ms gap → 10 samples in 2 seconds
    coroutine.yield = nil  -- can't yield from main, do a busy wait
    local t0 = os.clock()
    while os.clock() - t0 < 0.2 do end
end

print("[monitor] samples (mA):")
for i, v in ipairs(samples) do
    print(string.format("  %2d: %.3f", i, v))
end

-- Verify variation: max - min should be > 1 mA for a sinusoidal
-- 100 mA amplitude.
local mn, mx = samples[1], samples[1]
for _, v in ipairs(samples) do
    if v < mn then mn = v end
    if v > mx then mx = v end
end
local delta = mx - mn
print(string.format("[monitor] range = [%.3f, %.3f]  Δ = %.3f", mn, mx, delta))
assert(delta > 1.0, "expected beam current to vary >1 mA over 2s, got " .. delta)
print("[monitor] OK — variation observed")
