-- Kohzu DCM energy scan: drive `mini:BraggEAO` (energy setpoint),
-- read `mini:BraggThetaRdbkAO` (computed Bragg angle), and assert
-- the inverse Bragg relationship (E ∝ 1/sin(θ)) actually holds in
-- the captured events.
--
-- Two things make this a *real* motion test (a plain ca_motor on
-- BraggEAO returns at ao-record processing, seconds before the
-- kohzuCtl sequencer finishes moving theta/y/z, so every event
-- would read a stale readback):
--   1. the energy axis is a ca_positioner on the `mini:KohzuMoving`
--      busy flag (done_value 0 = "Done"), and
--   2. the sequencer only executes moves in Auto mode, so the
--      script switches `mini:KohzuModeBO` to Auto (1) first.

-- Put the Kohzu sequencer in Auto mode (idempotent; bo record).
local mode = ca_motor("kohzu_mode", "mini:KohzuModeBO", "mini:KohzuModeBO")
mode:move_to(1.0)

local energy = ca_positioner("dcm_energy", "mini:BraggEAO", "mini:BraggERdbkAO",
                             "mini:KohzuMoving", 0)
local theta = ca_detector("dcm_theta_rbv", "mini:BraggThetaRdbkAO")

print("[dcm] starting energy=", energy:locate().readback)
print("[dcm] starting theta=", theta:read().dcm_theta_rbv.value)

-- Capture (energy_rbv, theta_rbv) per event.
local points = {}
RE:subscribe(function(name, body)
    if name == "event" then
        table.insert(points, {
            e = body.data.dcm_energy,
            th = body.data.dcm_theta_rbv,
        })
    end
end)

-- 7-point energy scan: 6.0 to 12.0 keV in 1 keV steps.
print("[dcm] running 7-point energy scan 6 keV → 12 keV...")
local result = RE:run(scan({theta}, energy, 6.0, 12.0, 7))
print("[dcm] result:", result)

assert(string.find(result, "exit_status=success", 1, true) ~= nil,
       "dcm energy scan failed: " .. tostring(result))
assert(#points == 7, "expected 7 events, got " .. tostring(#points))

-- Physical assertions on the captured events.
local e_sin_th0 = nil
for i, p in ipairs(points) do
    local e_cmd = 6.0 + (i - 1)
    print(string.format("[dcm] point %d: E=%.4f keV  theta=%.4f deg", i, p.e, p.th))
    -- Energy readback tracked the commanded setpoint at capture time.
    assert(math.abs(p.e - e_cmd) < 0.01,
           string.format("point %d: energy readback %.4f != commanded %.1f", i, p.e, e_cmd))
    -- Theta strictly decreases as energy rises.
    if i > 1 then
        assert(p.th < points[i - 1].th,
               string.format("point %d: theta %.4f not < previous %.4f", i, p.th, points[i - 1].th))
    end
    -- Bragg: E * sin(theta) = hc / 2d is constant (within 0.1%).
    local e_sin_th = p.e * math.sin(math.rad(p.th))
    e_sin_th0 = e_sin_th0 or e_sin_th
    assert(math.abs(e_sin_th - e_sin_th0) / e_sin_th0 < 1e-3,
           string.format("point %d: E*sin(theta)=%.5f deviates from %.5f", i, e_sin_th, e_sin_th0))
end

print("[dcm] OK — Bragg relationship holds across all 7 points")
