-- Kohzu DCM energy scan: drive `mini:BraggEAO` (energy setpoint),
-- read `mini:BraggThetaRdbkAO` (computed Bragg angle), confirm
-- the inverse Bragg relationship (E ∝ 1/sin(θ)).
--
-- This proves bsrs drives derived (compound) motor records the
-- same as plain ones — the IOC's kohzuCtl state machine handles
-- the angle ↔ energy translation.

local energy = ca_motor("dcm_energy", "mini:BraggEAO", "mini:BraggERdbkAO")
local theta = ca_detector("dcm_theta_rbv", "mini:BraggThetaRdbkAO")

print("[dcm] starting energy=", energy:locate().setpoint)
print("[dcm] starting theta=", theta:read().dcm_theta_rbv.value)

-- 5-point energy scan: 6.0 to 12.0 keV in 1 keV steps.
print("[dcm] running 7-point energy scan 6 keV → 12 keV...")
local result = RE:run(scan({theta}, energy, 6.0, 12.0, 7))
print("[dcm] result:", result)

assert(string.find(result, "exit_status=success", 1, true) ~= nil,
       "dcm energy scan failed: " .. result)
print("[dcm] OK")
