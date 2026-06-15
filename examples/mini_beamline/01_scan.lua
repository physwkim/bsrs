-- Lua-driven verification of bsrs against the epics-rs
-- mini-beamline IOC. Run alongside the IOC:
--
--   # terminal 1: start the mini-beamline IOC
--   cd ~/codes/epics-rs
--   ./target/release/mini_ioc examples/mini-beamline/ioc/st.cmd
--
--   # terminal 2: drive bsrs from Lua
--   cd ~/codes/bsrs
--   cargo run -p bsrs-cli -- repl --script examples/mini_beamline_scan.lua
--
-- Equivalent to crates/bsrs/examples/mini_beamline_scan.rs but
-- driven through bsrs's Lua bridge — proves the same end-to-end
-- path (CA backend → device traits → RunEngine → Documents) works
-- when the user writes Lua instead of Rust.

print("[lua] connecting to mini-beamline IOC via CA...")

local m = ca_motor("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV")
local d = ca_detector("ph_det", "mini:ph:DetValue_RBV")

print("[lua] motor:", m)
print("[lua] det  :", d)

-- Mini-beamline's sim motor ships at VELO=0.2 — too slow to fit
-- inside the 30 s WRITE_NOTIFY put timeout per scan step. Bump it
-- via a dedicated CA channel before the scan. Real beamlines do
-- this at setup time.
local velo = ca_detector("ph_velo", "mini:ph:mtr.VELO")  -- read-only handle
print("[lua] current VELO:", velo:read().ph_velo.value)

-- Need to write VELO too — use a movable handle for that. We can't
-- use ca_motor (no .RBV needed); easiest is to write directly via
-- bsrs's `caput` shortcut if available, or skip if VELO already
-- set. For this script we assume the operator pre-set VELO=5.0
-- (or larger). See the Rust example for an inline put.

-- Read once before the scan to confirm the connection.
local pre = d:read()
print("[lua] pre-scan det value:", pre.ph_det.value)

-- 17-point scan from -8 to 8, covering the PinHole gaussian peak.
local plan = scan({d}, m, -8.0, 8.0, 17)

-- Subscribe to count Events. The Lua-side subscriber buffers
-- worker-thread emissions and drains them on the REPL thread
-- AFTER RE:run returns; by the time we read the counters below,
-- both the events and the stop document have been delivered.
local event_count = 0
local exit_status_seen = nil
RE:subscribe(function(name, body)
    if name == "event" then
        event_count = event_count + 1
    elseif name == "stop" then
        exit_status_seen = body.exit_status
    end
end)

print("[lua] running scan...")
local result = RE:run(plan)
print("[lua] result:", result)

-- Primary assertion: result string carries the exit status. This is
-- what RE:run synchronously returns regardless of subscriber timing.
assert(string.find(result, "exit_status=success", 1, true) ~= nil,
       "expected exit_status=success in result, got: " .. tostring(result))

-- Subscriber-side counters: report what we observed (best-effort).
print("[lua] subscriber observed " .. event_count .. " events, " ..
      "stop.exit_status=" .. tostring(exit_status_seen))
assert(event_count == 17,
       "subscriber expected 17 Event documents, got " .. tostring(event_count))

print("[lua] OK")
