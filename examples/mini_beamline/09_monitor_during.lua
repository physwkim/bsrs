-- 09_monitor_during.lua — `bpp.monitor_during` over live CA and PVA
-- channels.
--
-- Wraps a 5-point `scan` in `bpp.monitor_during` with three
-- monitorable devices: a ca_motor (`mini:ph:mtr`), a ca_detector
-- (`mini:ph:DetValue_RBV`) and a pva_detector (`mini:current`). For
-- every device the engine must (1) emit a `<name>_monitor` descriptor
-- keyed by the device's data key and (2) deliver at least one Event
-- of that stream carrying the key and its timestamp, while the primary
-- stream still gets exactly one Event per scan point.

local ph_mtr = ca_motor("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV")
local ph_det = ca_detector("ph_det", "mini:ph:DetValue_RBV")
local beam = pva_detector("beam", "mini:current")
print(tostring(ph_mtr)); print(tostring(ph_det)); print(tostring(beam))

local streams = { ph_mtr_monitor = "ph_mtr", ph_det_monitor = "ph_det", beam_monitor = "beam" }
local desc, events, primary = {}, {}, 0
for s, _ in pairs(streams) do events[s] = 0 end

RE:subscribe(function(name, body)
    if name == "descriptor" then
        if streams[body.name] ~= nil then
            desc[body.uid] = body.name
            local key = streams[body.name]
            assert(body.data_keys[key] ~= nil,
                   body.name .. " descriptor must be keyed by " .. key)
            assert(body.data_keys[key].source ~= nil, body.name .. " data key has no source")
        elseif body.name == "primary" then
            desc[body.uid] = "primary"
        end
    elseif name == "event" then
        local s = desc[body.descriptor]
        if s == "primary" then
            primary = primary + 1
        elseif s ~= nil then
            events[s] = events[s] + 1
            local key = streams[s]
            assert(body.data[key] ~= nil, s .. " event must carry " .. key)
            assert(body.timestamps[key] ~= nil, s .. " event must carry a timestamp for " .. key)
        end
    end
end, "all")

local result = RE:run(bpp.monitor_during(scan({ph_det, beam}, ph_mtr, -4, 4, 5), {ph_mtr, ph_det, beam}))
print("[monitor_during] result:", result)
assert(string.find(result, "exit_status=success", 1, true) ~= nil, "scan failed: " .. tostring(result))
assert(primary == 5, "expected 5 primary events, got " .. primary)
for s, n in pairs(events) do
    print(string.format("[monitor_during] %-16s events=%d", s, n))
end
-- The IOC's beam current updates every 100 ms; the readback of the
-- moving motor updates at every poll; the point detector re-evaluates
-- per motor step. Each stream must have fired at least once.
for s, n in pairs(events) do
    assert(n >= 1, s .. " never fired")
end
assert(events.beam_monitor >= 5, "beam_monitor fired only " .. events.beam_monitor .. " times over a multi-second scan")
print("[monitor_during] OK")
