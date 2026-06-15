-- Drives a small CA scan while every Document fans out to the
-- ZMQ PUB endpoint passed via `bsrs repl --doc-zmq`. Pair with
-- `05_remote_dispatcher.py` on the Python side; that script
-- attaches a `bluesky.callbacks.zmq.RemoteDispatcher` and asserts
-- the Documents arrive in the bluesky shape.
--
-- Usage:
--     # terminal 1: start Python subscriber
--     ~/mamba/envs/bs2026.1/bin/python \
--         examples/mini_beamline/05_remote_dispatcher.py tcp://localhost:5577
--
--     # terminal 2: bsrs publishes
--     cargo run -p bsrs-cli -- repl \
--         --doc-zmq 'tcp://*:5577' \
--         --script examples/mini_beamline/05_remote_dispatcher.lua

local m = ca_motor("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV")
local d = ca_detector("ph_det", "mini:ph:DetValue_RBV")

print("[lua] running 5-point scan, fanning out to ZMQ...")
local result = RE:run(scan({d}, m, -2.0, 2.0, 5))
print("[lua] result:", result)
assert(string.find(result, "exit_status=success", 1, true) ~= nil,
       "scan failed: " .. result)
print("[lua] OK")
