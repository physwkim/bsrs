-- Verify cirrus's TiledSink against a tiled-rs server. Documents
-- emitted by RE:run get registered into a Tiled container; we then
-- query the catalog from Lua via `tiled.from_uri(...)` to confirm
-- the run shows up.
--
-- Run with:
--     # terminal 1: start tiled-rs serve with sqlite catalog
--     TILED_SINGLE_USER_API_KEY=test123 \
--         ~/codes/tiled-rs/target/debug/tiled serve --port 8765 \
--         --catalog-uri 'sqlite:///tmp/cirrus-tiled.db' --api-key test123 &
--
--     # terminal 2:
--     cargo run -p cirrus-cli --features tiled -- repl \
--         --doc-tiled http://localhost:8765 \
--         --doc-tiled-key test123 \
--         --script examples/mini_beamline/07_tiled_sink.lua

local m = ca_motor("ph_mtr", "mini:ph:mtr.VAL", "mini:ph:mtr.RBV")
local d = ca_detector("ph_det", "mini:ph:DetValue_RBV")

print("[tiled] running 5-point CA scan...")
local result = RE:run(scan({d}, m, -1.0, 1.0, 5))
print("[tiled] result:", result)
assert(string.find(result, "exit_status=success", 1, true) ~= nil,
       "scan failed: " .. result)

-- Read-side: query the catalog to confirm the run was registered.
local cat = tiled.from_uri("http://localhost:8765?api_key=test123")
local keys = cat:keys()
print("[tiled] root keys:", table.concat(keys, ", "))
local has_cirrus = false
for _, k in ipairs(keys) do
    if k == "cirrus" then has_cirrus = true; break end
end
assert(has_cirrus, "expected 'cirrus' container at root, got: " .. table.concat(keys, ", "))

local cirrus_node = cat:get("cirrus")
local runs = cirrus_node:keys()
print("[tiled] runs in 'cirrus' container:", #runs)
assert(#runs >= 1, "expected ≥1 run registered, got " .. #runs)
print("[tiled] OK — found " .. #runs .. " run(s)")
