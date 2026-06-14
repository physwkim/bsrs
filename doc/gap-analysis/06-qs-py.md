# Gap Analysis: cirrus-qs vs bluesky-queueserver + cirrus-py vs ophyd-async/bluesky

**Date:** 2026-06-14  
**cirrus ref:** `crates/cirrus-qs/src/` (dispatch.rs, methods.rs, state.rs, queue.rs, registry.rs, transport.rs)  
**Python ref:** `daq/bluesky-queueserver/bluesky_queueserver/manager/` (manager.py, comms.py, plan_queue_ops.py)  
**cirrus-py ref:** `crates/cirrus-py/src/lib.rs`  
**Python-py ref:** `daq/ophyd-async/`, `daq/bluesky/`

---

## Part A — cirrus-qs vs bluesky-queueserver Wire Protocol

### What matches

All 44 method names from the Python `_zmq_execute` handler dict (manager.py:3699–3748) appear in
cirrus dispatch: the 37 implemented methods plus 6 NOT_IMPLEMENTED stubs
(`script_upload`, `function_execute`, `kernel_interrupt`, `permissions_set`,
`manager_stop`, `manager_kill`). No method name is entirely absent. The ZMQ REP
socket runs on the same default port 60615 (server.rs:56, comms.py:22).

---

### P0 — correctness / protocol divergence

---

#### QS-01 — ZMQ envelope incompatible: Python uses `{method, params}` not JSON-RPC 2.0

**cirrus:** `crates/cirrus-qs/src/transport.rs:52`, `crates/cirrus-qs/src/methods.rs:8–19`  
`RpcRequest { jsonrpc: String, method: String, params: Value, id: Option<Value> }`  
(`jsonrpc` is a non-optional `String`; missing field → serde error → INVALID_REQUEST reply)

**ref:** `daq/bluesky_queueserver/manager/comms.py:921–932`  
`_create_msg` builds `{"method": method, "params": params}` — no `jsonrpc`, no `id`.  
`_zmq_execute` (manager.py:3759) explicitly enforces `allowed_keys = ("method", "params")` and
raises on extra keys (including `jsonrpc`).  
Responses are flat dicts `{"success": bool, "msg": str, ...field...}` not JSON-RPC `result/error` objects.

**Gap:** The Python queueserver REManagerAPI client sends `{"method":…,"params":…}` and reads
top-level `success/msg`. cirrus-qs requires `{"jsonrpc":"2.0",…}` and wraps responses as
`{"jsonrpc":"2.0","result":{…}}`. Every call from REManagerAPI or `qserver` CLI will fail with
INVALID_REQUEST (cirrus) or a KeyError on `success` (client parsing cirrus response).

**Fix sketch:** Replace `RpcRequest.jsonrpc: String` with `Option<String>` (serde default None)
and treat absent/null as tolerated. On the send side, add a compatibility mode that serialises
`result` fields at top level alongside `success`/`msg`. The cleanest structural fix is a thin
adapter in `transport.rs::try_recv` that detects the `jsonrpc` key and routes to the JSON-RPC
path; otherwise translates the plain `{method, params}` envelope and re-wraps the flat
`{success, msg, …}` response. Keep JSON-RPC as the native internal path.

**Effort:** M

---

#### QS-02 — `ping` returns `"pong"` instead of full status

**cirrus:** `dispatch.rs:70`  
`"ping" => RpcResponse::ok(id, json!({"success": true, "msg": "pong"}))`

**ref:** `manager.py:1888–1892`  
`_ping_handler` calls `_status_handler` verbatim — returns the full status dict.

**Gap:** Any client that calls `ping` to warm-up or health-check and then reads
`manager_state`, `items_in_queue`, or any other status field from the response gets
nothing but `{"success":true,"msg":"pong"}`.

**Fix sketch:** Route `"ping"` through the same `status_response(…)` helper instead of the
one-liner, or make `"ping"` call `"status"` internally.

**Effort:** S

---

#### QS-03 — `plans_allowed` / `devices_allowed` return list of names, not rich dict

**cirrus:** `dispatch.rs:87–122`  
`"plans_allowed": registry.plan_names()` → `Vec<String>` (JSON array of strings)  
`"devices_allowed": registry.device_names()` → `Vec<String>`  
`"plans_existing"` / `"devices_existing"` same.

**ref:** `manager.py:1927–1955`  
Returns `{"plans_allowed": {name: {description, parameters, module, annotation, ...}}}` —
a dict keyed by plan name, each value a rich schema dict with parameter types, defaults,
docstrings. `user_group` param is **required** (exception raised if absent, line 1937).

**Gap:** Python clients (e.g. bluesky-widgets, QASE) iterate the returned dict and read
`.parameters`, `.description`, `.module` for each plan/device. An array of strings provides
none of that; clients silently get an empty parameter schema or crash on dict access.
Also, `user_group` filtering is completely absent in cirrus — all plans are returned
regardless of caller group.

**Fix sketch:** Introduce a `PlanMeta` struct with `name`, `description: Option<String>`,
`parameters: Vec<ParamSpec>` fields. Populate at registry registration time via
`register_plan_with_meta`. `plans_allowed` returns `{name: plan_meta_json}` dict.
`user_group` param: honour by consulting the Permissions table for which plans that
group may call, filtering the registry snapshot accordingly.

**Effort:** L

---

#### QS-04 — `queue_item_add` response returns `item_uid`, not full `item`

**cirrus:** `dispatch.rs:688–699`  
Returns `{"success", "msg", "qsize", "item_uid": …, "plan_queue_uid": …}`.

**ref:** `manager.py:2412–2413`  
Returns `{"success", "msg", "qsize", "item": <full_item_dict_with_uid>}`.

**Gap:** Python clients destructure `resp["item"]["item_uid"]`, `resp["item"]["args"]`, etc.
The `item_uid` top-level key is not what they read; they get KeyError or None.

**Fix sketch:** After inserting, serialize the `QueuedItem` as the full `item` payload.
Replace `"item_uid": item_uid` with `"item": serde_json::to_value(&queued).unwrap()`.
Remove the now-redundant `item_uid` top-level key.

**Effort:** S

---

#### QS-05 — `queue_item_add_batch` response key `items_added` should be `items`, missing `results`

**cirrus:** `dispatch.rs:730–739`  
Returns `{"success","msg","qsize","items_added":[uid…],"plan_queue_uid"}`.

**ref:** `manager.py:2530–2531`  
Returns `{"success","msg","qsize","items":[<full_item_dicts>],"results":[{"success":bool,"msg":str}…]}`.

**Gap:** (1) Key is `items_added` (list of UID strings) vs `items` (list of full item dicts). (2)
`results` array with per-item success/msg is missing entirely. Batch clients iterate `resp["items"]`
and check per-item `results` — both fail.

**Fix sketch:** Collect added `QueuedItem`s serialized to Value; return them under key `items`.
Add `results: Vec<{success,msg}>` in parallel, one entry per input item.

**Effort:** S

---

#### QS-06 — `re_metadata` response key is `metadata`, should be `re_metadata`

**cirrus:** `dispatch.rs:1086–1092`  
`json!({"success":true,"msg":"","metadata": re.md()})`

**ref:** `manager.py:3407`  
`{"success": success, "msg": msg, "re_metadata": re_metadata}`

**Gap:** Clients reading `resp["re_metadata"]` get None; cirrus returns the data under `"metadata"`.

**Fix sketch:** Rename the JSON key from `"metadata"` to `"re_metadata"` in the `re_metadata` arm.

**Effort:** S

---

#### QS-07 — `manager_state` missing five transitional states

**cirrus:** `state.rs:10–21` — EState: EnvironmentClosed, Idle, ExecutingQueue, Paused, Aborting  
Emitted strings: `"environment_closed"`, `"idle"`, `"executing_queue"`, `"paused"`, `"aborting"`.

**ref:** `manager.py:50–59` — MState: INITIALIZING, IDLE, PAUSED, CREATING_ENVIRONMENT,
STARTING_QUEUE, EXECUTING_QUEUE, EXECUTING_TASK, CLOSING_ENVIRONMENT, DESTROYING_ENVIRONMENT.

**Gap:** Clients polling `manager_state` to wait for "idle" after `environment_open` must
observe the "creating_environment" → "idle" transition. In cirrus the environment opens
synchronously, so the state jumps directly to idle — clients waiting for the specific
"creating_environment" state get an instant "idle" they may interpret as "already closed".
More critically: `"aborting"` is cirrus-only (not in Python), and `"executing_task"`,
`"closing_environment"`, `"destroying_environment"` / `"starting_queue"` are Python-only.
Any client code branching on exact state strings will misclassify cirrus states.

**Fix sketch:** Add an optional brief `CreatingEnvironment` / `ClosingEnvironment` transient
emission within `env_open`/`env_close` before the final state is committed, to satisfy
polling clients. Add `ExecutingTask` (maps to `lua_eval` execution path). Rename or
alias `"aborting"` to match Python's `"executing_queue"` with a secondary `re_state` of
"aborting" (which is how Python represents it: manager stays EXECUTING_QUEUE; RE state
goes to "aborting").

**Effort:** M

---

### P1 — meaningful completeness gap

---

#### QS-08 — `pause_pending` missing from status

**cirrus:** `dispatch.rs:549–601` — status JSON has no `pause_pending` key.

**ref:** `manager.py:434` — `"pause_pending": self._re_pause_pending` (True when a deferred
`re_pause` was accepted but not yet acted on by the worker).

**Gap:** UIs that show a "pausing" indicator, and plans that check pause_pending to drain
safely, get None / KeyError.

**Fix sketch:** Add `pause_pending: bool` field to `EngineState`; set it true in the
`re_pause` handler when `option == "deferred"`, clear it when the engine transitions to
Paused. Emit `"pause_pending": st.pause_pending` in `status_response`.

**Effort:** S

---

#### QS-09 — `queue_item_add` / `queue_item_add_batch` missing positional insertion (`pos`, `before_uid`, `after_uid`)

**cirrus:** `dispatch.rs:668–739` — `queue_item_add` always calls `q.push_back(queued)`;
`queue_item_add_batch` always appends. No `pos`, `before_uid`, or `after_uid` param read.

**ref:** `manager.py:2378,2394–2400` — supports `pos` (string "front"/"back" or integer index,
including negative), `before_uid`, `after_uid`; `plan_queue_ops.py::add_item_to_queue` implements
these. Batch equivalent at manager.py:2475,2489–2491.

**Gap:** Inserting into the front or at a specific position (very common for priority
override during a run) always silently falls to back. Before-/after-uid insertion
(used by drag-and-drop UIs) is absent.

**Fix sketch:** Add `insert_at(idx, item)` and `insert_before_uid(uid, item)` /
`insert_after_uid(uid, item)` to `PlanQueue`. In `queue_item_add`, read `pos`,
`before_uid`, `after_uid` from params and dispatch accordingly (negative index →
`len - |idx|`, clamp to 0/len). Same for batch.

**Effort:** M

---

#### QS-10 — `function_execute` is unimplemented

**cirrus:** `dispatch.rs:419–430` — NOT_IMPLEMENTED stub.

**ref:** `manager.py:2992–3060` — executes a named user function (registered in the worker
namespace) as a background task, returns `task_uid` for polling via `task_result`.

**Gap:** Beamline scripts that call `function_execute` (common for ad-hoc calibrations,
alignment routines) get NOT_IMPLEMENTED. In cirrus's model this maps to `lua_eval`,
but a standard bluesky client uses `function_execute`.

**Fix sketch:** Define a `function_execute` that accepts `{"item": {"name":…,"args":…,"kwargs":…}}`
and routes to the existing Lua evaluator via task_tracker (same path as `lua_eval`).
Return the same `{"success","msg","task_uid"}` shape.

**Effort:** M

---

#### QS-11 — `instruction` item_type not supported in `queue_item_add`

**cirrus:** `dispatch.rs:678–682` — `queue_item_add` reads `item.name` and looks up a plan;
no `item_type` dispatch. `queue.rs:12–15` — `QueuedItem.item_type` is always set to `"plan"`.

**ref:** `manager.py:2161–2205` — `_get_item_from_request` reads `item["item_type"]`
(required), validates against `("plan", "instruction")`. Instructions (e.g.
`{"item_type":"instruction","name":"queue_stop"}`) are processed differently at queue
execution time (manager.py:1154–1169): "queue_stop" stops the queue after that item
without a plan run.

**Gap:** Clients that insert `queue_stop` instructions to pause execution after N items get
a "unknown plan: queue_stop" error or incorrect behaviour (the item treated as a plan).

**Fix sketch:** Add `ItemType::Instruction` variant. In the queue runner (`server.rs`
`execute_queue_loop`), when an item's `item_type` is "instruction" and `name` is
"queue_stop", set `queue_stop_pending = true` then continue (skip plan execution).
Validate `item_type` in `queue_item_add`.

**Effort:** M

---

#### QS-12 — msgpack encoding not supported

**cirrus:** `transport.rs:51–52` — parses received bytes with `serde_json::from_slice`; sends
`serde_json::to_vec`. No msgpack path.

**ref:** `comms.py:24–51` — `ZMQEncoding.JSON` / `ZMQEncoding.MSGPACK`; client configured via
`--zmq-encoding` flag. Manager at lines 3791–3795 dispatches on encoding for recv; at 3808–3812
for send.

**Gap:** Any client built with `ZMQEncoding.MSGPACK` sends binary msgpack frames; cirrus
`serde_json::from_slice` errors; client gets INVALID_REQUEST. Affects high-frequency
polling deployments.

**Fix sketch:** In `transport.rs::try_recv`, probe the first byte: `0x7b` (`{`) → JSON path;
otherwise attempt msgpack deserialization (`rmp_serde::from_slice`). For send, mirror the
encoding the client used (track per-request or negotiate via first message).

**Effort:** M

---

#### QS-13 — `manager_stop` returns NOT_IMPLEMENTED (no clean shutdown path)

**cirrus:** `dispatch.rs:419–430` — `manager_stop` returns NOT_IMPLEMENTED.

**ref:** `manager.py:3746,1839–1842` — `_manager_stop_handler` sends `manager_stopping`
to watchdog and initiates graceful shutdown of the manager process.

**Gap:** Clients that call `manager_stop` to shut down the daemon get an error. There is
no RPC-level way to stop cirrus-qs; only OS signals work.

**Fix sketch:** In `server.rs`, expose a shutdown token (tokio CancellationToken). In
`dispatch`, pass a clone; `"manager_stop"` calls `token.cancel()` and returns success.
The `run_blocking` loop exits when token fires.

**Effort:** S

---

#### QS-14 — `plans_allowed` / `devices_allowed` require `user_group` param; cirrus ignores it

**cirrus:** `dispatch.rs:87–122` — `user_group` param not read; same list returned to all callers.

**ref:** `manager.py:1934–1945` — `user_group` is **required**; error raised if absent.
Returns only plans/devices that user's group is allowed to execute.

**Gap:** Standard qserver CLI always sends `user_group=primary` (qserver_cli.py:49).
Clients that omit `user_group` get an error from Python but silently succeed from cirrus.
More importantly: cirrus never filters by group, so a "read-only" group can see and
presumably submit all plans — permissions are only enforced at submit time via RBAC, not
at list-discovery time.

**Fix sketch:** Make `user_group` optional in cirrus (tolerate absence). When present and
the Permissions table contains group → allowed-plans mappings, intersect `registry.plan_names()`
with that set before returning. Same for devices.

**Effort:** S (tolerate absence) / M (full group-filtered listing)

---

#### QS-15 — Queue items missing `user` / `user_group` attribution

**cirrus:** `queue.rs:12–27` — `QueuedItem` has no `user` or `user_group` fields.

**ref:** `manager.py:1124–1134` — each item carries `user` and `user_group` injected by
`_prepare_item`; stored in queue and returned in `queue_get` / `history_get` responses.

**Gap:** History entries and queue snapshots returned by cirrus carry no provenance. Clients
that display or audit who submitted a plan (common in multi-user beamline ops) see no user.

**Fix sketch:** Add `pub user: Option<String>` and `pub user_group: Option<String>` to
`QueuedItem`. In `queue_item_add`, read `params["user"]` and `params["user_group"]` and
store. Serialize into snapshot/history responses.

**Effort:** S

---

### P2 — nice-to-have / completeness

---

#### QS-16 — `status_uid` missing from status response

**cirrus:** `dispatch.rs:564–601` — no `status_uid` field.

**ref:** `manager.py:436` — `"status_uid": _generate_uid()` — new UUID every status call;
used by polling clients to detect a changed status without deep equality checks.

**Fix sketch:** Add `status_uid: _generate_uid()` to the `status_response` output JSON.

**Effort:** S

---

#### QS-17 — `time` field missing from status response

**cirrus:** `dispatch.rs:564–601` — no `time` field.

**ref:** `manager.py:421` — `"time": self._get_timestamp_iso8601()`.

**Fix sketch:** Emit `"time": now_iso8601()` (the helper already exists in `state.rs:97`).

**Effort:** S

---

#### QS-18 — `worker_background_tasks` missing from status

**cirrus:** `dispatch.rs:564–601` — absent.

**ref:** `manager.py:430` — `"worker_background_tasks": background_tasks` (count of
background tasks executing in the worker process).

**Fix sketch:** Add `background_tasks: u64` to `EngineState`; increment/decrement around
`lua_eval` task spawns. Emit as `"worker_background_tasks": st.background_tasks`.

**Effort:** S

---

#### QS-19 — `config_get` response shape mismatch

**cirrus:** `dispatch.rs:72–84`  
`"config": {"implementation": "cirrus-qs", "runtime": "rust", "version": …, "wire_protocol": …}`

**ref:** `manager.py:1919–1924`  
`"config": {"ip_connect_info": {}}` (always `{}` when no IPython kernel).

**Gap:** Clients reading `resp["config"]["ip_connect_info"]` get KeyError. Different shapes.

**Fix sketch:** Keep cirrus-specific keys but also include `"ip_connect_info": {}` in
the config dict for compatibility.

**Effort:** S

---

#### QS-20 — `queue_item_move` and `queue_item_move_batch` missing `before_uid` / `after_uid` / `reorder`

**cirrus:** `dispatch.rs:869–925` — only `uid` + `pos_dest` (front/back/int).

**ref:** `manager.py:2679–2733` — supports `before_uid`, `after_uid`, `reorder` for move
and move_batch.

**Fix sketch:** Extend `PlanQueue::move_to` with `move_before_uid(item_uid, ref_uid)` /
`move_after_uid`; read `before_uid`/`after_uid` in dispatch.

**Effort:** S

---

#### QS-21 — `queue_item_update` missing `replace` parameter

**cirrus:** `dispatch.rs:742–774` — always keeps the same UID.

**ref:** `manager.py:2552–2564` — `replace: bool` → generate new UID before replacing.

**Fix sketch:** In `queue_item_update`, read `params["replace"].as_bool()`. If true, call
`Uuid::new_v4()` for the replacement item's UID before calling `q.update`.

**Effort:** S

---

#### QS-22 — `re_runs` `option` parameter ignored; `is_open` always false

**cirrus:** `dispatch.rs:1055–1065` — ignores `option`; emits `{"uid":…, "is_open": false}`.

**ref:** `manager.py:3344–3358` — filters by `option = "active"|"open"|"closed"`.

**Fix sketch:** Track run open/closed state in `EngineState.re_runs` as
`Vec<(uid, is_open)>`. Filter by `option` param in `re_runs` dispatch arm.

**Effort:** S

---

#### QS-23 — ZMQ CURVE encryption not supported

**cirrus:** `transport.rs:18–34` — plain `zmq::REP` socket, no CURVE key configuration.

**ref:** `comms.py:893–898` — supports optional server-public-key CURVE authentication.

**Fix sketch:** Expose `ServerBuilder::with_curve_key_pair(server_pk, server_sk)`;
call `socket.set_curve_server(true)` + `set_curve_publickey` / `set_curve_secretkey`.

**Effort:** M

---

#### QS-24 — `environment_destroy` is an alias for `environment_close`

**cirrus:** `dispatch.rs:157` — `"environment_destroy" => env_close(…)` (identical call).

**ref:** `manager.py:644–673` — `_environment_destroy_handler` forcibly kills the worker
process (SIGKILL path via watchdog), whereas `_environment_close_handler` sends a graceful
close request. The difference matters when the worker is hung.

**Fix sketch:** Add `env_force_close` variant in dispatch that sets a `force: bool` flag
or directly aborts the running queue task before dropping the engine.

**Effort:** S

---

## Part B — cirrus-py vs bluesky / ophyd-async Python API

### What exists

`cirrus_native` module (crates/cirrus-py/src/lib.rs) exposes:
- `SoftMotor(name, initial=0.0)` — name accessor, `__repr__`
- `SoftDetector(name)` — name accessor
- `Plan` — opaque, single-use handle
- `RunEngine()` — `.run(plan)` → `(exit_status, run_uid)`, releases GIL
- `count(detectors, num)` — plan factory
- `scan(detectors, motor, motor_reader, start, stop, num)` — plan factory
- `version()`

---

### P0 — correctness / essential API missing

---

#### PY-01 — No document subscription callback

**cirrus-py:** `lib.rs:121–138` — `run()` returns `(exit_status, run_uid)`, no subscription.

**ref:** bluesky `RunEngine.subscribe(cb, name='all')` → callback receives every document
(start, descriptor, event, resource, datum, stop). This is the primary output mechanism.

**Gap:** Any Python code that does `RE.subscribe(db.insert)` (the universal data-collection
line) is silently dropped — no data is stored. A cirrus RunEngine with a document sink
must expose subscription, not just a run result tuple.

**Fix sketch:** Expose `RunEngine.subscribe(callable)` from PyO3: wrap the Python callable
in a `Arc<dyn DocumentSink>` bridged via `Python::with_gil`. Call it through the Rust
`DocumentSink::push` path. Return a token integer from `subscribe`; expose `unsubscribe(token)`.

**Effort:** M

---

### P1 — meaningful completeness gap

---

#### PY-02 — No EPICS-backed device bindings

**cirrus-py:** `lib.rs:41–87` — only `SoftMotor`, `SoftDetector` (in-memory, no EPICS).

**ref:** ophyd-async `EpicsMotor`, `EpicsSignalRO`, `EpicsSignal` connect to real hardware via
CA/PVA. This is the dominant device class for beamline experiments.

**Gap:** Users cannot drive real EPICS hardware from Python using cirrus devices. The only
path is soft-device simulation.

**Fix sketch:** Expose `CaMotor(pv_prefix, name)` and `CaSignal(pv, name)` backed by
`cirrus-backend-ca` (if available). Use PyO3 async integration for the `connect()` call;
wrap the underlying `Arc<CaMotor>` as a `PyCaMotor`. Start with read-only CA signal for
MVP.

**Effort:** L

---

#### PY-03 — Device protocol methods not callable from Python

**cirrus-py:** `PySoftMotor` / `PySoftDetector` — expose only `name()` and `__repr__()`.

**ref:** ophyd/ophyd-async devices expose `read()`, `set(value)`, `trigger()`, `stage()`,
`unstage()` as async or sync callables.

**Gap:** Python users cannot independently call `motor.set(1.0)` or `detector.read()`.
All interaction is through plan factories only.

**Fix sketch:** Add `#[pymethods]` for `read()` → `dict`, `set(value)` → None, `trigger()` → None
on `PySoftMotor`/`PySoftDetector`. Bridge through `py.allow_threads(…)` + `block_on`.

**Effort:** S

---

#### PY-04 — Minimal plan set (missing `grid_scan`, `rel_scan`, `mv`, `abs_set`, etc.)

**cirrus-py:** `lib.rs:141–190` — only `count` and `scan`.

**ref:** `bluesky/plans/__init__.py` — `bp.*` includes `grid_scan`, `rel_scan`, `list_scan`,
`spiral`, `fly`, `mv`, `mvr`, `abs_set`, `rel_set`, `trigger_and_read`, `one_nd_step`, etc.

**Gap:** Any beamline that uses `bp.grid_scan` or `bp.rel_scan` has no cirrus-py equivalent.

**Fix sketch:** Expose Rust `cirrus_plans` equivalents. `grid_scan`, `rel_scan` are highest
priority. Add as `#[pyfunction]` wrappers following the same pattern as `scan`.

**Effort:** M

---

#### PY-05 — No user-defined Python plan execution

**cirrus-py:** `lib.rs` — only Rust-registered plan factories can be run. No mechanism for
Python generator plans.

**ref:** bluesky plans are Python generators that `yield Msg(...)`. The RunEngine dispatches
each `Msg` to the appropriate device. This is the core extensibility model.

**Gap:** Users cannot write `def my_plan(): yield from bp.count([det])` and run it via
cirrus-py. Completely blocks standard bluesky plan authoring.

**Fix sketch:** In cirrus-py, accept a Python callable/generator as `run(plan_gen)`. Wrap it
in a Rust `Plan` that iterates the Python generator inside `run_async`, extracting `Msg`
objects and executing them via the existing cirrus device dispatch. This requires a Msg
→ Rust dispatch table. High effort but unlocks the bluesky ecosystem.

**Effort:** L

---

### P2 — nice-to-have

---

#### PY-06 — RunEngine metadata not accessible from Python

**cirrus-py:** `lib.rs` — no `RE.md` accessor.

**ref:** bluesky `RE.md` is a `dict`-like that is merged into every start document.

**Fix sketch:** Add `RunEngine.md` property that reads/writes via `re.md()` / `re.md_replace()`.

**Effort:** S

---

#### PY-07 — No `async` RunEngine integration

**cirrus-py:** `lib.rs:120–138` — `run()` is blocking (releases GIL via `allow_threads`).

**ref:** bluesky `RunEngine` can be integrated into asyncio via `RE.call_returns_result` /
`awaitable_run`. Needed for notebook-based interactive scanning.

**Fix sketch:** Expose `async def run_async(plan)` using `pyo3_asyncio`; bridge to
`re.run_async(plan)` via the existing tokio runtime.

**Effort:** M

---

#### PY-08 — No RemoteDispatcher / pub-sub document stream

**cirrus-py:** no pub-sub mechanism.

**ref:** bluesky `RemoteDispatcher` connects to a ZMQ PUB socket and replays documents to
subscribed callbacks. cirrus-qs already has a `document_address` PUB socket (`server.rs:57`).

**Fix sketch:** Expose `cirrus_native.RemoteDispatcher(address)` that connects to
the cirrus document PUB socket and calls `subscribe(cb)` callbacks on received documents.

**Effort:** M

---

## Priority Summary

| ID | Title | Priority | Effort |
|----|-------|----------|--------|
| QS-01 | Wire envelope: plain `{method,params}` vs JSON-RPC 2.0 | P0 | M |
| QS-02 | `ping` must return status dict | P0 | S |
| QS-03 | `plans_allowed`/`devices_allowed` must return rich dict, not name list | P0 | L |
| QS-04 | `queue_item_add` response must include full `item`, not bare `item_uid` | P0 | S |
| QS-05 | `queue_item_add_batch` key `items_added` → `items` + add `results` | P0 | S |
| QS-06 | `re_metadata` response key `metadata` → `re_metadata` | P0 | S |
| QS-07 | `manager_state` missing transitional states | P0 | M |
| QS-08 | `pause_pending` missing from status | P1 | S |
| QS-09 | Positional insertion (`pos`, `before_uid`, `after_uid`) in `queue_item_add` | P1 | M |
| QS-10 | `function_execute` not implemented | P1 | M |
| QS-11 | `instruction` item_type not supported | P1 | M |
| QS-12 | Msgpack encoding not supported | P1 | M |
| QS-13 | `manager_stop` returns NOT_IMPLEMENTED | P1 | S |
| QS-14 | `plans_allowed` ignores `user_group` param | P1 | S/M |
| QS-15 | Queue items missing `user`/`user_group` attribution | P1 | S |
| QS-16 | `status_uid` missing from status | P2 | S |
| QS-17 | `time` missing from status | P2 | S |
| QS-18 | `worker_background_tasks` missing from status | P2 | S |
| QS-19 | `config_get` response shape mismatch | P2 | S |
| QS-20 | `queue_item_move*` missing `before_uid`/`after_uid` | P2 | S |
| QS-21 | `queue_item_update` missing `replace` param | P2 | S |
| QS-22 | `re_runs` `option` ignored; `is_open` always false | P2 | S |
| QS-23 | ZMQ CURVE encryption not supported | P2 | M |
| QS-24 | `environment_destroy` is alias for `environment_close` | P2 | S |
| PY-01 | No document subscription callback | P0 | M |
| PY-02 | No EPICS-backed device bindings | P1 | L |
| PY-03 | Device protocol methods not callable from Python | P1 | S |
| PY-04 | Minimal plan set (missing grid_scan, rel_scan, mv, etc.) | P1 | M |
| PY-05 | No user-defined Python plan execution (generator plans) | P1 | L |
| PY-06 | RunEngine metadata not accessible from Python | P2 | S |
| PY-07 | No async RunEngine integration | P2 | M |
| PY-08 | No RemoteDispatcher / pub-sub document stream | P2 | M |

**Counts:** P0: 8, P1: 10, P2: 12 (total: 30)
