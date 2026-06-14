# Gap Analysis — Callbacks, Event-Model, and Stream/Writers

**Area:** `cirrus-callbacks`, `cirrus-event-model`, `cirrus-stream`
**Ref:** `daq/event-model/src/event_model/`, `daq/bluesky/src/bluesky/callbacks/`
**Date:** 2026-06-14

---

## Status summary

| Priority | Count | Notes |
|----------|-------|-------|
| P0       | 3     | Wire/document correctness divergences that break interop |
| P1       | 11    | Meaningful completeness gaps |
| P2       | 7     | Nice-to-have |

Where cirrus already matches the reference, a one-line note appears under each heading.

---

## P0 — Correctness / Protocol Divergence

### CBEM-01 — KafkaDocumentSink and JsonlSink serialize the tagged `Document` enum wrapper, not the raw document dict

**cirrus:** `crates/cirrus-callbacks/src/kafka_sink.rs:93–97`, `crates/cirrus-callbacks/src/basic.rs:33`
**ref:** `daq/bluesky/src/bluesky/callbacks/zmq.py:120` (`self._serializer(doc)` where `doc` is the raw Python dict); NSLS-II bluesky-kafka envelope uses the raw dict as body.

**Gap:**
`KafkaDocumentSink::encode_body` uses `serde_json::to_vec(doc)` and `rmp_serde::to_vec_named(doc)` where `doc` is `&Document` — the tagged Rust enum. The `Document` enum carries `#[serde(tag = "name", content = "doc")]`, so the JSON body is:
```json
{"name":"stop","doc":{"uid":"…","exit_status":"success",…}}
```
instead of the raw document dict:
```json
{"uid":"…","exit_status":"success",…}
```
The msgpack variant wraps identically. Any downstream Python consumer using `msgpack.unpackb` or `json.loads` on the Kafka value will see the wrapper, not the expected bluesky document dict.

`JsonlSink::dispatch` has the same bug: `serde_json::to_string(doc)` on the full `Document` enum emits the `{"name":…,"doc":{…}}` wrapper, making JSONL files unreadable by Python `event_model` / `databroker`.

The **ZMQ sink** is correct: it per-arm matches `rmp_serde::to_vec_named(d)` on the inner variant `d`, not the wrapper.

**Fix sketch:** Apply the same per-arm match pattern used in `ZmqDocumentSink::encode_body` to `KafkaDocumentSink::encode_body` and to `JsonlSink::dispatch`. For Kafka JSON: replace `serde_json::to_vec(doc)` with a match that calls `serde_json::to_vec(inner)` on each `Document` variant. For JSONL: same match, then write the inner JSON. Effort: **S**.

---

### CBEM-02 — `EventPage.uid` is `String`; schema requires `Vec<String>`

**cirrus:** `crates/cirrus-event-model/src/documents.rs:229`
**ref:** `daq/event-model/src/event_model/documents/event_page.py:44` (`uid: list[str]`); `daq/event-model/src/event_model/__init__.py:2655` (`uid_list` in `pack_event_page`).

**Gap:** Cirrus defines `EventPage.uid: String` (single string). The event-model schema and `pack_event_page` both produce `uid` as a list, one per row in the page. A cirrus-produced `EventPage` is undeserializable by `unpack_event_page` (which iterates `event_page["uid"]`), and a Python-produced `EventPage` cannot be deserialized by cirrus. Round-trips through `ZmqDocumentSource` that receive Python `EventPage` documents will fail with a JSON type error.

**Fix sketch:** Change `pub uid: String` → `pub uid: Vec<String>` in `EventPage`. Update `RunBundle::event_page` (if/when it is added) to push a UID per row. Effort: **S** (type change; callers that build EventPage must update to push a UID list).

---

### CBEM-03 — `Resource.path_semantics` is required in cirrus; Python schema marks it `NotRequired`

**cirrus:** `crates/cirrus-event-model/src/documents.rs:257`
**ref:** `daq/event-model/src/event_model/documents/resource.py:41` (`path_semantics: NotRequired[Literal["posix","windows"]]`).

**Gap:** Cirrus defines `pub path_semantics: String` (always required, no `skip_serializing_if`). Any Python-side Resource document that omits `path_semantics` (which is the common case for newly written resources that don't set this field) will fail to deserialize in cirrus with a missing-field error. This breaks `ZmqDocumentSource` and `JsonlSink` round-trips.

**Fix sketch:** Change to `pub path_semantics: Option<String>` with `#[serde(skip_serializing_if = "Option::is_none", default)]`. Effort: **S**.

---

## P1 — Meaningful Completeness Gaps

### CBEM-04 — No `DocumentRouter` — no per-doc-type dispatch or event↔event_page shim

**cirrus:** `crates/cirrus-callbacks/src/` — only `DocumentSink` trait with `dispatch(&Document)`.
**ref:** `daq/event-model/src/event_model/__init__.py:109–308` (`DocumentRouter`, `_dispatch`, `start/stop/descriptor/event/event_page/…` methods, event↔page auto-shim).

**Gap:** The Python `DocumentRouter` provides: (1) a per-document-type method dispatch (`__call__` routes `name` to `self.name(doc)`); (2) automatic event↔event_page shim — if a subclass implements only `event`, it is called once per row when an `event_page` arrives, and vice versa; (3) an `emit` weakref chain for building processing pipelines. Cirrus has only `DocumentSink::dispatch(&Document)` — a flat async method. No method-per-type dispatch, no page↔event shim, no emit chain. This blocks implementing any pattern that mirrors Python `CallbackBase` / `BestEffortCallback` / `Filler` natively in Rust.

**Fix sketch:** Add a `DocumentRouter` trait (or a default-method-impl struct) with `fn start(…)`, `fn descriptor(…)`, `fn event(…)`, `fn event_page(…)`, etc., each defaulting to a no-op. A blanket `dispatch` implementation routes `&Document` to the right method. Add the event↔page shim after `pack_event_page` / `unpack_event_page` are added (CBEM-06). Effort: **M**.

---

### CBEM-05 — No `RunRouter` — no per-run factory-based callback routing

**cirrus:** absent.
**ref:** `daq/event-model/src/event_model/__init__.py:1405–1733` (`RunRouter`, factory dispatch, subfactory dispatch, per-run lifecycle).

**Gap:** The bluesky `RunRouter` is the primary callback composition primitive: it takes a list of factory functions `(name, start_doc) -> ([callbacks], [subfactories])` and maintains per-run state, routing each document to the correct set of callbacks. Without it, multi-run streams (e.g., from `ZmqDocumentSource`) cannot fan documents to per-run sinks (e.g., per-run Tiled containers, per-run JSONL files, per-run BestEffortCallback instances).

**Fix sketch:** Implement `RunRouter` as a struct holding `HashMap<run_uid, Vec<Box<dyn DocumentSink>>>` plus factory closures. `dispatch(&Document)` looks up the run UID, calls factories on `RunStart`, dispatches documents to the per-run sink set, and cleans up on `RunStop`. Start with a simplified factory-less variant first. Effort: **L**.

---

### CBEM-06 — No `pack_event_page` / `unpack_event_page` / datum equivalents

**cirrus:** absent.
**ref:** `daq/event-model/src/event_model/__init__.py:2620–2751` (`pack_event_page`, `unpack_event_page`, `pack_datum_page`, `unpack_datum_page`).

**Gap:** The transpose functions that convert between row-oriented `Event` documents and column-oriented `EventPage` documents are completely absent. They are needed by `DocumentRouter` (CBEM-04), by `Filler`, and for any processor that wants to batch-accumulate events into pages (for efficient wire transmission) or unpack pages into individual events for per-event processing.

**Fix sketch:** Implement free functions in `cirrus-event-model`:
- `pack_event_page(events: &[Event]) -> EventPage` — columns from row slice.
- `unpack_event_page(page: &EventPage) -> impl Iterator<Item = Event>`.
- Same for Datum/DatumPage. Effort: **S**.

---

### CBEM-07 — `RunBundle` missing `compose_resource` / `compose_datum` / `compose_datum_page`

**cirrus:** `crates/cirrus-event-model/src/compose.rs` — no Resource/Datum compose methods.
**ref:** `daq/event-model/src/event_model/__init__.py:2554–2617` (`ComposeRunBundle` has `compose_resource`, which returns a `ComposeResourceBundle` with `compose_datum` and `compose_datum_page`).

**Gap:** The old-style Resource/Datum document chain (used to describe externally-written HDF5 files, NeXus files, etc., addressed via spec + datum_kwargs) has no compose helpers in cirrus. Cirrus is stream-first (`StreamResource/StreamDatum`), but upstream code — including existing EPICS AD HDF5 file writers on Python — still emits Resource/Datum, and users may need to produce them. Without compose helpers there is no type-safe way to construct valid Resource/Datum UIDs with correct `datum_id` conventions.

**Fix sketch:** Add `RunBundle::resource(spec, root, resource_path, …) -> Resource` and a `DatumComposer` struct returned from it that holds the resource and a counter and provides `datum(datum_kwargs) -> Datum` / `datum_page(datum_kwargs_cols) -> DatumPage`. Effort: **M**.

---

### CBEM-08 — `RunStart` missing commonly-used optional fields: `data_session`, `data_groups`, `group`, `owner`, `project`, `projections`

**cirrus:** `crates/cirrus-event-model/src/documents.rs:19–36`
**ref:** `daq/event-model/src/event_model/documents/run_start.py:127–164` (`data_groups`, `data_session`, `group`, `owner`, `project`, `projections`).

**Gap:** Python `RunStart` has six widely-used optional fields for facility metadata and scan projection. In cirrus these collapse into `extra: HashMap<String, Value>` via `#[serde(flatten)]`, so documents round-trip but typed access is impossible. The `projections` field (type `Projections` with `LinkedEventProjection / StaticProjection / CalculatedEventProjection` variants) is entirely absent as a typed structure — callers cannot build projections without constructing raw `serde_json::Value`. Databroker and Tiled use `projections` for normalized access.

**Fix sketch:** Add the missing optional fields to `RunStart` as `pub data_session: Option<String>`, `pub data_groups: Option<Vec<String>>`, `pub group: Option<String>`, `pub owner: Option<String>`, `pub project: Option<String>`. Add typed `Projections` / `LinkedEventProjection` / `StaticProjection` / `CalculatedEventProjection` structs. All with `skip_serializing_if = "Option::is_none"` so existing callers are unaffected. Effort: **M**.

---

### CBEM-09 — `EventPage` missing `filled` column-store field

**cirrus:** `crates/cirrus-event-model/src/documents.rs:227–241`
**ref:** `daq/event-model/src/event_model/documents/event_page.py:17` (`filled: NotRequired[dict[str, list[bool | str]]]`).

**Gap:** The Python `EventPage.filled` is a column-store dict mapping external-reference keys to arrays of `bool | str`. `False` marks an unfilled external slot; `str` marks a foreign key after filling. Cirrus `EventPage` has no `filled` field at all. Any `EventPage` that arrives via `ZmqDocumentSource` from Python with unfilled external references cannot be round-tripped without data loss.

**Fix sketch:** Add `pub filled: HashMap<String, Vec<Value>>` (or `Vec<serde_json::Value>` to handle bool/str) with `#[serde(skip_serializing_if = "HashMap::is_empty", default)]`. Effort: **S**.

---

### CBEM-10 — `DataKey` missing `choices` field for enum/mbbo records

**cirrus:** `crates/cirrus-event-model/src/documents.rs:107–136`
**ref:** `daq/event-model/src/event_model/documents/event_descriptor.py:108` (`choices: NotRequired[list[str]]`).

**Gap:** EPICS mbbo, mbbi, and enum records expose a list of string choices that correspond to integer values. The `choices` field in `DataKey` is used by BestEffortCallback's `LiveTable` and by Tiled to render enum values correctly. Cirrus omits it entirely; any ophyd-epicsrs device that produces mbbo data will silently drop the choices.

**Fix sketch:** Add `pub choices: Option<Vec<String>>` with `skip_serializing_if = "Option::is_none"` to `DataKey`. Effort: **S**.

---

### CBEM-11 — `Limits` missing `rds` field (Tango RDS parameters)

**cirrus:** `crates/cirrus-event-model/src/documents.rs:87–104`
**ref:** `daq/event-model/src/event_model/documents/event_descriptor.py:52–99` (`rds: NotRequired[RdsRange | None]` with `time_difference`, `value_difference`).

**Gap:** The `rds` (read-different-than-set) field in `Limits` is used by Tango-connected ophyd-async devices to describe the acceptable drift between setpoint and readback. Without it, `Limits` documents from Tango sources cannot be fully round-tripped; the `rds` key will fall on the floor when deserializing.

**Fix sketch:** Add `pub struct RdsRange { pub time_difference: f64, pub value_difference: f64 }` and `pub rds: Option<RdsRange>` to `Limits`. Effort: **S**.

---

### CBEM-12 — `TiledSink` drops all non-Start/Stop documents

**cirrus:** `crates/cirrus-callbacks/src/tiled_sink.rs:133–159`
**ref:** `daq/bluesky/src/bluesky/callbacks/tiled_writer.py` (`TiledWriter` handles descriptor → table, stream_resource → array container, stream_datum → append, resource → asset registration).

**Gap:** `TiledSink::dispatch` explicitly drops `Descriptor`, `Event`, `EventPage`, `Resource`, `Datum`, `DatumPage`, `StreamResource`, and `StreamDatum` with a trace log. The only operations performed are registering a `BlueskyRun` container on `RunStart` and patching `stop` metadata on `RunStop`. No event data, no stream arrays, and no table entries reach Tiled. Users who point cirrus at a Tiled server via `TiledSink` receive a run container with start/stop metadata only — no usable data.

**Fix sketch:** Implement a `TiledFullSink` (or extend `TiledSink` behind a feature flag) that handles: (1) `Descriptor` → register table entries for each data_key; (2) `EventPage` → PATCH data to the table container; (3) `StreamResource` → register an array container; (4) `StreamDatum` → PATCH array data. Use `tiled-client::Context` for auth/CSRF as the existing minimal sink does. Effort: **L**.

---

### CBEM-13 — `StreamDatum.descriptor` is always empty string in `Hdf5FrameSink` and `BinaryFrameSink`

**cirrus:** `crates/cirrus-stream/src/sinks/hdf5.rs:263`, `crates/cirrus-stream/src/sinks/binary.rs:164`
**ref:** `daq/event-model/src/event_model/documents/stream_datum.py:11` (`descriptor: str` — UID of the EventDescriptor).

**Gap:** Both `Hdf5FrameSink::collect_stream_docs` and `BinaryFrameSink::collect_stream_docs` emit `StreamDatum` with `descriptor: String::new()` (empty string). The `StreamDatum.descriptor` field must be a valid EventDescriptor UID so that consumers can link stream data back to the descriptor that describes the data keys. Without it, Tiled and databroker cannot associate stream data with its schema. The `DetectorWriter::collect_stream_docs(up_to: u64)` interface has no way to receive the descriptor UID at call time.

**Fix sketch:** Extend the `DetectorWriter` trait (or `collect_stream_docs` signature) to accept a `descriptor_uid: &str` parameter. Alternatively, add a `set_descriptor_uid(&self, uid: &str)` method that the RunEngine calls after composing the descriptor, storing it in a `OnceLock<String>` on the sink. Both sinks use the stored UID when building `StreamDatum`. Effort: **M**.

---

### CBEM-14 — `Event.filled` restricted to `bool`; Python allows `bool | str`

**cirrus:** `crates/cirrus-event-model/src/documents.rs:221–224`
**ref:** `daq/event-model/src/event_model/documents/event.py:18` (`filled: NotRequired[dict[str, bool | str]]`).

**Gap:** Python `Event.filled` allows `str` values (a foreign key string placed in `filled` after the Filler resolves the external reference and moves the actual data into `data`). Cirrus uses `HashMap<String, bool>` which cannot represent the post-fill `str` foreign key. Any cirrus consumer of `ZmqDocumentSource` that receives filled events from a Python Filler will fail to deserialize the `str` values.

**Fix sketch:** Change to `pub filled: HashMap<String, Value>` (accepting both bool and string) with `skip_serializing_if = "HashMap::is_empty"`. Effort: **S**.

---

## P2 — Nice-to-Have

### CBEM-15 — No `Filler` (Resource+Datum external data resolver)

**cirrus:** absent.
**ref:** `daq/event-model/src/event_model/__init__.py:541–1158` (`Filler`, `fill_event`, `fill_event_page`, handler registry, root_map, retry intervals).

**Gap:** No equivalent to Python's `Filler` — the class that maps Resource→handler and Datum→data, substituting external references in Event documents with the actual loaded data. Required for consuming runs that reference externally-stored data (AD HDF5, SPEC, etc.) without Python.

**Fix sketch:** Implement a `Filler` struct with a `handler_registry: HashMap<String, Box<dyn ResourceHandler>>` trait-object map, `fill_event(Event) -> Event`, and `fill_event_page(EventPage) -> EventPage`. Effort: **L**.

---

### CBEM-16 — `RunStop.exit_status` is untyped `String`; should be an enum

**cirrus:** `crates/cirrus-event-model/src/documents.rs:50`
**ref:** `daq/event-model/src/event_model/documents/run_stop.py:24` (`exit_status: Literal["success", "abort", "fail"]`).

**Gap:** Cirrus accepts any string for `exit_status`, making it easy to produce invalid documents (e.g., `RunBundle::stop("failed", None)` — a common typo). Should be `pub enum ExitStatus { Success, Abort, Fail }` with `#[serde(rename_all = "lowercase")]`.

**Fix sketch:** Add `ExitStatus` enum. Update `RunBundle::stop` signature to take `ExitStatus`. Effort: **S**.

---

### CBEM-17 — `DataKey.dtype_numpy` cannot represent structured numpy dtypes

**cirrus:** `crates/cirrus-event-model/src/documents.rs:117`
**ref:** `daq/event-model/src/event_model/documents/event_descriptor.py:119` (`dtype_numpy: NotRequired[DtypeNumpy | list[DtypeNumpyItem]]` where `DtypeNumpyItem = tuple[str, str]`).

**Gap:** Cirrus `dtype_numpy: Option<String>` only handles scalar dtype strings like `"<f8"`. Structured numpy dtypes — used for compound detector records like `[("x", "<f4"), ("y", "<f4")]` — cannot be expressed. Any DataKey from a structured-array detector silently drops the structured dtype.

**Fix sketch:** Use `#[serde(untagged)] pub enum DtypeNumpy { Simple(String), Structured(Vec<(String, String)>) }` and change the field to `pub dtype_numpy: Option<DtypeNumpy>`. Effort: **S**.

---

### CBEM-18 — `Hints.dimensions` cannot represent mixed `str | list[str]` inner type

**cirrus:** `crates/cirrus-event-model/src/documents.rs:14`
**ref:** `daq/event-model/src/event_model/documents/run_start.py:17` (`dimensions: NotRequired[list[list[list[str] | str]]]`).

**Gap:** Python `Hints.dimensions` inner elements can be either `list[str]` or a bare `str` (e.g., `[["x", "y"], "time"]`). Cirrus `Vec<Vec<Vec<String>>>` requires three levels of nesting — the bare `str` alternative is unrepresentable. In practice bluesky only produces triple-nested lists, so this is unlikely to cause failures today.

**Fix sketch:** Change the inner type to `Vec<Vec<serde_json::Value>>` (accepting either a `String` or a JSON array of strings) or introduce a `DimensionEntry` untagged enum. Effort: **S**.

---

### CBEM-19 — No ZMQ `Proxy` (forwarder) implementation

**cirrus:** absent.
**ref:** `daq/bluesky/src/bluesky/callbacks/zmq.py:130–292` (`Proxy`, `zmq.FORWARDER` device).

**Gap:** Without a Proxy, a cirrus process that binds a PUB socket is not composable with Python `RemoteDispatcher` that expects to connect to a SUB-side proxy. Users must run a separate Python `bluesky-zmq-proxy` process or connect directly (which works when only one publisher and one subscriber are used but fails for fanout). `ZmqDocumentSink::connect` can connect to a Python proxy, but no Rust-side proxy is available.

**Fix sketch:** Add a `ZmqProxy::new(in_addr, out_addr)` / `ZmqProxy::start()` that spawns a thread running `zmq::device(FORWARDER, frontend, backend)`. Effort: **S**.

---

### CBEM-20 — No `BestEffortCallback` / `LiveTable` equivalent in Rust

**cirrus:** absent.
**ref:** `daq/bluesky/src/bluesky/callbacks/best_effort.py`, `daq/bluesky/src/bluesky/callbacks/core.py` (`LiveTable`, `CallbackBase`, `make_class_safe`).

**Gap:** No native Rust streaming text table, peak stats printer, or live-plot hook. Users must relay through `ZmqDocumentSink` → Python `RemoteDispatcher` → `BestEffortCallback`. This is the documented relay path (zmq_sink.rs doc comment), but it requires Python running alongside every cirrus process.

**Fix sketch:** Implement `LiveTable` as a `DocumentRouter` (after CBEM-04) that, on `descriptor`, records the hinted column names, and on each `event`, appends a text row to stdout. Peak stats requires post-`stop` analysis. Effort: **L** for full BEC parity; **M** for `LiveTable` text only.

---

### CBEM-21 — `RunBundle::descriptor` silently returns stale UID on re-compose for existing stream

**cirrus:** `crates/cirrus-event-model/src/compose.rs:83–90`
**ref:** `daq/event-model/src/event_model/__init__.py` (Python compose raises `InvalidData` / `MismatchedDataKeys` on key mismatch).

**Gap:** `RunBundle::descriptor` uses `entry.or_insert_with(...)` — if a stream already has a descriptor, the method returns the new (different-shaped) descriptor doc with `is_new = true` but the run-internal stream state still maps the stream name to the **old** descriptor UID. Subsequent `RunBundle::event` calls for that stream will carry the old descriptor UID even if the caller intended to reopen with new data keys. Python raises `MismatchedDataKeys` in this scenario.

**Fix sketch:** On the `entry.or_insert_with` path, check whether the incoming `data_keys` differ from those in the existing `StreamState`; if they do, return an error rather than silently returning the old UID. Requires `StreamState` to store `data_keys` for comparison. Effort: **S**.

---

## Already matching — no gap

- **ZMQ envelope wire format**: `<prefix> <name> <body>` with `b" ".join(...)` semantics — cirrus `build_envelope` and Python `Publisher.__call__` are byte-identical including the leading space when prefix is empty.
- **ZMQ source decode**: `decode_envelope` splits on first two spaces and dispatches by name — matches `RemoteDispatcher._poll`.
- **DocumentNames set**: cirrus `document_name()` covers all ten live document kinds; deprecated `bulk_datum`/`bulk_events` are correctly omitted.
- **StreamResource / StreamDatum struct fields**: both match the Python schema exactly (uid, data_key, mimetype, uri, parameters, run_start; uid, stream_resource, descriptor, indices, seq_nums).
- **`RunBundle::start` / `RunBundle::stop`**: basic UID and timestamp generation match the Python `compose_run` / `ComposeStop` semantics. `num_events` values are seq_num counts after N events, which equal N — consistent with Python.
- **`DataKey` core fields**: `source`, `dtype`, `shape`, `dtype_numpy` (simple string), `external`, `units`, `precision`, `object_name`, `dims`, `limits` — all present and correctly typed.
- **`Dtype` enum**: all five variants (`string`, `number`, `array`, `boolean`, `integer`) match `Literal` values in Python.
- **Kafka key**: `document_name(doc).as_bytes()` — matches the expected doc-kind key used by NSLS-II ingestion services.
- **`HDF5FrameSink`**: layout `/entry/instrument/<name>/data`, mimetype `application/x-hdf5`, NXroot attribute, chunked extensible u8 dataset — matches ophyd-async `HDFWriter` conventions.
- **`BinaryFrameSink`**: CIRBIN1 magic, u32-LE length-prefixed frames, mimetype `application/x-cirbin1` — a cirrus-specific format with no Python ref, no parity issue.
- **`PvaMonitorSource` / `FramePipe`**: no Python equivalent; cirrus-native.
