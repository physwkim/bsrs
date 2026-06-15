# 02 — Event Model

## Single source of truth: the JSON schemas

`/Users/stevek/codes/daq/event-model/src/event_model/schemas/` ships 12 JSON schemas.
The Python `TypedDict`s in `event_model/documents/*.py` are auto-generated from these
schemas via `datamodel-codegen`. **The schemas are authoritative. The Python types are
a derived artifact.**

bsrs follows the same pattern: it does not hand-write Document types. It generates
Rust `serde::{Serialize, Deserialize}` types from the same JSON schemas via the
`typify` crate.

```text
bsrs-event-model/
├── build.rs                 # typify::Generator → src/generated.rs
├── schemas/                 # copy or git submodule of event-model schemas
│   ├── run_start.json
│   ├── run_stop.json
│   ├── event_descriptor.json
│   ├── event.json
│   ├── event_page.json
│   ├── resource.json
│   ├── datum.json
│   ├── datum_page.json
│   ├── stream_resource.json
│   └── stream_datum.json
└── src/
    ├── lib.rs               # re-exports + Document enum
    ├── generated.rs         # typify output — never hand-edited
    ├── compose.rs           # ComposeRunBundle equivalent
    └── router.rs            # DocumentRouter / SingleRunDocumentRouter
```

When the upstream schema gains a field, `cargo build` pulls it in automatically. Zero
hand editing.

## The `Document` enum

The single fan-out type that the RunEngine broadcasts to every callback / sink:

```rust
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "name", content = "doc", rename_all = "snake_case")]
pub enum Document {
    Start(RunStart),
    Descriptor(EventDescriptor),
    Event(Event),
    EventPage(EventPage),
    Resource(Resource),
    Datum(Datum),
    DatumPage(DatumPage),
    StreamResource(StreamResource),
    StreamDatum(StreamDatum),
    Stop(RunStop),
}
```

The `tag = "name"` discriminator matches `event_model.DocumentNames` (`__init__.py:94`).

## What the schema fields actually carry

The non-obvious ones, distilled from the schemas:

### `RunStart` (`run_start.json`)

Identification fields (`uid`, `time`, `scan_id`), data-management grouping
(`data_groups`, `data_session`, `group`, `owner`, `project`, `sample`), visualization
hints (`hints.dimensions`), and **projections** — a versioned spec for "how to interpret
this run" (`Projections`, with `ConfigurationProjection` / `LinkedEventProjection` /
`StaticProjection` / `CalculatedEventProjection`). Projections matter for downstream
analysis tools (Tiled views).

### `EventDescriptor` (`event_descriptor.json`)

Maps stream name → `data_keys` (a dict of `DataKey`). Each `DataKey` carries
`source` / `dtype` / `dtype_numpy` / `shape` / `dims` / `units` / `precision` and an
optional `external` field plus EPICS-style `Limits` (alarm / control / display /
hysteresis / RDS). The `external: "STREAM:"` flag is what tells consumers a key resolves
through `StreamResource` / `StreamDatum` instead of `Event.data`.

### `StreamResource` (`stream_resource.json`)

Six fields: `uid`, `data_key`, `mimetype`, `uri`, `parameters`, `run_start`. The recent
schema renamed `spec` → `mimetype` and `resource_path` → `uri`. bsrs uses the new
names; older bluesky callbacks may need a translation layer.

### `StreamDatum` (`stream_datum.json`)

`indices: StreamRange{start, stop}` describes the slice of file data; `seq_nums`
describes the slice of Event sequence numbers. The relation `indices.stop ==
seq_nums.stop` if `multiplier == 1`, otherwise `indices.stop = seq_nums.stop * multiplier`.

## Compose helpers

The Python `compose_*` family in `event_model/__init__.py` takes care of UID generation,
descriptor caching (so identical `data_keys` reuse the same `descriptor.uid`), and
sequence-number bookkeeping. bsrs mirrors them in `compose.rs`:

| Python | Rust (bsrs) |
|---|---|
| `ComposeRunBundle` (`__init__.py:2528`) | `compose::run` returning `RunBundle { start, descriptor, event, resource, datum, stream_resource, stream_datum, stop }` |
| `compose_event` (`:2393`) | `RunBundle::event(name, data, timestamps)` |
| `compose_resource` (`:1977`) | `RunBundle::resource(...)` |
| `ComposeStreamResourceBundle` (`:2059`) | `RunBundle::stream_resource(...)` |
| `ComposeStreamDatum` (`:2003`) | `RunBundle::stream_datum(indices, seq_nums)` |

Each closure captures the parent UID so call sites cannot cross-link incorrectly.

## Document routing

Python's `DocumentRouter` and `SingleRunDocumentRouter` (`__init__.py:311-447`) become
a Rust trait + dispatcher:

```rust
#[async_trait]
pub trait DocumentSink: Send + Sync {
    async fn dispatch(&self, doc: &Document) -> Result<()>;
}
```

The RunEngine fan-out uses `tokio::sync::broadcast::Sender<Document>`. **Lagged drops
are exposed via an atomic counter** (rule K6) — never silently lost.

## Round-trip test (M0 acceptance)

The first acceptance test for `bsrs-event-model`:

1. Run a Python event-model session that emits all 10 document types.
2. Capture the JSONL output.
3. bsrs deserializes the same JSONL via `serde_json` into `Vec<Document>`.
4. Re-serializes back to JSONL.
5. Diff is empty.

This is the contract: bsrs never invents a field, never reorders, never coerces.
