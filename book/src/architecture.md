# Architecture

cirrus is a Cargo workspace. Each crate has a single responsibility;
boundaries are designed so a downstream user can swap one
implementation without touching the others.

## Crate map

```text
cirrus                    # umbrella re-exports + binary entry points
├── cirrus-cli            # binary: qs-manager, qs, repl, doctor, migrate, frame-source
├── cirrus-engine         # RunEngine, Msg, state machine, suspenders, preprocessors
├── cirrus-plans          # bp.* / bps.* / bpp.* mirrors (count, scan, grid_scan, ...)
├── cirrus-protocols      # Movable, Triggerable, Stageable, Readable (sync facade)
├── cirrus-protocols-async # ophyd-async-style traits over async fns
├── cirrus-derive         # #[derive(Device)], #[signal(...)], #[lua_methods] proc-macros
├── cirrus-devices        # SoftMotor, SoftDetector, NDSimDetector, ...
├── cirrus-backend-epics-ca   # SignalBackend over CA  (feature: real)
├── cirrus-backend-epics-pva  # SignalBackend over PVA (feature: real)
├── cirrus-callbacks      # Document sinks: jsonl, zmq, tiled, kafka
├── cirrus-stream         # FrameSink/FrameSource: hdf5, binary, pva
└── cirrus-qs             # bluesky-queueserver-compatible daemon
```

Each crate's docs lives at `doc/0N-<topic>.md` in the repo. This
chapter is a high-level orientation; for protocol contracts go to
the doc/ tree.

## The RunEngine

`cirrus_engine::RunEngine` owns the dispatch loop. A plan is an
`async_stream::Stream<Item = Msg>`; the engine drives it forward,
matches each `Msg`, and dispatches to the right handler.

```rust
pub enum Msg {
    OpenRun { md: BTreeMap<String, Value> },
    CloseRun { exit_status: ExitStatus },
    Create { name: String },
    Read(DeviceRef),
    Save,
    Set { device: DeviceRef, target: Value, group: Option<String> },
    Wait { group: String },
    Trigger { device: DeviceRef, group: Option<String> },
    Pause { defer: bool, hard: bool },
    Checkpoint,
    Stage(DeviceRef),
    Unstage(DeviceRef),
    // ...
    Custom { name: String, payload: Value },
    Fail(String),                 // plan-internal abort with reason
}
```

Engine state is an `AtomicState`: `Idle`, `Running`, `Pausing`,
`Paused`, `Aborting`, `Halting`, `Halted`. State transitions go
through one owner (the dispatch loop) and are visible to consumers
via a `tokio::sync::watch` channel.

## The Document plane

Every `Msg::Save` (and a few other handlers) emits one or more
Documents. Documents are typed:

```rust
pub enum Document {
    Start(RunStart),
    Descriptor(EventDescriptor),
    Event(Event),
    EventPage(EventPage),
    Stop(RunStop),
    Resource(Resource),
    Datum(Datum),
    DatumPage(DatumPage),
    StreamResource(StreamResource),
    StreamDatum(StreamDatum),
}
```

Each Document is fanned out through a `DocumentRouter` to all
attached subscribers. Subscribers are anything implementing
`async fn handle(&self, name: &str, body: Value)`:

- `JsonlSink` — append line-delimited JSON to a file
- `ZmqDocumentSink` — `<prefix> <name> <body>` envelope on a PUB socket
- `TiledSink` — register Resources via tiled-client HTTP
- `KafkaDocumentSink` — produce to a Kafka topic
- `Hdf5FrameSink` — turn `Datum` (frame_uid → bytes) into a NeXus group

The router is **not** the place to put business logic. Its job is
fan-out; if a subscriber is slow it doesn't block other subscribers
because each handler runs in its own task.

## The frame plane

Frame bytes never travel through the Document plane. A camera fills
buffers via `FrameSource`; those buffers go through a `FramePipe`
(a bounded channel) to one or more `FrameSink`s. The Document plane
sees only `StreamResource` (one per channel) + `StreamDatum` (one
per chunk of frames written), with file paths or shape descriptors
that downstream readers use to fetch the bytes.

This is what makes cirrus's "RunEngine on the IOC host" deployment
shape work: frame bytes stay local to the IOC host, only Documents
cross the network.

## Multi-process: D21

For sites where the camera produces faster than a single process
can write, the `cirrus frame-source` subcommand splits the picture:

```text
[ frame-source ]  --pva--> camera
                  ↓ writes frames locally to HDF5
                  ↓ publishes only StreamResource/StreamDatum docs
                  ↓ via ZMQ PUB
[ qs-manager ] ←--ZMQ----- subscribes via ZmqDocumentSource
                  ↓ rebroadcasts on its own document PUB
                  ↓ to downstream consumers
```

Each process can run on a different host. The wire format is
stable: anything that consumes bluesky 0MQ documents already
consumes the rebroadcast.

## Backend traits

The single most important interface for new device implementers:

```rust
pub trait SignalBackend: Send + Sync + 'static {
    type Datatype: Serialize + DeserializeOwned + Clone + Send + Sync;

    async fn get(&self) -> Result<Self::Datatype, BackendError>;
    async fn put(&self, value: Self::Datatype) -> Result<(), BackendError>;
    async fn subscribe(&self) -> Result<SubscriptionHandle<Self::Datatype>, BackendError>;
    // ...
}
```

`epics-ca-rs` and `epics-pva-rs` are concrete impls. A
hand-written backend (REST, custom binary, simulated) is roughly
~100 lines and slots into existing devices via the `B` generic
parameter.

## See also

- `doc/01-overview.md` — high-level diagram
- `doc/02-events.md` — Document shape contract
- `doc/03-runengine.md` — bluesky parity matrix
- `doc/04-devices.md` — device authoring guide
- `doc/05-backends.md` — backend trait set
- `doc/06-callbacks.md` — sink authoring guide
- `doc/07-frames.md` — frame plane in detail
- `doc/08-qs.md` — queueserver wire compat
- `doc/09-d21.md` — multi-process design
- `doc/10-roadmap.md` — what's next
