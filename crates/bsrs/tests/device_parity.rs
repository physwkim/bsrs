//! Device tree, naming, Kind, hints, readable/configurable, staging, protocol
//! and connect behaviour checked against ophyd / ophyd-async device tests.
//! IOC-backed connect tests live with the EPICS suites.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bsrs::backends::soft::{SoftDetector, SoftMotor, SoftSignalBackend};
use bsrs::core::msg::{ConfigurableObj, NamedObj, ReadableObj, StageableObj};
use bsrs::core::{
    BsrsError, ConfigureArgs, DataKey, Dtype, Kind, ReadingValue, Result, Status, StatusOutcome,
    SubToken, WaitError,
};
use bsrs::devices::{
    BackendFromPv, Device, DeviceVector, Signal, SignalConfig, SignalR, SignalRW, SignalX,
    StandardReadable, StandardReadableFormat,
};
use bsrs::protocols_async::{
    AsyncConfigurable, AsyncMovable, AsyncReadable, AsyncSubscribable, Locatable, PutCompletion,
    ReadingValueCallback, SignalBackend, Stageable, Stoppable, Triggerable,
};
use bsrs::protocols_sync::{Configurable, Movable, Readable, StageableSync};

type Soft = SoftSignalBackend<f64>;

fn soft_signal(name: &str, kind: Kind, v: f64) -> (Arc<SignalR<f64, Soft>>, Arc<Soft>) {
    let backend = Arc::new(Soft::new(v, Dtype::Number));
    let sig = Arc::new(Signal::new(
        backend.clone(),
        SignalConfig {
            source: name.into(),
            kind,
            name: name.into(),
        },
    ));
    (sig, backend)
}

fn sig(name: &str, kind: Kind) -> Arc<SignalR<f64, Soft>> {
    soft_signal(name, kind, 0.0).0
}

fn keys<V>(m: &HashMap<String, V>) -> BTreeSet<String> {
    m.keys().cloned().collect()
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

// ophyd test_kind::test_standalone_signals, test_hints::test_device_hints
// (signal part), test_bluesky_protocols::test_hinted.
#[tokio::test]
async fn signal_reads_under_its_own_name_and_hints_only_when_hinted() {
    for kind in [Kind::Normal, Kind::Config, Kind::Hinted, Kind::Omitted] {
        let name = format!("sig_{kind:?}");
        let (s, _) = soft_signal(&name, kind, 1.0);
        assert_eq!(s.kind(), kind);
        assert_eq!(keys(&s.read().await.unwrap()), set(&[&name]));
        assert_eq!(keys(&s.describe().await.unwrap()), set(&[&name]));
        let expected = (kind == Kind::Hinted).then(|| vec![name.clone()]);
        assert_eq!(ReadableObj::hint_fields(&*s), expected, "{kind:?}");
    }
}

// ophyd test_kind::test_nested_devices routing: normal → event, config →
// configuration, hinted → event + hints, omitted → nowhere; components
// default to normal.
#[test]
fn kind_predicates_route_like_ophyd() {
    let table = [
        (Kind::Normal, true, false, false),
        (Kind::Config, false, true, false),
        (Kind::Hinted, true, false, true),
        (Kind::Omitted, false, false, false),
    ];
    for (kind, event, config, hinted) in table {
        assert_eq!(kind.in_event_data(), event, "{kind:?} event");
        assert_eq!(kind.in_configuration(), config, "{kind:?} config");
        assert_eq!(kind.is_hinted(), hinted, "{kind:?} hinted");
    }
    assert_eq!(Kind::default(), Kind::Normal);
}

#[derive(Device)]
struct KindProbe {
    name: String,
    #[signal(rw, "{prefix}:normal", kind = normal)]
    normal: SignalRW<f64, Soft>,
    #[signal(ro, "{prefix}:hinted", kind = hinted)]
    hinted: SignalR<f64, Soft>,
    #[signal(rw, "{prefix}:config", kind = config)]
    config: SignalRW<f64, Soft>,
    #[signal(ro, "{prefix}:omitted", kind = omitted)]
    omitted: SignalR<f64, Soft>,
    #[signal(rw, "{prefix}:plain")]
    plain: SignalRW<f64, Soft>,
    #[signal(rw, "{prefix}:kind", kind = config)]
    kind_in_template: SignalRW<f64, Soft>,
    #[signal(ro, "{prefix}:capital", kind = Hinted)]
    capitalised: SignalR<f64, Soft>,
}

// ophyd test_kind::test_strings: a kind given by name (case-insensitive,
// `Kind[val.lower()]`) sets the component kind.
#[test]
fn derive_kind_attribute_sets_signal_kind() {
    let d = KindProbe::new("P");
    assert_eq!(d.normal.kind(), Kind::Normal);
    assert_eq!(d.hinted.kind(), Kind::Hinted);
    assert_eq!(d.config.kind(), Kind::Config);
    assert_eq!(d.omitted.kind(), Kind::Omitted);
    assert_eq!(d.plain.kind(), Kind::Normal);
    assert_eq!(d.kind_in_template.kind(), Kind::Config);
    assert_eq!(d.capitalised.kind(), Kind::Hinted);
}

fn leaf(prefix: &str) -> StandardReadable {
    let mut sr = StandardReadable::new(prefix);
    sr.add_signal(
        sig(&format!("{prefix}-value"), Kind::Normal),
        StandardReadableFormat::HintedSignal,
    )
    .unwrap();
    sr.add_signal(
        sig(&format!("{prefix}-config"), Kind::Config),
        StandardReadableFormat::ConfigSignal,
    )
    .unwrap();
    sr
}

// ophyd test_kind::test_nested_devices ("a_default comes along for the
// ride"), test_device::test_complexdevice; ophyd-async test_readable
// add_readables_adds_to_expected_attrs, add_children_multi_nested; epics/tango
// declarative subdevices contributing to the readable verbs and nesting.
#[tokio::test]
async fn standard_readable_child_merges_nested_read_configuration_and_hints() {
    let mut parent = StandardReadable::new("parent");
    parent.add_child(Arc::new(leaf("parent-sub"))).unwrap();
    assert_eq!(
        keys(&parent.read().await.unwrap()),
        set(&["parent-sub-value"])
    );
    assert_eq!(
        keys(&parent.describe().await.unwrap()),
        set(&["parent-sub-value"])
    );
    assert_eq!(
        keys(&parent.read_configuration().await.unwrap()),
        set(&["parent-sub-config"])
    );
    assert_eq!(
        keys(&parent.describe_configuration().await.unwrap()),
        set(&["parent-sub-config"])
    );
    assert_eq!(
        ReadableObj::hint_fields(&parent),
        Some(vec!["parent-sub-value".to_string()])
    );

    let mut mid = StandardReadable::new("top-mid");
    mid.add_child(Arc::new(leaf("top-mid-leaf"))).unwrap();
    let mut top = StandardReadable::new("top");
    top.add_child(Arc::new(mid)).unwrap();
    assert_eq!(
        keys(&top.read().await.unwrap()),
        set(&["top-mid-leaf-value"])
    );
    assert_eq!(
        keys(&top.read_configuration().await.unwrap()),
        set(&["top-mid-leaf-config"])
    );
}

// ophyd-async test_readable::test_standard_readable_hints; ophyd
// test_hints::test_device_hints, test_motor_bundle_hints.
#[test]
fn standard_readable_hints_concatenate_in_registration_order() {
    assert_eq!(
        ReadableObj::hint_fields(&StandardReadable::new("empty")),
        None
    );

    let mut first = StandardReadable::new("first");
    first
        .add_signal(
            sig("abc", Kind::Normal),
            StandardReadableFormat::HintedSignal,
        )
        .unwrap();
    let mut second = StandardReadable::new("second");
    second.add_child(sig("def", Kind::Hinted)).unwrap();
    second
        .add_signal(
            sig("ghi", Kind::Normal),
            StandardReadableFormat::HintedSignal,
        )
        .unwrap();

    let mut sr = StandardReadable::new("sr");
    sr.add_child(Arc::new(first)).unwrap();
    sr.add_child(Arc::new(second)).unwrap();
    sr.add_child(sig("jkl", Kind::Hinted)).unwrap();
    sr.add_child(sig("quiet", Kind::Normal)).unwrap();
    assert_eq!(
        ReadableObj::hint_fields(&sr),
        Some(vec![
            "abc".to_string(),
            "def".to_string(),
            "ghi".to_string(),
            "jkl".to_string()
        ])
    );
}

// ophyd-async test_readable::test_standard_readable_hints_raises_when_
// overriding_sequence: a hint field registered twice is an error, whether it
// comes from a CHILD or a Hinted* signal; an unnamed signal hints nothing.
#[tokio::test]
async fn standard_readable_rejects_a_hint_that_overrides_an_existing_one() {
    let mut inner = StandardReadable::new("inner");
    for name in ["f1", "f2"] {
        inner
            .add_signal(
                sig(name, Kind::Normal),
                StandardReadableFormat::HintedSignal,
            )
            .unwrap();
    }
    let mut sr = StandardReadable::new("sr");
    sr.add_child(sig("f2", Kind::Hinted)).unwrap();
    let overrides = "Hint fields f2 overrides existing hint";
    assert!(err_text(sr.add_child(Arc::new(inner))).contains(overrides));
    let hinted_uncached = StandardReadableFormat::HintedUncachedSignal;
    assert!(err_text(sr.add_signal(sig("f2", Kind::Normal), hinted_uncached)).contains(overrides));
    assert_eq!(ReadableObj::hint_fields(&sr), Some(vec!["f2".to_string()]));
    assert_eq!(keys(&sr.read().await.unwrap()), set(&["f2"]));

    let mut unnamed = StandardReadable::new("unnamed");
    for _ in 0..2 {
        unnamed
            .add_signal(sig("", Kind::Normal), StandardReadableFormat::HintedSignal)
            .unwrap();
    }
    assert_eq!(ReadableObj::hint_fields(&unnamed), None);
}

// ophyd-async test_readable add_readables_adds_to_expected_attrs and
// test_standard_readable_{uncached,hinted_uncached}_signal; ophyd
// test_device::test_attrs (describe keys track read keys, and likewise for
// configuration).
#[tokio::test]
async fn standard_readable_uncached_formats_route_to_read() {
    let mut sr = StandardReadable::new("sr");
    sr.add_signal(
        sig("u", Kind::Normal),
        StandardReadableFormat::UncachedSignal,
    )
    .unwrap();
    sr.add_signal(
        sig("hu", Kind::Normal),
        StandardReadableFormat::HintedUncachedSignal,
    )
    .unwrap();
    sr.add_signal(sig("c", Kind::Config), StandardReadableFormat::ConfigSignal)
        .unwrap();

    let read = keys(&sr.read().await.unwrap());
    assert_eq!(read, set(&["u", "hu"]));
    assert_eq!(keys(&sr.describe().await.unwrap()), read);
    let config = keys(&sr.read_configuration().await.unwrap());
    assert_eq!(config, set(&["c"]));
    assert_eq!(keys(&sr.describe_configuration().await.unwrap()), config);
    assert_eq!(ReadableObj::hint_fields(&sr), Some(vec!["hu".to_string()]));
}

/// Delivers its value once on subscribe and never again, so a staged
/// signal's cache keeps the old value while a backend read sees the new one.
struct SilentMonitor {
    value: Mutex<f64>,
}

#[async_trait::async_trait]
impl SignalBackend<f64> for SilentMonitor {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        Ok(())
    }
    async fn put_start(&self, _value: Option<f64>) -> Result<PutCompletion> {
        Err(BsrsError::Backend("read-only".into()))
    }
    async fn get_datakey(&self, _source: &str) -> Result<DataKey> {
        Err(BsrsError::Backend("no datakey".into()))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        Ok(ReadingValue::new(
            serde_json::json!(*self.value.lock().unwrap()),
            0.0,
        ))
    }
    async fn get_value(&self) -> Result<f64> {
        Ok(*self.value.lock().unwrap())
    }
    async fn get_setpoint(&self) -> Result<f64> {
        self.get_value().await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<f64>>) -> SubToken {
        if let Some(cb) = cb {
            cb(&self.value.lock().unwrap(), 0.0, None);
        }
        SubToken::noop()
    }
    fn source(&self, name: &str, _read: bool) -> String {
        format!("silent://{name}")
    }
}

const SLOW: Duration = Duration::from_millis(100);

/// Every verb takes `SLOW`, so two children take `SLOW` when gathered and
/// twice that when awaited one after the other.
struct SlowChild(String);

impl NamedObj for SlowChild {
    fn name(&self) -> &str {
        &self.0
    }
}

#[async_trait::async_trait]
impl ReadableObj for SlowChild {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        tokio::time::sleep(SLOW).await;
        Ok(HashMap::new())
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        tokio::time::sleep(SLOW).await;
        Ok(HashMap::new())
    }
    fn as_stageable(self: Arc<Self>) -> Option<Arc<dyn StageableObj>> {
        Some(self)
    }
    fn as_configurable(&self) -> Option<&dyn ConfigurableObj> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl ConfigurableObj for SlowChild {
    async fn read_configuration_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        tokio::time::sleep(SLOW).await;
        Ok(HashMap::new())
    }
    async fn describe_configuration_dyn(&self) -> Result<HashMap<String, DataKey>> {
        tokio::time::sleep(SLOW).await;
        Ok(HashMap::new())
    }
    async fn configure_dyn(&self, _args: ConfigureArgs) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl StageableObj for SlowChild {
    async fn stage_dyn(&self) -> Result<()> {
        tokio::time::sleep(SLOW).await;
        Ok(())
    }
    async fn unstage_dyn(&self) -> Result<()> {
        tokio::time::sleep(SLOW).await;
        Ok(())
    }
}

// ophyd-async merge_gathered_dicts and _StandardBase.stage/unstage gather the
// children (asyncio.gather) rather than awaiting them in turn.
#[tokio::test(start_paused = true)]
async fn standard_readable_gathers_children_concurrently() {
    let mut sr = StandardReadable::new("sr");
    for name in ["s1", "s2"] {
        sr.add_child(Arc::new(SlowChild(name.into()))).unwrap();
    }
    macro_rules! elapsed {
        ($verb:expr) => {{
            let t = tokio::time::Instant::now();
            $verb.await.unwrap();
            t.elapsed()
        }};
    }
    assert_eq!(elapsed!(sr.read()), SLOW, "read");
    assert_eq!(elapsed!(sr.describe()), SLOW, "describe");
    assert_eq!(
        elapsed!(sr.read_configuration()),
        SLOW,
        "read_configuration"
    );
    assert_eq!(
        elapsed!(sr.describe_configuration()),
        SLOW,
        "describe_configuration"
    );
    assert_eq!(elapsed!(Stageable::stage(&sr)), SLOW, "stage");
    assert_eq!(elapsed!(Stageable::unstage(&sr)), SLOW, "unstage");
}

// ophyd-async test_readable::test_standard_readable_{uncached,
// hinted_uncached}_signal: the Uncached formats read with cached=False, the
// others take the staged cache.
#[tokio::test]
async fn standard_readable_uncached_formats_bypass_the_signal_cache() {
    let mut sr = StandardReadable::new("sr");
    let mut backends = Vec::new();
    for (name, format) in [
        ("cached", StandardReadableFormat::HintedSignal),
        ("u", StandardReadableFormat::UncachedSignal),
        ("hu", StandardReadableFormat::HintedUncachedSignal),
    ] {
        let backend = Arc::new(SilentMonitor {
            value: Mutex::new(1.0),
        });
        let s: Arc<SignalR<f64, SilentMonitor>> = Arc::new(Signal::new(
            backend.clone(),
            SignalConfig {
                source: name.into(),
                kind: Kind::Normal,
                name: name.into(),
            },
        ));
        Stageable::stage(&*s).await.unwrap();
        sr.add_signal(s, format).unwrap();
        backends.push(backend);
    }
    for backend in &backends {
        *backend.value.lock().unwrap() = 2.0;
    }
    let read = sr.read().await.unwrap();
    let value = |k: &str| read[k].value.clone();
    assert_eq!(value("cached"), serde_json::json!(1.0));
    assert_eq!(value("u"), serde_json::json!(2.0));
    assert_eq!(value("hu"), serde_json::json!(2.0));
}

fn err_text<T>(r: Result<T>) -> String {
    match r {
        Ok(_) => panic!("expected an error"),
        Err(e) => e.to_string(),
    }
}

// ophyd-async test_utils merge_gathered_dicts duplicate / blank-key tests,
// test_readable::test_duplicate_readable_raises_exception.
#[tokio::test]
async fn standard_readable_rejects_duplicate_reading_keys() {
    let mut sr = StandardReadable::new("sr");
    for name in ["a", "a", "b", "c", "c"] {
        sr.add_child(sig(name, Kind::Normal)).unwrap();
    }
    for name in ["x", "x"] {
        sr.add_signal(
            sig(name, Kind::Config),
            StandardReadableFormat::ConfigSignal,
        )
        .unwrap();
    }
    let dup = "Duplicate keys found while merging dictionaries: a, c";
    assert!(err_text(sr.read().await).contains(dup));
    assert!(err_text(sr.describe().await).contains(dup));
    let dup_x = "Duplicate keys found while merging dictionaries: x";
    assert!(err_text(sr.read_configuration().await).contains(dup_x));
    assert!(err_text(sr.describe_configuration().await).contains(dup_x));

    let mut unnamed = StandardReadable::new("");
    unnamed.add_child(sig("", Kind::Normal)).unwrap();
    unnamed.add_child(sig("", Kind::Normal)).unwrap();
    assert!(err_text(unnamed.read().await).contains("Have you remembered to name the Device?"));
}

// ophyd-async test_readable add_readables_adds_to_expected_attrs and
// test_standard_readable_{hinted,config,uncached,hinted_uncached}_signal
// (stage buckets: a stageable CHILD and HINTED_SIGNAL stage, the other
// signal formats do not); ophyd test_device::test_device_state (stage,
// configure({}), unstage on a device with no staged children).
#[tokio::test]
async fn standard_readable_stages_children_and_hinted_signals_only() {
    let (a, a_backend) = soft_signal("a", Kind::Normal, 1.0);
    let (b, b_backend) = soft_signal("b", Kind::Normal, 2.0);
    let (c, c_backend) = soft_signal("c", Kind::Config, 3.0);
    let (u, u_backend) = soft_signal("u", Kind::Normal, 4.0);
    let (hu, hu_backend) = soft_signal("hu", Kind::Normal, 5.0);
    let mut inner = StandardReadable::new("inner");
    inner
        .add_signal(b, StandardReadableFormat::HintedSignal)
        .unwrap();
    let mut outer = StandardReadable::new("outer");
    outer
        .add_signal(a, StandardReadableFormat::HintedSignal)
        .unwrap();
    outer.add_child(Arc::new(inner)).unwrap();
    outer
        .add_signal(c, StandardReadableFormat::ConfigSignal)
        .unwrap();
    outer
        .add_signal(u, StandardReadableFormat::UncachedSignal)
        .unwrap();
    outer
        .add_signal(hu, StandardReadableFormat::HintedUncachedSignal)
        .unwrap();

    let backends = [&a_backend, &b_backend, &c_backend, &u_backend, &hu_backend];
    let counts = || backends.map(|b| b.subscriber_count());
    assert_eq!(counts(), [0; 5]);
    Stageable::stage(&outer).await.unwrap();
    assert_eq!(counts(), [1, 1, 0, 0, 0]);
    Stageable::unstage(&outer).await.unwrap();
    assert_eq!(counts(), [0; 5]);

    let empty = StandardReadable::new("empty");
    Stageable::stage(&empty).await.unwrap();
    empty.configure(ConfigureArgs::default()).await.unwrap();
    Stageable::unstage(&empty).await.unwrap();
}

const CONNECT_DELAY: Duration = Duration::from_millis(100);

/// Connects after `CONNECT_DELAY`; a PV containing `FAIL` refuses at once.
struct ProbeBackend {
    pv: String,
    connected: AtomicBool,
}

impl BackendFromPv for ProbeBackend {
    fn from_pv(pv: &str) -> Self {
        Self {
            pv: pv.to_string(),
            connected: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl SignalBackend<f64> for ProbeBackend {
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        if self.pv.contains("FAIL") {
            return Err(BsrsError::Backend(format!("cannot connect {}", self.pv)));
        }
        tokio::time::sleep(CONNECT_DELAY).await;
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn put_start(&self, _value: Option<f64>) -> Result<PutCompletion> {
        Err(BsrsError::Backend("probe does not put".into()))
    }
    async fn get_datakey(&self, _source: &str) -> Result<DataKey> {
        Err(BsrsError::Backend("probe has no datakey".into()))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        Err(BsrsError::Backend("probe has no reading".into()))
    }
    async fn get_value(&self) -> Result<f64> {
        if self.connected.load(Ordering::SeqCst) {
            Ok(0.0)
        } else {
            Err(BsrsError::Backend(format!("{} not connected", self.pv)))
        }
    }
    async fn get_setpoint(&self) -> Result<f64> {
        self.get_value().await
    }
    fn set_callback(&self, _cb: Option<ReadingValueCallback<f64>>) -> SubToken {
        SubToken::noop()
    }
    fn source(&self, name: &str, _read: bool) -> String {
        format!("probe://{name}")
    }
}

#[derive(Device)]
struct ProbeAxis {
    name: String,
    #[signal(rw, "{prefix}.VAL")]
    setpoint: SignalRW<f64, ProbeBackend>,
    #[signal(ro, "{prefix}.RBV")]
    readback: SignalR<f64, ProbeBackend>,
}

#[derive(Device)]
struct ProbeStage {
    name: String,
    #[device("{prefix}:x")]
    x: Arc<ProbeAxis>,
    #[device("{prefix}:y")]
    y: Arc<ProbeAxis>,
}

#[derive(Device)]
struct MixedStage {
    name: String,
    #[device("{prefix}:ok")]
    ok: Arc<ProbeAxis>,
    #[device("{prefix}:FAIL")]
    bad: Arc<ProbeAxis>,
}

async fn assert_axis_connected(axis: &ProbeAxis) {
    axis.setpoint.get().await.unwrap();
    axis.readback.get().await.unwrap();
}

// ophyd-async test_device::test_wait_for_connection,
// test_children_of_device_have_set_names_and_get_connected (connect part),
// test_many_individual_device_connects_not_slow.
#[tokio::test(start_paused = true)]
async fn connect_all_connects_every_child_concurrently() {
    let stage = ProbeStage::new("P");
    let t0 = tokio::time::Instant::now();
    stage.connect_all(Duration::from_secs(1)).await.unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed >= CONNECT_DELAY && elapsed < 2 * CONNECT_DELAY,
        "{elapsed:?}"
    );
    assert_axis_connected(&stage.x).await;
    assert_axis_connected(&stage.y).await;

    let stages: DeviceVector<Arc<ProbeStage>> = (1..=50)
        .map(|i| (i, ProbeStage::new(&format!("P{i}"))))
        .collect();
    let t0 = tokio::time::Instant::now();
    stages.connect_all(Duration::from_secs(1)).await.unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed >= CONNECT_DELAY && elapsed < 2 * CONNECT_DELAY,
        "{elapsed:?}"
    );
    for s in stages.values() {
        assert_axis_connected(&s.x).await;
        assert_axis_connected(&s.y).await;
    }
}

// ophyd-async test_device::test_wait_for_connection_propagates_error: one
// failing child fails the device while its siblings still finish connecting.
#[tokio::test(start_paused = true)]
async fn connect_all_propagates_child_failure_after_attempting_all() {
    let stage = MixedStage::new("M");
    let err = stage.connect_all(Duration::from_secs(1)).await.unwrap_err();
    assert!(err.to_string().contains("M:FAIL"), "{err}");
    assert_axis_connected(&stage.ok).await;
    assert!(stage.bad.readback.get().await.is_err());

    let healthy = ProbeStage::new("H");
    let mut vec: DeviceVector<Arc<dyn Device>> = DeviceVector::new();
    vec.insert(1, healthy.clone());
    vec.insert(2, MixedStage::new("N"));
    let err = vec.connect_all(Duration::from_secs(1)).await.unwrap_err();
    assert!(err.to_string().contains("N:FAIL"), "{err}");
    assert_axis_connected(&healthy.x).await;
    assert_axis_connected(&healthy.y).await;
}

fn async_readable<T: AsyncReadable + ?Sized>() {}
fn async_configurable<T: AsyncConfigurable + ?Sized>() {}
fn movable_and_locatable<T: AsyncMovable<f64> + Locatable<f64> + Movable<f64> + ?Sized>() {}
fn stageable<T: Stageable + StageableSync + ?Sized>() {}
fn subscribable<T: AsyncSubscribable<f64> + ?Sized>() {}
fn triggerable<T: Triggerable + ?Sized>() {}
fn sync_readable<T: Readable + Configurable + ?Sized>() {}

// ophyd test_bluesky_protocols (Readable/Movable/Stageable/Subscribable/
// Configurable conformance); ophyd-async test_protocol::test_readable.
#[test]
fn protocol_surfaces_compile() {
    async_readable::<SoftMotor>();
    movable_and_locatable::<SoftMotor>();
    async_readable::<SoftDetector>();
    async_readable::<SignalR<f64, Soft>>();
    subscribable::<SignalR<f64, Soft>>();
    stageable::<SignalR<f64, Soft>>();
    triggerable::<SignalX<(), SoftSignalBackend<()>>>();
    async_readable::<StandardReadable>();
    async_configurable::<StandardReadable>();
    stageable::<StandardReadable>();
    sync_readable::<StandardReadable>();
}

// ophyd-async test_sim_motor::test_stop / StandardMovable.stop: the sim motor
// is Stoppable, stop leaves setpoint == readback, and a later set still moves.
// SoftMotor moves instantly, so test_stop's cancelled in-flight move has no
// counterpart.
#[tokio::test]
async fn soft_motor_stop_holds_position_and_allows_the_next_move() {
    async fn stop<T: Stoppable + ?Sized>(dev: &T, success: bool) {
        dev.stop(success).await.unwrap();
    }
    let motor = SoftMotor::new("m", Some(0.0));
    AsyncMovable::set(&motor, 3.0).await.await.unwrap();
    stop(&motor, false).await;
    let loc = motor.locate().await.unwrap();
    assert_eq!((loc.setpoint, loc.readback), (3.0, 3.0));

    AsyncMovable::set(&motor, 5.0).await.await.unwrap();
    stop(&motor, true).await;
    let loc = motor.locate().await.unwrap();
    assert_eq!((loc.setpoint, loc.readback), (5.0, 5.0));
}

// ophyd test_device::test_device_state and test_status_wait through the
// blocking facade: set().wait(), read, stage, configure, unstage.
#[test]
fn sync_facade_drives_soft_motor_and_standard_readable() {
    let motor = SoftMotor::new("m", Some(0.0));
    motor
        .set_blocking(2.5)
        .wait(Some(Duration::from_secs(1)))
        .unwrap();
    assert_eq!(
        motor.read_blocking().unwrap()["m"].value,
        serde_json::json!(2.5)
    );

    let (x, x_backend) = soft_signal("x", Kind::Normal, 1.0);
    let mut sr = StandardReadable::new("dev");
    sr.add_signal(x, StandardReadableFormat::HintedSignal)
        .unwrap();
    sr.add_signal(sig("c", Kind::Config), StandardReadableFormat::ConfigSignal)
        .unwrap();

    sr.stage_blocking().unwrap();
    assert_eq!(x_backend.subscriber_count(), 1);
    assert_eq!(keys(&sr.read_blocking().unwrap()), set(&["x"]));
    assert_eq!(keys(&sr.describe_blocking().unwrap()), set(&["x"]));
    assert_eq!(
        keys(&sr.read_configuration_blocking().unwrap()),
        set(&["c"])
    );
    assert_eq!(
        keys(&sr.describe_configuration_blocking().unwrap()),
        set(&["c"])
    );
    sr.configure_blocking(ConfigureArgs::default()).unwrap();
    sr.unstage_blocking().unwrap();
    assert_eq!(x_backend.subscriber_count(), 0);
}

// ophyd test_ophydobj::test_unsubscribe, test_self_removing_cb.
#[test]
fn signal_subscription_token_unsubscribes() {
    let (s, backend) = soft_signal("s", Kind::Normal, 0.0);

    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let token = s.subscribe(Box::new(move |_, _, _| {
        h.fetch_add(1, Ordering::SeqCst);
    }));
    // Subscribing delivers the current reading, as ophyd-async
    // `SoftSignalBackend.set_callback` does.
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    backend.write_now(1.0);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    drop(token);
    backend.write_now(2.0);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert_eq!(backend.subscriber_count(), 0);

    let own: Arc<Mutex<Option<SubToken>>> = Arc::new(Mutex::new(None));
    let (remover_hits, sitter_hits) =
        (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (r, slot) = (remover_hits.clone(), own.clone());
    *own.lock().unwrap() = Some(s.subscribe(Box::new(move |_, _, _| {
        r.fetch_add(1, Ordering::SeqCst);
        drop(slot.lock().unwrap().take());
    })));
    let st = sitter_hits.clone();
    let _sitter = s.subscribe(Box::new(move |_, _, _| {
        st.fetch_add(1, Ordering::SeqCst);
    }));
    // The remover's first call, on subscribe, ran before its token was stored.
    backend.write_now(3.0);
    backend.write_now(4.0);
    assert_eq!(remover_hits.load(Ordering::SeqCst), 2);
    assert_eq!(sitter_hits.load(Ordering::SeqCst), 3);
    assert_eq!(backend.subscriber_count(), 1);
}

// ophyd test_ophydobj::test_status_basic, test_status_callback,
// test_status_wait, test_status_wait_timeout.
#[test]
fn status_wait_reports_finish_and_timeout() {
    let done = Status::done();
    assert!(done.done_state() && done.success());
    done.wait(Some(Duration::from_secs(1))).unwrap();

    let (pending, setter) = Status::new();
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    pending.add_callback(move |outcome| {
        assert!(matches!(outcome, StatusOutcome::Success));
        h.fetch_add(1, Ordering::SeqCst);
    });
    assert!(matches!(
        pending.wait(Some(Duration::from_millis(20))),
        Err(WaitError::Timeout)
    ));
    assert!(!pending.done_state());
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    setter.success();
    pending.wait(Some(Duration::from_secs(1))).unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}
