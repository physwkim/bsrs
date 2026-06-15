//! `#[derive(Device)]` round-trip — define a Motor and an XYStage, build
//! instances, run `connect_all`, verify field PV names.

use std::time::Duration;

use bsrs_backend_soft::SoftSignalBackend;
use bsrs_core::Kind;
use bsrs_devices::{walk_signal_sources, Device, DeviceVector, SignalR, SignalRW};

#[derive(Device)]
struct Motor {
    name: String,
    #[signal(rw, "{prefix}.VAL")]
    setpoint: SignalRW<f64, SoftSignalBackend<f64>>,
    #[signal(ro, "{prefix}.RBV", kind = hinted)]
    readback: SignalR<f64, SoftSignalBackend<f64>>,
    #[signal(rw, "{prefix}.VELO", kind = config)]
    velocity: SignalRW<f64, SoftSignalBackend<f64>>,
}

#[derive(Device)]
struct XYStage {
    name: String,
    #[device("{prefix}:x")]
    x: std::sync::Arc<Motor>,
    #[device("{prefix}:y")]
    y: std::sync::Arc<Motor>,
}

#[tokio::test]
async fn motor_derive_builds_and_connects() {
    let m = Motor::new("BL10C:m1");
    assert_eq!(m.name(), "BL10C:m1");
    // Connect should succeed (soft backend always connects).
    m.connect_all(Duration::from_millis(100)).await.unwrap();
    // Each signal's source field should reflect the expanded PV name.
    assert_eq!(m.setpoint.kind(), Kind::Normal);
    assert_eq!(m.readback.kind(), Kind::Hinted);
    assert_eq!(m.velocity.kind(), Kind::Config);

    // Access roles are enforced at the type level: the RW setpoint can be
    // put and read back, the RO readback can be read. (`m.readback.put(..)`
    // or `m.setpoint`-less access would not compile.)
    m.setpoint.put(1.5).await.await.unwrap();
    assert_eq!(m.setpoint.get().await.unwrap(), 1.5);
    let _ = m.readback.get().await.unwrap();
}

#[tokio::test]
async fn nested_device_propagates_prefix() {
    let stage = XYStage::new("BL10C");
    assert_eq!(stage.name(), "BL10C");
    // Nested motors carry expanded prefixes.
    assert_eq!(stage.x.name(), "BL10C:x");
    assert_eq!(stage.y.name(), "BL10C:y");
    stage.connect_all(Duration::from_millis(100)).await.unwrap();
}

#[tokio::test]
async fn new_named_propagates_bluesky_names() {
    // CP-06: new_named names the device and propagates `{name}-{field}`
    // recursively to sub-devices and signals (the bluesky convention),
    // while PVs still resolve from `prefix`.
    let stage = XYStage::new_named("BL10C", "stage");
    assert_eq!(stage.name(), "stage");
    // Sub-devices: name = "{dev}-{field}", PV still expanded from prefix.
    assert_eq!(stage.x.name(), "stage-x");
    assert_eq!(stage.y.name(), "stage-y");

    // Signals on a sub-device: name = "{subdev}-{field}". A signal's read key
    // is its name, so describe()/read() now key on the bluesky name.
    let read = stage.x.readback.read().await.unwrap();
    assert!(
        read.contains_key("stage-x-readback"),
        "keys: {:?}",
        read.keys()
    );
    let desc = stage.x.setpoint.describe().await.unwrap();
    assert!(
        desc.contains_key("stage-x-setpoint"),
        "keys: {:?}",
        desc.keys()
    );

    // Plain `new` keeps PV-based names (backward compatible).
    let bare = Motor::new("BL10C:m1");
    let bare_read = bare.readback.read().await.unwrap();
    assert!(
        bare_read.contains_key("BL10C:m1.RBV"),
        "keys: {:?}",
        bare_read.keys()
    );

    stage.connect_all(Duration::from_millis(100)).await.unwrap();
}

#[test]
fn walk_signal_sources_flat_device() {
    // CP-20: a flat device walks at the empty root prefix, so each key is the
    // bare field name and each value is the signal's transport source. Order is
    // field-declaration order (setpoint, readback, velocity).
    let m = Motor::new("BL10C:m1");
    let sources = walk_signal_sources(&*m);
    assert_eq!(
        sources,
        vec![
            ("setpoint".to_string(), "soft://BL10C:m1.VAL".to_string()),
            ("readback".to_string(), "soft://BL10C:m1.RBV".to_string()),
            ("velocity".to_string(), "soft://BL10C:m1.VELO".to_string()),
        ]
    );
}

#[test]
fn walk_signal_sources_nested_device() {
    // CP-20: a sub-device contributes `{field}.{signal}` keys, recursing
    // depth-first — every signal of `x` precedes every signal of `y`. The
    // source still resolves from the expanded PV prefix, not the dotted path.
    let stage = XYStage::new("BL10C");
    let sources = walk_signal_sources(&*stage);
    assert_eq!(
        sources,
        vec![
            ("x.setpoint".to_string(), "soft://BL10C:x.VAL".to_string()),
            ("x.readback".to_string(), "soft://BL10C:x.RBV".to_string()),
            ("x.velocity".to_string(), "soft://BL10C:x.VELO".to_string()),
            ("y.setpoint".to_string(), "soft://BL10C:y.VAL".to_string()),
            ("y.readback".to_string(), "soft://BL10C:y.RBV".to_string()),
            ("y.velocity".to_string(), "soft://BL10C:y.VELO".to_string()),
        ]
    );
}

#[tokio::test]
async fn device_vector_holds_and_connects_derived_devices() {
    // CP-07: a DeviceVector of derived Motors connects through the derive's
    // `impl Device`, indexes by key, and yields bluesky `children()` pairs.
    let mut cams: DeviceVector<std::sync::Arc<Motor>> = DeviceVector::new();
    for i in 1..=3u32 {
        cams.insert(
            i,
            Motor::new_named(&format!("BL10C:m{i}"), &format!("m{i}")),
        );
    }
    assert_eq!(cams.len(), 3);
    // Index + Device::name through Arc<Motor>.
    assert_eq!(cams[2].name(), "m2");
    // children() yields ascending ("1", ..), ("2", ..), ("3", ..).
    let keys: Vec<_> = cams.children().map(|(k, _)| k).collect();
    assert_eq!(keys, vec!["1", "2", "3"]);
    // connect_all drives every child's derived connect.
    cams.connect_all(Duration::from_millis(100)).await.unwrap();
}
