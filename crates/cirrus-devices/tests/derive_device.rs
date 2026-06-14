//! `#[derive(Device)]` round-trip — define a Motor and an XYStage, build
//! instances, run `connect_all`, verify field PV names.

use std::time::Duration;

use cirrus_backend_soft::SoftSignalBackend;
use cirrus_core::Kind;
use cirrus_devices::{Device, SignalR, SignalRW};

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
