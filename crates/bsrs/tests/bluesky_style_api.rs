//! Bluesky-style ergonomic API verification.
//!
//! Demonstrates that with `use bsrs::prelude::*` users can write
//! against `Arc<dyn TraitObj>` using the same short names bluesky
//! Python uses, without explicit `_dyn` suffixes.

use std::sync::Arc;

use bsrs::backends::soft::{SoftDetector, SoftMotor};
use bsrs::prelude::*;
use bsrs_core::msg::{LocatableObj, MovableObj, ReadableObj, StoppableObj};

#[tokio::test]
async fn position_read_set_trigger_short_names() {
    // Construct concrete devices.
    let motor = Arc::new(SoftMotor::new("m1", Some(0.5)));
    let det = SoftDetector::new("det1");

    // Cast to trait objects (the typical shape inside plan factories).
    let motor_loc: Arc<dyn LocatableObj> = motor.clone();
    let motor_mv: Arc<dyn MovableObj> = motor.clone();
    let motor_stop: Arc<dyn StoppableObj> = motor.clone();
    let det_read: Arc<dyn ReadableObj> = det;

    // -- Bluesky-style usage --
    // motor.position()  ->  Result<f64>
    let pos = motor_loc.position().await.unwrap();
    assert_eq!(pos, 0.5);

    // motor.target()    ->  Result<f64>  (setpoint)
    let tgt = motor_loc.target().await.unwrap();
    assert_eq!(tgt, 0.5);

    // motor.locate()    ->  Result<DynLocation>
    let loc = motor_loc.locate().await.unwrap();
    assert_eq!(loc.readback, 0.5);
    assert_eq!(loc.setpoint, 0.5);

    // motor.set(value)  ->  Status (await for completion)
    let status = motor_mv.set(1.5).await;
    status.await.expect("move to complete");
    assert_eq!(motor_loc.position().await.unwrap(), 1.5);

    // motor.move_to(v)  ->  Result<()>  (set + await Status)
    motor_mv.move_to(2.0).await.unwrap();
    assert_eq!(motor_loc.position().await.unwrap(), 2.0);

    // motor.stop()      ->  Result<()>
    motor_stop.stop().await.unwrap();

    // det.read()        ->  Result<HashMap<String, ReadingValue>>
    let reading = det_read.read().await.unwrap();
    assert!(!reading.is_empty(), "det1 should produce a reading");

    // det.describe()    ->  Result<HashMap<String, DataKey>>
    let dk = det_read.describe().await.unwrap();
    assert!(
        dk.keys().any(|k| k.starts_with("det1")),
        "describe should include a det1-prefixed key, got {:?}",
        dk.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn ext_traits_work_on_concrete_types_too() {
    // The blanket impl `impl<T: ReadableObj + ?Sized> ReadableExt for T`
    // also applies to concrete types via deref.
    let motor = Arc::new(SoftMotor::new("m1", Some(2.5)));

    let pos = motor.position().await.unwrap();
    assert!((pos - 2.5).abs() < 1e-9);

    let target = motor.target().await.unwrap();
    assert!((target - 2.5).abs() < 1e-9);

    let _location = motor.locate().await.unwrap();

    motor.move_to(7.0).await.unwrap();
    assert!((motor.position().await.unwrap() - 7.0).abs() < 1e-9);
}

#[tokio::test]
async fn bluesky_style_compose_in_plan_factory() {
    // A plain helper that uses Ext methods to build a Plan body
    // (without _dyn suffix anywhere in the user code).
    use bsrs_core::msg::Msg;
    use bsrs_core::plan::{plan_box, Plan};
    use futures::StreamExt;

    fn move_then_read(
        motor: Arc<dyn MovableObj>,
        readable: Arc<dyn ReadableObj>,
        target: f64,
    ) -> Plan {
        plan_box(async_stream::stream! {
            // No-op of this test: the *plan factory body* itself is async
            // Rust, not a coroutine, so we can read state directly with
            // bluesky-style names.
            yield Msg::OpenRun(Default::default());
            yield Msg::Set { obj: motor.clone(), value: target, group: Some("m".into()) };
            yield Msg::Wait { group: "m".into(), error_on_timeout: true, timeout: None };
            yield Msg::Create { stream_name: "primary".into() };
            yield Msg::Read(readable.clone());
            yield Msg::Save;
            yield Msg::CloseRun { exit_status: "success".into(), reason: None };
        })
    }

    let motor = Arc::new(SoftMotor::new("m1", Some(0.0)));
    let det = SoftDetector::new("det1");
    let re = RunEngine::new(vec![]);
    let plan = move_then_read(motor.clone(), det.clone(), 1.5);
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(result.exit_status, "success");

    // After running, motor reports the new position via the short-name
    // Ext API.
    assert!((motor.position().await.unwrap() - 1.5).abs() < 1e-9);

    // Sanity: stream out of move_then_read (non-execution path) to
    // confirm Plans are constructible from Ext-trait callers.
    let plan2 = move_then_read(motor, det, 2.0);
    let mut count = 0usize;
    let mut p = plan2;
    while p.next().await.is_some() {
        count += 1;
    }
    assert!(count >= 6, "plan should yield at least 6 messages");
}
