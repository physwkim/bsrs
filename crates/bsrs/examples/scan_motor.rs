//! Step scan: move a soft motor 0.0 → 4.0 in 5 steps, read a soft detector
//! at every position. Demonstrates the motor + detector + RunEngine pipeline.
//!
//! Run with `cargo run --example scan_motor`.

use std::sync::Arc;

use bsrs::backends::soft::{SoftDetector, SoftMotor};
use bsrs::callbacks::StderrTraceSink;
use bsrs::prelude::*;
use bsrs_core::msg::{MovableObj, ReadableObj};

#[tokio::main]
async fn main() -> Result<()> {
    let det = SoftDetector::new("det1");
    let motor = Arc::new(SoftMotor::new("m1", Some(0.0)));
    let trace: Arc<dyn DocumentSink> = Arc::new(StderrTraceSink);
    let re = RunEngine::new(vec![trace]);

    let plan = bsrs::ophyd_async::scan(
        vec![det as Arc<dyn ReadableObj>],
        motor.clone() as Arc<dyn MovableObj>,
        motor as Arc<dyn ReadableObj>,
        0.0,
        4.0,
        5,
    );
    let result = re.run_async(plan).await?;
    println!("scan finished: {}", result.exit_status);
    Ok(())
}
