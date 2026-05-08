//! Minimal cirrus example using the async (ophyd-async) API surface.
//!
//! Run with `cargo run --example async_count`.

use std::sync::Arc;

use cirrus::backends::soft::SoftDetector;
use cirrus::callbacks::StderrTraceSink;
use cirrus::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let det = SoftDetector::new("det1");
    let trace: Arc<dyn DocumentSink> = Arc::new(StderrTraceSink);
    let re = RunEngine::new(vec![trace]);

    let plan = cirrus::ophyd_async::count(vec![det], 5);
    let result = re.run_async(plan).await?;
    println!(
        "run finished: {} (uid: {:?})",
        result.exit_status, result.run_uid
    );
    Ok(())
}
