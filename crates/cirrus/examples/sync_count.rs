//! Same example as `async_count`, written with the sync (ophyd) API surface.
//!
//! Run with `cargo run --example sync_count`.

use std::sync::Arc;

use cirrus::backends::soft::SoftDetector;
use cirrus::callbacks::StderrTraceSink;
use cirrus::prelude::*;

fn main() -> Result<()> {
    let det = SoftDetector::new("det1");
    let trace: Arc<dyn DocumentSink> = Arc::new(StderrTraceSink);
    let re = RunEngine::new(vec![trace]);

    // Sync entry — ophyd-style. Plan is identical to the async version.
    let plan = cirrus::ophyd::count(vec![det], 5);
    let result = re.run_blocking(plan)?;
    println!(
        "run finished: {} (uid: {:?})",
        result.exit_status, result.run_uid
    );
    Ok(())
}
