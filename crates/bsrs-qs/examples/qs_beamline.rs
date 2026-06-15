//! Example bsrs-qs daemon. Registers a few soft devices and the `count`
//! plan, binds the standard queueserver control + document addresses,
//! serves forever.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example qs_beamline
//! ```
//!
//! Then in another shell, drive it with the queueserver `qserver` CLI
//! (or the integration test in `tests/`):
//!
//! ```bash
//! qserver ping
//! qserver environment open
//! qserver queue add count det1 5
//! qserver queue start
//! qserver status
//! ```

use std::sync::Arc;

use bsrs_backend_soft::SoftDetector;
use bsrs_core::msg::ReadableObj;
use bsrs_qs::{Registry, Server};

#[tokio::main]
async fn main() -> bsrs_core::error::Result<()> {
    let det1 = SoftDetector::new("det1");
    let det2 = SoftDetector::new("det2");

    let mut reg = Registry::new();
    reg.register_readable("det1", det1 as Arc<dyn ReadableObj>);
    reg.register_readable("det2", det2 as Arc<dyn ReadableObj>);
    reg.register_plan_count("count");

    println!("[bsrs-qs] starting on tcp://*:60615 (control), tcp://*:60625 (documents)");

    Server::builder()
        .control_address("tcp://*:60615")
        .document_address("tcp://*:60625")
        .registry(reg)
        .build()?
        .run_async()
        .await
}
