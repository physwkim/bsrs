//! Plan-loop overhead bench. Measures: how much time does the
//! engine spend per `Msg` when nothing else is going on?
//!
//! `count(soft_det, N)` produces a fixed Msg count (≈ 5N + 4 for
//! Start/Descriptor/Save/Stop bookkeeping plus per-point Read+Save).
//! Sweep N to amortize fixed overheads vs per-Msg overhead.

use std::sync::Arc;

use bsrs::backends::soft::SoftDetector;
use bsrs::core::msg::ReadableObj;
use bsrs::engine::RunEngine;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn bench_count_loop(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("count_plan");
    for n in [1, 10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let det = SoftDetector::new("d");
                let re = RunEngine::new(vec![]);
                let plan = bsrs::plans::count(vec![det as Arc<dyn ReadableObj>], n);
                let r = re.run_async(plan).await.unwrap();
                assert_eq!(r.exit_status, "success");
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_count_loop);
criterion_main!(benches);
