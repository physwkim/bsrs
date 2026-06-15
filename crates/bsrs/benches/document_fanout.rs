//! Document fan-out bench. Measures how cost scales with the
//! number of subscribers attached to a `RunEngine`. Each subscriber
//! is a no-op closure (just bumps an atomic) so the bench isolates
//! the broadcast overhead, not the subscriber's own work.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bsrs::backends::soft::SoftDetector;
use bsrs_core::msg::ReadableObj;
use bsrs_engine::{DocumentCallback, RunEngine};
use bsrs_event_model::Document;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn bench_fanout(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("document_fanout");
    for n_subs in [0usize, 1, 4, 16, 64].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n_subs), n_subs, |b, &n_subs| {
            b.to_async(&rt).iter_with_setup(
                || {
                    let counter = Arc::new(AtomicU64::new(0));
                    let re = RunEngine::new(vec![]);
                    for _ in 0..n_subs {
                        let c = counter.clone();
                        let cb: DocumentCallback = Arc::new(move |_d: &Document| {
                            c.fetch_add(1, Ordering::Relaxed);
                        });
                        re.subscribe(cb);
                    }
                    (re, counter)
                },
                |(re, _counter)| async move {
                    let det = SoftDetector::new("d");
                    let plan = bsrs_plans::count(vec![det as Arc<dyn ReadableObj>], 10);
                    let r = re.run_async(plan).await.unwrap();
                    assert_eq!(r.exit_status, "success");
                },
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_fanout);
criterion_main!(benches);
