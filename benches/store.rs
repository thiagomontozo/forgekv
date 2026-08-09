use std::{hint::black_box, sync::Arc};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use forgekv::{metrics::Metrics, store::ShardedStore};

fn store_benchmarks(criterion: &mut Criterion) {
    let store = ShardedStore::new(64, Arc::new(Metrics::default())).expect("valid shards");
    store
        .set(
            Bytes::from_static(b"existing"),
            Bytes::from_static(b"value"),
        )
        .expect("setup should work");

    criterion.bench_function("in_memory_set", |bench| {
        bench.iter(|| {
            store
                .set(
                    Bytes::from_static(b"write-key"),
                    black_box(Bytes::from_static(b"value")),
                )
                .expect("set should work")
        });
    });
    criterion.bench_function("get_hit", |bench| {
        bench.iter(|| store.get(black_box(b"existing")).expect("get should work"));
    });
    criterion.bench_function("get_miss", |bench| {
        bench.iter(|| store.get(black_box(b"missing")).expect("get should work"));
    });

    for index in 0..1_000 {
        store
            .set(
                Bytes::from(format!("snapshot:{index}")),
                Bytes::from_static(b"benchmark-value"),
            )
            .expect("snapshot setup should work");
    }
    criterion.bench_function("snapshot_1000_entries", |bench| {
        bench.iter(|| {
            black_box(
                store
                    .snapshot_entries()
                    .expect("snapshot should work"),
            )
        });
    });
}

criterion_group!(benches, store_benchmarks);
criterion_main!(benches);
