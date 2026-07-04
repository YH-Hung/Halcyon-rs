//! Criterion micro-benchmarks: pooled acquire/release round-trip and a single
//! scalar query on the fake. Run with:
//!   cargo bench -p halcyon --no-default-features --features stress
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

use halcyon::driver::ConnectionParams;
use halcyon::retry::BackoffPolicy;
use halcyon::stress::ConcurrentFakeDriver;
use halcyon::{Database, PoolConfig};

fn db() -> Database {
    let cfg = PoolConfig {
        min: 2,
        max: 4,
        acquire_timeout: Duration::from_millis(1000),
        idle_timeout: Duration::from_secs(3600),
        max_lifetime: Duration::from_secs(3600),
        validate_on_acquire: false,
        backoff: BackoffPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            max_attempts: 1,
        },
        maintenance_interval: Duration::from_secs(3600),
        start_maintenance_thread: false,
    };
    Database::open(
        Arc::new(ConcurrentFakeDriver::new()),
        ConnectionParams {
            connection_string: "dsn".into(),
        },
        cfg,
    )
    .unwrap()
}

fn bench_acquire_release(c: &mut Criterion) {
    let db = db();
    c.bench_function("pool_acquire_release", |b| {
        b.iter(|| {
            let lease = db.pool().acquire().unwrap();
            std::hint::black_box(&lease);
        });
    });
}

fn bench_scalar_query(c: &mut Criterion) {
    let db = db();
    c.bench_function("query_scalar", |b| {
        b.iter(|| {
            let rows: Vec<(i64,)> = db.query_as("SELECT 7 FROM SYSIBM.SYSDUMMY1", ()).unwrap();
            std::hint::black_box(rows);
        });
    });
}

criterion_group!(benches, bench_acquire_release, bench_scalar_query);
criterion_main!(benches);
