//! Concurrent pool throughput sweep. Reuses the stress runner in duration mode,
//! sweeping pool_max against a fixed contending-thread count, printing throughput
//! and p50/p95/p99. Deterministic (fake driver), no Db2. Run with:
//!   cargo run --release -p halcyon --no-default-features --features stress --example throughput
use std::sync::Arc;
use std::time::Duration;

use halcyon::driver::ConnectionParams;
use halcyon::stress::{
    config_for, make_pool_contention, run_workload, ConcurrentFakeDriver, RunConfig, Scenario, Stop,
};
use halcyon::Database;

fn main() {
    let threads = 32;
    let duration = Duration::from_millis(500);
    println!(
        "{:>8}  {:>12}  {:>8}  {:>8}  {:>8}  {:>8}",
        "pool_max", "throughput/s", "p50_us", "p95_us", "p99_us", "err_rate"
    );
    for pool_max in [1usize, 2, 4, 8, 16] {
        let fake = Arc::new(ConcurrentFakeDriver::new());
        let db = Database::open(
            fake,
            ConnectionParams { connection_string: "dsn".into() },
            config_for(Scenario::Pool, pool_max),
        )
        .unwrap();

        let body = make_pool_contention(&db);
        let cfg = RunConfig {
            threads,
            stop: Stop::Duration(duration),
            warmup: Some(Stop::Duration(Duration::from_millis(100))),
            seed: 1,
        };
        let r = run_workload("pool", &cfg, body);

        println!(
            "{:>8}  {:>12.0}  {:>8.1}  {:>8.1}  {:>8.1}  {:>8.4}",
            pool_max,
            r.throughput_per_sec(),
            r.p50_us(),
            r.p95_us(),
            r.p99_us(),
            r.error_rate()
        );
    }
}
