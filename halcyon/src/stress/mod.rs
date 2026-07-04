//! Concurrency stress harness (feature `stress`). Rust-native port of the ideas
//! in `../Halcyon/tests/stress/`: a deterministic thread-safe fake driver, a
//! barrier-based workload runner with a log2 latency histogram, and the pool
//! scenarios. Pure-Rust — no clidriver, no live Db2.

mod fake_driver;
mod histogram;
mod runner;
mod scenarios;

pub use fake_driver::ConcurrentFakeDriver;
pub use histogram::LatencyHistogram;
pub use runner::{run_workload, RunConfig, RunReport, Stop, WorkerCtx};
pub use scenarios::{
    config_for, make_pool_contention, make_reconnect_faults, make_txn_churn, select_n, Scenario,
};
