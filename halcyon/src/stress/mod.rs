//! Concurrency stress harness (feature `stress`). Rust-native port of the ideas
//! in `../Halcyon/tests/stress/`: a deterministic thread-safe fake driver, a
//! barrier-based workload runner with a log2 latency histogram, and the pool
//! scenarios. Pure-Rust — no clidriver, no live Db2.

mod fake_driver;
mod histogram;

pub use fake_driver::ConcurrentFakeDriver;
pub use histogram::LatencyHistogram;
