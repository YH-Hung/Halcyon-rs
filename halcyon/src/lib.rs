//! # Halcyon
//!
//! A high-level, ergonomic IBM Db2 client for Rust, built on the IBM Db2 CLI
//! (`libdb2` from clidriver). Rust counterpart of the C++
//! [Halcyon](https://github.com/YH-Hung/Halcyon) library.
//!
//! **Status: early development.** The target architecture mirrors the C++
//! layering. The driver seam, the core data path ([`Connection`], [`Statement`],
//! [`ResultSet`]/[`Row`], `?`/`:name` params, row → tuple mapping), the
//! connection [`pool`] (bounded sizing, blocking acquire, reaping, transparent
//! reconnect) with safe auto-[`retry`], and the pooled [`Database`] facade
//! (owned [`QueryResult`] cursors and [`ScopedTransaction`]s) are implemented and
//! mock-tested. The pool's concurrency-correctness track — `loom` model-checking
//! of the lock/condvar handoff plus a `ConcurrentFakeDriver` ThreadSanitizer
//! stress harness and perf benchmarks — is in place (Phase 4), and every pooled
//! connection carries a per-connection prepared-statement LRU
//! ([`PoolConfig::statement_cache_size`], default 64; Phase 5, validated
//! against live Db2). The `tokio` async facade ([`AsyncDatabase`], feature
//! `tokio`) offloads `execute`/`query_as`/`execute_batch`/`transaction` to
//! `spawn_blocking` (Phase 6, validated against live Db2). Observability
//! (Phase 7) is next. See the README roadmap:
//!
//! ```text
//! facade ─► pool ─► core ─► driver (CliDriver seam) ─► halcyon-sys ─► libdb2
//! ```
//!
//! Only `driver::db2` (feature `db2`) touches the CLI ABI; everything above the
//! seam is portable, mockable, `unsafe`-free Rust.
//!
//! ```ignore
//! use std::sync::Arc;
//! use halcyon::{Connection, driver::{Db2CliDriver, ConnectionParams}};
//!
//! # fn main() -> halcyon::Result<()> {
//! let driver = Arc::new(Db2CliDriver::new());
//! let conn = Connection::open(driver, &ConnectionParams {
//!     connection_string: "DATABASE=sample;HOSTNAME=localhost;...".into(),
//! })?;
//!
//! let mut rows = conn.query("SELECT id, name FROM users WHERE id > ?", (10,))?;
//! while let Some(row) = rows.next()? {
//!     let (id, name): (i64, String) = row.get()?;
//!     println!("{id}: {name}");
//! }
//! # Ok(())
//! # }
//! ```

// The seam rule, enforced: unsafe lives only in driver::db2 (which carries an
// explicit allow) and in the halcyon-sys crate.
#![deny(unsafe_code)]

// Lets `#[derive(FromRow)]`'s generated `::halcyon::…` paths resolve inside this
// crate's own tests, exactly as they do downstream.
extern crate self as halcyon;

mod core;
pub mod driver;
mod error;
mod facade;
// Feature-gated async facade (tokio spawn_blocking). See async_db.rs.
#[cfg(feature = "tokio")]
mod async_db;
// Feature-gated observability helpers (tracing + metrics). See obs/.
#[cfg(feature = "observability")]
mod obs;
pub mod pool;
pub mod retry;
mod value;

// Synchronization shim (std, or loom under `--cfg loom`). See sync.rs.
mod sync;

// Shared, scriptable mock driver for the layers above the seam. Test-only, so
// it never ships in a normal build; every upper-layer unit test builds on it.
#[cfg(test)]
mod testing;

// Concurrency stress harness (feature `stress`): a deterministic thread-safe
// fake driver, a workload runner, and the pool scenarios. Public so integration
// tests, benches, and examples can reach it; absent from a default build.
#[cfg(feature = "stress")]
pub mod stress;

#[cfg(feature = "tokio")]
pub use async_db::AsyncDatabase;
pub use core::{
    Connection, FromColumn, FromRow, IntoParams, Named, Params, QueryMap, ResultSet, Row,
    Statement, Transaction,
};
pub use error::{Error, ErrorCode, Result};
pub use facade::{Database, QueryResult, ScopedTransaction, TxCursor, TxQueryMap};
pub use pool::{ConnectionPool, PoolConfig, PooledConnection};
pub use retry::{BackoffPolicy, ExecPolicy};
pub use value::Value;

/// Derives [`FromRow`] for a struct, mapping a result-set row into its fields by
/// position (feature `derive`, on by default). Field types must implement
/// [`FromColumn`].
#[cfg(feature = "derive")]
pub use halcyon_derive::FromRow;
