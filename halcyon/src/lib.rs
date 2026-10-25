//! A Rust client for IBM Db2, built on the IBM Db2 CLI (`libdb2` from IBM's
//! clidriver). It is the Rust counterpart of the C++
//! [Halcyon](https://github.com/YH-Hung/Halcyon) library.
//!
//! The entry point is [`Database`], a cloneable, thread-safe handle over a
//! connection pool. Each call leases a connection, runs, and returns it. Reads
//! that fail with a retriable error are retried with backoff; writes, batches,
//! and LOB or streaming calls run exactly once.
//!
//! # Quick start
//!
//! The default `db2` feature links clidriver. Set `DB2_CLIDRIVER_ROOT` to its
//! directory when building, and put its `lib/` directory on `LD_LIBRARY_PATH`
//! (Linux) or `DYLD_LIBRARY_PATH` (macOS) when running.
//!
//! ```no_run
//! use halcyon::{Database, Result};
//!
//! # #[cfg(feature = "db2")]
//! fn main() -> Result<()> {
//!     let db = Database::connect(
//!         "DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret;",
//!     )?;
//!     let rows: Vec<(i64, String)> =
//!         db.query_as("SELECT id, name FROM users WHERE id > ?", (10_i64,))?;
//!     for (id, name) in rows {
//!         println!("{id}: {name}");
//!     }
//!     Ok(())
//! }
//! # #[cfg(not(feature = "db2"))]
//! # fn main() {}
//! ```
//!
//! `Database::connect_with` takes a [`PoolConfig`] for pool size, timeouts,
//! validation, statement-cache size, and session [`Isolation`].
//!
//! # Queries and parameters
//!
//! Parameters for `?` placeholders are a tuple of up to 12 values, a
//! `Vec<`[`Value`]`>`, or `()` for none. Use [`named!`] for `:name`
//! placeholders. Rows map into tuples, or into structs with
//! `#[derive(FromRow)]`, by column position. Use `Option<T>` for a nullable
//! column.
//!
//! ```no_run
//! # #[cfg(feature = "derive")]
//! # mod example {
//! use halcyon::{named, Database, FromRow, Result};
//!
//! #[derive(FromRow)]
//! struct User {
//!     id: i64,
//!     name: String,
//!     email: Option<String>,
//! }
//!
//! fn find(db: &Database, name: &str) -> Result<Vec<User>> {
//!     db.query_as(
//!         "SELECT id, name, email FROM users WHERE name = :name",
//!         named! { "name" => name },
//!     )
//! }
//! # }
//! ```
//!
//! [`Database::query_as`] collects every row. For a large result,
//! [`Database::query`] returns a [`QueryResult`] cursor that fetches rows in
//! blocks and keeps its pooled connection until it is dropped:
//!
//! ```no_run
//! use halcyon::{Database, Result};
//!
//! fn print_all(db: &Database) -> Result<()> {
//!     let mut rows = db.query("SELECT id, name FROM users", ())?;
//!     while let Some(row) = rows.next()? {
//!         let (id, name): (i64, String) = row.get()?;
//!         println!("{id}: {name}");
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Transactions
//!
//! [`Database::transaction`] runs a closure in one transaction, committing when
//! it returns `Ok` and rolling back when it returns `Err` or panics.
//! [`ScopedTransaction::nested`] runs part of the work under a savepoint, so a
//! failure there rolls back only that part:
//!
//! ```no_run
//! use halcyon::{Database, Result};
//!
//! fn tag_order(db: &Database, order: i64, tag: &str) -> Result<()> {
//!     db.transaction(|tx| {
//!         tx.execute("UPDATE orders SET tagged = 1 WHERE id = ?", (order,))?;
//!         let insert = tx.nested(|tx| {
//!             tx.execute("INSERT INTO order_tags(order_id, tag) VALUES (?, ?)", (order, tag))
//!         });
//!         match insert {
//!             // Duplicate key: the tag already exists, so keep the update.
//!             // Other constraint failures still roll back the transaction.
//!             Err(e) if e.sqlstate == "23505" => Ok(()),
//!             other => other.map(|_| ()),
//!         }
//!     })
//! }
//! ```
//!
//! [`Database::transaction_with_isolation`] overrides the isolation level for
//! one transaction. For explicit control, [`Database::begin`] returns a
//! [`ScopedTransaction`] guard that rolls back on drop unless committed, and
//! [`ScopedTransaction::savepoint`] returns a [`Savepoint`] guard.
//!
//! # Large objects
//!
//! [`Database::execute_with_lobs`] streams BLOB and CLOB parameters in chunks
//! from [`lob_file`], [`lob_reader`], or [`lob_callback`].
//! [`Database::query_streaming`] reads rows one at a time, and
//! [`StreamingRow::lob`] reads a LOB column in chunks through a [`LobReader`]:
//!
//! ```no_run
//! use halcyon::{lob_file, BindParam, Database, Result, Value};
//!
//! fn store_and_export(db: &Database) -> Result<()> {
//!     db.execute_with_lobs(
//!         "INSERT INTO docs(id, body) VALUES (?, ?)",
//!         vec![Value::Int(1).into(), BindParam::Lob(lob_file("report.pdf"))],
//!     )?;
//!     let mut rows = db.query_streaming("SELECT body FROM docs WHERE id = ?", (1_i64,))?;
//!     while let Some(mut row) = rows.next()? {
//!         row.lob(0)?.copy_to_file("report-copy.pdf")?;
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Errors
//!
//! Every fallible call returns [`Result`], whose [`Error`] carries an
//! [`ErrorCode`], the SQLSTATE, the native SQLCODE, a message, and a
//! `retriable` flag. No public API panics. When a call reports the connection
//! lost, Halcyon retires that connection so the pool replaces it.
//!
//! # Async
//!
//! With feature `tokio`, [`AsyncDatabase`] offers the same operations as
//! `async` methods that run on Tokio's blocking thread pool.
//!
//! # Feature flags
//!
//! | Feature | Default | Enables |
//! |---|---|---|
//! | `db2` | on | Linking clidriver: `Database::connect` and `driver::Db2CliDriver`. |
//! | `derive` | on | `#[derive(FromRow)]`. |
//! | `tokio` | off | `AsyncDatabase`. |
//! | `observability` | off | `tracing` spans and `metrics` instruments. |
//! | `stress` | off | The concurrency test harness used by Halcyon's own tests. |
//!
//! # Architecture
//!
//! ```text
//! facade ─► pool ─► core ─► driver (CliDriver seam) ─► halcyon-sys ─► libdb2
//! ```
//!
//! Only `driver::Db2CliDriver` touches the CLI ABI. Everything above the
//! [`driver::CliDriver`] trait is `unsafe`-free Rust, so [`Database::open`] and
//! [`Connection::open`] accept any implementation, including a mock for tests
//! that need no database.

// The seam rule, enforced: unsafe lives only in driver::db2 (which carries an
// explicit allow) and in the halcyon-sys crate.
#![deny(unsafe_code)]
#![warn(missing_docs)]

// Lets `#[derive(FromRow)]`'s generated `::halcyon::…` paths resolve inside this
// crate's own tests, exactly as they do downstream.
extern crate self as halcyon;

mod core;
pub mod driver;
mod error;
mod facade;
mod isolation;
mod lob;
// Feature-gated async facade (tokio spawn_blocking). See async_db.rs.
#[cfg(feature = "tokio")]
mod async_db;
// Observability helpers (tracing + metrics when feature on; no-op stubs off).
mod obs;
pub mod pool;
mod retry;
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
#[allow(missing_docs)] // test harness; docs.rs does not build it
pub mod stress;

#[cfg(feature = "tokio")]
pub use async_db::AsyncDatabase;
pub use core::{
    Connection, FromColumn, FromRow, IntoParams, Named, Params, QueryMap, ResultSet, Row,
    Savepoint, Statement, StreamingResultSet, Transaction,
};
pub use error::{Error, ErrorCode, Result};
pub use facade::{Database, QueryResult, ScopedTransaction, StreamingQueryResult};
pub use isolation::Isolation;
pub use lob::{
    lob_callback, lob_file, lob_reader, owned_lob_callback, owned_lob_file, BindParam, LobReader,
    LobSource, OwnedBindParam, OwnedLobSource, StreamingRow,
};
pub use pool::{ConnectionPool, PoolConfig, PooledConnection};
pub use retry::BackoffPolicy;
pub use value::Value;

/// Derives [`FromRow`] for a struct, mapping a result-set row into its fields by
/// position (feature `derive`, on by default). Field types must implement
/// [`FromColumn`].
#[cfg(feature = "derive")]
pub use halcyon_derive::FromRow;
