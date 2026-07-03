//! The driver seam: everything above this module is portable, mockable Rust.
//!
//! [`CliDriver`] is the Rust counterpart of the C++ `ICliDriver` interface —
//! the single boundary behind which all IBM CLI details live. The only real
//! implementation is [`Db2CliDriver`] (feature `db2`); tests substitute their
//! own implementations and need no live database.

// Gated because its only non-test caller today is Db2CliDriver; lift the cfg
// when the core layer starts validating batches above the seam (as in C++).
#[cfg(any(feature = "db2", test))]
pub(crate) mod batch;
mod sqlstate;
pub use sqlstate::{classify_sqlstate, Classification};

#[cfg(feature = "db2")]
#[allow(unsafe_code)]
mod db2;
#[cfg(feature = "db2")]
pub use db2::Db2CliDriver;

use crate::error::Result;
use crate::value::Value;

/// Opaque token for a driver-owned connection. `0` is never a live handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionHandle(pub u64);

impl ConnectionHandle {
    pub const INVALID: ConnectionHandle = ConnectionHandle(0);

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }
}

/// Opaque token for a driver-owned prepared statement. `0` is never live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatementHandle(pub u64);

impl StatementHandle {
    pub const INVALID: StatementHandle = StatementHandle(0);

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionParams {
    pub connection_string: String,
}

/// The mockable seam over the IBM Db2 CLI.
///
/// Implementations are shared across the pool and its worker threads, hence
/// `Send + Sync` with `&self` methods. Halcyon guarantees a single owner per
/// leased connection: a given connection (and any statement prepared on it) is
/// driven by at most one thread at a time, though different connections run
/// concurrently through the same driver.
pub trait CliDriver: Send + Sync {
    // --- Connection lifecycle ---
    fn connect(&self, params: &ConnectionParams) -> Result<ConnectionHandle>;
    fn disconnect(&self, conn: ConnectionHandle) -> Result<()>;
    fn is_alive(&self, conn: ConnectionHandle) -> Result<bool>;

    // --- Prepared-statement data path ---

    /// Prepares `sql` on `conn`; returns a valid statement handle on success.
    fn prepare(&self, conn: ConnectionHandle, sql: &str) -> Result<StatementHandle>;

    /// Binds positional parameters (index 0 == first `?`) before execute.
    fn bind_params(&self, stmt: StatementHandle, params: &[Value]) -> Result<()>;

    /// Executes the statement. Returns rows affected for DML (>= 0); for a
    /// result-set-producing statement the count is implementation-defined (0)
    /// and the cursor becomes fetchable.
    fn execute(&self, stmt: StatementHandle) -> Result<i64>;

    /// Binds `rows` as a column-wise parameter array and executes via array
    /// binding, chunking internally to bound memory. Returns total rows
    /// affected, summed across chunks. `rows` must be rectangular with each
    /// column type-homogeneous (NULLs match any type); implementations must
    /// reject violations with a `Mapping` error rather than trust the caller.
    /// Each chunk is atomic; the failing row index is not recoverable from the
    /// driver.
    fn execute_batch(&self, stmt: StatementHandle, rows: &[Vec<Value>]) -> Result<i64>;

    /// Number of columns in the active result set (0 when none).
    fn column_count(&self, stmt: StatementHandle) -> Result<usize>;

    /// Name of the 0-based column (may be empty for unnamed expressions).
    fn column_name(&self, stmt: StatementHandle, index: usize) -> Result<String>;

    /// Advances the cursor. `true` => a row is available; `false` => end.
    fn fetch(&self, stmt: StatementHandle) -> Result<bool>;

    /// Reads the current row's 0-based column as a neutral [`Value`].
    fn get_column(&self, stmt: StatementHandle, index: usize) -> Result<Value>;

    /// Releases a statement handle. Idempotent for already-finalized handles.
    fn finalize(&self, stmt: StatementHandle) -> Result<()>;

    /// Closes any open cursor on `stmt` and resets it so it can be re-bound
    /// and re-executed. Idempotent when no cursor is open. Required to reuse a
    /// cached prepared statement that previously produced a result set.
    fn close_cursor(&self, stmt: StatementHandle) -> Result<()>;

    // --- Transaction control ---

    /// Enables/disables autocommit on the connection. A transaction begins by
    /// disabling it and ends (commit/rollback) by re-enabling it.
    fn set_auto_commit(&self, conn: ConnectionHandle, enabled: bool) -> Result<()>;

    fn commit(&self, conn: ConnectionHandle) -> Result<()>;
    fn rollback(&self, conn: ConnectionHandle) -> Result<()>;
}
