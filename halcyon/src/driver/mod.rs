//! The driver seam: everything above this module is portable, mockable Rust.
//!
//! [`CliDriver`] is the Rust counterpart of the C++ `ICliDriver` interface —
//! the single boundary behind which all IBM CLI details live. The only real
//! implementation is `Db2CliDriver` (feature `db2`); tests substitute their
//! own implementations and need no live database.

// The core layer validates batches above the seam (as the C++
// `Connection::executeBatch` does), and `Db2CliDriver` re-checks before any
// unsafe buffer work — so this is always compiled.
pub(crate) mod batch;
mod sqlstate;
pub use sqlstate::{classify_sqlstate, Classification};

// `db2` is the one module permitted to use `unsafe`; the crate-wide
// `#![deny(unsafe_code)]` exception is a module-scoped inner attribute *inside*
// `db2.rs` itself, so the seam waiver lives at the seam (per the project rule).
#[cfg(feature = "db2")]
mod db2;
#[cfg(feature = "db2")]
pub use db2::Db2CliDriver;

use crate::error::Result;
use crate::value::Value;
use crate::Isolation;

/// Result of one chunked LOB read. `bytes == 0 && done` is clean EOF.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GetDataChunk {
    /// Bytes written to the caller's buffer.
    pub bytes: usize,
    /// No more data remains in the cell after this chunk.
    pub done: bool,
    /// The cell is SQL NULL (then `bytes == 0` and `done`).
    pub is_null: bool,
}

/// Pull callback behind a [`ParamStreamSource`]: fills the buffer and returns
/// the number of bytes written, with `0` meaning the stream is finished.
pub type ParamStreamPull<'a> = dyn FnMut(&mut [u8]) -> Result<usize> + 'a;

/// One data-at-execution parameter stream for [`CliDriver::execute_streaming`].
pub struct ParamStreamSource<'a> {
    /// 0-based position of the `?` placeholder this stream feeds.
    pub param_index: usize,
    /// Supplies the parameter's bytes chunk by chunk.
    pub pull: Box<ParamStreamPull<'a>>,
    /// Total length in bytes when known up front; `None` streams with an
    /// unknown length.
    pub size_hint: Option<u64>,
    /// Bind as character data (`CLOB`) instead of binary (`BLOB`).
    pub is_clob: bool,
}

/// Opaque token for a driver-owned connection. `0` is never a live handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionHandle(pub u64);

impl ConnectionHandle {
    /// The handle that never refers to a live connection.
    pub const INVALID: ConnectionHandle = ConnectionHandle(0);

    /// Whether this is not [`INVALID`](Self::INVALID).
    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }
}

/// Opaque token for a driver-owned prepared statement. `0` is never live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatementHandle(pub u64);

impl StatementHandle {
    /// The handle that never refers to a live statement.
    pub const INVALID: StatementHandle = StatementHandle(0);

    /// Whether this is not [`INVALID`](Self::INVALID).
    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }
}

/// What [`CliDriver::connect`] needs to open one connection.
#[derive(Debug, Clone)]
pub struct ConnectionParams {
    /// CLI connection string, such as
    /// `DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=...;PWD=...;`.
    pub connection_string: String,
    /// Session isolation level for a standalone `Connection::open`; `None`
    /// preserves the server default. Pooled connections take theirs from
    /// `PoolConfig::isolation`.
    pub isolation: Option<Isolation>,
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

    /// Opens a connection; returns a valid handle on success.
    fn connect(&self, params: &ConnectionParams) -> Result<ConnectionHandle>;

    /// Closes the connection and releases its handle.
    fn disconnect(&self, conn: ConnectionHandle) -> Result<()>;

    /// Probes whether `conn` can still run statements. The pool's validation
    /// check; `Ok(false)` means the connection should be replaced.
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

    /// Fetches up to `max_rows` rows from the open cursor as row-major neutral
    /// [`Value`]s. A non-empty (including short) block is success; an empty
    /// block means the cursor is exhausted. Driver failures are classified and
    /// returned as errors.
    fn fetch_block(&self, stmt: StatementHandle, max_rows: usize) -> Result<Vec<Vec<Value>>>;

    /// Positions a dedicated row-at-a-time streaming cursor on its next row.
    fn fetch_next(&self, stmt: StatementHandle) -> Result<bool>;

    /// Reads one scalar cell on the current streaming row.
    fn get_value(&self, stmt: StatementHandle, column: usize) -> Result<Value>;

    /// Reads one chunk of a cell on the current streaming row.
    fn get_data_chunk(
        &self,
        stmt: StatementHandle,
        column: usize,
        buffer: &mut [u8],
    ) -> Result<GetDataChunk>;

    /// Binds ordinary values and data-at-execution streams, then executes once.
    fn execute_streaming(
        &self,
        stmt: StatementHandle,
        params: &[Value],
        sources: Vec<ParamStreamSource<'_>>,
    ) -> Result<i64>;

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

    /// Sets the connection's transaction isolation attribute.
    fn set_isolation(&self, conn: ConnectionHandle, level: Isolation) -> Result<()>;

    /// Commits the connection's current unit of work.
    fn commit(&self, conn: ConnectionHandle) -> Result<()>;

    /// Rolls back the connection's current unit of work.
    fn rollback(&self, conn: ConnectionHandle) -> Result<()>;
}
