//! The high-level facade: [`Database`], the ergonomic, pooled entry point.
//!
//! Ported from the C++ `Database` (`include/halcyon/database.hpp`). A `Database`
//! is a cheap, cloneable, thread-safe handle over an `Arc<ConnectionPool>`: each
//! `execute`/`query`/`query_as`/`transaction` leases a connection, runs, and
//! returns it. Reads (per [`is_read_only`](crate::retry)) are transparently
//! auto-retried on a transient error; writes run once. A connection-class failure
//! marks the lease broken so the pool discards and replaces it.
//!
//! Two surfaces carry a live lease past the call and so are owned wrappers that
//! return the connection on drop — the Rust analog of the C++ `QueryResult` and
//! `ScopedTransaction`, which the borrow checker would otherwise forbid as
//! self-references:
//!
//! * [`QueryResult`] — a streaming cursor that owns its leased connection;
//! * [`ScopedTransaction`] — a RAII transaction that owns its leased connection,
//!   committing/rolling back and returning (or discarding) it on drop.
//!
//! Because a lease holds an `Arc` on the pool internals, either wrapper may
//! safely outlive every `Database` handle.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread;

use crate::core::{
    start_with_isolation, terminate, Connection, FromRow, IntoParams, IsolationBegin, Params,
    ResultSet, Row, StmtLease,
};
use crate::driver::{CliDriver, ConnectionParams, StatementHandle};
use crate::error::{Error, ErrorCode, Result};
use crate::pool::{ConnectionPool, PoolConfig, PooledConnection};
use crate::retry::{is_read_only, ExecPolicy};
use crate::value::Value;
use crate::Isolation;

/// The ergonomic, pooled Db2 database handle. Cloneable and thread-safe: clones
/// share one underlying [`ConnectionPool`].
#[derive(Clone, Debug)]
pub struct Database {
    pool: Arc<ConnectionPool>,
}

impl Database {
    /// Opens a `Database` over `driver` with pool `config` — the injectable entry
    /// point, used by tests (against a mock `CliDriver`) and by callers supplying
    /// their own driver. The `db2`-gated `Database::connect` is the
    /// no-CLI-types form. Warms the pool to `config.min`; a warmup failure
    /// returns the error.
    pub fn open(
        driver: Arc<dyn CliDriver>,
        params: ConnectionParams,
        config: PoolConfig,
    ) -> Result<Self> {
        let pool = ConnectionPool::create(driver, params, config)?;
        Ok(Database {
            pool: Arc::new(pool),
        })
    }

    /// The underlying connection pool — for sizing metrics (`idle_count`,
    /// `active_count`, `total_count`) and manual `maintain`.
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    /// Executes a non-query statement, returning rows affected. Accepts
    /// positional (`?`) or named (`:name`) parameters. A read is auto-retried on
    /// a transient failure; a write runs once.
    pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<i64> {
        let params = params.into_params();
        let policy = self.default_policy(sql);
        crate::obs::instrument_execute(sql, || {
            self.run_with_policy(&policy, |c| c.execute(sql, params.clone()))
        })
    }

    /// Runs a streaming query, returning a [`QueryResult`] cursor that owns its
    /// leased connection until dropped. The lease is not returned to the pool
    /// while the cursor is live.
    pub fn query(&self, sql: &str, params: impl IntoParams) -> Result<QueryResult> {
        let params = params.into_params();
        let policy = self.default_policy(sql);
        crate::obs::instrument_query(sql, || self.query_with_policy(&policy, sql, params))
    }

    /// Runs a query and maps every row into `T` via [`FromRow`], collecting a
    /// `Vec` (the lease is released inside the call). Auto-retried for reads.
    pub fn query_as<T: FromRow>(&self, sql: &str, params: impl IntoParams) -> Result<Vec<T>> {
        let params = params.into_params();
        let policy = self.default_policy(sql);
        crate::obs::instrument_query(sql, || {
            self.run_with_policy(&policy, |c| c.query_as(sql, params.clone()))
        })
    }

    /// Validates and runs a column-wise batch (rectangular + type-homogeneous)
    /// through the driver's array-binding path. No auto-retry — the caller
    /// re-drives on failure; a connection-class failure discards the lease.
    pub fn execute_batch(&self, sql: &str, rows: &[Vec<Value>]) -> Result<i64> {
        crate::obs::instrument_execute(sql, || self.execute_batch_inner(sql, rows))
    }

    fn execute_batch_inner(&self, sql: &str, rows: &[Vec<Value>]) -> Result<i64> {
        let mut lease = self.pool.acquire()?;
        let result = lease.execute_batch(sql, rows);
        if let Err(ref e) = result {
            if e.code == ErrorCode::Connection {
                lease.mark_broken();
            }
        }
        result
    }

    /// Begins a transaction on a freshly leased connection, returning a
    /// [`ScopedTransaction`] that owns the lease for the unit of work. Commit or
    /// roll back explicitly; an uncommitted transaction rolls back on drop.
    pub fn begin(&self) -> Result<ScopedTransaction> {
        let mut lease = self.pool.acquire()?;
        // Flip autocommit off directly on the leased connection — the owned
        // analog of `Connection::begin`. A failure can leave the connection dead
        // or with autocommit in an unknown state, so discard it.
        if let Err(e) = lease.driver_ref().set_auto_commit(lease.handle(), false) {
            lease.mark_broken();
            return Err(e);
        }
        Ok(ScopedTransaction {
            lease,
            active: true,
            poisoned: false,
            restore_isolation: None,
            conn_broken: Cell::new(false),
        })
    }

    /// Begins a transaction at `level` on a freshly leased connection. Its
    /// configured session isolation is restored when the transaction ends.
    pub fn begin_with_isolation(&self, level: Isolation) -> Result<ScopedTransaction> {
        let mut lease = self.pool.acquire()?;
        let restore_target = lease.isolation_restore_target();
        match start_with_isolation(lease.driver_ref(), lease.handle(), level, restore_target) {
            IsolationBegin::Started => Ok(ScopedTransaction {
                lease,
                active: true,
                poisoned: false,
                restore_isolation: Some(restore_target),
                conn_broken: Cell::new(false),
            }),
            IsolationBegin::OverrideFailed(e) => Err(e),
            IsolationBegin::AutoCommitFailed(e) => {
                lease.mark_broken();
                Err(e)
            }
        }
    }

    /// Runs `f` inside a transaction: commits if it returns `Ok` (propagating a
    /// commit failure), rolls back if it returns `Err` (propagating that error),
    /// and rolls back if it panics (the [`ScopedTransaction`] drop guard fires as
    /// the panic unwinds, discarding the lease if the rollback poisoned it).
    /// Mirrors the C++ `Database::transaction`.
    pub fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&ScopedTransaction) -> Result<T>,
    {
        crate::obs::instrument_transaction(|| self.transaction_inner(f))
    }

    fn transaction_inner<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&ScopedTransaction) -> Result<T>,
    {
        let mut tx = self.begin()?;
        match f(&tx) {
            Ok(value) => {
                tx.commit()?;
                Ok(value)
            }
            Err(e) => {
                // Best-effort rollback; surface the caller's original error (as
                // C++ does). The drop guard is the panic path's safety net.
                let _ = tx.rollback();
                Err(e)
            }
        }
    }

    /// Runs `f` in a transaction at `level`, committing on `Ok` and rolling
    /// back on `Err` or an unwinding panic.
    pub fn transaction_with_isolation<T, F>(&self, level: Isolation, f: F) -> Result<T>
    where
        F: FnOnce(&ScopedTransaction) -> Result<T>,
    {
        crate::obs::instrument_transaction(|| self.transaction_with_isolation_inner(level, f))
    }

    fn transaction_with_isolation_inner<T, F>(&self, level: Isolation, f: F) -> Result<T>
    where
        F: FnOnce(&ScopedTransaction) -> Result<T>,
    {
        let mut tx = self.begin_with_isolation(level)?;
        match f(&tx) {
            Ok(value) => {
                tx.commit()?;
                Ok(value)
            }
            Err(e) => {
                let _ = tx.rollback();
                Err(e)
            }
        }
    }

    // --- retry plumbing ---

    /// Default per-call policy: read-only statements are auto-retried up to the
    /// pool's configured attempt count; writes run once. The pool's backoff is
    /// attached either way. Mirrors the C++ `default_policy_for`.
    fn default_policy(&self, sql: &str) -> ExecPolicy {
        let attempts = self.pool.backoff_policy().max_attempts.max(1);
        let mut policy = if is_read_only(sql) {
            ExecPolicy::idempotent(attempts)
        } else {
            ExecPolicy::once()
        };
        policy.backoff = self.pool.backoff_policy();
        policy
    }

    /// Runs `op` under `policy`, leasing a fresh connection each attempt. A
    /// connection-class error marks the lease broken (the pool discards it) so a
    /// retry connects anew; a non-retriable error or the last attempt returns.
    /// Mirrors the C++ `run_with_policy_on`.
    fn run_with_policy<T>(
        &self,
        policy: &ExecPolicy,
        op: impl Fn(&Connection) -> Result<T>,
    ) -> Result<T> {
        let mut last = Error::new(ErrorCode::Unknown, "no attempt made");
        let mut emitted_retry = false;
        for attempt in 1..=policy.max_attempts {
            let mut lease = self.pool.acquire()?;
            match op(&lease) {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if e.code == ErrorCode::Connection {
                        lease.mark_broken();
                    }
                    last = e;
                    if !last.retriable || attempt == policy.max_attempts {
                        // C++: exhausted only when attempts actually ran out
                        // (`attempt > 1 && attempt == maxAttempts`).
                        if emitted_retry && attempt == policy.max_attempts {
                            crate::obs::emit_retry("exhausted");
                        }
                        return Err(last);
                    }
                    crate::obs::emit_retry("retried");
                    emitted_retry = true;
                    drop(lease); // return/discard before the backoff sleep
                    thread::sleep(policy.backoff.delay_for(attempt));
                }
            }
        }
        Err(last)
    }

    /// Like [`run_with_policy`](Self::run_with_policy) but keeps the successful
    /// lease inside the returned [`QueryResult`] (its cursor borrows the leased
    /// connection). Mirrors the C++ `query_impl_on`.
    fn query_with_policy(
        &self,
        policy: &ExecPolicy,
        sql: &str,
        params: Params,
    ) -> Result<QueryResult> {
        let mut last = Error::new(ErrorCode::Unknown, "no attempt made");
        let mut emitted_retry = false;
        for attempt in 1..=policy.max_attempts {
            let mut lease = self.pool.acquire()?;
            match lease.execute_query_raw(sql, params.clone()) {
                Ok((stmt_lease, columns)) => {
                    return Ok(QueryResult::new(lease, stmt_lease, columns))
                }
                Err(e) => {
                    if e.code == ErrorCode::Connection {
                        lease.mark_broken();
                    }
                    last = e;
                    if !last.retriable || attempt == policy.max_attempts {
                        // C++: exhausted only when attempts actually ran out
                        // (`attempt > 1 && attempt == maxAttempts`).
                        if emitted_retry && attempt == policy.max_attempts {
                            crate::obs::emit_retry("exhausted");
                        }
                        return Err(last);
                    }
                    crate::obs::emit_retry("retried");
                    emitted_retry = true;
                    drop(lease);
                    thread::sleep(policy.backoff.delay_for(attempt));
                }
            }
        }
        Err(last)
    }
}

#[cfg(feature = "db2")]
impl Database {
    /// Opens a `Database` over a fresh real
    /// [`Db2CliDriver`](crate::driver::Db2CliDriver) built from `dsn`, with a
    /// default [`PoolConfig`]. The ergonomic entry point, with no CLI types in
    /// the call (the C++ `Database::open(dsn)` analog).
    pub fn connect(dsn: &str) -> Result<Self> {
        Database::connect_with(dsn, PoolConfig::default())
    }

    /// Like [`connect`](Self::connect) but with an explicit [`PoolConfig`].
    pub fn connect_with(dsn: &str, config: PoolConfig) -> Result<Self> {
        let driver = Arc::new(crate::driver::Db2CliDriver::new());
        Database::open(
            driver,
            ConnectionParams {
                connection_string: dsn.to_string(),
                isolation: None,
            },
            config,
        )
    }
}

#[cfg(feature = "tokio")]
impl Database {
    /// Wraps this handle in an [`AsyncDatabase`](crate::AsyncDatabase) — the
    /// feature-gated `tokio` async facade. Cheap (moves the `Arc`-backed handle).
    pub fn into_async(self) -> crate::AsyncDatabase {
        crate::AsyncDatabase::new(self)
    }
}

/// A streaming query cursor that owns the pooled connection it runs on. Drive it
/// with [`next`](Self::next) (a lending cursor — each [`Row`] borrows the set) or
/// materialize with [`collect_rows`](Self::collect_rows). On drop it finalizes the
/// statement and returns the connection to the pool — or discards it if the scan
/// died on a connection-class error. The Rust analog of the C++ `QueryResult`.
pub struct QueryResult {
    lease: PooledConnection,
    /// The statement lease, released back to the connection's cache on drop.
    /// `Option` so `Drop` can move it out.
    stmt_lease: Option<StmtLease>,
    /// Copied handle for fetch/read calls (infallible access).
    stmt: StatementHandle,
    columns: usize,
    // The cursor's poison slot: a driver-side read failure is recorded here (as
    // in `ResultSet`), and it decides whether the lease is discarded on drop.
    error: RefCell<Option<Error>>,
    done: bool,
}

impl QueryResult {
    fn new(lease: PooledConnection, stmt_lease: StmtLease, columns: usize) -> Self {
        let stmt = stmt_lease.handle();
        QueryResult {
            lease,
            stmt_lease: Some(stmt_lease),
            stmt,
            columns,
            error: RefCell::new(None),
            done: false,
        }
    }

    pub fn column_count(&self) -> usize {
        self.columns
    }

    /// The driver-side failure that spoiled the scan, if any — the signal used to
    /// discard the leased connection on drop. Empty after a clean end.
    pub fn error(&self) -> Option<Error> {
        self.error.borrow().clone()
    }

    /// Advances to the next row. `Ok(Some(row))` while rows remain, `Ok(None)` at
    /// a clean end, `Err` on a mid-stream fetch failure (also retained in
    /// [`error`](Self::error)). Once it yields `None`/`Err` it stays done.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Row<'_>>> {
        if self.done {
            return Ok(None);
        }
        let stmt = self.stmt;
        match self.lease.driver_ref().fetch(stmt) {
            Ok(true) => Ok(Some(Row::new(
                self.lease.driver_ref(),
                stmt,
                self.columns,
                &self.error,
            ))),
            Ok(false) => {
                self.done = true;
                Ok(None)
            }
            Err(e) => {
                self.done = true;
                let mut slot = self.error.borrow_mut();
                if slot.is_none() {
                    *slot = Some(e.clone());
                }
                drop(slot);
                Err(e)
            }
        }
    }

    /// Materializes all remaining rows via [`FromRow`]. A mid-stream error (fetch
    /// or mapping) short-circuits to `Err`.
    pub fn collect_rows<T: FromRow>(&mut self) -> Result<Vec<T>> {
        let mut out = Vec::new();
        while let Some(row) = self.next()? {
            out.push(T::from_row(&row)?);
        }
        Ok(out)
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        // Release the statement back to the leased connection's cache, then
        // decide the lease's fate: a mid-stream connection-class failure means
        // the connection is dead, so discard it instead of pooling it.
        if let Some(stmt_lease) = self.stmt_lease.take() {
            // Any driver-side scan failure poisons the statement so its cache
            // entry is dropped; a connection-class one additionally discards
            // the whole connection below.
            if self.error.borrow().is_some() {
                stmt_lease.poison();
            }
            self.lease.release_stmt(stmt_lease);
        }
        let broken = self
            .error
            .borrow()
            .as_ref()
            .is_some_and(|e| e.code == ErrorCode::Connection);
        if broken {
            self.lease.mark_broken();
        }
        // `lease` drops after this → returns or discards the connection.
    }
}

impl fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryResult")
            .field("stmt", &self.stmt)
            .field("columns", &self.columns)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

/// A transaction that owns the pooled connection it runs on, returned by
/// [`Database::begin`]. Autocommit is off for its lifetime; commit/rollback end
/// it and restore autocommit. An uncommitted transaction rolls back on drop, and
/// a poisoned terminal op discards the lease. The Rust analog of the C++
/// `ScopedTransaction`.
pub struct ScopedTransaction {
    lease: PooledConnection,
    active: bool,
    poisoned: bool,
    restore_isolation: Option<Isolation>,
    // Set when a statement run *inside* the unit of work dies on a
    // connection-class error — from a materializing call here or from a streaming
    // [`TxCursor`] that spoiled mid-scan. The terminal op's own poison
    // ([`poisoned`](Self::poisoned)) is a separate signal; either one discards the
    // lease on drop. Interior-mutable because the query/execute methods (and the
    // cursor that borrows this slot) run behind `&self`, while `mark_broken` needs
    // `&mut` and so waits for drop. Fixes a gap the C++ reference still carries:
    // its `ScopedTransaction` consults only the terminal op's poison, so a cursor
    // that died mid-scan while commit/rollback succeeded would recycle a dead
    // connection.
    conn_broken: Cell<bool>,
}

impl ScopedTransaction {
    /// True until committed or rolled back (including the implicit rollback on
    /// drop).
    pub fn active(&self) -> bool {
        self.active
    }

    /// True if a terminal op (or the drop rollback) failed, leaving the
    /// connection dead or autocommit unknown — the lease is then discarded.
    pub fn poisoned(&self) -> bool {
        self.poisoned
    }

    /// Executes a non-query statement within the unit of work.
    pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<i64> {
        let params = params.into_params();
        crate::obs::instrument_execute(sql, move || self.note(self.lease.execute(sql, params)))
    }

    /// Runs a streaming query within the unit of work, returning a [`TxCursor`]
    /// that borrows the transaction (so it must be finished — dropped — before
    /// commit/rollback). A connection-class failure while scanning the cursor
    /// discards the leased connection on transaction drop, exactly as the pooled
    /// [`QueryResult`] does outside a transaction.
    pub fn query(&self, sql: &str, params: impl IntoParams) -> Result<TxCursor<'_>> {
        let params = params.into_params();
        crate::obs::instrument_query(sql, move || {
            let rows = self.note(self.lease.query(sql, params))?;
            Ok(TxCursor {
                rows,
                broken: &self.conn_broken,
            })
        })
    }

    /// Runs a query and maps every row into `T`.
    pub fn query_as<T: FromRow>(&self, sql: &str, params: impl IntoParams) -> Result<Vec<T>> {
        let params = params.into_params();
        crate::obs::instrument_query(sql, move || self.note(self.lease.query_as(sql, params)))
    }

    /// Batch insert within the unit of work.
    pub fn execute_batch(&self, sql: &str, rows: &[Vec<Value>]) -> Result<i64> {
        crate::obs::instrument_execute(sql, || self.note(self.lease.execute_batch(sql, rows)))
    }

    /// Flags the unit of work's connection broken if `result` is a
    /// connection-class error, then passes the result through unchanged. Applied to
    /// every materializing call so a dead connection surfaced inside the
    /// transaction discards the lease on drop.
    fn note<T>(&self, result: Result<T>) -> Result<T> {
        if let Err(ref e) = result {
            if e.code == ErrorCode::Connection {
                self.conn_broken.set(true);
            }
        }
        result
    }

    /// Commits the unit of work and restores autocommit. A no-op (`Ok`) if
    /// already ended; on failure the transaction is poisoned.
    pub fn commit(&mut self) -> Result<()> {
        self.finish(true)
    }

    /// Rolls the unit of work back and restores autocommit. A no-op (`Ok`) if
    /// already ended; on failure the transaction is poisoned.
    pub fn rollback(&mut self) -> Result<()> {
        self.finish(false)
    }

    fn finish(&mut self, commit: bool) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        let (result, poisoned) = terminate(
            self.lease.driver_ref(),
            self.lease.handle(),
            commit,
            self.restore_isolation,
        );
        self.active = false;
        if poisoned {
            self.poisoned = true;
        }
        result
    }
}

impl Drop for ScopedTransaction {
    fn drop(&mut self) {
        // Drive the implicit rollback here so we can see its outcome before the
        // lease is returned: a poisoned terminal op *or* a connection-class failure
        // recorded inside the unit of work (see `conn_broken`) discards the
        // connection.
        if self.active {
            let _ = self.finish(false);
        }
        if self.poisoned || self.conn_broken.get() {
            self.lease.mark_broken();
        }
    }
}

impl fmt::Debug for ScopedTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedTransaction")
            .field("active", &self.active)
            .field("poisoned", &self.poisoned)
            .field("conn_broken", &self.conn_broken.get())
            .finish_non_exhaustive()
    }
}

/// A streaming cursor for a query run inside a [`ScopedTransaction`]. Borrows the
/// transaction (so it must be dropped before commit/rollback) and its core
/// [`ResultSet`] runs on the transaction's leased connection. On drop it reports a
/// connection-class scan failure back to the transaction, so a connection that
/// died mid-scan is discarded rather than recycled — the transaction-scoped analog
/// of the pooled [`QueryResult`]. Drive it with [`next`](Self::next) or
/// materialize with [`collect_rows`](Self::collect_rows).
pub struct TxCursor<'t> {
    rows: ResultSet<'t>,
    // The owning transaction's `conn_broken` slot; poisoned on drop if the scan
    // died on a connection-class error.
    broken: &'t Cell<bool>,
}

impl<'t> TxCursor<'t> {
    pub fn column_count(&self) -> usize {
        self.rows.column_count()
    }

    /// The driver-side failure that spoiled the scan, if any — empty after a clean
    /// end. A connection-class error here discards the leased connection when the
    /// transaction is dropped.
    pub fn error(&self) -> Option<Error> {
        self.rows.error()
    }

    /// Advances to the next row. `Ok(Some(row))` while rows remain, `Ok(None)` at a
    /// clean end, `Err` on a mid-stream fetch failure (also retained in
    /// [`error`](Self::error)).
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Row<'_>>> {
        self.rows.next()
    }

    /// Materializes all remaining rows via [`FromRow`]. A mid-stream error (fetch
    /// or mapping) short-circuits to `Err`.
    pub fn collect_rows<T: FromRow>(&mut self) -> Result<Vec<T>> {
        self.rows.collect_rows()
    }

    /// Adapts the cursor into an `Iterator` of owned `Result<T>`, mapping each row
    /// with `f` — the transaction-scoped analog of [`ResultSet::query_map`], for
    /// composing with `for`/`.collect()` despite the lending cursor underneath. The
    /// returned [`TxQueryMap`] owns this cursor, so the same connection-class
    /// poison-on-drop still discards a connection that died mid-scan.
    pub fn query_map<T, F>(self, f: F) -> TxQueryMap<'t, T, F>
    where
        F: FnMut(&Row<'_>) -> Result<T>,
    {
        TxQueryMap {
            cursor: self,
            f,
            _out: PhantomData,
        }
    }
}

impl Drop for TxCursor<'_> {
    fn drop(&mut self) {
        // A connection-class scan failure means the leased connection is dead; flag
        // it so the transaction discards the lease instead of returning it. `rows`
        // finalizes its statement when it drops after this.
        if self
            .rows
            .error()
            .is_some_and(|e| e.code == ErrorCode::Connection)
        {
            self.broken.set(true);
        }
    }
}

impl fmt::Debug for TxCursor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxCursor")
            .field("columns", &self.rows.column_count())
            .finish_non_exhaustive()
    }
}

/// Owned-item iterator produced by [`TxCursor::query_map`]. The
/// transaction-scoped analog of [`QueryMap`](crate::QueryMap): it owns the
/// [`TxCursor`], so dropping it runs the cursor's poison-on-drop and discards a
/// connection that died mid-scan.
pub struct TxQueryMap<'t, T, F> {
    cursor: TxCursor<'t>,
    f: F,
    _out: PhantomData<fn() -> T>,
}

impl<T, F> Iterator for TxQueryMap<'_, T, F>
where
    F: FnMut(&Row<'_>) -> Result<T>,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor.next() {
            Ok(Some(row)) => Some((self.f)(&row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<T, F> fmt::Debug for TxQueryMap<'_, T, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxQueryMap")
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, ErrorCode};
    use crate::named;
    use crate::retry::BackoffPolicy;
    use crate::testing::{MockCliDriver, ScriptedRows};
    use std::time::Duration;

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "DSN=mock".into(),
            isolation: None,
        }
    }

    /// A deterministic single-thread pool config: min=1, generous acquire, no
    /// reaper thread, zero backoff, and 3 attempts (so reads auto-retry instantly).
    fn config() -> PoolConfig {
        PoolConfig {
            min: 1,
            max: 4,
            acquire_timeout: Duration::from_millis(500),
            idle_timeout: Duration::from_secs(3600),
            max_lifetime: Duration::from_secs(3600),
            validate_on_acquire: false,
            backoff: BackoffPolicy {
                base_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
                max_attempts: 3,
            },
            maintenance_interval: Duration::from_secs(3600),
            start_maintenance_thread: false,
            statement_cache_size: 0,
            isolation: None,
        }
    }

    /// A single-connection pool with the statement cache on, so cache hits can
    /// be asserted across pooled calls (every lease is the same connection).
    fn cached_config() -> PoolConfig {
        PoolConfig {
            min: 1,
            max: 1,
            statement_cache_size: 8,
            ..config()
        }
    }

    fn open(mock: &Arc<MockCliDriver>) -> Database {
        Database::open(mock.clone(), params(), config()).unwrap()
    }

    fn retriable(code: ErrorCode, msg: &str) -> Error {
        let mut e = Error::new(code, msg);
        e.retriable = true;
        e
    }

    #[test]
    fn pooled_facade_reuses_cached_statement_across_leases() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(7)]]));
        mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(7)]]));
        let db = Database::open(mock.clone(), params(), cached_config()).unwrap();
        for _ in 0..2 {
            let rows: Vec<(i64,)> = db.query_as("SELECT id FROM t", ()).unwrap();
            assert_eq!(rows, vec![(7,)]);
        }
        assert_eq!(mock.prepared_sql().len(), 1); // second call hit the cache
        assert_eq!(mock.close_cursor_calls(), 2); // QueryResult released cleanly
    }

    #[test]
    fn read_retry_after_transient_error_reprepares_the_poisoned_statement() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_execute_error(retriable(ErrorCode::Deadlock, "40001"));
        mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(7)]]));
        let db = Database::open(mock.clone(), params(), cached_config()).unwrap();
        let rows: Vec<(i64,)> = db.query_as("SELECT id FROM t", ()).unwrap();
        assert_eq!(rows, vec![(7,)]);
        // Attempt 1 prepared + failed (entry poisoned and dropped); the replay
        // re-acquired the same connection and re-prepared.
        assert_eq!(mock.prepared_sql().len(), 2);
        assert_eq!(mock.finalize_calls(), 1); // exactly the poisoned statement
    }

    #[test]
    fn open_warms_up_the_pool() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        assert_eq!(mock.connect_calls(), 1); // min=1
        assert_eq!(db.pool().idle_count(), 1);
    }

    #[test]
    fn execute_returns_row_count() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_exec_row_count(3);
        let db = open(&mock);
        let n = db
            .execute("UPDATE t SET a = ? WHERE id = ?", (1, 5))
            .unwrap();
        assert_eq!(n, 3);
        // Lease returned to the pool after the call.
        assert_eq!(db.pool().idle_count(), 1);
        assert_eq!(db.pool().active_count(), 0);
    }

    #[test]
    fn query_iterates_rows_over_a_lease() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["id"],
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(4)],
            ],
        ));
        let db = open(&mock);
        {
            let mut rs = db.query("SELECT id FROM t WHERE id > ?", (0,)).unwrap();
            // The cursor holds the lease while live.
            assert_eq!(db.pool().active_count(), 1);
            let mut sum = 0;
            while let Some(row) = rs.next().unwrap() {
                sum += row.column::<i64>(0).unwrap();
            }
            assert_eq!(sum, 7);
        } // cursor drops → lease returned
        assert_eq!(db.pool().active_count(), 0);
        assert_eq!(db.pool().idle_count(), 1);
    }

    #[test]
    fn query_as_maps_tuples() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["id", "name"],
            vec![
                vec![Value::Int(1), Value::Text("ann".into())],
                vec![Value::Int(2), Value::Text("bob".into())],
            ],
        ));
        let db = open(&mock);
        let people: Vec<(i64, String)> = db.query_as("SELECT id, name FROM people", ()).unwrap();
        assert_eq!(people.len(), 2);
        assert_eq!(people[1], (2, "bob".to_string()));
    }

    #[test]
    fn named_params_through_facade() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["id", "name"],
            vec![vec![Value::Int(7), Value::Text("z".into())]],
        ));
        let db = open(&mock);
        let mut rs = db
            .query(
                "SELECT id, name FROM t WHERE id = :id",
                named! { "id" => 7 },
            )
            .unwrap();
        let mut rows = 0;
        while rs.next().unwrap().is_some() {
            rows += 1;
        }
        assert_eq!(rows, 1);
        // The facade rewrote the named placeholder to positional before prepare.
        assert_eq!(
            mock.prepared_sql(),
            vec!["SELECT id, name FROM t WHERE id = ?"]
        );
    }

    #[test]
    fn execute_batch_delegates_to_connection() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let rows = vec![
            vec![Value::Int(1), Value::Text("a".into())],
            vec![Value::Int(2), Value::Text("b".into())],
        ];
        let affected = db
            .execute_batch("INSERT INTO t VALUES (?, ?)", &rows)
            .unwrap();
        assert_eq!(affected, 2);
        assert_eq!(mock.execute_batch_calls(), 1);
        assert_eq!(mock.last_batch_rows(), rows);
    }

    #[test]
    fn transaction_commits_on_ok() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let out = db.transaction(|tx| -> Result<i32> {
            tx.execute("INSERT INTO t VALUES (?)", (1,))?;
            Ok(99)
        });
        assert_eq!(out.unwrap(), 99);
        assert_eq!(mock.commit_calls(), 1);
        assert_eq!(mock.rollback_calls(), 0);
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
        assert_eq!(db.pool().idle_count(), 1); // lease returned
    }

    #[test]
    fn transaction_rolls_back_on_err() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let out: Result<i32> = db.transaction(|_tx| Err(Error::new(ErrorCode::Constraint, "dup")));
        assert_eq!(out.unwrap_err().code, ErrorCode::Constraint);
        assert_eq!(mock.commit_calls(), 0);
        assert_eq!(mock.rollback_calls(), 1);
    }

    #[test]
    fn transaction_rolls_back_on_panic() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);

        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = catch_unwind(AssertUnwindSafe(|| {
            db.transaction(|_tx| -> Result<()> { panic!("boom") })
        }));
        std::panic::set_hook(prev);

        assert!(result.is_err());
        assert_eq!(mock.commit_calls(), 0);
        assert_eq!(mock.rollback_calls(), 1);
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
    }

    #[test]
    fn begin_returns_usable_scoped_transaction() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let mut tx = db.begin().unwrap();
        tx.execute("UPDATE t SET a = ?", (1,)).unwrap();
        tx.commit().unwrap();
        assert_eq!(mock.commit_calls(), 1);
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
    }

    #[test]
    fn transaction_with_isolation_restores_after_commit() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);

        let result = db.transaction_with_isolation(crate::Isolation::UncommittedRead, |_tx| {
            Ok::<_, Error>(42)
        });

        assert_eq!(result.unwrap(), 42);
        assert_eq!(
            mock.isolation_calls(),
            vec![
                crate::Isolation::UncommittedRead,
                crate::Isolation::CursorStability
            ]
        );
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
        assert_eq!(mock.commit_calls(), 1);
        assert_eq!(db.pool().idle_count(), 1);
    }

    #[test]
    fn pooled_begin_compensation_failure_marks_lease_broken() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        mock.push_txn_error(Error::new(
            ErrorCode::Connection,
            "autocommit disabled failed",
        ));
        mock.fail_isolation_on_call(
            2,
            Error::new(ErrorCode::Connection, "isolation restore failed"),
        );

        let error = db
            .begin_with_isolation(crate::Isolation::UncommittedRead)
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Connection);
        assert_eq!(db.pool().total_count(), 0);
        assert_eq!(db.pool().idle_count(), 0);
        assert_eq!(db.pool().active_count(), 0);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn read_auto_retries_after_a_transient_connection_error() {
        let mock = Arc::new(MockCliDriver::new());
        // First execute of the SELECT dies with a retriable connection error;
        // the retry (on a fresh connection) succeeds.
        mock.push_execute_error(retriable(ErrorCode::Connection, "dropped"));
        mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(42)]]));
        let db = open(&mock);
        let rows: Vec<(i64,)> = db.query_as("SELECT id FROM t", ()).unwrap();
        assert_eq!(rows, vec![(42,)]);
        // The dead lease was discarded and the retry connected anew.
        assert_eq!(mock.connect_calls(), 2); // warmup + reconnect
        assert_eq!(mock.disconnect_calls(), 1); // the broken one
    }

    #[test]
    fn write_does_not_auto_retry() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_execute_error(retriable(ErrorCode::Connection, "dropped"));
        let db = open(&mock);
        let err = db.execute("INSERT INTO t VALUES (?)", (1,)).unwrap_err();
        assert_eq!(err.code, ErrorCode::Connection);
        // Ran once (a write): no reconnect, and the dead lease was discarded.
        assert_eq!(mock.connect_calls(), 1);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn non_retriable_read_error_is_not_retried() {
        let mock = Arc::new(MockCliDriver::new());
        // A syntax error is not retriable even though a SELECT is idempotent.
        mock.push_execute_error(Error::new(ErrorCode::Syntax, "42601"));
        let db = open(&mock);
        let err = db.query_as::<(i64,)>("SELECT id FROM t", ()).unwrap_err();
        assert_eq!(err.code, ErrorCode::Syntax);
        // Exactly one prepare/execute attempt (no retry), and the connection was
        // not a connection-class failure, so it is kept.
        assert_eq!(mock.prepared_sql().len(), 1);
        assert_eq!(mock.disconnect_calls(), 0);
    }

    #[test]
    fn query_result_discards_lease_after_midstream_connection_drop() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["id"],
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ));
        // The second fetch drops the connection.
        mock.fail_fetch_on_call(2, Error::new(ErrorCode::Connection, "dropped mid-scan"));
        let db = open(&mock);
        {
            let mut rs = db.query("SELECT id FROM t", ()).unwrap();
            assert!(rs.next().unwrap().is_some()); // row 1
            let err = rs.next().unwrap_err(); // fetch 2 fails
            assert_eq!(err.code, ErrorCode::Connection);
            assert_eq!(rs.error().map(|e| e.code), Some(ErrorCode::Connection));
        } // cursor drops → lease discarded (not returned)
        assert_eq!(db.pool().idle_count(), 0);
        assert_eq!(db.pool().total_count(), 0);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn transaction_streaming_query_discards_lease_after_midstream_connection_drop() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["id"],
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ));
        // The second fetch inside the transaction drops the connection.
        mock.fail_fetch_on_call(2, Error::new(ErrorCode::Connection, "dropped mid-scan"));
        let db = open(&mock);
        let out: Result<()> = db.transaction(|tx| {
            let mut rs = tx.query("SELECT id FROM t", ())?;
            while rs.next()?.is_some() {}
            Ok(())
        });
        assert_eq!(out.unwrap_err().code, ErrorCode::Connection);
        // The connection died mid-scan, so it must be discarded — even though the
        // rollback on the (mock-)still-responsive handle did not itself poison the
        // transaction. Without threading the cursor's poison into the lease, the
        // dead connection would be returned to the pool.
        assert_eq!(db.pool().idle_count(), 0);
        assert_eq!(db.pool().total_count(), 0);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn transaction_query_as_discards_lease_after_connection_error() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(1)]]));
        // The materializing read fails with a connection-class error.
        mock.fail_fetch_on_call(1, Error::new(ErrorCode::Connection, "dropped"));
        let db = open(&mock);
        let out: Result<Vec<(i64,)>> = db.transaction(|tx| tx.query_as("SELECT id FROM t", ()));
        assert_eq!(out.unwrap_err().code, ErrorCode::Connection);
        // Same discard rule as the streaming case, via the materializing path.
        assert_eq!(db.pool().idle_count(), 0);
        assert_eq!(db.pool().total_count(), 0);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn transaction_query_map_iterates_owned_items() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["id"],
            vec![vec![Value::Int(2)], vec![Value::Int(5)]],
        ));
        let db = open(&mock);
        let ids = db
            .transaction(|tx| {
                tx.query("SELECT id FROM t", ())?
                    .query_map(|row| row.column::<i64>(0))
                    .collect::<Result<Vec<i64>>>()
            })
            .unwrap();
        assert_eq!(ids, vec![2, 5]);
        assert_eq!(db.pool().idle_count(), 1); // clean end → lease returned
    }

    #[test]
    fn transaction_query_map_discards_lease_after_midstream_connection_drop() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["id"],
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ));
        mock.fail_fetch_on_call(2, Error::new(ErrorCode::Connection, "dropped mid-scan"));
        let db = open(&mock);
        let out = db.transaction(|tx| {
            tx.query("SELECT id FROM t", ())?
                .query_map(|row| row.column::<i64>(0))
                .collect::<Result<Vec<i64>>>()
        });
        assert_eq!(out.unwrap_err().code, ErrorCode::Connection);
        // Poison flows through TxQueryMap → the owned TxCursor → drop → discard.
        assert_eq!(db.pool().idle_count(), 0);
        assert_eq!(db.pool().total_count(), 0);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn database_is_send_sync_and_clone() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Database>();
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let clone = db.clone();
        // Both handles share one pool.
        let _l = db.pool().acquire().unwrap();
        assert_eq!(clone.pool().active_count(), 1);
    }
}
