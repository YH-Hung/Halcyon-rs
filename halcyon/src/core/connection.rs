//! `Connection` and `Statement`: the RAII entry points to the core data path.
//!
//! Ported from the C++ `Connection`/`Statement`. A [`Connection`] owns a driver
//! connection handle and disconnects on drop; a [`Statement`] owns a prepared
//! statement handle and finalizes on drop. Both are move-only. `Statement<'c>`
//! borrows its `Connection`, so the borrow checker guarantees statements are
//! finalized before the connection is disconnected — the ordering the C++
//! `Connection::reset` enforces by hand.
//!
//! Statements are leased from the per-connection statement cache (C++
//! `StatementCache`, Phase 5): with a nonzero capacity
//! ([`Connection::open_with_cache`]), repeated SQL reuses its prepared handle;
//! at capacity 0 (the plain [`Connection::open`] default) every lease is
//! transient — prepared and finalized per call.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::core::mapping;
use crate::core::params::{bind_named, IntoParams, Params};
use crate::core::result_set::{FromRow, ResultSet};
use crate::core::{StatementCache, StmtLease, StreamingResultSet};
use crate::driver::batch::validate_batch_rows;
use crate::driver::{CliDriver, ConnectionHandle, ConnectionParams, StatementHandle};
use crate::error::{Error, ErrorCode, Result};
use crate::lob::BindParam;
use crate::value::Value;
use crate::Isolation;

/// SQLSTATE 40003: the connection was lost, so the statement's completion is
/// unknown. IBM documents it as a communication-link failure.
const COMPLETION_UNKNOWN: &str = "40003";

/// A single logical Db2 connection over the CLI seam.
///
/// Owns the physical connection handle and disconnects on drop. Move-only.
///
/// `Send` but deliberately **not `Sync`**: the seam's contract is that one
/// connection (and any statement on it) is driven by a single thread at a time
/// (see [`crate::driver::CliDriver`]). Being `Send` lets the pool move a
/// connection to a worker thread; being `!Sync` stops a `&Connection` from being
/// shared across threads, so the type system — not just this doc — upholds the
/// single-owner invariant. The driver itself is shared (`Arc<dyn CliDriver>`),
/// so different connections still run concurrently through one driver.
pub struct Connection {
    driver: Arc<dyn CliDriver>,
    handle: ConnectionHandle,
    /// Per-connection prepared-statement LRU (Phase 5). `RefCell`, not a lock:
    /// `Connection` is `Send + !Sync`, one owner at a time.
    cache: RefCell<StatementCache>,
    /// Configured session default, retained for per-transaction restore.
    configured_isolation: Option<Isolation>,
    /// A standalone connection whose session transition became untrustworthy.
    unusable: Cell<bool>,
    /// Only one connection-borrowing transaction may own this session.
    pub(super) transaction_active: Cell<bool>,
    // `Cell<()>` is `Send + !Sync`, which propagates to `Connection` without a
    // runtime cost or any borrowed data. It carries no value.
    _not_sync: PhantomData<Cell<()>>,
}

impl Connection {
    /// Opens a connection through `driver` with statement caching disabled.
    pub fn open(driver: Arc<dyn CliDriver>, params: &ConnectionParams) -> Result<Self> {
        Self::open_with_cache(driver, params, 0)
    }

    /// Opens a connection with a per-connection prepared-statement LRU of
    /// `cache_capacity` entries (0 disables caching — every statement is
    /// prepared and finalized per call). The pool passes
    /// `PoolConfig::statement_cache_size` here.
    pub fn open_with_cache(
        driver: Arc<dyn CliDriver>,
        params: &ConnectionParams,
        cache_capacity: usize,
    ) -> Result<Self> {
        let handle = driver.connect(params)?;
        if let Some(level) = params.isolation {
            if let Err(e) = driver.set_isolation(handle, level) {
                let _ = driver.disconnect(handle);
                return Err(e);
            }
        }
        Ok(Connection {
            driver,
            handle,
            cache: RefCell::new(StatementCache::new(cache_capacity)),
            configured_isolation: params.isolation,
            unusable: Cell::new(false),
            transaction_active: Cell::new(false),
            _not_sync: PhantomData,
        })
    }

    pub fn handle(&self) -> ConnectionHandle {
        self.handle
    }

    /// The shared driver as a trait object — for transaction control
    /// ([`Connection::begin`]) and cursor row reads.
    pub(crate) fn driver_ref(&self) -> &dyn CliDriver {
        self.driver.as_ref()
    }

    pub(crate) fn ensure_usable(&self) -> Result<()> {
        if self.unusable.get() {
            return Err(Error::new(
                ErrorCode::InvalidState,
                "connection unusable after a failed session transition or a lost connection",
            ));
        }
        Ok(())
    }

    pub(crate) fn isolation_restore_target(&self) -> Isolation {
        self.configured_isolation
            .unwrap_or(Isolation::CursorStability)
    }

    pub(crate) fn mark_unusable(&self) {
        self.unusable.set(true);
    }

    /// Whether a failed session transition or a lost connection left this
    /// connection unsafe to pool.
    pub(crate) fn is_unusable(&self) -> bool {
        self.unusable.get()
    }

    /// Retires this connection after a driver failure that reports it lost —
    /// a connection-class error, or SQLSTATE 40003 (statement completion
    /// unknown, as after the CLI `ReceiveTimeout`) — so the pool discards it
    /// even if the caller handles the error. The error's classification, and
    /// with it whether the statement may be retried, is unchanged.
    pub(crate) fn note_failure(&self, error: &Error) {
        if error.code == ErrorCode::Connection || error.sqlstate == COMPLETION_UNKNOWN {
            self.mark_unusable();
        }
    }

    /// Prepares `sql` (or leases it from the statement cache), returning a
    /// reusable [`Statement`].
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        self.ensure_usable()?;
        let lease = self
            .cache
            .borrow_mut()
            .acquire(self.driver.as_ref(), self.handle, sql)
            .map_err(|e| {
                self.note_failure(&e);
                e
            })?;
        Ok(Statement {
            conn: self,
            handle: lease.handle(),
            lease: Some(lease),
        })
    }

    /// Prepares, binds, and executes a non-query statement, returning rows
    /// affected. Accepts positional (`?`) or named (`:name`) parameters.
    pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<i64> {
        self.ensure_usable()?;
        let stmt = self.prepare_and_bind(sql, params.into_params())?;
        let result = self.driver.execute(stmt.handle());
        if let Err(e) = &result {
            stmt.fail(e);
        }
        result
    }

    /// Executes positional values and single-use LOB sources exactly once.
    /// Named parameters are intentionally unsupported on this path.
    pub fn execute_with_lobs(&self, sql: &str, params: Vec<BindParam<'_>>) -> Result<i64> {
        self.ensure_usable()?;
        let stmt = self.prepare(sql)?;
        let (values, sources) = lower_lob_params(params);
        let result = self
            .driver
            .execute_streaming(stmt.handle(), &values, sources);
        if let Err(e) = &result {
            stmt.fail(e);
        }
        result
    }

    /// Prepares, binds, executes, and returns a streaming [`ResultSet`] cursor.
    /// Accepts positional (`?`) or named (`:name`) parameters.
    pub fn query(&self, sql: &str, params: impl IntoParams) -> Result<ResultSet<'_>> {
        self.ensure_usable()?;
        let stmt = self.prepare_and_bind(sql, params.into_params())?;
        let columns = match self
            .driver
            .execute(stmt.handle())
            .and_then(|_| self.driver.column_count(stmt.handle()))
        {
            Ok(columns) => columns,
            Err(e) => {
                stmt.fail(&e);
                return Err(e);
            }
        };
        Ok(ResultSet::from_executed(stmt, columns))
    }

    /// Runs a dedicated row-at-a-time query for chunked LOB access. This path
    /// never calls `fetch_block` and is always a single attempt.
    pub fn query_streaming(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> Result<StreamingResultSet<'_>> {
        self.ensure_usable()?;
        let stmt = self.prepare_and_bind(sql, params.into_params())?;
        let columns = match self
            .driver
            .execute(stmt.handle())
            .and_then(|_| self.driver.column_count(stmt.handle()))
        {
            Ok(columns) => columns,
            Err(error) => {
                stmt.fail(&error);
                return Err(error);
            }
        };
        Ok(StreamingResultSet::from_executed(stmt, columns))
    }

    /// Runs `sql` and maps every row into `T` via [`FromRow`], collecting a
    /// `Vec`. The convenience form of `query(..)?.collect_rows()`; with
    /// `#[derive(FromRow)]` this is the struct-mapping analog of the C++ `queryAs`.
    pub fn query_as<T: FromRow>(&self, sql: &str, params: impl IntoParams) -> Result<Vec<T>> {
        self.ensure_usable()?;
        self.query(sql, params)?.collect_rows()
    }

    /// Validates a column-wise batch (rectangular + type-homogeneous) then runs
    /// it through the driver's array-binding path. Returns total rows affected;
    /// an empty batch is a no-op returning 0. Mirrors C++ `Connection::executeBatch`.
    pub fn execute_batch(&self, sql: &str, rows: &[Vec<Value>]) -> Result<i64> {
        self.ensure_usable()?;
        if rows.is_empty() {
            return Ok(0);
        }
        validate_batch_rows(rows)?;
        let stmt = self.prepare(sql)?;
        let result = self.driver.execute_batch(stmt.handle(), rows);
        if let Err(e) = &result {
            stmt.fail(e);
        }
        result
    }

    fn prepare_and_bind(&self, sql: &str, params: Params) -> Result<Statement<'_>> {
        let lease = self.prepare_and_bind_raw(sql, params)?;
        Ok(Statement {
            conn: self,
            handle: lease.handle(),
            lease: Some(lease),
        })
    }

    /// Prepares and binds `sql` (rewriting named params to positional),
    /// returning the statement **lease** the caller must hand back via
    /// [`release_stmt`](Self::release_stmt) (RAII holders do this from `Drop`).
    /// The cache key is the final, rewritten SQL. On a bind failure the lease
    /// is poisoned and released here.
    fn prepare_and_bind_raw(&self, sql: &str, params: Params) -> Result<StmtLease> {
        let (sql, values) = match params {
            Params::Positional(values) => (Cow::Borrowed(sql), values),
            Params::Named(pairs) => {
                let pre = bind_named(sql, &pairs)?;
                (Cow::Owned(pre.sql), pre.params)
            }
        };
        let lease = self
            .cache
            .borrow_mut()
            .acquire(self.driver.as_ref(), self.handle, &sql)
            .map_err(|e| {
                self.note_failure(&e);
                e
            })?;
        if let Err(e) = self.driver.bind_params(lease.handle(), &values) {
            self.note_failure(&e);
            lease.poison();
            self.release_stmt(lease);
            return Err(e);
        }
        Ok(lease)
    }

    /// Prepares, binds, and executes `sql`, returning the executed statement's
    /// lease and its column count — transferring release responsibility to the
    /// caller. On any failure the lease is poisoned and released here. The
    /// facade's pooled [`QueryResult`](crate::QueryResult) holds the lease for
    /// the cursor's life, so it cannot borrow the leased [`Connection`] the
    /// way [`ResultSet`] does.
    pub(crate) fn execute_query_raw(
        &self,
        sql: &str,
        params: Params,
    ) -> Result<(StmtLease, usize)> {
        self.ensure_usable()?;
        let lease = self.prepare_and_bind_raw(sql, params)?;
        match self
            .driver
            .execute(lease.handle())
            .and_then(|_| self.driver.column_count(lease.handle()))
        {
            Ok(columns) => Ok((lease, columns)),
            Err(e) => {
                self.note_failure(&e);
                lease.poison();
                self.release_stmt(lease);
                Err(e)
            }
        }
    }

    /// Owned-facade counterpart of [`query_streaming`](Self::query_streaming):
    /// transfers the executed statement lease to `StreamingQueryResult`.
    pub(crate) fn execute_streaming_query_raw(
        &self,
        sql: &str,
        params: Params,
    ) -> Result<(StmtLease, usize)> {
        self.execute_query_raw(sql, params)
    }

    /// Hands a statement lease back to this connection's cache: transient
    /// leases are finalized, cached ones reset and returned to the LRU (or
    /// dropped if poisoned). The release half of every prepare path above.
    pub(crate) fn release_stmt(&self, lease: StmtLease) {
        let cleanup = self.cache.borrow_mut().release(self.driver.as_ref(), lease);
        if let Some(error) = cleanup {
            self.note_failure(&error);
        }
    }
}

fn lower_lob_params<'a>(
    params: Vec<BindParam<'a>>,
) -> (Vec<Value>, Vec<crate::driver::ParamStreamSource<'a>>) {
    let mut values = Vec::with_capacity(params.len());
    let mut sources = Vec::new();
    for (index, param) in params.into_iter().enumerate() {
        match param {
            BindParam::Value(value) => values.push(value),
            BindParam::Lob(source) => {
                values.push(Value::Null);
                sources.push(source.into_stream(index));
            }
        }
    }
    (values, sources)
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Cached statement handles must be finalized BEFORE disconnecting:
        // they are invalid once their connection is gone (the C++ destructor
        // ordering, `cache_.reset()` before the handle drop).
        self.cache.borrow_mut().clear(self.driver.as_ref());
        if self.handle.is_valid() {
            let _ = self.driver.disconnect(self.handle);
        }
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

/// A prepared, reusable statement leased from its connection's statement
/// cache (or transient when caching is off/overflowed). Releases the lease on
/// drop. Borrows the owning [`Connection`] so it cannot outlive it.
pub struct Statement<'c> {
    conn: &'c Connection,
    handle: StatementHandle,
    lease: Option<StmtLease>,
}

impl<'c> Statement<'c> {
    pub fn handle(&self) -> StatementHandle {
        self.handle
    }

    /// The shared driver, as a trait object — for the cursor to fetch/read rows.
    pub(crate) fn driver_ref(&self) -> &dyn CliDriver {
        self.conn.driver.as_ref()
    }

    /// The owning connection, for cursors that record driver failures on it.
    pub(crate) fn connection(&self) -> &'c Connection {
        self.conn
    }

    /// Marks the leased statement possibly corrupt (driver-side failure while
    /// in use), so release drops the cache entry instead of reusing it.
    pub(crate) fn poison(&self) {
        if let Some(lease) = &self.lease {
            lease.poison();
        }
    }

    /// Poisons the statement after `error`, retiring the connection too if the
    /// error reports it lost.
    pub(crate) fn fail(&self, error: &Error) {
        self.conn.note_failure(error);
        self.poison();
    }

    /// Rebinds positional params and re-executes, returning rows affected. A
    /// prepared statement is reusable, so this may be called repeatedly. Named
    /// params are rejected here: they rewrite the SQL, which is fixed at prepare
    /// time — use [`Connection::execute`] for those.
    pub fn execute(&mut self, params: impl IntoParams) -> Result<i64> {
        self.conn.ensure_usable()?;
        let values = positional_only(params.into_params())?;
        let result = self
            .conn
            .driver
            .bind_params(self.handle, &values)
            .and_then(|_| self.conn.driver.execute(self.handle));
        if let Err(e) = &result {
            self.fail(e);
        }
        result
    }

    /// Consumes this prepared statement into a streaming query cursor.
    pub fn into_query(self, params: impl IntoParams) -> Result<ResultSet<'c>> {
        self.conn.ensure_usable()?;
        let values = positional_only(params.into_params())?;
        let columns = match self
            .conn
            .driver
            .bind_params(self.handle, &values)
            .and_then(|_| self.conn.driver.execute(self.handle))
            .and_then(|_| self.conn.driver.column_count(self.handle))
        {
            Ok(columns) => columns,
            Err(e) => {
                self.fail(&e);
                return Err(e);
            }
        };
        Ok(ResultSet::from_executed(self, columns))
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.conn.release_stmt(lease);
        }
    }
}

impl fmt::Debug for Statement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Statement")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

fn positional_only(params: Params) -> Result<Vec<Value>> {
    match params {
        Params::Positional(v) => Ok(v),
        Params::Named(_) => Err(mapping(
            "named parameters require Connection::query/execute (the rewritten \
             SQL must be prepared); a prepared Statement takes positional params",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, ErrorCode};
    use crate::named;
    use crate::testing::{MockCliDriver, ScriptedRows};
    use std::sync::Arc;

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "DSN=mock".into(),
            isolation: None,
        }
    }

    fn users_grid() -> ScriptedRows {
        ScriptedRows::new(
            ["id", "name"],
            vec![
                vec![Value::Int(1), Value::Text("ada".into())],
                vec![Value::Int(2), Value::Text("bob".into())],
            ],
        )
    }

    /// Builds a connection over a scripted mock, returning both so the test can
    /// inspect the mock after driving the connection.
    fn connect(mock: &Arc<MockCliDriver>) -> Connection {
        Connection::open(mock.clone(), &params()).unwrap()
    }

    fn completion_unknown() -> Error {
        let mut e = Error::new(ErrorCode::Unknown, "statement completion unknown");
        e.sqlstate = "40003".into();
        e.native_error = -99999;
        e
    }

    #[test]
    fn completion_unknown_retires_the_connection_without_reclassifying() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_execute_error(completion_unknown());
        let conn = connect(&mock);

        let err = conn.execute("UPDATE t SET x = 1", ()).unwrap_err();
        assert_eq!(
            (err.code, err.sqlstate.as_str(), err.retriable),
            (ErrorCode::Unknown, "40003", false)
        );
        assert!(conn.is_unusable());
        let next = conn.execute("UPDATE t SET x = 1", ()).unwrap_err();
        assert_eq!(next.code, ErrorCode::InvalidState);
    }

    #[test]
    fn fetch_failure_retires_the_connection_while_the_cursor_is_alive() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        mock.fail_fetch_block_on_call(1, completion_unknown());
        let conn = connect(&mock);

        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
        assert!(rs.next().is_err());

        let next = conn.execute("UPDATE t SET x = 1", ()).unwrap_err();
        assert_eq!(next.code, ErrorCode::InvalidState); // never reached the driver
        drop(rs);
    }

    #[test]
    fn streaming_fetch_failure_retires_the_connection_while_the_cursor_is_alive() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        mock.fail_fetch_next_on_call(1, completion_unknown());
        let conn = connect(&mock);

        let mut cursor = conn.query_streaming("SELECT id, name FROM u", ()).unwrap();
        assert!(cursor.next().is_err());

        let next = conn.execute("UPDATE t SET x = 1", ()).unwrap_err();
        assert_eq!(next.code, ErrorCode::InvalidState); // never reached the driver
        drop(cursor);
    }

    #[test]
    fn other_statement_failures_keep_the_connection() {
        let mock = Arc::new(MockCliDriver::new());
        let mut duplicate = Error::new(ErrorCode::Constraint, "duplicate key");
        duplicate.sqlstate = "23505".into();
        mock.push_execute_error(duplicate);
        let conn = connect(&mock);

        assert!(conn.execute("INSERT INTO t VALUES (1)", ()).is_err());
        assert!(!conn.is_unusable());
    }

    #[test]
    fn open_connect_failure_propagates() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_connect_error(Error::new(ErrorCode::Connection, "refused"));
        let e = Connection::open(mock.clone(), &params()).unwrap_err();
        assert_eq!(e.code, ErrorCode::Connection);
    }

    #[test]
    fn open_applies_isolation_from_params() {
        let mock = Arc::new(MockCliDriver::new());
        let params = ConnectionParams {
            connection_string: "DSN=mock".into(),
            isolation: Some(crate::Isolation::ReadStability),
        };

        let _conn = Connection::open(mock.clone(), &params).unwrap();

        assert_eq!(
            mock.isolation_calls(),
            vec![crate::Isolation::ReadStability]
        );
    }

    #[test]
    fn open_set_isolation_failure_disconnects_handle() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_isolation_error(Error::new(ErrorCode::Connection, "set isolation failed"));
        let params = ConnectionParams {
            connection_string: "DSN=mock".into(),
            isolation: Some(crate::Isolation::ReadStability),
        };

        let error = Connection::open(mock.clone(), &params).unwrap_err();

        assert_eq!(error.code, ErrorCode::Connection);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn query_streams_rows_and_maps_tuples() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        let conn = connect(&mock);

        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
        assert_eq!(rs.column_count(), 2);

        let mut got = Vec::new();
        while let Some(row) = rs.next().unwrap() {
            let (id, name): (i64, String) = row.get().unwrap();
            got.push((id, name));
        }
        assert_eq!(got, vec![(1, "ada".to_string()), (2, "bob".to_string())]);
        assert!(rs.error().is_none()); // clean end
    }

    #[test]
    fn query_map_yields_owned_items_for_iterator_use() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        let conn = connect(&mock);

        let rs = conn.query("SELECT id, name FROM u", ()).unwrap();
        let names: Vec<String> = rs
            .query_map(|row| row.column::<String>(1))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(names, vec!["ada", "bob"]);
    }

    #[test]
    fn collect_rows_materializes_via_from_row() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        let conn = connect(&mock);
        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
        let rows: Vec<(i64, String)> = rs.collect_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1], (2, "bob".to_string()));
    }

    #[test]
    fn tuple_arity_mismatch_is_mapping_error() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        let conn = connect(&mock);
        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
        let row = rs.next().unwrap().unwrap();
        // 1-tuple against a 2-column row.
        let bad = row.get::<(i64,)>();
        assert_eq!(bad.unwrap_err().code, ErrorCode::Mapping);
    }

    #[test]
    fn null_into_non_optional_is_mapping_error_mid_row() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(["name"], vec![vec![Value::Null]]));
        let conn = connect(&mock);
        let mut rs = conn.query("SELECT name FROM u", ()).unwrap();
        let row = rs.next().unwrap().unwrap();
        assert_eq!(row.get::<(String,)>().unwrap_err().code, ErrorCode::Mapping);
        // Option target reads the NULL cleanly.
        assert_eq!(row.get::<(Option<String>,)>().unwrap(), (None,));
    }

    #[test]
    fn mid_stream_fetch_error_is_surfaced_not_dropped() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        // Force one row per block so the second block fetch fails before the
        // second row can be handed to the caller.
        mock.set_fetch_block_size(1);
        mock.fail_fetch_block_on_call(2, {
            let mut e = Error::new(ErrorCode::Connection, "boom");
            e.retriable = true;
            e
        });
        let conn = connect(&mock);
        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();

        // First row arrives (the temporary Row is released at the `;`).
        assert!(rs.next().unwrap().is_some());
        // The second advance surfaces the error rather than a clean end.
        let err = rs.next().unwrap_err();
        assert_eq!(err.code, ErrorCode::Connection);
        assert_eq!(err.message, "boom");
        // ...and it is retained on the cursor for the pool layer to inspect.
        assert_eq!(rs.error().map(|e| e.code), Some(ErrorCode::Connection));
        // Once errored, the cursor stays done.
        assert!(rs.next().unwrap().is_none());
    }

    #[test]
    fn midstream_fetch_block_error_poisons_cursor() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        mock.set_fetch_block_size(1);
        mock.fail_fetch_block_on_call(
            2,
            Error::new(ErrorCode::Connection, "dropped mid-fetch-block"),
        );
        let conn = connect(&mock);
        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();

        assert!(rs.next().unwrap().is_some());
        let error = rs.next().unwrap_err();
        assert_eq!(error.code, ErrorCode::Connection);
        // A block-fetch failure is retained so the pool can discard the
        // connection. Mapping rows from buffered Values never poisons it.
        assert_eq!(rs.error().map(|e| e.code), Some(ErrorCode::Connection));
    }

    #[test]
    fn fetch_block_empty_means_eof() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(["id"], vec![]));
        let conn = connect(&mock);
        let mut rs = conn.query("SELECT id FROM u", ()).unwrap();

        assert!(rs.next().unwrap().is_none());
        assert!(rs.error().is_none());
        assert_eq!(mock.fetch_block_calls(), 1);
    }

    #[test]
    fn fetch_block_short_block_then_empty() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        // The portable cursor asks for 4096; a smaller successful block is not
        // EOF and it must make one more call to observe the empty EOF block.
        mock.set_fetch_block_size(2);
        let conn = connect(&mock);
        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();

        let rows = rs.collect_rows::<(i64, String)>().unwrap();
        assert_eq!(rows, vec![(1, "ada".into()), (2, "bob".into())]);
        assert_eq!(mock.fetch_block_requests(), vec![4096, 4096]);
    }

    #[test]
    fn client_side_mapping_error_during_read_does_not_poison_the_cursor() {
        let mock = Arc::new(MockCliDriver::new());
        // Column holds an Int; reading it as String is a client-side type error.
        mock.push_result_set(ScriptedRows::new(["name"], vec![vec![Value::Int(7)]]));
        let conn = connect(&mock);
        let mut rs = conn.query("SELECT name FROM u", ()).unwrap();

        let is_err = {
            let row = rs.next().unwrap().unwrap();
            row.column::<String>(0).is_err()
        };
        assert!(is_err);
        // A client mapping error does not mean the connection is bad.
        assert!(rs.error().is_none());
    }

    #[test]
    fn execute_returns_rows_affected_and_finalizes_statement() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_exec_row_count(3);
        let conn = connect(&mock);
        let affected = conn.execute("UPDATE t SET x = ?", (1,)).unwrap();
        assert_eq!(affected, 3);
        // The transient statement was finalized after execute.
        assert_eq!(mock.finalize_calls(), 1);
    }

    #[test]
    fn execute_with_lobs_routes_to_execute_streaming_once() {
        use crate::{lob_callback, BindParam};

        let mock = Arc::new(MockCliDriver::new());
        mock.push_stream_row_count(4);
        mock.set_stream_pull_cap(2);
        let conn = Connection::open_with_cache(mock.clone(), &params(), 8).unwrap();
        let mut offset = 0;
        let affected = conn
            .execute_with_lobs(
                "INSERT INTO docs VALUES (?, ?)",
                vec![
                    BindParam::Value(Value::Int(7)),
                    BindParam::Lob(lob_callback(
                        move |buf| {
                            let source = b"lob";
                            let count = buf.len().min(source.len() - offset);
                            buf[..count].copy_from_slice(&source[offset..offset + count]);
                            offset += count;
                            Ok(count)
                        },
                        Some(3),
                    )),
                ],
            )
            .unwrap();

        assert_eq!(affected, 4);
        assert_eq!(mock.execute_streaming_calls(), 1);
        assert_eq!(mock.last_stream_values(), vec![Value::Int(7), Value::Null]);
        assert_eq!(mock.last_streamed_lobs().get(&1), Some(&b"lob".to_vec()));
    }

    #[test]
    fn execute_with_lobs_driver_failure_is_not_retried() {
        use crate::BindParam;

        let mock = Arc::new(MockCliDriver::new());
        let mut error = Error::new(ErrorCode::Transient, "retryable but single-use");
        error.retriable = true;
        mock.push_execute_streaming_error(error);
        let conn = connect(&mock);

        let got = conn
            .execute_with_lobs(
                "INSERT INTO docs VALUES (?)",
                vec![BindParam::Value(Value::Bytes(vec![1]))],
            )
            .unwrap_err();
        assert!(got.retriable);
        assert_eq!(mock.execute_streaming_calls(), 1);
    }

    #[test]
    fn execute_with_lobs_failure_poisons_statement_lease() {
        use crate::{lob_callback, BindParam};

        let mock = Arc::new(MockCliDriver::new());
        let conn = Connection::open_with_cache(mock.clone(), &params(), 8).unwrap();
        let err = conn
            .execute_with_lobs(
                "INSERT INTO docs VALUES (?)",
                vec![BindParam::Lob(lob_callback(
                    |_| Err(Error::new(ErrorCode::Mapping, "source failed")),
                    None,
                ))],
            )
            .unwrap_err();
        assert_eq!(err.message, "source failed");
        assert_eq!(mock.finalize_calls(), 1);

        mock.push_stream_row_count(1);
        conn.execute_with_lobs(
            "INSERT INTO docs VALUES (?)",
            vec![BindParam::Value(Value::Bytes(vec![1]))],
        )
        .unwrap();
        assert_eq!(mock.prepared_sql().len(), 2);
    }

    #[test]
    fn lob_reader_chunk_boundaries_and_streaming_never_fetches_blocks() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["empty", "null", "exact", "tail"],
            vec![vec![
                Value::Bytes(Vec::new()),
                Value::Null,
                Value::Bytes(b"abcdefgh".to_vec()),
                Value::Bytes(b"abandoned".to_vec()),
            ]],
        ));
        mock.set_lob_chunk_cap(4);
        let conn = Connection::open_with_cache(mock.clone(), &params(), 8).unwrap();
        let mut rs = conn
            .query_streaming("SELECT empty, null, exact, tail FROM t", ())
            .unwrap();
        let mut row = rs.next().unwrap().unwrap();

        let mut buf = [0u8; 4];
        {
            let mut empty = row.lob(0).unwrap();
            assert_eq!(empty.read(&mut buf).unwrap(), 0);
            assert!(!empty.is_null());
        }
        {
            let mut null = row.lob(1).unwrap();
            assert_eq!(null.read(&mut buf).unwrap(), 0);
            assert!(null.is_null());
        }
        {
            let mut exact = row.lob(2).unwrap();
            assert_eq!(exact.read(&mut buf).unwrap(), 4);
            assert_eq!(&buf, b"abcd");
            assert_eq!(exact.read(&mut buf).unwrap(), 4);
            assert_eq!(&buf, b"efgh");
            assert_eq!(exact.read(&mut buf).unwrap(), 0);
        }
        {
            let mut abandoned = row.lob(3).unwrap();
            assert_eq!(abandoned.read(&mut buf).unwrap(), 4);
        }
        drop(rs);
        assert_eq!(mock.fetch_block_calls(), 0);
        assert_eq!(mock.fetch_next_calls(), 1);
        assert_eq!(mock.close_cursor_calls(), 1);
    }

    #[test]
    fn lob_reader_driver_error_is_retained_and_poisons_statement() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["doc"],
            vec![vec![Value::Bytes(vec![1, 2])]],
        ));
        mock.push_get_data_chunk_error(Error::new(ErrorCode::Connection, "chunk failed"));
        let conn = Connection::open_with_cache(mock.clone(), &params(), 8).unwrap();
        {
            let mut rs = conn.query_streaming("SELECT doc FROM docs", ()).unwrap();
            let mut row = rs.next().unwrap().unwrap();
            {
                let mut lob = row.lob(0).unwrap();
                assert_eq!(lob.read(&mut [0u8; 4]).unwrap_err().message, "chunk failed");
                assert_eq!(lob.read(&mut [0u8; 4]).unwrap(), 0);
            }
            assert_eq!(
                rs.error().map(|error| error.code),
                Some(ErrorCode::Connection)
            );
        }
        assert_eq!(mock.finalize_calls(), 1);
    }

    #[test]
    fn ascending_column_violation_is_invalid_state() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["a", "b"],
            vec![vec![Value::Int(1), Value::Int(2)]],
        ));
        let conn = connect(&mock);
        let mut rs = conn.query_streaming("SELECT a, b FROM t", ()).unwrap();
        let mut row = rs.next().unwrap().unwrap();
        assert_eq!(row.get::<i64>(1).unwrap(), 2);
        assert_eq!(row.get::<i64>(0).unwrap_err().code, ErrorCode::InvalidState);
    }

    #[test]
    fn positional_params_are_bound_in_order() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        conn.execute("INSERT INTO t VALUES (?, ?, ?)", (21, "NYC", true))
            .unwrap();
        // The prepared statement handle got these bound params.
        let stmts = mock.prepared_sql();
        assert_eq!(stmts, vec!["INSERT INTO t VALUES (?, ?, ?)"]);
        assert_eq!(
            mock.bound_params(StatementHandle(1)),
            vec![Value::Int(21), Value::Text("NYC".into()), Value::Bool(true)]
        );
    }

    #[test]
    fn named_params_rewrite_sql_and_bind_values() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        let conn = connect(&mock);
        let mut rs = conn
            .query(
                "SELECT id, name FROM u WHERE age > :age AND city = :city",
                named! { "age" => 21, "city" => "NYC" },
            )
            .unwrap();
        let _ = rs.collect_rows::<(i64, String)>().unwrap();
        // The driver saw positional SQL and values in appearance order.
        assert_eq!(
            mock.prepared_sql(),
            vec!["SELECT id, name FROM u WHERE age > ? AND city = ?"]
        );
        assert_eq!(
            mock.bound_params(StatementHandle(1)),
            vec![Value::Int(21), Value::Text("NYC".into())]
        );
    }

    #[test]
    fn prepared_statement_is_reusable_for_dml() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_exec_row_count(1).push_exec_row_count(1);
        let conn = connect(&mock);
        let mut stmt = conn.prepare("INSERT INTO t VALUES (?)").unwrap();
        assert_eq!(stmt.execute((10,)).unwrap(), 1);
        assert_eq!(stmt.execute((20,)).unwrap(), 1);
        // One prepare, two executes on the same handle.
        assert_eq!(mock.prepared_sql().len(), 1);
        assert_eq!(mock.bound_params(StatementHandle(1)), vec![Value::Int(20)]);
    }

    #[test]
    fn statement_rejects_named_params() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let mut stmt = conn.prepare("INSERT INTO t VALUES (?)").unwrap();
        let e = stmt.execute(named! { "x" => 1 }).unwrap_err();
        assert_eq!(e.code, ErrorCode::Mapping);
    }

    #[test]
    fn execute_batch_validates_then_delegates() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let rows = vec![
            vec![Value::Int(1), Value::Text("a".into())],
            vec![Value::Int(2), Value::Text("b".into())],
        ];
        let affected = conn
            .execute_batch("INSERT INTO t VALUES (?, ?)", &rows)
            .unwrap();
        assert_eq!(affected, 2);
        assert_eq!(mock.execute_batch_calls(), 1);
        assert_eq!(mock.last_batch_rows(), rows);
    }

    #[test]
    fn execute_batch_rejects_ragged_rows_before_touching_driver() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let rows = vec![
            vec![Value::Int(1), Value::Text("a".into())],
            vec![Value::Int(2)],
        ];
        let e = conn
            .execute_batch("INSERT INTO t VALUES (?, ?)", &rows)
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Mapping);
        // Validation happened above the seam: the driver's batch path was never called.
        assert_eq!(mock.execute_batch_calls(), 0);
    }

    #[test]
    fn empty_batch_is_a_noop() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        assert_eq!(
            conn.execute_batch("INSERT INTO t VALUES (?)", &[]).unwrap(),
            0
        );
        assert_eq!(mock.execute_batch_calls(), 0);
    }

    #[test]
    fn connection_disconnects_after_statements_finalize_on_drop() {
        let mock = Arc::new(MockCliDriver::new());
        {
            let conn = connect(&mock);
            let _ = conn.execute("UPDATE t SET x = 1", ()).unwrap();
            // conn drops here.
        }
        // The statement was finalized, then the connection disconnected.
        assert_eq!(mock.finalize_calls(), 1);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    /// Builds a caching connection over the mock.
    fn connect_cached(mock: &Arc<MockCliDriver>, capacity: usize) -> Connection {
        Connection::open_with_cache(mock.clone(), &params(), capacity).unwrap()
    }

    #[test]
    fn cached_connection_reuses_prepared_statement_across_queries() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        mock.push_result_set(users_grid());
        let conn = connect_cached(&mock, 8);
        for _ in 0..2 {
            let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
            let _rows: Vec<(i64, String)> = rs.collect_rows().unwrap();
        }
        assert_eq!(mock.prepared_sql().len(), 1); // second query was a hit
        assert_eq!(mock.finalize_calls(), 0); // entry still cached
        assert_eq!(mock.close_cursor_calls(), 2); // reset on each release
    }

    #[test]
    fn named_params_cache_on_the_rewritten_sql() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        mock.push_result_set(users_grid());
        let conn = connect_cached(&mock, 8);
        for _ in 0..2 {
            let mut rs = conn
                .query(
                    "SELECT id, name FROM u WHERE age > :age",
                    named! { "age" => 21 },
                )
                .unwrap();
            let _rows: Vec<(i64, String)> = rs.collect_rows().unwrap();
        }
        // One prepare, keyed by the rewritten (positional) SQL.
        assert_eq!(
            mock.prepared_sql(),
            vec!["SELECT id, name FROM u WHERE age > ?"]
        );
    }

    #[test]
    fn execute_error_poisons_cached_statement_so_next_use_reprepares() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect_cached(&mock, 8);
        conn.execute("UPDATE t SET x = ?", (1,)).unwrap(); // miss: prepare #1
                                                           // Not connection-class: that would retire the connection itself.
        mock.push_execute_error(Error::new(ErrorCode::Unknown, "broken"));
        conn.execute("UPDATE t SET x = ?", (2,)).unwrap_err(); // hit, exec fails
        assert_eq!(mock.finalize_calls(), 1); // poisoned entry dropped
        conn.execute("UPDATE t SET x = ?", (3,)).unwrap(); // miss again
        assert_eq!(mock.prepared_sql().len(), 2);
    }

    #[test]
    fn fetch_error_drops_cached_statement_via_cursor_drop() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        mock.push_result_set(users_grid());
        // Not connection-class: that would retire the connection itself.
        mock.fail_fetch_block_on_call(1, Error::new(ErrorCode::Unknown, "boom"));
        let conn = connect_cached(&mock, 8);
        {
            let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
            let _ = rs.next().unwrap_err(); // driver-side scan failure
        } // ResultSet drop → Statement poisoned → entry finalized + dropped
        assert_eq!(mock.finalize_calls(), 1);
        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
        let _rows: Vec<(i64, String)> = rs.collect_rows().unwrap();
        assert_eq!(mock.prepared_sql().len(), 2); // re-prepared after the drop
    }

    #[test]
    fn client_side_mapping_error_does_not_drop_cached_statement() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(["n"], vec![vec![Value::Int(7)]]));
        mock.push_result_set(ScriptedRows::new(["n"], vec![vec![Value::Int(7)]]));
        let conn = connect_cached(&mock, 8);
        {
            let mut rs = conn.query("SELECT n FROM u", ()).unwrap();
            let row = rs.next().unwrap().unwrap();
            assert!(row.column::<String>(0).is_err()); // Int as String: mapping
        }
        let mut rs = conn.query("SELECT n FROM u", ()).unwrap();
        let _rows: Vec<(i64,)> = rs.collect_rows().unwrap();
        assert_eq!(mock.prepared_sql().len(), 1); // entry survived: still a hit
        assert_eq!(mock.finalize_calls(), 0);
    }

    #[test]
    fn overlapping_same_sql_statements_use_transient_overflow() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect_cached(&mock, 8);
        let s1 = conn.prepare("SELECT 1").unwrap();
        let s2 = conn.prepare("SELECT 1").unwrap(); // busy hit → transient
        assert_eq!(mock.prepared_sql().len(), 2);
        drop(s2);
        assert_eq!(mock.finalize_calls(), 1); // transient finalized
        drop(s1);
        assert_eq!(mock.finalize_calls(), 1); // cached entry kept
    }

    #[test]
    fn drop_finalizes_cached_entries_before_disconnect() {
        let mock = Arc::new(MockCliDriver::new());
        {
            let conn = connect_cached(&mock, 8);
            conn.execute("UPDATE t SET x = 1", ()).unwrap(); // one cached entry
        }
        // Cache cleared (finalize) strictly before disconnect.
        assert_eq!(mock.events(), vec!["finalize", "disconnect"]);
    }

    #[test]
    fn connection_is_send_for_pool_handoff() {
        fn assert_send<T: Send>() {}
        assert_send::<Connection>();
    }

    // Compile-time guard that `Connection` is NOT `Sync` (the single-owner seam
    // contract, enforced by the `PhantomData<Cell<()>>` marker): `resolve`'s trait
    // parameter is inferable only while the type is `!Sync`. If `Connection`
    // regained `Sync`, both impls would apply, the `_` would be ambiguous, and the
    // test below would fail to compile.
    trait AmbiguousIfSync<A> {
        fn resolve() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}

    #[test]
    fn connection_is_not_sync() {
        let _resolve = <Connection as AmbiguousIfSync<_>>::resolve;
    }
}
