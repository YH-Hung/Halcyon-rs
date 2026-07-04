//! `Connection` and `Statement`: the RAII entry points to the core data path.
//!
//! Ported from the C++ `Connection`/`Statement`. A [`Connection`] owns a driver
//! connection handle and disconnects on drop; a [`Statement`] owns a prepared
//! statement handle and finalizes on drop. Both are move-only. `Statement<'c>`
//! borrows its `Connection`, so the borrow checker guarantees statements are
//! finalized before the connection is disconnected — the ordering the C++
//! `Connection::reset` enforces by hand.
//!
//! The per-connection statement cache (C++ `StatementCache`) is Phase 5; here
//! every `query`/`execute` prepares a fresh statement.

use std::borrow::Cow;
use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::core::mapping;
use crate::core::params::{bind_named, IntoParams, Params};
use crate::core::result_set::{FromRow, ResultSet};
use crate::driver::batch::validate_batch_rows;
use crate::driver::{CliDriver, ConnectionHandle, ConnectionParams, StatementHandle};
use crate::error::Result;
use crate::value::Value;

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
    // `Cell<()>` is `Send + !Sync`, which propagates to `Connection` without a
    // runtime cost or any borrowed data. It carries no value.
    _not_sync: PhantomData<Cell<()>>,
}

impl Connection {
    /// Opens a connection through `driver`.
    pub fn open(driver: Arc<dyn CliDriver>, params: &ConnectionParams) -> Result<Self> {
        let handle = driver.connect(params)?;
        Ok(Connection {
            driver,
            handle,
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

    /// Prepares `sql`, returning a reusable [`Statement`].
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        let handle = self.driver.prepare(self.handle, sql)?;
        Ok(Statement { conn: self, handle })
    }

    /// Prepares, binds, and executes a non-query statement, returning rows
    /// affected. Accepts positional (`?`) or named (`:name`) parameters.
    pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<i64> {
        let stmt = self.prepare_and_bind(sql, params.into_params())?;
        self.driver.execute(stmt.handle)
    }

    /// Prepares, binds, executes, and returns a streaming [`ResultSet`] cursor.
    /// Accepts positional (`?`) or named (`:name`) parameters.
    pub fn query(&self, sql: &str, params: impl IntoParams) -> Result<ResultSet<'_>> {
        let stmt = self.prepare_and_bind(sql, params.into_params())?;
        self.driver.execute(stmt.handle)?;
        let columns = self.driver.column_count(stmt.handle)?;
        Ok(ResultSet::from_executed(stmt, columns))
    }

    /// Runs `sql` and maps every row into `T` via [`FromRow`], collecting a
    /// `Vec`. The convenience form of `query(..)?.collect_rows()`; with
    /// `#[derive(FromRow)]` this is the struct-mapping analog of the C++ `queryAs`.
    pub fn query_as<T: FromRow>(&self, sql: &str, params: impl IntoParams) -> Result<Vec<T>> {
        self.query(sql, params)?.collect_rows()
    }

    /// Validates a column-wise batch (rectangular + type-homogeneous) then runs
    /// it through the driver's array-binding path. Returns total rows affected;
    /// an empty batch is a no-op returning 0. Mirrors C++ `Connection::executeBatch`.
    pub fn execute_batch(&self, sql: &str, rows: &[Vec<Value>]) -> Result<i64> {
        if rows.is_empty() {
            return Ok(0);
        }
        validate_batch_rows(rows)?;
        let stmt = self.prepare(sql)?;
        self.driver.execute_batch(stmt.handle, rows)
    }

    fn prepare_and_bind(&self, sql: &str, params: Params) -> Result<Statement<'_>> {
        let handle = self.prepare_and_bind_raw(sql, params)?;
        Ok(Statement { conn: self, handle })
    }

    /// Prepares and binds `sql` (rewriting named params to positional), returning
    /// a **raw** statement handle the caller owns and must `finalize`. On a bind
    /// failure the handle is finalized here. [`prepare_and_bind`] wraps this in an
    /// RAII [`Statement`]; [`execute_query_raw`](Self::execute_query_raw) hands the
    /// raw handle to the facade's owned pooled cursor.
    fn prepare_and_bind_raw(&self, sql: &str, params: Params) -> Result<StatementHandle> {
        let (sql, values) = match params {
            Params::Positional(values) => (Cow::Borrowed(sql), values),
            Params::Named(pairs) => {
                let pre = bind_named(sql, &pairs)?;
                (Cow::Owned(pre.sql), pre.params)
            }
        };
        let handle = self.driver.prepare(self.handle, &sql)?;
        if let Err(e) = self.driver.bind_params(handle, &values) {
            let _ = self.driver.finalize(handle);
            return Err(e);
        }
        Ok(handle)
    }

    /// Prepares, binds, and executes `sql`, returning the executed statement's raw
    /// handle and its column count — transferring finalize responsibility to the
    /// caller. On any failure the handle is finalized here. The facade's pooled
    /// [`QueryResult`](crate::QueryResult) owns the handle for the cursor's life,
    /// so it cannot borrow the leased [`Connection`] the way [`ResultSet`] does.
    pub(crate) fn execute_query_raw(
        &self,
        sql: &str,
        params: Params,
    ) -> Result<(StatementHandle, usize)> {
        let handle = self.prepare_and_bind_raw(sql, params)?;
        match self
            .driver
            .execute(handle)
            .and_then(|_| self.driver.column_count(handle))
        {
            Ok(columns) => Ok((handle, columns)),
            Err(e) => {
                let _ = self.driver.finalize(handle);
                Err(e)
            }
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
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

/// A prepared, reusable statement. Owns its driver statement handle and finalizes
/// it on drop. Borrows the owning [`Connection`] so it cannot outlive it.
pub struct Statement<'c> {
    conn: &'c Connection,
    handle: StatementHandle,
}

impl<'c> Statement<'c> {
    pub fn handle(&self) -> StatementHandle {
        self.handle
    }

    /// The shared driver, as a trait object — for the cursor to fetch/read rows.
    pub(crate) fn driver_ref(&self) -> &dyn CliDriver {
        self.conn.driver.as_ref()
    }

    /// Rebinds positional params and re-executes, returning rows affected. A
    /// prepared statement is reusable, so this may be called repeatedly. Named
    /// params are rejected here: they rewrite the SQL, which is fixed at prepare
    /// time — use [`Connection::execute`] for those.
    pub fn execute(&mut self, params: impl IntoParams) -> Result<i64> {
        let values = positional_only(params.into_params())?;
        self.conn.driver.bind_params(self.handle, &values)?;
        self.conn.driver.execute(self.handle)
    }

    /// Consumes this prepared statement into a streaming query cursor.
    pub fn into_query(self, params: impl IntoParams) -> Result<ResultSet<'c>> {
        let values = positional_only(params.into_params())?;
        self.conn.driver.bind_params(self.handle, &values)?;
        self.conn.driver.execute(self.handle)?;
        let columns = self.conn.driver.column_count(self.handle)?;
        Ok(ResultSet::from_executed(self, columns))
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        if self.handle.is_valid() {
            let _ = self.conn.driver.finalize(self.handle);
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

    #[test]
    fn open_connect_failure_propagates() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_connect_error(Error::new(ErrorCode::Connection, "refused"));
        let e = Connection::open(mock.clone(), &params()).unwrap_err();
        assert_eq!(e.code, ErrorCode::Connection);
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
        // First row fetches OK; the second fetch errors.
        mock.fail_fetch_on_call(2, {
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
    fn mid_stream_column_read_error_poisons_the_cursor() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        // The first get_column (column 0 of the first row) fails as a driver
        // error, e.g. a connection drop during SQLGetData.
        mock.fail_get_column_on_call(1, Error::new(ErrorCode::Connection, "dropped mid-read"));
        let conn = connect(&mock);
        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();

        // The read propagates the driver error to the caller...
        let code = {
            let row = rs.next().unwrap().unwrap();
            row.column::<i64>(0).unwrap_err().code
        };
        assert_eq!(code, ErrorCode::Connection);
        // ...and, unlike a plain mapping error, it is retained on the cursor so
        // the pool can discard the connection (mirrors C++ note_column_error).
        assert_eq!(rs.error().map(|e| e.code), Some(ErrorCode::Connection));
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
