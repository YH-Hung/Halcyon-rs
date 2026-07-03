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

use std::fmt;
use std::sync::Arc;

use crate::core::mapping;
use crate::core::params::{bind_named, IntoParams, Params};
use crate::core::result_set::ResultSet;
use crate::driver::batch::validate_batch_rows;
use crate::driver::{CliDriver, ConnectionHandle, ConnectionParams, StatementHandle};
use crate::error::Result;
use crate::value::Value;

/// A single logical Db2 connection over the CLI seam.
///
/// Owns the physical connection handle and disconnects on drop. Move-only and
/// not safe for concurrent use by multiple threads (the pool leases one to a
/// single thread at a time). The driver is shared (`Arc<dyn CliDriver>`) so many
/// connections can run concurrently through one driver.
pub struct Connection {
    driver: Arc<dyn CliDriver>,
    handle: ConnectionHandle,
}

impl Connection {
    /// Opens a connection through `driver`.
    pub fn open(driver: Arc<dyn CliDriver>, params: &ConnectionParams) -> Result<Self> {
        let handle = driver.connect(params)?;
        Ok(Connection { driver, handle })
    }

    pub fn handle(&self) -> ConnectionHandle {
        self.handle
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
        match params {
            Params::Positional(values) => {
                let stmt = self.prepare(sql)?;
                self.driver.bind_params(stmt.handle, &values)?;
                Ok(stmt)
            }
            Params::Named(pairs) => {
                let pre = bind_named(sql, &pairs)?;
                let stmt = self.prepare(&pre.sql)?;
                self.driver.bind_params(stmt.handle, &pre.params)?;
                Ok(stmt)
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
}
