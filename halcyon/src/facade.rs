//! The high-level facade: [`Database`], the ergonomic entry point.
//!
//! Ported from the C++ `Database` (`include/halcyon/database.hpp`),
//! single-connection first: this Phase-3 `Database` wraps one [`Connection`],
//! delegates query/execute/batch to it, and layers the ergonomic transaction
//! surface (`begin` + closure `transaction`) on top. Phase 4 re-targets it onto
//! the connection pool for thread-safe, multi-connection use — the same order the
//! C++ project took (facade ergonomics first, pool concurrency second).
//!
//! Because it currently owns a single `!Sync` [`Connection`], a `Database` is
//! `Send` but not `Sync`; the pooled Phase-4 form will be freely shareable. The
//! C++ facade's `QueryResult` / `ScopedTransaction` lease wrappers have no analog
//! here: Rust's borrow checker ties a [`ResultSet`] / [`Transaction`] to the
//! `&Database` it came from, which is exactly the lifetime coupling those
//! wrappers implement by hand. They return with the pool in Phase 4.

use std::sync::Arc;

use crate::core::{Connection, FromRow, IntoParams, ResultSet, Transaction};
use crate::driver::{CliDriver, ConnectionParams};
use crate::error::Result;
use crate::value::Value;

/// The ergonomic Db2 database handle. (Phase 3: a single owned connection; the
/// connection pool arrives in Phase 4.)
#[derive(Debug)]
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Opens a `Database` over `driver` — the injectable entry point, used by
    /// tests (against a mock `CliDriver`) and by callers supplying their own
    /// driver. The `db2`-gated [`Database::connect`] is the no-CLI-types form.
    pub fn open(driver: Arc<dyn CliDriver>, params: &ConnectionParams) -> Result<Self> {
        Ok(Database {
            conn: Connection::open(driver, params)?,
        })
    }

    /// Executes a non-query statement, returning rows affected. Accepts
    /// positional (`?`) or named (`:name`) parameters. See [`Connection::execute`].
    pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<i64> {
        self.conn.execute(sql, params)
    }

    /// Runs a streaming query, returning a cursor that borrows the `Database`.
    /// See [`Connection::query`].
    pub fn query(&self, sql: &str, params: impl IntoParams) -> Result<ResultSet<'_>> {
        self.conn.query(sql, params)
    }

    /// Runs a query and maps every row into `T` via [`FromRow`], collecting a
    /// `Vec`. See [`Connection::query_as`].
    pub fn query_as<T: FromRow>(&self, sql: &str, params: impl IntoParams) -> Result<Vec<T>> {
        self.conn.query_as(sql, params)
    }

    /// Validates and runs a column-wise batch (rectangular + type-homogeneous)
    /// through the driver's array-binding path. See [`Connection::execute_batch`].
    pub fn execute_batch(&self, sql: &str, rows: &[Vec<Value>]) -> Result<i64> {
        self.conn.execute_batch(sql, rows)
    }

    /// Begins a transaction, returning a [`Transaction`] bound to the database's
    /// connection. Commit or roll back explicitly; an uncommitted transaction
    /// rolls back when dropped.
    pub fn begin(&self) -> Result<Transaction<'_>> {
        self.conn.begin()
    }

    /// Runs `f` inside a transaction: commits if it returns `Ok` (propagating a
    /// commit failure), rolls back if it returns `Err` (propagating that error),
    /// and rolls back if it panics (the [`Transaction`] drop guard fires as the
    /// panic unwinds). Mirrors the C++ `Database::transaction`.
    pub fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T>,
    {
        let mut tx = self.conn.begin()?;
        match f(&tx) {
            Ok(value) => {
                tx.commit()?;
                Ok(value)
            }
            Err(e) => {
                // Best-effort rollback; surface the caller's original error, not
                // the rollback's (matches C++, which ignores the rollback result
                // here). A panic instead unwinds and the drop guard rolls back.
                let _ = tx.rollback();
                Err(e)
            }
        }
    }
}

#[cfg(feature = "db2")]
impl Database {
    /// Opens a `Database` over a fresh real
    /// [`Db2CliDriver`](crate::driver::Db2CliDriver) built from `dsn` — the
    /// ergonomic entry point, with no CLI types in the call. Available with the
    /// `db2` feature (the C++ `Database::open(dsn)` analog).
    pub fn connect(dsn: &str) -> Result<Self> {
        let driver = Arc::new(crate::driver::Db2CliDriver::new());
        Database::open(
            driver,
            &ConnectionParams {
                connection_string: dsn.to_string(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, ErrorCode};
    use crate::named;
    use crate::testing::{MockCliDriver, ScriptedRows};

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "DSN=mock".into(),
        }
    }

    fn open(mock: &Arc<MockCliDriver>) -> Database {
        Database::open(mock.clone(), &params()).unwrap()
    }

    #[test]
    fn open_connects_once() {
        let mock = Arc::new(MockCliDriver::new());
        let _db = open(&mock);
        assert_eq!(mock.connect_calls(), 1);
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
    }

    #[test]
    fn query_iterates_rows() {
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
        let mut rs = db.query("SELECT id FROM t WHERE id > ?", (0,)).unwrap();
        let mut sum = 0;
        while let Some(row) = rs.next().unwrap() {
            sum += row.column::<i64>(0).unwrap();
        }
        assert_eq!(sum, 7);
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

        // Silence the default panic hook so the intentional panic below does not
        // spew a backtrace into the test output.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = catch_unwind(AssertUnwindSafe(|| {
            db.transaction(|_tx| -> Result<()> { panic!("boom") })
        }));
        std::panic::set_hook(prev);

        // The panic propagated out of transaction(), and the drop guard rolled
        // the unit of work back as it unwound.
        assert!(result.is_err());
        assert_eq!(mock.commit_calls(), 0);
        assert_eq!(mock.rollback_calls(), 1);
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
    }

    #[test]
    fn begin_returns_usable_transaction() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let mut tx = db.begin().unwrap();
        tx.execute("UPDATE t SET a = ?", (1,)).unwrap();
        tx.commit().unwrap();
        assert_eq!(mock.commit_calls(), 1);
    }

    #[test]
    fn database_is_send_for_future_pool_handoff() {
        fn assert_send<T: Send>() {}
        assert_send::<Database>();
    }
}
