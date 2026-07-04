//! `Transaction`: a scoped unit of work over a single [`Connection`].
//!
//! Ported from the C++ `Transaction` (`include/halcyon/transaction.hpp`).
//! [`Connection::begin`] disables autocommit and hands back a [`Transaction`];
//! [`commit`](Transaction::commit) / [`rollback`](Transaction::rollback) end it
//! and restore autocommit. If neither is called, the [`Drop`] guard rolls back —
//! so the unit of work is undone on an early return, a `?`, or a panic that
//! unwinds through it. Move-only (it borrows its connection) and never
//! auto-retried: the caller re-drives the whole transaction on failure.

use std::fmt;

use crate::core::connection::Connection;
use crate::core::params::IntoParams;
use crate::core::result_set::{FromRow, ResultSet};
use crate::driver::{CliDriver, ConnectionHandle};
use crate::error::Result;
use crate::value::Value;

/// A scoped unit of work over one [`Connection`]. Autocommit is off for its
/// lifetime; see the module docs for the commit/rollback and drop semantics.
pub struct Transaction<'c> {
    conn: &'c Connection,
    active: bool,
    poisoned: bool,
}

/// Whether [`Transaction::finish`] should commit or roll back.
enum Outcome {
    Commit,
    Rollback,
}

impl<'c> Transaction<'c> {
    /// Constructed only by [`Connection::begin`], which has already flipped
    /// autocommit off on `conn`.
    fn new(conn: &'c Connection) -> Self {
        Transaction {
            conn,
            active: true,
            poisoned: false,
        }
    }

    /// True until the transaction is committed or rolled back (including the
    /// implicit rollback on drop).
    pub fn active(&self) -> bool {
        self.active
    }

    /// True if a commit/rollback (or the implicit rollback on drop) failed,
    /// leaving the connection dead or autocommit in an unknown state. The pool
    /// (Phase 4) discards such a connection rather than reusing it; mirrors the
    /// C++ `Transaction::poisoned`.
    pub fn poisoned(&self) -> bool {
        self.poisoned
    }

    /// Executes a non-query statement within the unit of work. See
    /// [`Connection::execute`].
    pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<i64> {
        self.conn.execute(sql, params)
    }

    /// Runs a streaming query within the unit of work. The returned cursor
    /// borrows the transaction, so it must be finished (dropped) before
    /// commit/rollback. See [`Connection::query`].
    pub fn query(&self, sql: &str, params: impl IntoParams) -> Result<ResultSet<'_>> {
        self.conn.query(sql, params)
    }

    /// Runs a query and maps every row into `T`. See [`Connection::query_as`].
    pub fn query_as<T: FromRow>(&self, sql: &str, params: impl IntoParams) -> Result<Vec<T>> {
        self.conn.query_as(sql, params)
    }

    /// Batch insert within the unit of work. See [`Connection::execute_batch`].
    pub fn execute_batch(&self, sql: &str, rows: &[Vec<Value>]) -> Result<i64> {
        self.conn.execute_batch(sql, rows)
    }

    /// Commits the unit of work and restores autocommit. A no-op (`Ok`) if the
    /// transaction has already ended. On failure the transaction is
    /// [`poisoned`](Transaction::poisoned) and the error is returned.
    pub fn commit(&mut self) -> Result<()> {
        self.finish(Outcome::Commit)
    }

    /// Rolls the unit of work back and restores autocommit. A no-op (`Ok`) if the
    /// transaction has already ended. On failure the transaction is
    /// [`poisoned`](Transaction::poisoned) and the error is returned.
    pub fn rollback(&mut self) -> Result<()> {
        self.finish(Outcome::Rollback)
    }

    /// Shared commit/rollback path: run the terminal op, flip `active` off,
    /// restore autocommit, and poison on any failure — the exact ordering of the
    /// C++ `Transaction::commit`/`rollback`. Returns the terminal op's error if it
    /// failed, otherwise the autocommit-restore result.
    fn finish(&mut self, outcome: Outcome) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        let (result, poisoned) = terminate(
            self.conn.driver_ref(),
            self.conn.handle(),
            matches!(outcome, Outcome::Commit),
        );
        self.active = false;
        if poisoned {
            self.poisoned = true; // dead connection or autocommit lost
        }
        result
    }
}

/// Runs the terminal transaction op (commit if `commit`, else rollback) then
/// restores autocommit — the exact order the C++ `Transaction::commit`/`rollback`
/// use. Returns the op's error if it failed, otherwise the autocommit-restore
/// result, plus whether the connection is now poisoned (dead, or autocommit left
/// in an unknown state). Shared by [`Transaction`] and the facade's owned
/// `ScopedTransaction`, which both wrap it with their own `active`/`poisoned`
/// bookkeeping.
pub(crate) fn terminate(
    driver: &dyn CliDriver,
    handle: ConnectionHandle,
    commit: bool,
) -> (Result<()>, bool) {
    let ended = if commit {
        driver.commit(handle)
    } else {
        driver.rollback(handle)
    };
    let restored = driver.set_auto_commit(handle, true);
    let poisoned = ended.is_err() || restored.is_err();
    (ended.and(restored), poisoned)
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // An uncommitted transaction rolls back (C++ `finish_rollback`). An error
        // can't propagate out of `drop`, but it still poisons the connection so
        // the pool (Phase 4) can discard it.
        let _ = self.finish(Outcome::Rollback);
    }
}

impl fmt::Debug for Transaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("active", &self.active)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl Connection {
    /// Begins a transaction: disables autocommit and returns a [`Transaction`]
    /// bound to this connection. If enabling manual commit fails, no transaction
    /// is created and the error is returned. Ported from C++ `Connection::begin`.
    pub fn begin(&self) -> Result<Transaction<'_>> {
        self.driver_ref().set_auto_commit(self.handle(), false)?;
        Ok(Transaction::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::ConnectionParams;
    use crate::error::{Error, ErrorCode};
    use crate::testing::{MockCliDriver, ScriptedRows};
    use std::sync::Arc;

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "DSN=mock".into(),
        }
    }

    fn connect(mock: &Arc<MockCliDriver>) -> Connection {
        Connection::open(mock.clone(), &params()).unwrap()
    }

    #[test]
    fn begin_disables_autocommit_then_commit_restores() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        {
            let mut tx = conn.begin().unwrap();
            assert!(tx.active());
            tx.execute("INSERT INTO t VALUES (?)", (1,)).unwrap();
            tx.commit().unwrap();
            assert!(!tx.active());
        }
        // begin → autocommit(false); commit → autocommit(true).
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
        assert_eq!(mock.commit_calls(), 1);
        assert_eq!(mock.rollback_calls(), 0);
    }

    #[test]
    fn auto_rolls_back_when_dropped_uncommitted() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        {
            let tx = conn.begin().unwrap();
            tx.execute("UPDATE t SET a = ?", (2,)).unwrap();
            // No commit → the drop guard rolls back.
        }
        assert_eq!(mock.commit_calls(), 0);
        assert_eq!(mock.rollback_calls(), 1);
        // The implicit rollback also restores autocommit.
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
    }

    #[test]
    fn explicit_rollback_ends_the_unit_of_work_once() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let mut tx = conn.begin().unwrap();
        tx.rollback().unwrap();
        assert!(!tx.active());
        assert_eq!(mock.rollback_calls(), 1);
        // Dropping the already-rolled-back transaction does not roll back again.
        drop(tx);
        assert_eq!(mock.rollback_calls(), 1);
    }

    #[test]
    fn query_runs_inside_the_transaction() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(["c"], vec![vec![Value::Int(7)]]));
        let conn = connect(&mock);
        let mut tx = conn.begin().unwrap();
        let sum = {
            let mut rs = tx.query("SELECT c FROM t", ()).unwrap();
            let mut acc = 0;
            while let Some(row) = rs.next().unwrap() {
                acc += row.column::<i64>(0).unwrap();
            }
            acc
        };
        assert_eq!(sum, 7);
        tx.commit().unwrap();
        assert_eq!(mock.commit_calls(), 1);
    }

    #[test]
    fn committed_transaction_does_not_roll_back_on_drop() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        {
            let mut tx = conn.begin().unwrap();
            tx.execute("DELETE FROM t", ()).unwrap();
            tx.commit().unwrap();
            // Drop here must NOT issue a second terminal op.
        }
        assert_eq!(mock.commit_calls(), 1);
        assert_eq!(mock.rollback_calls(), 0);
    }

    #[test]
    fn commit_failure_poisons_the_transaction() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let mut tx = conn.begin().unwrap();
        // The next transaction call (commit) fails with a connection-class error.
        mock.push_txn_error(Error::new(ErrorCode::Connection, "dead"));
        let e = tx.commit().unwrap_err();
        assert_eq!(e.code, ErrorCode::Connection);
        assert!(tx.poisoned());
        assert!(!tx.active());
    }

    #[test]
    fn begin_failure_propagates_and_creates_no_transaction() {
        let mock = Arc::new(MockCliDriver::new());
        // begin()'s set_auto_commit(false) fails.
        mock.push_txn_error(Error::new(ErrorCode::Connection, "dead"));
        let conn = connect(&mock);
        let e = conn.begin().unwrap_err();
        assert_eq!(e.code, ErrorCode::Connection);
        // No transaction was created, so nothing rolls back.
        assert_eq!(mock.rollback_calls(), 0);
    }
}
