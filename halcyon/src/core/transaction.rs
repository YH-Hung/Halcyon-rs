//! `Transaction`: a scoped unit of work over a single [`Connection`].
//!
//! Ported from the C++ `Transaction` (`include/halcyon/transaction.hpp`).
//! [`Connection::begin`] disables autocommit and hands back a [`Transaction`];
//! [`commit`](Transaction::commit) / [`rollback`](Transaction::rollback) end it
//! and restore autocommit. If neither is called, the [`Drop`] guard rolls back —
//! so the unit of work is undone on an early return, a `?`, or a panic that
//! unwinds through it. Move-only (it borrows its connection) and never
//! auto-retried: the caller re-drives the whole transaction on failure.

use std::cell::Cell;
use std::fmt;

use crate::core::connection::Connection;
use crate::core::params::IntoParams;
use crate::core::result_set::{FromRow, ResultSet};
use crate::core::StreamingResultSet;
use crate::core::{
    auto_savepoint, named_savepoint, nested_savepoint, Savepoint, SavepointOwner, SavepointState,
};
use crate::driver::{CliDriver, ConnectionHandle};
use crate::error::{Error, ErrorCode, Result};
use crate::lob::BindParam;
use crate::value::Value;
use crate::Isolation;

/// A scoped unit of work over one [`Connection`]. Autocommit is off for its
/// lifetime; see the module docs for the commit/rollback and drop semantics.
/// Data operations after commit or rollback return `ErrorCode::InvalidState`.
pub struct Transaction<'c> {
    conn: &'c Connection,
    active: Cell<bool>,
    poisoned: Cell<bool>,
    restore_isolation: Option<Isolation>,
    savepoints: SavepointState,
}

/// Whether [`Transaction::finish`] should commit or roll back.
enum Outcome {
    Commit,
    Rollback,
}

impl<'c> Transaction<'c> {
    /// Constructed only by [`Connection::begin`], which has already flipped
    /// autocommit off on `conn`.
    fn new(conn: &'c Connection, restore_isolation: Option<Isolation>) -> Self {
        conn.transaction_active.set(true);
        Transaction {
            conn,
            active: Cell::new(true),
            poisoned: Cell::new(false),
            restore_isolation,
            savepoints: SavepointState::new(),
        }
    }

    /// True until the transaction is committed or rolled back (including the
    /// implicit rollback on drop).
    pub fn active(&self) -> bool {
        self.active.get()
    }

    /// True if a commit/rollback (or the implicit rollback on drop) failed;
    /// mirrors the C++ `Transaction::poisoned`. The pool discards such a
    /// connection rather than reusing it. A standalone connection is condemned
    /// only when the autocommit/isolation restore itself failed — a failed
    /// terminal op with a clean restore (e.g. a deadlock victim) leaves the
    /// connection usable for re-driving the unit of work.
    pub fn poisoned(&self) -> bool {
        self.poisoned.get()
    }

    fn ensure_active(&self) -> Result<()> {
        if !self.active.get() {
            return Err(Error::new(ErrorCode::InvalidState, "transaction has ended"));
        }
        Ok(())
    }

    /// Executes a non-query statement within the unit of work. See
    /// [`Connection::execute`].
    pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<i64> {
        self.ensure_active()?;
        self.conn.execute(sql, params)
    }

    /// Executes positional values and LOB streams once within this transaction.
    pub fn execute_with_lobs(&self, sql: &str, params: Vec<BindParam<'_>>) -> Result<i64> {
        self.ensure_active()?;
        self.conn.execute_with_lobs(sql, params)
    }

    /// Runs a streaming query within the unit of work. The returned cursor
    /// borrows the transaction, so it must be finished (dropped) before
    /// commit/rollback. See [`Connection::query`].
    pub fn query(&self, sql: &str, params: impl IntoParams) -> Result<ResultSet<'_>> {
        self.ensure_active()?;
        self.conn.query(sql, params)
    }

    /// Opens a row-at-a-time streaming cursor within this transaction.
    pub fn query_streaming(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> Result<StreamingResultSet<'_>> {
        self.ensure_active()?;
        self.conn.query_streaming(sql, params)
    }

    /// Runs a query and maps every row into `T`. See [`Connection::query_as`].
    pub fn query_as<T: FromRow>(&self, sql: &str, params: impl IntoParams) -> Result<Vec<T>> {
        self.ensure_active()?;
        self.conn.query_as(sql, params)
    }

    /// Batch insert within the unit of work. See [`Connection::execute_batch`].
    pub fn execute_batch(&self, sql: &str, rows: &[Vec<Value>]) -> Result<i64> {
        self.ensure_active()?;
        self.conn.execute_batch(sql, rows)
    }

    /// Creates a savepoint with an auto-generated `halcyon_sp_<n>` name.
    pub fn savepoint(&self) -> Result<Savepoint<'_>> {
        auto_savepoint(self)
    }

    /// Creates a savepoint named by the validated, unquoted identifier `name`.
    pub fn savepoint_named(&self, name: &str) -> Result<Savepoint<'_>> {
        named_savepoint(self, name)
    }

    /// Runs `f` in a savepoint scope. Successful work is released; a returned
    /// error is rolled back while preserving that original error.
    pub fn nested<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Self) -> Result<T>,
    {
        nested_savepoint(self, || f(self))
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
        if !self.active.get() {
            return Ok(());
        }
        let (result, state) = terminate(
            self.conn.driver_ref(),
            self.conn.handle(),
            matches!(outcome, Outcome::Commit),
            self.restore_isolation,
        );
        self.active.set(false);
        self.conn.transaction_active.set(false);
        if state.poisoned {
            self.poison();
        }
        if state.session_lost {
            // Autocommit/isolation state is unknown; a failed terminal op with
            // a clean restore does NOT condemn the connection (see TerminalState).
            self.conn.mark_unusable();
        }
        result
    }

    fn poison(&self) {
        if !self.poisoned.replace(true) {
            crate::obs::emit_txn_poisoned();
        }
    }
}

impl SavepointOwner for Transaction<'_> {
    fn execute_savepoint(&self, sql: &str) -> Result<i64> {
        self.execute(sql, ())
    }

    fn savepoint_active(&self) -> bool {
        self.active.get()
    }

    fn poison_from_savepoint(&self) {
        // This mirrors C++ `Transaction::poison`: a failed savepoint terminal
        // operation makes the transaction untrustworthy, but does not itself
        // claim the standalone connection's session transition was lost.
        self.poison();
    }

    fn savepoint_state(&self) -> &SavepointState {
        &self.savepoints
    }
}

/// How a terminal transaction op left the session, alongside its result.
pub(crate) struct TerminalState {
    /// Any failure occurred: the transaction is untrustworthy, so it is
    /// poisoned and a pooled lease is discarded rather than reused — the C++
    /// `Transaction::poisoned` contract.
    pub(crate) poisoned: bool,
    /// The autocommit/isolation restore itself failed, leaving the session
    /// transition in an unknown state. Only this condemns a standalone
    /// connection: a failed commit/rollback alone (e.g. a deadlock victim)
    /// leaves a cleanly restored session the caller can re-drive.
    pub(crate) session_lost: bool,
}

/// Runs the terminal transaction op (commit if `commit`, else rollback) then
/// restores autocommit — the exact order the C++ `Transaction::commit`/`rollback`
/// use. Returns the op's error if it failed, otherwise the autocommit-restore
/// result, plus the [`TerminalState`] the session was left in. Shared by
/// [`Transaction`] and the facade's owned `ScopedTransaction`, which both wrap
/// it with their own `active`/`poisoned` bookkeeping.
pub(crate) fn terminate(
    driver: &dyn CliDriver,
    handle: ConnectionHandle,
    commit: bool,
    restore_isolation: Option<Isolation>,
) -> (Result<()>, TerminalState) {
    let ended = if commit {
        driver.commit(handle)
    } else {
        driver.rollback(handle)
    };
    let restored = driver.set_auto_commit(handle, true);
    let isolation_restored = match restore_isolation {
        Some(level) => driver.set_isolation(handle, level).map_err(|error| {
            crate::obs::emit_isolation_restore_fail(&error);
            error
        }),
        None => Ok(()),
    };
    let state = TerminalState {
        poisoned: ended.is_err() || restored.is_err() || isolation_restored.is_err(),
        session_lost: restored.is_err() || isolation_restored.is_err(),
    };
    (ended.and(restored).and(isolation_restored), state)
}

/// Outcome of setting a temporary isolation level before disabling autocommit.
/// The caller owns the physical connection and decides whether an uncertain
/// transition poisons a standalone connection or breaks a pooled lease.
pub(crate) enum IsolationBegin {
    Started,
    OverrideFailed(Error),
    AutoCommitFailed(Error),
}

pub(crate) fn start_with_isolation(
    driver: &dyn CliDriver,
    handle: ConnectionHandle,
    level: Isolation,
    restore_target: Isolation,
) -> IsolationBegin {
    if let Err(e) = driver.set_isolation(handle, level) {
        return IsolationBegin::OverrideFailed(e);
    }
    if let Err(e) = driver.set_auto_commit(handle, false) {
        // The original autocommit error remains observable. Even a successful
        // compensation cannot prove the session transition was reliable.
        let _ = driver.set_isolation(handle, restore_target);
        return IsolationBegin::AutoCommitFailed(e);
    }
    IsolationBegin::Started
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
            .field("active", &self.active.get())
            .field("poisoned", &self.poisoned.get())
            .finish_non_exhaustive()
    }
}

impl Connection {
    fn ensure_can_begin(&self) -> Result<()> {
        self.ensure_usable()?;
        if self.transaction_active.get() {
            return Err(Error::new(
                ErrorCode::InvalidState,
                "connection already has an active transaction; use a savepoint for nested work",
            ));
        }
        Ok(())
    }

    /// Begins a transaction: disables autocommit and returns a [`Transaction`]
    /// bound to this connection. If enabling manual commit fails, no transaction
    /// is created and the error is returned. An overlapping transaction is
    /// rejected with `InvalidState`; use a savepoint for nested work.
    pub fn begin(&self) -> Result<Transaction<'_>> {
        self.ensure_can_begin()?;
        self.driver_ref().set_auto_commit(self.handle(), false)?;
        Ok(Transaction::new(self, None))
    }

    /// Begins a transaction at `level`, restoring this connection's configured
    /// default (or Cursor Stability when none was configured) when it ends.
    pub fn begin_with_isolation(&self, level: Isolation) -> Result<Transaction<'_>> {
        self.ensure_can_begin()?;
        let restore_target = self.isolation_restore_target();
        match start_with_isolation(self.driver_ref(), self.handle(), level, restore_target) {
            IsolationBegin::Started => Ok(Transaction::new(self, Some(restore_target))),
            IsolationBegin::OverrideFailed(e) => Err(e),
            IsolationBegin::AutoCommitFailed(e) => {
                self.mark_unusable();
                Err(e)
            }
        }
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
            isolation: None,
        }
    }

    fn connect(mock: &Arc<MockCliDriver>) -> Connection {
        Connection::open(mock.clone(), &params()).unwrap()
    }

    #[test]
    fn overlapping_transactions_are_rejected_before_session_changes() {
        for isolation in [None, Some(Isolation::ReadStability)] {
            let mock = Arc::new(MockCliDriver::new());
            let conn = connect(&mock);
            let mut tx = match isolation {
                None => conn.begin().unwrap(),
                Some(level) => conn.begin_with_isolation(level).unwrap(),
            };
            let events = mock.transaction_events();
            assert_eq!(conn.begin().unwrap_err().code, ErrorCode::InvalidState);
            assert_eq!(
                conn.begin_with_isolation(Isolation::RepeatableRead)
                    .unwrap_err()
                    .code,
                ErrorCode::InvalidState
            );
            assert_eq!(mock.transaction_events(), events);
            assert!(tx.active());
            tx.commit().unwrap();
            let mut next = conn.begin().unwrap();
            next.rollback().unwrap();
            drop(conn.begin().unwrap());
            conn.begin_with_isolation(Isolation::ReadStability)
                .unwrap()
                .rollback()
                .unwrap();
        }
    }

    #[test]
    fn ended_transaction_rejects_every_data_operation() {
        for commit in [false, true] {
            let mock = Arc::new(MockCliDriver::new());
            let conn = connect(&mock);
            let mut tx = conn.begin().unwrap();
            if commit {
                tx.commit().unwrap();
            } else {
                tx.rollback().unwrap();
            }
            assert_eq!(
                tx.execute("INSERT INTO t VALUES (1)", ()).unwrap_err().code,
                ErrorCode::InvalidState
            );
            assert_eq!(
                tx.execute_with_lobs("INSERT INTO t VALUES (1)", vec![])
                    .unwrap_err()
                    .code,
                ErrorCode::InvalidState
            );
            assert_eq!(
                tx.execute_batch("INSERT INTO t VALUES (?)", &[vec![Value::Int(1)]])
                    .unwrap_err()
                    .code,
                ErrorCode::InvalidState
            );
            assert_eq!(
                tx.query("SELECT 1", ()).unwrap_err().code,
                ErrorCode::InvalidState
            );
            assert_eq!(
                tx.query_streaming("SELECT 1", ()).unwrap_err().code,
                ErrorCode::InvalidState
            );
            assert_eq!(
                tx.query_as::<(i64,)>("SELECT 1", ()).unwrap_err().code,
                ErrorCode::InvalidState
            );
            assert!(mock.prepared_sql().is_empty());
        }
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
    fn begin_with_isolation_sets_then_restores_on_commit() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);

        let mut tx = conn
            .begin_with_isolation(crate::Isolation::UncommittedRead)
            .unwrap();
        tx.commit().unwrap();

        // No configured default normalizes to CursorStability on restore.
        assert_eq!(
            mock.isolation_calls(),
            vec![
                crate::Isolation::UncommittedRead,
                crate::Isolation::CursorStability
            ]
        );
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
        assert_eq!(mock.commit_calls(), 1);
        assert_eq!(
            mock.transaction_events(),
            vec![
                crate::testing::TxnEvent::SetIsolation(crate::Isolation::UncommittedRead),
                crate::testing::TxnEvent::SetAutoCommit(false),
                crate::testing::TxnEvent::Commit,
                crate::testing::TxnEvent::SetAutoCommit(true),
                crate::testing::TxnEvent::SetIsolation(crate::Isolation::CursorStability),
            ]
        );
    }

    #[test]
    fn begin_with_isolation_restores_configured_default() {
        let mock = Arc::new(MockCliDriver::new());
        let params = ConnectionParams {
            connection_string: "DSN=mock".into(),
            isolation: Some(crate::Isolation::ReadStability),
        };
        let conn = Connection::open(mock.clone(), &params).unwrap();

        let mut tx = conn
            .begin_with_isolation(crate::Isolation::UncommittedRead)
            .unwrap();
        tx.commit().unwrap();

        assert_eq!(
            mock.isolation_calls(),
            vec![
                crate::Isolation::ReadStability,
                crate::Isolation::UncommittedRead,
                crate::Isolation::ReadStability
            ]
        );
    }

    #[test]
    fn dropped_isolation_transaction_restores_default_after_rollback() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);

        {
            let _tx = conn
                .begin_with_isolation(crate::Isolation::UncommittedRead)
                .unwrap();
        }

        assert_eq!(mock.rollback_calls(), 1);
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
        assert_eq!(
            mock.isolation_calls(),
            vec![
                crate::Isolation::UncommittedRead,
                crate::Isolation::CursorStability
            ]
        );
    }

    #[test]
    fn isolation_restore_failure_poisons_standalone_connection() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        mock.fail_isolation_on_call(
            2,
            Error::new(ErrorCode::Connection, "isolation restore failed"),
        );

        let mut tx = conn
            .begin_with_isolation(crate::Isolation::UncommittedRead)
            .unwrap();
        let error = tx.commit().unwrap_err();

        assert_eq!(error.code, ErrorCode::Connection);
        assert!(tx.poisoned());
        assert_eq!(
            conn.execute("UPDATE t SET x = 1", ()).unwrap_err().code,
            ErrorCode::InvalidState
        );
    }

    #[test]
    fn begin_autocommit_fail_restores_and_marks_standalone_unusable() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        mock.push_txn_error(Error::new(
            ErrorCode::Connection,
            "autocommit disabled failed",
        ));

        let error = conn
            .begin_with_isolation(crate::Isolation::UncommittedRead)
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Connection);
        assert_eq!(
            mock.isolation_calls(),
            vec![
                crate::Isolation::UncommittedRead,
                crate::Isolation::CursorStability
            ]
        );
        assert_eq!(mock.auto_commit_calls(), vec![false]);
        assert_eq!(
            conn.execute("UPDATE t SET x = 1", ()).unwrap_err().code,
            ErrorCode::InvalidState
        );
    }

    #[test]
    fn failed_isolation_override_leaves_standalone_connection_usable() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_isolation_error(Error::new(ErrorCode::Connection, "override failed"));
        let conn = connect(&mock);

        let error = conn
            .begin_with_isolation(crate::Isolation::UncommittedRead)
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Connection);
        assert_eq!(mock.auto_commit_calls(), Vec::<bool>::new());
        assert_eq!(conn.execute("UPDATE t SET x = 1", ()).unwrap(), 0);
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
    fn recoverable_commit_failure_keeps_standalone_connection_usable() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let mut tx = conn.begin().unwrap();
        // Only the terminal commit fails (a deadlock victim); the autocommit
        // restore that follows succeeds, so the session state is intact.
        mock.push_txn_error(Error::new(ErrorCode::Deadlock, "deadlock victim"));
        let e = tx.commit().unwrap_err();
        assert_eq!(e.code, ErrorCode::Deadlock);
        assert!(tx.poisoned());
        drop(tx);
        // The caller may re-drive the unit of work on the same connection.
        assert_eq!(conn.execute("UPDATE t SET x = 1", ()).unwrap(), 0);
    }

    #[test]
    fn lost_autocommit_restore_marks_standalone_connection_unusable() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let mut tx = conn.begin().unwrap();
        // The terminal commit fails AND the autocommit restore fails: the
        // session transition is unknown, so the connection must not be reused.
        mock.push_txn_error(Error::new(ErrorCode::Deadlock, "deadlock victim"));
        mock.push_txn_error(Error::new(ErrorCode::Connection, "restore failed"));
        let e = tx.commit().unwrap_err();
        assert_eq!(e.code, ErrorCode::Deadlock);
        assert!(tx.poisoned());
        assert_eq!(
            conn.execute("UPDATE t SET x = 1", ()).unwrap_err().code,
            ErrorCode::InvalidState
        );
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
