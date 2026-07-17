//! Shared test-support: a scriptable in-memory [`CliDriver`] so every layer
//! above the seam can be exercised with no live Db2 and no clidriver installed.
//!
//! Rust port of the C++ `halcyon::testing::MockCliDriver`
//! (`../Halcyon/tests/unit/mock_cli_driver.hpp`). Queue up connect / prepare /
//! execute outcomes and result grids, hand the mock to the code under test, and
//! afterwards inspect what it recorded.
//!
//! The one structural difference from the C++ header: the [`CliDriver`] trait is
//! `Send + Sync` with `&self` methods (the pool shares one driver across worker
//! threads), so the mutable script/record state lives behind a [`Mutex`] and is
//! reached through methods rather than C++'s public fields.
//!
//! The C++ `executeHook` / `connectHook` thread gates are intentionally not
//! ported yet — they only matter for the pool/async concurrency tests
//! (Phase 4/6) and are added there, where the lock interaction can be designed
//! and tested against a real consumer.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard};

use crate::driver::{CliDriver, ConnectionHandle, ConnectionParams, StatementHandle};
use crate::error::{Error, ErrorCode, Result};
use crate::value::Value;
use crate::Isolation;

/// A scripted result grid that [`execute`](CliDriver::execute) attaches to a
/// statement's cursor; `fetch_block` then returns it as row-major blocks.
#[derive(Debug, Clone, Default)]
pub struct ScriptedRows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl ScriptedRows {
    /// Builds a grid from column names and a row-major set of values.
    pub fn new<C, S>(columns: C, rows: Vec<Vec<Value>>) -> Self
    where
        C: IntoIterator<Item = S>,
        S: Into<String>,
    {
        ScriptedRows {
            columns: columns.into_iter().map(Into::into).collect(),
            rows,
        }
    }
}

/// Ordered transaction-control calls recorded by [`MockCliDriver`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnEvent {
    SetAutoCommit(bool),
    SetIsolation(Isolation),
    Commit,
    Rollback,
}

/// Per-statement record: the SQL it was prepared with, the last bound params,
/// the attached cursor, and the next unread row (`-1` before the first block).
#[derive(Debug, Default)]
struct StmtState {
    #[allow(dead_code)] // recorded for tests that assert on prepared SQL per handle
    sql: String,
    bound_params: Vec<Value>,
    cursor: ScriptedRows,
    position: i64,
}

/// All scriptable inputs and recorded outputs, guarded by the mock's mutex.
#[derive(Default)]
struct State {
    // --- connect scripting ---
    connect_errors: VecDeque<Error>,
    alive_results: VecDeque<bool>,
    connect_calls: u32,
    disconnect_calls: u32,
    alive_calls: u32,
    connect_params: Vec<ConnectionParams>,

    // --- statement scripting ---
    prepare_errors: VecDeque<Error>,
    execute_errors: VecDeque<Error>,
    result_sets: VecDeque<ScriptedRows>,
    exec_row_counts: VecDeque<i64>,
    prepared_sql: Vec<String>,
    statements: HashMap<u64, StmtState>,

    // --- batch scripting (array binding) ---
    batch_row_counts: VecDeque<i64>,
    batch_errors: VecDeque<Error>,
    execute_batch_calls: u32,
    last_batch_rows: Vec<Vec<Value>>,

    // --- block-fetch scripting ---
    fetch_block_error: Option<Error>,
    fail_fetch_block_on_call: u32, // 1-based fetch_block() index; 0 = never
    fetch_block_calls: u32,
    fetch_block_size: usize, // 0 => caller's max_rows; >0 => cap per call
    unbounded_columns: bool, // long/LOB mode caps each successful block at one row
    fetch_block_requests: Vec<usize>,

    // --- finalize / close_cursor ---
    finalize_calls: u32,
    finalized: Vec<StatementHandle>,
    close_cursor_calls: u32,
    close_cursor_errors: VecDeque<Error>,
    /// Ordered log of lifecycle calls whose relative order tests assert on
    /// (statement finalize vs. connection disconnect).
    events: Vec<&'static str>,

    // --- transaction scripting ---
    txn_errors: VecDeque<Error>,
    isolation_errors: VecDeque<Error>,
    fail_isolation_on_call: Option<(u32, Error)>,
    auto_commit_calls: Vec<bool>,
    isolation_calls: Vec<Isolation>,
    commit_calls: u32,
    rollback_calls: u32,
    transaction_events: Vec<TxnEvent>,

    next_conn: u64,
    next_stmt: u64,
}

impl State {
    /// Pops the next scripted transaction outcome (used by
    /// set_auto_commit/commit/rollback), defaulting to success.
    fn next_txn_result(&mut self) -> Result<()> {
        match self.txn_errors.pop_front() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// A scriptable fake [`CliDriver`]. Share it as `Arc<MockCliDriver>`: the
/// scripting/inspection methods and the trait methods all take `&self`.
#[derive(Default)]
pub struct MockCliDriver {
    state: Mutex<State>,
}

fn unknown_stmt() -> Error {
    Error::new(ErrorCode::Mapping, "unknown statement handle")
}

fn range_error() -> Error {
    Error::new(ErrorCode::Mapping, "column index out of range")
}

impl MockCliDriver {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("mock driver mutex poisoned")
    }

    // --- scripting (chainable) ------------------------------------------------

    /// The next `connect` fails with `e`.
    pub fn push_connect_error(&self, e: Error) -> &Self {
        self.lock().connect_errors.push_back(e);
        self
    }

    /// The next `is_alive` returns `alive` (default when unscripted: `true`).
    pub fn push_alive_result(&self, alive: bool) -> &Self {
        self.lock().alive_results.push_back(alive);
        self
    }

    /// The next `prepare` fails with `e`.
    pub fn push_prepare_error(&self, e: Error) -> &Self {
        self.lock().prepare_errors.push_back(e);
        self
    }

    /// The next `execute` fails with `e`.
    pub fn push_execute_error(&self, e: Error) -> &Self {
        self.lock().execute_errors.push_back(e);
        self
    }

    /// The next `execute` attaches `rows` as the statement's result cursor and
    /// returns a rows-affected count of 0.
    pub fn push_result_set(&self, rows: ScriptedRows) -> &Self {
        self.lock().result_sets.push_back(rows);
        self
    }

    /// The next `execute` with no scripted result set returns `n` rows affected.
    pub fn push_exec_row_count(&self, n: i64) -> &Self {
        self.lock().exec_row_counts.push_back(n);
        self
    }

    /// The next `execute_batch` returns `n` (default when unscripted: row count).
    pub fn push_batch_row_count(&self, n: i64) -> &Self {
        self.lock().batch_row_counts.push_back(n);
        self
    }

    /// The next `execute_batch` fails with `e`.
    pub fn push_batch_error(&self, e: Error) -> &Self {
        self.lock().batch_errors.push_back(e);
        self
    }

    /// The next `close_cursor` fails with `e` (after resetting the cursor).
    pub fn push_close_cursor_error(&self, e: Error) -> &Self {
        self.lock().close_cursor_errors.push_back(e);
        self
    }

    /// The next `set_auto_commit` / `commit` / `rollback` fails with `e`.
    pub fn push_txn_error(&self, e: Error) -> &Self {
        self.lock().txn_errors.push_back(e);
        self
    }

    /// The next `set_isolation` fails with `e`.
    pub fn push_isolation_error(&self, e: Error) -> &Self {
        self.lock().isolation_errors.push_back(e);
        self
    }

    /// Makes the `call_index`-th (1-based) `set_isolation` fail with `e`.
    pub fn fail_isolation_on_call(&self, call_index: u32, e: Error) -> &Self {
        self.lock().fail_isolation_on_call = Some((call_index, e));
        self
    }

    /// Makes the `call_index`-th (1-based) `fetch_block` fail with `e`.
    pub fn fail_fetch_block_on_call(&self, call_index: u32, e: Error) -> &Self {
        let mut s = self.lock();
        s.fail_fetch_block_on_call = call_index;
        s.fetch_block_error = Some(e);
        drop(s);
        self
    }

    /// Caps successful blocks to `rows`, allowing portable-layer tests to make
    /// multiple block requests without fabricating driver behavior.
    pub fn set_fetch_block_size(&self, rows: usize) -> &Self {
        self.lock().fetch_block_size = rows;
        self
    }

    /// Models a result set containing a long/LOB column: the real Db2 driver
    /// intentionally uses the portable one-row fallback for such statements.
    pub fn set_unbounded_columns(&self, enabled: bool) -> &Self {
        self.lock().unbounded_columns = enabled;
        self
    }

    // --- inspection -----------------------------------------------------------

    pub fn connect_calls(&self) -> u32 {
        self.lock().connect_calls
    }
    pub fn disconnect_calls(&self) -> u32 {
        self.lock().disconnect_calls
    }
    pub fn alive_calls(&self) -> u32 {
        self.lock().alive_calls
    }
    pub fn connect_params(&self) -> Vec<ConnectionParams> {
        self.lock().connect_params.clone()
    }
    pub fn prepared_sql(&self) -> Vec<String> {
        self.lock().prepared_sql.clone()
    }
    /// The params last bound to `stmt` (empty if never bound / unknown handle).
    pub fn bound_params(&self, stmt: StatementHandle) -> Vec<Value> {
        self.lock()
            .statements
            .get(&stmt.0)
            .map(|s| s.bound_params.clone())
            .unwrap_or_default()
    }
    pub fn execute_batch_calls(&self) -> u32 {
        self.lock().execute_batch_calls
    }
    pub fn last_batch_rows(&self) -> Vec<Vec<Value>> {
        self.lock().last_batch_rows.clone()
    }
    pub fn finalize_calls(&self) -> u32 {
        self.lock().finalize_calls
    }
    pub fn finalized(&self) -> Vec<StatementHandle> {
        self.lock().finalized.clone()
    }
    pub fn close_cursor_calls(&self) -> u32 {
        self.lock().close_cursor_calls
    }
    /// Ordered finalize/disconnect log — asserts teardown ordering.
    pub fn events(&self) -> Vec<&'static str> {
        self.lock().events.clone()
    }
    pub fn commit_calls(&self) -> u32 {
        self.lock().commit_calls
    }
    pub fn rollback_calls(&self) -> u32 {
        self.lock().rollback_calls
    }
    pub fn auto_commit_calls(&self) -> Vec<bool> {
        self.lock().auto_commit_calls.clone()
    }
    pub fn isolation_calls(&self) -> Vec<Isolation> {
        self.lock().isolation_calls.clone()
    }
    pub fn transaction_events(&self) -> Vec<TxnEvent> {
        self.lock().transaction_events.clone()
    }
    pub fn fetch_block_calls(&self) -> u32 {
        self.lock().fetch_block_calls
    }
    pub fn fetch_block_requests(&self) -> Vec<usize> {
        self.lock().fetch_block_requests.clone()
    }
}

impl CliDriver for MockCliDriver {
    fn connect(&self, params: &ConnectionParams) -> Result<ConnectionHandle> {
        let mut s = self.lock();
        s.connect_calls += 1;
        s.connect_params.push(params.clone());
        if let Some(e) = s.connect_errors.pop_front() {
            return Err(e);
        }
        s.next_conn += 1;
        Ok(ConnectionHandle(s.next_conn))
    }

    fn disconnect(&self, _conn: ConnectionHandle) -> Result<()> {
        let mut s = self.lock();
        s.disconnect_calls += 1;
        s.events.push("disconnect");
        Ok(())
    }

    fn is_alive(&self, _conn: ConnectionHandle) -> Result<bool> {
        let mut s = self.lock();
        s.alive_calls += 1;
        Ok(s.alive_results.pop_front().unwrap_or(true))
    }

    fn prepare(&self, _conn: ConnectionHandle, sql: &str) -> Result<StatementHandle> {
        let mut s = self.lock();
        if let Some(e) = s.prepare_errors.pop_front() {
            return Err(e);
        }
        s.prepared_sql.push(sql.to_owned());
        s.next_stmt += 1;
        let h = s.next_stmt;
        s.statements.insert(
            h,
            StmtState {
                sql: sql.to_owned(),
                position: -1,
                ..Default::default()
            },
        );
        Ok(StatementHandle(h))
    }

    fn bind_params(&self, stmt: StatementHandle, params: &[Value]) -> Result<()> {
        let mut s = self.lock();
        let st = s.statements.get_mut(&stmt.0).ok_or_else(unknown_stmt)?;
        st.bound_params = params.to_vec();
        Ok(())
    }

    fn execute(&self, stmt: StatementHandle) -> Result<i64> {
        let mut s = self.lock();
        if let Some(e) = s.execute_errors.pop_front() {
            return Err(e);
        }
        // Pop the scripted outputs before borrowing the statement mutably.
        let next_rs = s.result_sets.pop_front();
        let scripted_count = if next_rs.is_none() {
            s.exec_row_counts.pop_front()
        } else {
            None
        };
        let st = s.statements.get_mut(&stmt.0).ok_or_else(unknown_stmt)?;
        st.position = -1;
        match next_rs {
            Some(rs) => {
                st.cursor = rs;
                Ok(0)
            }
            None => {
                st.cursor = ScriptedRows::default();
                Ok(scripted_count.unwrap_or(0))
            }
        }
    }

    fn execute_batch(&self, _stmt: StatementHandle, rows: &[Vec<Value>]) -> Result<i64> {
        let mut s = self.lock();
        s.execute_batch_calls += 1;
        s.last_batch_rows = rows.to_vec();
        if let Some(e) = s.batch_errors.pop_front() {
            return Err(e);
        }
        Ok(s.batch_row_counts.pop_front().unwrap_or(rows.len() as i64))
    }

    fn column_count(&self, stmt: StatementHandle) -> Result<usize> {
        let s = self.lock();
        let st = s.statements.get(&stmt.0).ok_or_else(unknown_stmt)?;
        Ok(st.cursor.columns.len())
    }

    fn column_name(&self, stmt: StatementHandle, index: usize) -> Result<String> {
        let s = self.lock();
        let st = s.statements.get(&stmt.0).ok_or_else(unknown_stmt)?;
        st.cursor
            .columns
            .get(index)
            .cloned()
            .ok_or_else(range_error)
    }

    fn fetch_block(&self, stmt: StatementHandle, max_rows: usize) -> Result<Vec<Vec<Value>>> {
        let mut s = self.lock();
        s.fetch_block_calls += 1;
        s.fetch_block_requests.push(max_rows);
        if s.fail_fetch_block_on_call != 0 && s.fetch_block_calls == s.fail_fetch_block_on_call {
            return Err(s.fetch_block_error.clone().unwrap_or_else(|| {
                Error::new(ErrorCode::Unknown, "scripted fetch_block failure")
            }));
        }
        let mut limit = match s.fetch_block_size {
            0 => max_rows,
            cap => cap.min(max_rows),
        };
        if s.unbounded_columns {
            limit = limit.min(1);
        }
        let st = s.statements.get_mut(&stmt.0).ok_or_else(unknown_stmt)?;
        let start = (st.position + 1) as usize;
        let end = start.saturating_add(limit).min(st.cursor.rows.len());
        let block = st.cursor.rows[start..end].to_vec();
        st.position = end as i64 - 1;
        Ok(block)
    }

    fn finalize(&self, stmt: StatementHandle) -> Result<()> {
        // Mirror C++: retain the StmtState so tests can still introspect bound
        // params after the owning Statement is dropped (handles never alias).
        let mut s = self.lock();
        s.finalize_calls += 1;
        s.events.push("finalize");
        s.finalized.push(stmt);
        Ok(())
    }

    fn close_cursor(&self, stmt: StatementHandle) -> Result<()> {
        let mut s = self.lock();
        s.close_cursor_calls += 1;
        if let Some(st) = s.statements.get_mut(&stmt.0) {
            st.position = -1; // cursor reset
        }
        if let Some(e) = s.close_cursor_errors.pop_front() {
            return Err(e);
        }
        Ok(())
    }

    fn set_auto_commit(&self, _conn: ConnectionHandle, enabled: bool) -> Result<()> {
        let mut s = self.lock();
        s.auto_commit_calls.push(enabled);
        s.transaction_events.push(TxnEvent::SetAutoCommit(enabled));
        s.next_txn_result()
    }

    fn set_isolation(&self, _conn: ConnectionHandle, level: Isolation) -> Result<()> {
        let mut s = self.lock();
        s.isolation_calls.push(level);
        s.transaction_events.push(TxnEvent::SetIsolation(level));
        if let Some(e) = s.isolation_errors.pop_front() {
            return Err(e);
        }
        if s.fail_isolation_on_call
            .as_ref()
            .is_some_and(|(call_index, _)| *call_index == s.isolation_calls.len() as u32)
        {
            if let Some((_, e)) = s.fail_isolation_on_call.take() {
                return Err(e);
            }
        }
        Ok(())
    }

    fn commit(&self, _conn: ConnectionHandle) -> Result<()> {
        let mut s = self.lock();
        s.commit_calls += 1;
        s.transaction_events.push(TxnEvent::Commit);
        s.next_txn_result()
    }

    fn rollback(&self, _conn: ConnectionHandle) -> Result<()> {
        let mut s = self.lock();
        s.rollback_calls += 1;
        s.transaction_events.push(TxnEvent::Rollback);
        s.next_txn_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::{Connection, Isolation};

    fn conn_params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "DSN=mock".into(),
            isolation: None,
        }
    }

    #[test]
    fn mock_records_set_isolation_calls() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = Connection::open(mock.clone(), &conn_params()).unwrap();
        mock.set_isolation(conn.handle(), Isolation::UncommittedRead)
            .unwrap();
        assert_eq!(mock.isolation_calls(), vec![Isolation::UncommittedRead]);
    }

    #[test]
    fn connect_hands_out_increasing_valid_handles_and_records_params() {
        let m = MockCliDriver::new();
        let a = m.connect(&conn_params()).unwrap();
        let b = m.connect(&conn_params()).unwrap();
        assert!(a.is_valid() && b.is_valid());
        assert_ne!(a, b);
        assert_eq!(m.connect_calls(), 2);
        assert_eq!(m.connect_params().len(), 2);
    }

    #[test]
    fn scripted_connect_error_is_returned_once_then_recovers() {
        let m = MockCliDriver::new();
        m.push_connect_error(Error::new(ErrorCode::Connection, "boom"));
        assert_eq!(
            m.connect(&conn_params()).unwrap_err().code,
            ErrorCode::Connection
        );
        // Next connect succeeds — the scripted error is consumed.
        assert!(m.connect(&conn_params()).unwrap().is_valid());
    }

    #[test]
    fn is_alive_defaults_true_and_honors_script() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        m.push_alive_result(false).push_alive_result(true);
        assert!(!m.is_alive(c).unwrap());
        assert!(m.is_alive(c).unwrap());
        assert!(m.is_alive(c).unwrap()); // default once script drains
        assert_eq!(m.alive_calls(), 3);
    }

    #[test]
    fn prepare_records_sql_and_bind_is_introspectable() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT 1 FROM t WHERE id = ?").unwrap();
        m.bind_params(s, &[Value::Int(42)]).unwrap();
        assert_eq!(m.prepared_sql(), vec!["SELECT 1 FROM t WHERE id = ?"]);
        assert_eq!(m.bound_params(s), vec![Value::Int(42)]);
    }

    #[test]
    fn fetch_block_returns_rows_then_empty_eof() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT id, name FROM t").unwrap();
        m.push_result_set(ScriptedRows::new(
            ["id", "name"],
            vec![
                vec![Value::Int(1), Value::Text("a".into())],
                vec![Value::Int(2), Value::Text("b".into())],
            ],
        ));
        assert_eq!(m.execute(s).unwrap(), 0); // result-set execute reports 0
        assert_eq!(m.column_count(s).unwrap(), 2);
        assert_eq!(m.column_name(s, 1).unwrap(), "name");

        let block = m.fetch_block(s, 100).unwrap();
        assert_eq!(
            block,
            vec![
                vec![Value::Int(1), Value::Text("a".into())],
                vec![Value::Int(2), Value::Text("b".into())],
            ]
        );
        assert!(m.fetch_block(s, 100).unwrap().is_empty()); // exhausted
    }

    #[test]
    fn fetch_block_honors_max_rows_and_resumes() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT id FROM t").unwrap();
        m.push_result_set(ScriptedRows::new(
            ["id"],
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)],
            ],
        ));
        m.execute(s).unwrap();
        assert_eq!(
            m.fetch_block(s, 2).unwrap(),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
        assert_eq!(m.fetch_block(s, 2).unwrap(), vec![vec![Value::Int(3)]]);
        assert!(m.fetch_block(s, 2).unwrap().is_empty());
    }

    #[test]
    fn mock_unbounded_fallback_returns_at_most_one_row() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT id, body FROM t").unwrap();
        m.push_result_set(ScriptedRows::new(
            ["id", "body"],
            (1..=5)
                .map(|id| vec![Value::Int(id), Value::Text(format!("lob-{id}"))])
                .collect(),
        ));
        m.set_unbounded_columns(true);
        m.execute(s).unwrap();

        for id in 1..=5 {
            assert_eq!(
                m.fetch_block(s, 10).unwrap(),
                vec![vec![Value::Int(id), Value::Text(format!("lob-{id}")),]]
            );
        }
        assert!(m.fetch_block(s, 10).unwrap().is_empty());
    }

    #[test]
    fn execute_without_result_set_returns_scripted_row_count() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "UPDATE t SET x = 1").unwrap();
        m.push_exec_row_count(3);
        assert_eq!(m.execute(s).unwrap(), 3);
        assert_eq!(m.column_count(s).unwrap(), 0);
        // Unscripted execute defaults to 0 rows affected.
        assert_eq!(m.execute(s).unwrap(), 0);
    }

    #[test]
    fn fetch_block_error_injection_fires_on_the_nth_call() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT id FROM t").unwrap();
        m.push_result_set(ScriptedRows::new(
            ["id"],
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ));
        m.execute(s).unwrap();
        m.set_fetch_block_size(1);
        m.fail_fetch_block_on_call(2, Error::new(ErrorCode::Connection, "dropped mid-scan"));
        assert_eq!(m.fetch_block(s, 100).unwrap(), vec![vec![Value::Int(1)]]); // call 1 ok
        assert_eq!(
            m.fetch_block(s, 100).unwrap_err().code,
            ErrorCode::Connection
        ); // call 2 fails
    }

    #[test]
    fn execute_batch_records_rows_and_returns_scripted_count() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "INSERT INTO t VALUES (?)").unwrap();
        let rows = vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ];
        // Unscripted: returns the row count.
        assert_eq!(m.execute_batch(s, &rows).unwrap(), 3);
        assert_eq!(m.execute_batch_calls(), 1);
        assert_eq!(m.last_batch_rows(), rows);
        // Scripted count wins.
        m.push_batch_row_count(99);
        assert_eq!(m.execute_batch(s, &rows).unwrap(), 99);
    }

    #[test]
    fn close_cursor_resets_position_and_can_be_scripted_to_fail() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT id FROM t").unwrap();
        m.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(1)]]));
        m.execute(s).unwrap();
        assert_eq!(m.fetch_block(s, 1).unwrap(), vec![vec![Value::Int(1)]]);
        m.close_cursor(s).unwrap();
        assert_eq!(m.close_cursor_calls(), 1);
        // After reset, block fetch starts over from the top.
        assert_eq!(m.fetch_block(s, 1).unwrap(), vec![vec![Value::Int(1)]]);

        m.push_close_cursor_error(Error::new(ErrorCode::Connection, "reset failed"));
        assert_eq!(m.close_cursor(s).unwrap_err().code, ErrorCode::Connection);
    }

    #[test]
    fn finalize_is_recorded_and_state_survives_for_introspection() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT 1").unwrap();
        m.bind_params(s, &[Value::Int(5)]).unwrap();
        m.finalize(s).unwrap();
        assert_eq!(m.finalize_calls(), 1);
        assert_eq!(m.finalized(), vec![s]);
        // Bound params still introspectable after finalize.
        assert_eq!(m.bound_params(s), vec![Value::Int(5)]);
    }

    #[test]
    fn transaction_calls_are_counted_and_scriptable() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        m.set_auto_commit(c, false).unwrap();
        m.commit(c).unwrap();
        m.set_auto_commit(c, true).unwrap();
        assert_eq!(m.auto_commit_calls(), vec![false, true]);
        assert_eq!(m.commit_calls(), 1);

        m.push_txn_error(Error::new(ErrorCode::Deadlock, "40001"));
        assert_eq!(m.rollback(c).unwrap_err().code, ErrorCode::Deadlock);
        assert_eq!(m.rollback_calls(), 1);
    }

    #[test]
    fn scripted_prepare_and_execute_errors_fire_once() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        m.push_prepare_error(Error::new(ErrorCode::Syntax, "42601"));
        assert_eq!(
            m.prepare(c, "SELEKT 1").unwrap_err().code,
            ErrorCode::Syntax
        );
        // Recovers on the next prepare.
        let s = m.prepare(c, "SELECT 1").unwrap();
        m.push_execute_error(Error::new(ErrorCode::Timeout, "57014"));
        assert_eq!(m.execute(s).unwrap_err().code, ErrorCode::Timeout);
        assert_eq!(m.execute(s).unwrap(), 0); // scripted error consumed
    }

    #[test]
    fn scripted_batch_error_is_returned() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "INSERT INTO t VALUES (?)").unwrap();
        m.push_batch_error(Error::new(ErrorCode::Constraint, "23505"));
        let rows = vec![vec![Value::Int(1)]];
        assert_eq!(
            m.execute_batch(s, &rows).unwrap_err().code,
            ErrorCode::Constraint
        );
        // The rows are still recorded even on a failed batch.
        assert_eq!(m.execute_batch_calls(), 1);
        assert_eq!(m.last_batch_rows(), rows);
    }

    #[test]
    fn fetch_block_records_requested_sizes() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT a FROM t").unwrap();
        m.push_result_set(ScriptedRows::new(["a"], vec![vec![Value::Int(1)]]));
        m.execute(s).unwrap();
        assert_eq!(m.fetch_block(s, 7).unwrap(), vec![vec![Value::Int(1)]]);
        assert!(m.fetch_block(s, 9).unwrap().is_empty());
        assert_eq!(m.fetch_block_requests(), vec![7, 9]);
    }

    #[test]
    fn event_log_records_finalize_and_disconnect_order() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT 1").unwrap();
        m.finalize(s).unwrap();
        m.disconnect(c).unwrap();
        assert_eq!(m.events(), vec!["finalize", "disconnect"]);
    }

    #[test]
    fn disconnect_is_counted() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        m.disconnect(c).unwrap();
        assert_eq!(m.disconnect_calls(), 1);
    }

    #[test]
    fn is_send_and_sync_so_it_can_back_an_arc_dyn_clidriver() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockCliDriver>();
        // And it coerces to the shared trait object the upper layers will hold.
        let _shared: std::sync::Arc<dyn CliDriver> = std::sync::Arc::new(MockCliDriver::new());
    }
}
