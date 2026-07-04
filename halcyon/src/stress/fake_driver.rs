//! A thread-safe, deterministic `CliDriver` for concurrency testing. Unlike the
//! FIFO-scripted `MockCliDriver`, every response is derived from its inputs, so
//! any number of threads may call it with no ordering assumptions. Counters are
//! atomics; the handle maps are guarded by one mutex (the fake is intentionally
//! a little pessimistic — the contention we measure lives in the pool, not here).
//! Port of `../Halcyon/tests/stress/support/concurrent_fake_driver.hpp`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use crate::driver::{CliDriver, ConnectionHandle, ConnectionParams, StatementHandle};
use crate::error::{Error, ErrorCode, Result};
use crate::value::Value;

struct StmtState {
    conn: u64,
    sql: String,
    position: i64,
}

#[derive(Default)]
struct State {
    live: HashSet<u64>,
    dead: HashSet<u64>,
    stmts: HashMap<u64, StmtState>,
    next_conn: u64,
    next_stmt: u64,
}

/// Deterministic, thread-safe fake driver. Construct with [`new`](Self::new),
/// tune the atomic fault knobs, share via `Arc`, and read the atomic counters
/// after a run.
#[derive(Default)]
pub struct ConcurrentFakeDriver {
    state: Mutex<State>,

    // --- fault injection (1-in-N; 0 disables) ---
    pub fail_connect_every: AtomicI64,
    pub fail_execute_every: AtomicI64,
    pub kill_connection_every: AtomicI64,

    // --- counters ---
    connect_calls: AtomicI64,
    disconnect_calls: AtomicI64,
    prepare_calls: AtomicI64,
    execute_calls: AtomicI64,
    alive_calls: AtomicI64,
    finalize_calls: AtomicI64,
    close_cursor_calls: AtomicI64,
    commit_calls: AtomicI64,
    rollback_calls: AtomicI64,
    auto_commit_on: AtomicI64,
    auto_commit_off: AtomicI64,
    in_flight: AtomicI64,
    peak_in_flight: AtomicI64,
}

/// Decrements `in_flight` when an in-flight execute finishes (any path).
struct InFlightGuard<'a>(&'a AtomicI64);
impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ConcurrentFakeDriver {
    pub fn new() -> Self {
        Self::default()
    }

    // --- counter accessors (loaded snapshots) ---
    pub fn connect_calls(&self) -> i64 {
        self.connect_calls.load(Ordering::Relaxed)
    }
    pub fn disconnect_calls(&self) -> i64 {
        self.disconnect_calls.load(Ordering::Relaxed)
    }
    pub fn prepare_calls(&self) -> i64 {
        self.prepare_calls.load(Ordering::Relaxed)
    }
    pub fn execute_calls(&self) -> i64 {
        self.execute_calls.load(Ordering::Relaxed)
    }
    pub fn commit_calls(&self) -> i64 {
        self.commit_calls.load(Ordering::Relaxed)
    }
    pub fn rollback_calls(&self) -> i64 {
        self.rollback_calls.load(Ordering::Relaxed)
    }
    pub fn auto_commit_on(&self) -> i64 {
        self.auto_commit_on.load(Ordering::Relaxed)
    }
    pub fn auto_commit_off(&self) -> i64 {
        self.auto_commit_off.load(Ordering::Relaxed)
    }
    pub fn peak_in_flight(&self) -> i64 {
        self.peak_in_flight.load(Ordering::Relaxed)
    }

    fn enter_in_flight(&self) -> InFlightGuard<'_> {
        let cur = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        let mut prev = self.peak_in_flight.load(Ordering::Relaxed);
        while cur > prev {
            match self.peak_in_flight.compare_exchange_weak(
                prev,
                cur,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => prev = p,
            }
        }
        InFlightGuard(&self.in_flight)
    }
}

// Leading non-space token is SELECT or VALUES (the fake never sees real DML
// result sets; DML returns an affected-row count instead).
fn is_select(sql: &str) -> bool {
    let t = sql.trim_start();
    let up = t.get(..6.min(t.len())).unwrap_or("").to_ascii_uppercase();
    up.starts_with("SELECT") || t.trim_start().to_ascii_uppercase().starts_with("VALUES")
}

// The first contiguous run of digits in the SQL (e.g. "SELECT 7 FROM ..." -> 7).
fn encoded_value(sql: &str) -> i64 {
    let mut v: i64 = 0;
    let mut seen = false;
    for ch in sql.bytes() {
        if ch.is_ascii_digit() {
            v = v * 10 + i64::from(ch - b'0');
            seen = true;
        } else if seen {
            break;
        }
    }
    v
}

fn transient() -> Error {
    let mut e = Error::new(ErrorCode::Transient, "injected transient");
    e.sqlstate = "40001".into();
    e.retriable = true;
    e
}
fn stale() -> Error {
    let mut e = Error::new(ErrorCode::Connection, "statement used on a dead connection");
    e.sqlstate = "08003".into();
    e.retriable = true;
    e
}
fn mapping() -> Error {
    Error::new(ErrorCode::Mapping, "column index out of range")
}

impl CliDriver for ConcurrentFakeDriver {
    fn connect(&self, _params: &ConnectionParams) -> Result<ConnectionHandle> {
        let n = self.connect_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let every = self.fail_connect_every.load(Ordering::Relaxed);
        if every > 0 && n % every == 0 {
            return Err(transient());
        }
        let mut g = self.state.lock().unwrap();
        g.next_conn += 1;
        let h = g.next_conn;
        g.live.insert(h);
        Ok(ConnectionHandle(h))
    }

    fn disconnect(&self, conn: ConnectionHandle) -> Result<()> {
        self.disconnect_calls.fetch_add(1, Ordering::Relaxed);
        let mut g = self.state.lock().unwrap();
        g.live.remove(&conn.0);
        g.dead.remove(&conn.0);
        Ok(())
    }

    fn is_alive(&self, conn: ConnectionHandle) -> Result<bool> {
        self.alive_calls.fetch_add(1, Ordering::Relaxed);
        let g = self.state.lock().unwrap();
        if g.dead.contains(&conn.0) {
            return Ok(false);
        }
        Ok(g.live.contains(&conn.0))
    }

    fn prepare(&self, conn: ConnectionHandle, sql: &str) -> Result<StatementHandle> {
        self.prepare_calls.fetch_add(1, Ordering::Relaxed);
        let mut g = self.state.lock().unwrap();
        if !g.live.contains(&conn.0) {
            return Err(stale());
        }
        g.next_stmt += 1;
        let h = g.next_stmt;
        g.stmts.insert(
            h,
            StmtState {
                conn: conn.0,
                sql: sql.to_owned(),
                position: -1,
            },
        );
        Ok(StatementHandle(h))
    }

    fn bind_params(&self, stmt: StatementHandle, _params: &[Value]) -> Result<()> {
        let g = self.state.lock().unwrap();
        match g.stmts.get(&stmt.0) {
            Some(s) if g.live.contains(&s.conn) => Ok(()),
            _ => Err(stale()),
        }
    }

    fn execute(&self, stmt: StatementHandle) -> Result<i64> {
        let n = self.execute_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _guard = self.enter_in_flight();

        let fe = self.fail_execute_every.load(Ordering::Relaxed);
        if fe > 0 && n % fe == 0 {
            return Err(transient());
        }
        let ke = self.kill_connection_every.load(Ordering::Relaxed);

        let mut g = self.state.lock().unwrap();
        let (conn, is_sel) = match g.stmts.get(&stmt.0) {
            Some(s) => (s.conn, is_select(&s.sql)),
            None => return Err(stale()),
        };
        if !g.live.contains(&conn) {
            return Err(stale());
        }
        if let Some(s) = g.stmts.get_mut(&stmt.0) {
            s.position = -1;
        }
        if ke > 0 && n % ke == 0 {
            g.live.remove(&conn); // next isAlive -> false
            g.dead.insert(conn);
        }
        Ok(if is_sel { 0 } else { 1 })
    }

    fn execute_batch(&self, stmt: StatementHandle, rows: &[Vec<Value>]) -> Result<i64> {
        let mut total = 0;
        for row in rows {
            self.bind_params(stmt, row)?;
            total += self.execute(stmt)?;
        }
        Ok(total)
    }

    fn column_count(&self, stmt: StatementHandle) -> Result<usize> {
        let g = self.state.lock().unwrap();
        match g.stmts.get(&stmt.0) {
            Some(s) if is_select(&s.sql) => Ok(1),
            _ => Ok(0),
        }
    }

    fn column_name(&self, _stmt: StatementHandle, _index: usize) -> Result<String> {
        Ok("c0".into())
    }

    fn fetch(&self, stmt: StatementHandle) -> Result<bool> {
        let mut g = self.state.lock().unwrap();
        let conn = match g.stmts.get(&stmt.0) {
            Some(s) => s.conn,
            None => return Err(stale()),
        };
        if !g.live.contains(&conn) {
            return Err(stale());
        }
        let s = g.stmts.get_mut(&stmt.0).unwrap();
        s.position += 1;
        Ok(s.position < 1) // exactly one row
    }

    fn get_column(&self, stmt: StatementHandle, index: usize) -> Result<Value> {
        let g = self.state.lock().unwrap();
        match g.stmts.get(&stmt.0) {
            Some(s) if s.position == 0 && index == 0 => Ok(Value::Int(encoded_value(&s.sql))),
            _ => Err(mapping()),
        }
    }

    fn finalize(&self, stmt: StatementHandle) -> Result<()> {
        self.finalize_calls.fetch_add(1, Ordering::Relaxed);
        self.state.lock().unwrap().stmts.remove(&stmt.0);
        Ok(())
    }

    fn close_cursor(&self, stmt: StatementHandle) -> Result<()> {
        self.close_cursor_calls.fetch_add(1, Ordering::Relaxed);
        let mut g = self.state.lock().unwrap();
        if let Some(s) = g.stmts.get_mut(&stmt.0) {
            s.position = -1;
        }
        Ok(())
    }

    fn set_auto_commit(&self, _conn: ConnectionHandle, enabled: bool) -> Result<()> {
        if enabled {
            self.auto_commit_on.fetch_add(1, Ordering::Relaxed);
        } else {
            self.auto_commit_off.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn commit(&self, _conn: ConnectionHandle) -> Result<()> {
        self.commit_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn rollback(&self, _conn: ConnectionHandle) -> Result<()> {
        self.rollback_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{CliDriver, ConnectionParams};
    use crate::value::Value;

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "dsn".into(),
        }
    }

    #[test]
    fn select_returns_sql_encoded_value() {
        let d = ConcurrentFakeDriver::new();
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "SELECT 7 FROM SYSIBM.SYSDUMMY1").unwrap();
        d.bind_params(s, &[]).unwrap();
        assert_eq!(d.execute(s).unwrap(), 0); // SELECT reports 0 affected
        assert_eq!(d.column_count(s).unwrap(), 1);
        assert!(d.fetch(s).unwrap()); // one row
        assert_eq!(d.get_column(s, 0).unwrap(), Value::Int(7));
        assert!(!d.fetch(s).unwrap()); // end of cursor
    }

    #[test]
    fn fail_execute_every_trips_at_rate() {
        let d = ConcurrentFakeDriver::new();
        d.fail_execute_every
            .store(2, std::sync::atomic::Ordering::Relaxed);
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "SELECT 1 FROM SYSIBM.SYSDUMMY1").unwrap();
        assert!(d.execute(s).is_ok()); // call 1
        let e = d.execute(s).unwrap_err(); // call 2 -> fail
        assert!(e.retriable);
    }

    #[test]
    fn statement_on_dead_connection_errors() {
        let d = ConcurrentFakeDriver::new();
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "SELECT 1 FROM SYSIBM.SYSDUMMY1").unwrap();
        d.disconnect(c).unwrap();
        assert!(d.execute(s).is_err()); // use after the connection died
    }

    #[test]
    fn insert_reports_one_affected_row() {
        let d = ConcurrentFakeDriver::new();
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(d.execute(s).unwrap(), 1);
    }
}
