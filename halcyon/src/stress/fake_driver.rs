//! A thread-safe, deterministic `CliDriver` for concurrency testing. Unlike the
//! FIFO-scripted `MockCliDriver`, every response is derived from its inputs, so
//! any number of threads may call it with no ordering assumptions. Counters are
//! atomics; the handle maps are guarded by one mutex (the fake is intentionally
//! a little pessimistic — the contention we measure lives in the pool, not here).
//! Port of `../Halcyon/tests/stress/support/concurrent_fake_driver.hpp`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use crate::driver::{
    CliDriver, ConnectionHandle, ConnectionParams, GetDataChunk, ParamStreamSource, StatementHandle,
};
use crate::error::{Error, ErrorCode, Result};
use crate::value::Value;
use crate::Isolation;

struct StmtState {
    conn: u64,
    sql: String,
    bound_params: Vec<Value>,
    position: i64,
    lob_offsets: HashMap<usize, usize>,
}

#[derive(Default)]
struct State {
    live: HashSet<u64>,
    dead: HashSet<u64>,
    connections: HashMap<u64, ConnectionState>,
    stmts: HashMap<u64, StmtState>,
    next_conn: u64,
    next_stmt: u64,
}

/// Loaded per-connection session state exposed to stress assertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionState {
    pub autocommit: bool,
    pub isolation: Option<Isolation>,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            autocommit: true,
            isolation: None,
        }
    }
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

    // --- realism knobs (microseconds; 0 = no sleep) ---
    // Widen the in-flight / connect windows so concurrent overlap is easier to
    // force (useful under ThreadSanitizer to surface interleavings). Default 0
    // keeps the deterministic correctness runs behavior-identical.
    pub query_latency_us: AtomicI64,
    pub connect_latency_us: AtomicI64,

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
    set_isolation_calls: AtomicI64,
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
    pub fn set_isolation_calls(&self) -> i64 {
        self.set_isolation_calls.load(Ordering::Relaxed)
    }
    pub fn peak_in_flight(&self) -> i64 {
        self.peak_in_flight.load(Ordering::Relaxed)
    }
    pub fn connection_states(&self) -> Vec<ConnectionState> {
        let state = self.state.lock().unwrap();
        state
            .live
            .iter()
            .filter_map(|handle| state.connections.get(handle).copied())
            .collect()
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
fn sleep_us(us: i64) {
    if us > 0 {
        std::thread::sleep(std::time::Duration::from_micros(us as u64));
    }
}

impl CliDriver for ConcurrentFakeDriver {
    fn connect(&self, _params: &ConnectionParams) -> Result<ConnectionHandle> {
        let n = self.connect_calls.fetch_add(1, Ordering::Relaxed) + 1;
        sleep_us(self.connect_latency_us.load(Ordering::Relaxed));
        let every = self.fail_connect_every.load(Ordering::Relaxed);
        if every > 0 && n % every == 0 {
            return Err(transient());
        }
        let mut g = self.state.lock().unwrap();
        g.next_conn += 1;
        let h = g.next_conn;
        g.live.insert(h);
        g.connections.insert(h, ConnectionState::default());
        Ok(ConnectionHandle(h))
    }

    fn disconnect(&self, conn: ConnectionHandle) -> Result<()> {
        self.disconnect_calls.fetch_add(1, Ordering::Relaxed);
        let mut g = self.state.lock().unwrap();
        g.live.remove(&conn.0);
        g.dead.remove(&conn.0);
        g.connections.remove(&conn.0);
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
                bound_params: Vec::new(),
                position: -1,
                lob_offsets: HashMap::new(),
            },
        );
        Ok(StatementHandle(h))
    }

    fn bind_params(&self, stmt: StatementHandle, params: &[Value]) -> Result<()> {
        let mut g = self.state.lock().unwrap();
        let conn = g.stmts.get(&stmt.0).map(|s| s.conn).ok_or_else(stale)?;
        if !g.live.contains(&conn) {
            return Err(stale());
        }
        g.stmts.get_mut(&stmt.0).ok_or_else(stale)?.bound_params = params.to_vec();
        Ok(())
    }

    fn execute(&self, stmt: StatementHandle) -> Result<i64> {
        let n = self.execute_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _guard = self.enter_in_flight();
        // Sleep while in-flight is elevated, widening the overlap window.
        sleep_us(self.query_latency_us.load(Ordering::Relaxed));

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
            s.lob_offsets.clear();
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

    fn fetch_block(&self, stmt: StatementHandle, max_rows: usize) -> Result<Vec<Vec<Value>>> {
        let mut g = self.state.lock().unwrap();
        let conn = match g.stmts.get(&stmt.0) {
            Some(s) => s.conn,
            None => return Err(stale()),
        };
        if !g.live.contains(&conn) {
            return Err(stale());
        }
        let s = g.stmts.get_mut(&stmt.0).unwrap();
        if max_rows == 0 || s.position >= 0 {
            return Ok(Vec::new()); // empty is clean EOF
        }
        s.position = 0;
        Ok(vec![vec![Value::Int(encoded_value(&s.sql))]]) // exactly one row
    }

    fn fetch_next(&self, stmt: StatementHandle) -> Result<bool> {
        let mut g = self.state.lock().unwrap();
        let conn = g.stmts.get(&stmt.0).map(|s| s.conn).ok_or_else(stale)?;
        if !g.live.contains(&conn) {
            return Err(stale());
        }
        let s = g.stmts.get_mut(&stmt.0).ok_or_else(stale)?;
        if s.position >= 0 || !is_select(&s.sql) {
            Ok(false)
        } else {
            s.position = 0;
            s.lob_offsets.clear();
            Ok(true)
        }
    }

    fn get_value(&self, stmt: StatementHandle, column: usize) -> Result<Value> {
        let g = self.state.lock().unwrap();
        let s = g.stmts.get(&stmt.0).ok_or_else(stale)?;
        if s.position < 0 {
            return Err(Error::new(
                ErrorCode::InvalidState,
                "streaming cursor is not on a row",
            ));
        }
        if column != 0 {
            return Err(Error::new(ErrorCode::Mapping, "column index out of range"));
        }
        Ok(s.bound_params
            .first()
            .cloned()
            .unwrap_or_else(|| Value::Int(encoded_value(&s.sql))))
    }

    fn get_data_chunk(
        &self,
        stmt: StatementHandle,
        column: usize,
        buffer: &mut [u8],
    ) -> Result<GetDataChunk> {
        if buffer.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "LOB chunk buffer must not be empty",
            ));
        }
        let mut g = self.state.lock().unwrap();
        let (conn, position, cell) = {
            let s = g.stmts.get(&stmt.0).ok_or_else(stale)?;
            (
                s.conn,
                s.position,
                s.bound_params
                    .get(column)
                    .cloned()
                    .ok_or_else(|| Error::new(ErrorCode::Mapping, "column index out of range"))?,
            )
        };
        if !g.live.contains(&conn) {
            return Err(stale());
        }
        if position < 0 {
            return Err(Error::new(
                ErrorCode::InvalidState,
                "streaming cursor is not on a row",
            ));
        }
        if matches!(cell, Value::Null) {
            return Ok(GetDataChunk {
                bytes: 0,
                done: true,
                is_null: true,
            });
        }
        let bytes = match &cell {
            Value::Bytes(bytes) => bytes.as_slice(),
            Value::Text(text) => text.as_bytes(),
            _ => return Err(Error::new(ErrorCode::Mapping, "cell is not a LOB")),
        };
        let s = g.stmts.get_mut(&stmt.0).ok_or_else(stale)?;
        let offset = s.lob_offsets.entry(column).or_default();
        let count = buffer.len().min(bytes.len().saturating_sub(*offset));
        buffer[..count].copy_from_slice(&bytes[*offset..*offset + count]);
        *offset += count;
        Ok(GetDataChunk {
            bytes: count,
            done: *offset == bytes.len(),
            is_null: false,
        })
    }

    fn execute_streaming(
        &self,
        stmt: StatementHandle,
        params: &[Value],
        mut sources: Vec<ParamStreamSource<'_>>,
    ) -> Result<i64> {
        let mut seen = HashSet::new();
        for source in &sources {
            if source.param_index >= params.len() || !seen.insert(source.param_index) {
                return Err(Error::new(
                    ErrorCode::Mapping,
                    "bad data-at-exec stream source index",
                ));
            }
        }

        let rows = self.execute(stmt)?;
        let mut bound = params.to_vec();
        let mut stage = vec![0u8; 8 * 1024];
        for source in &mut sources {
            let mut bytes = Vec::new();
            loop {
                let count = (source.pull)(&mut stage)?;
                if count > stage.len() {
                    return Err(Error::new(
                        ErrorCode::Mapping,
                        "LOB source returned an oversized chunk",
                    ));
                }
                if count == 0 {
                    break;
                }
                bytes.extend_from_slice(&stage[..count]);
            }
            bound[source.param_index] = if source.is_clob {
                Value::Text(String::from_utf8(bytes).map_err(|error| {
                    Error::new(
                        ErrorCode::Mapping,
                        format!("streamed CLOB is not UTF-8: {error}"),
                    )
                })?)
            } else {
                Value::Bytes(bytes)
            };
        }
        self.state
            .lock()
            .unwrap()
            .stmts
            .get_mut(&stmt.0)
            .ok_or_else(stale)?
            .bound_params = bound;
        Ok(rows)
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
            s.lob_offsets.clear();
        }
        Ok(())
    }

    fn set_auto_commit(&self, conn: ConnectionHandle, enabled: bool) -> Result<()> {
        if enabled {
            self.auto_commit_on.fetch_add(1, Ordering::Relaxed);
        } else {
            self.auto_commit_off.fetch_add(1, Ordering::Relaxed);
        }
        self.state
            .lock()
            .unwrap()
            .connections
            .get_mut(&conn.0)
            .ok_or_else(stale)?
            .autocommit = enabled;
        Ok(())
    }

    fn set_isolation(&self, conn: ConnectionHandle, level: Isolation) -> Result<()> {
        self.set_isolation_calls.fetch_add(1, Ordering::Relaxed);
        self.state
            .lock()
            .unwrap()
            .connections
            .get_mut(&conn.0)
            .ok_or_else(stale)?
            .isolation = Some(level);
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
            isolation: None,
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
        assert_eq!(d.fetch_block(s, 4096).unwrap(), vec![vec![Value::Int(7)]]);
        assert!(d.fetch_block(s, 4096).unwrap().is_empty()); // end of cursor
    }

    #[test]
    fn streaming_cursor_reads_bound_scalar_script() {
        let d = ConcurrentFakeDriver::new();
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "SELECT 7 FROM SYSIBM.SYSDUMMY1").unwrap();
        d.bind_params(s, &[Value::Int(42)]).unwrap();
        d.execute(s).unwrap();

        assert!(d.fetch_next(s).unwrap());
        assert_eq!(d.get_value(s, 0).unwrap(), Value::Int(42));
        assert!(!d.fetch_next(s).unwrap());
    }

    #[test]
    fn streaming_cursor_reads_bound_lob_in_chunks() {
        let d = ConcurrentFakeDriver::new();
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "VALUES (?)").unwrap();
        d.bind_params(s, &[Value::Bytes(b"abcde".to_vec())])
            .unwrap();
        d.execute(s).unwrap();
        assert!(d.fetch_next(s).unwrap());

        let mut buffer = [0u8; 2];
        let first = d.get_data_chunk(s, 0, &mut buffer).unwrap();
        assert_eq!(
            (&buffer, first),
            (
                b"ab",
                GetDataChunk {
                    bytes: 2,
                    done: false,
                    is_null: false
                }
            )
        );
        let second = d.get_data_chunk(s, 0, &mut buffer).unwrap();
        assert_eq!(
            (&buffer, second),
            (
                b"cd",
                GetDataChunk {
                    bytes: 2,
                    done: false,
                    is_null: false
                }
            )
        );
        let last = d.get_data_chunk(s, 0, &mut buffer).unwrap();
        assert_eq!(
            (&buffer[..1], last),
            (
                &b"e"[..],
                GetDataChunk {
                    bytes: 1,
                    done: true,
                    is_null: false,
                },
            )
        );
    }

    #[test]
    fn execute_streaming_consumes_and_records_source_script() {
        let d = ConcurrentFakeDriver::new();
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "INSERT INTO docs VALUES (?, ?)").unwrap();
        let mut offset = 0usize;
        let source = ParamStreamSource {
            param_index: 0,
            pull: Box::new(|buffer| {
                let bytes = b"streamed";
                let count = buffer.len().min(bytes.len().saturating_sub(offset));
                buffer[..count].copy_from_slice(&bytes[offset..offset + count]);
                offset += count;
                Ok(count)
            }),
            size_hint: Some(8),
            is_clob: false,
        };

        assert_eq!(
            d.execute_streaming(s, &[Value::Null, Value::Int(9)], vec![source])
                .unwrap(),
            1
        );
        assert_eq!(offset, 8);
        assert_eq!(
            d.state
                .lock()
                .unwrap()
                .stmts
                .get(&s.0)
                .unwrap()
                .bound_params,
            vec![Value::Bytes(b"streamed".to_vec()), Value::Int(9)]
        );
    }

    #[test]
    fn latency_knobs_do_not_change_results() {
        use std::sync::atomic::Ordering;
        let d = ConcurrentFakeDriver::new();
        d.connect_latency_us.store(1, Ordering::Relaxed);
        d.query_latency_us.store(1, Ordering::Relaxed);
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "SELECT 5 FROM SYSIBM.SYSDUMMY1").unwrap();
        assert_eq!(d.execute(s).unwrap(), 0);
        assert_eq!(d.fetch_block(s, 4096).unwrap(), vec![vec![Value::Int(5)]]);
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
