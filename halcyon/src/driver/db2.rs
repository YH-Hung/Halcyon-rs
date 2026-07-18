//! The only module in this crate that touches `halcyon-sys` (the IBM CLI ABI).
//! Port of the C++ `Db2CliDriver` — same locking discipline, same error
//! classification, same array-binding batch algorithm.

// The Db2 seam: the single place above `halcyon-sys` allowed to use `unsafe`.
// The crate-wide `#![deny(unsafe_code)]` (lib.rs) forbids it everywhere else;
// this module-scoped waiver is the one sanctioned exception, kept here at the
// seam so the boundary is mechanically obvious (per the project's seam rule).
#![allow(unsafe_code)]

use std::collections::HashMap;
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard};

use halcyon_sys as cli;

use crate::driver::sqlstate::classify_sqlstate;
use crate::driver::{CliDriver, ConnectionHandle, ConnectionParams, StatementHandle};
use crate::error::{Error, ErrorCode, Result};
use crate::value::Value;
use crate::Isolation;

const FETCH_BLOCK_BYTES: usize = 2 * 1024 * 1024;
const MAX_BLOCK_ROWS: usize = 4096;
const MAX_BOUND_COL_BYTES: usize = 64 * 1024;

/// Raw CLI handle that may cross threads. The IBM CLI is thread-safe for
/// distinct handles used from distinct threads, and Halcyon guarantees a
/// single owner per leased connection/statement, so moving/sharing the token
/// itself is sound; all use happens through the discipline in this module.
#[derive(Debug, Clone, Copy)]
struct RawHandle(cli::SQLHANDLE);

unsafe impl Send for RawHandle {}
unsafe impl Sync for RawHandle {}

fn cli_ok(rc: cli::SQLRETURN) -> bool {
    rc == cli::SQL_SUCCESS || rc == cli::SQL_SUCCESS_WITH_INFO
}

fn bytes_to_string(buf: &[u8]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

/// Pulls the first diagnostic record off a handle and builds a classified Error.
fn make_error(
    handle_type: cli::SQLSMALLINT,
    handle: cli::SQLHANDLE,
    fallback: ErrorCode,
    context: &str,
) -> Error {
    let mut state = [0u8; 6];
    let mut native: cli::SQLINTEGER = 0;
    let mut msg = [0u8; 1024];
    let mut msg_len: cli::SQLSMALLINT = 0;
    let mut e = Error::new(fallback, "");
    let rc = unsafe {
        cli::SQLGetDiagRec(
            handle_type,
            handle,
            1,
            state.as_mut_ptr(),
            &mut native,
            msg.as_mut_ptr(),
            msg.len() as cli::SQLSMALLINT,
            &mut msg_len,
        )
    };
    if rc == cli::SQL_SUCCESS || msg_len > 0 {
        e.sqlstate = bytes_to_string(&state);
        e.native_error = native;
        e.message = bytes_to_string(&msg);
        let c = classify_sqlstate(&e.sqlstate, e.native_error);
        e.code = c.code;
        e.retriable = c.retriable;
    }
    if e.message.is_empty() {
        e.message = context.to_owned();
    }
    e
}

fn unknown_conn() -> Error {
    Error::new(ErrorCode::Connection, "unknown connection handle")
}

fn unknown_stmt() -> Error {
    Error::new(ErrorCode::Unknown, "unknown statement handle")
}

/// Integer-valued statement/connection attribute passed by value in the
/// SQLPOINTER argument, per the CLI convention.
fn attr_value(v: cli::SQLULEN) -> cli::SQLPOINTER {
    v as usize as cli::SQLPOINTER
}

fn isolation_attr_value(level: Isolation) -> cli::SQLULEN {
    match level {
        Isolation::UncommittedRead => cli::SQL_TXN_READ_UNCOMMITTED,
        Isolation::CursorStability => cli::SQL_TXN_READ_COMMITTED,
        Isolation::ReadStability => cli::SQL_TXN_REPEATABLE_READ,
        Isolation::RepeatableRead => cli::SQL_TXN_SERIALIZABLE,
    }
}

/// A parameter bound for a scalar execute. The CLI stores pointers into this
/// struct's fields between `bind_params` and `execute`, so the containing
/// `Vec<BoundParam>` must not be touched (no push/realloc) until the next
/// `bind_params` replaces it wholesale.
struct BoundParam {
    c_type: cli::SQLSMALLINT,
    sql_type: cli::SQLSMALLINT,
    /// SQL_NULL_DATA for null, else the byte length indicator.
    len_ind: cli::SQLLEN,
    /// Backing storage kept alive until execute.
    buf: Vec<u8>,
    i64_val: i64,
    f64_val: f64,
}

#[derive(Clone, Copy)]
struct ColMeta {
    sql_type: cli::SQLSMALLINT,
    c_type: cli::SQLSMALLINT,
    bind_width: usize,
    bounded: bool,
}

/// Heap-pinned because the CLI retains pointers into every field until all
/// rowset bindings and pointer attributes have been cleared (or the statement
/// handle is freed). On cleanup failure the box moves into `StmtState` without
/// moving the allocation, preventing dangling native pointers.
struct RowsetStorage {
    buffers: Vec<Vec<i64>>,
    indicators: Vec<Vec<cli::SQLLEN>>,
    rows_fetched: cli::SQLULEN,
    row_status: Vec<cli::SQLUSMALLINT>,
}

impl BoundParam {
    fn from_value(v: &Value) -> BoundParam {
        let mut bp = BoundParam {
            c_type: cli::SQL_C_CHAR,
            sql_type: cli::SQL_VARCHAR,
            len_ind: 0,
            buf: Vec::new(),
            i64_val: 0,
            f64_val: 0.0,
        };
        match v {
            Value::Null => bp.len_ind = cli::SQL_NULL_DATA,
            Value::Bool(b) => {
                // Bind as SQL_C_BIT: a single 0/1 byte kept in buf, bound
                // through the generic buffer path.
                bp.c_type = cli::SQL_C_BIT;
                bp.sql_type = cli::SQL_BIT;
                bp.buf = vec![u8::from(*b)];
            }
            Value::Int(i) => {
                bp.c_type = cli::SQL_C_SBIGINT;
                bp.sql_type = cli::SQL_BIGINT;
                bp.i64_val = *i;
            }
            Value::Double(d) => {
                bp.c_type = cli::SQL_C_DOUBLE;
                bp.sql_type = cli::SQL_DOUBLE;
                bp.f64_val = *d;
            }
            Value::Text(s) => {
                bp.buf = s.as_bytes().to_vec();
                bp.len_ind = bp.buf.len() as cli::SQLLEN;
            }
            Value::Bytes(b) => {
                bp.c_type = cli::SQL_C_BINARY;
                bp.sql_type = cli::SQL_BINARY;
                bp.buf = b.clone();
                bp.len_ind = bp.buf.len() as cli::SQLLEN;
            }
        }
        bp
    }
}

struct StmtState {
    handle: RawHandle,
    /// Kept alive (and address-stable) between bind_params and execute — the
    /// CLI holds pointers into these until the statement is executed or rebound.
    bound: Vec<BoundParam>,
    /// Described once per successfully executed cursor, then cleared on close
    /// or re-execute so cached statements never retain stale result metadata.
    cols: Option<Vec<ColMeta>>,
    block_fallback: bool,
    pending_rowset: Option<Box<RowsetStorage>>,
}

/// A live statement plus the connection it was prepared on, so disconnecting
/// (or dropping the driver) can free child statements before their parent.
struct StmtEntry {
    conn: u64,
    state: Arc<Mutex<StmtState>>,
}

/// Gives the native CLI the last word on whether it destroyed a statement. A
/// failed `SQLFreeHandle` leaves the statement valid, so return its Rust owner
/// intact rather than dropping storage that the CLI may still reference.
fn attempt_free_stmt<F>(entry: StmtEntry, free: F) -> std::result::Result<(), StmtEntry>
where
    F: FnOnce(cli::SQLHANDLE) -> cli::SQLRETURN,
{
    let rc = {
        // Wait out any in-flight operation before attempting terminal cleanup.
        let st = entry.state.lock().unwrap_or_else(|p| p.into_inner());
        free(st.handle.0)
    };
    if cli_ok(rc) {
        Ok(())
    } else {
        Err(entry)
    }
}

fn stmt_handle(entry: &StmtEntry) -> cli::SQLHANDLE {
    entry
        .state
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .handle
        .0
}

#[derive(Default)]
struct Inner {
    conns: HashMap<u64, RawHandle>,
    stmts: HashMap<u64, StmtEntry>,
    next_conn: u64,
    next_stmt: u64,
}

/// The real IBM Db2 CLI driver: the sole [`CliDriver`] implementation that
/// talks to `libdb2`.
///
/// The bookkeeping mutex is held only around map lookups/inserts/erasures and
/// counter bumps — never across a CLI round-trip — so distinct connections
/// still execute concurrently. Per-statement mutexes serialize the (already
/// single-owner) statement data path defensively.
pub struct Db2CliDriver {
    env: RawHandle,
    inner: Mutex<Inner>,
}

impl Db2CliDriver {
    pub fn new() -> Db2CliDriver {
        let mut env: cli::SQLHANDLE = ptr::null_mut();
        let rc =
            unsafe { cli::SQLAllocHandle(cli::SQL_HANDLE_ENV, cli::SQL_NULL_HANDLE, &mut env) };
        // Construction stays infallible (env alloc fails only on OOM / a broken
        // driver manager), so normalize a failed alloc to a null handle. That
        // makes the failure unambiguous at `connect`, which reports it as a clear
        // `ErrorCode::Connection` rather than deferring a murky diagnostic off a
        // dangling handle.
        if !cli_ok(rc) {
            env = ptr::null_mut();
        }
        Db2CliDriver {
            env: RawHandle(env),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// The bookkeeping is consistent between operations and the lock is never
    /// held across user code, so a poisoned mutex (a panic mid-bookkeeping on
    /// another thread) is recovered rather than propagated.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn conn_handle(&self, h: ConnectionHandle) -> Option<RawHandle> {
        self.lock().conns.get(&h.0).copied()
    }

    fn stmt_state(&self, h: StatementHandle) -> Option<Arc<Mutex<StmtState>>> {
        self.lock().stmts.get(&h.0).map(|e| e.state.clone())
    }

    fn end_tran(&self, conn: ConnectionHandle, how: cli::SQLSMALLINT, context: &str) -> Result<()> {
        let Some(dbc) = self.conn_handle(conn) else {
            return Err(unknown_conn());
        };
        let rc = unsafe { cli::SQLEndTran(cli::SQL_HANDLE_DBC, dbc.0, how) };
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_DBC,
                dbc.0,
                ErrorCode::Unknown,
                context,
            ));
        }
        Ok(())
    }
}

impl Default for Db2CliDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Db2CliDriver {
    // Drains every live handle in child-before-parent order so dropping the
    // driver directly (without the pool having disconnected everything) leaks
    // nothing and never frees a parent under a live child.
    fn drop(&mut self) {
        let inner = self.inner.get_mut().unwrap_or_else(|p| p.into_inner());
        for (_, entry) in inner.stmts.drain() {
            if let Err(entry) = attempt_free_stmt(entry, |h| unsafe {
                cli::SQLFreeHandle(cli::SQL_HANDLE_STMT, h)
            }) {
                // Drop cannot surface the CLI diagnostic. The contract says a
                // failed free leaves the native statement valid, which may
                // retain rowset/parameter pointers, so leak the owner rather
                // than freeing its backing storage underneath the native handle.
                std::mem::forget(entry);
            }
        }
        for (_, dbc) in inner.conns.drain() {
            unsafe {
                cli::SQLDisconnect(dbc.0);
                cli::SQLFreeHandle(cli::SQL_HANDLE_DBC, dbc.0);
            }
        }
        if !self.env.0.is_null() {
            unsafe { cli::SQLFreeHandle(cli::SQL_HANDLE_ENV, self.env.0) };
        }
    }
}

/// Restores single-row paramset state so a cached statement is clean for later
/// scalar bind/execute reuse. Returns the CLI status so the success path can
/// fail if the reset did not stick.
fn reset_paramset(h: cli::SQLHANDLE) -> cli::SQLRETURN {
    unsafe { cli::SQLSetStmtAttr(h, cli::SQL_ATTR_PARAMSET_SIZE, attr_value(1), 0) }
}

/// Reads a (possibly long) character column by looping SQLGetData.
fn get_string(h: cli::SQLHANDLE, col: cli::SQLUSMALLINT) -> Result<Value> {
    let mut out: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut ind: cli::SQLLEN = 0;
    loop {
        let rc = unsafe {
            cli::SQLGetData(
                h,
                col,
                cli::SQL_C_CHAR,
                chunk.as_mut_ptr().cast(),
                chunk.len() as cli::SQLLEN,
                &mut ind,
            )
        };
        if rc == cli::SQL_NO_DATA {
            break;
        }
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_STMT,
                h,
                ErrorCode::Unknown,
                "SQLGetData char failed",
            ));
        }
        if ind == cli::SQL_NULL_DATA {
            return Ok(Value::Null);
        }
        let copied = if ind == cli::SQLLEN::from(cli::SQL_NTS) || ind >= chunk.len() as cli::SQLLEN
        {
            // Truncated chunk: the CLI NUL-terminates within the buffer.
            chunk.iter().position(|&b| b == 0).unwrap_or(chunk.len())
        } else {
            ind as usize
        };
        out.extend_from_slice(&chunk[..copied]);
        if rc == cli::SQL_SUCCESS {
            break; // no truncation: done
        }
    }
    Ok(Value::Text(String::from_utf8_lossy(&out).into_owned()))
}

/// Reads a (possibly long) binary column by looping SQLGetData with
/// SQL_C_BINARY. Unlike get_string there is no NUL terminator: the indicator
/// gives the byte length, and on truncation (SQL_SUCCESS_WITH_INFO) it reports
/// the full remaining size (>= buffer) or SQL_NO_TOTAL, so the whole buffer
/// was filled — take all of it and loop until a SQL_SUCCESS chunk.
fn get_binary(h: cli::SQLHANDLE, col: cli::SQLUSMALLINT) -> Result<Value> {
    let mut out: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut ind: cli::SQLLEN = 0;
    loop {
        let rc = unsafe {
            cli::SQLGetData(
                h,
                col,
                cli::SQL_C_BINARY,
                chunk.as_mut_ptr().cast(),
                chunk.len() as cli::SQLLEN,
                &mut ind,
            )
        };
        if rc == cli::SQL_NO_DATA {
            break;
        }
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_STMT,
                h,
                ErrorCode::Unknown,
                "SQLGetData binary failed",
            ));
        }
        if ind == cli::SQL_NULL_DATA {
            return Ok(Value::Null);
        }
        let copied = if ind == cli::SQL_NO_TOTAL || ind >= chunk.len() as cli::SQLLEN {
            chunk.len()
        } else {
            ind as usize
        };
        out.extend_from_slice(&chunk[..copied]);
        if rc == cli::SQL_SUCCESS {
            break; // last chunk delivered in full
        }
    }
    Ok(Value::Bytes(out))
}

/// Reads one current-row cell using already-cached SQL type metadata. The
/// unbounded fallback calls this at most once per `fetch_block` invocation.
fn read_column_typed(h: cli::SQLHANDLE, index: usize, sql_type: cli::SQLSMALLINT) -> Result<Value> {
    let col = (index + 1) as cli::SQLUSMALLINT;
    match sql_type {
        cli::SQL_SMALLINT | cli::SQL_INTEGER | cli::SQL_BIGINT => {
            let mut v: i64 = 0;
            let mut ind: cli::SQLLEN = 0;
            let rc = unsafe {
                cli::SQLGetData(
                    h,
                    col,
                    cli::SQL_C_SBIGINT,
                    (&mut v as *mut i64).cast(),
                    std::mem::size_of::<i64>() as cli::SQLLEN,
                    &mut ind,
                )
            };
            if !cli_ok(rc) {
                return Err(make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLGetData int failed",
                ));
            }
            if ind == cli::SQL_NULL_DATA {
                Ok(Value::Null)
            } else {
                Ok(Value::Int(v))
            }
        }
        cli::SQL_REAL | cli::SQL_FLOAT | cli::SQL_DOUBLE => {
            let mut v: f64 = 0.0;
            let mut ind: cli::SQLLEN = 0;
            let rc = unsafe {
                cli::SQLGetData(
                    h,
                    col,
                    cli::SQL_C_DOUBLE,
                    (&mut v as *mut f64).cast(),
                    std::mem::size_of::<f64>() as cli::SQLLEN,
                    &mut ind,
                )
            };
            if !cli_ok(rc) {
                return Err(make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLGetData double failed",
                ));
            }
            if ind == cli::SQL_NULL_DATA {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(v))
            }
        }
        cli::SQL_BIT => {
            let mut v = 0u8;
            let mut ind: cli::SQLLEN = 0;
            let rc = unsafe {
                cli::SQLGetData(
                    h,
                    col,
                    cli::SQL_C_BIT,
                    (&mut v as *mut u8).cast(),
                    1,
                    &mut ind,
                )
            };
            if !cli_ok(rc) {
                return Err(make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLGetData bit failed",
                ));
            }
            if ind == cli::SQL_NULL_DATA {
                Ok(Value::Null)
            } else {
                Ok(Value::Int(i64::from(v != 0)))
            }
        }
        // Raw bytes: read as SQL_C_BINARY so embedded NULs survive.
        cli::SQL_BINARY | cli::SQL_VARBINARY | cli::SQL_LONGVARBINARY | cli::SQL_BLOB => {
            get_binary(h, col)
        }
        // CHAR/VARCHAR/DECIMAL/NUMERIC/TIMESTAMP/DATE/TIME → UTF-8 string.
        _ => get_string(h, col),
    }
}

fn describe_columns(st: &mut StmtState) -> Result<()> {
    let h = st.handle.0;
    let mut count: cli::SQLSMALLINT = 0;
    let rc = unsafe { cli::SQLNumResultCols(h, &mut count) };
    if !cli_ok(rc) {
        return Err(make_error(
            cli::SQL_HANDLE_STMT,
            h,
            ErrorCode::Unknown,
            "SQLNumResultCols failed",
        ));
    }

    let mut cols = Vec::with_capacity(count.max(0) as usize);
    let mut any_unbounded = false;
    for index in 0..count.max(0) as usize {
        let mut name = [0u8; 1];
        let mut name_len: cli::SQLSMALLINT = 0;
        let mut sql_type: cli::SQLSMALLINT = 0;
        let mut col_size: cli::SQLULEN = 0;
        let mut scale: cli::SQLSMALLINT = 0;
        let mut nullable: cli::SQLSMALLINT = 0;
        let rc = unsafe {
            cli::SQLDescribeCol(
                h,
                (index + 1) as cli::SQLUSMALLINT,
                name.as_mut_ptr(),
                name.len() as cli::SQLSMALLINT,
                &mut name_len,
                &mut sql_type,
                &mut col_size,
                &mut scale,
                &mut nullable,
            )
        };
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_STMT,
                h,
                ErrorCode::Unknown,
                "SQLDescribeCol failed",
            ));
        }

        // Character declarations count code units; rowset buffers require byte
        // capacity. SQL_DESC_OCTET_LENGTH supplies that capacity for multibyte
        // data. Text-rendered scalar types may have a wider display size, so use
        // the larger of the two just as the C++ driver does.
        let mut octet: cli::SQLLEN = 0;
        unsafe {
            cli::SQLColAttribute(
                h,
                (index + 1) as cli::SQLUSMALLINT,
                cli::SQL_DESC_OCTET_LENGTH,
                ptr::null_mut(),
                0,
                ptr::null_mut(),
                (&mut octet as *mut cli::SQLLEN).cast(),
            );
        }
        let data_width = (col_size as usize).max(octet.max(0) as usize);

        let mut meta = ColMeta {
            sql_type,
            c_type: cli::SQL_C_CHAR,
            bind_width: 0,
            bounded: true,
        };
        match sql_type {
            cli::SQL_SMALLINT | cli::SQL_INTEGER | cli::SQL_BIGINT => {
                meta.c_type = cli::SQL_C_SBIGINT;
                meta.bind_width = std::mem::size_of::<i64>();
            }
            cli::SQL_REAL | cli::SQL_FLOAT | cli::SQL_DOUBLE => {
                meta.c_type = cli::SQL_C_DOUBLE;
                meta.bind_width = std::mem::size_of::<f64>();
            }
            cli::SQL_BIT => {
                meta.c_type = cli::SQL_C_BIT;
                meta.bind_width = 1;
            }
            cli::SQL_BINARY | cli::SQL_VARBINARY => {
                meta.c_type = cli::SQL_C_BINARY;
                meta.bind_width = data_width;
            }
            cli::SQL_LONGVARBINARY
            | cli::SQL_BLOB
            | cli::SQL_CLOB
            | cli::SQL_DBCLOB
            | cli::SQL_LONGVARCHAR => {
                meta.bounded = false;
            }
            _ => {
                meta.c_type = cli::SQL_C_CHAR;
                meta.bind_width = data_width.saturating_add(1);
            }
        }
        if meta.bounded && (meta.bind_width == 0 || meta.bind_width > MAX_BOUND_COL_BYTES) {
            meta.bounded = false;
        }
        any_unbounded |= !meta.bounded;
        cols.push(meta);
    }
    st.cols = Some(cols);
    st.block_fallback = any_unbounded;
    Ok(())
}

/// Best-effort restoration of the default single-row statement state. The
/// first failure is converted to an Error immediately, before later cleanup
/// calls can replace its diagnostic record; all remaining cleanup still runs.
fn reset_rowset(h: cli::SQLHANDLE) -> Option<Error> {
    fn remember(first: &mut Option<Error>, h: cli::SQLHANDLE, rc: cli::SQLRETURN, context: &str) {
        if first.is_none() && !cli_ok(rc) {
            *first = Some(make_error(
                cli::SQL_HANDLE_STMT,
                h,
                ErrorCode::Unknown,
                context,
            ));
        }
    }

    let mut first = None;
    let rc = unsafe { cli::SQLFreeStmt(h, cli::SQL_UNBIND) };
    remember(&mut first, h, rc, "SQLFreeStmt(SQL_UNBIND) failed");
    let rc = unsafe { cli::SQLSetStmtAttr(h, cli::SQL_ATTR_ROW_ARRAY_SIZE, attr_value(1), 0) };
    remember(&mut first, h, rc, "reset SQL_ATTR_ROW_ARRAY_SIZE failed");
    let rc = unsafe { cli::SQLSetStmtAttr(h, cli::SQL_ATTR_ROW_STATUS_PTR, ptr::null_mut(), 0) };
    remember(&mut first, h, rc, "reset SQL_ATTR_ROW_STATUS_PTR failed");
    let rc = unsafe { cli::SQLSetStmtAttr(h, cli::SQL_ATTR_ROWS_FETCHED_PTR, ptr::null_mut(), 0) };
    remember(&mut first, h, rc, "reset SQL_ATTR_ROWS_FETCHED_PTR failed");
    first
}

fn ensure_rowset_reset(st: &mut StmtState) -> Result<()> {
    if st.pending_rowset.is_none() {
        return Ok(());
    }
    if let Some(error) = reset_rowset(st.handle.0) {
        return Err(error);
    }
    st.pending_rowset = None;
    Ok(())
}

fn finish_rowset(
    st: &mut StmtState,
    storage: Box<RowsetStorage>,
    outcome: Result<Vec<Vec<Value>>>,
    reset_error: Option<Error>,
) -> Result<Vec<Vec<Value>>> {
    if let Some(reset_error) = reset_error {
        // Even when the fetch itself already failed, retain every address the
        // CLI may still hold. A later operation retries cleanup first; finalize
        // and driver drop free the native handle before this box is dropped.
        st.pending_rowset = Some(storage);
        return match outcome {
            Ok(_) => Err(reset_error),
            Err(operation_error) => Err(operation_error),
        };
    }
    outcome
}

fn fetch_block_fallback(st: &StmtState) -> Result<Vec<Vec<Value>>> {
    let h = st.handle.0;
    let rc = unsafe { cli::SQLFetch(h) };
    if rc == cli::SQL_NO_DATA {
        return Ok(Vec::new());
    }
    if !cli_ok(rc) {
        return Err(make_error(
            cli::SQL_HANDLE_STMT,
            h,
            ErrorCode::Unknown,
            "SQLFetch failed",
        ));
    }

    let cols = st.cols.as_deref().unwrap_or_default();
    let mut row = Vec::with_capacity(cols.len());
    for (index, meta) in cols.iter().enumerate() {
        row.push(read_column_typed(h, index, meta.sql_type)?);
    }
    Ok(vec![row])
}

fn fetch_block_rowset(st: &mut StmtState, max_rows: usize) -> Result<Vec<Vec<Value>>> {
    let h = st.handle.0;
    let cols = st.cols.clone().unwrap_or_default();
    let per_row = cols
        .iter()
        .try_fold(0usize, |total, meta| total.checked_add(meta.bind_width))
        .ok_or_else(|| Error::new(ErrorCode::Mapping, "rowset width overflow"))?;
    let mut row_count = FETCH_BLOCK_BYTES
        .checked_div(per_row)
        .unwrap_or(MAX_BLOCK_ROWS)
        .max(1);
    row_count = row_count.min(MAX_BLOCK_ROWS).min(max_rows);

    // The entire storage object is heap-pinned before any address is handed to
    // the CLI. Its data buffers are i64-backed for 8-byte numeric alignment.
    let mut storage = Box::new(RowsetStorage {
        buffers: Vec::with_capacity(cols.len()),
        indicators: Vec::with_capacity(cols.len()),
        rows_fetched: 0,
        row_status: vec![0; row_count],
    });
    for meta in &cols {
        let bytes = meta
            .bind_width
            .checked_mul(row_count)
            .ok_or_else(|| Error::new(ErrorCode::Mapping, "rowset buffer size overflow"))?;
        storage
            .buffers
            .push(vec![0i64; bytes.div_ceil(std::mem::size_of::<i64>())]);
        storage.indicators.push(vec![0; row_count]);
    }

    let outcome = (|| -> Result<Vec<Vec<Value>>> {
        for (attribute, value) in [
            (
                cli::SQL_ATTR_ROW_BIND_TYPE,
                attr_value(cli::SQL_BIND_BY_COLUMN),
            ),
            (
                cli::SQL_ATTR_ROW_ARRAY_SIZE,
                attr_value(row_count as cli::SQLULEN),
            ),
            (
                cli::SQL_ATTR_ROW_STATUS_PTR,
                storage.row_status.as_mut_ptr().cast(),
            ),
            (
                cli::SQL_ATTR_ROWS_FETCHED_PTR,
                (&mut storage.rows_fetched as *mut cli::SQLULEN).cast(),
            ),
        ] {
            let rc = unsafe { cli::SQLSetStmtAttr(h, attribute, value, 0) };
            if !cli_ok(rc) {
                return Err(make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLSetStmtAttr (rowset) failed",
                ));
            }
        }

        for (index, meta) in cols.iter().enumerate() {
            let rc = unsafe {
                cli::SQLBindCol(
                    h,
                    (index + 1) as cli::SQLUSMALLINT,
                    meta.c_type,
                    storage.buffers[index].as_mut_ptr().cast(),
                    meta.bind_width as cli::SQLLEN,
                    storage.indicators[index].as_mut_ptr(),
                )
            };
            if !cli_ok(rc) {
                return Err(make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLBindCol (rowset) failed",
                ));
            }
        }

        // Db2 can report SQL_NO_DATA together with a non-zero partial final
        // rowset. Only zero rows fetched is the true end-of-cursor signal.
        let rc = unsafe { cli::SQLFetch(h) };
        if rc == cli::SQL_NO_DATA && storage.rows_fetched == 0 {
            return Ok(Vec::new());
        }
        if !cli_ok(rc) && rc != cli::SQL_NO_DATA {
            return Err(make_error(
                cli::SQL_HANDLE_STMT,
                h,
                ErrorCode::Unknown,
                "SQLFetch (rowset) failed",
            ));
        }

        let fetched = storage.rows_fetched as usize;
        if fetched > row_count {
            return Err(Error::new(
                ErrorCode::Mapping,
                "Db2 reported more rowset rows than the bound capacity",
            ));
        }
        let mut block: Vec<Vec<Value>> = (0..fetched)
            .map(|_| Vec::with_capacity(cols.len()))
            .collect();
        for (col_index, meta) in cols.iter().enumerate() {
            for (row_index, (row, &indicator)) in block
                .iter_mut()
                .zip(storage.indicators[col_index].iter())
                .enumerate()
            {
                if indicator == cli::SQL_NULL_DATA {
                    row.push(Value::Null);
                    continue;
                }
                let offset = meta.bind_width * row_index;
                let base = storage.buffers[col_index].as_ptr().cast::<u8>();
                // Safety: offset and bind_width were used to size this column's
                // backing allocation, and row_index is bounded by row_count.
                let slot = unsafe { std::slice::from_raw_parts(base.add(offset), meta.bind_width) };
                match meta.c_type {
                    cli::SQL_C_SBIGINT => {
                        let mut bytes = [0u8; std::mem::size_of::<i64>()];
                        bytes.copy_from_slice(&slot[..std::mem::size_of::<i64>()]);
                        row.push(Value::Int(i64::from_ne_bytes(bytes)));
                    }
                    cli::SQL_C_DOUBLE => {
                        let mut bytes = [0u8; std::mem::size_of::<f64>()];
                        bytes.copy_from_slice(&slot[..std::mem::size_of::<f64>()]);
                        row.push(Value::Double(f64::from_ne_bytes(bytes)));
                    }
                    cli::SQL_C_BIT => row.push(Value::Int(i64::from(slot[0] != 0))),
                    cli::SQL_C_BINARY => {
                        let len = usize::try_from(indicator)
                            .unwrap_or(meta.bind_width)
                            .min(meta.bind_width);
                        row.push(Value::Bytes(slot[..len].to_vec()));
                    }
                    _ => {
                        let capacity = meta.bind_width.saturating_sub(1);
                        let len = usize::try_from(indicator).unwrap_or(capacity).min(capacity);
                        row.push(Value::Text(
                            String::from_utf8_lossy(&slot[..len]).into_owned(),
                        ));
                    }
                }
            }
        }
        Ok(block)
    })();

    let reset_error = reset_rowset(h);
    finish_rowset(st, storage, outcome, reset_error)
}

impl CliDriver for Db2CliDriver {
    fn connect(&self, params: &ConnectionParams) -> Result<ConnectionHandle> {
        // A null env means SQLAllocHandle(SQL_HANDLE_ENV) failed at construction
        // (see `Db2CliDriver::new`); no connection can be allocated against it.
        if self.env.0.is_null() {
            return Err(Error::new(
                ErrorCode::Connection,
                "Db2 environment handle allocation failed",
            ));
        }
        let mut dbc: cli::SQLHANDLE = ptr::null_mut();
        {
            // Allocating a DBC touches the shared env handle; serialize it.
            let _lk = self.lock();
            let rc = unsafe { cli::SQLAllocHandle(cli::SQL_HANDLE_DBC, self.env.0, &mut dbc) };
            if !cli_ok(rc) {
                return Err(make_error(
                    cli::SQL_HANDLE_ENV,
                    self.env.0,
                    ErrorCode::Connection,
                    "alloc dbc",
                ));
            }
        }
        // The dbc is private to this thread until it is published into conns
        // below, so the (potentially slow, network-bound) connect runs unlocked.
        let mut dsn = params.connection_string.clone().into_bytes();
        // SQLDriverConnect takes the string length as an SQLSMALLINT (i16); a DSN
        // past 32767 bytes would wrap negative before crossing the ABI. Reject it
        // with a structured error (freeing the dbc first) instead of casting.
        let dsn_len = match cli::SQLSMALLINT::try_from(dsn.len()) {
            Ok(n) => n,
            Err(_) => {
                unsafe { cli::SQLFreeHandle(cli::SQL_HANDLE_DBC, dbc) };
                return Err(Error::new(
                    ErrorCode::Connection,
                    "connection string exceeds the CLI maximum of 32767 bytes",
                ));
            }
        };
        let mut out = [0u8; 1024];
        let mut out_len: cli::SQLSMALLINT = 0;
        let rc = unsafe {
            cli::SQLDriverConnect(
                dbc,
                ptr::null_mut(),
                dsn.as_mut_ptr(),
                dsn_len,
                out.as_mut_ptr(),
                out.len() as cli::SQLSMALLINT,
                &mut out_len,
                cli::SQL_DRIVER_NOPROMPT,
            )
        };
        if !cli_ok(rc) {
            let e = make_error(
                cli::SQL_HANDLE_DBC,
                dbc,
                ErrorCode::Connection,
                "SQLDriverConnect failed",
            );
            unsafe { cli::SQLFreeHandle(cli::SQL_HANDLE_DBC, dbc) };
            return Err(e);
        }
        let mut inner = self.lock();
        inner.next_conn += 1;
        let h = inner.next_conn;
        inner.conns.insert(h, RawHandle(dbc));
        Ok(ConnectionHandle(h))
    }

    fn disconnect(&self, conn: ConnectionHandle) -> Result<()> {
        let (dbc, mut children) = {
            let mut inner = self.lock();
            let Some(dbc) = inner.conns.remove(&conn.0) else {
                return Ok(());
            };
            // Unregister this connection's statements so a later finalize is a
            // clean no-op instead of freeing a stale child handle.
            let mut children = Vec::new();
            for (stmt, entry) in std::mem::take(&mut inner.stmts) {
                if entry.conn == conn.0 {
                    children.push((stmt, entry));
                } else {
                    inner.stmts.insert(stmt, entry);
                }
            }
            (dbc, children)
        };
        // Free children before the parent dbc (waiting out in-flight use).
        while let Some((stmt, entry)) = children.pop() {
            if let Err(entry) = attempt_free_stmt(entry, |h| unsafe {
                cli::SQLFreeHandle(cli::SQL_HANDLE_STMT, h)
            }) {
                let h = stmt_handle(&entry);
                let error = make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLFreeHandle(SQL_HANDLE_STMT) failed during disconnect",
                );
                let mut inner = self.lock();
                inner.conns.insert(conn.0, dbc);
                inner.stmts.insert(stmt, entry);
                for (stmt, entry) in children {
                    inner.stmts.insert(stmt, entry);
                }
                return Err(error);
            }
        }
        unsafe {
            cli::SQLDisconnect(dbc.0);
            cli::SQLFreeHandle(cli::SQL_HANDLE_DBC, dbc.0);
        }
        Ok(())
    }

    fn is_alive(&self, conn: ConnectionHandle) -> Result<bool> {
        let Some(dbc) = self.conn_handle(conn) else {
            return Ok(false);
        };
        let mut s: cli::SQLHANDLE = ptr::null_mut();
        let rc = unsafe { cli::SQLAllocHandle(cli::SQL_HANDLE_STMT, dbc.0, &mut s) };
        if !cli_ok(rc) {
            return Ok(false);
        }
        // Explicit byte length — never SQL_NTS: a Rust buffer carries no NUL
        // terminator for the CLI to stop at.
        let mut sql = b"SELECT 1 FROM SYSIBM.SYSDUMMY1".to_vec();
        let rc = unsafe { cli::SQLExecDirect(s, sql.as_mut_ptr(), sql.len() as cli::SQLINTEGER) };
        let alive = cli_ok(rc);
        unsafe { cli::SQLFreeHandle(cli::SQL_HANDLE_STMT, s) };
        Ok(alive)
    }

    fn prepare(&self, conn: ConnectionHandle, sql: &str) -> Result<StatementHandle> {
        let Some(dbc) = self.conn_handle(conn) else {
            return Err(unknown_conn());
        };
        let mut s: cli::SQLHANDLE = ptr::null_mut();
        let rc = unsafe { cli::SQLAllocHandle(cli::SQL_HANDLE_STMT, dbc.0, &mut s) };
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_DBC,
                dbc.0,
                ErrorCode::Unknown,
                "alloc stmt",
            ));
        }
        // Explicit byte length — never SQL_NTS (no NUL terminator in Rust buffers).
        let mut text = sql.as_bytes().to_vec();
        let rc = unsafe { cli::SQLPrepare(s, text.as_mut_ptr(), text.len() as cli::SQLINTEGER) };
        if !cli_ok(rc) {
            let e = make_error(
                cli::SQL_HANDLE_STMT,
                s,
                ErrorCode::Syntax,
                "SQLPrepare failed",
            );
            unsafe { cli::SQLFreeHandle(cli::SQL_HANDLE_STMT, s) };
            return Err(e);
        }
        let mut inner = self.lock();
        // The connection may have been disconnected while SQLPrepare ran
        // unlocked; registering the statement would leak it past its parent.
        if !inner.conns.contains_key(&conn.0) {
            drop(inner);
            unsafe { cli::SQLFreeHandle(cli::SQL_HANDLE_STMT, s) };
            return Err(unknown_conn());
        }
        inner.next_stmt += 1;
        let h = inner.next_stmt;
        inner.stmts.insert(
            h,
            StmtEntry {
                conn: conn.0,
                state: Arc::new(Mutex::new(StmtState {
                    handle: RawHandle(s),
                    bound: Vec::new(),
                    cols: None,
                    block_fallback: false,
                    pending_rowset: None,
                })),
            },
        );
        Ok(StatementHandle(h))
    }

    fn bind_params(&self, stmt: StatementHandle, params: &[Value]) -> Result<()> {
        let Some(st) = self.stmt_state(stmt) else {
            return Err(unknown_stmt());
        };
        let mut st = st.lock().unwrap_or_else(|p| p.into_inner());
        ensure_rowset_reset(&mut st)?;
        // Build every BoundParam first so the Vec reaches its final size, then
        // bind — the addresses handed to the CLI must not move afterwards.
        st.bound = params.iter().map(BoundParam::from_value).collect();
        let h = st.handle.0;
        // CLI input parameters are read-only; a shared empty buffer stands in
        // for zero-length strings/blobs (mirrors the C++ "" bind).
        static EMPTY: [u8; 1] = [0];
        for (i, bp) in st.bound.iter_mut().enumerate() {
            let mut buf_len: cli::SQLLEN = 0;
            let value_ptr: cli::SQLPOINTER = if bp.c_type == cli::SQL_C_SBIGINT {
                (&mut bp.i64_val as *mut i64).cast()
            } else if bp.c_type == cli::SQL_C_DOUBLE {
                (&mut bp.f64_val as *mut f64).cast()
            } else if bp.buf.is_empty() {
                EMPTY.as_ptr() as *mut u8 as cli::SQLPOINTER
            } else {
                buf_len = bp.buf.len() as cli::SQLLEN;
                bp.buf.as_mut_ptr().cast()
            };
            let column_size: cli::SQLULEN = if bp.len_ind == cli::SQL_NULL_DATA {
                0
            } else {
                buf_len as cli::SQLULEN
            };
            let rc = unsafe {
                cli::SQLBindParameter(
                    h,
                    (i + 1) as cli::SQLUSMALLINT,
                    cli::SQL_PARAM_INPUT,
                    bp.c_type,
                    bp.sql_type,
                    column_size,
                    0,
                    value_ptr,
                    buf_len,
                    &mut bp.len_ind,
                )
            };
            if !cli_ok(rc) {
                return Err(make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLBindParameter failed",
                ));
            }
        }
        Ok(())
    }

    fn execute(&self, stmt: StatementHandle) -> Result<i64> {
        let Some(st) = self.stmt_state(stmt) else {
            return Err(unknown_stmt());
        };
        let mut st = st.lock().unwrap_or_else(|p| p.into_inner());
        ensure_rowset_reset(&mut st)?;
        let h = st.handle.0;
        let rc = unsafe { cli::SQLExecute(h) };
        if !cli_ok(rc) && rc != cli::SQL_NO_DATA {
            return Err(make_error(
                cli::SQL_HANDLE_STMT,
                h,
                ErrorCode::Unknown,
                "SQLExecute failed",
            ));
        }
        st.cols = None;
        st.block_fallback = false;
        let mut rows: cli::SQLLEN = 0;
        unsafe { cli::SQLRowCount(h, &mut rows) };
        Ok(i64::from(rows.max(0)))
    }

    fn execute_batch(&self, stmt: StatementHandle, rows: &[Vec<Value>]) -> Result<i64> {
        // Soundness gate: every unsafe buffer write below assumes a rectangular,
        // type-homogeneous batch. Validate before touching anything.
        crate::driver::batch::validate_batch_rows(rows)?;
        let Some(st) = self.stmt_state(stmt) else {
            return Err(unknown_stmt());
        };
        let mut st = st.lock().unwrap_or_else(|p| p.into_inner());
        ensure_rowset_reset(&mut st)?;
        st.cols = None;
        st.block_fallback = false;
        let h = st.handle.0;
        let n_rows = rows.len();
        if n_rows == 0 {
            return Ok(0);
        }
        let n_cols = rows[0].len();

        // Resolve each column's CLI type from its first non-null value. Input
        // is validated rectangular + homogeneous above the seam; an all-null
        // column defaults to VARCHAR (mirrors the scalar null path).
        let mut c_type = vec![cli::SQL_C_CHAR; n_cols];
        let mut sql_type = vec![cli::SQL_VARCHAR; n_cols];
        let mut is_var = vec![true; n_cols];
        let mut fixed_w = vec![0 as cli::SQLLEN; n_cols];
        for c in 0..n_cols {
            for row in rows {
                match &row[c] {
                    Value::Null => continue,
                    Value::Bool(_) => {
                        c_type[c] = cli::SQL_C_BIT;
                        sql_type[c] = cli::SQL_BIT;
                        is_var[c] = false;
                        fixed_w[c] = 1;
                    }
                    Value::Int(_) => {
                        c_type[c] = cli::SQL_C_SBIGINT;
                        sql_type[c] = cli::SQL_BIGINT;
                        is_var[c] = false;
                        fixed_w[c] = std::mem::size_of::<i64>() as cli::SQLLEN;
                    }
                    Value::Double(_) => {
                        c_type[c] = cli::SQL_C_DOUBLE;
                        sql_type[c] = cli::SQL_DOUBLE;
                        is_var[c] = false;
                        fixed_w[c] = std::mem::size_of::<f64>() as cli::SQLLEN;
                    }
                    Value::Text(_) => {
                        c_type[c] = cli::SQL_C_CHAR;
                        sql_type[c] = cli::SQL_VARCHAR;
                        is_var[c] = true;
                    }
                    Value::Bytes(_) => {
                        c_type[c] = cli::SQL_C_BINARY;
                        sql_type[c] = cli::SQL_BINARY;
                        is_var[c] = true;
                    }
                }
                break; // first non-null defines the column
            }
        }

        const BYTE_BUDGET: usize = 16 * 1024 * 1024;
        let mut total: i64 = 0;
        let mut start = 0usize;
        while start < n_rows {
            // Grow the chunk until adding the next row would exceed the budget.
            // col_max tracks each var-width column's longest element in the chunk.
            let mut col_max = vec![0usize; n_cols];
            let mut end = start;
            while end < n_rows {
                let mut new_max = col_max.clone();
                for c in 0..n_cols {
                    if !is_var[c] {
                        continue;
                    }
                    let len = match &rows[end][c] {
                        Value::Text(s) => s.len(),
                        Value::Bytes(b) => b.len(),
                        _ => 0,
                    };
                    if len > new_max[c] {
                        new_max[c] = len;
                    }
                }
                let prospective_rows = end - start + 1;
                let mut bytes = 0usize;
                for c in 0..n_cols {
                    let w = if is_var[c] {
                        new_max[c].max(1)
                    } else {
                        fixed_w[c] as usize
                    };
                    bytes += w * prospective_rows;
                }
                if end > start && bytes > BYTE_BUDGET {
                    break; // keep >= 1 row
                }
                col_max = new_max;
                end += 1;
            }
            let chunk_rows = end - start;

            // Column-wise buffers + indicator arrays; must outlive SQLExecute.
            // Each column's byte buffer is backed by i64 storage so its base is
            // 8-byte aligned by construction: fixed-width SQL_C_SBIGINT /
            // SQL_C_DOUBLE slots (stride == 8) are then correctly aligned for
            // the CLI's typed reads.
            let mut buf: Vec<Vec<i64>> = Vec::with_capacity(n_cols);
            let mut ind: Vec<Vec<cli::SQLLEN>> = Vec::with_capacity(n_cols);
            let mut width = vec![0 as cli::SQLLEN; n_cols];
            for c in 0..n_cols {
                width[c] = if is_var[c] {
                    col_max[c].max(1) as cli::SQLLEN
                } else {
                    fixed_w[c]
                };
                let w = width[c] as usize;
                let nbytes = w * chunk_rows;
                let mut col = vec![0i64; nbytes.div_ceil(std::mem::size_of::<i64>())];
                let base = col.as_mut_ptr().cast::<u8>();
                let mut col_ind = vec![0 as cli::SQLLEN; chunk_rows];
                for (i, ci) in col_ind.iter_mut().enumerate() {
                    let v = &rows[start + i][c];
                    // Safety: slot stays within col's allocation (w * chunk_rows
                    // bytes, rounded up), and each write below fits one slot.
                    let slot = unsafe { base.add(w * i) };
                    match v {
                        Value::Null => *ci = cli::SQL_NULL_DATA,
                        Value::Bool(b) => unsafe { *slot = u8::from(*b) },
                        Value::Int(p) => unsafe { slot.cast::<i64>().write(*p) },
                        Value::Double(d) => unsafe { slot.cast::<f64>().write(*d) },
                        Value::Text(s) => unsafe {
                            ptr::copy_nonoverlapping(s.as_ptr(), slot, s.len());
                            *ci = s.len() as cli::SQLLEN;
                        },
                        Value::Bytes(b) => unsafe {
                            ptr::copy_nonoverlapping(b.as_ptr(), slot, b.len());
                            *ci = b.len() as cli::SQLLEN;
                        },
                    }
                }
                buf.push(col);
                ind.push(col_ind);
            }

            let mut rc_set = unsafe {
                cli::SQLSetStmtAttr(
                    h,
                    cli::SQL_ATTR_PARAMSET_SIZE,
                    attr_value(chunk_rows as cli::SQLULEN),
                    0,
                )
            };
            if cli_ok(rc_set) {
                rc_set = unsafe {
                    cli::SQLSetStmtAttr(
                        h,
                        cli::SQL_ATTR_PARAM_BIND_TYPE,
                        attr_value(cli::SQL_PARAM_BIND_BY_COLUMN),
                        0,
                    )
                };
            }
            if !cli_ok(rc_set) {
                let e = make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLSetStmtAttr (array binding) failed",
                );
                reset_paramset(h);
                return Err(e);
            }

            for c in 0..n_cols {
                let rc = unsafe {
                    cli::SQLBindParameter(
                        h,
                        (c + 1) as cli::SQLUSMALLINT,
                        cli::SQL_PARAM_INPUT,
                        c_type[c],
                        sql_type[c],
                        width[c] as cli::SQLULEN,
                        0,
                        buf[c].as_mut_ptr().cast(),
                        width[c],
                        ind[c].as_mut_ptr(),
                    )
                };
                if !cli_ok(rc) {
                    let e = make_error(
                        cli::SQL_HANDLE_STMT,
                        h,
                        ErrorCode::Unknown,
                        "SQLBindParameter (array) failed",
                    );
                    reset_paramset(h);
                    return Err(e);
                }
            }

            let rc = unsafe { cli::SQLExecute(h) };
            if !cli_ok(rc) && rc != cli::SQL_NO_DATA {
                // Db2 applies each array chunk atomically: a failed chunk
                // commits nothing, and the CLI does not expose which row failed
                // — surface the Db2 diagnostic (SQLSTATE) as-is. Wrap the call
                // in a transaction for whole-batch atomicity and re-drive on
                // error.
                let e = make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLExecute (array binding) failed",
                );
                reset_paramset(h);
                return Err(e);
            }
            let mut affected: cli::SQLLEN = 0;
            unsafe { cli::SQLRowCount(h, &mut affected) };
            total += i64::from(affected.max(0));
            start = end;
        }
        // On success the reset must stick, or a later reuse of this cached
        // statement would execute with the wrong paramset size.
        if !cli_ok(reset_paramset(h)) {
            return Err(make_error(
                cli::SQL_HANDLE_STMT,
                h,
                ErrorCode::Unknown,
                "SQLSetStmtAttr reset after array binding failed",
            ));
        }
        Ok(total)
    }

    fn column_count(&self, stmt: StatementHandle) -> Result<usize> {
        let Some(st) = self.stmt_state(stmt) else {
            return Err(unknown_stmt());
        };
        let mut st = st.lock().unwrap_or_else(|p| p.into_inner());
        ensure_rowset_reset(&mut st)?;
        let h = st.handle.0;
        let mut n: cli::SQLSMALLINT = 0;
        let rc = unsafe { cli::SQLNumResultCols(h, &mut n) };
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_STMT,
                h,
                ErrorCode::Unknown,
                "SQLNumResultCols failed",
            ));
        }
        Ok(n.max(0) as usize)
    }

    fn column_name(&self, stmt: StatementHandle, index: usize) -> Result<String> {
        let Some(st) = self.stmt_state(stmt) else {
            return Err(unknown_stmt());
        };
        let mut st = st.lock().unwrap_or_else(|p| p.into_inner());
        ensure_rowset_reset(&mut st)?;
        let h = st.handle.0;
        let mut name = [0u8; 256];
        let mut name_len: cli::SQLSMALLINT = 0;
        let mut sql_type: cli::SQLSMALLINT = 0;
        let mut col_size: cli::SQLULEN = 0;
        let mut scale: cli::SQLSMALLINT = 0;
        let mut nullable: cli::SQLSMALLINT = 0;
        let rc = unsafe {
            cli::SQLDescribeCol(
                h,
                (index + 1) as cli::SQLUSMALLINT,
                name.as_mut_ptr(),
                name.len() as cli::SQLSMALLINT,
                &mut name_len,
                &mut sql_type,
                &mut col_size,
                &mut scale,
                &mut nullable,
            )
        };
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_STMT,
                h,
                ErrorCode::Unknown,
                "SQLDescribeCol failed",
            ));
        }
        let len = (name_len.max(0) as usize).min(name.len());
        Ok(String::from_utf8_lossy(&name[..len]).into_owned())
    }

    fn fetch_block(&self, stmt: StatementHandle, max_rows: usize) -> Result<Vec<Vec<Value>>> {
        let Some(st) = self.stmt_state(stmt) else {
            return Err(unknown_stmt());
        };
        // A zero-sized request is explicitly allowed: it must not advance the
        // native cursor, and its empty success value is indistinguishable from
        // EOF only to a caller that chose not to request any rows.
        if max_rows == 0 {
            return Ok(Vec::new());
        }
        let mut st = st.lock().unwrap_or_else(|p| p.into_inner());
        ensure_rowset_reset(&mut st)?;
        if st.cols.is_none() {
            describe_columns(&mut st)?;
        }
        if st.block_fallback {
            fetch_block_fallback(&st)
        } else {
            fetch_block_rowset(&mut st, max_rows)
        }
    }

    fn finalize(&self, stmt: StatementHandle) -> Result<()> {
        let entry = {
            let mut inner = self.lock();
            match inner.stmts.remove(&stmt.0) {
                Some(entry) => entry,
                None => return Ok(()),
            }
        };
        match attempt_free_stmt(entry, |h| unsafe {
            cli::SQLFreeHandle(cli::SQL_HANDLE_STMT, h)
        }) {
            Ok(()) => Ok(()),
            Err(entry) => {
                let h = stmt_handle(&entry);
                let error = make_error(
                    cli::SQL_HANDLE_STMT,
                    h,
                    ErrorCode::Unknown,
                    "SQLFreeHandle(SQL_HANDLE_STMT) failed",
                );
                self.lock().stmts.insert(stmt.0, entry);
                Err(error)
            }
        }
    }

    fn close_cursor(&self, stmt: StatementHandle) -> Result<()> {
        // Unknown/finalized handle: no-op.
        let Some(st) = self.stmt_state(stmt) else {
            return Ok(());
        };
        let mut st = st.lock().unwrap_or_else(|p| p.into_inner());
        ensure_rowset_reset(&mut st)?;
        let rc = unsafe { cli::SQLFreeStmt(st.handle.0, cli::SQL_CLOSE) };
        // SQL_SUCCESS_WITH_INFO / "no cursor open" are benign; only a hard
        // error is reported.
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_STMT,
                st.handle.0,
                ErrorCode::Unknown,
                "SQLFreeStmt(SQL_CLOSE) failed",
            ));
        }
        st.cols = None;
        st.block_fallback = false;
        Ok(())
    }

    fn set_auto_commit(&self, conn: ConnectionHandle, enabled: bool) -> Result<()> {
        let Some(dbc) = self.conn_handle(conn) else {
            return Err(unknown_conn());
        };
        let v = attr_value(if enabled {
            cli::SQL_AUTOCOMMIT_ON
        } else {
            cli::SQL_AUTOCOMMIT_OFF
        });
        let rc = unsafe {
            cli::SQLSetConnectAttr(dbc.0, cli::SQL_ATTR_AUTOCOMMIT, v, cli::SQL_IS_INTEGER)
        };
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_DBC,
                dbc.0,
                ErrorCode::Unknown,
                "SQLSetConnectAttr(AUTOCOMMIT) failed",
            ));
        }
        Ok(())
    }

    fn set_isolation(&self, conn: ConnectionHandle, level: Isolation) -> Result<()> {
        let Some(dbc) = self.conn_handle(conn) else {
            return Err(unknown_conn());
        };
        let rc = unsafe {
            cli::SQLSetConnectAttr(
                dbc.0,
                cli::SQL_ATTR_TXN_ISOLATION,
                attr_value(isolation_attr_value(level)),
                0,
            )
        };
        if !cli_ok(rc) {
            return Err(make_error(
                cli::SQL_HANDLE_DBC,
                dbc.0,
                ErrorCode::Connection,
                "SQLSetConnectAttr(SQL_ATTR_TXN_ISOLATION) failed",
            ));
        }
        Ok(())
    }

    fn commit(&self, conn: ConnectionHandle) -> Result<()> {
        self.end_tran(conn, cli::SQL_COMMIT, "SQLEndTran(COMMIT) failed")
    }

    fn rollback(&self, conn: ConnectionHandle) -> Result<()> {
        self.end_tran(conn, cli::SQL_ROLLBACK, "SQLEndTran(ROLLBACK) failed")
    }
}

// These tests need libdb2 linked (feature `db2`) but no running database:
// they exercise only the driver's bookkeeping and validation paths.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isolation_levels_map_to_db2_cli_values() {
        assert_eq!(
            isolation_attr_value(Isolation::UncommittedRead),
            cli::SQL_TXN_READ_UNCOMMITTED
        );
        assert_eq!(
            isolation_attr_value(Isolation::CursorStability),
            cli::SQL_TXN_READ_COMMITTED
        );
        assert_eq!(
            isolation_attr_value(Isolation::ReadStability),
            cli::SQL_TXN_REPEATABLE_READ
        );
        assert_eq!(
            isolation_attr_value(Isolation::RepeatableRead),
            cli::SQL_TXN_SERIALIZABLE
        );
    }

    #[test]
    fn driver_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Db2CliDriver>();
    }

    #[test]
    fn reset_failure_retains_native_rowset_pointer_storage() {
        let mut storage = Box::new(RowsetStorage {
            buffers: vec![vec![0]],
            indicators: vec![vec![0]],
            rows_fetched: 0,
            row_status: vec![0],
        });
        let fetched_ptr = std::ptr::addr_of_mut!(storage.rows_fetched);
        let status_ptr = storage.row_status.as_mut_ptr();
        let mut st = StmtState {
            handle: RawHandle(ptr::null_mut()),
            bound: Vec::new(),
            cols: None,
            block_fallback: false,
            pending_rowset: None,
        };

        let err = finish_rowset(
            &mut st,
            storage,
            Ok(Vec::new()),
            Some(Error::new(ErrorCode::Unknown, "reset failed")),
        )
        .unwrap_err();
        assert_eq!(err.message, "reset failed");
        let retained = st.pending_rowset.as_mut().unwrap();
        assert_eq!(std::ptr::addr_of_mut!(retained.rows_fetched), fetched_ptr);
        assert_eq!(retained.row_status.as_mut_ptr(), status_ptr);
    }

    #[test]
    fn failed_statement_free_returns_pending_rowset_owner() {
        let mut storage = Box::new(RowsetStorage {
            buffers: vec![vec![0]],
            indicators: vec![vec![0]],
            rows_fetched: 0,
            row_status: vec![0],
        });
        let fetched_ptr = std::ptr::addr_of_mut!(storage.rows_fetched);
        let status_ptr = storage.row_status.as_mut_ptr();
        let entry = StmtEntry {
            conn: 1,
            state: Arc::new(Mutex::new(StmtState {
                handle: RawHandle(ptr::null_mut()),
                bound: Vec::new(),
                cols: None,
                block_fallback: false,
                pending_rowset: Some(storage),
            })),
        };

        let retained = attempt_free_stmt(entry, |_| cli::SQL_ERROR).unwrap_err();
        let mut state = retained.state.lock().unwrap();
        let storage = state.pending_rowset.as_mut().unwrap();
        assert_eq!(std::ptr::addr_of_mut!(storage.rows_fetched), fetched_ptr);
        assert_eq!(storage.row_status.as_mut_ptr(), status_ptr);
    }

    #[test]
    fn unknown_handles_are_rejected_or_no_ops() {
        let d = Db2CliDriver::new();
        let conn = ConnectionHandle(42);
        let stmt = StatementHandle(42);

        assert!(!d.is_alive(conn).unwrap());
        d.disconnect(conn).unwrap(); // idempotent no-op
        d.finalize(stmt).unwrap(); // idempotent no-op
        d.close_cursor(stmt).unwrap(); // no-op

        assert_eq!(
            d.prepare(conn, "SELECT 1").unwrap_err().code,
            ErrorCode::Connection
        );
        assert_eq!(
            d.set_auto_commit(conn, false).unwrap_err().code,
            ErrorCode::Connection
        );
        assert_eq!(d.commit(conn).unwrap_err().code, ErrorCode::Connection);
        assert_eq!(d.rollback(conn).unwrap_err().code, ErrorCode::Connection);

        assert_eq!(
            d.bind_params(stmt, &[]).unwrap_err().code,
            ErrorCode::Unknown
        );
        assert_eq!(d.execute(stmt).unwrap_err().code, ErrorCode::Unknown);
        assert_eq!(d.fetch_block(stmt, 1).unwrap_err().code, ErrorCode::Unknown);
        assert_eq!(d.column_count(stmt).unwrap_err().code, ErrorCode::Unknown);
    }

    #[test]
    fn connect_rejects_oversized_connection_string() {
        // A DSN longer than SQLSMALLINT::MAX (32767) bytes would wrap to a
        // negative length at the ABI; the checked conversion rejects it cleanly.
        // Runs without a live database: the length check fires before
        // SQLDriverConnect ever opens a socket.
        let d = Db2CliDriver::new();
        let params = ConnectionParams {
            connection_string: "A".repeat(40_000),
            isolation: None,
        };
        let err = d.connect(&params).unwrap_err();
        assert_eq!(err.code, ErrorCode::Connection);
    }

    #[test]
    fn execute_batch_rejects_malformed_shape_before_any_binding() {
        let d = Db2CliDriver::new();
        let stmt = StatementHandle(42);

        let ragged = vec![vec![Value::Int(1), Value::Int(2)], vec![Value::Int(3)]];
        assert_eq!(
            d.execute_batch(stmt, &ragged).unwrap_err().code,
            ErrorCode::Mapping
        );

        let mixed = vec![
            vec![Value::Int(1)],
            vec![Value::Text("overflows an 8-byte slot".into())],
        ];
        assert_eq!(
            d.execute_batch(stmt, &mixed).unwrap_err().code,
            ErrorCode::Mapping
        );
    }
}
