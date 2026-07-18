//! No-op observability helpers when the `observability` feature is off.
//!
//! Keeps call sites as a single body (`instrument_*` / `emit_*`) so feature
//! arms cannot diverge. Pool pending-metrics stay behind `cfg` (they need
//! tracing spans), so they are not stubbed here.

use crate::error::{Error, Result};

pub(crate) fn instrument_execute(_sql: &str, f: impl FnOnce() -> Result<i64>) -> Result<i64> {
    f()
}

pub(crate) fn instrument_query<T>(_sql: &str, f: impl FnOnce() -> Result<T>) -> Result<T> {
    f()
}

pub(crate) fn instrument_transaction<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    f()
}

pub(crate) fn emit_retry(_outcome: &'static str) {}

pub(crate) fn emit_connect_fail(_attempt: u32, _error: &Error) {}

pub(crate) fn emit_connect_ok(_attempt: u32) {}

pub(crate) fn emit_reconnect_attempt() {}

pub(crate) fn emit_reconnect_ok() {}

pub(crate) fn emit_reconnect_fail(_error: &Error) {}

pub(crate) fn emit_pool_exhausted(_timeout_ms: u64) {}

pub(crate) fn emit_pool_reap(_count: usize) {}

pub(crate) fn emit_stmt_cache_evict(_sql: &str) {}

pub(crate) fn emit_txn_poisoned() {}

pub(crate) fn emit_savepoint_poisoned(_name: &str) {}

pub(crate) fn emit_isolation_restore_fail(_error: &Error) {}

pub(crate) fn emit_stmt_cache(_result: &'static str, _size: usize) {}
