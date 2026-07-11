//! No-op observability helpers when the `observability` feature is off.
//!
//! Keeps call sites as a single body (`instrument_*` / `emit_*`) so feature
//! arms cannot diverge. Pool pending-metrics stay behind `cfg` (they need
//! tracing spans), so they are not stubbed here.

use crate::error::Result;

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

pub(crate) fn emit_stmt_cache(_result: &'static str, _size: usize) {}
