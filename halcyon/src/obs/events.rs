//! Structured tracing events matching the C++ observability catalog.

use crate::error::Error;

pub(crate) fn emit_connect_ok(attempt: u32) {
    tracing::event!(
        name: "connect.ok",
        tracing::Level::INFO,
        attempt = attempt as u64,
    );
}

pub(crate) fn emit_connect_fail(attempt: u32, error: &Error) {
    tracing::event!(
        name: "connect.fail",
        tracing::Level::ERROR,
        attempt = attempt as u64,
        code = error.code.as_str(),
        sqlstate = error.sqlstate.as_str(),
    );
}

pub(crate) fn emit_reconnect_attempt() {
    tracing::event!(name: "reconnect.attempt", tracing::Level::WARN, {});
}

pub(crate) fn emit_reconnect_ok() {
    tracing::event!(name: "reconnect.ok", tracing::Level::INFO, {});
}

pub(crate) fn emit_reconnect_fail(error: &Error) {
    tracing::event!(
        name: "reconnect.fail",
        tracing::Level::ERROR,
        code = error.code.as_str(),
    );
}

pub(crate) fn emit_pool_exhausted(timeout_ms: u64) {
    tracing::event!(
        name: "pool.exhausted",
        tracing::Level::WARN,
        timeout_ms,
    );
}

pub(crate) fn emit_pool_reap(count: usize) {
    tracing::event!(
        name: "pool.reap",
        tracing::Level::DEBUG,
        count = count as u64,
    );
}

pub(crate) fn emit_retry_attempt(outcome: &'static str) {
    tracing::event!(
        name: "retry.attempt",
        tracing::Level::WARN,
        outcome,
    );
}

pub(crate) fn emit_stmt_cache_evict(sql: &str) {
    let sql_hash = fnv1a_sql_hash(sql);
    tracing::event!(
        name: "stmt_cache.evict",
        tracing::Level::DEBUG,
        sql_hash = sql_hash.as_str(),
    );
}

fn fnv1a_sql_hash(sql: &str) -> String {
    let mut hash = 1_469_598_103_934_665_603_u64;
    for byte in sql.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    format!("{hash:016x}")
}

pub(crate) fn emit_txn_poisoned() {
    tracing::event!(name: "txn.poisoned", tracing::Level::ERROR, {});
}

pub(crate) fn emit_savepoint_poisoned(name: &str) {
    tracing::event!(
        name: "savepoint.poisoned",
        tracing::Level::ERROR,
        name,
    );
}

pub(crate) fn emit_isolation_restore_fail(error: &Error) {
    tracing::event!(
        name: "isolation.restore_fail",
        tracing::Level::ERROR,
        code = error.code.as_str(),
    );
}
