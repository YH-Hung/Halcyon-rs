//! Query/execute/transaction instrumentation — C++ `Database::instrument` parity.

use std::time::Instant;

use crate::error::Result;
use crate::obs::labels::{code_label, op_label};
use tracing::subscriber::NoSubscriber;

/// True when the current dispatcher will discard all spans/events.
fn tracing_uninstrumented() -> bool {
    tracing::dispatcher::get_default(|d| d.is::<NoSubscriber>())
}

/// Wraps an execute-style call: `halcyon.execute` span + query metrics + `db.rows_affected`.
pub(crate) fn instrument_execute(sql: &str, f: impl FnOnce() -> Result<i64>) -> Result<i64> {
    if tracing_uninstrumented() {
        // Still emit metrics (no-op without a recorder); skip span creation so we
        // do not register `halcyon.*` callsites as disabled under NoSubscriber
        // (which races concurrent tests / late subscriber install).
        let op = op_label(sql);
        let start = Instant::now();
        let result = f();
        emit_query_metrics(op, start, &result, None);
        return result;
    }
    let op = op_label(sql);
    let span = tracing::info_span!(
        "halcyon.execute",
        db.system = "db2",
        db.statement = sql,
        db.rows_affected = tracing::field::Empty,
        error.sqlstate = tracing::field::Empty,
    );
    let _guard = span.enter();
    let start = Instant::now();
    let result = f();
    emit_query_metrics(op, start, &result, Some(&span));
    if let Ok(n) = &result {
        span.record("db.rows_affected", tracing::field::display(n));
    }
    result
}

/// Wraps a query-style call: `halcyon.query` span + query metrics (no rows_affected).
pub(crate) fn instrument_query<T>(sql: &str, f: impl FnOnce() -> Result<T>) -> Result<T> {
    if tracing_uninstrumented() {
        let op = op_label(sql);
        let start = Instant::now();
        let result = f();
        emit_query_metrics(op, start, &result, None);
        return result;
    }
    let op = op_label(sql);
    let span = tracing::info_span!(
        "halcyon.query",
        db.system = "db2",
        db.statement = sql,
        error.sqlstate = tracing::field::Empty,
    );
    let _guard = span.enter();
    let start = Instant::now();
    let result = f();
    emit_query_metrics(op, start, &result, Some(&span));
    result
}

fn emit_query_metrics<T>(
    op: &'static str,
    start: Instant,
    result: &Result<T>,
    span: Option<&tracing::Span>,
) {
    let elapsed = start.elapsed().as_secs_f64();
    // Literal label arms keep the metrics macros on static Keys (no Vec/Key
    // allocation when no recorder is installed — C++ zero-cost-when-off goal).
    record_duration(op, elapsed);
    match result {
        Ok(_) => bump_queries(op, true),
        Err(e) => {
            bump_queries(op, false);
            bump_error(e.code);
            if let Some(span) = span {
                if !e.sqlstate.is_empty() {
                    span.record("error.sqlstate", e.sqlstate.as_str());
                }
            }
            tracing::error!(error.sqlstate = %e.sqlstate, "halcyon operation failed");
        }
    }
}

fn record_duration(op: &'static str, elapsed: f64) {
    match op {
        "select" => {
            metrics::histogram!("halcyon_query_duration_seconds", "op" => "select").record(elapsed)
        }
        "insert" => {
            metrics::histogram!("halcyon_query_duration_seconds", "op" => "insert").record(elapsed)
        }
        "update" => {
            metrics::histogram!("halcyon_query_duration_seconds", "op" => "update").record(elapsed)
        }
        "delete" => {
            metrics::histogram!("halcyon_query_duration_seconds", "op" => "delete").record(elapsed)
        }
        "merge" => {
            metrics::histogram!("halcyon_query_duration_seconds", "op" => "merge").record(elapsed)
        }
        "with" => {
            metrics::histogram!("halcyon_query_duration_seconds", "op" => "with").record(elapsed)
        }
        "values" => {
            metrics::histogram!("halcyon_query_duration_seconds", "op" => "values").record(elapsed)
        }
        _ => metrics::histogram!("halcyon_query_duration_seconds", "op" => "other").record(elapsed),
    }
}

fn bump_queries(op: &'static str, ok: bool) {
    match (op, ok) {
        ("select", true) => {
            metrics::counter!("halcyon_queries_total", "op" => "select", "status" => "ok")
                .increment(1)
        }
        ("select", false) => {
            metrics::counter!("halcyon_queries_total", "op" => "select", "status" => "error")
                .increment(1)
        }
        ("insert", true) => {
            metrics::counter!("halcyon_queries_total", "op" => "insert", "status" => "ok")
                .increment(1)
        }
        ("insert", false) => {
            metrics::counter!("halcyon_queries_total", "op" => "insert", "status" => "error")
                .increment(1)
        }
        ("update", true) => {
            metrics::counter!("halcyon_queries_total", "op" => "update", "status" => "ok")
                .increment(1)
        }
        ("update", false) => {
            metrics::counter!("halcyon_queries_total", "op" => "update", "status" => "error")
                .increment(1)
        }
        ("delete", true) => {
            metrics::counter!("halcyon_queries_total", "op" => "delete", "status" => "ok")
                .increment(1)
        }
        ("delete", false) => {
            metrics::counter!("halcyon_queries_total", "op" => "delete", "status" => "error")
                .increment(1)
        }
        ("merge", true) => {
            metrics::counter!("halcyon_queries_total", "op" => "merge", "status" => "ok")
                .increment(1)
        }
        ("merge", false) => {
            metrics::counter!("halcyon_queries_total", "op" => "merge", "status" => "error")
                .increment(1)
        }
        ("with", true) => {
            metrics::counter!("halcyon_queries_total", "op" => "with", "status" => "ok")
                .increment(1)
        }
        ("with", false) => {
            metrics::counter!("halcyon_queries_total", "op" => "with", "status" => "error")
                .increment(1)
        }
        ("values", true) => {
            metrics::counter!("halcyon_queries_total", "op" => "values", "status" => "ok")
                .increment(1)
        }
        ("values", false) => {
            metrics::counter!("halcyon_queries_total", "op" => "values", "status" => "error")
                .increment(1)
        }
        (_, true) => metrics::counter!("halcyon_queries_total", "op" => "other", "status" => "ok")
            .increment(1),
        (_, false) => {
            metrics::counter!("halcyon_queries_total", "op" => "other", "status" => "error")
                .increment(1)
        }
    }
}

fn bump_error(code: crate::error::ErrorCode) {
    // Dispatch through `code_label` so the closed set stays in one place.
    match code_label(code) {
        "connection" => {
            metrics::counter!("halcyon_errors_total", "code" => "connection").increment(1)
        }
        "timeout" => metrics::counter!("halcyon_errors_total", "code" => "timeout").increment(1),
        "constraint" => {
            metrics::counter!("halcyon_errors_total", "code" => "constraint").increment(1)
        }
        "syntax" => metrics::counter!("halcyon_errors_total", "code" => "syntax").increment(1),
        "deadlock" => metrics::counter!("halcyon_errors_total", "code" => "deadlock").increment(1),
        "transient" => {
            metrics::counter!("halcyon_errors_total", "code" => "transient").increment(1)
        }
        "mapping" => metrics::counter!("halcyon_errors_total", "code" => "mapping").increment(1),
        "pool" => metrics::counter!("halcyon_errors_total", "code" => "pool").increment(1),
        _ => metrics::counter!("halcyon_errors_total", "code" => "unknown").increment(1),
    }
}

/// Emits `halcyon_retries_total{outcome}`.
pub(crate) fn emit_retry(outcome: &'static str) {
    match outcome {
        "retried" => {
            metrics::counter!("halcyon_retries_total", "outcome" => "retried").increment(1)
        }
        "exhausted" => {
            metrics::counter!("halcyon_retries_total", "outcome" => "exhausted").increment(1)
        }
        _ => metrics::counter!("halcyon_retries_total", "outcome" => "other").increment(1),
    }
    super::events::emit_retry_attempt(outcome);
}

/// Pending pool metric samples — filled under the pool mutex, flushed after unlock.
#[derive(Default)]
pub(crate) struct PendingMetrics {
    pub idle: Option<u64>,
    pub active: Option<u64>,
    pub acquire_wait_secs: Option<f64>,
    pub reconnects: u64,
    pub emit_reconnect_span: bool,
}

impl PendingMetrics {
    pub(crate) fn set_gauges(&mut self, idle: usize, active: usize) {
        self.idle = Some(idle as u64);
        self.active = Some(active as u64);
    }
}

/// Flush pool metrics / reconnect span. Must not be called while holding the pool lock.
pub(crate) fn flush_pool_metrics(pm: &PendingMetrics) {
    if let Some(v) = pm.idle {
        metrics::gauge!("halcyon_pool_connections", "state" => "idle").set(v as f64);
    }
    if let Some(v) = pm.active {
        metrics::gauge!("halcyon_pool_connections", "state" => "active").set(v as f64);
    }
    if let Some(s) = pm.acquire_wait_secs {
        metrics::histogram!("halcyon_pool_acquire_wait_seconds").record(s);
    }
    if pm.reconnects > 0 {
        metrics::counter!("halcyon_reconnects_total").increment(pm.reconnects);
    }
    if pm.emit_reconnect_span
        && !tracing::dispatcher::get_default(|d| d.is::<tracing::subscriber::NoSubscriber>())
    {
        let span = tracing::info_span!("halcyon.reconnect");
        let _g = span.enter();
    }
}

/// Statement-cache counter + size gauge (`hit`/`miss`/`evict`/`overflow`).
pub(crate) fn emit_stmt_cache(result: &'static str, size: usize) {
    match result {
        "hit" => metrics::counter!("halcyon_stmt_cache_total", "result" => "hit").increment(1),
        "miss" => metrics::counter!("halcyon_stmt_cache_total", "result" => "miss").increment(1),
        "evict" => metrics::counter!("halcyon_stmt_cache_total", "result" => "evict").increment(1),
        "overflow" => {
            metrics::counter!("halcyon_stmt_cache_total", "result" => "overflow").increment(1)
        }
        _ => metrics::counter!("halcyon_stmt_cache_total", "result" => "other").increment(1),
    }
    metrics::gauge!("halcyon_stmt_cache_size").set(size as f64);
}

/// Wraps a transaction unit of work in a `halcyon.transaction` span.
pub(crate) fn instrument_transaction<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    if tracing_uninstrumented() {
        return f();
    }
    let span = tracing::info_span!(
        "halcyon.transaction",
        db.system = "db2",
        error.sqlstate = tracing::field::Empty,
    );
    let _guard = span.enter();
    match f() {
        Err(e) => {
            if !e.sqlstate.is_empty() {
                span.record("error.sqlstate", e.sqlstate.as_str());
            }
            tracing::error!(error.sqlstate = %e.sqlstate, "halcyon transaction failed");
            Err(e)
        }
        ok => ok,
    }
}
