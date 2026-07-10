//! Query/execute/transaction instrumentation — C++ `Database::instrument` parity.

use std::time::Instant;

use crate::error::Result;
use crate::obs::labels::{code_label, op_label};

/// Wraps an execute-style call: `halcyon.execute` span + query metrics + `db.rows_affected`.
pub(crate) fn instrument_execute(sql: &str, f: impl FnOnce() -> Result<i64>) -> Result<i64> {
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
    metrics::histogram!("halcyon_query_duration_seconds", "op" => op).record(elapsed);
    match result {
        Ok(_) => {
            metrics::counter!("halcyon_queries_total", "op" => op, "status" => "ok").increment(1);
        }
        Err(e) => {
            metrics::counter!("halcyon_queries_total", "op" => op, "status" => "error").increment(1);
            metrics::counter!("halcyon_errors_total", "code" => code_label(e.code)).increment(1);
            if let Some(span) = span {
                if !e.sqlstate.is_empty() {
                    span.record("error.sqlstate", e.sqlstate.as_str());
                }
            }
            tracing::error!(error.sqlstate = %e.sqlstate, "halcyon operation failed");
        }
    }
}

/// Emits `halcyon_retries_total{outcome}`.
pub(crate) fn emit_retry(outcome: &'static str) {
    metrics::counter!("halcyon_retries_total", "outcome" => outcome).increment(1);
}

/// Wraps a transaction unit of work in a `halcyon.transaction` span.
pub(crate) fn instrument_transaction<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
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
