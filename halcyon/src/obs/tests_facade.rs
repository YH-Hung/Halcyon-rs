//! Facade observability tests (Task 3+).

use std::sync::Arc;
use std::time::Duration;

use crate::driver::ConnectionParams;
use crate::error::{Error, ErrorCode, Result};
use crate::facade::Database;
use crate::obs::{count, find_child, find_span, obs_lock, with_obs};
use crate::pool::PoolConfig;
use crate::retry::BackoffPolicy;
use crate::testing::MockCliDriver;
use crate::value::Value;

fn params() -> ConnectionParams {
    ConnectionParams {
        connection_string: "DSN=mock".into(),
        isolation: None,
    }
}

fn config() -> PoolConfig {
    PoolConfig {
        min: 1,
        max: 4,
        acquire_timeout: Duration::from_millis(500),
        idle_timeout: Duration::from_secs(3600),
        max_lifetime: Duration::from_secs(3600),
        validate_on_acquire: false,
        backoff: BackoffPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            max_attempts: 3,
        },
        maintenance_interval: Duration::from_secs(3600),
        start_maintenance_thread: false,
        statement_cache_size: 0,
        isolation: None,
    }
}

#[test]
fn execute_emits_queries_total_and_duration() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    mock.push_exec_row_count(1);
    let db = Database::open(mock, params(), config()).unwrap();

    with_obs(|rec, spans| {
        assert_eq!(db.execute("INSERT INTO t VALUES (?)", (1,)).unwrap(), 1);
        let samples = rec.snapshot();
        assert_eq!(
            count(
                &samples,
                "halcyon_queries_total",
                &[("op", "insert"), ("status", "ok")]
            ),
            1
        );
        assert_eq!(
            count(
                &samples,
                "halcyon_query_duration_seconds",
                &[("op", "insert")]
            ),
            1
        );
        let guard = spans.lock().unwrap();
        let exec = find_span(&guard, "halcyon.execute").expect("execute span");
        assert_eq!(exec.attrs.get("db.system").map(String::as_str), Some("db2"));
        assert_eq!(
            exec.attrs.get("db.statement").map(String::as_str),
            Some("INSERT INTO t VALUES (?)")
        );
        assert_eq!(
            exec.attrs.get("db.rows_affected").map(String::as_str),
            Some("1")
        );
    });
}

#[test]
fn error_path_emits_errors_total_and_error_status() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    mock.push_execute_error(Error::new(ErrorCode::Syntax, "bad sql"));
    let db = Database::open(mock, params(), config()).unwrap();

    with_obs(|rec, spans| {
        assert!(db.execute("INSERT INTO t VALUES (?)", (1,)).is_err());
        let samples = rec.snapshot();
        assert_eq!(
            count(
                &samples,
                "halcyon_queries_total",
                &[("op", "insert"), ("status", "error")]
            ),
            1
        );
        assert_eq!(
            count(&samples, "halcyon_errors_total", &[("code", "syntax")]),
            1
        );
        let guard = spans.lock().unwrap();
        let exec = find_span(&guard, "halcyon.execute").expect("execute span");
        assert!(exec.errored);
    });
}

#[test]
fn retries_emit_retried_then_exhausted() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    for _ in 0..3 {
        let mut e = Error::new(ErrorCode::Deadlock, "deadlock");
        e.retriable = true;
        mock.push_execute_error(e);
    }
    let db = Database::open(mock, params(), config()).unwrap();

    with_obs(|rec, _spans| {
        let r: Result<Vec<(i64,)>> = db.query_as("SELECT n FROM t", ());
        assert!(r.is_err());
        let samples = rec.snapshot();
        assert_eq!(
            count(&samples, "halcyon_retries_total", &[("outcome", "retried")]),
            2
        );
        assert_eq!(
            count(
                &samples,
                "halcyon_retries_total",
                &[("outcome", "exhausted")]
            ),
            1
        );
        assert_eq!(
            count(&samples, "halcyon_queries_total", &[("status", "error")]),
            1
        );
    });
}

#[test]
fn mid_retry_non_retriable_does_not_emit_exhausted() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let mut deadlock = Error::new(ErrorCode::Deadlock, "deadlock");
    deadlock.retriable = true;
    mock.push_execute_error(deadlock);
    mock.push_execute_error(Error::new(ErrorCode::Syntax, "syntax"));
    let db = Database::open(mock, params(), config()).unwrap();

    with_obs(|rec, _spans| {
        let r: Result<Vec<(i64,)>> = db.query_as("SELECT n FROM t", ());
        assert!(r.is_err());
        let samples = rec.snapshot();
        assert_eq!(
            count(&samples, "halcyon_retries_total", &[("outcome", "retried")]),
            1
        );
        assert_eq!(
            count(
                &samples,
                "halcyon_retries_total",
                &[("outcome", "exhausted")]
            ),
            0
        );
    });
}

#[test]
fn execute_batch_emits_execute_metrics() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    mock.push_exec_row_count(2);
    let db = Database::open(mock, params(), config()).unwrap();
    let rows = vec![vec![Value::Int(1)], vec![Value::Int(2)]];

    with_obs(|rec, spans| {
        assert_eq!(
            db.execute_batch("INSERT INTO t VALUES (?)", &rows).unwrap(),
            2
        );
        let samples = rec.snapshot();
        assert_eq!(
            count(
                &samples,
                "halcyon_queries_total",
                &[("op", "insert"), ("status", "ok")]
            ),
            1
        );
        let guard = spans.lock().unwrap();
        assert!(find_span(&guard, "halcyon.execute").is_some());
    });
}

#[test]
fn scoped_transaction_execute_emits_execute_span_under_transaction() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    mock.push_exec_row_count(1);
    let db = Database::open(mock, params(), config()).unwrap();

    with_obs(|_rec, spans| {
        db.transaction(|tx| {
            tx.execute("INSERT INTO t VALUES (?)", (1,))?;
            Ok(())
        })
        .unwrap();
        let guard = spans.lock().unwrap();
        let txn = find_span(&guard, "halcyon.transaction").expect("txn span");
        assert!(find_child(&guard, "halcyon.execute", txn.id).is_some());
    });
}
