//! Facade observability tests (Task 3+).

use std::sync::Arc;
use std::time::Duration;

use crate::driver::ConnectionParams;
use crate::error::{Error, ErrorCode};
use crate::facade::Database;
use crate::obs::{count, find_span, obs_lock, with_obs};
use crate::pool::PoolConfig;
use crate::retry::BackoffPolicy;
use crate::testing::MockCliDriver;

fn params() -> ConnectionParams {
    ConnectionParams {
        connection_string: "DSN=mock".into(),
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
            count(&samples, "halcyon_query_duration_seconds", &[("op", "insert")]),
            1
        );
        let guard = spans.lock().unwrap();
        let exec = find_span(&guard, "halcyon.execute").expect("execute span");
        assert_eq!(
            exec.attrs.get("db.system").map(String::as_str),
            Some("db2")
        );
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
