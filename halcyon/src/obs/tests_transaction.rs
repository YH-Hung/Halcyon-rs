//! Transaction and savepoint structured-event tests.

use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, ErrorCode};
use crate::facade::Database;
use crate::obs::{obs_lock, with_tracing_events};
use crate::pool::PoolConfig;
use crate::retry::BackoffPolicy;
use crate::testing::MockCliDriver;
use crate::Isolation;

fn config() -> PoolConfig {
    PoolConfig {
        min: 1,
        max: 1,
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
fn failed_terminal_operation_emits_txn_poisoned_once() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let db = Database::open(mock.clone(), "DSN=mock", config()).unwrap();

    with_tracing_events(|_, events| {
        let mut tx = db.begin().unwrap();
        mock.push_txn_error(Error::new(ErrorCode::Connection, "commit failed"));
        assert!(tx.commit().is_err());
        drop(tx);
        let guard = events.lock().unwrap();
        let poisoned: Vec<_> = guard
            .iter()
            .filter(|event| event.name == "txn.poisoned")
            .collect();
        assert_eq!(poisoned.len(), 1);
        assert_eq!(poisoned[0].level, tracing::Level::ERROR);
        assert!(poisoned[0].fields.is_empty());
    });
}

#[test]
fn failed_named_savepoint_emits_savepoint_and_txn_poison_events_once() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let db = Database::open(mock.clone(), "DSN=mock", config()).unwrap();

    with_tracing_events(|_, events| {
        let tx = db.begin().unwrap();
        let savepoint = tx.savepoint_named("unit_work").unwrap();
        mock.push_execute_error(Error::new(ErrorCode::Connection, "release failed"));
        assert!(savepoint.release().is_err());
        drop(savepoint);
        drop(tx);

        let guard = events.lock().unwrap();
        let savepoint_events: Vec<_> = guard
            .iter()
            .filter(|event| event.name == "savepoint.poisoned")
            .collect();
        assert_eq!(savepoint_events.len(), 1);
        assert_eq!(savepoint_events[0].level, tracing::Level::ERROR);
        assert_eq!(savepoint_events[0].fields.len(), 1);
        assert_eq!(
            savepoint_events[0].fields.get("name").map(String::as_str),
            Some("unit_work")
        );
        assert_eq!(
            guard
                .iter()
                .filter(|event| event.name == "txn.poisoned")
                .count(),
            1
        );
    });
}

#[test]
fn isolation_restore_failure_emits_exact_error_code() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let db = Database::open(mock.clone(), "DSN=mock", config()).unwrap();
    mock.fail_isolation_on_call(
        2,
        Error::new(ErrorCode::Connection, "isolation restore failed"),
    );

    with_tracing_events(|_, events| {
        let mut tx = db.begin_with_isolation(Isolation::UncommittedRead).unwrap();
        assert!(tx.commit().is_err());
        let guard = events.lock().unwrap();
        let restore = guard
            .iter()
            .find(|event| event.name == "isolation.restore_fail")
            .expect("isolation.restore_fail");
        assert_eq!(restore.level, tracing::Level::ERROR);
        assert_eq!(restore.fields.len(), 1);
        assert_eq!(
            restore.fields.get("code").map(String::as_str),
            Some("Connection")
        );
    });
}
