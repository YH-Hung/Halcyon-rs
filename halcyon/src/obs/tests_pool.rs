//! Pool observability tests.

use std::sync::Arc;
use std::time::Duration;

use crate::driver::ConnectionParams;
use crate::facade::Database;
use crate::obs::{
    count, find_child, find_span, max_value, obs_lock, with_metrics, with_obs, with_tracing_events,
    RecordingRecorder,
};
use crate::pool::{ConnectionPool, PoolConfig};
use crate::retry::BackoffPolicy;
use crate::testing::MockCliDriver;
use metrics::{
    Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

fn params() -> ConnectionParams {
    ConnectionParams {
        connection_string: "DSN=mock".into(),
        isolation: None,
    }
}

fn config() -> PoolConfig {
    PoolConfig {
        min: 2,
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
fn pool_gauge_and_acquire_wait_emitted() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    mock.push_exec_row_count(0);

    with_obs(|rec, spans| {
        let db = Database::open(mock.clone(), params(), config()).unwrap();
        assert_eq!(
            max_value(
                &rec.snapshot(),
                "halcyon_pool_connections",
                &[("state", "idle")]
            ),
            2.0
        );
        assert!(db.execute("UPDATE t SET a=?", (1,)).is_ok());
        assert!(count(&rec.snapshot(), "halcyon_pool_acquire_wait_seconds", &[]) >= 1);
        let guard = spans.lock().unwrap();
        let exec = find_span(&guard, "halcyon.execute").expect("execute");
        assert!(find_child(&guard, "halcyon.acquire", exec.id).is_some());
    });
}

#[test]
fn reconnect_emits_reconnects_total() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    mock.push_alive_result(false);
    mock.push_exec_row_count(0);
    let mut cfg = config();
    cfg.min = 1;
    cfg.validate_on_acquire = true;

    with_metrics(|rec| {
        let db = Database::open(mock, params(), cfg).unwrap();
        assert!(db.execute("UPDATE t SET a=?", (1,)).is_ok());
        assert!(count(&rec.snapshot(), "halcyon_reconnects_total", &[]) >= 1);
    });
}

#[test]
fn connect_attempts_emit_exact_success_and_failure_events() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let mut failure = crate::error::Error::new(crate::error::ErrorCode::Connection, "refused");
    failure.sqlstate = "08001".into();
    failure.retriable = true;
    mock.push_connect_error(failure);
    let mut cfg = config();
    cfg.min = 1;

    with_tracing_events(|_, events| {
        ConnectionPool::create(mock, params(), cfg).unwrap();
        let guard = events.lock().unwrap();
        let fail = guard
            .iter()
            .find(|event| event.name == "connect.fail")
            .expect("connect.fail");
        assert_eq!(fail.level, tracing::Level::ERROR);
        assert_eq!(fail.fields.get("attempt").map(String::as_str), Some("1"));
        assert_eq!(
            fail.fields.get("code").map(String::as_str),
            Some("connection")
        );
        assert_eq!(
            fail.fields.get("sqlstate").map(String::as_str),
            Some("08001")
        );

        let ok = guard
            .iter()
            .find(|event| event.name == "connect.ok")
            .expect("connect.ok");
        assert_eq!(ok.level, tracing::Level::INFO);
        assert_eq!(ok.fields.len(), 1);
        assert_eq!(ok.fields.get("attempt").map(String::as_str), Some("2"));
    });
}

#[test]
fn reconnect_success_emits_attempt_and_ok_events() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let mut cfg = config();
    cfg.min = 1;
    cfg.validate_on_acquire = true;
    let pool = ConnectionPool::create(mock.clone(), params(), cfg).unwrap();
    mock.push_alive_result(false);

    with_tracing_events(|_, events| {
        drop(pool.acquire().unwrap());
        let guard = events.lock().unwrap();
        let attempt = guard
            .iter()
            .find(|event| event.name == "reconnect.attempt")
            .expect("reconnect.attempt");
        assert_eq!(attempt.level, tracing::Level::WARN);
        assert!(attempt.fields.is_empty());
        let ok = guard
            .iter()
            .find(|event| event.name == "reconnect.ok")
            .expect("reconnect.ok");
        assert_eq!(ok.level, tracing::Level::INFO);
        assert!(ok.fields.is_empty());
    });
}

#[test]
fn reconnect_failure_emits_error_code() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let mut cfg = config();
    cfg.min = 1;
    cfg.validate_on_acquire = true;
    let pool = ConnectionPool::create(mock.clone(), params(), cfg).unwrap();
    mock.push_alive_result(false);
    mock.push_connect_error(crate::error::Error::new(
        crate::error::ErrorCode::Connection,
        "refused",
    ));

    with_tracing_events(|_, events| {
        assert!(pool.acquire().is_err());
        let guard = events.lock().unwrap();
        let fail = guard
            .iter()
            .find(|event| event.name == "reconnect.fail")
            .expect("reconnect.fail");
        assert_eq!(fail.level, tracing::Level::ERROR);
        assert_eq!(fail.fields.len(), 1);
        assert_eq!(
            fail.fields.get("code").map(String::as_str),
            Some("connection")
        );
    });
}

#[test]
fn acquire_timeout_emits_pool_exhausted_timeout() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let mut cfg = config();
    cfg.min = 1;
    cfg.max = 1;
    cfg.acquire_timeout = Duration::from_millis(30);
    let pool = ConnectionPool::create(mock, params(), cfg).unwrap();
    let _hold = pool.acquire().unwrap();

    with_tracing_events(|_, events| {
        assert!(pool.acquire().is_err());
        let guard = events.lock().unwrap();
        let exhausted = guard
            .iter()
            .find(|event| event.name == "pool.exhausted")
            .expect("pool.exhausted");
        assert_eq!(exhausted.level, tracing::Level::WARN);
        assert_eq!(exhausted.fields.len(), 1);
        assert_eq!(
            exhausted.fields.get("timeout_ms").map(String::as_str),
            Some("30")
        );
    });
}

#[test]
fn maintain_emits_pool_reap_count() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let mut cfg = config();
    cfg.min = 1;
    cfg.idle_timeout = Duration::ZERO;
    let pool = ConnectionPool::create(mock, params(), cfg).unwrap();
    let a = pool.acquire().unwrap();
    let b = pool.acquire().unwrap();
    let c = pool.acquire().unwrap();
    drop((a, b, c));

    with_tracing_events(|_, events| {
        pool.maintain();
        let guard = events.lock().unwrap();
        let reap = guard
            .iter()
            .find(|event| event.name == "pool.reap")
            .expect("pool.reap");
        assert_eq!(reap.level, tracing::Level::DEBUG);
        assert_eq!(reap.fields.len(), 1);
        assert_eq!(reap.fields.get("count").map(String::as_str), Some("2"));
    });
}

struct ReenterGauge {
    pool: Arc<ConnectionPool>,
}

impl GaugeFn for ReenterGauge {
    fn increment(&self, _: f64) {}
    fn decrement(&self, _: f64) {}
    fn set(&self, _: f64) {
        let _ = self.pool.idle_count();
        let _ = self.pool.active_count();
    }
}

struct ReenterRecorder {
    pool: Arc<ConnectionPool>,
    inner: RecordingRecorder,
}

impl Recorder for ReenterRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, meta: &Metadata<'_>) -> Counter {
        self.inner.register_counter(key, meta)
    }

    fn register_histogram(&self, key: &Key, meta: &Metadata<'_>) -> Histogram {
        self.inner.register_histogram(key, meta)
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(ReenterGauge {
            pool: Arc::clone(&self.pool),
        }))
    }
}

#[test]
fn sink_may_reenter_pool_without_deadlock() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    mock.push_exec_row_count(0);
    // Build the pool first (warmup gauges use a no-op recorder), then install the
    // re-entrant recorder and acquire — flush runs after the pool mutex is dropped,
    // so idle_count/active_count from the gauge hook must not deadlock.
    let pool = Arc::new(ConnectionPool::create(mock, params(), config()).unwrap());
    let rec = ReenterRecorder {
        pool: Arc::clone(&pool),
        inner: RecordingRecorder::new(),
    };
    metrics::with_local_recorder(&rec, || {
        let lease = pool.acquire().unwrap();
        drop(lease);
    });
}

#[test]
fn maintain_flushes_pool_gauges() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let mut cfg = config();
    cfg.min = 1;
    cfg.max = 4;
    cfg.idle_timeout = Duration::ZERO;

    with_metrics(|rec| {
        let pool = ConnectionPool::create(mock, params(), cfg).unwrap();
        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        let c = pool.acquire().unwrap();
        drop((a, b, c));
        assert_eq!(pool.idle_count(), 3);
        // Clear warmup/acquire gauge samples so we assert the maintain flush.
        let _ = rec.snapshot();
        // Snapshot drains? No — RecordingRecorder accumulates. Grab length
        // before maintain and require a post-maintain idle=1 sample.
        pool.maintain();
        assert_eq!(pool.idle_count(), 1);
        let samples = rec.snapshot();
        let idle_after = samples
            .iter()
            .rev()
            .find(|s| {
                s.name == "halcyon_pool_connections"
                    && s.labels.iter().any(|(k, v)| k == "state" && v == "idle")
            })
            .map(|s| s.value);
        assert_eq!(idle_after, Some(1.0));
    });
}

#[test]
fn acquire_timeout_marks_span_errored() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    let mut cfg = config();
    cfg.min = 1;
    cfg.max = 1;
    cfg.acquire_timeout = Duration::from_millis(30);

    with_obs(|_rec, spans| {
        let pool = ConnectionPool::create(mock, params(), cfg).unwrap();
        let _hold = pool.acquire().unwrap();
        let err = pool.acquire().unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Pool);
        let guard = spans.lock().unwrap();
        let acquire = guard
            .iter()
            .rev()
            .find(|s| s.name == "halcyon.acquire")
            .expect("acquire span");
        assert!(acquire.errored, "timed-out acquire must be marked errored");
        assert!(acquire.attrs.contains_key("error.sqlstate"));
    });
}
