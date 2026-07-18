//! Statement-cache observability tests.

use std::sync::Arc;
use std::time::Duration;

use crate::driver::ConnectionParams;
use crate::facade::Database;
use crate::obs::{count, max_value, obs_lock, with_metrics, with_tracing_events};
use crate::pool::PoolConfig;
use crate::retry::BackoffPolicy;
use crate::testing::{MockCliDriver, ScriptedRows};
use crate::value::Value;

fn params() -> ConnectionParams {
    ConnectionParams {
        connection_string: "DSN=mock".into(),
        isolation: None,
    }
}

fn cached_config() -> PoolConfig {
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
        statement_cache_size: 8,
        isolation: None,
    }
}

#[test]
fn statement_cache_hit_and_miss_emit_counters() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(7)]]));
    mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(7)]]));

    with_metrics(|rec| {
        let db = Database::open(mock, params(), cached_config()).unwrap();
        for _ in 0..2 {
            let rows: Vec<(i64,)> = db.query_as("SELECT id FROM t", ()).unwrap();
            assert_eq!(rows, vec![(7,)]);
        }
        let samples = rec.snapshot();
        assert_eq!(
            count(&samples, "halcyon_stmt_cache_total", &[("result", "miss")]),
            1
        );
        assert_eq!(
            count(&samples, "halcyon_stmt_cache_total", &[("result", "hit")]),
            1
        );
        assert!(max_value(&samples, "halcyon_stmt_cache_size", &[]) >= 1.0);
    });
}

#[test]
fn statement_cache_eviction_emits_only_fnv_sql_hash() {
    let _g = obs_lock();
    let mock = Arc::new(MockCliDriver::new());
    mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(1)]]));
    mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(2)]]));
    let mut cfg = cached_config();
    cfg.statement_cache_size = 1;
    let db = Database::open(mock, params(), cfg).unwrap();

    with_tracing_events(|_, events| {
        let first: Vec<(i64,)> = db.query_as("SELECT id FROM first", ()).unwrap();
        let second: Vec<(i64,)> = db.query_as("SELECT id FROM second", ()).unwrap();
        assert_eq!(first, vec![(1,)]);
        assert_eq!(second, vec![(2,)]);

        let guard = events.lock().unwrap();
        let evict = guard
            .iter()
            .find(|event| event.name == "stmt_cache.evict")
            .expect("stmt_cache.evict");
        assert_eq!(evict.level, tracing::Level::DEBUG);
        assert_eq!(evict.fields.len(), 1);
        assert_eq!(
            evict.fields.get("sql_hash").map(String::as_str),
            Some("24051bc60870353a")
        );
        assert!(evict.fields.values().all(|value| !value.contains("SELECT")));
    });
}
