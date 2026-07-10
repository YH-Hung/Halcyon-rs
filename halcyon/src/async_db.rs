//! The async facade: [`AsyncDatabase`], a feature-gated `tokio` wrapper over the
//! synchronous pooled [`Database`].
//!
//! Ported from the C++ `Database::executeAsync`/`queryAsync`
//! (`../Halcyon/include/halcyon/async.hpp`). The C++ submits work to a bespoke
//! `Executor` thread pool and returns `std::future`; the Rust port offloads each
//! sync `Database` method to tokio's blocking pool with
//! [`tokio::task::spawn_blocking`] and awaits it. The synchronous data path is
//! reused verbatim, so read/write auto-retry, connection pooling, the
//! per-connection statement cache, and broken-connection discard all carry over.
//!
//! Only `execute`, materialized `query_as`, `execute_batch`, and closure
//! `transaction` are async; streaming `query` stays sync-only (a pool lease must
//! not cross an await point — a documented C++ non-goal). Observability is
//! Phase 7 and absent here.
//!
//! ## Cancellation
//! Dropping the returned future does **not** cancel in-flight work: a
//! `spawn_blocking` task runs to completion and the pooled connection is returned
//! (or discarded) when the sync op finishes. Do not rely on future-drop to abort
//! a running statement.
//!
//! ## Runtime
//! Methods must be called from within a tokio runtime (tokio's own
//! `spawn_blocking` contract). Effective DB concurrency is bounded by
//! [`PoolConfig::max`](crate::PoolConfig), not tokio's blocking-thread ceiling —
//! surplus concurrent calls block on the pool's `acquire`, exactly as concurrent
//! sync threads do.

use crate::facade::Database;
use crate::pool::ConnectionPool;

/// Async, pooled Db2 handle: a feature-gated `tokio` wrapper over [`Database`].
/// Cloneable and thread-safe — clones share one underlying [`ConnectionPool`].
#[derive(Clone, Debug)]
pub struct AsyncDatabase {
    inner: Database,
}

impl AsyncDatabase {
    /// Wraps a synchronous [`Database`] in the async facade — the injectable
    /// entry point (used by tests over a mock driver and by callers supplying
    /// their own `Database`). See also [`Database::into_async`].
    pub fn new(db: Database) -> Self {
        AsyncDatabase { inner: db }
    }

    /// Borrows the underlying synchronous [`Database`] — e.g. to run a streaming
    /// `query` (sync-only) on the same pool.
    pub fn blocking(&self) -> &Database {
        &self.inner
    }

    /// The underlying connection pool — for sizing metrics (`idle_count`,
    /// `active_count`, `total_count`) and manual `maintain`.
    pub fn pool(&self) -> &ConnectionPool {
        self.inner.pool()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::ConnectionParams;
    use crate::pool::PoolConfig;
    use crate::retry::BackoffPolicy;
    use crate::testing::MockCliDriver;
    use std::sync::Arc;
    use std::time::Duration;

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "DSN=mock".into(),
        }
    }

    /// Deterministic single-thread-friendly config: min=1, generous acquire, no
    /// reaper thread, zero backoff, 3 attempts (so reads auto-retry instantly).
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

    fn open(mock: &Arc<MockCliDriver>) -> AsyncDatabase {
        AsyncDatabase::new(Database::open(mock.clone(), params(), config()).unwrap())
    }

    #[test]
    fn async_database_is_send_sync_and_clone() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AsyncDatabase>();
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let clone = db.clone();
        // Both handles share one pool.
        let _l = db.pool().acquire().unwrap();
        assert_eq!(clone.pool().active_count(), 1);
    }
}
