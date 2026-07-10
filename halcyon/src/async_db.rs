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

use crate::core::{FromRow, IntoParams};
use crate::error::Result;
use crate::facade::Database;
use crate::pool::ConnectionPool;
use crate::value::Value;

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

    /// Executes a non-query statement, returning rows affected. Accepts
    /// positional (`?`) or named (`:name`) parameters. Runs the sync
    /// [`Database::execute`] on a blocking thread; a read is auto-retried on a
    /// transient failure, a write runs once (unchanged from the sync path).
    pub async fn execute(&self, sql: impl Into<String>, params: impl IntoParams) -> Result<i64> {
        let db = self.inner.clone();
        let sql = sql.into();
        let params = params.into_params();
        join(tokio::task::spawn_blocking(move || db.execute(&sql, params)).await)
    }

    /// Runs a query and maps every row into `T` via [`FromRow`], collecting a
    /// `Vec` on a blocking thread (no pool lease escapes the future). Auto-retried
    /// for reads.
    pub async fn query_as<T>(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams,
    ) -> Result<Vec<T>>
    where
        T: FromRow + Send + 'static,
    {
        let db = self.inner.clone();
        let sql = sql.into();
        let params = params.into_params();
        join(tokio::task::spawn_blocking(move || db.query_as::<T>(&sql, params)).await)
    }

    /// Validates and runs a column-wise batch (rectangular + type-homogeneous)
    /// through the driver's array-binding path on a blocking thread. Takes owned
    /// `rows` (they move into the blocking closure). No auto-retry — the caller
    /// re-drives on failure; a connection-class failure discards the lease.
    pub async fn execute_batch(
        &self,
        sql: impl Into<String>,
        rows: Vec<Vec<Value>>,
    ) -> Result<i64> {
        let db = self.inner.clone();
        let sql = sql.into();
        join(tokio::task::spawn_blocking(move || db.execute_batch(&sql, &rows)).await)
    }
}

/// Flattens a `spawn_blocking` join result: the inner `Result<T, Error>` passes
/// through, and a panic in the blocking closure is re-raised on this task
/// (spawn_blocking tasks are never cancelled, so a `JoinError` can only be a
/// panic). This preserves the sync contract — a user transaction closure that
/// panics rolls back on the blocking thread via the `ScopedTransaction` drop
/// guard, then the panic propagates to the awaiter — and never turns a panic
/// into a spurious `Ok`.
fn join<T>(res: std::result::Result<Result<T>, tokio::task::JoinError>) -> Result<T> {
    match res {
        Ok(inner) => inner,
        Err(err) => std::panic::resume_unwind(err.into_panic()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::ConnectionParams;
    use crate::pool::PoolConfig;
    use crate::retry::BackoffPolicy;
    use crate::testing::{MockCliDriver, ScriptedRows};
    use crate::value::Value;
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

    #[tokio::test]
    async fn execute_returns_row_count() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_exec_row_count(3);
        let db = open(&mock);
        let n = db
            .execute("UPDATE t SET a = ? WHERE id = ?", (1, 5))
            .await
            .unwrap();
        assert_eq!(n, 3);
        // Lease returned to the pool after the call.
        assert_eq!(db.pool().idle_count(), 1);
        assert_eq!(db.pool().active_count(), 0);
    }

    #[tokio::test]
    async fn query_as_maps_tuples() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(
            ["id", "name"],
            vec![
                vec![Value::Int(1), Value::Text("ann".into())],
                vec![Value::Int(2), Value::Text("bob".into())],
            ],
        ));
        let db = open(&mock);
        let people: Vec<(i64, String)> = db
            .query_as("SELECT id, name FROM people", ())
            .await
            .unwrap();
        assert_eq!(people, vec![(1, "ann".to_string()), (2, "bob".to_string())]);
        assert_eq!(db.pool().idle_count(), 1); // lease released inside the call
    }

    #[tokio::test]
    async fn execute_batch_delegates_to_connection() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let rows = vec![
            vec![Value::Int(1), Value::Text("a".into())],
            vec![Value::Int(2), Value::Text("b".into())],
        ];
        let affected = db
            .execute_batch("INSERT INTO t VALUES (?, ?)", rows.clone())
            .await
            .unwrap();
        assert_eq!(affected, 2);
        assert_eq!(mock.execute_batch_calls(), 1);
        assert_eq!(mock.last_batch_rows(), rows);
    }
}
