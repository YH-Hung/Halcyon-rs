//! The async facade: [`AsyncDatabase`], a feature-gated `tokio` wrapper over the
//! synchronous pooled [`Database`].
//!
//! Ported from the C++ `Database::executeAsync`/`queryAsync`
//! (`../Halcyon/include/halcyon/async.hpp`). The C++ submits work to a fixed-size
//! `Executor` thread pool and returns `std::future`; the Rust port offloads each
//! sync `Database` method to tokio's blocking pool with
//! [`tokio::task::spawn_blocking`] and awaits it. The synchronous data path is
//! reused verbatim, so read/write auto-retry, connection pooling, the
//! per-connection statement cache, and broken-connection discard all carry over.
//!
//! Only `execute`, materialized `query_as`, `execute_batch`, and closure
//! `transaction` are async; streaming `query` stays sync-only (a pool lease must
//! not cross an await point — a documented C++ non-goal). With the
//! `observability` feature, the current [`tracing`] span and `Dispatch` are
//! captured on the async task and entered/set inside the blocking closure so
//! caller spans parent `halcyon.*` spans across `spawn_blocking`. (The
//! `metrics` crate's thread-local recorder does not cross `spawn_blocking`;
//! install a process-global recorder if you need metrics from async callers.)
//!
//! ## Cancellation
//! Dropping the returned future does **not** cancel in-flight work: a
//! `spawn_blocking` task runs to completion and the pooled connection is returned
//! (or discarded) when the sync op finishes. Do not rely on future-drop to abort
//! a running statement.
//!
//! ## Runtime
//! Methods must be called from within a tokio runtime. Calling them with no
//! current runtime returns [`Error`](crate::Error) (`ErrorCode::Unknown`) rather
//! than panicking — checked via [`Handle::try_current`](tokio::runtime::Handle::try_current)
//! before [`spawn_blocking`](tokio::task::spawn_blocking).
//!
//! ## Admission control
//! An async [`Semaphore`](tokio::sync::Semaphore) with
//! [`PoolConfig::max`](crate::PoolConfig) permits is acquired **before**
//! `spawn_blocking` (the permit moves into the blocking closure). Surplus
//! callers wait as async tasks — matching the C++ fixed-size `Executor` — so a
//! large fan-out cannot exhaust Tokio's shared blocking pool. DB parallelism
//! remains capped by the connection pool; blocking-thread occupancy is capped
//! by the same `max`.

use std::fmt;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::core::{FromRow, IntoParams};
use crate::error::{Error, ErrorCode, Result};
use crate::facade::{Database, ScopedTransaction};
use crate::pool::ConnectionPool;
use crate::value::Value;

/// Async, pooled Db2 handle: a feature-gated `tokio` wrapper over [`Database`].
/// Cloneable and thread-safe — clones share one underlying [`ConnectionPool`]
/// and one admission [`Semaphore`].
#[derive(Clone)]
pub struct AsyncDatabase {
    inner: Database,
    /// Caps in-flight `spawn_blocking` work to [`ConnectionPool::max`].
    admit: Arc<Semaphore>,
}

impl fmt::Debug for AsyncDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncDatabase")
            .field("inner", &self.inner)
            .field("admit_available", &self.admit.available_permits())
            .finish()
    }
}

impl AsyncDatabase {
    /// Wraps a synchronous [`Database`] in the async facade — the injectable
    /// entry point (used by tests over a mock driver and by callers supplying
    /// their own `Database`). See also [`Database::into_async`].
    pub fn new(db: Database) -> Self {
        let max = db.pool().max().max(1);
        AsyncDatabase {
            admit: Arc::new(Semaphore::new(max)),
            inner: db,
        }
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
        run_blocking(&self.admit, move || db.execute(&sql, params)).await
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
        run_blocking(&self.admit, move || db.query_as::<T>(&sql, params)).await
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
        run_blocking(&self.admit, move || db.execute_batch(&sql, &rows)).await
    }

    /// Runs `f` inside a transaction on a single blocking thread: commits if it
    /// returns `Ok`, rolls back on `Err`, and rolls back on panic (via the
    /// [`ScopedTransaction`] drop guard, whose panic then propagates to the
    /// awaiter). `f` is synchronous — you cannot `.await` inside it (the whole
    /// unit of work runs on the blocking thread). Delegates to the sync
    /// [`Database::transaction`].
    pub async fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&ScopedTransaction) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let db = self.inner.clone();
        run_blocking(&self.admit, move || db.transaction(f)).await
    }
}

#[cfg(feature = "db2")]
impl AsyncDatabase {
    /// Opens an `AsyncDatabase` over a fresh real
    /// [`Db2CliDriver`](crate::driver::Db2CliDriver) built from `dsn`, with a
    /// default [`PoolConfig`] — the async analog of [`Database::connect`].
    /// Pool warmup (network connect) runs on a blocking thread so the caller
    /// task is not stalled.
    pub async fn connect(dsn: &str) -> Result<Self> {
        let dsn = dsn.to_string();
        // Bootstrap has no admission semaphore yet (the pool does not exist);
        // a single connect occupies one blocking thread.
        run_blocking_raw(move || Database::connect(&dsn).map(AsyncDatabase::new)).await
    }

    /// Like [`connect`](Self::connect) but with an explicit [`PoolConfig`].
    pub async fn connect_with(dsn: &str, config: crate::pool::PoolConfig) -> Result<Self> {
        let dsn = dsn.to_string();
        run_blocking_raw(move || Database::connect_with(&dsn, config).map(AsyncDatabase::new)).await
    }
}

/// Spawns `f` onto tokio's blocking pool after confirming a runtime is current.
/// Returns [`ErrorCode::Unknown`] when called outside a Tokio runtime (no panic).
fn spawn_blocking_checked<F, R>(f: F) -> Result<tokio::task::JoinHandle<R>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let handle = tokio::runtime::Handle::try_current().map_err(|_| {
        Error::new(
            ErrorCode::Unknown,
            "AsyncDatabase method called outside a Tokio runtime",
        )
    })?;
    Ok(handle.spawn_blocking(f))
}

/// Acquires an admission permit, then offloads `f` to the blocking pool.
async fn run_blocking<F, T>(admit: &Arc<Semaphore>, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let permit = admit.clone().acquire_owned().await.map_err(|_| {
        Error::new(
            ErrorCode::Unknown,
            "AsyncDatabase admission semaphore closed",
        )
    })?;
    run_blocking_with_permit(permit, f).await
}

/// Propagates the current tracing context onto a blocking-pool thread.
///
/// Skips [`tracing::dispatcher::set_default`] when the captured dispatch is the
/// no-op [`NoSubscriber`]: other concurrent `spawn_blocking` work would otherwise
/// churn the global callsite-interest cache and flake span-recording tests.
#[cfg(feature = "observability")]
fn enter_blocking_trace_context(
    parent: tracing::Span,
    dispatch: tracing::Dispatch,
) -> (
    Option<tracing::dispatcher::DefaultGuard>,
    tracing::span::EnteredSpan,
) {
    use tracing::subscriber::NoSubscriber;
    let guard = if dispatch.is::<NoSubscriber>() {
        None
    } else {
        Some(tracing::dispatcher::set_default(&dispatch))
    };
    let entered = parent.entered();
    (guard, entered)
}

/// Offloads without admission (bootstrap `connect` only).
#[cfg(feature = "db2")]
async fn run_blocking_raw<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "observability")]
    {
        let parent = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(|d| d.clone());
        join(
            spawn_blocking_checked(move || {
                let (_disp, _span) = enter_blocking_trace_context(parent, dispatch);
                f()
            })?
            .await,
        )
    }
    #[cfg(not(feature = "observability"))]
    {
        join(spawn_blocking_checked(f)?.await)
    }
}

async fn run_blocking_with_permit<F, T>(permit: OwnedSemaphorePermit, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "observability")]
    {
        let parent = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(|d| d.clone());
        join(
            spawn_blocking_checked(move || {
                let _permit = permit;
                let (_disp, _span) = enter_blocking_trace_context(parent, dispatch);
                f()
            })?
            .await,
        )
    }
    #[cfg(not(feature = "observability"))]
    {
        join(
            spawn_blocking_checked(move || {
                let _permit = permit;
                f()
            })?
            .await,
        )
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
    use crate::error::{Error, ErrorCode};
    use crate::pool::PoolConfig;
    use crate::retry::BackoffPolicy;
    use crate::testing::{MockCliDriver, ScriptedRows};
    use crate::value::Value;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "DSN=mock".into(),
            isolation: None,
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
        assert_eq!(
            db.admit.available_permits(),
            clone.admit.available_permits()
        )
    }

    #[test]
    fn spawn_blocking_checked_errors_without_runtime() {
        let err = spawn_blocking_checked(|| ()).unwrap_err();
        assert_eq!(err.code, ErrorCode::Unknown);
        assert!(err.message.contains("Tokio runtime"));
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

    #[tokio::test]
    async fn transaction_commits_on_ok() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let out = db
            .transaction(|tx| -> Result<i32> {
                tx.execute("INSERT INTO t VALUES (?)", (1,))?;
                Ok(99)
            })
            .await;
        assert_eq!(out.unwrap(), 99);
        assert_eq!(mock.commit_calls(), 1);
        assert_eq!(mock.rollback_calls(), 0);
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
        assert_eq!(db.pool().idle_count(), 1); // lease returned
    }

    #[tokio::test]
    async fn transaction_rolls_back_on_err() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);
        let out: Result<i32> = db
            .transaction(|_tx| Err(Error::new(ErrorCode::Constraint, "dup")))
            .await;
        assert_eq!(out.unwrap_err().code, ErrorCode::Constraint);
        assert_eq!(mock.commit_calls(), 0);
        assert_eq!(mock.rollback_calls(), 1);
    }

    #[tokio::test]
    async fn transaction_panic_rolls_back_and_propagates() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);

        // Spawn onto the runtime so the propagated panic is captured as a
        // JoinError instead of unwinding the test thread. Do not touch the
        // process-global panic hook (races with parallel tests).
        let db2 = db.clone();
        let handle = tokio::spawn(async move {
            db2.transaction(|_tx| -> Result<()> { panic!("boom") })
                .await
        });
        let joined = handle.await; // Err(JoinError::is_panic) if the panic propagated

        assert!(joined.is_err()); // the panic surfaced through the future
        assert!(joined.unwrap_err().is_panic());
        assert_eq!(mock.commit_calls(), 0);
        assert_eq!(mock.rollback_calls(), 1); // drop guard rolled back
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
    }

    #[tokio::test]
    async fn read_auto_retries_after_a_transient_connection_error() {
        let mock = Arc::new(MockCliDriver::new());
        // First execute of the SELECT dies with a retriable connection error;
        // the retry (on a fresh connection) succeeds. Confirms the sync retry
        // policy is intact under spawn_blocking.
        let mut e = Error::new(ErrorCode::Connection, "dropped");
        e.retriable = true;
        mock.push_execute_error(e);
        mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(42)]]));
        let db = open(&mock);
        let rows: Vec<(i64,)> = db.query_as("SELECT id FROM t", ()).await.unwrap();
        assert_eq!(rows, vec![(42,)]);
        assert_eq!(mock.connect_calls(), 2); // warmup + reconnect
        assert_eq!(mock.disconnect_calls(), 1); // the broken one
    }

    #[tokio::test]
    async fn into_async_wraps_the_database() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_exec_row_count(1);
        let db = Database::open(mock.clone(), params(), config())
            .unwrap()
            .into_async();
        assert_eq!(db.execute("UPDATE t SET a = 1", ()).await.unwrap(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_calls_share_the_pool() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock); // max = 4
        let peak = Arc::new(AtomicUsize::new(0));
        let mut set = tokio::task::JoinSet::new();
        // Hold each lease briefly so peak active_count is observable; admission
        // semaphore + pool max both cap at 4.
        for _ in 0..8 {
            let db = db.clone();
            let peak = peak.clone();
            set.spawn(async move {
                let probe = db.clone();
                db.transaction(move |_tx| {
                    peak.fetch_max(probe.pool().active_count(), Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(40));
                    Ok(1i64)
                })
                .await
            });
        }
        let mut total = 0;
        while let Some(res) = set.join_next().await {
            total += res.unwrap().unwrap();
        }
        assert_eq!(total, 8);
        let peak = peak.load(Ordering::SeqCst);
        assert!(
            peak >= 2,
            "expected overlapping leases, peak active_count was {peak}"
        );
        assert!(
            peak <= db.pool().max(),
            "peak active_count {peak} exceeded pool.max {}",
            db.pool().max()
        );
        assert_eq!(db.pool().active_count(), 0);
        assert_eq!(db.pool().idle_count(), db.pool().total_count());
        assert_eq!(db.admit.available_permits(), db.pool().max());
    }

    #[cfg(feature = "observability")]
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)] // OBS_LOCK serializes process-global subscriber install across tests
    async fn execute_async_inherits_caller_context() {
        use crate::obs::{find_span, obs_lock, RecordingSubscriber};
        use std::sync::Mutex;

        let _g = obs_lock();
        let mock = Arc::new(MockCliDriver::new());
        mock.push_exec_row_count(1);
        let db = AsyncDatabase::new(Database::open(mock, params(), config()).unwrap());

        let spans = Arc::new(Mutex::new(Vec::new()));
        let sub = RecordingSubscriber::new(Arc::clone(&spans));
        let dispatch = tracing::Dispatch::new(sub);
        let _disp = tracing::dispatcher::set_default(&dispatch);
        // Rebuild after install: other parallel tests may have registered
        // `halcyon.*` callsites as disabled under NoSubscriber.
        tracing_core::callsite::rebuild_interest_cache();

        let parent = tracing::info_span!("test.parent");
        let parent_id = {
            let guard = spans.lock().unwrap();
            find_span(&guard, "test.parent").map(|s| s.id)
        };
        assert!(parent_id.is_some(), "parent span must be recorded");
        {
            let _e = parent.enter();
            assert_eq!(
                db.execute("INSERT INTO t VALUES (?)", (1,)).await.unwrap(),
                1
            );
        }
        let guard = spans.lock().unwrap();
        let names: Vec<_> = guard.iter().map(|s| s.name.as_str()).collect();
        let exec = find_span(&guard, "halcyon.execute")
            .unwrap_or_else(|| panic!("execute span missing; saw {names:?}"));
        assert_eq!(exec.parent_id, parent_id);
    }
}

// Live Db2 async round-trip. Lives here (not in tests/db2_roundtrip.rs) so no
// tokio dev-dependency is needed — keeping tokio off the pure-Rust and MSRV CI
// lanes. Requires the `db2` feature (real driver) and is DSN-gated like the sync
// live round-trip: a no-op pass when HALCYON_TEST_DSN is unset or empty.
#[cfg(all(test, feature = "db2"))]
mod live_tests {
    use super::*;
    use crate::error::{Error, ErrorCode};

    #[tokio::test]
    async fn live_async_round_trip() {
        let Some(dsn) = std::env::var("HALCYON_TEST_DSN")
            .ok()
            .filter(|s| !s.is_empty())
        else {
            eprintln!("HALCYON_TEST_DSN not set; skipping live async round-trip");
            return;
        };
        let db = AsyncDatabase::connect(&dsn).await.expect("connect");

        // Fresh table (ignore the drop error if it does not exist yet).
        let _ = db.execute("DROP TABLE halcyon_async", ()).await;
        db.execute(
            "CREATE TABLE halcyon_async (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(32))",
            (),
        )
        .await
        .expect("create");

        db.execute(
            "INSERT INTO halcyon_async (id, name) VALUES (?, ?)",
            (1, "ann"),
        )
        .await
        .expect("insert");

        // Commit path: a whole unit of work on one blocking thread.
        let n = db
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO halcyon_async (id, name) VALUES (?, ?)",
                    (2, "bob"),
                )?;
                tx.execute(
                    "INSERT INTO halcyon_async (id, name) VALUES (?, ?)",
                    (3, "cy"),
                )
            })
            .await
            .expect("transaction commit");
        assert_eq!(n, 1); // rows affected by the last statement

        // Rollback path: failed unit of work must not leave rows behind.
        let rolled: Result<i64> = db
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO halcyon_async (id, name) VALUES (?, ?)",
                    (4, "dan"),
                )?;
                Err(Error::new(ErrorCode::Constraint, "force rollback"))
            })
            .await;
        assert_eq!(rolled.unwrap_err().code, ErrorCode::Constraint);

        let rows: Vec<(i64, String)> = db
            .query_as("SELECT id, name FROM halcyon_async ORDER BY id", ())
            .await
            .expect("query_as");
        assert_eq!(
            rows,
            vec![
                (1, "ann".to_string()),
                (2, "bob".to_string()),
                (3, "cy".to_string()),
            ]
        );

        db.execute("DROP TABLE halcyon_async", ())
            .await
            .expect("drop");
    }
}
