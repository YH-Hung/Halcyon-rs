# Phase 6 `tokio` Async Facade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `AsyncDatabase`, a feature-gated (`tokio`) async facade that runs the existing synchronous pooled `Database` operations on tokio's blocking pool via `spawn_blocking`.

**Architecture:** `AsyncDatabase` is a thin `Clone` wrapper holding a `Database` (a cheap `Arc<ConnectionPool>` handle). Each async method clones the inner `Database`, converts its args to owned `'static` values, offloads the corresponding sync method to `tokio::task::spawn_blocking`, and awaits it — so retry, pooling, statement-cache, and broken-connection discard all carry over unchanged. Streaming `query` stays sync-only (a lease must not cross an await point). No observability (Phase 7).

**Tech Stack:** Rust, tokio (`rt`/`rt-multi-thread`/`macros`, feature-gated and optional), the existing mock `CliDriver` test harness.

**Design reference:** [docs/superpowers/specs/2026-07-10-phase6-async-facade-design.md](../specs/2026-07-10-phase6-async-facade-design.md); C++ `../Halcyon/include/halcyon/async.hpp` + `Database::executeAsync`/`queryAsync`.

---

## File Structure

- **Create** `halcyon/src/async_db.rs` — the whole async facade: `AsyncDatabase`, its constructors/accessors, the four async methods (`execute`, `query_as`, `execute_batch`, `transaction`), the `join` JoinError helper, and the inline `#[cfg(test)]` tokio test suite (mock + DSN-gated live). Compiled only under `feature = "tokio"`. Single responsibility: async facade.
- **Modify** `halcyon/Cargo.toml` — add the `tokio` feature and the optional `tokio` dependency.
- **Modify** `halcyon/src/lib.rs` — declare the module and re-export `AsyncDatabase`, both `#[cfg(feature = "tokio")]`.
- **Modify** `halcyon/src/facade.rs` — add `Database::into_async` under `#[cfg(feature = "tokio")]`.
- **Modify** `.github/workflows/ci.yml` — add `tokio` lints/tests to the pure-Rust lane and an async live step to `live-db2`.
- **Modify** `README.md` and `halcyon/src/lib.rs` — roadmap checkbox, status line, feature-flag table.

**Why the live async test is a lib unit test (not in `tests/db2_roundtrip.rs`):** an integration-test file that uses `#[tokio::test]` would require `tokio` as a **dev-dependency**, which Cargo compiles for *every* `cargo test` on the package — including the pure-Rust and MSRV lanes. Keeping all tokio behind the optional feature dep (with the test-runtime features bundled in) means tokio never compiles unless `--features tokio` is passed, preserving the lean pure/MSRV lanes (the same discipline the Cargo.toml already applies to criterion/loom).

---

## Task 1: Feature, dependency, module scaffold

**Files:**
- Modify: `halcyon/Cargo.toml`
- Modify: `halcyon/src/lib.rs`
- Create: `halcyon/src/async_db.rs`

- [ ] **Step 1: Add the `tokio` feature and optional dependency to `halcyon/Cargo.toml`**

In the `[features]` table, after the `derive = [...]` line, add:

```toml
# Feature-gated async facade: `AsyncDatabase` over tokio's spawn_blocking. Off
# by default so the pure-Rust and MSRV lanes never compile tokio (mirrors how
# the criterion/loom deps are kept off those paths). The optional tokio dep
# below bundles the test-runtime features (rt-multi-thread, macros) alongside
# `rt`, so the `#[tokio::test]` suite runs from lib unit tests with no
# dev-dependency — keeping tokio out of every build that omits this feature.
tokio = ["dep:tokio"]
```

In the `[dependencies]` table, after the `halcyon-derive = { ... }` line, add:

```toml
# Async facade runtime (feature `tokio`). The library itself needs only `rt`
# (for spawn_blocking); `rt-multi-thread` + `macros` are here so the gated
# `#[tokio::test]` async unit tests build without a separate dev-dependency
# (which would otherwise compile tokio on the pure-Rust/MSRV test lanes).
tokio = { version = "1", optional = true, features = ["rt", "rt-multi-thread", "macros"] }
```

- [ ] **Step 2: Declare the module and re-export in `halcyon/src/lib.rs`**

After the `mod facade;` line, add:

```rust
// Feature-gated async facade (tokio spawn_blocking). See async_db.rs.
#[cfg(feature = "tokio")]
mod async_db;
```

After the `pub use facade::{...};` line, add:

```rust
#[cfg(feature = "tokio")]
pub use async_db::AsyncDatabase;
```

- [ ] **Step 3: Create `halcyon/src/async_db.rs` with the struct, constructors, accessors, and a Send/Sync test**

```rust
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
```

- [ ] **Step 4: Verify the scaffold builds and the test passes**

Run: `cargo test -p halcyon --no-default-features --features tokio async_database_is_send_sync_and_clone`
Expected: PASS (1 test). Also run `cargo clippy -p halcyon --no-default-features --features tokio --all-targets -- -D warnings` → no warnings.

- [ ] **Step 5: Commit**

```bash
git add halcyon/Cargo.toml halcyon/src/lib.rs halcyon/src/async_db.rs
git commit -m "feat(async): scaffold tokio feature and AsyncDatabase handle"
```

---

## Task 2: `execute`

**Files:**
- Modify: `halcyon/src/async_db.rs`

- [ ] **Step 1: Write the failing test** (add inside `mod tests`)

```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p halcyon --no-default-features --features tokio execute_returns_row_count`
Expected: FAIL — compile error, no method `execute` on `AsyncDatabase`.

- [ ] **Step 3: Add the import, the `join` helper, and the `execute` method**

At the top of the file, change the import block to add `IntoParams` and `Result`:

```rust
use crate::core::IntoParams;
use crate::error::Result;
use crate::facade::Database;
use crate::pool::ConnectionPool;
```

Inside `impl AsyncDatabase` (after `pool`), add:

```rust
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
```

After the `impl AsyncDatabase` block (before `#[cfg(test)]`), add the helper:

```rust
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p halcyon --no-default-features --features tokio execute_returns_row_count`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/async_db.rs
git commit -m "feat(async): AsyncDatabase::execute over spawn_blocking"
```

---

## Task 3: `query_as`

**Files:**
- Modify: `halcyon/src/async_db.rs`

- [ ] **Step 1: Write the failing test** (add inside `mod tests`)

```rust
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
```

Add these imports to the `use` list at the top of `mod tests`:

```rust
    use crate::testing::{MockCliDriver, ScriptedRows};
    use crate::value::Value;
```

(Replace the existing `use crate::testing::MockCliDriver;` line with the `ScriptedRows` form and add the `Value` import.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p halcyon --no-default-features --features tokio query_as_maps_tuples`
Expected: FAIL — no method `query_as` on `AsyncDatabase`.

- [ ] **Step 3: Add the import and the `query_as` method**

Add `FromRow` to the top-of-file core import:

```rust
use crate::core::{FromRow, IntoParams};
```

Inside `impl AsyncDatabase` (after `execute`), add:

```rust
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p halcyon --no-default-features --features tokio query_as_maps_tuples`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/async_db.rs
git commit -m "feat(async): AsyncDatabase::query_as over spawn_blocking"
```

---

## Task 4: `execute_batch`

**Files:**
- Modify: `halcyon/src/async_db.rs`

- [ ] **Step 1: Write the failing test** (add inside `mod tests`)

```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p halcyon --no-default-features --features tokio execute_batch_delegates_to_connection`
Expected: FAIL — no method `execute_batch` on `AsyncDatabase`.

- [ ] **Step 3: Add the `execute_batch` method**

Inside `impl AsyncDatabase` (after `query_as`), add:

```rust
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
```

Add `Value` to the top-of-file import (it is currently only imported inside `mod tests`):

```rust
use crate::value::Value;
```

(Place it with the other `use crate::...` lines at the top of the file.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p halcyon --no-default-features --features tokio execute_batch_delegates_to_connection`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/async_db.rs
git commit -m "feat(async): AsyncDatabase::execute_batch over spawn_blocking"
```

---

## Task 5: `transaction` (commit / rollback)

**Files:**
- Modify: `halcyon/src/async_db.rs`

- [ ] **Step 1: Write the failing tests** (add inside `mod tests`)

```rust
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
```

Add to the `use` list at the top of `mod tests`:

```rust
    use crate::error::{Error, ErrorCode};
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p halcyon --no-default-features --features tokio transaction_`
Expected: FAIL — no method `transaction` on `AsyncDatabase`.

- [ ] **Step 3: Add the import and the `transaction` method**

Add `ScopedTransaction` to the top-of-file facade import:

```rust
use crate::facade::{Database, ScopedTransaction};
```

Inside `impl AsyncDatabase` (after `execute_batch`), add:

```rust
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
        join(tokio::task::spawn_blocking(move || db.transaction(f)).await)
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p halcyon --no-default-features --features tokio transaction_`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/async_db.rs
git commit -m "feat(async): AsyncDatabase::transaction over spawn_blocking"
```

---

## Task 6: `transaction` panic propagation

**Files:**
- Modify: `halcyon/src/async_db.rs`

- [ ] **Step 1: Write the failing test** (add inside `mod tests`)

This proves the `join` helper re-raises a panic from the blocking closure (rather than swallowing it or hanging), and that the rollback still runs.

```rust
    #[tokio::test]
    async fn transaction_panic_rolls_back_and_propagates() {
        use std::panic::AssertUnwindSafe;
        use futures_or_std::FutureExt as _; // see note below

        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);

        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = AssertUnwindSafe(db.transaction(|_tx| -> Result<()> { panic!("boom") }))
            .catch_unwind()
            .await;
        std::panic::set_hook(prev);

        assert!(result.is_err()); // the panic surfaced through the future
        assert_eq!(mock.commit_calls(), 0);
        assert_eq!(mock.rollback_calls(), 1); // drop guard rolled back
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
    }
```

**Note — `catch_unwind` on a future:** `FutureExt::catch_unwind` comes from the `futures` crate, which is *not* a dependency here. Instead of adding one, catch the panic by driving the future to the `spawn_blocking` boundary through a nested runtime is awkward — so use `tokio::task::spawn` + `JoinHandle`, which captures the panic as a `JoinError`. Replace the test body above with this dependency-free version:

```rust
    #[tokio::test]
    async fn transaction_panic_rolls_back_and_propagates() {
        let mock = Arc::new(MockCliDriver::new());
        let db = open(&mock);

        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        // Spawn onto the runtime so the propagated panic is captured as a
        // JoinError instead of unwinding the test thread.
        let db2 = db.clone();
        let handle =
            tokio::spawn(async move { db2.transaction(|_tx| -> Result<()> { panic!("boom") }).await });
        let joined = handle.await; // Err(JoinError::is_panic) if the panic propagated
        std::panic::set_hook(prev);

        assert!(joined.is_err()); // the panic surfaced through the future
        assert!(joined.unwrap_err().is_panic());
        assert_eq!(mock.commit_calls(), 0);
        assert_eq!(mock.rollback_calls(), 1); // drop guard rolled back
        assert_eq!(mock.auto_commit_calls(), vec![false, true]);
    }
```

Delete the first (futures-based) version; keep only this second one. `tokio::spawn` requires the runtime, so mark the test `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` if it hangs on the current-thread runtime — but a plain `#[tokio::test]` current-thread runtime supports `tokio::spawn` and `spawn_blocking` concurrently, so start with `#[tokio::test]`.

- [ ] **Step 2: Run test to verify it fails**

First confirm it *compiles and fails meaningfully*. If the implementation from Task 2's `join` is correct (it uses `resume_unwind`), this test should already PASS once written — panic propagation is a property of `join`, which already exists. So:

Run: `cargo test -p halcyon --no-default-features --features tokio transaction_panic_rolls_back_and_propagates`
Expected: PASS (the `join` helper already re-raises the panic). This task is a **characterization test** locking in the panic contract; no new implementation is needed. If it FAILS, inspect whether `join` swallows the panic and fix `join` to `resume_unwind(err.into_panic())`.

- [ ] **Step 3: (No new implementation — contract already satisfied by `join`.)**

- [ ] **Step 4: Run the full async suite to confirm no regressions**

Run: `cargo test -p halcyon --no-default-features --features tokio`
Expected: PASS (all async tests so far).

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/async_db.rs
git commit -m "test(async): transaction panic rolls back and propagates through the future"
```

---

## Task 7: Read auto-retry composition + concurrency fan-out

**Files:**
- Modify: `halcyon/src/async_db.rs`

- [ ] **Step 1: Write the failing tests** (add inside `mod tests`)

```rust
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_calls_share_the_pool() {
        let mock = Arc::new(MockCliDriver::new());
        for _ in 0..8 {
            mock.push_exec_row_count(1);
        }
        let db = open(&mock); // max = 4
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let db = db.clone();
            set.spawn(async move { db.execute("UPDATE t SET a = 1", ()).await });
        }
        let mut total = 0;
        while let Some(res) = set.join_next().await {
            total += res.unwrap().unwrap();
        }
        assert_eq!(total, 8);
        // All leases returned; real parallelism was bounded by max=4.
        assert_eq!(db.pool().active_count(), 0);
    }
```

- [ ] **Step 2: Run tests to verify they pass**

These exercise already-implemented methods, so they should PASS immediately (characterizing retry + concurrency behavior). Run:
`cargo test -p halcyon --no-default-features --features tokio read_auto_retries_after_a_transient_connection_error concurrent_calls_share_the_pool`
Expected: PASS (2 tests). If `concurrent_calls_share_the_pool` fails to compile on `JoinSet`, confirm `rt-multi-thread` is in the tokio dep features (Task 1).

- [ ] **Step 3: (No new implementation.)**

- [ ] **Step 4: Run the full async suite**

Run: `cargo test -p halcyon --no-default-features --features tokio,derive`
Expected: PASS (all async tests, with derive also enabled to prove the feature combination compiles).

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/async_db.rs
git commit -m "test(async): read retry composes and concurrent calls share the pool"
```

---

## Task 8: `Database::into_async` and `db2`-gated `connect`

**Files:**
- Modify: `halcyon/src/facade.rs`
- Modify: `halcyon/src/async_db.rs`

- [ ] **Step 1: Write the failing test** (add inside `mod tests` in `async_db.rs`)

```rust
    #[tokio::test]
    async fn into_async_wraps_the_database() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_exec_row_count(1);
        let db = Database::open(mock.clone(), params(), config())
            .unwrap()
            .into_async();
        assert_eq!(db.execute("UPDATE t SET a = 1", ()).await.unwrap(), 1);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p halcyon --no-default-features --features tokio into_async_wraps_the_database`
Expected: FAIL — no method `into_async` on `Database`.

- [ ] **Step 3: Add `into_async` to `facade.rs` and the `db2` connect constructors to `async_db.rs`**

In `halcyon/src/facade.rs`, after the `#[cfg(feature = "db2")] impl Database { ... }` block (the one with `connect`/`connect_with`), add:

```rust
#[cfg(feature = "tokio")]
impl Database {
    /// Wraps this handle in an [`AsyncDatabase`](crate::AsyncDatabase) — the
    /// feature-gated `tokio` async facade. Cheap (moves the `Arc`-backed handle).
    pub fn into_async(self) -> crate::AsyncDatabase {
        crate::AsyncDatabase::new(self)
    }
}
```

In `halcyon/src/async_db.rs`, after the main `impl AsyncDatabase` block, add the DSN entry points (exercised by the live test in Task 9):

```rust
#[cfg(feature = "db2")]
impl AsyncDatabase {
    /// Opens an `AsyncDatabase` over a fresh real
    /// [`Db2CliDriver`](crate::driver::Db2CliDriver) built from `dsn`, with a
    /// default [`PoolConfig`] — the async analog of [`Database::connect`].
    pub fn connect(dsn: &str) -> Result<Self> {
        Ok(AsyncDatabase::new(Database::connect(dsn)?))
    }

    /// Like [`connect`](Self::connect) but with an explicit [`PoolConfig`].
    pub fn connect_with(dsn: &str, config: crate::pool::PoolConfig) -> Result<Self> {
        Ok(AsyncDatabase::new(Database::connect_with(dsn, config)?))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p halcyon --no-default-features --features tokio into_async_wraps_the_database`
Expected: PASS. Then verify the full-feature build compiles the `db2` connect path (needs clidriver; run only where available, otherwise rely on CI's `db2`/`live-db2` jobs):
`cargo build -p halcyon --features tokio` → compiles `AsyncDatabase::connect`.

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/facade.rs halcyon/src/async_db.rs
git commit -m "feat(async): Database::into_async and db2 AsyncDatabase::connect"
```

---

## Task 9: Live async round-trip (DSN-gated lib unit test)

**Files:**
- Modify: `halcyon/src/async_db.rs`

- [ ] **Step 1: Add the DSN-gated live test module** (at the bottom of `async_db.rs`, a sibling of `mod tests`)

```rust
// Live Db2 async round-trip. Lives here (not in tests/db2_roundtrip.rs) so no
// tokio dev-dependency is needed — keeping tokio off the pure-Rust and MSRV CI
// lanes. Requires the `db2` feature (real driver) and is DSN-gated like the sync
// live round-trip: a no-op pass when HALCYON_TEST_DSN is unset.
#[cfg(all(test, feature = "db2"))]
mod live_tests {
    use super::*;

    #[tokio::test]
    async fn live_async_round_trip() {
        let Some(dsn) = std::env::var("HALCYON_TEST_DSN").ok() else {
            eprintln!("HALCYON_TEST_DSN not set; skipping live async round-trip");
            return;
        };
        let db = AsyncDatabase::connect(&dsn).expect("connect");

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

        // A whole unit of work on one blocking thread.
        let n = db
            .transaction(|tx| {
                tx.execute("INSERT INTO halcyon_async (id, name) VALUES (?, ?)", (2, "bob"))?;
                tx.execute("INSERT INTO halcyon_async (id, name) VALUES (?, ?)", (3, "cy"))
            })
            .await
            .expect("transaction");
        assert_eq!(n, 1); // rows affected by the last statement

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
```

- [ ] **Step 2: Verify it compiles and is a no-op pass without a DSN**

Run (no clidriver needed for the *pure* build, but this module is `db2`-gated so it only compiles with `db2`; verify compilation where clidriver is available, else rely on CI):
`HALCYON_TEST_DSN= cargo test -p halcyon --features tokio --lib live_async_round_trip -- --nocapture`
Expected: PASS with "HALCYON_TEST_DSN not set; skipping..." (no-op) when unset. Against a real Db2 (DSN set), expected: PASS with the full round-trip.

- [ ] **Step 3: (No further implementation.)**

- [ ] **Step 4: Confirm the pure lane still ignores it**

Run: `cargo test -p halcyon --no-default-features --features tokio`
Expected: PASS — the `live_tests` module is absent (no `db2` feature), so no clidriver is required.

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/async_db.rs
git commit -m "test(async,live): DSN-gated async round-trip on real Db2"
```

---

## Task 10: CI lanes

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add `tokio` clippy + test to the pure-Rust lane**

In the `lint-and-pure` job, replace the "Clippy (pure-Rust, no clidriver)" step's `run:` block with:

```yaml
        run: |
          cargo clippy -p halcyon-derive --all-targets -- -D warnings
          cargo clippy -p halcyon --no-default-features --all-targets -- -D warnings
          cargo clippy -p halcyon --no-default-features --features derive --all-targets -- -D warnings
          cargo clippy -p halcyon --no-default-features --features tokio --all-targets -- -D warnings
          cargo clippy -p halcyon --no-default-features --features tokio,derive --all-targets -- -D warnings
```

Replace the "Test (pure-Rust, no clidriver)" step's `run:` block with:

```yaml
        run: |
          cargo test -p halcyon --no-default-features
          cargo test -p halcyon --no-default-features --features derive
          cargo test -p halcyon --no-default-features --features tokio
          cargo test -p halcyon --no-default-features --features tokio,derive
          cargo test -p halcyon-derive
```

- [ ] **Step 2: Add the async live step to the `live-db2` job**

In the `live-db2` job, immediately after the "Live round-trip" step (`cargo test --test db2_roundtrip -- --nocapture`) and before "Tear down Db2", insert:

```yaml
      - name: Live async round-trip
        run: cargo test -p halcyon --features tokio --lib live_async_round_trip -- --nocapture
```

(The job already exports `HALCYON_TEST_DSN`, so this step runs the async round-trip against the same Dockerized Db2.)

- [ ] **Step 3: Validate the workflow YAML**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('ok')"`
Expected: `ok` (well-formed YAML).

- [ ] **Step 4: (No test run — CI change; covered by the local `--features tokio` runs from Tasks 1–9.)**

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: exercise the tokio async feature (pure lane + live-db2)"
```

---

## Task 11: Docs (README + lib.rs status)

**Files:**
- Modify: `README.md`
- Modify: `halcyon/src/lib.rs`

- [ ] **Step 1: Update the README roadmap checkbox**

In `README.md`, change the line:

```markdown
- [ ] `tokio` async facade (feature-gated, `spawn_blocking`)
```

to:

```markdown
- [x] `tokio` async facade (feature-gated, `spawn_blocking`): `AsyncDatabase` wraps the pooled `Database` and offloads `execute`/`query_as`/`execute_batch`/`transaction` to `tokio::task::spawn_blocking`; streaming `query` stays sync-only. Off by default via the `tokio` feature — covered by mock `#[tokio::test]`s and a DSN-gated async round-trip on live Db2 (`live-db2`)
```

- [ ] **Step 2: Update the README status paragraph**

In `README.md`, in the `> **Status: early development.**` blockquote, replace the trailing sentence:

```markdown
The `tokio` async facade (Phase 6) is next. See the roadmap below.
```

with:

```markdown
The `tokio` async facade — `AsyncDatabase` over `spawn_blocking`, feature-gated and validated on live Db2 — is implemented (Phase 6 accepted). Observability (Phase 7) is next. See the roadmap below.
```

- [ ] **Step 3: Add the `tokio` row to the README feature-flag table**

In `README.md`, in the "Feature flags" table, after the `derive` row, add:

```markdown
| `tokio` | off | Enables the async facade (`AsyncDatabase`) over `tokio::task::spawn_blocking`. Pulls in `tokio`; off by default so the pure-Rust/MSRV builds never compile it. |
```

- [ ] **Step 4: Update the `lib.rs` status doc-comment**

In `halcyon/src/lib.rs`, replace the sentence:

```rust
//! against live Db2). The `tokio` async facade (Phase 6) is next. See the
//! README roadmap:
```

with:

```rust
//! against live Db2). The `tokio` async facade ([`AsyncDatabase`], feature
//! `tokio`) offloads `execute`/`query_as`/`execute_batch`/`transaction` to
//! `spawn_blocking` (Phase 6, validated against live Db2). Observability
//! (Phase 7) is next. See the README roadmap:
```

- [ ] **Step 5: Verify docs build and commit**

Run: `cargo doc -p halcyon --no-default-features --features tokio,derive --no-deps`
Expected: builds with no warnings (the `[`AsyncDatabase`]` intra-doc links resolve).

```bash
git add README.md halcyon/src/lib.rs
git commit -m "docs: mark Phase 6 (tokio async facade) accepted"
```

---

## Self-Review

**Spec coverage:**
- §4.1 feature/deps → Task 1. ✓
- §4.2 `AsyncDatabase` + constructors/accessors → Tasks 1 (`new`/`blocking`/`pool`), 8 (`into_async`, `connect`/`connect_with`). ✓
- §4.3 async surface (`execute`/`query_as`/`execute_batch`/`transaction`) → Tasks 2–5. ✓
- §4.4 `JoinError`/panic handling → `join` helper (Task 2), locked by Task 6. ✓
- §4.5 cancellation/runtime/concurrency docs → module docs (Task 1), concurrency test (Task 7). ✓
- §5 error handling (no new classes; retry composes) → Task 7 retry test. ✓
- §6 testing: mock unit tests (Tasks 2–8), CI lane (Task 10), live Db2 (Task 9), docs (Task 11). ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code. The only "no new implementation" steps (Tasks 6–7) are deliberate characterization tests over behavior already implemented by `join`/the sync path, with explicit fallback instructions if they fail. ✓

**Type consistency:** `join` signature and call sites match across Tasks 2–5; `AsyncDatabase`/`Database::into_async`/`AsyncDatabase::new` names are consistent; `execute_batch` takes owned `Vec<Vec<Value>>` in both the method (Task 4) and the live test (Task 9); feature name `tokio` is consistent across Cargo, `#[cfg]`, and CI. ✓

**Import churn note for the executor:** imports are added incrementally (Task 1: `Database`, `ConnectionPool`; Task 2: `IntoParams`, `Result`; Task 3: `FromRow`; Task 4: `Value`; Task 5: `ScopedTransaction`) to keep each intermediate commit warning-free under `-D warnings`. If you implement several methods in one pass, add all the corresponding imports together.
