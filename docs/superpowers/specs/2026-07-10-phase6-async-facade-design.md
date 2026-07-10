# Halcyon-rs — Phase 6 `tokio` Async Facade — Design

**Date:** 2026-07-10
**Status:** Approved design; implementation plan to follow.
**Scope:** Phase 6 of the production-readiness roadmap: a feature-gated,
`tokio`-based async facade over the existing synchronous pooled `Database`,
ported from the C++ reference `../Halcyon/include/halcyon/async.hpp` +
`Database::executeAsync`/`queryAsync` (`../Halcyon/include/halcyon/database.hpp`).
The C++ uses a bespoke `Executor` thread pool + `std::future`; the Rust port
uses `tokio::task::spawn_blocking` (the settled design decision in CLAUDE.md).
Observability (`instrument`/OTel span propagation — Phase 7) and async streaming
over a live cursor (a documented C++ non-goal) are out of scope.

## 1. Goal

Provide an idiomatic async surface — `AsyncDatabase` — that lets callers run the
proven synchronous pooled operations (`execute`, materialized `query_as`,
`execute_batch`, closure `transaction`) from a tokio runtime without blocking an
executor thread. Each call offloads the existing sync method to tokio's blocking
pool via `spawn_blocking` and awaits it. No behavior of the data path changes;
this is a thin, additive wrapper. Exit criterion (matching prior phases): the
async surface is covered by mock-driver `#[tokio::test]`s with no clidriver, and
an async round-trip runs green on live Db2.

## 2. Guiding invariants (unchanged from CLAUDE.md)

- **The seam rule.** No new driver entry points and no `unsafe`. The async layer
  is pure safe Rust above the seam; `#![deny(unsafe_code)]` stands.
- **No hard dependencies for optional functionality.** Async is behind a new,
  **off-by-default** `tokio` feature. `cargo test -p halcyon --no-default-features`
  and the default build are unaffected; the MSRV job (pure-Rust, no tokio) is
  untouched.
- **Error model.** `Result<T, Error>` only. The wrapper introduces no new error
  paths; a genuine panic inside a user transaction closure is re-propagated
  faithfully (never swallowed, never converted to a spurious `Ok`).
- **Port semantics, then idiomatize.** The C++ async observable contract —
  materialize results on a worker thread so no pool lease escapes; capture the
  shared pool (not a specific handle) so the work outlives the launching handle;
  read/write retry via the same policy — is preserved. The `std::future` +
  hand-rolled `Executor` mechanism is replaced by `spawn_blocking`.

## 3. Decisions made during brainstorming

1. **Next phase = Phase 6** (async facade), per the roadmap ordering.
   Observability stays Phase 7.
2. **Separate `AsyncDatabase` handle** (chosen over async methods on `Database`
   or free helpers). Keeps the sync surface untouched, avoids sync-vs-async
   method-name collisions on one type, and gives a clean feature boundary.
3. **Parity minus streaming** (chosen over minimal C++ parity or async
   streaming). Async `execute`, `query_as`, `execute_batch`, and `transaction`;
   streaming cursor `query` stays sync-only — matching the C++ documented
   non-goal (a future must not carry a pool-lease lifetime, and holding a lease
   across an await point invites cancellation hazards).
4. **Mechanism = `spawn_blocking`** (settled in CLAUDE.md). Rejected: a bespoke
   Rust thread pool mirroring the C++ `Executor` (reinvents what tokio provides
   and needs its own correctness track) and an async-native driver (would breach
   the seam and abandon the proven sync path). Reusing the sync path verbatim
   means retry, pooling, statement-cache, and discard-on-connection-error
   semantics all carry over for free.
5. **No observability now.** The C++ async path is woven through `instrument`
   (metrics) and OTel context capture/propagation. The Rust equivalents attach
   in Phase 7; Phase 6's `spawn_blocking` wrapper has nothing woven in.
6. **Feature name `tokio`** (matches the roadmap wording and the `dep:tokio`
   enablement pattern), rather than an `async` feature that enables tokio
   internally.

## 4. Design

### 4.1 Feature flag & dependencies

- New feature `tokio` (off by default) enables `dep:tokio` with only the `rt`
  feature — `spawn_blocking` is all the library needs:
  `tokio = { version = "1", optional = true, features = ["rt"] }`.
- Dev-dependencies add `tokio` with `["macros", "rt-multi-thread"]` so the async
  tests can use `#[tokio::test]` on a multi-thread runtime.
- The feature is documented in the README feature-flag table and gates the new
  module and its `Database::into_async` conversion.

### 4.2 `AsyncDatabase` (new module `async_db.rs`, gated on `feature = "tokio"`)

```rust
#[derive(Clone, Debug)]
pub struct AsyncDatabase { inner: Database }
```

`Database` is already a cheap, cloneable, `Send + Sync` handle over
`Arc<ConnectionPool>`, so `AsyncDatabase` is likewise `Clone + Send + Sync`. It
holds a `Database` and clones it (one `Arc` bump) into each `spawn_blocking`
closure. This is the direct analog of the C++ task capturing the shared pool
rather than `this`: the closure owns everything it needs and is `'static`, so the
blocking work stays valid even if the launching `AsyncDatabase` handle is dropped
mid-flight.

**Constructors / accessors:**

- `AsyncDatabase::new(db: Database) -> Self` — the injectable entry point (wraps
  a `Database` built over a mock `CliDriver` in tests, mirroring
  `Database::open`).
- `Database::into_async(self) -> AsyncDatabase` — ergonomic conversion, gated on
  the `tokio` feature.
- `#[cfg(feature = "db2")] AsyncDatabase::connect(dsn) / connect_with(dsn, cfg)`
  — the no-CLI-types entry point (delegates to `Database::connect*`).
- `fn blocking(&self) -> &Database` — drop back to the sync surface (e.g. for a
  streaming `query`).
- `fn pool(&self) -> &ConnectionPool` — sizing metrics, as on `Database`.

### 4.3 Async surface

Every method clones `inner`, converts its arguments to owned `'static` values
**before** spawning, offloads the corresponding sync `Database` method to the
blocking pool, and awaits it:

```rust
pub async fn execute(&self, sql: impl Into<String>, params: impl IntoParams) -> Result<i64>
pub async fn query_as<T>(&self, sql: impl Into<String>, params: impl IntoParams) -> Result<Vec<T>>
    where T: FromRow + Send + 'static
pub async fn execute_batch(&self, sql: impl Into<String>, rows: Vec<Vec<Value>>) -> Result<i64>
pub async fn transaction<T, F>(&self, f: F) -> Result<T>
    where F: FnOnce(&ScopedTransaction) -> Result<T> + Send + 'static, T: Send + 'static
```

Canonical body (shown for `execute`; the others differ only in the sync call):

```rust
let db = self.inner.clone();
let sql = sql.into();
let params = params.into_params();          // owned Params (Send + 'static)
join(tokio::task::spawn_blocking(move || db.execute(&sql, params)).await)
```

- `IntoParams::into_params()` yields an owned `Params` (a `Vec<Value>`-backed
  type — already `Send + 'static`); conversion happens on the caller's task, so
  only owned data crosses into the closure.
- `execute_batch` takes an owned `Vec<Vec<Value>>` (rather than a borrowed
  `&[Vec<Value>]` as the sync form does) so the rows move into the closure.
- `transaction` runs the **entire** unit of work on one blocking thread: `f` is a
  `Send + 'static` synchronous closure receiving `&ScopedTransaction`. You cannot
  `.await` inside `f` — the same constraint as the C++ path materializing on its
  worker thread. Commit-on-`Ok` / rollback-on-`Err` / rollback-on-panic all come
  from the sync `Database::transaction` unchanged.

`query_as` requires `T: Send + 'static` because the materialized `Vec<T>` crosses
the `JoinHandle` back to the async task.

### 4.4 `JoinError` handling (`join` helper)

`spawn_blocking(...).await` yields `Result<R, tokio::task::JoinError>`. Because
`spawn_blocking` tasks are **never cancelled by the runtime**, a `JoinError` can
only mean the blocking closure panicked. The helper:

- `Ok(r)` → return `r` (already a `Result<T, Error>`).
- `Err(e)` where `e.is_panic()` → `std::panic::resume_unwind(e.into_panic())`,
  re-raising the original panic on the async task. This preserves the sync
  contract: the sync `Database::transaction`'s drop guard rolls back on the
  blocking thread as the panic unwinds, then the panic propagates to the awaiter
  — no lease is leaked and no panic is silently turned into an error.

### 4.5 Behavioral contracts (inherited, documented)

Because each call is the sync method verbatim, all of these carry over with no
new code: read/write auto-retry via `default_policy`, min/max pooling, per-lease
statement cache, and discard-of-broken-connection on a connection-class error.
Two async-specific points are documented on `AsyncDatabase`:

- **Cancellation.** Dropping the returned future does **not** cancel in-flight
  work. The `spawn_blocking` task runs to completion; the pooled connection is
  returned (or discarded) when the sync op finishes. Callers must not rely on
  future-drop to abort a running statement.
- **Runtime & concurrency.** Methods must be called within a tokio runtime
  (tokio's own `spawn_blocking` contract; it panics otherwise). Effective DB
  concurrency is bounded by `PoolConfig::max` (the pool's blocking-acquire), not
  by tokio's blocking-thread ceiling — many concurrent async calls simply queue
  on `acquire` exactly as concurrent sync threads do today.

### 4.6 Data flow

`async_db.execute(sql, params).await` → clone `Database`, own `sql`/`params` →
`spawn_blocking` → (on a blocking thread) `Database::execute` → lease a
connection → statement-cache acquire → bind/execute → release lease → return
`Result<i64>` → `JoinHandle` resolves → `join` helper unwraps → the awaiting task
receives `Result<i64>`. Concurrency: N spawned async calls fan out to N blocking
threads, each contending on the pool's `acquire` exactly as N sync threads would;
`max` bounds real parallelism, the rest block on acquire.

## 5. Error handling

- No new error classes. Every returned `Result<T, Error>` originates in the sync
  path with its existing classification and `retriable` flag.
- A panic inside a user transaction closure is re-propagated (§4.4), not
  converted to an `Error` — matching the sync surface.
- A pool `acquire` timeout, connection-class discard, or non-retriable error
  surfaces identically to the sync call; the async wrapper is transparent.
- No `unsafe`, no new poisoning logic; the pool's broken-connection discard and
  the statement cache's poison-on-driver-error are reached through the sync
  methods unchanged.

## 6. Testing & acceptance

**Unit / integration (mock driver, `#[tokio::test]`, no clidriver)** — run under
`cargo test -p halcyon --no-default-features --features tokio,derive`:

1. `execute` returns the affected-row count.
2. `query_as` maps rows into tuples / a `FromRow` struct.
3. `execute_batch` delegates to the connection's array-binding path.
4. `transaction` commits on `Ok` and rolls back on `Err` (assert `commit`/
   `rollback`/`auto_commit` call counts on the mock, as the sync suite does).
5. A panic inside a `transaction` closure rolls back and re-propagates the panic
   through the future (`catch_unwind` around `.await`).
6. `AsyncDatabase` is `Send + Sync + Clone`; two clones share one pool.
7. Concurrency: fan out N `execute`/`query_as` calls with `tokio::join!` /
   `JoinSet` over a small `max`, assert all succeed and the pool returns to a
   clean idle count (real parallelism bounded by `max`).
8. Read auto-retry composes through the async wrapper (a retriable transient on
   attempt 1, success on the replay) — confirming the sync retry policy is intact
   under `spawn_blocking`.

**CI** — add a build/test lane exercising `--features tokio` (pure-Rust +
tokio), alongside the existing `--no-default-features` matrix.

**Live Db2 (phase exit)** — add an async round-trip to the DSN-gated
`tests/db2_roundtrip.rs` (or a sibling `db2_roundtrip_async.rs`) behind
`#[tokio::test]`: connect → DDL → async insert → async `query_as` read →
async `transaction` commit/rollback, verifying results on real Db2 (amd64 Linux
container / CI `live-db2` job). Phase 6 is "accepted" only once this is green —
the same bar as Phases 3–5.

**Docs** — README roadmap checkbox + status paragraph + feature-flag table
(`tokio` row), and `lib.rs` status sync, as done at each prior phase acceptance.
