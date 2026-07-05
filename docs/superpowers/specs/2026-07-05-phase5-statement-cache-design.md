# Halcyon-rs — Phase 5 Per-Connection Statement Cache — Design

**Date:** 2026-07-05
**Status:** Approved design; implementation plan to follow.
**Scope:** Phase 5 of the production-readiness roadmap: a per-connection LRU of
prepared statements, ported from the C++ reference
`../Halcyon/include/halcyon/detail/statement_cache.hpp` and its unit suite
`../Halcyon/tests/unit/test_statement_cache.cpp`. Metrics emission (Phase 7),
the async facade (Phase 6), and any cross-connection statement sharing are out
of scope.

## 1. Goal

Every `Connection::query`/`execute` today prepares a fresh statement handle and
finalizes it on drop. Phase 5 inserts the C++ `StatementCache` semantics at that
exact point: repeated SQL on the same connection reuses its prepared handle,
with the `close_cursor` reset-before-reuse discipline the seam trait already
declares. Exit criterion (from the master design doc): cache hit/miss/eviction
covered by tests, and reused cached statements re-execute correctly on live Db2.

## 2. Guiding invariants (unchanged from CLAUDE.md)

- **The seam rule.** No new driver entry points; `close_cursor` already exists
  on `CliDriver`. The cache is pure safe Rust above the seam.
- **Mock-testability.** All cache behavior is verified against the mock driver
  with no clidriver; `cargo test -p halcyon --no-default-features` must pass.
- **Error model.** `Result<T, Error>` only; cache-internal recovery (a failed
  `close_cursor` on release) is swallowed by design, never panics.
- **Port semantics, then idiomatize.** The C++ LRU/lease observable behavior is
  preserved one-for-one; the pointer-into-`std::list` plumbing is replaced with
  safe-Rust equivalents.

## 3. Decisions made during brainstorming

1. **Next phase = Phase 5** (statement cache), per the roadmap ordering. Async
   and observability stay Phases 6–7.
2. **Config mirrors C++:** `PoolConfig` gains `statement_cache_size: usize`
   with default **64**; `0` disables. The pooled `Database` facade therefore
   gets caching out of the box. Direct `Connection::open` stays uncached; a new
   constructor takes an explicit capacity (the pool uses it).
3. **Approach A chosen:** cache lives inside core `Connection` behind a
   `RefCell`, with an explicit-release `pub(crate)` lease token. Rejected:
   pool-layer cache (duplicates named-param rewriting, skips direct users) and
   `Arc<Mutex>` self-releasing leases (per-call locking a single-owner
   structure, and an `Arc`'d cache could outlive `Connection::drop`,
   reintroducing the finalize-after-disconnect hazard the C++ destructor
   ordering exists to prevent).
4. **No metrics hooks now.** The C++ cache emits `halcyon_stmt_cache_*`
   counters/gauges; the Rust equivalents attach in Phase 7 alongside the rest
   of observability. The C++ `EmitsMetrics` test defers with them.

## 4. Design

### 4.1 `StatementCache` (new `pub(crate)` module `core/stmt_cache.rs`)

Owned by `Connection` as `RefCell<StatementCache>`. This fits the established
contract: `Connection` is compile-time asserted `Send` **and deliberately
`!Sync`** (`core/connection.rs` `AmbiguousIfSync` test), and the pool leases
each connection to a single owner at a time, so no locking is needed and
`ResultSet` already uses `RefCell` internally for its error slot.

Storage is `HashMap<Box<str>, Entry>` with
`Entry { id: u64, handle: StatementHandle, busy: bool, last_used: u64 }`, a
monotonic tick for LRU stamps, and an id counter — replacing the C++
`std::list` + address-stable raw pointers. Hits are O(1); the eviction scan is
O(n ≤ capacity), trivial at the default 64.

The cache key is the **final** SQL text — after named-param (`:name` → `?`)
rewriting — exactly as the C++ caches `pre.value().sql`.

`acquire(driver, conn_handle, sql) -> Result<StmtLease>` ports the C++
semantics one-for-one:

- capacity 0 → transient (prepare; the lease finalizes on release);
- hit on an idle entry → mark busy, bump stamp, return the cached handle;
- hit on a **busy** entry (overlapping use of the same SQL, e.g. two live
  cursors) → transient overflow;
- miss → `driver.prepare`; if at capacity, evict the idle entry with the
  oldest stamp (finalize its handle); if **every** entry is busy, do not
  insert — return the fresh handle as transient.

`release(driver, lease)`:

- transient → finalize;
- cached + poisoned → finalize and remove the entry;
- cached + clean → `close_cursor`; on failure, finalize and remove the entry
  (never re-serve a half-reset handle — C++ spec §7), swallowing the error;
  on success, mark idle and bump the stamp (returns as most-recently-used).

`clear(driver)` finalizes every cached handle. `Connection::drop` calls it
**before** disconnect — the ordering the C++ destructor comment mandates
(finalizing statement handles after their connection is gone is invalid).

### 4.2 `StmtLease` (plain data token, no `Drop`)

`pub(crate) struct StmtLease { handle: StatementHandle, entry_id: Option<u64>,
poisoned: bool }` with a `poison()` method; `entry_id == None` means transient.
The C++ `StatementLease` RAII-releases via a raw pointer back into the cache;
safe Rust can't hold that pointer, so release is an explicit call:
`Connection::release_stmt(lease)`.

This is a convention, not compiler-enforced — but the holder set is closed and
each holder already has a `Drop` impl that finalizes the handle today; that
call becomes the release call:

| Holder | Today | After |
|---|---|---|
| core `Statement<'c>` (and `ResultSet<'c>` via `into_query`) | finalizes handle in `Drop` | holds `&'c Connection` + `StmtLease`; releases in `Drop` |
| facade `QueryResult` | `driver.finalize(stmt)` in `Drop` | stores the `StmtLease` from `execute_query_raw`; releases via its `PooledConnection` before the lease returns to the pool |
| facade `TxCursor` | finalizes via its owned statement | same pattern as `QueryResult` |

A leaked (never-released) lease degrades that entry to permanently-busy —
later acquires of its SQL go transient; degraded, not broken. A debug assertion
in the cache's teardown path flags leaked-busy entries under test.

### 4.3 `Connection` and plumbing changes

- New field `cache: RefCell<StatementCache>`.
- `prepare_and_bind_raw`, public `prepare`, and `execute_batch` route through
  `cache.acquire(final_sql)` instead of `driver.prepare`.
- `execute_query_raw` returns a `StmtLease` instead of a raw
  `StatementHandle`; the facade stores it.
- `Connection::open` keeps its signature and stays uncached (capacity 0). A
  new constructor `open_with_cache(driver, params, capacity)` is added; the
  pool calls it.
- `PoolConfig.statement_cache_size: usize`, default 64, validated like the
  other fields (any value legal; 0 disables). Both the pool's initial connect
  and its transparent-reconnect path pass it through — a reconnected
  connection starts with a fresh, empty cache.

### 4.4 Data flow

`db.query(sql, params)` → rewrite named params → `cache.acquire(final_sql)` →
miss: `driver.prepare` + insert busy entry / hit: reuse handle → bind →
execute → `QueryResult` streams rows → drop: `release_stmt` → `close_cursor`,
entry back to idle-MRU. The second identical query hits; `driver.prepare` runs
once per SQL per connection lifetime. Auto-retry composes: a replayed read
re-acquires from the cache, and if the failed attempt poisoned the entry the
replay re-prepares.

## 5. Error handling

**Any driver-side error while a lease is held poisons it** — bind, execute,
`execute_batch`, fetch, column read (the same trigger points where `ResultSet`
records driver-side errors today). A poisoned lease's entry is finalized and
dropped on release; the next acquire re-prepares.

- Client-side mapping errors (wrong target type, NULL into non-`Option`,
  bad column index) do **not** poison — preserving the existing
  driver-side/client-side distinction in `ResultSet`.
- Acquire-time prepare failure propagates; nothing is inserted.
- `close_cursor` failure on release → finalize + drop entry, error swallowed.
- The pool's poisoned-connection discard logic is untouched; a discarded
  connection's cache dies with it in `Connection::drop` (clear, then
  disconnect).

## 6. Testing & acceptance

**Unit (mock driver counting `prepare`/`finalize`/`close_cursor` calls)** —
port of the C++ suite:

1. miss-then-hit prepares once;
2. capacity 0 is always transient;
3. evicts least-recently-used at capacity;
4. busy hit serves transient overflow;
5. poison drops the entry;
6. `close_cursor` failure drops the entry;
7. drop finalizes live entries **before** disconnect (mock records call
   order).

(The C++ `EmitsMetrics` test is deferred to Phase 7.)

**Integration (mock)** — `Connection::query` twice → one prepare; facade
through the pool with default config exercises caching end-to-end; a retry
replay after a poisoned attempt re-prepares.

**Stress/perf** — no loom work (the cache is single-threaded per connection by
the pool's single-owner invariant). The existing TSan stress scenarios pick up
the cache automatically once the pool defaults to 64; `ConcurrentFakeDriver`
gains `close_cursor` support if it lacks it. The criterion scalar-query bench
gains a cached-vs-uncached comparison as the phase's perf evidence.

**Live Db2 (phase exit)** — extend the facade round-trip: repeated
parameterized queries through the pool, verifying correct results across
cached re-execution on real Db2 (amd64 Linux container / CI `live-db2` job).

**Docs** — README roadmap checkbox + status paragraph + `lib.rs` status sync,
as done at each prior phase acceptance.
