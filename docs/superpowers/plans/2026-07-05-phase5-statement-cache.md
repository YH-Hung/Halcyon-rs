# Phase 5 — Per-Connection Statement Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the C++ `detail/statement_cache.hpp` per-connection prepared-statement LRU into the Rust core, so repeated SQL on a connection reuses its prepared handle (default 64 entries per pooled connection, 0 disables).

**Architecture:** A `StatementCache` (HashMap + monotonic LRU stamps) lives inside `Connection` behind a `RefCell` — `Connection` is already compile-time-asserted `Send + !Sync`, so no locking. Every prepare path returns a `pub(crate) StmtLease` token (cached or transient) that holders release explicitly via `Connection::release_stmt` from their existing `Drop` impls. Any driver-side error while leased poisons the lease; poisoned/close-failed entries are finalized and dropped, never reused.

**Tech Stack:** Pure safe Rust (`std` only — `RefCell`, `HashMap`, `Cell`), existing `MockCliDriver` for tests, criterion for the perf comparison. No new dependencies, no seam changes (`CliDriver::close_cursor` already exists).

**Spec:** `docs/superpowers/specs/2026-07-05-phase5-statement-cache-design.md`
**C++ reference:** `../Halcyon/include/halcyon/detail/statement_cache.hpp`, `../Halcyon/tests/unit/test_statement_cache.cpp`, `../Halcyon/include/halcyon/connection.hpp` (lease call sites / poison policy).

**Working branch:** `feat/phase5-statement-cache` (already created off `main`).

**Primary test command (used throughout; needs no clidriver):**
```bash
cargo test -p halcyon --no-default-features
```

---

## File structure

| File | Change |
|---|---|
| `halcyon/src/core/stmt_cache.rs` | **Create.** `StatementCache` + `StmtLease` + module unit tests (the C++ suite port). |
| `halcyon/src/core/mod.rs` | Register `mod stmt_cache;`, `pub(crate) use` the two types. |
| `halcyon/src/core/connection.rs` | `Connection` gains the cache; `Statement` becomes a lease holder; poison wiring; `open_with_cache`; new tests. |
| `halcyon/src/core/result_set.rs` | `Drop for ResultSet` poisons the statement on a recorded driver-side error. |
| `halcyon/src/testing.rs` | Ordered `events()` log (finalize/disconnect) for the drop-ordering test. |
| `halcyon/src/pool.rs` | `PoolConfig.statement_cache_size` (default 64), `make_connection` uses it; test cfgs updated; new test. |
| `halcyon/src/facade.rs` | `QueryResult` holds a `StmtLease`; test config updated; new cache tests. |
| `halcyon/src/stress/scenarios.rs` | Scenario configs set `statement_cache_size: 64` (stress/TSan now exercises the cache). |
| `halcyon/benches/pool_bench.rs` | Cached-vs-uncached scalar-query benchmark. |
| `halcyon/tests/db2_roundtrip.rs` | DSN-gated live reuse test. |
| `README.md`, `halcyon/src/lib.rs` | Status + roadmap updates. |

Design invariants to preserve in every task (from CLAUDE.md):
- No `unsafe` anywhere in these files (`#![deny(unsafe_code)]` stays).
- `cargo test -p halcyon --no-default-features` passes after **every** task.
- No panicking public APIs (internal `expect` on just-checked invariants is the existing house style, e.g. `"pool mutex poisoned"`).

---

### Task 1: Ordered finalize/disconnect event log in the mock driver

The drop-ordering test (cache entries must be finalized **before** disconnect) needs call order, which the mock's counters can't express.

**Files:**
- Modify: `halcyon/src/testing.rs`

- [ ] **Step 1: Write the failing test** (append inside `mod tests` in `testing.rs`)

```rust
    #[test]
    fn event_log_records_finalize_and_disconnect_order() {
        let m = MockCliDriver::new();
        let c = m.connect(&conn_params()).unwrap();
        let s = m.prepare(c, "SELECT 1").unwrap();
        m.finalize(s).unwrap();
        m.disconnect(c).unwrap();
        assert_eq!(m.events(), vec!["finalize", "disconnect"]);
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p halcyon --no-default-features event_log_records`
Expected: FAIL to compile — `no method named 'events' found`.

- [ ] **Step 3: Implement**

In `struct State` (after the `close_cursor_errors` field):

```rust
    /// Ordered log of lifecycle calls whose relative order tests assert on
    /// (statement finalize vs. connection disconnect).
    events: Vec<&'static str>,
```

In `fn disconnect` (next to the counter increment):

```rust
    fn disconnect(&self, _conn: ConnectionHandle) -> Result<()> {
        let mut s = self.lock();
        s.disconnect_calls += 1;
        s.events.push("disconnect");
        Ok(())
    }
```

In `fn finalize` (next to the counter increment): add `s.events.push("finalize");` after `s.finalize_calls += 1;`.

In the inspection block (next to `finalize_calls()`):

```rust
    /// Ordered finalize/disconnect log — asserts teardown ordering.
    pub fn events(&self) -> Vec<&'static str> {
        self.lock().events.clone()
    }
```

- [ ] **Step 4: Run the full suite**

Run: `cargo test -p halcyon --no-default-features`
Expected: all PASS (the change is additive).

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/testing.rs
git commit -m "test(mock): ordered finalize/disconnect event log"
```

---

### Task 2: `StatementCache` core — acquire/release, LRU, busy overflow

**Files:**
- Create: `halcyon/src/core/stmt_cache.rs`
- Modify: `halcyon/src/core/mod.rs`

- [ ] **Step 1: Create the module with types and failing tests**

Create `halcyon/src/core/stmt_cache.rs`:

```rust
//! Per-connection LRU cache of prepared statements — the Rust port of the C++
//! `detail/statement_cache.hpp` (Phase 5).
//!
//! A [`StatementCache`] lives inside a `Connection` behind a `RefCell`: the
//! connection is `Send + !Sync` (one owner at a time — the pool's lease
//! invariant), so no locking is needed. A hit hands out the already-prepared
//! handle; on release the cursor is closed (`close_cursor`, the
//! reset-before-reuse discipline the seam trait declares) and the entry
//! returns to the LRU as most-recently-used.
//!
//! The C++ `StatementLease` RAII-releases through a raw pointer back into the
//! cache; safe Rust replaces that with a plain data token ([`StmtLease`]) that
//! holders hand back explicitly via `Connection::release_stmt`. The holder set
//! is closed — `Statement` and the facade's owned cursors — and each already
//! releases from its own `Drop`. A leaked lease degrades its entry to
//! permanently busy (later acquires of that SQL go transient); it never
//! double-frees or serves a stale handle.

use std::cell::Cell;
use std::collections::HashMap;

use crate::driver::{CliDriver, ConnectionHandle, StatementHandle};
use crate::error::Result;

/// One cached prepared statement.
struct Entry {
    /// Identity token carried by the lease (stable across LRU reordering).
    id: u64,
    handle: StatementHandle,
    /// Leased out right now. Busy entries are never evicted.
    busy: bool,
    /// Monotonic use stamp; the idle entry with the smallest stamp is the
    /// eviction victim (the C++ list-splice LRU, without the pointers).
    last_used: u64,
}

/// Borrow token for a prepared statement handle. Two modes, as in C++:
/// **cached** (`entry_id: Some`) — returned to the LRU on release — and
/// **transient** (`entry_id: None`) — finalized on release. Plain data with no
/// `Drop` of its own: holders release through `Connection::release_stmt`.
pub(crate) struct StmtLease {
    handle: StatementHandle,
    entry_id: Option<u64>,
    poisoned: Cell<bool>,
}

impl StmtLease {
    fn cached(handle: StatementHandle, id: u64) -> Self {
        StmtLease {
            handle,
            entry_id: Some(id),
            poisoned: Cell::new(false),
        }
    }

    fn transient(handle: StatementHandle) -> Self {
        StmtLease {
            handle,
            entry_id: None,
            poisoned: Cell::new(false),
        }
    }

    pub(crate) fn handle(&self) -> StatementHandle {
        self.handle
    }

    /// Marks the handle possibly corrupt (a driver-side error surfaced while
    /// leased). On release the entry is finalized and dropped, never reused.
    /// `&self` via `Cell` so shared-borrow holders (cursor rows) can poison.
    pub(crate) fn poison(&self) {
        self.poisoned.set(true);
    }
}

/// Fixed-capacity per-connection LRU keyed by the **final** SQL text (after
/// `:name` → `?` rewriting). Capacity 0 disables caching entirely.
pub(crate) struct StatementCache {
    capacity: usize,
    entries: HashMap<Box<str>, Entry>,
    tick: u64,
    next_id: u64,
}

impl StatementCache {
    pub(crate) fn new(capacity: usize) -> Self {
        StatementCache {
            capacity,
            entries: HashMap::new(),
            tick: 0,
            next_id: 0,
        }
    }

    /// Leases a prepared statement for `sql`: a cache hit reuses the entry's
    /// handle; a miss prepares (evicting the LRU idle entry at capacity). A
    /// busy hit — overlapping use of the same SQL — and a full cache of busy
    /// entries both fall back to a transient prepare (C++ "overflow").
    pub(crate) fn acquire(
        &mut self,
        driver: &dyn CliDriver,
        conn: ConnectionHandle,
        sql: &str,
    ) -> Result<StmtLease> {
        if self.capacity == 0 {
            return Ok(StmtLease::transient(driver.prepare(conn, sql)?));
        }
        if let Some(e) = self.entries.get_mut(sql) {
            if e.busy {
                return Ok(StmtLease::transient(driver.prepare(conn, sql)?));
            }
            e.busy = true;
            self.tick += 1;
            e.last_used = self.tick;
            return Ok(StmtLease::cached(e.handle, e.id));
        }
        let handle = driver.prepare(conn, sql)?;
        if self.entries.len() >= self.capacity {
            let victim = self
                .entries
                .iter()
                .filter(|(_, e)| !e.busy)
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone());
            match victim {
                Some(key) => {
                    let e = self.entries.remove(&key).expect("victim key just found");
                    let _ = driver.finalize(e.handle);
                }
                // Every entry is leased out: don't insert, serve transient.
                None => return Ok(StmtLease::transient(handle)),
            }
        }
        self.next_id += 1;
        self.tick += 1;
        self.entries.insert(
            sql.into(),
            Entry {
                id: self.next_id,
                handle,
                busy: true,
                last_used: self.tick,
            },
        );
        Ok(StmtLease::cached(handle, self.next_id))
    }

    /// Returns a lease. Transient leases are finalized. Cached leases have
    /// their cursor closed and re-enter the LRU as most-recently-used.
    /// (Poison and close-failure handling arrive in the next task.)
    pub(crate) fn release(&mut self, driver: &dyn CliDriver, lease: StmtLease) {
        let Some(id) = lease.entry_id else {
            let _ = driver.finalize(lease.handle);
            return;
        };
        let Some(key) = self
            .entries
            .iter()
            .find(|(_, e)| e.id == id)
            .map(|(k, _)| k.clone())
        else {
            // Unreachable by construction (busy entries are never evicted),
            // but never leak a handle if the invariant is ever broken.
            debug_assert!(false, "released a lease for an unknown cache entry");
            let _ = driver.finalize(lease.handle);
            return;
        };
        let _ = driver.close_cursor(lease.handle);
        let e = self.entries.get_mut(&key).expect("key just found");
        e.busy = false;
        self.tick += 1;
        e.last_used = self.tick;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::ConnectionParams;
    use crate::error::{Error, ErrorCode};
    use crate::testing::MockCliDriver;
    use std::sync::Arc;

    fn setup() -> (Arc<MockCliDriver>, ConnectionHandle) {
        let mock = Arc::new(MockCliDriver::new());
        let conn = mock
            .connect(&ConnectionParams {
                connection_string: "DSN=mock".into(),
            })
            .unwrap();
        (mock, conn)
    }

    #[test]
    fn miss_then_hit_prepares_once() {
        let (mock, conn) = setup();
        let mut cache = StatementCache::new(4);
        let l1 = cache.acquire(mock.as_ref(), conn, "SELECT 1").unwrap();
        cache.release(mock.as_ref(), l1);
        let l2 = cache.acquire(mock.as_ref(), conn, "SELECT 1").unwrap();
        cache.release(mock.as_ref(), l2);
        assert_eq!(mock.prepared_sql().len(), 1);
        assert_eq!(mock.finalize_calls(), 0); // both releases returned to the LRU
        assert_eq!(mock.close_cursor_calls(), 2); // reset-before-reuse each time
    }

    #[test]
    fn capacity_zero_is_always_transient() {
        let (mock, conn) = setup();
        let mut cache = StatementCache::new(0);
        for _ in 0..2 {
            let l = cache.acquire(mock.as_ref(), conn, "SELECT 1").unwrap();
            cache.release(mock.as_ref(), l);
        }
        assert_eq!(mock.prepared_sql().len(), 2);
        assert_eq!(mock.finalize_calls(), 2);
        assert_eq!(mock.close_cursor_calls(), 0); // transient: finalize, no reset
    }

    #[test]
    fn busy_hit_serves_transient_overflow() {
        let (mock, conn) = setup();
        let mut cache = StatementCache::new(4);
        let held = cache.acquire(mock.as_ref(), conn, "SELECT 1").unwrap();
        let overflow = cache.acquire(mock.as_ref(), conn, "SELECT 1").unwrap();
        assert_eq!(mock.prepared_sql().len(), 2); // overflow prepared afresh
        cache.release(mock.as_ref(), overflow);
        assert_eq!(mock.finalize_calls(), 1); // transient finalized...
        cache.release(mock.as_ref(), held);
        assert_eq!(mock.finalize_calls(), 1); // ...cached entry kept
    }

    #[test]
    fn evicts_least_recently_used_at_capacity() {
        let (mock, conn) = setup();
        let mut cache = StatementCache::new(2);
        for sql in ["A", "B"] {
            let l = cache.acquire(mock.as_ref(), conn, sql).unwrap();
            cache.release(mock.as_ref(), l);
        }
        // Touch A so B becomes the LRU victim.
        let l = cache.acquire(mock.as_ref(), conn, "A").unwrap();
        cache.release(mock.as_ref(), l);
        // C evicts B.
        let l = cache.acquire(mock.as_ref(), conn, "C").unwrap();
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.finalize_calls(), 1); // exactly one eviction
        assert_eq!(mock.prepared_sql(), vec!["A", "B", "C"]); // A's re-acquire was a hit
        // A still hits; B re-prepares.
        let l = cache.acquire(mock.as_ref(), conn, "A").unwrap();
        cache.release(mock.as_ref(), l);
        let before = mock.prepared_sql().len();
        let l = cache.acquire(mock.as_ref(), conn, "B").unwrap();
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.prepared_sql().len(), before + 1); // B was a miss again
    }

    #[test]
    fn all_entries_busy_at_capacity_serves_transient_without_insert() {
        let (mock, conn) = setup();
        let mut cache = StatementCache::new(1);
        let held = cache.acquire(mock.as_ref(), conn, "A").unwrap();
        let t = cache.acquire(mock.as_ref(), conn, "B").unwrap();
        cache.release(mock.as_ref(), t);
        assert_eq!(mock.finalize_calls(), 1); // B was transient, never inserted
        cache.release(mock.as_ref(), held);
        let l = cache.acquire(mock.as_ref(), conn, "B").unwrap(); // still a miss
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.prepared_sql(), vec!["A", "B", "B"]);
    }

    #[test]
    fn prepare_failure_on_miss_propagates_and_caches_nothing() {
        let (mock, conn) = setup();
        let mut cache = StatementCache::new(4);
        mock.push_prepare_error(Error::new(ErrorCode::Syntax, "42601"));
        assert!(cache.acquire(mock.as_ref(), conn, "SELEKT").is_err());
        let l = cache.acquire(mock.as_ref(), conn, "SELEKT").unwrap();
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.prepared_sql().len(), 2); // second acquire re-prepared
    }
}
```

- [ ] **Step 2: Register the module.** In `halcyon/src/core/mod.rs`, after `mod result_set;`:

```rust
mod stmt_cache;
```

and after the `pub(crate) use transaction::terminate;` line:

```rust
// The cache and its lease token cross into `connection.rs` and the facade's
// owned cursors, which live outside this module tree.
pub(crate) use stmt_cache::{StatementCache, StmtLease};
```

- [ ] **Step 3: Run the cache tests**

Run: `cargo test -p halcyon --no-default-features stmt_cache`
Expected: 6 PASS. (If the `pub(crate) use` triggers an unused-import warning at this point, that's expected — `connection.rs` consumes it in Task 4; silence nothing, it's a warning not an error. If the project's CI-lint discipline bothers you, add `#[allow(unused_imports)]` temporarily and remove it in Task 4 — but prefer leaving the warning for one task.)

- [ ] **Step 4: Run the full suite**

Run: `cargo test -p halcyon --no-default-features`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/core/stmt_cache.rs halcyon/src/core/mod.rs
git commit -m "feat(core): StatementCache LRU with lease tokens (acquire/release core)"
```

---

### Task 3: Poison, close-failure, and teardown semantics

**Files:**
- Modify: `halcyon/src/core/stmt_cache.rs`

- [ ] **Step 1: Write the failing tests** (append to `mod tests`)

```rust
    #[test]
    fn poison_drops_entry_and_next_acquire_reprepares() {
        let (mock, conn) = setup();
        let mut cache = StatementCache::new(4);
        let l = cache.acquire(mock.as_ref(), conn, "SELECT 1").unwrap();
        l.poison();
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.finalize_calls(), 1);
        assert_eq!(mock.close_cursor_calls(), 0); // poisoned: dropped, not reset
        let l = cache.acquire(mock.as_ref(), conn, "SELECT 1").unwrap();
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.prepared_sql().len(), 2); // re-prepared after the drop
    }

    #[test]
    fn close_cursor_failure_drops_entry() {
        let (mock, conn) = setup();
        let mut cache = StatementCache::new(4);
        let l = cache.acquire(mock.as_ref(), conn, "SELECT 1").unwrap();
        mock.push_close_cursor_error(Error::new(ErrorCode::Connection, "reset failed"));
        cache.release(mock.as_ref(), l); // error swallowed...
        assert_eq!(mock.finalize_calls(), 1); // ...entry finalized + dropped
        let l = cache.acquire(mock.as_ref(), conn, "SELECT 1").unwrap();
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.prepared_sql().len(), 2); // never re-serves a bad handle
    }

    #[test]
    fn clear_finalizes_live_entries() {
        let (mock, conn) = setup();
        let mut cache = StatementCache::new(4);
        for sql in ["A", "B"] {
            let l = cache.acquire(mock.as_ref(), conn, sql).unwrap();
            cache.release(mock.as_ref(), l);
        }
        cache.clear(mock.as_ref());
        assert_eq!(mock.finalize_calls(), 2);
        // The cache is reusable-but-empty afterwards.
        let l = cache.acquire(mock.as_ref(), conn, "A").unwrap();
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.prepared_sql().len(), 3);
    }
```

- [ ] **Step 2: Run to verify failures**

Run: `cargo test -p halcyon --no-default-features stmt_cache`
Expected: `clear_finalizes_live_entries` FAILS to compile (`no method named 'clear'`); the poison test fails on `close_cursor_calls == 0` (current release always resets); the close-failure test fails on `finalize_calls == 1`.

- [ ] **Step 3: Implement.** Replace the body of `release` from the `if let`/close_cursor line down, and add `clear`:

```rust
        if lease.poisoned.get() {
            let e = self.entries.remove(&key).expect("key just found");
            let _ = driver.finalize(e.handle);
            return;
        }
        // Best-effort cursor reset. If `close_cursor` fails the handle may be
        // only half-reset; swallow the error but drop the entry so a bad
        // handle is never served to a later acquire (C++ spec §7).
        if driver.close_cursor(lease.handle).is_err() {
            let e = self.entries.remove(&key).expect("key just found");
            let _ = driver.finalize(e.handle);
            return;
        }
        let e = self.entries.get_mut(&key).expect("key just found");
        e.busy = false;
        self.tick += 1;
        e.last_used = self.tick;
    }

    /// Finalizes every cached handle. `Connection::drop` calls this **before**
    /// disconnecting — statement handles are invalid once their connection is
    /// gone (the C++ destructor ordering).
    pub(crate) fn clear(&mut self, driver: &dyn CliDriver) {
        for (_, e) in self.entries.drain() {
            debug_assert!(!e.busy, "statement cache cleared while a lease is outstanding");
            let _ = driver.finalize(e.handle);
        }
    }
```

- [ ] **Step 4: Run cache tests, then full suite**

Run: `cargo test -p halcyon --no-default-features stmt_cache` → 9 PASS.
Run: `cargo test -p halcyon --no-default-features` → all PASS.

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/core/stmt_cache.rs
git commit -m "feat(core): poison, close-failure, and clear semantics for the statement cache"
```

---

### Task 4: Route every statement path through the cache (capacity 0 — zero behavior change)

The pure-plumbing refactor: `Connection` owns a cache (capacity 0 for now, so every lease is transient and every existing test's driver-call pattern is unchanged), `Statement` and the facade `QueryResult` carry leases instead of finalizing raw handles. **The existing test suite is the regression gate for this task.**

**Files:**
- Modify: `halcyon/src/core/connection.rs`
- Modify: `halcyon/src/facade.rs`

- [ ] **Step 1: Rework `Connection`.** In `connection.rs`:

Imports — add `RefCell`:

```rust
use std::cell::{Cell, RefCell};
```

(and remove the now-covered `use std::cell::Cell;` line), plus:

```rust
use crate::core::{StatementCache, StmtLease};
```

Struct — add the cache field:

```rust
pub struct Connection {
    driver: Arc<dyn CliDriver>,
    handle: ConnectionHandle,
    /// Per-connection prepared-statement LRU (Phase 5). `RefCell`, not a lock:
    /// `Connection` is `Send + !Sync`, one owner at a time.
    cache: RefCell<StatementCache>,
    // `Cell<()>` is `Send + !Sync`, which propagates to `Connection` without a
    // runtime cost or any borrowed data. It carries no value.
    _not_sync: PhantomData<Cell<()>>,
}
```

Constructors — `open` delegates; `open_with_cache` is new public API:

```rust
    /// Opens a connection through `driver` with statement caching disabled.
    pub fn open(driver: Arc<dyn CliDriver>, params: &ConnectionParams) -> Result<Self> {
        Self::open_with_cache(driver, params, 0)
    }

    /// Opens a connection with a per-connection prepared-statement LRU of
    /// `cache_capacity` entries (0 disables caching — every statement is
    /// prepared and finalized per call). The pool passes
    /// `PoolConfig::statement_cache_size` here.
    pub fn open_with_cache(
        driver: Arc<dyn CliDriver>,
        params: &ConnectionParams,
        cache_capacity: usize,
    ) -> Result<Self> {
        let handle = driver.connect(params)?;
        Ok(Connection {
            driver,
            handle,
            cache: RefCell::new(StatementCache::new(cache_capacity)),
            _not_sync: PhantomData,
        })
    }
```

`prepare` acquires from the cache:

```rust
    /// Prepares `sql` (or leases it from the statement cache), returning a
    /// reusable [`Statement`].
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        let lease = self
            .cache
            .borrow_mut()
            .acquire(self.driver.as_ref(), self.handle, sql)?;
        Ok(Statement {
            conn: self,
            handle: lease.handle(),
            lease: Some(lease),
        })
    }
```

`execute_batch` — only the handle access changes:

```rust
        let stmt = self.prepare(sql)?;
        self.driver.execute_batch(stmt.handle(), rows)
```

`prepare_and_bind` / `prepare_and_bind_raw` — the raw path now returns a lease and poisons-then-releases on bind failure (identical observable behavior at capacity 0: poisoned transient ⇒ finalize, exactly what the old code did):

```rust
    fn prepare_and_bind(&self, sql: &str, params: Params) -> Result<Statement<'_>> {
        let lease = self.prepare_and_bind_raw(sql, params)?;
        Ok(Statement {
            conn: self,
            handle: lease.handle(),
            lease: Some(lease),
        })
    }

    /// Prepares and binds `sql` (rewriting named params to positional),
    /// returning the statement **lease** the caller must hand back via
    /// [`release_stmt`](Self::release_stmt) (RAII holders do this from `Drop`).
    /// The cache key is the final, rewritten SQL. On a bind failure the lease
    /// is poisoned and released here.
    fn prepare_and_bind_raw(&self, sql: &str, params: Params) -> Result<StmtLease> {
        let (sql, values) = match params {
            Params::Positional(values) => (Cow::Borrowed(sql), values),
            Params::Named(pairs) => {
                let pre = bind_named(sql, &pairs)?;
                (Cow::Owned(pre.sql), pre.params)
            }
        };
        let lease = self
            .cache
            .borrow_mut()
            .acquire(self.driver.as_ref(), self.handle, &sql)?;
        if let Err(e) = self.driver.bind_params(lease.handle(), &values) {
            lease.poison();
            self.release_stmt(lease);
            return Err(e);
        }
        Ok(lease)
    }
```

`execute_query_raw` — returns the lease; poisons + releases on failure:

```rust
    /// Prepares, binds, and executes `sql`, returning the executed statement's
    /// lease and its column count — transferring release responsibility to the
    /// caller. On any failure the lease is poisoned and released here. The
    /// facade's pooled [`QueryResult`](crate::QueryResult) holds the lease for
    /// the cursor's life, so it cannot borrow the leased [`Connection`] the
    /// way [`ResultSet`] does.
    pub(crate) fn execute_query_raw(
        &self,
        sql: &str,
        params: Params,
    ) -> Result<(StmtLease, usize)> {
        let lease = self.prepare_and_bind_raw(sql, params)?;
        match self
            .driver
            .execute(lease.handle())
            .and_then(|_| self.driver.column_count(lease.handle()))
        {
            Ok(columns) => Ok((lease, columns)),
            Err(e) => {
                lease.poison();
                self.release_stmt(lease);
                Err(e)
            }
        }
    }

    /// Hands a statement lease back to this connection's cache: transient
    /// leases are finalized, cached ones reset and returned to the LRU (or
    /// dropped if poisoned). The release half of every prepare path above.
    pub(crate) fn release_stmt(&self, lease: StmtLease) {
        self.cache
            .borrow_mut()
            .release(self.driver.as_ref(), lease);
    }
```

`Drop for Connection` — clear before disconnect:

```rust
impl Drop for Connection {
    fn drop(&mut self) {
        // Cached statement handles must be finalized BEFORE disconnecting:
        // they are invalid once their connection is gone (the C++ destructor
        // ordering, `cache_.reset()` before the handle drop).
        self.cache.borrow_mut().clear(self.driver.as_ref());
        if self.handle.is_valid() {
            let _ = self.driver.disconnect(self.handle);
        }
    }
}
```

`Statement` — lease holder (keeps a copied `handle` so `handle()` stays infallible):

```rust
/// A prepared, reusable statement leased from its connection's statement
/// cache (or transient when caching is off/overflowed). Releases the lease on
/// drop. Borrows the owning [`Connection`] so it cannot outlive it.
pub struct Statement<'c> {
    conn: &'c Connection,
    handle: StatementHandle,
    lease: Option<StmtLease>,
}
```

Add a `poison` method to `impl<'c> Statement<'c>` (used by error paths in Task 5 and by `ResultSet::drop`):

```rust
    /// Marks the leased statement possibly corrupt (driver-side failure while
    /// in use), so release drops the cache entry instead of reusing it.
    pub(crate) fn poison(&self) {
        if let Some(lease) = &self.lease {
            lease.poison();
        }
    }
```

`Drop for Statement`:

```rust
impl Drop for Statement<'_> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.conn.release_stmt(lease);
        }
    }
}
```

(`Statement::handle`, `driver_ref`, `execute`, `into_query` bodies are untouched in this task.)

- [ ] **Step 2: Rework the facade `QueryResult`.** In `facade.rs`:

Import the lease type (with the other `crate::core` imports; the file currently pulls core items via `crate::...` paths — match the existing import style at the top of the file):

```rust
use crate::core::StmtLease;
```

Struct + constructor:

```rust
pub struct QueryResult {
    lease: PooledConnection,
    /// The statement lease, released back to the connection's cache on drop.
    /// `Option` so `Drop` can move it out.
    stmt_lease: Option<StmtLease>,
    /// Copied handle for fetch/read calls (infallible access).
    stmt: StatementHandle,
    columns: usize,
    // The cursor's poison slot: a driver-side read failure is recorded here (as
    // in `ResultSet`), and it decides whether the lease is discarded on drop.
    error: RefCell<Option<Error>>,
    done: bool,
}

impl QueryResult {
    fn new(lease: PooledConnection, stmt_lease: StmtLease, columns: usize) -> Self {
        let stmt = stmt_lease.handle();
        QueryResult {
            lease,
            stmt_lease: Some(stmt_lease),
            stmt,
            columns,
            error: RefCell::new(None),
            done: false,
        }
    }
```

Call site in `query_with_policy`:

```rust
                Ok((stmt_lease, columns)) => {
                    return Ok(QueryResult::new(lease, stmt_lease, columns))
                }
```

`Drop for QueryResult` — release instead of finalize (poisoning arrives in Task 5; keep behavior identical for now):

```rust
impl Drop for QueryResult {
    fn drop(&mut self) {
        // Release the statement back to the leased connection's cache, then
        // decide the lease's fate: a mid-stream connection-class failure means
        // the connection is dead, so discard it instead of pooling it.
        if let Some(stmt_lease) = self.stmt_lease.take() {
            self.lease.release_stmt(stmt_lease);
        }
        let broken = self
            .error
            .borrow()
            .as_ref()
            .is_some_and(|e| e.code == ErrorCode::Connection);
        if broken {
            self.lease.mark_broken();
        }
        // `lease` drops after this → returns or discards the connection.
    }
}
```

(`self.lease.release_stmt(...)` resolves through `PooledConnection`'s `Deref<Target = Connection>`; `release_stmt` is `pub(crate)` so the facade can call it.)

- [ ] **Step 3: Run the full suite — this is the regression gate**

Run: `cargo test -p halcyon --no-default-features`
Expected: **all PASS with no test edits.** Every connection still opens at capacity 0, so every lease is transient and finalize/prepare call counts are byte-identical to before. If any existing test fails, the refactor changed behavior — stop and fix before proceeding.

Also run: `cargo test -p halcyon --no-default-features --features derive`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add halcyon/src/core/connection.rs halcyon/src/facade.rs
git commit -m "refactor(core): route statement paths through cache leases (capacity 0, no behavior change)"
```

---

### Task 5: Enable caching on `Connection` + poison-on-driver-error wiring

**Files:**
- Modify: `halcyon/src/core/connection.rs`
- Modify: `halcyon/src/core/result_set.rs`
- Modify: `halcyon/src/facade.rs` (QueryResult drop poison)

- [ ] **Step 1: Write the failing tests** (append to `mod tests` in `connection.rs`)

```rust
    /// Builds a caching connection over the mock.
    fn connect_cached(mock: &Arc<MockCliDriver>, capacity: usize) -> Connection {
        Connection::open_with_cache(mock.clone(), &params(), capacity).unwrap()
    }

    #[test]
    fn cached_connection_reuses_prepared_statement_across_queries() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        mock.push_result_set(users_grid());
        let conn = connect_cached(&mock, 8);
        for _ in 0..2 {
            let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
            let _rows: Vec<(i64, String)> = rs.collect_rows().unwrap();
        }
        assert_eq!(mock.prepared_sql().len(), 1); // second query was a hit
        assert_eq!(mock.finalize_calls(), 0); // entry still cached
        assert_eq!(mock.close_cursor_calls(), 2); // reset on each release
    }

    #[test]
    fn named_params_cache_on_the_rewritten_sql() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        mock.push_result_set(users_grid());
        let conn = connect_cached(&mock, 8);
        for _ in 0..2 {
            let mut rs = conn
                .query(
                    "SELECT id, name FROM u WHERE age > :age",
                    named! { "age" => 21 },
                )
                .unwrap();
            let _rows: Vec<(i64, String)> = rs.collect_rows().unwrap();
        }
        // One prepare, keyed by the rewritten (positional) SQL.
        assert_eq!(
            mock.prepared_sql(),
            vec!["SELECT id, name FROM u WHERE age > ?"]
        );
    }

    #[test]
    fn execute_error_poisons_cached_statement_so_next_use_reprepares() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect_cached(&mock, 8);
        conn.execute("UPDATE t SET x = ?", (1,)).unwrap(); // miss: prepare #1
        mock.push_execute_error(Error::new(ErrorCode::Connection, "broken"));
        conn.execute("UPDATE t SET x = ?", (2,)).unwrap_err(); // hit, exec fails
        assert_eq!(mock.finalize_calls(), 1); // poisoned entry dropped
        conn.execute("UPDATE t SET x = ?", (3,)).unwrap(); // miss again
        assert_eq!(mock.prepared_sql().len(), 2);
    }

    #[test]
    fn fetch_error_drops_cached_statement_via_cursor_drop() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(users_grid());
        mock.push_result_set(users_grid());
        mock.fail_fetch_on_call(1, Error::new(ErrorCode::Connection, "boom"));
        let conn = connect_cached(&mock, 8);
        {
            let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
            let _ = rs.next().unwrap_err(); // driver-side scan failure
        } // ResultSet drop → Statement poisoned → entry finalized + dropped
        assert_eq!(mock.finalize_calls(), 1);
        let mut rs = conn.query("SELECT id, name FROM u", ()).unwrap();
        let _rows: Vec<(i64, String)> = rs.collect_rows().unwrap();
        assert_eq!(mock.prepared_sql().len(), 2); // re-prepared after the drop
    }

    #[test]
    fn client_side_mapping_error_does_not_drop_cached_statement() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(["n"], vec![vec![Value::Int(7)]]));
        mock.push_result_set(ScriptedRows::new(["n"], vec![vec![Value::Int(7)]]));
        let conn = connect_cached(&mock, 8);
        {
            let mut rs = conn.query("SELECT n FROM u", ()).unwrap();
            let row = rs.next().unwrap().unwrap();
            assert!(row.column::<String>(0).is_err()); // Int as String: mapping
        }
        let mut rs = conn.query("SELECT n FROM u", ()).unwrap();
        let _rows: Vec<(i64,)> = rs.collect_rows().unwrap();
        assert_eq!(mock.prepared_sql().len(), 1); // entry survived: still a hit
        assert_eq!(mock.finalize_calls(), 0);
    }

    #[test]
    fn overlapping_same_sql_statements_use_transient_overflow() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect_cached(&mock, 8);
        let s1 = conn.prepare("SELECT 1").unwrap();
        let s2 = conn.prepare("SELECT 1").unwrap(); // busy hit → transient
        assert_eq!(mock.prepared_sql().len(), 2);
        drop(s2);
        assert_eq!(mock.finalize_calls(), 1); // transient finalized
        drop(s1);
        assert_eq!(mock.finalize_calls(), 1); // cached entry kept
    }

    #[test]
    fn drop_finalizes_cached_entries_before_disconnect() {
        let mock = Arc::new(MockCliDriver::new());
        {
            let conn = connect_cached(&mock, 8);
            conn.execute("UPDATE t SET x = 1", ()).unwrap(); // one cached entry
        }
        // Cache cleared (finalize) strictly before disconnect.
        assert_eq!(mock.events(), vec!["finalize", "disconnect"]);
    }
```

- [ ] **Step 2: Run to verify failures**

Run: `cargo test -p halcyon --no-default-features -- cached_connection execute_error_poisons fetch_error_drops client_side_mapping overlapping_same_sql drop_finalizes named_params_cache`
Expected: `execute_error_poisons…` FAILS (execute error path doesn't poison yet → entry reused → 1 prepare) and `fetch_error_drops…` FAILS (no `Drop for ResultSet` yet). The pure-reuse tests already pass (Task 2/4 machinery).

- [ ] **Step 3: Wire poison into the error paths.**

`connection.rs` — `Connection::execute`:

```rust
    pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<i64> {
        let stmt = self.prepare_and_bind(sql, params.into_params())?;
        let result = self.driver.execute(stmt.handle());
        if result.is_err() {
            stmt.poison();
        }
        result
    }
```

`Connection::query`:

```rust
    pub fn query(&self, sql: &str, params: impl IntoParams) -> Result<ResultSet<'_>> {
        let stmt = self.prepare_and_bind(sql, params.into_params())?;
        let columns = match self
            .driver
            .execute(stmt.handle())
            .and_then(|_| self.driver.column_count(stmt.handle()))
        {
            Ok(columns) => columns,
            Err(e) => {
                stmt.poison();
                return Err(e);
            }
        };
        Ok(ResultSet::from_executed(stmt, columns))
    }
```

`Connection::execute_batch` (the tail):

```rust
        let stmt = self.prepare(sql)?;
        let result = self.driver.execute_batch(stmt.handle(), rows);
        if result.is_err() {
            stmt.poison();
        }
        result
```

`Statement::execute`:

```rust
    pub fn execute(&mut self, params: impl IntoParams) -> Result<i64> {
        let values = positional_only(params.into_params())?;
        let result = self
            .conn
            .driver_ref()
            .bind_params(self.handle, &values)
            .and_then(|_| self.conn.driver_ref().execute(self.handle));
        if result.is_err() {
            self.poison();
        }
        result
    }
```

`Statement::into_query`:

```rust
    pub fn into_query(self, params: impl IntoParams) -> Result<ResultSet<'c>> {
        let values = positional_only(params.into_params())?;
        let columns = match self
            .conn
            .driver_ref()
            .bind_params(self.handle, &values)
            .and_then(|_| self.conn.driver_ref().execute(self.handle))
            .and_then(|_| self.conn.driver_ref().column_count(self.handle))
        {
            Ok(columns) => columns,
            Err(e) => {
                self.poison();
                return Err(e);
            }
        };
        Ok(ResultSet::from_executed(self, columns))
    }
```

`result_set.rs` — add after the `impl<'c> ResultSet<'c>` block:

```rust
impl Drop for ResultSet<'_> {
    fn drop(&mut self) {
        // A driver-side scan failure (fetch or column read) may leave the
        // handle mid-cursor in a bad state: poison the statement so a cached
        // entry is finalized and dropped on release, not reused (C++ spec §5).
        // Client-side mapping errors are never recorded here, so they don't
        // poison.
        if self.error.borrow().is_some() {
            self.stmt.poison();
        }
    }
}
```

`facade.rs` — `Drop for QueryResult`, poison before release (replace the `if let Some(stmt_lease)` block from Task 4):

```rust
        if let Some(stmt_lease) = self.stmt_lease.take() {
            // Any driver-side scan failure poisons the statement so its cache
            // entry is dropped; a connection-class one additionally discards
            // the whole connection below.
            if self.error.borrow().is_some() {
                stmt_lease.poison();
            }
            self.lease.release_stmt(stmt_lease);
        }
```

- [ ] **Step 4: Run the new tests, then the full suite**

Run: `cargo test -p halcyon --no-default-features` (and `--features derive`)
Expected: all PASS, including the 8 new tests. The pre-existing suite must stay green: at capacity 0 a poisoned transient lease still just finalizes.

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/core/connection.rs halcyon/src/core/result_set.rs halcyon/src/facade.rs
git commit -m "feat(core): per-connection statement caching via open_with_cache + poison-on-driver-error"
```

---

### Task 6: `PoolConfig.statement_cache_size` — pool, facade, and stress plumbing

**Files:**
- Modify: `halcyon/src/pool.rs`
- Modify: `halcyon/src/facade.rs`
- Modify: `halcyon/src/stress/scenarios.rs`

- [ ] **Step 1: Write the failing tests.**

In `pool.rs` `mod tests` (the std one, not the loom one):

```rust
    #[test]
    fn default_config_enables_the_statement_cache() {
        assert_eq!(PoolConfig::default().statement_cache_size, 64);
    }

    #[test]
    fn pooled_connection_keeps_cached_statements_across_lease_cycles() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 1);
        config.statement_cache_size = 8;
        let p = pool(&mock, config);
        for _ in 0..2 {
            let lease = p.acquire().unwrap();
            lease.execute("UPDATE t SET x = 1", ()).unwrap();
        }
        // The prepared statement survived the release/re-acquire cycle.
        assert_eq!(mock.prepared_sql().len(), 1);
    }
```

In `facade.rs` `mod tests`:

```rust
    /// A single-connection pool with the statement cache on, so cache hits can
    /// be asserted across pooled calls (every lease is the same connection).
    fn cached_config() -> PoolConfig {
        PoolConfig {
            min: 1,
            max: 1,
            statement_cache_size: 8,
            ..config()
        }
    }

    #[test]
    fn pooled_facade_reuses_cached_statement_across_leases() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(7)]]));
        mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(7)]]));
        let db = Database::open(mock.clone(), params(), cached_config()).unwrap();
        for _ in 0..2 {
            let rows: Vec<(i64,)> = db.query_as("SELECT id FROM t", ()).unwrap();
            assert_eq!(rows, vec![(7,)]);
        }
        assert_eq!(mock.prepared_sql().len(), 1); // second call hit the cache
        assert_eq!(mock.close_cursor_calls(), 2); // QueryResult released cleanly
    }

    #[test]
    fn read_retry_after_transient_error_reprepares_the_poisoned_statement() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_execute_error(retriable(ErrorCode::Deadlock, "40001"));
        mock.push_result_set(ScriptedRows::new(["id"], vec![vec![Value::Int(7)]]));
        let db = Database::open(mock.clone(), params(), cached_config()).unwrap();
        let rows: Vec<(i64,)> = db.query_as("SELECT id FROM t", ()).unwrap();
        assert_eq!(rows, vec![(7,)]);
        // Attempt 1 prepared + failed (entry poisoned and dropped); the replay
        // re-acquired the same connection and re-prepared.
        assert_eq!(mock.prepared_sql().len(), 2);
        assert_eq!(mock.finalize_calls(), 1); // exactly the poisoned statement
    }
```

- [ ] **Step 2: Run to verify failures**

Run: `cargo test -p halcyon --no-default-features default_config_enables`
Expected: FAIL to compile — `struct PoolConfig has no field named statement_cache_size` (the new tests and `cached_config` all hit it).

- [ ] **Step 3: Implement.** In `pool.rs`:

Field (after `start_maintenance_thread` in `PoolConfig`):

```rust
    /// Per-connection prepared-statement LRU capacity; 0 disables caching.
    /// Every pooled connection (including transparent reconnects) gets its own
    /// cache of this size, cleared when the physical connection is dropped.
    pub statement_cache_size: usize,
```

`Default` impl: add `statement_cache_size: 64,`.

Also update the `PoolConfig` rustdoc paragraph (currently says "the observability and statement-cache fields land in later phases") to say only the observability fields remain for a later phase.

`make_connection` — the single seam where physical connections are born (initial warm-up, growth, reconnect, refill all funnel here):

```rust
            match Connection::open_with_cache(
                Arc::clone(&self.driver),
                &self.params,
                self.config.statement_cache_size,
            ) {
```

Test config literals (exhaustive struct literals — the compiler will point at each):
- `pool.rs` std-tests `fn cfg(min, max)`: add `statement_cache_size: 0,` (keeps every existing pool test's driver-call counts unchanged).
- `pool.rs` loom-tests `fn cfg(max)`: add `statement_cache_size: 0,` (the loom models exercise the handoff, not statements).
- `facade.rs` `fn config()`: add `statement_cache_size: 0,` (existing facade tests keep exact call patterns; the new cache tests opt in via `cached_config`).
- `halcyon/src/stress/scenarios.rs` `config_for`: add `statement_cache_size: 64,` — the stress scenarios (and therefore the TSan job) now exercise the cache path with the production default.
- `halcyon/benches/pool_bench.rs` `fn db()`: add `statement_cache_size: 64,` for now (Task 7 parametrizes it).

- [ ] **Step 4: Run everything, including stress compile**

Run: `cargo test -p halcyon --no-default-features`
Expected: all PASS including the 4 new tests.
Run: `cargo test -p halcyon --no-default-features --features stress`
Expected: all PASS (stress scenarios still deterministic — the cache is per-connection, single-owner, no new cross-thread state).

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/pool.rs halcyon/src/facade.rs halcyon/src/stress/scenarios.rs halcyon/benches/pool_bench.rs
git commit -m "feat(pool): statement_cache_size in PoolConfig (default 64), wired through every connect path"
```

---

### Task 7: Cached-vs-uncached criterion benchmark

**Files:**
- Modify: `halcyon/benches/pool_bench.rs`

- [ ] **Step 1: Parametrize the bench harness.** Change `fn db()` to take the cache size, and split the scalar-query bench:

```rust
fn db(statement_cache_size: usize) -> Database {
    let cfg = PoolConfig {
        min: 2,
        max: 4,
        acquire_timeout: Duration::from_millis(1000),
        idle_timeout: Duration::from_secs(3600),
        max_lifetime: Duration::from_secs(3600),
        validate_on_acquire: false,
        backoff: BackoffPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            max_attempts: 1,
        },
        maintenance_interval: Duration::from_secs(3600),
        start_maintenance_thread: false,
        statement_cache_size,
    };
    Database::open(
        Arc::new(ConcurrentFakeDriver::new()),
        ConnectionParams {
            connection_string: "dsn".into(),
        },
        cfg,
    )
    .unwrap()
}

fn bench_acquire_release(c: &mut Criterion) {
    let db = db(64);
    c.bench_function("pool_acquire_release", |b| {
        b.iter(|| {
            let lease = db.pool().acquire().unwrap();
            std::hint::black_box(&lease);
        });
    });
}

/// The Phase 5 perf evidence: the same scalar query with the per-connection
/// statement cache on (default 64) vs. off (0). The cached variant should
/// show the per-call prepare/finalize round-trip removed.
fn bench_scalar_query(c: &mut Criterion) {
    let cached = db(64);
    c.bench_function("query_scalar_cached", |b| {
        b.iter(|| {
            let rows: Vec<(i64,)> = cached
                .query_as("SELECT 7 FROM SYSIBM.SYSDUMMY1", ())
                .unwrap();
            std::hint::black_box(rows);
        });
    });
    let uncached = db(0);
    c.bench_function("query_scalar_uncached", |b| {
        b.iter(|| {
            let rows: Vec<(i64,)> = uncached
                .query_as("SELECT 7 FROM SYSIBM.SYSDUMMY1", ())
                .unwrap();
            std::hint::black_box(rows);
        });
    });
}
```

- [ ] **Step 2: Compile-check and smoke-run the benches**

Run: `cargo bench -p halcyon --no-default-features --features stress -- --test`
Expected: both `query_scalar_cached` and `query_scalar_uncached` run in test mode (single iteration, no timing) and PASS.

- [ ] **Step 3 (optional but recommended): Record the numbers**

Run: `cargo bench -p halcyon --no-default-features --features stress query_scalar`
Expected: both benches complete; note the cached-vs-uncached delta in the commit message body (the fake driver's prepare is cheap, so expect a modest but visible gap — the live `batch_bench`/round-trip is the real-Db2 evidence).

- [ ] **Step 4: Commit**

```bash
git add halcyon/benches/pool_bench.rs
git commit -m "bench(pool): cached vs uncached scalar-query comparison"
```

---

### Task 8: Live Db2 round-trip — cached re-execution correctness

**Files:**
- Modify: `halcyon/tests/db2_roundtrip.rs`

- [ ] **Step 1: Add the DSN-gated test** (alongside `live_facade_round_trip`; it follows that test's structure and helpers — `test_dsn()`, `Database::connect`):

```rust
/// Phase 5 exit criterion: repeated statements through the pooled facade (the
/// default config caches 64 per connection) must reuse prepared handles and
/// still return correct results on real Db2 — including a parameterized SELECT
/// re-executed with different bindings and a DML re-executed after a cursor.
#[test]
fn live_statement_cache_reuse() {
    let Some(dsn) = test_dsn() else {
        eprintln!("HALCYON_TEST_DSN not set; skipping live statement-cache round-trip");
        return;
    };
    let db = Database::connect(&dsn).expect("open database");

    let _ = db.execute("DROP TABLE halcyon_stmt_cache", ());
    db.execute(
        "CREATE TABLE halcyon_stmt_cache(id INT NOT NULL, name VARCHAR(64))",
        (),
    )
    .expect("create table");

    // Re-execute the same INSERT through the cache with different bindings.
    for (id, name) in [(1, "ada"), (2, "bob"), (3, "cy")] {
        db.execute("INSERT INTO halcyon_stmt_cache VALUES (?, ?)", (id, name))
            .expect("cached insert");
    }

    // Re-execute the same parameterized SELECT; each run opens and closes a
    // cursor on the cached handle (close_cursor reset-before-reuse on live Db2).
    for lo in 1..=3 {
        let got: Vec<(i64, String)> = db
            .query_as(
                "SELECT id, name FROM halcyon_stmt_cache WHERE id >= ? ORDER BY id",
                (lo,),
            )
            .expect("cached select");
        assert_eq!(got.len() as i64, 4 - lo, "wrong row count for lo={lo}");
        assert_eq!(got[0].0, lo, "wrong first row for lo={lo}");
    }

    // DML re-execution after cursors: the cached INSERT handle still works.
    db.execute("INSERT INTO halcyon_stmt_cache VALUES (?, ?)", (4, "di"))
        .expect("insert after cursor reuse");
    let count: Vec<(i64,)> = db
        .query_as("SELECT COUNT(*) FROM halcyon_stmt_cache", ())
        .expect("count");
    assert_eq!(count[0].0, 4);

    db.execute("DROP TABLE halcyon_stmt_cache", ())
        .expect("drop table");
}
```

- [ ] **Step 2: Verify it compiles and no-ops without a DSN**

Run: `cargo test -p halcyon --no-default-features --test db2_roundtrip 2>/dev/null || cargo build -p halcyon --tests`
Note: the round-trip test file needs the `db2` feature to build the real driver — if a clidriver is available locally, run `cargo test --test db2_roundtrip` (unset `HALCYON_TEST_DSN` → each live test prints its skip line and passes). If no clidriver is available, `cargo check -p halcyon --tests` with default features must at least type-check via CI.
Expected: the test is a no-op pass without `HALCYON_TEST_DSN`.

- [ ] **Step 3: Live validation (the actual phase gate).** On this machine, live Db2 runs in an emulated amd64 Linux container on the Db2 docker network (the macOS host cannot complete the DRDA handshake — see `docker/docker-compose.yml` and the README's live-test section). Either run it there, or rely on CI's `live-db2` job once pushed. Record which one accepted the phase in the final task's status update.

- [ ] **Step 4: Commit**

```bash
git add halcyon/tests/db2_roundtrip.rs
git commit -m "test(live): DSN-gated statement-cache reuse round-trip"
```

---

### Task 9: Docs, lints, and final gates

**Files:**
- Modify: `README.md`
- Modify: `halcyon/src/lib.rs`
- Modify: `halcyon/src/core/connection.rs` (module doc)

- [ ] **Step 1: Update the docs.**

`README.md` status paragraph (line ~5): replace the last two sentences ("The pool's concurrency-correctness track … is next.") with:

```markdown
The pool's concurrency-correctness track — `loom` model-checking of the lock/condvar handoff, a `ConcurrentFakeDriver` stress harness under ThreadSanitizer, and criterion + throughput perf harnesses — is in place (Phase 4 accepted), and each pooled connection now carries a per-connection prepared-statement LRU (`PoolConfig::statement_cache_size`, default 64; Phase 5). The `tokio` async facade (Phase 6) is next. See the roadmap below.
```

`README.md` roadmap: change the statement-cache line to:

```markdown
- [x] Statement cache (per-connection LRU): `StatementCache` + lease tokens inside `Connection` (`open_with_cache`), keyed by rewritten SQL with `close_cursor` reset-before-reuse, busy-hit transient overflow, poison/close-failure entry drop, and clear-before-disconnect; `PoolConfig.statement_cache_size` (default 64, 0 disables) applies it to every pooled connection — covered by the ported C++ cache suite on mock and a live-Db2 reuse round-trip
```

`halcyon/src/lib.rs` status block (lines ~13-16): replace "…is in place, so Phase 4 is accepted; the per-connection statement cache (Phase 5) is next." with:

```text
stress harness and perf benchmarks — is in place (Phase 4), and every pooled
connection carries a per-connection prepared-statement LRU
(`PoolConfig::statement_cache_size`, default 64; Phase 5). The `tokio` async
facade (Phase 6) is next. See the README roadmap:
```

`halcyon/src/core/connection.rs` module doc (lines ~10-11): replace "The per-connection statement cache (C++ `StatementCache`) is Phase 5; here every `query`/`execute` prepares a fresh statement." with:

```rust
//! Statements are leased from the per-connection [`StatementCache`] (C++
//! `StatementCache`, Phase 5): with a nonzero capacity, repeated SQL reuses
//! its prepared handle; at capacity 0 (the plain [`Connection::open`] default)
//! every lease is transient — prepared and finalized per call.
```

(Use a plain-text `StatementCache` reference or intra-doc link to the crate-private path only if rustdoc resolves it; `pub(crate)` items can't be intra-doc-linked from public docs — write it as plain code font, no link.)

- [ ] **Step 2: Full verification gates**

```bash
cargo fmt
cargo test -p halcyon --no-default-features
cargo test -p halcyon --no-default-features --features derive
cargo test -p halcyon --no-default-features --features stress
cargo clippy -p halcyon --no-default-features --features derive,stress --all-targets
```

Expected: fmt makes no changes (or stage what it fixes), all tests PASS, clippy is warning-free. If a clidriver is available, also `cargo build --all-targets` for the `db2` feature. The loom job, TSan stress, and `live-db2` run in CI on push.

- [ ] **Step 3: Commit**

```bash
git add README.md halcyon/src/lib.rs halcyon/src/core/connection.rs
git commit -m "docs: per-connection statement cache landed (Phase 5)"
```

---

## Acceptance checklist (maps to the spec §6)

- [ ] Cache unit suite (Tasks 2–3) ports all seven C++ behaviors: miss-then-hit, capacity 0, LRU eviction, busy-hit overflow, poison drop, close-failure drop, teardown finalize (ordering asserted via the mock event log).
- [ ] Connection-level integration (Task 5): reuse, rewritten-SQL keying, poison on execute/fetch, mapping errors don't poison, overlap overflow, clear-before-disconnect.
- [ ] Pool/facade (Task 6): default 64, reuse across lease cycles, retry-replay re-prepares.
- [ ] Perf evidence (Task 7): cached vs uncached criterion comparison.
- [ ] Live Db2 (Task 8): `live_statement_cache_reuse` green in the amd64 container or CI `live-db2` — **the phase exit gate**.
- [ ] Docs synced (Task 9).
