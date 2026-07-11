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
            crate::obs::emit_stmt_cache("overflow", self.entries.len());
            return Ok(StmtLease::transient(driver.prepare(conn, sql)?));
        }
        if let Some(e) = self.entries.get_mut(sql) {
            if e.busy {
                crate::obs::emit_stmt_cache("overflow", self.entries.len());
                return Ok(StmtLease::transient(driver.prepare(conn, sql)?));
            }
            e.busy = true;
            self.tick += 1;
            e.last_used = self.tick;
            let handle = e.handle;
            let id = e.id;
            crate::obs::emit_stmt_cache("hit", self.entries.len());
            return Ok(StmtLease::cached(handle, id));
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
                    crate::obs::emit_stmt_cache("evict", self.entries.len());
                }
                // Every entry is leased out: don't insert, serve transient.
                None => {
                    crate::obs::emit_stmt_cache("overflow", self.entries.len());
                    return Ok(StmtLease::transient(handle));
                }
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
        crate::obs::emit_stmt_cache("miss", self.entries.len());
        Ok(StmtLease::cached(handle, self.next_id))
    }

    /// Returns a lease. Transient leases are finalized. Poisoned cached leases
    /// have their entry finalized and dropped; clean ones have their cursor
    /// closed and re-enter the LRU as most-recently-used — unless the reset
    /// fails, in which case the entry is dropped too (never re-serve a
    /// half-reset handle).
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
            debug_assert!(
                !e.busy,
                "statement cache cleared while a lease is outstanding"
            );
            let _ = driver.finalize(e.handle);
        }
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
        assert_eq!(mock.prepared_sql(), vec!["A", "B", "C"]); // A's re-acquire hit
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
        // (The mock records prepared SQL only on success.) Nothing was cached:
        // the next acquire drives a fresh prepare, which then does get cached.
        let l = cache.acquire(mock.as_ref(), conn, "SELEKT").unwrap();
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.prepared_sql().len(), 1);
        let l = cache.acquire(mock.as_ref(), conn, "SELEKT").unwrap(); // hit
        cache.release(mock.as_ref(), l);
        assert_eq!(mock.prepared_sql().len(), 1);
    }

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
}
