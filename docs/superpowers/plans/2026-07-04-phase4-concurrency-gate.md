# Phase 4 Concurrency-Correctness Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the loom + ThreadSanitizer + perf verification track that formally accepts Phase 4 (the connection pool), then flip the roadmap to done.

**Architecture:** One tiny production change — a cfg-gated `sync` shim so `loom` can model-check the pool's `Mutex`/`Condvar`/thread logic. Everything else is new test-only code behind a `stress` feature: a deterministic thread-safe `ConcurrentFakeDriver`, a barrier-based workload runner with a log2 latency histogram, four concurrency scenarios run under ThreadSanitizer, loom models of the acquire/release handoff, and perf harnesses (criterion micro-benchmarks + a custom throughput sweep + a live batch-insert bench).

**Tech Stack:** Rust 2021 (MSRV 1.75), `std::thread::scope` + `Barrier`, `loom` (cfg-gated dep), `criterion` (dev-dep, default-features off), nightly `-Zsanitizer=thread`.

**Reference:** Spec at [docs/superpowers/specs/2026-07-04-phase4-concurrency-gate-design.md](../specs/2026-07-04-phase4-concurrency-gate-design.md). C++ originals under `../Halcyon/tests/stress/`.

---

## Design notes that constrain every task

- **The seam rule holds.** All new code is safe Rust above the seam; no `allow(unsafe_code)`. The `stress` module never links clidriver — scenarios drive `Database` over the fake, so the whole track runs with `--no-default-features`.
- **loom shims only `Mutex`, `Condvar`, and `thread`** (spawn/`JoinHandle`) — **not `Arc`**. `Arc<PoolInner>` and `Arc<dyn CliDriver>` stay `std::sync::Arc` (std's refcount is already correct; the races we model live in the lock/condvar handoff). This keeps the production diff to a handful of lines in `pool.rs`.
- **Determinism.** `PoolConfig.backoff.base_delay = ZERO` makes `delay_for` return `ZERO`, so retries never really sleep. Fault knobs are 1-in-N. Each worker owns a seeded splitmix64 RNG — no `rand` dependency (protects MSRV).
- **Row type is `(i64,)`, not a derived struct** — tuples already implement `FromRow`, so scenarios don't need the `derive` feature.
- **Deferred (out of scope, per spec §6):** statement-cache and async/executor scenarios (need Phases 5/6). The harness leaves room for them but ships neither.

## File / module layout

```
halcyon/src/sync.rs                 NEW  cfg-gated Mutex/Condvar/thread shim (production)
halcyon/src/pool.rs                 EDIT import swap + std::thread::sleep + cfg-gate maintenance + loom module
halcyon/src/lib.rs                  EDIT add `mod sync;` and `#[cfg(feature="stress")] pub mod stress;`
halcyon/src/stress/mod.rs           NEW  re-exports; #[cfg(feature="stress")]
halcyon/src/stress/fake_driver.rs   NEW  ConcurrentFakeDriver
halcyon/src/stress/histogram.rs     NEW  LatencyHistogram
halcyon/src/stress/runner.rs        NEW  WorkerCtx / RunConfig / RunReport / run_workload
halcyon/src/stress/scenarios.rs     NEW  Scenario config presets + the four workload builders
halcyon/tests/stress.rs             NEW  the four scenarios as #[test]s (also the TSan target)
halcyon/benches/pool_bench.rs       NEW  criterion micro-benchmarks
halcyon/examples/throughput.rs      NEW  custom concurrent throughput sweep
halcyon/examples/batch_bench.rs     NEW  live (DSN-gated) batch-insert bench
halcyon/Cargo.toml                  EDIT stress feature, loom dep, criterion dev-dep, targets
.github/workflows/ci.yml            EDIT add stress, stress-tsan, loom jobs
README.md                           EDIT tick Phase 4; name Phase 5 next
```

---

## Task 1: `sync` shim + retarget `pool.rs` for loom (production change)

**Files:**
- Create: `halcyon/src/sync.rs`
- Modify: `halcyon/src/lib.rs:57` (add `mod sync;`)
- Modify: `halcyon/src/pool.rs:21-26` (imports), `:148` / `:188` / `:218` (`thread::sleep` → `std::thread::sleep`), `:419-436` (cfg-gate maintenance), `:564` (`#[cfg(test)]` → `#[cfg(all(test, not(loom)))]`)
- Modify: `halcyon/Cargo.toml` (loom dep — see Task 11 does the rest; the dep line lands here)

- [ ] **Step 1: Add the loom dependency (cfg-gated) to `halcyon/Cargo.toml`**

Add after the `[dev-dependencies]` block:

```toml
# loom is pulled in ONLY under `--cfg loom` (the dedicated CI job / manual runs).
# Normal builds, MSRV, and docs.rs never see it. It backs the cfg-gated `sync`
# shim so the pool's Mutex/Condvar/thread logic can be model-checked.
[target.'cfg(loom)'.dependencies]
loom = "0.7"
```

- [ ] **Step 2: Create the shim `halcyon/src/sync.rs`**

```rust
//! Synchronization-primitive shim so the pool can be model-checked with `loom`.
//!
//! Under `--cfg loom` these resolve to `loom`'s instrumented `Mutex`/`Condvar`/
//! `thread`; otherwise to `std`. `Arc` is deliberately NOT shimmed — the pool
//! keeps `std::sync::Arc` (its refcount is already correct); the interleavings
//! worth modelling live in the lock/condvar handoff. The shim compiles straight
//! to `std` in every normal build, so there is no runtime cost.

#[cfg(not(loom))]
pub(crate) use std::sync::{Condvar, Mutex};
#[cfg(not(loom))]
pub(crate) use std::thread;

#[cfg(loom)]
pub(crate) use loom::sync::{Condvar, Mutex};
#[cfg(loom)]
pub(crate) use loom::thread;
```

- [ ] **Step 3: Register the module in `halcyon/src/lib.rs`**

Change line 57 area — after `mod value;` insert:

```rust
mod value;

// Synchronization shim (std, or loom under `--cfg loom`). See sync.rs.
mod sync;
```

- [ ] **Step 4: Retarget `pool.rs` imports**

Replace lines 24-25:

```rust
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
```

with:

```rust
use std::sync::Arc;

use crate::sync::thread::{self, JoinHandle};
use crate::sync::{Condvar, Mutex};
```

- [ ] **Step 5: Make backoff sleeps always use `std` (loom's thread has no `sleep`)**

In `pool.rs`, replace each `thread::sleep(` with `std::thread::sleep(`. There are three call sites: `make_connection` (was line ~148), the facade does its own, so only the pool's `make_connection`. Search: `rg -n 'thread::sleep' halcyon/src/pool.rs` — expect one hit. After edit:

```rust
                    std::thread::sleep(self.config.backoff.delay_for(attempt));
```

- [ ] **Step 6: cfg-gate the maintenance thread out under loom**

loom models run with `start_maintenance_thread = false` and cannot model the timed `wait_timeout_while`. Gate the whole helper so it never compiles under loom. Change the `start_maintenance` method (line ~419) to add the attribute:

```rust
    #[cfg(not(loom))]
    fn start_maintenance(&mut self) {
        let inner = Arc::clone(&self.inner);
        // ... unchanged body ...
    }
```

And guard its call inside `create` (line ~413):

```rust
        #[cfg(not(loom))]
        if config.start_maintenance_thread {
            pool.start_maintenance();
        }
```

- [ ] **Step 7: Exclude the std-based unit tests under loom**

The existing `#[cfg(test)] mod tests` (line 564) uses `std::thread` + `MockCliDriver`; it must not compile under `--cfg loom`. Change line 564:

```rust
#[cfg(all(test, not(loom)))]
mod tests {
```

- [ ] **Step 8: Verify the normal build is byte-identical in behavior**

Run: `cargo test -p halcyon --no-default-features`
Expected: PASS — the same pool tests as before (the shim compiles to `std`).

Run: `cargo test -p halcyon` (needs clidriver via `DB2_CLIDRIVER_ROOT`)
Expected: PASS.

Run: `cargo clippy -p halcyon --no-default-features --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 9: Commit**

```bash
git add halcyon/src/sync.rs halcyon/src/lib.rs halcyon/src/pool.rs halcyon/Cargo.toml
git commit -m "refactor(pool): route sync primitives through a cfg-gated loom shim"
```

---

## Task 2: `stress` feature scaffold + `ConcurrentFakeDriver`

**Files:**
- Modify: `halcyon/Cargo.toml` (add `stress` feature)
- Modify: `halcyon/src/lib.rs` (add `#[cfg(feature="stress")] pub mod stress;`)
- Create: `halcyon/src/stress/mod.rs`
- Create: `halcyon/src/stress/fake_driver.rs`

- [ ] **Step 1: Add the `stress` feature to `halcyon/Cargo.toml`**

Under `[features]`, after the `derive` line:

```toml
# Exposes the concurrency stress harness (ConcurrentFakeDriver, runner,
# scenarios) as `halcyon::stress`. Pure-Rust, test/bench only; off by default.
stress = []
```

- [ ] **Step 2: Expose the module in `halcyon/src/lib.rs`**

After the `#[cfg(test)] mod testing;` block (line 62), add:

```rust
// Concurrency stress harness (feature `stress`): a deterministic thread-safe
// fake driver, a workload runner, and the pool scenarios. Public so integration
// tests, benches, and examples can reach it; absent from a default build.
#[cfg(feature = "stress")]
pub mod stress;
```

- [ ] **Step 3: Write the failing self-tests for the fake in `halcyon/src/stress/fake_driver.rs`**

Put the tests at the bottom of the new file (module code comes in Step 5):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{CliDriver, ConnectionParams};
    use crate::value::Value;

    fn params() -> ConnectionParams {
        ConnectionParams { connection_string: "dsn".into() }
    }

    #[test]
    fn select_returns_sql_encoded_value() {
        let d = ConcurrentFakeDriver::new();
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "SELECT 7 FROM SYSIBM.SYSDUMMY1").unwrap();
        d.bind_params(s, &[]).unwrap();
        assert_eq!(d.execute(s).unwrap(), 0); // SELECT reports 0 affected
        assert_eq!(d.column_count(s).unwrap(), 1);
        assert!(d.fetch(s).unwrap()); // one row
        assert_eq!(d.get_column(s, 0).unwrap(), Value::Int(7));
        assert!(!d.fetch(s).unwrap()); // end of cursor
    }

    #[test]
    fn fail_execute_every_trips_at_rate() {
        let d = ConcurrentFakeDriver::new();
        d.fail_execute_every.store(2, std::sync::atomic::Ordering::Relaxed);
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "SELECT 1 FROM SYSIBM.SYSDUMMY1").unwrap();
        assert!(d.execute(s).is_ok()); // call 1
        let e = d.execute(s).unwrap_err(); // call 2 -> fail
        assert!(e.retriable);
    }

    #[test]
    fn statement_on_dead_connection_errors() {
        let d = ConcurrentFakeDriver::new();
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "SELECT 1 FROM SYSIBM.SYSDUMMY1").unwrap();
        d.disconnect(c).unwrap();
        assert!(d.execute(s).is_err()); // use after the connection died
    }

    #[test]
    fn insert_reports_one_affected_row() {
        let d = ConcurrentFakeDriver::new();
        let c = d.connect(&params()).unwrap();
        let s = d.prepare(c, "INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(d.execute(s).unwrap(), 1);
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p halcyon --no-default-features --features stress fake_driver`
Expected: FAIL — `ConcurrentFakeDriver` not defined / module empty.

- [ ] **Step 5: Implement the fake in `halcyon/src/stress/fake_driver.rs`** (above the test module from Step 3)

```rust
//! A thread-safe, deterministic `CliDriver` for concurrency testing. Unlike the
//! FIFO-scripted `MockCliDriver`, every response is derived from its inputs, so
//! any number of threads may call it with no ordering assumptions. Counters are
//! atomics; the handle maps are guarded by one mutex (the fake is intentionally
//! a little pessimistic — the contention we measure lives in the pool, not here).
//! Port of `../Halcyon/tests/stress/support/concurrent_fake_driver.hpp`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use crate::driver::{
    CliDriver, ConnectionHandle, ConnectionParams, StatementHandle,
};
use crate::error::{Error, ErrorCode, Result};
use crate::value::Value;

struct StmtState {
    conn: u64,
    sql: String,
    position: i64,
}

#[derive(Default)]
struct State {
    live: HashSet<u64>,
    dead: HashSet<u64>,
    stmts: HashMap<u64, StmtState>,
    next_conn: u64,
    next_stmt: u64,
}

/// Deterministic, thread-safe fake driver. Construct with [`new`](Self::new),
/// tune the atomic fault knobs, share via `Arc`, and read the atomic counters
/// after a run.
#[derive(Default)]
pub struct ConcurrentFakeDriver {
    state: Mutex<State>,

    // --- fault injection (1-in-N; 0 disables) ---
    pub fail_connect_every: AtomicI64,
    pub fail_execute_every: AtomicI64,
    pub kill_connection_every: AtomicI64,

    // --- counters ---
    connect_calls: AtomicI64,
    disconnect_calls: AtomicI64,
    prepare_calls: AtomicI64,
    execute_calls: AtomicI64,
    alive_calls: AtomicI64,
    finalize_calls: AtomicI64,
    close_cursor_calls: AtomicI64,
    commit_calls: AtomicI64,
    rollback_calls: AtomicI64,
    auto_commit_on: AtomicI64,
    auto_commit_off: AtomicI64,
    in_flight: AtomicI64,
    peak_in_flight: AtomicI64,
}

/// Decrements `in_flight` when an in-flight execute finishes (any path).
struct InFlightGuard<'a>(&'a AtomicI64);
impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ConcurrentFakeDriver {
    pub fn new() -> Self {
        Self::default()
    }

    // --- counter accessors (loaded snapshots) ---
    pub fn connect_calls(&self) -> i64 { self.connect_calls.load(Ordering::Relaxed) }
    pub fn disconnect_calls(&self) -> i64 { self.disconnect_calls.load(Ordering::Relaxed) }
    pub fn prepare_calls(&self) -> i64 { self.prepare_calls.load(Ordering::Relaxed) }
    pub fn execute_calls(&self) -> i64 { self.execute_calls.load(Ordering::Relaxed) }
    pub fn commit_calls(&self) -> i64 { self.commit_calls.load(Ordering::Relaxed) }
    pub fn rollback_calls(&self) -> i64 { self.rollback_calls.load(Ordering::Relaxed) }
    pub fn auto_commit_on(&self) -> i64 { self.auto_commit_on.load(Ordering::Relaxed) }
    pub fn auto_commit_off(&self) -> i64 { self.auto_commit_off.load(Ordering::Relaxed) }
    pub fn peak_in_flight(&self) -> i64 { self.peak_in_flight.load(Ordering::Relaxed) }

    fn enter_in_flight(&self) -> InFlightGuard<'_> {
        let cur = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        let mut prev = self.peak_in_flight.load(Ordering::Relaxed);
        while cur > prev {
            match self.peak_in_flight.compare_exchange_weak(
                prev, cur, Ordering::Relaxed, Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => prev = p,
            }
        }
        InFlightGuard(&self.in_flight)
    }
}

// Leading non-space token is SELECT or VALUES (the fake never sees real DML
// result sets; DML returns an affected-row count instead).
fn is_select(sql: &str) -> bool {
    let t = sql.trim_start();
    let up = t.get(..6.min(t.len())).unwrap_or("").to_ascii_uppercase();
    up.starts_with("SELECT") || t.trim_start().to_ascii_uppercase().starts_with("VALUES")
}

// The first contiguous run of digits in the SQL (e.g. "SELECT 7 FROM ..." -> 7).
fn encoded_value(sql: &str) -> i64 {
    let mut v: i64 = 0;
    let mut seen = false;
    for ch in sql.bytes() {
        if ch.is_ascii_digit() {
            v = v * 10 + i64::from(ch - b'0');
            seen = true;
        } else if seen {
            break;
        }
    }
    v
}

fn transient() -> Error {
    let mut e = Error::new(ErrorCode::Transient, "injected transient");
    e.sqlstate = "40001".into();
    e.retriable = true;
    e
}
fn stale() -> Error {
    let mut e = Error::new(ErrorCode::Connection, "statement used on a dead connection");
    e.sqlstate = "08003".into();
    e.retriable = true;
    e
}
fn mapping() -> Error {
    Error::new(ErrorCode::Mapping, "column index out of range")
}

impl CliDriver for ConcurrentFakeDriver {
    fn connect(&self, _params: &ConnectionParams) -> Result<ConnectionHandle> {
        let n = self.connect_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let every = self.fail_connect_every.load(Ordering::Relaxed);
        if every > 0 && n % every == 0 {
            return Err(transient());
        }
        let mut g = self.state.lock().unwrap();
        g.next_conn += 1;
        let h = g.next_conn;
        g.live.insert(h);
        Ok(ConnectionHandle(h))
    }

    fn disconnect(&self, conn: ConnectionHandle) -> Result<()> {
        self.disconnect_calls.fetch_add(1, Ordering::Relaxed);
        let mut g = self.state.lock().unwrap();
        g.live.remove(&conn.0);
        g.dead.remove(&conn.0);
        Ok(())
    }

    fn is_alive(&self, conn: ConnectionHandle) -> Result<bool> {
        self.alive_calls.fetch_add(1, Ordering::Relaxed);
        let g = self.state.lock().unwrap();
        if g.dead.contains(&conn.0) {
            return Ok(false);
        }
        Ok(g.live.contains(&conn.0))
    }

    fn prepare(&self, conn: ConnectionHandle, sql: &str) -> Result<StatementHandle> {
        self.prepare_calls.fetch_add(1, Ordering::Relaxed);
        let mut g = self.state.lock().unwrap();
        if !g.live.contains(&conn.0) {
            return Err(stale());
        }
        g.next_stmt += 1;
        let h = g.next_stmt;
        g.stmts.insert(h, StmtState { conn: conn.0, sql: sql.to_owned(), position: -1 });
        Ok(StatementHandle(h))
    }

    fn bind_params(&self, stmt: StatementHandle, _params: &[Value]) -> Result<()> {
        let g = self.state.lock().unwrap();
        match g.stmts.get(&stmt.0) {
            Some(s) if g.live.contains(&s.conn) => Ok(()),
            _ => Err(stale()),
        }
    }

    fn execute(&self, stmt: StatementHandle) -> Result<i64> {
        let n = self.execute_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _guard = self.enter_in_flight();

        let fe = self.fail_execute_every.load(Ordering::Relaxed);
        if fe > 0 && n % fe == 0 {
            return Err(transient());
        }
        let ke = self.kill_connection_every.load(Ordering::Relaxed);

        let mut g = self.state.lock().unwrap();
        let (conn, is_sel) = match g.stmts.get(&stmt.0) {
            Some(s) => (s.conn, is_select(&s.sql)),
            None => return Err(stale()),
        };
        if !g.live.contains(&conn) {
            return Err(stale());
        }
        if let Some(s) = g.stmts.get_mut(&stmt.0) {
            s.position = -1;
        }
        if ke > 0 && n % ke == 0 {
            g.live.remove(&conn); // next isAlive -> false
            g.dead.insert(conn);
        }
        Ok(if is_sel { 0 } else { 1 })
    }

    fn execute_batch(&self, stmt: StatementHandle, rows: &[Vec<Value>]) -> Result<i64> {
        let mut total = 0;
        for row in rows {
            self.bind_params(stmt, row)?;
            total += self.execute(stmt)?;
        }
        Ok(total)
    }

    fn column_count(&self, stmt: StatementHandle) -> Result<usize> {
        let g = self.state.lock().unwrap();
        match g.stmts.get(&stmt.0) {
            Some(s) if is_select(&s.sql) => Ok(1),
            _ => Ok(0),
        }
    }

    fn column_name(&self, _stmt: StatementHandle, _index: usize) -> Result<String> {
        Ok("c0".into())
    }

    fn fetch(&self, stmt: StatementHandle) -> Result<bool> {
        let mut g = self.state.lock().unwrap();
        let conn = match g.stmts.get(&stmt.0) {
            Some(s) => s.conn,
            None => return Err(stale()),
        };
        if !g.live.contains(&conn) {
            return Err(stale());
        }
        let s = g.stmts.get_mut(&stmt.0).unwrap();
        s.position += 1;
        Ok(s.position < 1) // exactly one row
    }

    fn get_column(&self, stmt: StatementHandle, index: usize) -> Result<Value> {
        let g = self.state.lock().unwrap();
        match g.stmts.get(&stmt.0) {
            Some(s) if s.position == 0 && index == 0 => Ok(Value::Int(encoded_value(&s.sql))),
            _ => Err(mapping()),
        }
    }

    fn finalize(&self, stmt: StatementHandle) -> Result<()> {
        self.finalize_calls.fetch_add(1, Ordering::Relaxed);
        self.state.lock().unwrap().stmts.remove(&stmt.0);
        Ok(())
    }

    fn close_cursor(&self, stmt: StatementHandle) -> Result<()> {
        self.close_cursor_calls.fetch_add(1, Ordering::Relaxed);
        let mut g = self.state.lock().unwrap();
        if let Some(s) = g.stmts.get_mut(&stmt.0) {
            s.position = -1;
        }
        Ok(())
    }

    fn set_auto_commit(&self, _conn: ConnectionHandle, enabled: bool) -> Result<()> {
        if enabled {
            self.auto_commit_on.fetch_add(1, Ordering::Relaxed);
        } else {
            self.auto_commit_off.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn commit(&self, _conn: ConnectionHandle) -> Result<()> {
        self.commit_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn rollback(&self, _conn: ConnectionHandle) -> Result<()> {
        self.rollback_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
```

- [ ] **Step 6: Create `halcyon/src/stress/mod.rs` exposing the fake**

```rust
//! Concurrency stress harness (feature `stress`). Rust-native port of the ideas
//! in `../Halcyon/tests/stress/`: a deterministic thread-safe fake driver, a
//! barrier-based workload runner with a log2 latency histogram, and the pool
//! scenarios. Pure-Rust — no clidriver, no live Db2.

mod fake_driver;

pub use fake_driver::ConcurrentFakeDriver;
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p halcyon --no-default-features --features stress fake_driver`
Expected: PASS (4 tests).

Run: `cargo clippy -p halcyon --no-default-features --features stress --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 8: Commit**

```bash
git add halcyon/Cargo.toml halcyon/src/lib.rs halcyon/src/stress/
git commit -m "test(stress): deterministic thread-safe ConcurrentFakeDriver behind `stress` feature"
```

---

## Task 3: `LatencyHistogram`

**Files:**
- Create: `halcyon/src/stress/histogram.rs`
- Modify: `halcyon/src/stress/mod.rs` (add `mod histogram; pub use ...`)

- [ ] **Step 1: Write the failing tests in `halcyon/src/stress/histogram.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_merges() {
        let mut a = LatencyHistogram::new();
        for _ in 0..100 { a.record(1_000); }
        let mut b = LatencyHistogram::new();
        for _ in 0..100 { b.record(1_000); }
        a.merge(&b);
        assert_eq!(a.count(), 200);
    }

    #[test]
    fn percentile_is_monotonic() {
        let mut h = LatencyHistogram::new();
        for _ in 0..1000 { h.record(1_000); }        // ~1us
        for _ in 0..10 { h.record(1_000 * 1_000); }  // ~1ms tail
        assert!(h.count() > 0);
        assert!(h.percentile_us(0.50) <= h.percentile_us(0.99));
        assert!(h.percentile_us(0.99) <= h.max_us());
    }

    #[test]
    fn empty_is_zero() {
        let h = LatencyHistogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.percentile_us(0.5), 0.0);
        assert_eq!(h.max_us(), 0.0);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p halcyon --no-default-features --features stress histogram`
Expected: FAIL — `LatencyHistogram` not found.

- [ ] **Step 3: Implement (above the test module)**

```rust
//! Fixed-bucket log2 histogram of nanosecond latencies. Bucket i holds samples
//! in [2^i, 2^(i+1)) ns. Allocation-free, trivially mergeable; percentiles
//! return the bucket's upper bound in microseconds (approximate by design —
//! cheap and distortion-free on the hot path). Port of
//! `../Halcyon/tests/stress/support/latency_histogram.hpp`.

const BUCKETS: usize = 64;

#[derive(Clone)]
pub struct LatencyHistogram {
    counts: [u64; BUCKETS],
    total: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        LatencyHistogram { counts: [0; BUCKETS], total: 0 }
    }

    pub fn record(&mut self, ns: u64) {
        self.counts[bucket_for(ns)] += 1;
        self.total += 1;
    }

    pub fn merge(&mut self, other: &LatencyHistogram) {
        for i in 0..BUCKETS {
            self.counts[i] += other.counts[i];
        }
        self.total += other.total;
    }

    pub fn count(&self) -> u64 {
        self.total
    }

    pub fn percentile_us(&self, p: f64) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        let target = (p * self.total as f64) as u64;
        let mut cum = 0u64;
        for i in 0..BUCKETS {
            cum += self.counts[i];
            if cum >= target {
                return bucket_upper_us(i);
            }
        }
        self.max_us()
    }

    pub fn max_us(&self) -> f64 {
        for i in (0..BUCKETS).rev() {
            if self.counts[i] != 0 {
                return bucket_upper_us(i);
            }
        }
        0.0
    }
}

fn bucket_for(mut ns: u64) -> usize {
    let mut b = 0usize;
    while b < BUCKETS - 1 {
        ns >>= 1;
        if ns == 0 {
            break;
        }
        b += 1;
    }
    b
}

// Upper bound of bucket i is 2^(i+1) ns -> /1000 us. `powi`/`ldexp` via f64 so
// the top bucket never triggers a shift-by-64.
fn bucket_upper_us(i: usize) -> f64 {
    2f64.powi(i as i32 + 1) / 1000.0
}
```

- [ ] **Step 4: Wire into `halcyon/src/stress/mod.rs`**

```rust
mod fake_driver;
mod histogram;

pub use fake_driver::ConcurrentFakeDriver;
pub use histogram::LatencyHistogram;
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p halcyon --no-default-features --features stress histogram`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add halcyon/src/stress/histogram.rs halcyon/src/stress/mod.rs
git commit -m "test(stress): log2 LatencyHistogram"
```

---

## Task 4: workload runner (`WorkerCtx` / `RunConfig` / `RunReport` / `run_workload`)

**Files:**
- Create: `halcyon/src/stress/runner.rs`
- Modify: `halcyon/src/stress/mod.rs`

- [ ] **Step 1: Write the failing tests in `halcyon/src/stress/runner.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    #[test]
    fn runs_exactly_total_iterations_across_threads() {
        let ops = AtomicI64::new(0);
        let cfg = RunConfig { threads: 4, stop: Stop::Iters(4000), warmup: None, seed: 1 };
        let r = run_workload("count", &cfg, |_ctx| {
            ops.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(ops.load(Ordering::Relaxed), 4000);
        assert_eq!(r.ops, 4000);
        assert!(!r.failed);
        assert_eq!(r.hist.count(), 4000);
    }

    #[test]
    fn surfaces_worker_failure() {
        let cfg = RunConfig { threads: 2, stop: Stop::Iters(10), warmup: None, seed: 1 };
        let r = run_workload("boom", &cfg, |ctx| ctx.fail("kaboom"));
        assert!(r.failed);
        assert_eq!(r.first_error.as_deref(), Some("kaboom"));
    }

    #[test]
    fn duration_mode_runs_and_reports_throughput() {
        let cfg = RunConfig {
            threads: 2,
            stop: Stop::Duration(std::time::Duration::from_millis(50)),
            warmup: None,
            seed: 1,
        };
        let r = run_workload("spin", &cfg, |_ctx| {});
        assert!(r.ops > 0);
        assert!(r.throughput_per_sec() > 0.0);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p halcyon --no-default-features --features stress runner`
Expected: FAIL — items not found.

- [ ] **Step 3: Implement (above the test module)**

```rust
//! Barrier-released workload runner. Spawns `threads` workers via
//! `std::thread::scope`, releases them together, loops the body until the stop
//! condition, times each call into a per-worker histogram, then joins and
//! merges. The first worker failure is surfaced in the report. Rust-native port
//! of `../Halcyon/tests/stress/support/workload_runner.*`.

use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use super::histogram::LatencyHistogram;

/// Stop after a fixed iteration count (deterministic; correctness) OR after a
/// wall-clock duration (steady-state; perf).
#[derive(Clone, Copy)]
pub enum Stop {
    Iters(u64),
    Duration(Duration),
}

pub struct RunConfig {
    pub threads: usize,
    pub stop: Stop,
    /// Optional discarded warmup phase before timing (perf).
    pub warmup: Option<Stop>,
    pub seed: u64,
}

/// Per-worker state passed to the body. Each worker owns its RNG and histogram —
/// no shared per-op state, so no false contention and no measurement skew.
pub struct WorkerCtx {
    pub thread_index: usize,
    rng: u64,
    hist: LatencyHistogram,
    failed: Option<String>,
    errors: u64,
}

impl WorkerCtx {
    /// splitmix64 — a seeded, dependency-free PRNG for workload choices.
    pub fn next_u64(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Record the first hard failure for this worker (stops its loop).
    pub fn fail(&mut self, msg: impl Into<String>) {
        if self.failed.is_none() {
            self.failed = Some(msg.into());
        }
    }

    /// Count a tolerated (non-fatal) error — a pool acquire-timeout or a
    /// genuinely-exhausted bounded retry. Drives the report's error column.
    pub fn note_error(&mut self) {
        self.errors += 1;
    }
}

pub struct RunReport {
    pub name: String,
    pub threads: usize,
    pub ops: u64,
    pub errors: u64,
    pub wall: Duration,
    pub hist: LatencyHistogram,
    pub failed: bool,
    pub first_error: Option<String>,
}

impl RunReport {
    pub fn throughput_per_sec(&self) -> f64 {
        let secs = self.wall.as_secs_f64();
        if secs > 0.0 { self.ops as f64 / secs } else { 0.0 }
    }
    pub fn p50_us(&self) -> f64 { self.hist.percentile_us(0.50) }
    pub fn p95_us(&self) -> f64 { self.hist.percentile_us(0.95) }
    pub fn p99_us(&self) -> f64 { self.hist.percentile_us(0.99) }
    pub fn max_us(&self) -> f64 { self.hist.max_us() }
    pub fn error_rate(&self) -> f64 {
        if self.ops > 0 { self.errors as f64 / self.ops as f64 } else { 0.0 }
    }
}

struct WorkerOut {
    ops: u64,
    errors: u64,
    hist: LatencyHistogram,
    failed: Option<String>,
}

pub fn run_workload<F>(name: &str, cfg: &RunConfig, body: F) -> RunReport
where
    F: Fn(&mut WorkerCtx) + Sync,
{
    let threads = cfg.threads.max(1);
    let barrier = Barrier::new(threads);
    let body = &body;
    let t0 = Instant::now();

    let outs: Vec<WorkerOut> = thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|i| {
                let barrier = &barrier;
                scope.spawn(move || {
                    let mut ctx = WorkerCtx {
                        thread_index: i,
                        rng: cfg.seed ^ (i as u64).wrapping_mul(0x1000_0000_1B3),
                        hist: LatencyHistogram::new(),
                        failed: None,
                        errors: 0,
                    };

                    // Warmup (discarded): run, then reset the measured state.
                    if let Some(w) = cfg.warmup {
                        run_phase(&mut ctx, threads, w, body);
                        ctx.hist = LatencyHistogram::new();
                        ctx.errors = 0;
                        ctx.failed = None;
                    }

                    barrier.wait();
                    let ops = run_phase(&mut ctx, threads, cfg.stop, body);
                    WorkerOut {
                        ops,
                        errors: ctx.errors,
                        hist: ctx.hist,
                        failed: ctx.failed,
                    }
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let wall = t0.elapsed();
    let mut report = RunReport {
        name: name.to_string(),
        threads,
        ops: 0,
        errors: 0,
        wall,
        hist: LatencyHistogram::new(),
        failed: false,
        first_error: None,
    };
    for out in outs {
        report.ops += out.ops;
        report.errors += out.errors;
        report.hist.merge(&out.hist);
        if let Some(msg) = out.failed {
            if !report.failed {
                report.failed = true;
                report.first_error = Some(msg);
            }
        }
    }
    report
}

// Runs one phase (warmup or timed), returning this worker's op count. Stops early
// if the worker recorded a hard failure.
fn run_phase<F>(ctx: &mut WorkerCtx, threads: usize, stop: Stop, body: &F) -> u64
where
    F: Fn(&mut WorkerCtx) + Sync,
{
    let mut ops = 0u64;
    match stop {
        Stop::Iters(total) => {
            // Split iterations across threads; earlier threads take the remainder.
            let base = total / threads as u64;
            let rem = total % threads as u64;
            let quota = base + if (ctx.thread_index as u64) < rem { 1 } else { 0 };
            for _ in 0..quota {
                if ctx.failed.is_some() {
                    break;
                }
                let t = Instant::now();
                body(ctx);
                ctx.hist.record(t.elapsed().as_nanos() as u64);
                ops += 1;
            }
        }
        Stop::Duration(d) => {
            let deadline = Instant::now() + d;
            while Instant::now() < deadline {
                if ctx.failed.is_some() {
                    break;
                }
                let t = Instant::now();
                body(ctx);
                ctx.hist.record(t.elapsed().as_nanos() as u64);
                ops += 1;
            }
        }
    }
    ops
}
```

- [ ] **Step 4: Wire into `halcyon/src/stress/mod.rs`**

```rust
mod fake_driver;
mod histogram;
mod runner;

pub use fake_driver::ConcurrentFakeDriver;
pub use histogram::LatencyHistogram;
pub use runner::{run_workload, RunConfig, RunReport, Stop, WorkerCtx};
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p halcyon --no-default-features --features stress runner`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add halcyon/src/stress/runner.rs halcyon/src/stress/mod.rs
git commit -m "test(stress): barrier-based workload runner over thread::scope"
```

---

## Task 5: scenario config presets + workload builders

**Files:**
- Create: `halcyon/src/stress/scenarios.rs`
- Modify: `halcyon/src/stress/mod.rs`

- [ ] **Step 1: Write the failing tests in `halcyon/src/stress/scenarios.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_for_sets_min_and_max() {
        let c = config_for(Scenario::Pool, 8);
        assert_eq!(c.max, 8);
        assert_eq!(c.min, 2);
        assert!(!c.start_maintenance_thread);
    }

    #[test]
    fn config_for_clamps_tiny_max() {
        let c = config_for(Scenario::Txn, 0);
        assert_eq!(c.max, 1);
        assert_eq!(c.min, 1);
    }

    #[test]
    fn lifecycle_enables_the_reaper() {
        let c = config_for(Scenario::Lifecycle, 6);
        assert!(c.start_maintenance_thread);
        assert!(c.max_lifetime <= std::time::Duration::from_millis(5));
    }

    #[test]
    fn select_n_embeds_the_value() {
        assert_eq!(select_n(7), "SELECT 7 FROM SYSIBM.SYSDUMMY1");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p halcyon --no-default-features --features stress scenarios`
Expected: FAIL — items not found.

- [ ] **Step 3: Implement (above the test module)**

```rust
//! Pool config presets and the workload builders each scenario runs. The four
//! scenarios that map onto today's Rust surface: pool contention, reconnect +
//! transient faults, transaction churn, reaper race. (Statement-cache and
//! async scenarios are deferred to Phases 5/6 — see the design spec §6.)

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use crate::error::{Error, ErrorCode};
use crate::facade::Database;
use crate::retry::BackoffPolicy;
use crate::pool::PoolConfig;

use super::runner::WorkerCtx;

#[derive(Clone, Copy)]
pub enum Scenario {
    Pool,
    Reconnect,
    Txn,
    Lifecycle,
}

/// The tuned `PoolConfig` for each scenario. `pool_max` is the only sweepable
/// knob the perf harness varies; everything else is scenario-intrinsic. Backoff
/// is zero so retries never really sleep (deterministic).
pub fn config_for(id: Scenario, pool_max: usize) -> PoolConfig {
    let max = pool_max.max(1);
    let min = if max < 2 { max } else { 2 };
    let mut cfg = PoolConfig {
        min,
        max,
        acquire_timeout: Duration::from_millis(5000),
        idle_timeout: Duration::from_millis(60_000),
        max_lifetime: Duration::from_millis(1_800_000),
        validate_on_acquire: false,
        backoff: BackoffPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            max_attempts: 3,
        },
        maintenance_interval: Duration::from_millis(1000),
        start_maintenance_thread: false,
    };
    match id {
        Scenario::Pool => {
            cfg.acquire_timeout = Duration::from_millis(50);
        }
        Scenario::Reconnect => {
            cfg.validate_on_acquire = true;
            cfg.backoff.max_attempts = 3;
        }
        Scenario::Txn => {}
        Scenario::Lifecycle => {
            cfg.start_maintenance_thread = true;
            cfg.maintenance_interval = Duration::from_millis(1);
            cfg.idle_timeout = Duration::from_millis(1);
            cfg.max_lifetime = Duration::from_millis(5);
        }
    }
    cfg
}

/// A canonical scalar SELECT the fake/live encode as `n`.
pub fn select_n(n: i64) -> String {
    format!("SELECT {n} FROM SYSIBM.SYSDUMMY1")
}

// --- Scenario 1: raw pool acquire/release contention (pool mechanics only) ---
pub fn make_pool_contention(db: &Database) -> impl Fn(&mut WorkerCtx) + Sync + '_ {
    move |ctx| {
        let lease = match db.pool().acquire() {
            Ok(l) => l,
            Err(e) => {
                if e.code != ErrorCode::Pool {
                    ctx.fail(format!("unexpected acquire error: {e}"));
                } else {
                    ctx.note_error(); // tolerated acquire-timeout under contention
                }
                return;
            }
        };
        match lease.query_as::<(i64,)>(&select_n(7), ()) {
            Ok(rows) if rows == vec![(7,)] => {}
            Ok(_) => ctx.fail("wrong scalar from pooled query"),
            Err(e) => ctx.fail(format!("query failed: {e}")),
        }
    }
}

// --- Scenario 2: reconnect + transient-fault injection under load ---
pub fn make_reconnect_faults<'a>(
    db: &'a Database,
    recovered: &'a AtomicI64,
) -> impl Fn(&mut WorkerCtx) + Sync + 'a {
    move |ctx| {
        match db.query_as::<(i64,)>(&select_n(7), ()) {
            Ok(rows) if rows == vec![(7,)] => {
                recovered.fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) => ctx.fail("wrong scalar after recovery"),
            Err(e) => {
                // Exhausting a bounded retry budget surfaces a *retriable* error:
                // allowed. A non-retriable error means recovery corrupted the path.
                if !e.retriable {
                    ctx.fail(format!("non-retriable error surfaced: {e}"));
                } else {
                    ctx.note_error();
                }
            }
        }
    }
}

// --- Scenario 3: transaction churn (commit + rollback) ---
pub fn make_txn_churn(db: &Database) -> impl Fn(&mut WorkerCtx) + Sync + '_ {
    move |ctx| {
        let do_commit = (ctx.next_u64() & 1) == 0;
        let r: crate::error::Result<i64> = db.transaction(|tx| {
            let rows = tx.query_as::<(i64,)>(&select_n(7), ())?;
            if rows != vec![(7,)] {
                return Err(Error::new(ErrorCode::Mapping, "wrong scalar in tx"));
            }
            if do_commit {
                Ok(1)
            } else {
                Err(Error::new(ErrorCode::Unknown, "intentional rollback"))
            }
        });
        // A committed tx returns Ok; a rolled-back tx returns our forced error.
        // A Mapping error means the in-tx read was wrong -> real failure.
        if let Err(e) = r {
            if e.code == ErrorCode::Mapping {
                ctx.fail("in-transaction read returned wrong scalar");
            }
        }
    }
}
```

- [ ] **Step 4: Wire into `halcyon/src/stress/mod.rs`**

```rust
mod fake_driver;
mod histogram;
mod runner;
mod scenarios;

pub use fake_driver::ConcurrentFakeDriver;
pub use histogram::LatencyHistogram;
pub use runner::{run_workload, RunConfig, RunReport, Stop, WorkerCtx};
pub use scenarios::{
    config_for, make_pool_contention, make_reconnect_faults, make_txn_churn, select_n, Scenario,
};
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p halcyon --no-default-features --features stress scenarios`
Expected: PASS (4 tests).

Run: `cargo clippy -p halcyon --no-default-features --features stress --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add halcyon/src/stress/scenarios.rs halcyon/src/stress/mod.rs
git commit -m "test(stress): scenario config presets + pool/reconnect/txn workload builders"
```

---

## Task 6: the four scenarios as integration tests (also the TSan target)

**Files:**
- Create: `halcyon/tests/stress.rs`

- [ ] **Step 1: Write the scenario tests in `halcyon/tests/stress.rs`**

These assert the C++ invariants (`../Halcyon/tests/stress/correctness/test_stress_concurrency.cpp`). The file is entirely gated on `stress` so a normal `cargo test` ignores it.

```rust
//! Concurrency-correctness scenarios (feature `stress`). Run under
//! ThreadSanitizer in CI. Each drives the pool/facade over the deterministic
//! `ConcurrentFakeDriver` from many threads and asserts the pool invariants.
#![cfg(feature = "stress")]

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use halcyon::stress::{
    config_for, make_pool_contention, make_reconnect_faults, make_txn_churn, run_workload,
    ConcurrentFakeDriver, RunConfig, Scenario, Stop,
};
use halcyon::{ConnectionParams, Database};

fn open(fake: Arc<ConcurrentFakeDriver>, scenario: Scenario, pool_max: usize) -> Database {
    Database::open(
        fake,
        ConnectionParams { connection_string: "dsn".into() },
        config_for(scenario, pool_max),
    )
    .expect("pool warmup")
}

#[test]
fn pool_contention_stays_consistent() {
    let fake = Arc::new(ConcurrentFakeDriver::new());
    let db = open(fake.clone(), Scenario::Pool, 8);

    let body = make_pool_contention(&db);
    let cfg = RunConfig { threads: 32, stop: Stop::Iters(20_000), warmup: None, seed: 1 };
    let r = run_workload("pool", &cfg, body);

    assert!(!r.failed, "{:?}", r.first_error);
    assert!(fake.peak_in_flight() <= 8, "never more leases than max");
    assert_eq!(db.pool().active_count(), 0);
    assert_eq!(db.pool().idle_count(), db.pool().total_count());
}

#[test]
fn reconnect_and_retry_recover_under_faults() {
    let fake = Arc::new(ConcurrentFakeDriver::new());
    fake.fail_execute_every.store(7, Ordering::Relaxed); // retriable statement errors
    fake.kill_connection_every.store(11, Ordering::Relaxed); // forces validate reconnects
    let db = open(fake.clone(), Scenario::Reconnect, 6);
    let connects_after_warmup = fake.connect_calls();

    let recovered = AtomicI64::new(0);
    let body = make_reconnect_faults(&db, &recovered);
    let cfg = RunConfig { threads: 12, stop: Stop::Iters(30_000), warmup: None, seed: 2 };
    let r = run_workload("reconnect", &cfg, body);

    assert!(!r.failed, "{:?}", r.first_error); // no wrong scalar / non-retriable
    assert!(fake.connect_calls() > connects_after_warmup, "reconnects fired");
    assert!(fake.execute_calls() > r.ops as i64, "retries fired");
    assert!(
        recovered.load(Ordering::Relaxed) > (r.ops as i64) * 9 / 10,
        "vast majority recovered"
    );
    assert_eq!(db.pool().active_count(), 0); // no leaked leases
}

#[test]
fn transaction_churn_commits_and_rollbacks_balance() {
    let fake = Arc::new(ConcurrentFakeDriver::new());
    let db = open(fake.clone(), Scenario::Txn, 6);

    let body = make_txn_churn(&db);
    let cfg = RunConfig { threads: 8, stop: Stop::Iters(16_000), warmup: None, seed: 3 };
    let r = run_workload("txn", &cfg, body);

    assert!(!r.failed, "{:?}", r.first_error);
    let txns = fake.auto_commit_off(); // each begin() flips off
    assert_eq!(txns, 16_000);
    assert_eq!(fake.commit_calls() + fake.rollback_calls(), txns);
    assert_eq!(fake.auto_commit_on(), txns); // each tx restores it
    assert_eq!(db.pool().active_count(), 0); // no tx leaks a connection
}

#[test]
fn reaper_races_acquire_release_safely() {
    let fake = Arc::new(ConcurrentFakeDriver::new());
    let db = open(fake.clone(), Scenario::Lifecycle, 6); // reaper on, tiny intervals

    let body = make_pool_contention(&db); // acquire/query/release loop
    let cfg = RunConfig {
        threads: 16,
        stop: Stop::Duration(Duration::from_millis(300)), // let the reaper churn
        warmup: None,
        seed: 4,
    };
    let r = run_workload("lifecycle", &cfg, body);

    assert!(!r.failed, "{:?}", r.first_error);
    assert!(fake.peak_in_flight() <= 6, "reaper never reaps a live lease");
    assert_eq!(db.pool().active_count(), 0);
    assert!(db.pool().total_count() >= db.pool().idle_count(), "accounting consistent");
}
```

- [ ] **Step 2: Export `ConnectionParams` from the crate root (the test imports it)**

Check `halcyon/src/lib.rs` — the `pub use` block exports `Database` but not `ConnectionParams`. It lives in `pub mod driver`, so `halcyon::driver::ConnectionParams` is already reachable. Update the test import to use that path instead of a root re-export:

Change the test's import line to:

```rust
use halcyon::driver::ConnectionParams;
use halcyon::Database;
```

(No lib change needed — `driver` is already `pub`.)

- [ ] **Step 3: Run the scenarios**

Run: `cargo test -p halcyon --no-default-features --features stress --test stress -- --nocapture`
Expected: PASS (4 tests). Each spins many threads; runtime is a few seconds.

- [ ] **Step 4: Run under ThreadSanitizer locally (optional but recommended; needs nightly)**

Run:
```bash
RUSTFLAGS="-Zsanitizer=thread" cargo +nightly test -Zbuild-std \
  --target x86_64-unknown-linux-gnu \
  -p halcyon --no-default-features --features stress --test stress
```
Expected: PASS with no ThreadSanitizer data-race report. (On Apple Silicon use `--target aarch64-apple-darwin`; on Intel macOS `x86_64-apple-darwin`.)

- [ ] **Step 5: Commit**

```bash
git add halcyon/tests/stress.rs
git commit -m "test(stress): four pool concurrency scenarios (TSan target)"
```

---

## Task 7: loom models of the acquire/release handoff

**Files:**
- Modify: `halcyon/src/pool.rs` (append a `#[cfg(all(test, loom))] mod loom_tests`)

- [ ] **Step 1: Append the loom module at the end of `halcyon/src/pool.rs`**

It reuses the pool's private internals (`use super::*`) and a minimal loom-safe fake. Keep the model small (loom's state space is exponential): 2 threads, `max = 1`, maintenance off.

```rust
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::driver::{
        CliDriver, ConnectionHandle, ConnectionParams, StatementHandle,
    };
    use crate::error::{Error, ErrorCode, Result};
    use crate::value::Value;
    use loom::sync::Arc as LoomArc; // for spawning; pool internals use std::sync::Arc

    // Minimal stateless fake: everything trivially succeeds. `max = 1` plus the
    // pool's `pending` reservation means at most one connection exists at a time,
    // so a constant handle never collides. No atomics/locks of its own, so it adds
    // zero branches beyond the pool's own Mutex/Condvar for loom to explore.
    struct LoomFake;

    impl CliDriver for LoomFake {
        fn connect(&self, _p: &ConnectionParams) -> Result<ConnectionHandle> {
            Ok(ConnectionHandle(1))
        }
        fn disconnect(&self, _c: ConnectionHandle) -> Result<()> { Ok(()) }
        fn is_alive(&self, _c: ConnectionHandle) -> Result<bool> { Ok(true) }
        fn prepare(&self, _c: ConnectionHandle, _s: &str) -> Result<StatementHandle> {
            Ok(StatementHandle(1))
        }
        fn bind_params(&self, _s: StatementHandle, _p: &[Value]) -> Result<()> { Ok(()) }
        fn execute(&self, _s: StatementHandle) -> Result<i64> { Ok(0) }
        fn execute_batch(&self, _s: StatementHandle, _r: &[Vec<Value>]) -> Result<i64> { Ok(0) }
        fn column_count(&self, _s: StatementHandle) -> Result<usize> { Ok(0) }
        fn column_name(&self, _s: StatementHandle, _i: usize) -> Result<String> { Ok(String::new()) }
        fn fetch(&self, _s: StatementHandle) -> Result<bool> { Ok(false) }
        fn get_column(&self, _s: StatementHandle, _i: usize) -> Result<Value> {
            Err(Error::new(ErrorCode::Mapping, "none"))
        }
        fn finalize(&self, _s: StatementHandle) -> Result<()> { Ok(()) }
        fn close_cursor(&self, _s: StatementHandle) -> Result<()> { Ok(()) }
        fn set_auto_commit(&self, _c: ConnectionHandle, _e: bool) -> Result<()> { Ok(()) }
        fn commit(&self, _c: ConnectionHandle) -> Result<()> { Ok(()) }
        fn rollback(&self, _c: ConnectionHandle) -> Result<()> { Ok(()) }
    }

    fn params() -> ConnectionParams {
        ConnectionParams { connection_string: "loom".into() }
    }

    // A deterministic base config: no maintenance thread, zero backoff, one slot.
    fn cfg(max: usize) -> PoolConfig {
        PoolConfig {
            min: 0,
            max,
            acquire_timeout: Duration::from_millis(1),
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
        }
    }

    // Two threads contend for a single connection. Whichever acquires first
    // releases it; the other must eventually get it or time out — never a lost
    // wakeup, a double-hand-out, or a deadlock. loom explores every interleaving.
    #[test]
    fn acquire_release_handoff_has_no_lost_wakeup() {
        loom::model(|| {
            let driver = std::sync::Arc::new(LoomFake);
            let pool = LoomArc::new(ConnectionPool::create(driver, params(), cfg(1)).unwrap());

            let p2 = LoomArc::clone(&pool);
            let t = loom::thread::spawn(move || {
                if let Ok(lease) = p2.acquire() {
                    drop(lease); // hand it back
                }
            });

            if let Ok(lease) = pool.acquire() {
                drop(lease);
            }
            t.join().unwrap();

            // Both leases returned or timed out: at rest nothing is leased.
            assert_eq!(pool.active_count(), 0);
        });
    }

    // Dropping the pool while a second thread races an acquire/release must tear
    // down cleanly with no deadlock (the shutdown path notifies waiters).
    #[test]
    fn shutdown_races_acquire_cleanly() {
        loom::model(|| {
            let driver = std::sync::Arc::new(LoomFake);
            let pool = ConnectionPool::create(driver, params(), cfg(1)).unwrap();
            let pool = LoomArc::new(pool);

            let p2 = LoomArc::clone(&pool);
            let t = loom::thread::spawn(move || {
                if let Ok(lease) = p2.acquire() {
                    drop(lease);
                }
            });
            t.join().unwrap();
            // `pool` drops when the last LoomArc goes: teardown must not hang.
            drop(pool);
        });
    }
}
```

> **Risk / mitigation:** `loom::sync::Condvar::wait_timeout` must exist for `acquire`'s timeout path to compile under loom (loom 0.7 provides it). If a future loom drop the method, keep the two models above (which exercise the handoff and shutdown, not a real timeout) and set `acquire_timeout` large so the timeout branch is rarely taken. The maintenance thread is already cfg-gated out under loom (Task 1, Step 6), so its `wait_timeout_while` never needs a loom equivalent.

- [ ] **Step 2: Run the loom models**

Run:
```bash
RUSTFLAGS="--cfg loom" LOOM_MAX_PREEMPTIONS=3 \
  cargo test -p halcyon --no-default-features --lib loom_tests -- --nocapture
```
Expected: PASS. loom explores thousands of interleavings per model; runtime is seconds to a couple of minutes. If it reports a failing interleaving, that is a real pool bug — do NOT weaken the model; fix `pool.rs` (systematic-debugging skill) and re-run.

- [ ] **Step 3: Confirm the normal build still ignores loom**

Run: `cargo test -p halcyon --no-default-features`
Expected: PASS — `loom_tests` is `#[cfg(all(test, loom))]`, so it does not compile here.

- [ ] **Step 4: Commit**

```bash
git add halcyon/src/pool.rs
git commit -m "test(pool): loom models of the acquire/release handoff and shutdown"
```

---

## Task 8: criterion micro-benchmarks

**Files:**
- Create: `halcyon/benches/pool_bench.rs`
- Modify: `halcyon/Cargo.toml` (criterion dev-dep + `[[bench]]` with `required-features`)

- [ ] **Step 1: Add criterion + the bench target to `halcyon/Cargo.toml`**

In `[dev-dependencies]`:

```toml
# Micro-benchmarks (benches/pool_bench.rs); default-features off keeps plotters/
# rayon and their heavier deps out (and off the MSRV path), but `cargo_bench_support`
# stays on so `harness = false` benches still parse `cargo bench` args. The bench
# target is gated on `stress` via required-features, so a normal `cargo test`/MSRV
# build never compiles it.
criterion = { version = "0.5", default-features = false, features = ["cargo_bench_support"] }
```

At the end of the file:

```toml
[[bench]]
name = "pool_bench"
harness = false
required-features = ["stress"]
```

- [ ] **Step 2: Write `halcyon/benches/pool_bench.rs`**

```rust
//! Criterion micro-benchmarks: pooled acquire/release round-trip and a single
//! scalar query on the fake. Run with:
//!   cargo bench -p halcyon --no-default-features --features stress
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

use halcyon::driver::ConnectionParams;
use halcyon::retry::BackoffPolicy;
use halcyon::stress::ConcurrentFakeDriver;
use halcyon::{Database, PoolConfig};

fn db() -> Database {
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
    };
    Database::open(
        Arc::new(ConcurrentFakeDriver::new()),
        ConnectionParams { connection_string: "dsn".into() },
        cfg,
    )
    .unwrap()
}

fn bench_acquire_release(c: &mut Criterion) {
    let db = db();
    c.bench_function("pool_acquire_release", |b| {
        b.iter(|| {
            let lease = db.pool().acquire().unwrap();
            std::hint::black_box(&lease);
        });
    });
}

fn bench_scalar_query(c: &mut Criterion) {
    let db = db();
    c.bench_function("query_scalar", |b| {
        b.iter(|| {
            let rows: Vec<(i64,)> =
                db.query_as("SELECT 7 FROM SYSIBM.SYSDUMMY1", ()).unwrap();
            std::hint::black_box(rows);
        });
    });
}

criterion_group!(benches, bench_acquire_release, bench_scalar_query);
criterion_main!(benches);
```

- [ ] **Step 3: Verify it compiles and runs a quick pass**

Run: `cargo bench -p halcyon --no-default-features --features stress -- --warm-up-time 1 --measurement-time 2`
Expected: builds; prints `pool_acquire_release` and `query_scalar` timings.

- [ ] **Step 4: Verify the MSRV/default path does NOT build criterion**

Run: `cargo build -p halcyon --no-default-features --features derive`
Expected: PASS and fast — the bench target is skipped (no `stress`), so criterion is never compiled.

- [ ] **Step 5: Commit**

```bash
git add halcyon/Cargo.toml halcyon/benches/pool_bench.rs
git commit -m "bench(pool): criterion micro-benchmarks for acquire/release and scalar query"
```

---

## Task 9: custom concurrent throughput sweep

**Files:**
- Create: `halcyon/examples/throughput.rs`
- Modify: `halcyon/Cargo.toml` (`[[example]]` with `required-features`)

- [ ] **Step 1: Add the example target to `halcyon/Cargo.toml`**

```toml
[[example]]
name = "throughput"
required-features = ["stress"]
```

- [ ] **Step 2: Write `halcyon/examples/throughput.rs`**

```rust
//! Concurrent pool throughput sweep. Reuses the stress runner in duration mode,
//! sweeping pool_max against a fixed contending-thread count, printing throughput
//! and p50/p95/p99. Deterministic (fake driver), no Db2. Run with:
//!   cargo run --release -p halcyon --no-default-features --features stress --example throughput
use std::sync::Arc;
use std::time::Duration;

use halcyon::driver::ConnectionParams;
use halcyon::stress::{
    config_for, make_pool_contention, run_workload, ConcurrentFakeDriver, RunConfig, Scenario, Stop,
};
use halcyon::Database;

fn main() {
    let threads = 32;
    let duration = Duration::from_millis(500);
    println!(
        "{:>8}  {:>12}  {:>8}  {:>8}  {:>8}  {:>8}",
        "pool_max", "throughput/s", "p50_us", "p95_us", "p99_us", "err_rate"
    );
    for pool_max in [1usize, 2, 4, 8, 16] {
        let fake = Arc::new(ConcurrentFakeDriver::new());
        let db = Database::open(
            fake,
            ConnectionParams { connection_string: "dsn".into() },
            config_for(Scenario::Pool, pool_max),
        )
        .unwrap();

        let body = make_pool_contention(&db);
        let cfg = RunConfig {
            threads,
            stop: Stop::Duration(duration),
            warmup: Some(Stop::Duration(Duration::from_millis(100))),
            seed: 1,
        };
        let r = run_workload("pool", &cfg, body);

        println!(
            "{:>8}  {:>12.0}  {:>8.1}  {:>8.1}  {:>8.1}  {:>8.4}",
            pool_max,
            r.throughput_per_sec(),
            r.p50_us(),
            r.p95_us(),
            r.p99_us(),
            r.error_rate()
        );
    }
}
```

- [ ] **Step 3: Run it**

Run: `cargo run -p halcyon --no-default-features --features stress --example throughput`
Expected: prints one row per `pool_max` with increasing throughput as the pool grows (until contention on the fake's single mutex dominates).

- [ ] **Step 4: Commit**

```bash
git add halcyon/Cargo.toml halcyon/examples/throughput.rs
git commit -m "bench(pool): concurrent throughput sweep example over the stress runner"
```

---

## Task 10: live batch-insert benchmark (DSN-gated)

**Files:**
- Create: `halcyon/examples/batch_bench.rs`
- Modify: `halcyon/Cargo.toml` (`[[example]]` with `required-features = ["db2"]`)

- [ ] **Step 1: Add the example target to `halcyon/Cargo.toml`**

```toml
[[example]]
name = "batch_bench"
required-features = ["db2"]
```

- [ ] **Step 2: Write `halcyon/examples/batch_bench.rs`** (port of `../Halcyon/tests/stress/perf/batch_insert_bench.cpp`)

```rust
//! Live batch-insert throughput benchmark. Requires HALCYON_TEST_DSN; a no-op
//! (exit 0) when unset. Times inserting N rows three ways and prints rows/sec:
//!   1) per-row execute() under autocommit
//!   2) execute_batch (array binding) under autocommit
//!   3) execute_batch wrapped in a transaction (one commit)
//! Run with:
//!   cargo run --release -p halcyon --example batch_bench
use std::time::Instant;

use halcyon::{Database, Value};

fn recreate(db: &Database) {
    let _ = db.execute("DROP TABLE halcyon_bench", ());
    db.execute(
        "CREATE TABLE halcyon_bench(id BIGINT NOT NULL, payload VARCHAR(64))",
        (),
    )
    .expect("create table");
}

fn main() {
    let dsn = match std::env::var("HALCYON_TEST_DSN") {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("HALCYON_TEST_DSN not set; skipping benchmark");
            return;
        }
    };
    let db = Database::connect(&dsn).expect("connect");
    let n: i64 = 50_000;
    let payload = "x".repeat(48);
    let sql = "INSERT INTO halcyon_bench(id,payload) VALUES (?,?)";

    let rows: Vec<Vec<Value>> = (0..n)
        .map(|i| vec![Value::Int(i), Value::Text(payload.clone())])
        .collect();

    // 1) per-row execute under autocommit
    recreate(&db);
    let t0 = Instant::now();
    for i in 0..n {
        db.execute(sql, (i, payload.clone())).expect("insert");
    }
    let per_row = t0.elapsed().as_secs_f64();

    // 2) array binding under autocommit
    recreate(&db);
    let t0 = Instant::now();
    db.execute_batch(sql, &rows).expect("batch");
    let array = t0.elapsed().as_secs_f64();

    // 3) array binding inside one transaction
    recreate(&db);
    let t0 = Instant::now();
    {
        let mut tx = db.begin().expect("begin");
        tx.execute_batch(sql, &rows).expect("tx batch");
        tx.commit().expect("commit");
    }
    let array_txn = t0.elapsed().as_secs_f64();

    let _ = db.execute("DROP TABLE halcyon_bench", ());

    let rps = |s: f64| if s > 0.0 { n as f64 / s } else { 0.0 };
    println!("rows={n}");
    println!("per_row_autocommit : {per_row:.3}s  {:.0} rows/s", rps(per_row));
    println!("array_autocommit   : {array:.3}s  {:.0} rows/s", rps(array));
    println!("array_transaction  : {array_txn:.3}s  {:.0} rows/s", rps(array_txn));
}
```

- [ ] **Step 3: Verify it compiles (no DSN needed)**

Run: `cargo build -p halcyon --example batch_bench` (needs clidriver via `DB2_CLIDRIVER_ROOT`)
Expected: builds. Running without `HALCYON_TEST_DSN` prints the skip message and exits 0.

- [ ] **Step 4: Verify the pure-Rust path ignores it**

Run: `cargo build -p halcyon --no-default-features --features derive`
Expected: PASS — the example is `required-features = ["db2"]`, so it is skipped.

- [ ] **Step 5: Commit**

```bash
git add halcyon/Cargo.toml halcyon/examples/batch_bench.rs
git commit -m "bench(live): DSN-gated batch-insert throughput example"
```

---

## Task 11: CI jobs (stress, stress-tsan, loom)

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add three jobs after the `msrv` job (before `db2-linux`) in `.github/workflows/ci.yml`**

```yaml
  # ---------------------------------------------------------------------------
  # Concurrency stress scenarios on stable (fast feedback). Pure-Rust: the fake
  # driver needs no clidriver. Also runs the stress module's own unit tests.
  # ---------------------------------------------------------------------------
  stress:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - name: Stress scenarios + unit tests
        run: |
          cargo test -p halcyon --no-default-features --features stress --lib
          cargo test -p halcyon --no-default-features --features stress --test stress

  # ---------------------------------------------------------------------------
  # The same scenarios under ThreadSanitizer on nightly — proves race/deadlock
  # freedom on interleavings the deterministic runs don't reach. Pure-Rust, so
  # no clidriver. -Zbuild-std rebuilds std with the sanitizer.
  # ---------------------------------------------------------------------------
  stress-tsan:
    runs-on: ubuntu-24.04
    env:
      RUSTFLAGS: "-Zsanitizer=thread"
      RUST_TEST_THREADS: "1"
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@nightly
        with:
          components: rust-src
          targets: x86_64-unknown-linux-gnu
      - uses: Swatinem/rust-cache@v2
      - name: Stress scenarios under TSan
        run: |
          cargo test -Zbuild-std --target x86_64-unknown-linux-gnu \
            -p halcyon --no-default-features --features stress --test stress

  # ---------------------------------------------------------------------------
  # loom model-checking of the pool's Mutex/Condvar handoff. `--cfg loom` swaps
  # in loom's instrumented primitives (see src/sync.rs); the std unit tests are
  # excluded under loom, so only the loom models run.
  # ---------------------------------------------------------------------------
  loom:
    runs-on: ubuntu-24.04
    env:
      RUSTFLAGS: "--cfg loom"
      LOOM_MAX_PREEMPTIONS: "3"
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - name: loom models
        run: cargo test -p halcyon --no-default-features --lib loom_tests
```

- [ ] **Step 2: Validate the workflow YAML locally**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Sanity-check each job's command locally where possible**

Run (stable stress): `cargo test -p halcyon --no-default-features --features stress --lib && cargo test -p halcyon --no-default-features --features stress --test stress`
Expected: PASS.

Run (loom): `RUSTFLAGS="--cfg loom" LOOM_MAX_PREEMPTIONS=3 cargo test -p halcyon --no-default-features --lib loom_tests`
Expected: PASS.

(The TSan job needs nightly + `rust-src`; run it if a nightly toolchain is available, otherwise rely on CI.)

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add stress, stress-tsan (nightly), and loom jobs"
```

---

## Task 12: flip the roadmap — Phase 4 accepted

**Files:**
- Modify: `README.md` (status paragraph + roadmap checkboxes)

- [ ] **Step 1: Update the status blurb in `README.md`**

Change the Status sentence (around line 5) that currently says the concurrency track "is still outstanding". Replace:

```
The pool's concurrency-correctness track (loom + ThreadSanitizer stress + perf) that *gates* Phase 4 acceptance is still outstanding, and the per-connection statement cache is next.
```

with:

```
The pool's concurrency-correctness track — `loom` model-checking of the lock/condvar handoff, a `ConcurrentFakeDriver` stress harness under ThreadSanitizer, and criterion + throughput perf harnesses — is now in place, so Phase 4 is accepted. The per-connection statement cache (Phase 5) is next.
```

- [ ] **Step 2: Tick the Phase 4 roadmap item (around line 86)**

Change the `- [ ] Pool: …` bullet's leading `- [ ]` to `- [x]`, and trim its trailing "**Not yet accepted:** …" clause to a past-tense "accepted via the loom + TSan + perf track (`halcyon::stress`, `tests/stress.rs`, `benches/`, `examples/throughput.rs`)."

- [ ] **Step 3: Add a roadmap sub-bullet recording the track (after the Pool bullet)**

```
- [x] Concurrency-correctness gate (accepts Phase 4): `loom` models of the pool handoff (`src/pool.rs` under `--cfg loom`), the `ConcurrentFakeDriver` + workload-runner scenarios ([tests/stress.rs](halcyon/tests/stress.rs)) run under ThreadSanitizer, and perf harnesses (criterion [benches/pool_bench.rs](halcyon/benches/pool_bench.rs) + the [throughput](halcyon/examples/throughput.rs) sweep + the live [batch_bench](halcyon/examples/batch_bench.rs)); wired into CI's `stress`, `stress-tsan`, and `loom` jobs
```

- [ ] **Step 4: Verify the whole suite is green**

Run:
```bash
cargo test -p halcyon --no-default-features --features derive
cargo test -p halcyon --no-default-features --features stress --lib --test stress
RUSTFLAGS="--cfg loom" cargo test -p halcyon --no-default-features --lib loom_tests
cargo clippy -p halcyon --no-default-features --features stress --all-targets -- -D warnings
cargo fmt --all --check
```
Expected: all PASS / no warnings.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: mark Phase 4 accepted (loom + TSan stress + perf landed)"
```

---

## Self-review notes (author checklist — completed)

- **Spec coverage:** sync shim (Task 1) ↔ spec §4.1; ConcurrentFakeDriver (Task 2) ↔ §4.2; histogram (Task 3) + runner (Task 4) ↔ §4.3; four scenarios (Tasks 5–6) ↔ §4.4; loom models (Task 7) ↔ §4.5; criterion + throughput + live bench (Tasks 8–10) ↔ §4.6; CI (Task 11) ↔ §4.7; acceptance/README (Task 12) ↔ §7. Deferred cache/async scenarios (§6) are intentionally absent.
- **Type consistency:** `ConcurrentFakeDriver` fault knobs are `pub AtomicI64` set via `.store(..., Relaxed)` in Task 6 (matches Task 2). Counter accessors return `i64`; scenario/test assertions compare against `i64` / cast `r.ops as i64`. `run_workload(name, &cfg, body)` signature and `RunConfig { threads, stop, warmup, seed }` are identical across Tasks 4/6/8/9. `Stop::{Iters(u64), Duration(Duration)}` consistent. `config_for(Scenario, usize)`, `select_n(i64)`, `make_pool_contention(&Database)`, `make_reconnect_faults(&Database, &AtomicI64)`, `make_txn_churn(&Database)` consistent across Tasks 5/6/9.
- **Placeholder scan:** no TBD/TODO; every code step is complete and compilable.
- **Known risk (flagged in Task 7):** `loom::sync::Condvar::wait_timeout` availability — mitigation documented inline.
