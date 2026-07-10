# Phase 7 Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Feature-gated (`observability`) instrumentation that emits the C++ Halcyon metric/span catalog through process-global `tracing` and `metrics` facades on the sync facade/pool/cache path and across async `spawn_blocking`.

**Architecture:** A central `obs` module holds C++-parity helpers (`op_label`, `code_label`, `instrument_query`, `emit_retry`, pool flush, cache emit). Call sites in `facade`, `pool`, `stmt_cache`, and `async_db` wrap existing work; no `PoolConfig` sinks and no in-tree exporters. Async captures the current `tracing` span **and** `Dispatch`, then enters/sets them inside the blocking closure so parentage works with both global and thread-local subscribers.

**Tech Stack:** Rust 2021 (MSRV 1.75), `tracing` 0.1, `metrics` 0.24, in-tree recording subscriber/recorder for tests (no `tracing-subscriber`/`metrics-util` hard requirement — keeps the feature lean).

**Design reference:** [docs/superpowers/specs/2026-07-11-phase7-observability-design.md](../specs/2026-07-11-phase7-observability-design.md); C++ `../Halcyon/include/halcyon/observability/*`, `../Halcyon/docs/guide/observability.md`, `../Halcyon/tests/unit/test_observability.cpp`.

---

## File Structure

- **Create** `halcyon/src/obs/mod.rs` — feature-gated module: labels, `instrument_query`, `emit_retry`, pool/cache helpers; `#[cfg(test)]` recording harness.
- **Create** `halcyon/src/obs/labels.rs` — `op_label` / `code_label` (pure; always compiled under the feature).
- **Create** `halcyon/src/obs/instrument.rs` — query/execute/transaction instrumentation wrappers.
- **Create** `halcyon/src/obs/record.rs` — `#[cfg(test)]` `RecordingSubscriber` + `RecordingRecorder` + helpers (`with_obs`, count/find).
- **Modify** `halcyon/Cargo.toml` — `observability` feature + optional `tracing` / `metrics`.
- **Modify** `halcyon/src/lib.rs` — `mod obs` under the feature; status docs.
- **Modify** `halcyon/src/facade.rs` — wrap execute/query/query_as/execute_batch/transaction/retry; instrument `ScopedTransaction` methods.
- **Modify** `halcyon/src/pool.rs` — acquire span + pending metrics flushed after unlock; reconnect counter/span.
- **Modify** `halcyon/src/core/stmt_cache.rs` — hit/miss/evict/overflow + size gauge.
- **Modify** `halcyon/src/async_db.rs` — propagate `Dispatch` + `Span` in `run_blocking*`.
- **Modify** `.github/workflows/ci.yml` — clippy/test lanes for `observability` and `observability,tokio`.
- **Modify** `README.md` — roadmap, status, feature table.

**Stub when feature off:** call sites use `#[cfg(feature = "observability")]` blocks or thin `obs` stubs that are no-ops — prefer cfg at the call site around a single helper invocation so the hot path stays readable.

---

## Task 1: Feature, deps, `op_label` / `code_label`

**Files:**
- Modify: `halcyon/Cargo.toml`
- Modify: `halcyon/src/lib.rs`
- Create: `halcyon/src/obs/mod.rs`
- Create: `halcyon/src/obs/labels.rs`

- [ ] **Step 1: Write failing label tests in `halcyon/src/obs/labels.rs` (module will not exist yet — create file with tests + empty fns that `todo!()`, or add tests that call `crate::obs::op_label` after scaffolding in Step 3)**

Create `halcyon/src/obs/labels.rs`:

```rust
//! Bounded label helpers — port of C++ `detail::obs::op_label` / `code_label`.

use crate::error::ErrorCode;

/// Leading SQL verb → stable `{op}` label (lowercase, fixed set).
pub(crate) fn op_label(sql: &str) -> &'static str {
    let tok = leading_word(sql);
    match tok {
        s if eq_ignore_ascii_case(s, "SELECT") => "select",
        s if eq_ignore_ascii_case(s, "INSERT") => "insert",
        s if eq_ignore_ascii_case(s, "UPDATE") => "update",
        s if eq_ignore_ascii_case(s, "DELETE") => "delete",
        s if eq_ignore_ascii_case(s, "MERGE") => "merge",
        s if eq_ignore_ascii_case(s, "WITH") => "with",
        s if eq_ignore_ascii_case(s, "VALUES") => "values",
        _ => "other",
    }
}

/// Lowercased [`ErrorCode`] for `halcyon_errors_total{code}`.
pub(crate) fn code_label(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::Connection => "connection",
        ErrorCode::Timeout => "timeout",
        ErrorCode::Constraint => "constraint",
        ErrorCode::Syntax => "syntax",
        ErrorCode::Deadlock => "deadlock",
        ErrorCode::Transient => "transient",
        ErrorCode::Mapping => "mapping",
        ErrorCode::Pool => "pool",
        ErrorCode::Unknown => "unknown",
    }
}

fn leading_word(sql: &str) -> &str {
    let s = sql.trim_start();
    let end = s
        .char_indices()
        .find(|(_, c)| !c.is_ascii_alphabetic())
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    &s[..end]
}

fn eq_ignore_ascii_case(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .all(|(x, y)| x.to_ascii_uppercase() == y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_label_classifies_verbs() {
        assert_eq!(op_label("  INSERT INTO t VALUES (?)"), "insert");
        assert_eq!(op_label("select 1"), "select");
        assert_eq!(op_label("WITH cte AS (SELECT 1) SELECT 1"), "with");
        assert_eq!(op_label("EXPLAIN SELECT 1"), "other");
    }

    #[test]
    fn code_label_is_lowercase() {
        assert_eq!(code_label(ErrorCode::Syntax), "syntax");
        assert_eq!(code_label(ErrorCode::Deadlock), "deadlock");
    }
}
```

- [ ] **Step 2: Add the feature and optional deps to `halcyon/Cargo.toml`**

In `[features]`, after the `tokio = ...` line:

```toml
# Feature-gated observability: process-global `tracing` spans + `metrics`
# counters/histograms/gauges (C++ Halcyon catalog). Off by default so pure-Rust
# and MSRV lanes never compile these deps. Downstream installs a subscriber /
# recorder (and exporters); Halcyon does not ship Prometheus/OTel adapters.
observability = ["dep:tracing", "dep:metrics"]
```

In `[dependencies]`, after the `tokio = ...` line:

```toml
# Observability facades (feature `observability`). MSRV-friendly: tracing 0.1
# (MSRV 1.65) and metrics 0.24 (MSRV 1.71.1) both build on Halcyon's 1.75.
tracing = { version = "0.1", optional = true, default-features = false, features = ["std"] }
metrics = { version = "0.24", optional = true, default-features = false }
```

Update `[package.metadata.docs.rs]`:

```toml
[package.metadata.docs.rs]
no-default-features = true
features = ["derive", "tokio", "observability"]
```

- [ ] **Step 3: Wire the module in `halcyon/src/lib.rs`**

After `mod facade;`:

```rust
// Feature-gated observability helpers (tracing + metrics). See obs/.
#[cfg(feature = "observability")]
mod obs;
```

Create `halcyon/src/obs/mod.rs`:

```rust
//! Observability helpers — C++ `observability/instrument.hpp` + label helpers.
//!
//! Enabled only with `--features observability`. Emits the Halcyon metric/span
//! catalog via process-global [`tracing`] and [`metrics`] facades.

mod labels;

pub(crate) use labels::{code_label, op_label};
```

- [ ] **Step 4: Run label tests**

Run: `cargo test -p halcyon --no-default-features --features observability --lib obs::labels`

Expected: PASS (2 tests).

Also run: `cargo test -p halcyon --no-default-features --features derive`  
Expected: PASS (observability off; `obs` module absent).

- [ ] **Step 5: Commit**

```bash
git add halcyon/Cargo.toml halcyon/src/lib.rs halcyon/src/obs/
git commit -m "feat(obs): scaffold observability feature and SQL/error labels"
```

---

## Task 2: In-tree recording test harness

**Files:**
- Create: `halcyon/src/obs/record.rs`
- Modify: `halcyon/src/obs/mod.rs`

- [ ] **Step 1: Implement `RecordingRecorder` + `RecordingSubscriber` under `#[cfg(test)]`**

Create `halcyon/src/obs/record.rs` with:

- `MetricSample { name, value, labels: Vec<(String,String)>, kind: Counter|Histogram|Gauge }`
- `SpanRecord { name, attrs, errored, id, parent_id: Option<u64> }`
- `RecordingRecorder(Arc<Mutex<Vec<MetricSample>>>)` implementing `metrics::Recorder` (register_* returning no-op handles that push on increment/record/set — use `metrics::CounterFn`/`HistogramFn`/`GaugeFn` via `Arc` wrappers, or the 0.24 `Recorder` API with `register_counter` returning `Counter::from_arc(...)`).
- `RecordingSubscriber` implementing `tracing::Subscriber` that records `new_span` metadata name + attributes from `record`, tracks current span stack per-thread for parentage, marks errored on `Event` with `error` level or via a recorded field `error.sqlstate` / explicit test helper that inspects span status — **prefer:** store attrs from `record` visits; set `errored = true` when the instrument helper records an `error.sqlstate` attribute (instrument will set that field on failure).
- Helpers:
  - `fn with_metrics<R>(f: impl FnOnce(&RecordingRecorder) -> R) -> R` using `metrics::with_local_recorder`
  - `fn with_tracing<R>(f: impl FnOnce(Arc<Mutex<Vec<SpanRecord>>>) -> R) -> R` using `tracing::subscriber::with_default`
  - `fn count(samples, name, labels) -> usize`
  - `fn find_span(spans, name) -> Option<SpanRecord>`
  - `fn find_child(spans, name, parent_id) -> Option<SpanRecord>`
  - `static OBS_LOCK: Mutex<()>` — take at the start of every observability test that installs globals (async tests)

**metrics 0.24 note:** implement `Recorder` with `describe_*` no-ops and `register_counter/gauge/histogram` returning handles backed by the shared `Arc<Mutex<Vec<_>>>`. On each `increment`/`set`/`record`, push a `MetricSample`.

**tracing note:** a minimal `Subscriber` needs `enabled` → true, `new_span` → allocate id + capture name from metadata, `record` → visit string fields into attrs, `enter`/`exit` → thread-local stack, `event` → ignore (or mark error). Parent = current stack top at `new_span` time.

- [ ] **Step 2: Export from `obs/mod.rs`**

```rust
#[cfg(test)]
mod record;
#[cfg(test)]
pub(crate) use record::*;
```

- [ ] **Step 3: Smoke-test the harness**

Add in `record.rs`:

```rust
#[test]
fn recorder_captures_counter() {
    let _g = OBS_LOCK.lock().unwrap();
    with_metrics(|rec| {
        metrics::counter!("halcyon_queries_total", "op" => "insert", "status" => "ok").increment(1);
        assert_eq!(count(&rec.snapshot(), "halcyon_queries_total", &[("op","insert"),("status","ok")]), 1);
    });
}
```

Run: `cargo test -p halcyon --no-default-features --features observability --lib obs::record`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add halcyon/src/obs/
git commit -m "test(obs): in-tree recording metrics recorder and tracing subscriber"
```

---

## Task 3: `instrument_query` + facade execute / query_as

**Files:**
- Create: `halcyon/src/obs/instrument.rs`
- Modify: `halcyon/src/obs/mod.rs`
- Modify: `halcyon/src/facade.rs`

- [ ] **Step 1: Write failing facade observability tests**

Add `halcyon/src/obs/tests_facade.rs` (or `#[cfg(test)] mod tests` in `instrument.rs` that builds a `Database` over `MockCliDriver`) — prefer tests living in `obs/instrument.rs` `#[cfg(test)]` that use `crate::facade::Database` + `crate::testing::MockCliDriver` so they fail until facade is wired:

```rust
#[test]
fn execute_emits_queries_total_and_duration() {
    let _g = OBS_LOCK.lock().unwrap();
    let mock = Arc::new(MockCliDriver::new());
    mock.exec_row_counts.lock().unwrap().push(1); // match existing mock API field names
    let db = Database::open(mock, params(), config()).unwrap();
    with_metrics(|rec| {
        with_tracing(|spans| {
            assert_eq!(db.execute("INSERT INTO t VALUES (?)", (1,)).unwrap(), 1);
            let samples = rec.snapshot();
            assert_eq!(count(&samples, "halcyon_queries_total", &[("op","insert"),("status","ok")]), 1);
            assert_eq!(count(&samples, "halcyon_query_duration_seconds", &[("op","insert")]), 1);
            let exec = find_span(&spans.lock().unwrap(), "halcyon.execute").expect("span");
            assert_eq!(exec.attrs.get("db.system").map(String::as_str), Some("db2"));
            assert_eq!(exec.attrs.get("db.statement").map(String::as_str), Some("INSERT INTO t VALUES (?)"));
            assert_eq!(exec.attrs.get("db.rows_affected").map(String::as_str), Some("1"));
        });
    });
}
```

**Adapt** field/method names to the real `MockCliDriver` / `Database::open` / `PoolConfig` helpers already used in `facade.rs` tests (copy `params()` / `config()` from there). Do **not** invent a different mock API.

- [ ] **Step 2: Run tests — expect FAIL**

Run: `cargo test -p halcyon --no-default-features --features observability,derive --lib execute_emits_queries_total`

Expected: FAIL (no metrics/spans yet, or compile error if test references missing helpers — wire helpers next).

- [ ] **Step 3: Implement `instrument_query` in `obs/instrument.rs`**

```rust
use crate::error::{ErrorCode, Result};
use crate::obs::labels::{code_label, op_label};
use std::time::Instant;

/// Wraps one logical query/execute: span + duration + queries_total + errors_total.
/// `span_name` is `"halcyon.query"` or `"halcyon.execute"`.
pub(crate) fn instrument_query<T>(
    span_name: &'static str,
    sql: &str,
    f: impl FnOnce() -> Result<T>,
) -> Result<T>
where
    T: RowsAffected,
{
    let op = op_label(sql);
    let span = tracing::info_span!(
        span_name,
        db.system = "db2",
        db.statement = sql,
        db.rows_affected = tracing::field::Empty,
        error.sqlstate = tracing::field::Empty,
    );
    let _guard = span.enter();
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed().as_secs_f64();
    metrics::histogram!("halcyon_query_duration_seconds", "op" => op).record(elapsed);
    match &result {
        Ok(v) => {
            metrics::counter!("halcyon_queries_total", "op" => op, "status" => "ok").increment(1);
            if let Some(n) = v.rows_affected() {
                span.record("db.rows_affected", tracing::field::display(n));
            }
        }
        Err(e) => {
            metrics::counter!("halcyon_queries_total", "op" => op, "status" => "error").increment(1);
            metrics::counter!("halcyon_errors_total", "code" => code_label(e.code)).increment(1);
            if !e.sqlstate.is_empty() {
                span.record("error.sqlstate", e.sqlstate.as_str());
            }
            // RecordingSubscriber treats presence of error.sqlstate as errored;
            // also emit a tracing event at error level for generic subscribers.
            tracing::error!(error.sqlstate = %e.sqlstate, "halcyon operation failed");
        }
    }
    result
}

/// Trait so execute (`i64`) records `db.rows_affected` and query (`Vec`/`QueryResult`) does not.
pub(crate) trait RowsAffected {
    fn rows_affected(&self) -> Option<i64>;
}
impl RowsAffected for i64 {
    fn rows_affected(&self) -> Option<i64> {
        Some(*self)
    }
}
impl<T> RowsAffected for Vec<T> {
    fn rows_affected(&self) -> Option<i64> {
        None
    }
}
impl RowsAffected for crate::facade::QueryResult {
    fn rows_affected(&self) -> Option<i64> {
        None
    }
}
```

**Span name caveat:** `tracing::info_span!(span_name, ...)` requires a string literal for the name in older APIs — if the macro does not accept a runtime/`&'static str` variable, use two wrappers `instrument_execute` / `instrument_select` with literal `"halcyon.execute"` / `"halcyon.query"`, or `tracing::Span::new` via `callsite` — prefer **two thin wrappers** with literal names if the macro complains.

Export from `obs/mod.rs`.

- [ ] **Step 4: Wire `Database::execute` and `query_as` (and `query`)**

In `facade.rs`, change:

```rust
pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<i64> {
    let params = params.into_params();
    let policy = self.default_policy(sql);
    #[cfg(feature = "observability")]
    {
        return crate::obs::instrument_execute(sql, || {
            self.run_with_policy(&policy, |c| c.execute(sql, params.clone()))
        });
    }
    #[cfg(not(feature = "observability"))]
    {
        self.run_with_policy(&policy, |c| c.execute(sql, params.clone()))
    }
}
```

Same pattern for `query` / `query_as` with `instrument_query` / `instrument_select`. Keep `run_with_policy` itself free of query metrics (retries are Task 4).

- [ ] **Step 5: Run tests — expect PASS**

Run: `cargo test -p halcyon --no-default-features --features observability,derive --lib`

Expected: all pass, including the new execute span/metrics test.

Also: `cargo test -p halcyon --no-default-features --features derive`  
Expected: PASS (no observability).

- [ ] **Step 6: Commit**

```bash
git add halcyon/src/obs/ halcyon/src/facade.rs
git commit -m "feat(obs): instrument execute/query with tracing spans and query metrics"
```

---

## Task 4: Error path + retry metrics

**Files:**
- Modify: `halcyon/src/obs/instrument.rs` (add `emit_retry`)
- Modify: `halcyon/src/facade.rs` (`run_with_policy` / `query_with_policy`)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn error_path_emits_errors_total_and_error_status() { /* Syntax error on execute → status=error, errors_total{code=syntax}, span errored */ }

#[test]
fn retries_emit_retried_then_exhausted() {
    // Script 3 retriable Deadlock failures on a SELECT; backoff.max_attempts = 3; sleep no-op.
    // Expect retries_total{retried}=2, {exhausted}=1, queries_total{status=error}=1
}
```

Use the same mock scripting patterns as existing facade retry tests.

- [ ] **Step 2: Run — expect FAIL** (retry counters missing).

- [ ] **Step 3: Add `emit_retry` and wire the retry loops**

```rust
pub(crate) fn emit_retry(outcome: &'static str) {
    metrics::counter!("halcyon_retries_total", "outcome" => outcome).increment(1);
}
```

In `run_with_policy` / `query_with_policy`, when `last.retriable && attempt < policy.max_attempts`:

```rust
#[cfg(feature = "observability")]
crate::obs::emit_retry("retried");
drop(lease);
thread::sleep(...);
```

When returning the final `Err` after `policy.max_attempts > 1` and at least one retry was attempted (i.e. `attempt == max` after prior retried emissions, or simply: if `max_attempts > 1` and this is the failing return path after a retriable error):

```rust
#[cfg(feature = "observability")]
if policy.max_attempts > 1 {
    crate::obs::emit_retry("exhausted");
}
return Err(last);
```

Match C++: exhausted only when the op was under a multi-attempt policy and finally failed (the test with 3 attempts / 3 failures expects exactly one exhausted).

- [ ] **Step 4: Run tests — PASS**

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/obs/ halcyon/src/facade.rs
git commit -m "feat(obs): emit errors_total and retries_total on failure paths"
```

---

## Task 5: Transactions, execute_batch, ScopedTransaction

**Files:**
- Modify: `halcyon/src/obs/instrument.rs`
- Modify: `halcyon/src/facade.rs`

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn transaction_span_parents_acquire() { /* begin/acquire under halcyon.transaction; see Task 6 for acquire — if acquire not yet wired, assert transaction span exists and execute inside txn emits halcyon.execute child */ }

#[test]
fn execute_batch_emits_execute_metrics() { /* op from SQL, status ok */ }

#[test]
fn scoped_transaction_execute_emits_execute_span() {
    // db.transaction(|tx| tx.execute(...)) → halcyon.transaction parent of halcyon.execute
}
```

- [ ] **Step 2: Implement `instrument_transaction`**

```rust
pub(crate) fn instrument_transaction<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    let span = tracing::info_span!("halcyon.transaction", db.system = "db2", error.sqlstate = tracing::field::Empty);
    let _g = span.enter();
    match f() {
        Err(e) => {
            if !e.sqlstate.is_empty() {
                span.record("error.sqlstate", e.sqlstate.as_str());
            }
            tracing::error!(error.sqlstate = %e.sqlstate, "halcyon transaction failed");
            Err(e)
        }
        ok => ok,
    }
}
```

Wrap `Database::transaction` body with it. Wrap `execute_batch` with `instrument_execute`. Wrap `ScopedTransaction::execute` / `query` / `query_as` / `execute_batch` with the same instrument helpers (C++ instruments txn methods too).

- [ ] **Step 3: Run — PASS; commit**

```bash
git add halcyon/src/obs/ halcyon/src/facade.rs
git commit -m "feat(obs): instrument transactions, batch, and ScopedTransaction ops"
```

---

## Task 6: Pool acquire / gauges / reconnect (emit after unlock)

**Files:**
- Modify: `halcyon/src/obs/instrument.rs` (or `obs/pool.rs`)
- Modify: `halcyon/src/pool.rs`

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn pool_gauge_and_acquire_wait_emitted() {
    // min=2 warmup → gauge idle=2; execute → acquire_wait histogram ≥1; acquire span exists
}

#[test]
fn reconnect_emits_reconnects_total() {
    // validate_on_acquire + alive=false → reconnects_total ≥1
}

#[test]
fn sink_may_reenter_pool_without_deadlock() {
    // Custom recorder whose gauge hook calls pool.idle_count()/active_count().
    // Must complete (proves flush after unlock).
}

#[test]
fn query_span_parents_acquire() {
    // find_child(spans, "halcyon.acquire", execute_or_query_id)
}
```

- [ ] **Step 2: Add `PendingMetrics` + flush helpers in `obs`**

```rust
#[derive(Default)]
pub(crate) struct PendingMetrics {
    pub idle: Option<u64>,
    pub active: Option<u64>,
    pub acquire_wait_secs: Option<f64>,
    pub reconnects: u64,
}

pub(crate) fn flush_pool_metrics(pm: &PendingMetrics) {
    if let Some(v) = pm.idle {
        metrics::gauge!("halcyon_pool_connections", "state" => "idle").set(v as f64);
    }
    if let Some(v) = pm.active {
        metrics::gauge!("halcyon_pool_connections", "state" => "active").set(v as f64);
    }
    if let Some(s) = pm.acquire_wait_secs {
        metrics::histogram!("halcyon_pool_acquire_wait_seconds").record(s);
    }
    if pm.reconnects > 0 {
        metrics::counter!("halcyon_reconnects_total").increment(pm.reconnects);
    }
}
```

- [ ] **Step 3: Wire `ConnectionPool::acquire`**

Pattern (match C++):

1. Before taking the mutex (or immediately after entry): start `tracing::info_span!("halcyon.acquire", error.sqlstate = Empty)` and enter it for the duration of the acquire attempt.
2. Under the lock: only fill a `PendingMetrics` (idle/active counts, wait seconds from timer).
3. **After** `drop(guard)` / unlock: call `flush_pool_metrics`.
4. On timeout: record `error.sqlstate` if any, mark error, flush, return Err.
5. In `validated_lease` when reconnecting: bump `pm.reconnects`, and after unlock emit + `tracing::info_span!("halcyon.reconnect").in_scope(|| {})` (or start/end briefly).

Touch every exit path of `acquire` / `validated_lease` / release paths that update idle/active so gauges stay accurate (at least on acquire and release — match C++ flush points; read `pool.hpp` `flush_metrics` call sites and mirror).

**Hard rule:** never call `metrics::*` or start reconnect spans while holding `self.lock()`.

- [ ] **Step 4: Run tests — PASS; commit**

```bash
git add halcyon/src/obs/ halcyon/src/pool.rs
git commit -m "feat(obs): pool acquire spans and metrics flushed after unlock"
```

---

## Task 7: Statement cache metrics

**Files:**
- Modify: `halcyon/src/core/stmt_cache.rs`
- Modify: `halcyon/src/obs/instrument.rs` (cache helpers)

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn statement_cache_hit_and_miss_emit_counters() {
    // PoolConfig.statement_cache_size = 8; execute same SQL twice → miss then hit;
    // stmt_cache_size gauge moves.
}
```

- [ ] **Step 2: Emit from `StatementCache::acquire` / eviction / overflow**

```rust
#[cfg(feature = "observability")]
fn emit_cache(result: &'static str, size: usize) {
    metrics::counter!("halcyon_stmt_cache_total", "result" => result).increment(1);
    metrics::gauge!("halcyon_stmt_cache_size").set(size as f64);
}
```

Call sites in `acquire`:
- capacity == 0 → `overflow` (or skip size — C++ still counts overflow)
- busy hit → `overflow`
- idle hit → `hit`
- miss insert → `miss`
- eviction before insert → `evict` (then miss on the new prepare — match C++ order: emit evict when removing victim, miss when inserting)

Read C++ `statement_cache.hpp` emit points and mirror exactly.

- [ ] **Step 3: Run — PASS; commit**

```bash
git add halcyon/src/core/stmt_cache.rs halcyon/src/obs/
git commit -m "feat(obs): statement-cache hit/miss/evict/overflow metrics"
```

---

## Task 8: Async Dispatch + Span propagation

**Files:**
- Modify: `halcyon/src/async_db.rs`

- [ ] **Step 1: Write failing async parentage tests** (under `#[cfg(all(test, feature = "observability"))]` inside `async_db.rs` or `obs`)

```rust
#[tokio::test]
async fn execute_async_inherits_caller_context() {
    let _g = OBS_LOCK.lock().unwrap();
    // Install RecordingSubscriber as the default for this test via with_default on the
    // async task; run_blocking must propagate Dispatch so the blocking thread records.
    let parent = tracing::info_span!("test.parent");
    let parent_id = /* id from subscriber after enter */;
    with_tracing_async(/* ... */).await; // or manually:
    parent.in_scope(|| ());
    let _e = parent.enter();
    db.execute("INSERT ...", (1,)).await.unwrap();
    let exec = find_span(..., "halcyon.execute").unwrap();
    assert_eq!(exec.parent_id, Some(parent_id));
}
```

Because `with_local_recorder` / `with_default` are thread-local, async tests must either:
1. Propagate `Dispatch` into the blocking thread (production fix below), and use `with_default` on the async side only; or
2. Take `OBS_LOCK` and use a process-global subscriber installed once.

**Require (1) in production code** so (tests + apps using `with_default`) work.

- [ ] **Step 2: Update `run_blocking_with_permit` (and `run_blocking_raw`)**

```rust
async fn run_blocking_with_permit<F, T>(permit: OwnedSemaphorePermit, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "observability")]
    {
        let parent = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(|d| d.clone());
        return join(
            spawn_blocking_checked(move || {
                let _permit = permit;
                let _dispatch = tracing::dispatcher::set_default(&dispatch);
                let _span = parent.entered();
                f()
            })?
            .await,
        );
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
```

Apply the same capture/enter pattern to `run_blocking_raw`.

Update the module docs: remove “Observability is Phase 7 and absent here”; note Dispatch+Span propagation when `observability` is on.

- [ ] **Step 3: Run**

```bash
cargo test -p halcyon --no-default-features --features observability,tokio,derive --lib
```

Expected: PASS including async parentage.

- [ ] **Step 4: Commit**

```bash
git add halcyon/src/async_db.rs halcyon/src/obs/
git commit -m "feat(obs): propagate tracing context across async spawn_blocking"
```

---

## Task 9: CI lanes

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Extend the pure-Rust clippy job**

After the existing tokio clippy lines, add:

```yaml
cargo clippy -p halcyon --no-default-features --features observability --all-targets -- -D warnings
cargo clippy -p halcyon --no-default-features --features observability,derive --all-targets -- -D warnings
cargo clippy -p halcyon --no-default-features --features observability,tokio,derive --all-targets -- -D warnings
```

- [ ] **Step 2: Extend the pure-Rust test job**

```yaml
cargo test -p halcyon --no-default-features --features observability
cargo test -p halcyon --no-default-features --features observability,derive
cargo test -p halcyon --no-default-features --features observability,tokio,derive
```

- [ ] **Step 3: Validate YAML**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('ok')"`  
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: exercise the observability feature (pure lane ± tokio)"
```

---

## Task 10: Docs + Phase 7 acceptance

**Files:**
- Modify: `README.md`
- Modify: `halcyon/src/lib.rs`

- [ ] **Step 1: README status paragraph**

Replace the trailing “Observability (Phase 7) is next.” with a sentence that Phase 7 (`tracing`/`metrics` facades, feature `observability`) is accepted, and that the crates.io release track is next.

- [ ] **Step 2: Roadmap checkbox**

Change:

```markdown
- [ ] Observability: `metrics`/`tracing`-facade adapters (feature-gated)
```

to a checked item summarizing: process-global `tracing`+`metrics`, C++ catalog, sync+async, feature `observability` off by default, mock suite + CI.

- [ ] **Step 3: Feature-flag table row**

```markdown
| `observability` | off | Enables C++-parity `tracing` spans and `metrics` counters/histograms/gauges. Downstream installs a subscriber/recorder (and exporters). Off by default. |
```

- [ ] **Step 4: `lib.rs` status doc-comment**

Mark Phase 7 done; release track next.

- [ ] **Step 5: Verify**

```bash
cargo test -p halcyon --no-default-features --features derive
cargo test -p halcyon --no-default-features --features observability,tokio,derive
cargo clippy -p halcyon --no-default-features --features observability,tokio,derive --all-targets -- -D warnings
cargo doc -p halcyon --no-default-features --features observability,tokio,derive --no-deps
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add README.md halcyon/src/lib.rs
git commit -m "docs: mark Phase 7 (observability) accepted"
```

---

## Spec coverage (self-review)

| Spec requirement | Task |
|---|---|
| Process-global wiring, no PoolConfig sinks | 1, 3–8 |
| Single `observability` feature + tracing/metrics deps | 1 |
| `op_label` / `code_label` | 1 |
| Query/execute spans + query metrics + errors_total | 3–4 |
| Retries retried/exhausted | 4 |
| Transaction + ScopedTransaction + batch | 5 |
| Pool acquire/gauges/reconnect; emit after unlock; reentrancy test | 6 |
| Statement cache metrics | 7 |
| Async Dispatch+Span parentage | 8 |
| CI lanes | 9 |
| README / lib.rs acceptance | 10 |
| Full SQL on `db.statement` | 3 |
| MSRV-friendly pins | 1 |
| In-tree test harness (no forced exporter deps) | 2 |

**Placeholder scan:** none intentional. Mock API field names in Task 3 must be copied from existing `facade.rs` tests at implementation time (do not invent).

**Type consistency:** `instrument_execute` / `instrument_select` / `instrument_transaction` / `emit_retry` / `PendingMetrics` / `flush_pool_metrics` names are stable across tasks.
