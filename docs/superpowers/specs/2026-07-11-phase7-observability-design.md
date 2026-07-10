# Halcyon-rs — Phase 7 Observability — Design

**Date:** 2026-07-11
**Status:** Approved design; implementation plan to follow.
**Scope:** Phase 7 of the production-readiness roadmap: a feature-gated
observability layer that emits the C++ Halcyon metric/span set through
process-global `tracing` and `metrics` facades. Downstream chooses exporters.
No Prometheus/OTel adapters ship in-tree. Reference:
`../Halcyon/include/halcyon/observability/*`,
`../Halcyon/docs/guide/observability.md`,
`../Halcyon/tests/unit/test_observability.cpp`.

## 1. Goal

Instrument the proven sync data path (facade, pool, retry, statement cache) and
the Phase 6 async `spawn_blocking` path so that, when the `observability`
feature is on and a global subscriber/recorder is installed, Halcyon emits the
same spans and metrics as C++ Halcyon. When the feature is off, the layer
compiles out cleanly with zero residual dependency cost. Exit criterion:
mock-driver tests assert the catalog under a recording subscriber/recorder; async
caller-span parentage is proven across `spawn_blocking`; CI exercises the
feature alone and combined with `tokio`.

## 2. Guiding invariants (unchanged from CLAUDE.md)

- **The seam rule.** No new driver entry points and no `unsafe`. Observability
  is pure safe Rust above the seam; `#![deny(unsafe_code)]` stands.
- **No hard dependencies for optional functionality.** Observability is behind a
  new, **off-by-default** `observability` feature. Default builds,
  `--no-default-features`, and the MSRV job never compile `tracing`/`metrics`.
- **Error model.** `Result<T, Error>` only. Instrumentation never converts
  failures into panics, never swallows errors, and never changes retry/pool
  semantics — it only observes.
- **Port semantics, then idiomatize.** Preserve the C++ metric/span names,
  labels, nesting (`halcyon.acquire` under query/execute/txn), and the
  emit-after-pool-unlock discipline. Replace C++ `MetricsSink`/`Tracer` +
  `ObservabilityConfig` with idiomatic process-global facades.

## 3. Decisions made during brainstorming

1. **Next phase = Phase 7** (observability), per the roadmap after Phase 6.
2. **Wiring = process-global only.** Call `tracing` / `metrics` when the feature
   is on. Consumers install a global subscriber/recorder. No
   `PoolConfig.observability`, no custom sink traits, no in-tree adapters.
3. **Single `observability` feature** enabling both `tracing` and `metrics`
   (not separate flags; not always-on macros).
4. **Scope = sync + async.** Instrument facade/pool/retry/cache and weave async
   so caller spans parent `halcyon.*` spans across `spawn_blocking`.
5. **Naming = C++ parity.** Span names `halcyon.query|execute|transaction|
   acquire|reconnect`; metric names/labels match the C++ guide.
6. **`db.statement` = full SQL text** (placeholders only; bound values are not
   interpolated), matching C++. Callers who must not retain SQL filter in their
   subscriber.
7. **Structure = central `obs` helpers** (not scattered macros, not
   `#[tracing::instrument]` alone). Mirrors C++ `instrument.hpp` +
   `Database::instrument`.

## 4. Design

### 4.1 Feature flag & dependencies

- New feature `observability` (off by default):
  `observability = ["dep:tracing", "dep:metrics"]`.
- Optional deps: `tracing` (spans) and `metrics` (counter / histogram / gauge).
  No `opentelemetry`, `prometheus`, or `tracing-opentelemetry` in the library
  crate — downstream wires exporters.
- Pin versions that build on MSRV **1.75**; verify during implementation and
  document any forced bump.
- CI: pure-Rust matrix and clippy gain lanes for `--features observability` and
  `--features observability,tokio` (plus existing derive as needed).
- `package.metadata.docs.rs` features include `observability`.
- README feature-flag table gains an `observability` row.

When the feature is off, call sites resolve to no-op stubs (cfg-gated empty
helpers or `#[cfg]` branches) with no `tracing`/`metrics` symbols in the binary.

### 4.2 `obs` module (`halcyon/src/obs.rs`)

Feature-gated module holding C++-parity helpers. Public surface stays minimal —
helpers are `pub(crate)` unless a test/doc example needs a re-export.

| Helper | Role |
|---|---|
| `op_label(sql)` | Leading SQL verb → `select\|insert\|update\|delete\|merge\|with\|values\|other` (bounded cardinality) |
| `code_label(ErrorCode)` | Lowercase `ErrorCode` label for `errors_total{code}` |
| `instrument_query(span_name, sql, f)` | Span + duration histogram + `queries_total` + optional `errors_total` / error status / `db.rows_affected` |
| `emit_retry(outcome)` | `halcyon_retries_total{outcome=retried\|exhausted}` |
| Pool flush helpers | Acquire span; after unlock: wait histogram, idle/active gauges, reconnect counter/span |
| Cache helpers | `halcyon_stmt_cache_total{result}` + `halcyon_stmt_cache_size` |

`instrument_query` mirrors C++ `Database::instrument`:

1. If feature off → call `f` directly (compile-time elision).
2. Start span `span_name` with attrs `db.system=db2`, `db.statement=<sql>`.
3. Time `f()`.
4. Emit `halcyon_query_duration_seconds{op}` and
   `halcyon_queries_total{op,status}`.
5. On `Err`: `halcyon_errors_total{code}`, mark span error, set
   `error.sqlstate` when available.
6. On `Ok(i64)` execute path: set `db.rows_affected`.
7. Return the original `Result`.

### 4.3 Telemetry catalog (C++ parity)

**Spans**

| Span | Attributes |
|---|---|
| `halcyon.query` | `db.system=db2`, `db.statement`; `error.sqlstate` on failure |
| `halcyon.execute` | `db.system=db2`, `db.statement`, `db.rows_affected` on ok; `error.sqlstate` on failure |
| `halcyon.transaction` | `error.sqlstate` on failure |
| `halcyon.acquire` | Nested under query/execute/txn; `error.sqlstate` on timeout |
| `halcyon.reconnect` | On transparent reconnect; `error.sqlstate` on failure |

**Metrics**

| Metric | Type | Labels |
|---|---|---|
| `halcyon_queries_total` | Counter | `op`, `status` (`ok`/`error`) |
| `halcyon_query_duration_seconds` | Histogram | `op` |
| `halcyon_errors_total` | Counter | `code` |
| `halcyon_retries_total` | Counter | `outcome` (`retried`/`exhausted`) |
| `halcyon_pool_connections` | Gauge | `state` (`idle`/`active`) |
| `halcyon_pool_acquire_wait_seconds` | Histogram | — |
| `halcyon_reconnects_total` | Counter | — |
| `halcyon_stmt_cache_total` | Counter | `result` (`hit`/`miss`/`evict`/`overflow`) |
| `halcyon_stmt_cache_size` | Gauge | — |

### 4.4 Call-site wiring

**`facade.rs`**
- `execute` → `instrument_query("halcyon.execute", …)`
- `query` / `query_as` → `instrument_query("halcyon.query", …)`
- `execute_batch` → treat as execute (same span/metrics; `op` from SQL)
- `transaction` → `halcyon.transaction` span (`db.system=db2`) around the
  whole unit of work (match C++). `begin`/acquire nests under it; execute/
  query on `ScopedTransaction` also go through `instrument_query`, so inner
  ops are children of the transaction span (and still emit query metrics).
- Retry loop → `emit_retry("retried")` before replay, `emit_retry("exhausted")`
  when attempts are spent

**`pool.rs`**
- Start `halcyon.acquire` before waiting; end after handoff (or on timeout with
  error status).
- **Lock discipline (hard rule):** while holding the pool mutex, only record
  *pending* metric values. Emit gauges/histograms/counters and start/end
  reconnect spans **after** the mutex is released — same as C++
  `PendingMetrics` / `flush_metrics`. Prevents re-entrant deadlock if a
  recorder calls back into `idle_count`/`active_count`.

**`core/stmt_cache.rs`**
- On hit / miss / evict / overflow: `halcyon_stmt_cache_total{result=…}`
- On size change: `halcyon_stmt_cache_size`

**No public API additions** for wiring. Enabling the Cargo feature and
installing a global `tracing` subscriber + `metrics` recorder is sufficient.
There is no Halcyon `useParentContext` API — `tracing::Span::current()` is the
parent context.

### 4.5 Async propagation

`AsyncDatabase` already delegates to sync `Database` methods inside
`spawn_blocking`, so sync instrumentation runs on the blocking thread
automatically (metrics and `halcyon.*` spans).

Additional requirement for parentage (C++
`QueryAsyncInheritsCallerContext` / `ExecuteAsyncInheritsCallerContext`):

1. On the async task, capture `let parent = tracing::Span::current();`
2. Move `parent` into the `spawn_blocking` closure.
3. Enter it (`let _g = parent.entered();`) **before** calling the sync method.

That makes `halcyon.execute` / `halcyon.query` children of the caller's span
even though work runs on another thread. Prefer implementing this once in the
shared `run_blocking` helper so every async method inherits it.

Cancellation semantics are unchanged from Phase 6: dropping the future does not
cancel in-flight blocking work; spans end when the sync op completes.

### 4.6 Data flow (example)

`db.execute(sql, params)` with feature on:

1. `instrument_query("halcyon.execute", sql, …)` starts span + timer.
2. Facade acquires a lease → pool emits `halcyon.acquire` (child of execute)
   and records pending wait/gauge samples; flushes after unlock.
3. Statement cache may emit hit/miss/… .
4. Driver execute runs; on retriable read failure, `emit_retry` then replay.
5. On return: duration + `queries_total{status}`; on error also `errors_total`
   and span error status; on ok execute set `db.rows_affected`.
6. Lease drops; pool may refresh gauges after unlock.

Async path: caller span → capture → `spawn_blocking` → enter parent → same
sync flow → await JoinHandle.

## 5. Error handling

- Instrumentation never alters `Result` values or `Error.retriable`.
- A panic in a user transaction closure still rolls back via the existing drop
  guard and propagates; spans should end (Drop of span guards) on unwind.
- Pool acquire timeout still returns `ErrorCode::Pool` (or existing code);
  acquire span is marked errored when a tracer is active.
- Recorder/subscriber panics are out of Halcyon's control; the lock-discipline
  rule still holds so a well-behaved recorder can re-enter pool sizing APIs.

## 6. Testing & acceptance

**Unit (mock driver, no clidriver)** — run under
`cargo test -p halcyon --no-default-features --features observability,derive`
(and a sibling lane with `observability,tokio,derive`):

1. Execute emits `halcyon_queries_total{op=insert,status=ok}` + duration.
2. Error path emits `status=error`, `halcyon_errors_total{code=…}`, errored span.
3. Retries emit `retried` then `exhausted` under a failing read policy.
4. Pool warmup/acquire emits idle/active gauges and acquire-wait histogram.
5. Spans carry `db.system=db2`, `db.statement`, and `db.rows_affected` on
   successful execute; `halcyon.acquire` is a child of query/execute/txn.
6. Reconnect (validate-on-acquire + dead connection) bumps
   `halcyon_reconnects_total`.
7. Statement-cache hit/miss/evict/overflow emit cache counters; size gauge
   moves.
8. Recorder may call `idle_count`/`active_count` from a metric hook without
   deadlock (emit-after-unlock).
9. Async: with a parent span entered on the async task, `halcyon.query` /
   `halcyon.execute` record that parent across `spawn_blocking`.

Implementation detail: use a recording `tracing` `Layer`/`Subscriber` and a
recording `metrics::Recorder` (or `metrics-util` if it satisfies MSRV) installed
for the duration of each test. Prefer scoped install so tests do not race on
process-global state; serialize observability tests with a mutex if the chosen
crates only support a process-global recorder.

**CI** — pure-Rust + clippy lanes for `observability` and
`observability`+`tokio`. Default / no-observability lanes must stay green and
must not link the optional deps.

**Docs** — README roadmap checkbox + status blurb + feature table; `lib.rs`
crate docs mention the feature; short consumer note: install a `tracing`
subscriber and a `metrics` recorder (exporters are the app's concern).

**Live Db2** — not required for Phase 7 acceptance (telemetry is mock-assertable).
Optional: a smoke that runs one live execute under a no-op subscriber to ensure
the feature builds in `live-db2` — nice-to-have, not a gate.

**Exit:** feature off → clean build, no deps; feature on → catalog matches §4.3;
mock suite green including async parentage; CI lanes green; README marks Phase 7
accepted. Release track (crates.io) remains after this phase.

## 7. Out of scope

- `ObservabilityConfig` / per-pool sinks
- In-tree Prometheus or OpenTelemetry adapters
- SQL redaction / statement sampling knobs
- Changing retry, pool, or cache behavior
- Async streaming cursors (still sync-only per Phase 6)
- crates.io publish / CHANGELOG release track (post–Phase 7)
