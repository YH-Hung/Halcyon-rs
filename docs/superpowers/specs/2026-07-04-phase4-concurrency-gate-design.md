# Halcyon-rs — Phase 4 Concurrency-Correctness Gate — Design

**Date:** 2026-07-04
**Status:** Approved design; implementation plan to follow.
**Scope:** The verification track that *accepts* Phase 4. The pool itself
(`ConnectionPool`/`PoolConfig`/`PooledConnection`, backoff, reaping, transparent
reconnect, safe auto-retry) is already implemented and mock-tested; this document
covers only the concurrency/perf work that gates its acceptance.

## 1. Goal

The README lists Phase 4 as *implemented but not accepted*: its exit gate demands
a concurrency-correctness track that does not yet exist in the Rust repo —
`loom` model-checking of the lock/condvar logic, a `ConcurrentFakeDriver` stress
harness under ThreadSanitizer, and a perf harness. This design delivers all three,
Rust-native, and defines the acceptance bar that flips Phase 4 to done.

The C++ reference is `../Halcyon/tests/stress/` (correctness + perf + support) and
`../Halcyon/tests/unit/test_pool*.cpp`. We preserve its **invariants and scenario
semantics**, but implement with Rust-idiomatic tooling (`loom`, ThreadSanitizer,
`criterion`) rather than porting the C++ files 1:1.

## 2. Guiding invariants (unchanged from CLAUDE.md)

- **The seam rule.** Only `halcyon-sys` and `driver/db2.rs` touch the CLI ABI.
  `#![deny(unsafe_code)]` stays; the `sync` shim and all test code are safe Rust.
- **Mock-testability.** The entire track is pure-Rust: it drives the pool through
  the `CliDriver` trait with a new in-crate fake — **no clidriver, no live Db2**.
  `cargo test -p halcyon --no-default-features` must still pass, and the stress /
  loom / criterion targets must build and run without Db2 installed.
- **No runtime cost.** The one production-code change (the `sync` shim) compiles
  straight to `std` in every normal build; `loom` is a `cfg(loom)`-only
  dev-dependency, invisible to released artifacts and to the default build.
- **Port semantics, then idiomatize.** The fake driver's fault/latency knobs,
  error classes (`08003` stale, `40001` transient/retriable), and the scenario
  invariants match the C++ originals; the plumbing is Rust.

## 3. Key architectural insight — loom and TSan cover different halves

`loom` model-checks by exhaustively exploring interleavings of the synchronization
primitives, but it **cannot model real time**: timed `Condvar::wait_timeout`,
`Instant`-based idle/lifetime reaping, and `thread::sleep` backoff are outside the
interleavings it explores, and its thread/branch budget is exponential. So the
gate splits cleanly in two, and neither half alone is sufficient:

- **`loom` — the timeless synchronization core.** Acquire ↔ release ↔ `Condvar`
  notify handoff; the acquire-timeout-returns-`Err(Pool)` path; clean shutdown and
  `Drop` ordering (leases and the pool dropping in any order). Runs with the
  **maintenance thread disabled** and tiny bounds (2–3 loom threads, pool max 1–2).
- **ThreadSanitizer stress — the time-based and end-to-end behavior.** The four
  §4.4 scenarios — pool contention, transient-fault retry + reconnect, transaction
  churn, and the reaper racing acquire/release — with
  real threads, a real clock, the new `ConcurrentFakeDriver`, run under
  `-Zsanitizer=thread` to prove race/deadlock freedom on interleavings loom does
  not reach.

Together they constitute Phase 4 acceptance.

## 4. Components

### 4.1 `sync` shim — the only production-code change

New module `halcyon/src/sync.rs` (~30 lines) re-exporting the synchronization
primitives the pool uses, selected by cfg:

```rust
#[cfg(not(loom))]
pub(crate) use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
};
#[cfg(not(loom))]
pub(crate) use std::thread::JoinHandle;

#[cfg(loom)]
pub(crate) use loom::{
    sync::{Arc, Condvar, Mutex},
    thread,
};
#[cfg(loom)]
pub(crate) use loom::thread::JoinHandle;
```

`pool.rs` swaps `use std::sync::{Arc, Condvar, Mutex};` → `use crate::sync::{...}`
and `use std::thread::{self, JoinHandle};` → the shim. This is the tokio/crossbeam
pattern; the runtime code path is byte-identical to today.

Notes / edge cases the plan must handle:
- `loom::sync::Mutex::lock()` returns a `Result`-like guard that must be unwrapped
  differently than `std`; the shim (or call sites) reconcile the small API deltas.
- Backoff uses `std::thread::sleep` + `Instant`. Loom models never trigger the
  retry/backoff path (no fault injection in the loom scenarios, `base_delay:
  ZERO`), so `sleep`/`Instant` stay on `std` even under `--cfg loom`; only
  `Arc`/`Mutex`/`Condvar`/`thread::spawn`/`JoinHandle` are shimmed.
- The maintenance thread is not spawned in loom scenarios
  (`start_maintenance_thread: false`), sidestepping loom's lack of timed waits.

### 4.2 `ConcurrentFakeDriver` — deterministic, thread-safe `CliDriver`

The existing `MockCliDriver` is FIFO-scripted behind one `Mutex<State>`, so it
cannot serve many threads with no ordering assumptions. We add a new test double
(test-only; not part of the public API) whose every response is **derived from its
inputs**:

- `SELECT <n> FROM …` / `VALUES …` returns a single `int64` column equal to the
  first digit-run in the SQL (`n`); DML returns an affected-row count of 1.
- Atomic counters: `connect/disconnect/prepare/execute/close_cursor/is_alive/
  finalize/commit/rollback/auto_commit_on/auto_commit_off`, plus `in_flight` and
  `peak_in_flight` (CAS-updated on each `execute`).
- Fault knobs (1-in-N, 0 disables): `fail_connect_every`, `fail_execute_every`,
  `kill_connection_every` (kills the underlying connection so the next
  `is_alive`/use fails → forces validate-on-acquire reconnect).
- Latency knobs (`query_latency`, `connect_latency`) to create realistic overlap.
- Handle maps guarded by one `Mutex` (the fake is deliberately a little
  pessimistic; the contention we measure lives in the pool, not here); counters
  are atomics.
- Error classes match the C++ fake: stale handle → `ErrorCode::Connection`/`08003`
  (retriable), injected transient → `ErrorCode::Transient`/`40001` (retriable).

Placement: `halcyon/src/stress/` behind `#[cfg(any(test, feature = "stress"))]`
so it is reusable from in-crate loom/stress tests, integration tests, and the perf
binaries. (Plan step decides `feature = "stress"` vs. a `tests/support/` module;
a feature is preferred because the perf binaries and benches live outside the unit
test cfg.) Small self-tests mirror the C++ `FakeDriverTest` cases
(SQL-encoded value, `fail_execute_every` trips at rate, use-after-disconnect
errors).

### 4.3 Stress runner + histogram

Rust-native equivalents of `workload_runner` + `latency_histogram`:

- **`LatencyHistogram`** — fixed 64-bucket log2 histogram of nanosecond latencies,
  allocation-free, mergeable; `percentile_us(p)` / `max_us()` return bucket upper
  bounds (approximate by design, cheap on the hot path).
- **`WorkerCtx`** — per-worker RNG + histogram (no shared per-op state → no false
  contention), with `fail(msg)` (hard failure, stops the worker) and `note_error`
  (tolerated non-fatal: a pool acquire-timeout under contention, or a genuinely
  exhausted bounded retry).
- **`run_workload`** — spawns `threads` workers, releases them from a barrier
  together, loops the body until the stop condition (`total_iters` for
  deterministic correctness runs, `duration` for perf steady-state), times each
  call into the worker histogram, joins, merges, and surfaces the first failure in
  a `RunReport` (`ops`, `errors`, `throughput_per_sec`, `p50/p95/p99/max`).

### 4.4 Scenarios (ported now)

Driven by the runner against `ConcurrentFakeDriver`, each asserting the C++
invariants. **Four of the six C++ scenarios map onto today's Rust surface;** the
other two are explicitly deferred (§6).

1. **Pool contention** — `threads` ≫ pool `max`; acquire → one scalar query on the
   leased connection → RAII release. Invariants: `peak_in_flight ≤ max` (never more
   leases than the ceiling), no wrong scalar, at rest `active_count == 0` and
   `idle_count == total_count`. A pool acquire-timeout is a tolerated `note_error`,
   not a failure.
2. **Reconnect + transient faults** — driven through the facade so safe-retry is in
   play; `fail_execute_every` + `kill_connection_every` + `validate_on_acquire`.
   Read-only queries transparently retry/reconnect. Invariants: no *non-retriable*
   surfaced error and no wrong scalar (a cross-thread handle/row mixup); reconnects
   and retries actually fired (`connect_calls` grew, `execute_calls > ops`); the
   vast majority recovered with the right value; `active_count == 0` at rest.
   A bounded-retry budget legitimately exhausting → a still-*retriable* error is a
   tolerated `note_error`, not a failure (matches C++ spec §6.4).
3. **Transaction churn** — half the ops commit, half force a rollback via an `Err`
   return. Invariants: `auto_commit_off == iters` (each `begin` flips it),
   `commit + rollback == iters`, `auto_commit_on == iters` (each tx restores it),
   `active_count == 0` (no tx leaks a lease).
4. **Reaper race** — maintenance thread on, tiny `maintenance_interval` /
   `idle_timeout` / `max_lifetime` so the reaper fires constantly while
   acquires/releases race it, duration-mode. Invariants: `peak_in_flight ≤ max`
   (the reaper never reaps a live lease), `active_count == 0` at rest,
   `total_count ≥ idle_count` (accounting stays consistent). Point-in-time
   `total ≥ min` is deliberately **not** asserted — with a tiny `max_lifetime` the
   reaper legitimately evicts at/below min and refills, so `total` transiently dips
   below min by design.

### 4.5 loom tests

`#[cfg(loom)]` models, run via `RUSTFLAGS="--cfg loom" cargo test --test loom`
(or an in-crate `#[cfg(loom)] mod`). Bounded small (2–3 loom threads, pool max
1–2, maintenance disabled):

- **Acquire/release handoff** — two threads contend for one slot; the releaser's
  `Condvar` notify must hand the connection to the waiter; assert no lost wakeup,
  no double-hand-out, correct final accounting.
- **Acquire timeout** — a waiter that times out returns `Err(Pool)` and leaves the
  accounting consistent for a concurrent releaser.
- **Shutdown / Drop ordering** — the pool and outstanding leases dropping in any
  interleaving finalize cleanly with no use-after-free of `PoolInner` (leases hold
  `Arc<PoolInner>`, so this checks the reference-count teardown, not raw memory).

### 4.6 Perf harnesses (both, per decision)

- **criterion micro-benchmarks** (`halcyon/benches/`, dev-dependency): acquire →
  release round-trip; a single scalar query on the fake. Statistically-rigorous
  per-op latency; run on demand, not gated in CI.
- **Custom throughput sweep** (a `stress`-feature binary): reuse `run_workload` in
  duration mode, sweep `pool_max` (e.g. 1/2/4/8/16), N contending threads, print
  throughput + p50/p95/p99 from the histogram. Deterministic, no Db2, CI-runnable.
- **Live batch-insert bench** (`HALCYON_TEST_DSN`-gated binary, mirrors
  `batch_insert_bench.cpp`): times inserting N rows three ways — per-row execute
  under autocommit, array-binding under autocommit, array-binding in one
  transaction — and prints rows/sec. Rides the existing `live-db2` path; a no-op
  when the DSN is unset.

### 4.7 CI

- **`loom` job** — `RUSTFLAGS="--cfg loom" cargo test` for the loom models,
  pure-Rust (no clidriver), on stable or nightly Linux.
- **`stress-tsan` job** — nightly Linux x86_64, `--no-default-features` (no
  clidriver needed), `RUSTFLAGS="-Zsanitizer=thread"` (+ `-Zbuild-std` as
  required), running the four scenarios. Iteration counts sized to run in
  reasonable CI time while still forcing overlap.
- The throughput sweep and criterion run on demand / manually; the live bench
  rides the existing `live-db2` job. The default `cargo test` and the pure-Rust
  matrix are unaffected — stress/loom/perf are separate targets/jobs.

## 5. File / module layout

```
halcyon/src/sync.rs                cfg-gated Arc/Mutex/Condvar/thread shim (prod)
halcyon/src/stress/mod.rs          #[cfg(any(test, feature="stress"))]
  ├── fake_driver.rs               ConcurrentFakeDriver
  ├── histogram.rs                 LatencyHistogram
  ├── runner.rs                    WorkerCtx / RunConfig / RunReport / run_workload
  └── scenarios.rs                 the four workloads + PoolConfig presets
halcyon/tests/loom.rs              #[cfg(loom)] models (or in-crate cfg module)
halcyon/tests/stress.rs            the four scenarios as #[test]s (TSan-run in CI)
halcyon/benches/pool.rs            criterion micro-benchmarks
halcyon/src/bin/throughput.rs      custom throughput sweep (stress feature)
halcyon/src/bin/batch_bench.rs     live batch-insert bench (DSN-gated)
```

Exact placement (feature vs. `tests/support`, bin vs. example) is a plan-step
decision; the shape above is the target.

## 6. Scope boundary — deferred, and why

Two C++ scenarios depend on features Halcyon-rs does not have yet and are **out of
scope** for this gate; the harness leaves hooks so they slot in without rework:

- **Statement-cache churn** (`make_cache_churn`) — needs the Phase 5 per-connection
  LRU statement cache and its `halcyon_stmt_cache_*` metrics. Added in Phase 5.
- **Executor / async saturation** (`make_executor_saturation`,
  `DestroyDatabaseWithInflightAsyncWork`) — needs the Phase 6 `tokio` facade.
  Added in Phase 6.

Observability-gauge assertions in the C++ reaper scenario
(`halcyon_pool_connections{state=…}`) are likewise deferred to Phase 7; the Rust
reaper scenario asserts the same invariants directly against the pool's
`active_count`/`idle_count`/`total_count` accessors instead of via a metrics sink.

## 7. Acceptance criteria (flips Phase 4 to done)

1. `loom` passes all §4.5 models clean under `--cfg loom` (no assertion failure, no
   loom-detected deadlock, within the branch budget).
2. The four §4.4 scenarios pass under ThreadSanitizer with no TSan report and all
   listed invariants holding.
3. Both perf harnesses (criterion micro + custom sweep) build and run; the live
   batch bench builds and runs against a DSN when present, no-ops without.
4. `cargo test -p halcyon --no-default-features` and the existing pure-Rust matrix
   still pass; no `allow(unsafe_code)` added anywhere; the `sync` shim compiles to
   `std` in normal builds.
5. README roadmap updated: Phase 4 checkbox ticked, the "not yet accepted" caveat
   removed, and Phase 5 (statement cache) named as next.

## 8. Risks / open questions (resolve during planning)

- **loom API deltas.** `loom::sync` guards and `loom::thread` differ subtly from
  `std`; the shim must reconcile them without leaking `cfg(loom)` into `pool.rs`
  call sites more than necessary. Mitigate by keeping loom models minimal.
- **loom state-space blowup.** Keep threads ≤ 3 and pool ≤ 2; use
  `loom::model` with explicit small bounds; disable the maintenance thread.
- **TSan on macOS vs. Linux.** `-Zsanitizer=thread` is most reliable on Linux
  x86_64; the stress-tsan job runs there. macOS Intel keeps the normal (non-TSan)
  stress run for coverage without the sanitizer requirement.
- **Stress determinism.** `base_delay: ZERO` makes backoff instant; fault knobs are
  1-in-N deterministic; RNG is per-worker seeded — runs are reproducible, but
  iteration counts must be tuned so faults and overlap actually occur within CI
  time.
