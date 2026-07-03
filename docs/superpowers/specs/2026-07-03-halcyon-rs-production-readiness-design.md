# Halcyon-rs — Production Readiness — Strategy & Roadmap

**Date:** 2026-07-03
**Status:** Proposed direction (pre-implementation)
**Distribution target (locked):** crates.io (published)

## 1. Where we are

The **driver seam and everything below it is built; nothing above it is.**

Implemented (~1,750 LOC, all unit-tested):

- `halcyon-sys` — hand-written FFI over the IBM Db2 CLI (19 entry points), type
  widths verified against the vendored clidriver headers.
- `halcyon::error` — `ErrorCode`, `Error`, `retriable`, `Display`.
- `halcyon::value` — boundary `Value` enum + ergonomic `From` conversions.
- `halcyon::driver::CliDriver` — the mockable seam trait.
- `halcyon::driver::db2::Db2CliDriver` — the only module touching the CLI ABI:
  connect/prepare/bind/execute/fetch, chunked array-binding batch, transactions.
- `driver::batch` — rectangular / type-homogeneous batch validation.
- `driver::sqlstate::classify_sqlstate` — SQLSTATE → `ErrorCode` + `retriable`.

Not yet present:

- The entire portable stack **above** the seam: `core`, `pool`, `facade`,
  `derive`, `async`, `observability`.
- **No git history** (the workspace is uncommitted), **no CI**, **no integration
  or stress tests**.
- The `Db2CliDriver` seam **has never been exercised against a live Db2** — this
  is the single largest unknown in the codebase.

The C++ [Halcyon](https://github.com/YH-Hung/Halcyon) is a fully-mature v1 and is
the design reference for every layer below. "Production ready" for the Rust port
means: port the remaining layers in dependency order, preserving the C++
semantics (locking discipline, error classification, retry rules); validate the
seam against real Db2 early; and stand up the production tracks (CI, concurrency
verification, security audit, crates.io release) the C++ repo already has.

## 2. Definition of "production ready"

Four bars, all currently unmet:

1. **Functional parity** with the C++ facade — query/execute, `?` + `:name`
   params, row→tuple + struct mapping, transactions, pool, transparent
   reconnect, safe auto-retry, batch, statement cache.
2. **Proven against real Db2** — the seam must pass a Dockerized integration
   round-trip, not just mock tests.
3. **Resilient under concurrency** — pool acquire/release/validate/reconnect/retry
   correctness, verified under a race checker, not by inspection.
4. **Consumable & maintained** — published to crates.io, CI-gated, documented,
   SemVer'd, dependency- and `unsafe`-audited.

## 3. Guiding invariants (hold at every phase)

These are the hard rules from `CLAUDE.md`/`AGENTS.md`; they constrain every phase
below and must not be relaxed to make a layer easier:

- **The seam rule.** Only `halcyon-sys` and `halcyon/src/driver/db2.rs` touch the
  CLI ABI. `#![deny(unsafe_code)]` enforces it; no new `allow(unsafe_code)`.
- **Mock-testability.** `cargo test -p halcyon --no-default-features` must always
  pass — every layer above the seam is testable against a mock `CliDriver` with
  no clidriver installed.
- **`Result`-only.** No panicking public APIs. Rust collapses the C++ dual
  `Result<T>`/throwing model into the single idiomatic `Result<T, Error>`.
- **`Error.retriable` drives auto-retry**; classification stays in
  `driver/sqlstate.rs` and must match the C++ `classify_sqlstate`.
- **No hard deps for optional functionality.** Db2 linkage is behind `db2`
  (default on); async and observability are feature-gated too.
- **Port semantics, then idiomatize.** Read the corresponding C++ header/source
  first; preserve locking discipline, error classification, and retry rules;
  then express idiomatically.

## 4. Phased roadmap (dependency-ordered)

Each phase lists the deliverable, the C++ reference sources to port from, the
invariant that most matters there, the Rust-specific decisions, and the exit
criteria.

### Phase 0 — Baseline hygiene *(do first; ~half a day)*

An entire working seam is currently uncommitted and unguarded.

- **Commit** the foundation to git (conventional commits, matching the C++ repo).
- **CI** (GitHub Actions): jobs for
  - `cargo test -p halcyon --no-default-features` (pure-Rust, no driver),
  - full `cargo build`/`test` with clidriver resolved via `DB2_CLIDRIVER_ROOT`,
  - `cargo clippy --all-targets -- -D warnings`,
  - `cargo fmt --check`,
  - MSRV pin check (1.75),
  - matrix Linux x86_64 **+ macOS Intel** (clidriver is x86_64-only — an Intel
    runner is required; mirror the C++ repo's macOS-driver-artifact approach).
- **Shared `MockCliDriver`** test-support: a configurable in-memory `CliDriver`
  (port `../Halcyon/tests/unit/mock_cli_driver.hpp`) that every upper layer's
  tests build on. This is the linchpin of the mock-testability invariant.

**Exit:** green CI on both feature configs; mock lands with self-tests.

### Phase 1 — Core data path

- `Connection`, `Statement`, `ResultSet`/`Row`.
- Parameter model: positional `?` and named `:name`, with a parser that ignores
  `:`/`?` inside string literals and comments (port the C++ parser semantics
  exactly — this is a correctness trap).
- Row → tuple mapping and typed column extraction
  (`FromColumn` / `Row::get::<T>()`), the Rust analog of C++ `Row::as<...>`.
- **Reference:** `include/halcyon/connection.hpp`, `result.hpp`,
  `parameters.hpp`, `types.hpp`; tests `test_connection.cpp`, `test_result*.cpp`,
  `test_parameters.cpp`, `test_type_binder.cpp`.

**Exit:** full query/execute/param/mapping path works against `MockCliDriver`;
`--no-default-features` tests cover it.

### Phase 1.5 — First live validation *(pull forward; do not defer)*

As soon as Phase 1 lands, prove the seam against a real database:

- Stand up Dockerized Db2 (reuse `../Halcyon/docker/docker-compose.yml`).
- `tests/integration/` gated on an env DSN (e.g. `HALCYON_TEST_DSN`), **skipped
  when unset** — mirrors the C++ `HALCYON_TEST_DSN` convention.
- One end-to-end round-trip: connect → create temp table → param insert → query
  → typed read → transaction commit/rollback → batch insert.

**Exit:** the seam is proven, not assumed. Retire the biggest project risk here.

### Phase 2 — Derive macro

- New workspace member `halcyon-derive` (proc-macro crate).
- `#[derive(FromRow)]`, re-exported as `halcyon::FromRow`; mirror the
  `HALCYON_REFLECT` field-mapping semantics.
- **Reference:** the `HALCYON_REFLECT` machinery and `test_reflect.cpp`.

**Exit:** `#[derive(FromRow)]` maps a struct from a mock result set; trybuild
tests cover error messages.

### Phase 3 — Facade (single-connection first)

- `Database` entry point, `execute_batch`, RAII + closure `transaction()`.
- Transaction semantics: commit on `Ok`, rollback on `Err` **or panic** (a `Drop`
  guard rolls back an uncommitted transaction).
- Build over a single connection first; re-target the pool in Phase 4 (this is
  the order the C++ project used: facade ergonomics → pool concurrency).
- **Reference:** `database.hpp`, `transaction.hpp`; tests `test_database*.cpp`,
  `test_transaction.cpp`.

**Exit:** the README quick-start-equivalent API compiles and works on mock + live.

### Phase 4 — Pool & resilience *(highest-subtlety layer)*

- `PoolConfig`: min/max, acquire timeout, validation, idle/lifetime reaping.
- Transparent reconnect and **safe auto-retry** driven by `Error.retriable`.
- This layer is what actually *enforces* the seam's `Send + Sync` "single owner
  per leased connection" invariant — design the ownership/lease model
  deliberately, don't assume it.
- **Reference:** `pool.hpp`, `retry.hpp`; tests `test_pool.cpp`,
  `test_pool_maintenance.cpp`, `test_retry.cpp`, `test_database_retry.cpp`.

**Exit:** pool passes mock tests **and** the concurrency track (below).

### Phase 5 — Statement cache

- Per-connection LRU of prepared statements.
- Enforce the `close_cursor` reset-before-reuse discipline already declared in
  the seam trait.
- **Reference:** `detail/statement_cache.hpp`, `test_statement_cache.cpp`.

**Exit:** cache hit/miss/eviction covered; reused cached statements re-execute
correctly on live Db2.

### Phase 6 — Async facade *(feature-gated)*

- `tokio` facade over `spawn_blocking`; no async leaks into the sync core.
- Decide the surface: `async fn` returning `Result` vs. exposed `JoinHandle`;
  define cancellation semantics.
- **Reference:** `async.hpp`, `test_async.cpp`, `test_database_async.cpp`.

**Exit:** async mirrors the sync API under a feature flag; sync build unaffected.

### Phase 7 — Observability *(feature-gated)*

- Idiomatic Rust: `tracing` spans + `metrics` facade — **not** direct OTel /
  Prometheus bindings. Downstream chooses exporters. Zero overhead when disabled.
- Map the C++ metric/span set: `halcyon.query|execute|transaction|acquire|
  reconnect` spans; query/pool/reconnect/retry/cache metrics.
- **Reference:** `observability/*`, `test_observability.cpp`,
  `test_otel_propagation.cpp`.

**Exit:** spans/metrics emit when the feature is on; compile out cleanly when off.

## 5. Cross-cutting production tracks

Run alongside the phases, not after them.

### Concurrency correctness (gates Phase 4)

- Model-check the pool's lock/condvar logic with **`loom`**.
- A `ConcurrentFakeDriver` stress harness (port
  `../Halcyon/tests/stress/`) run under nightly **ThreadSanitizer** to prove
  race/deadlock freedom, plus a perf harness reporting throughput and
  p50/p95/p99 latency.

### Safety & security

- The `unsafe` boundary lives in exactly two crates. Keep a written audit of the
  `RawHandle: Send + Sync` justification and the single-owner invariant — and
  ensure the Phase-4 pool actually *upholds* it.
- Fuzz the param parser (Phase 1) and `validate_batch_rows`.
- `cargo-deny` + `cargo-audit` on the (deliberately small) dependency tree.
- `#![deny(missing_docs)]` on the public API; `cargo test --doc` for examples.

### Distribution — crates.io *(locked)*

Publishing three crates (`halcyon-sys`, `halcyon-derive`, `halcyon`) with a
proprietary, user-supplied clidriver imposes specific requirements:

- **`links = "db2"`** means only one `-sys` crate version may link at a time
  (already set) — keep `-sys` minimal and stable.
- **docs.rs** builds in a sandbox with no clidriver. Add
  `[package.metadata.docs.rs]` to build `halcyon` with `--no-default-features`
  (or a docs-only stub feature) so the docs build never needs the driver.
- **Consumer build:** the driver stays user-supplied; the consumer sets
  `DB2_CLIDRIVER_ROOT` and the `-sys` `build.rs` resolves it. Document the RPATH
  caveat (the driver lib dir must exist at runtime).
- **Publish order:** `halcyon-sys` → `halcyon-derive` → `halcyon`.
- **Metadata:** `keywords`, `categories`, `readme`, `documentation`, `rust-version`
  on each crate before first publish.

### Release hygiene

- `CHANGELOG.md` (Keep a Changelog), seeded once the facade is usable.
- SemVer policy: the **public `halcyon` API is stable**; `halcyon-sys` is an
  implementation detail and may change. State it in the README.
- MSRV policy (1.75 today) and its bump rules.
- A `docs/RELEASING.md` checklist: full matrix (unit + no-default, loom/TSan
  stress, live-Db2 integration, doc build) green → bump version → `cargo publish`
  in order → tag + GitHub Release.

## 6. Decisions

**Locked:**

| Topic | Decision |
|---|---|
| Distribution | crates.io — publish all three crates |
| Error model | `Result<T, Error>` only (no throwing/dual model) |
| Async | feature-gated `tokio` over `spawn_blocking`, ships after sync core |
| Observability | `tracing` + `metrics` facades, feature-gated |
| Reflection | `#[derive(FromRow)]` proc-macro in a new `halcyon-derive` crate |
| Concurrency verification | `loom` for pool logic + TSan stress harness |

**Open (decide when the phase is reached):**

- Streaming vs. materialized result sets as the default (C++ supports both).
- Async surface shape (`async fn` vs. `JoinHandle`) and cancellation semantics.
- Whether statement caching is on by default or opt-in per `Database`.
- Deferred features (savepoints/isolation levels, LOB streaming, Windows) —
  explicitly out of scope for the first production release, as in C++ v1.

## 7. Sequencing

```
Phase 0 (hygiene, CI, mock)
   └─► Phase 1 (core) ─► Phase 1.5 (LIVE validation) ─► Phase 2 (derive)
          └─► Phase 3 (facade) ─► Phase 4 (pool + loom/TSan) ─► Phase 5 (cache)
                 └─► Phase 6 (async) ─► Phase 7 (observability)
                        └─► Release track: crates.io publish + docs + CHANGELOG
```

Milestones: **M1** = Phase 1.5 (seam proven live). **M2** = Phase 4 (resilient
pooled facade — the minimum viable production surface). **M3** = Phases 5–7 +
release track (feature-complete v1 on crates.io).

## 8. Immediate next step

Phase 0: **commit the foundation and stand up CI.** Everything else builds on a
guarded, reproducible baseline, and it's the smallest high-leverage move
available today.
