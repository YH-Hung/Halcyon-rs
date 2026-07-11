# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The versioning policy is described in the [README](README.md#versioning--msrv):
the public `halcyon` API is SemVer-stable; `halcyon-sys` and `halcyon-derive`
are implementation details.

## [Unreleased]

### Added

Initial release of the Rust counterpart of the C++
[Halcyon](https://github.com/YH-Hung/Halcyon) Db2 client. All three crates
(`halcyon-sys`, `halcyon-derive`, `halcyon`) at 0.1.0.

- **Driver seam** — `CliDriver` trait with the only CLI-touching implementation
  `Db2CliDriver` (feature `db2`, default on): connect/prepare/bind/execute/fetch,
  chunked array-binding batch insert with shape validation, transactions.
  `halcyon-sys` carries the hand-written FFI (19 entry points, type widths
  verified against the clidriver headers). Everything above the seam is
  `#![deny(unsafe_code)]` and testable against a mock driver.
- **Error model** — `Error` (code, SQLSTATE, SQLCODE, message, `retriable`)
  with SQLSTATE classification matching the C++ `classify_sqlstate`;
  `Result`-only public API, no panicking entry points.
- **Core data path** — `Connection`, `Statement`, streaming `ResultSet`,
  positional (`?`) and named (`:name`) parameters, row → tuple mapping,
  `query_map`, and `#[derive(FromRow)]` by-position struct mapping
  (feature `derive`, default on).
- **Connection pool** — min/max bounds, blocking acquire with timeout,
  validation, idle/lifetime reaping, transparent reconnect, and safe auto-retry
  (`BackoffPolicy`/`ExecPolicy`; writes run once, reads replay on transient
  errors). Concurrency correctness verified by `loom` models of the pool
  handoff and a ThreadSanitizer stress harness.
- **Statement cache** — per-connection prepared-statement LRU
  (`PoolConfig::statement_cache_size`, default 64), with cursor reset before
  reuse, poison-on-failure eviction, and clear-before-disconnect.
- **`Database` facade** — cheap, cloneable, `Send + Sync` pooled handle:
  `query`/`query_as`/`execute`/`execute_batch`, closure and RAII
  `transaction()` (commit on `Ok`, rollback on `Err`/panic).
- **Async facade** — `AsyncDatabase` over `tokio::task::spawn_blocking`
  (feature `tokio`, default off).
- **Observability** — process-global `tracing` spans and `metrics`
  counters/histograms/gauges matching the C++ catalog, on sync and async paths
  (feature `observability`, default off).
- **Validation** — mock-driver unit suite (no database needed), a live-Db2
  round-trip integration test (env-gated on `HALCYON_TEST_DSN`, Dockerized
  Db2), trybuild UI tests for the derive macro, criterion benches, and a
  loom/TSan/perf CI matrix.

[Unreleased]: https://github.com/YH-Hung/Halcyon-rs/commits/main
