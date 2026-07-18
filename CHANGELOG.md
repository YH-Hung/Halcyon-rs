# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The versioning policy is described in the [README](README.md#versioning--msrv):
the public `halcyon` API is SemVer-stable; `halcyon-sys` and `halcyon-derive`
are implementation details.

## [Unreleased]

## [0.1.0] - Pending publication

This is the complete planned initial release surface. The crates have not been
published and no `v0.1.0` tag or GitHub release exists yet. The `halcyon-sys`,
`halcyon-derive`, and `halcyon` workspace crates share version 0.1.0.

### Added

- **CLI seam and FFI** — the mockable `driver::CliDriver` trait and its
  `Db2CliDriver` implementation behind the default `db2` feature. Hand-written
  IBM CLI declarations remain confined to `halcyon-sys` and the Db2 seam;
  `halcyon` denies unsafe code.
- **Structured errors** — `Error` carries `ErrorCode`, SQLSTATE, native SQLCODE,
  message, and the `retriable` bit used by policy. Codes cover connection,
  timeout, constraint, syntax, deadlock, transient, mapping, pool,
  invalid-argument, invalid-state, and unknown failures; SQLSTATE classification
  matches C++ `classify_sqlstate`. Public fallible APIs return `Result`.
- **Core and mapping** — `Connection`, `Statement`, ordinary `ResultSet`,
  positional (`?`) and named (`:name`) parameters, `Value`, tuple mapping,
  `query_map`, and by-position `#[derive(FromRow)]` through the default `derive`
  feature.
- **Batch and block fetch** — rectangular/type-homogeneous column-wise
  `execute_batch` with bounded internal chunks, plus `CliDriver::fetch_block`
  rowsets for ordinary cursors. Short non-empty blocks are success and an empty
  block is EOF.
- **Pooled facade** — cloneable, `Send + Sync` `Database` over
  `ConnectionPool`, with min/max sizing, acquire timeout, validation,
  idle/lifetime maintenance, transparent reconnect, and broken-lease discard.
  `BackoffPolicy`/`ExecPolicy` replay classified read-only failures; writes run
  once.
- **Prepared-statement cache** — per-connection LRU controlled by
  `PoolConfig::statement_cache_size` (64 by default, zero disables), including
  reset-before-reuse, transient overflow for busy hits, poisoned-entry eviction,
  and clear-before-disconnect behavior.
- **Transactions and isolation** — standalone and pooled RAII/closure
  transactions, session defaults through `ConnectionParams::isolation` and
  `PoolConfig::isolation`, and per-transaction `begin_with_isolation` /
  `transaction_with_isolation` overrides with restoration.
- **Savepoints** — `Savepoint` guards from `savepoint` and `savepoint_named`,
  plus `nested` scopes. Live guards roll back and release on drop; explicit
  rollback/release is one-shot; names are validated and active case-folded
  duplicates are rejected client-side.
- **LOB input and lending output** — single-use `BindParam` /
  `OwnedBindParam`, `lob_file`, `lob_reader`, `lob_callback`, owned async source
  constructors `owned_lob_file` / `owned_lob_callback`, and
  `execute_with_lobs` for chunked BLOB/CLOB writes.
  `query_streaming`, `StreamingRow`, and `LobReader` provide ascending,
  row-at-a-time scalar/LOB reads with `read`, `copy_to`, `copy_to_file`,
  `to_vec`, `to_string`, and `is_null`.
- **Async facade** — feature-gated `AsyncDatabase` over
  `tokio::task::spawn_blocking`, with `execute`, materialized `query_as`,
  `execute_batch`, owned-LOB `execute_with_lobs`, and closure-scoped
  `transaction`, `transaction_with_isolation`, and `query_streaming`.
  Admission is bounded by pool capacity and tracing context propagates into the
  blocking closure when observability is enabled.
- **Observability** — optional `tracing` spans and `metrics`
  counters/histograms/gauges for query/execute/transaction, pool, retry,
  reconnect, and statement-cache paths. Structured events use the exact C++
  catalog: `connect.ok`, `connect.fail`, `reconnect.attempt`, `reconnect.ok`,
  `reconnect.fail`, `pool.exhausted`, `pool.reap`, `retry.attempt`,
  `stmt_cache.evict`, `txn.poisoned`, `savepoint.poisoned`, and
  `isolation.restore_fail`.
- **Validation assets** — pure-Rust mock tests, trybuild derive UI snapshots,
  deterministic concurrent stress, ThreadSanitizer and loom jobs, criterion and
  throughput harnesses, and `HALCYON_TEST_DSN`-gated sync/async Db2 tests for
  transactions, cache/cancel reuse, block/fallback reads, isolation/savepoints,
  and deterministic 32 MiB LOB paths. With no DSN these tests print a skip and
  return; this changelog does not claim that the new catch-up assertions ran
  live in this pre-publication session.

### Changed

> **Seam implementers:** `CliDriver` gained or changed required methods for
> `set_isolation`, block fetch (`fetch_block`), row-at-a-time streaming
> (`fetch_next`, `get_value`, `get_data_chunk`), and data-at-execution writes
> (`execute_streaming`). These methods have no compatibility defaults;
> out-of-tree `CliDriver` implementations must update before using 0.1.0.

- When a long/LOB column is present, Rust's ordinary `Db2CliDriver::fetch_block`
  path intentionally returns **at most one complete row per call**. This bounds
  large-row materialization and differs from the C++ seam, which can return a
  multi-row LOB-bearing block. Public `Value`s stay correct; lending LOB reads
  use the separate `fetch_next`/`query_streaming` path.
- If no session isolation was configured, a per-transaction override restores
  Cursor Stability, Db2's server-default target. Configure
  `ConnectionParams::isolation` or `PoolConfig::isolation` for another stable
  baseline.
- Streaming queries and LOB executions are deliberately single-attempt; the
  library does not replay partially consumed sources or cursors. Async streaming
  is closure-scoped so lending rows, transaction guards, and pool leases never
  cross an await point. Dropping a `spawn_blocking` future does not cancel its
  in-flight database work.

### Fixed

- Corrected non-Windows IBM CLI `SQLLEN`/`SQLULEN` widths to 32 bits, preserving
  the `-1` NULL indicator sentinel observed in live round-trips.
- Statement failures poison cached entries so they cannot be reused in an
  uncertain state. Aborted data-at-execution writes call `SQLCancel`; the
  DSN-gated same-handle regression test exercises a subsequent execution on the
  exact statement handle.
- Long-column fallback preserves complete binary/text `Value`s, embedded NULs,
  NULL indicators, and multibyte UTF-8 while keeping block sizes bounded.
- Structured-event levels and fields match the C++ catalog, including bounded
  `stmt_cache.evict` SQL hashes and `isolation.restore_fail` error codes.

[Unreleased]: https://github.com/YH-Hung/Halcyon-rs/commits/main
