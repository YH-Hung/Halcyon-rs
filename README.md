# Halcyon-rs

A modern Rust client library for **IBM Db2**, built on the IBM Db2 CLI (`libdb2`). The Rust counterpart of the C++ [Halcyon](https://github.com/YH-Hung/Halcyon) library — same layered architecture, same error model, same driver-seam philosophy, expressed idiomatically in Rust.

> **Status: early development.** The driver seam, the core data path, and the pooled `Database` facade — connection pool (bounded sizing, blocking acquire, idle/lifetime reaping, transparent reconnect), safe auto-retry, and RAII transactions — are implemented and covered by mock + live-Db2 tests. The pool's concurrency-correctness track (`loom` + TSan stress + perf harnesses) is in place — Phase 4 accepted — and each pooled connection now carries a per-connection prepared-statement LRU (`PoolConfig::statement_cache_size`, default 64; Phase 5), mock-complete with live acceptance riding the `live-db2` reuse round-trip. The `tokio` async facade (Phase 6) is next. See the roadmap below.

## Architecture

Strict layering with a **thin CLI seam** — only `halcyon-sys` and `halcyon::driver::db2` ever touch the IBM CLI ABI. Everything above the seam is portable, mockable, `unsafe`-free Rust (enforced by `#![deny(unsafe_code)]`).

```
facade ─► pool ─► core ─► driver (CliDriver trait) ─► halcyon-sys ─► libdb2
```

| C++ Halcyon | Halcyon-rs |
|---|---|
| `ICliDriver` interface | `driver::CliDriver` trait |
| `Db2CliDriver` (only code including `sqlcli1.h`) | `driver::Db2CliDriver` (only module using `halcyon-sys`) |
| boundary `Value` variant | `Value` enum (`Null`/`Bool`/`Int`/`Double`/`Text`/`Bytes`) |
| `Result<T>` + throwing overloads | idiomatic `Result<T, Error>` only |
| `Error` (code, SQLSTATE, SQLCODE, message, retriable) | `Error` — identical fields and classification |
| `HALCYON_REFLECT` struct mapping | `#[derive(FromRow)]` proc macro (`halcyon-derive`) |
| `std::future` async over executor threads | feature-gated `tokio` facade over `spawn_blocking` *(planned)* |

## Requirements

- Rust 1.75+
- IBM Db2 CLI driver (**clidriver**) — user-supplied, never redistributed. Point `DB2_CLIDRIVER_ROOT` at it, or vendor it at `third_party/clidriver/`. A sibling checkout of the C++ Halcyon repo works out of the box via [.cargo/config.toml](.cargo/config.toml).

## Build & test

```bash
# Full build (links libdb2; needs clidriver)
cargo build
cargo test

# Pure-Rust layers only — no clidriver required (for CI without Db2)
cargo build -p halcyon --no-default-features
cargo test  -p halcyon --no-default-features
```

The clidriver `lib/` directory is embedded as an rpath in test/example binaries, so they run without `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH`. Downstream consumers should do the same in their own build script (read `DEP_DB2_LIB_DIR`) or set the library path at runtime.

### Live integration test

An end-to-end round-trip against a real Db2 lives in [halcyon/tests/db2_roundtrip.rs](halcyon/tests/db2_roundtrip.rs). It is gated on `HALCYON_TEST_DSN` and skipped (a no-op pass) when unset, so the default `cargo test` needs no database. To run it against a local Db2:

```bash
docker compose -f docker/docker-compose.yml up -d   # wait until healthy (first boot: a few minutes)
export HALCYON_TEST_DSN='DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=halcyon;'
cargo test --test db2_roundtrip -- --nocapture
```

> On macOS + Docker Desktop the host cannot complete the Db2 DRDA handshake through the port proxy (IBM's own `db2cli` fails identically), so run the round-trip on Linux — or in a Linux container on the Db2 network — where it connects natively. CI's `live-db2` job does exactly this.

### Derive UI tests

`#[derive(FromRow)]`'s compile-error messages are covered by [trybuild](halcyon/tests/ui/) snapshots. Because `.stderr` is rustc-version-specific, they run only under `HALCYON_UI_TESTS=1` (the pinned `derive-ui` CI job) and are a no-op pass otherwise. Regenerate after an intended change:

```bash
TRYBUILD=overwrite HALCYON_UI_TESTS=1 cargo test -p halcyon --no-default-features --features derive --test derive_ui
```

### Continuous integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push/PR: `fmt`, `clippy`, the pure-Rust test matrix (`--no-default-features` ± `derive`), an MSRV (1.75) check, and the full `db2` build/test on **Linux x86_64 + macOS Intel** (clidriver is x86_64-only), with the driver fetched by a composite action. A `live-db2` job stands up the Dockerized Db2 and runs the round-trip on pushes to `main`. *(The workflow runs once a GitHub remote is configured.)*

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `db2` | on | Links clidriver and enables the real `Db2CliDriver`. Disable to build/test the pure-Rust layers without Db2 installed. |
| `derive` | on | Enables `#[derive(FromRow)]` (the `halcyon-derive` proc macro). Pure-Rust; disable to drop the proc-macro dependency. |

## Roadmap

- [x] Workspace scaffolding (`halcyon`, `halcyon-sys`)
- [x] Error model: `ErrorCode`, `Error`, SQLSTATE→code classification, `retriable` flag
- [x] Boundary `Value` type with ergonomic `From` conversions
- [x] `CliDriver` seam trait (mockable; unit tests need no live database)
- [x] `Db2CliDriver`: connect/prepare/bind/execute/fetch, chunked array-binding batch, transactions — port of the C++ driver seam, exercised end-to-end against live Db2
- [x] Batch shape validation (rectangular, type-homogeneous) enforced inside the driver
- [x] Core: `Connection`, `Statement`, `ResultSet`, named (`:name`) & positional (`?`) parameters, row → tuple mapping (streaming cursor + `query_map`)
- [x] `#[derive(FromRow)]` proc macro (`halcyon-derive`) — by-position struct mapping + `Connection::query_as`
- [x] Facade: `Database` entry point (a cheap, cloneable, `Send + Sync` handle over the pool), RAII + closure `transaction()` (commit on `Ok`, rollback on `Err`/panic via a `Drop` guard), `execute_batch`, `query`/`query_as`/`execute` — validated on mock **and** live Db2; re-targeted onto the pool in Phase 4 (`query` now returns an owned `QueryResult` cursor, `begin` an owned `ScopedTransaction`, each holding its lease)
- [x] Pool: min/max bounds, acquire timeout, validation, idle/lifetime reaping, transparent reconnect, safe auto-retry — **implemented** (`ConnectionPool`/`PoolConfig`/`PooledConnection`, retry driven by `BackoffPolicy`/`ExecPolicy` + read-only detection: writes run once, reads replay on a transient error) and covered by mock unit tests with no live database or real clock (boundary durations keep reaping/backoff deterministic); accepted via the loom + TSan + perf track (`halcyon::stress`, `tests/stress.rs`, `benches/`, `examples/throughput.rs`).
- [x] Concurrency-correctness gate (accepts Phase 4): `loom` models of the pool handoff (`src/pool.rs` under `--cfg loom`), the `ConcurrentFakeDriver` + workload-runner scenarios ([tests/stress.rs](halcyon/tests/stress.rs)) run under ThreadSanitizer, and perf harnesses (criterion [benches/pool_bench.rs](halcyon/benches/pool_bench.rs) + the [throughput](halcyon/examples/throughput.rs) sweep + the live [batch_bench](halcyon/examples/batch_bench.rs)); wired into CI's `stress`, `stress-tsan`, and `loom` jobs
- [x] Statement cache (per-connection LRU): `StatementCache` + lease tokens inside `Connection` (`open_with_cache`), keyed by rewritten SQL with `close_cursor` reset-before-reuse, busy-hit transient overflow, poison/close-failure entry drop, and clear-before-disconnect; `PoolConfig.statement_cache_size` (default 64, 0 disables) applies it to every pooled connection, including reconnects — covered by the ported C++ cache suite plus connection/pool/facade integration on mock, compared cached-vs-uncached in criterion, and proven on real Db2 by the [live reuse round-trip](halcyon/tests/db2_roundtrip.rs) in `live-db2`
- [ ] `tokio` async facade (feature-gated, `spawn_blocking`)
- [ ] Observability: `metrics`/`tracing`-facade adapters (feature-gated)
- [x] CI (GitHub Actions): fmt/clippy/MSRV + pure-Rust matrix + `db2` Linux x86_64 & macOS-Intel + `live-db2` — runs once a GitHub remote is configured
- [x] Integration tests against Dockerized Db2 — env-gated round-trip ([tests/db2_roundtrip.rs](halcyon/tests/db2_roundtrip.rs) + [compose](docker/docker-compose.yml)) **validated end-to-end against live Db2**: connect → DDL → positional/named/batch insert → typed streaming read (incl. NULL → `None`) → commit/rollback via the seam primitives. It caught a real seam ABI bug (32-bit `SQLLEN`). Phase 3 adds a facade round-trip alongside it (ergonomic `Database::connect`, closure/RAII `Transaction` commit + rollback); both are wired into CI's `live-db2` job on real x86_64

## License

BSD 3-Clause — same as the C++ Halcyon.
