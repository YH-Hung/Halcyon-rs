# Halcyon-rs

A modern Rust client library for **IBM Db2**, built on the IBM Db2 CLI (`libdb2`). The Rust counterpart of the C++ [Halcyon](https://github.com/YH-Hung/Halcyon) library — same layered architecture, same error model, same driver-seam philosophy, expressed idiomatically in Rust.

> **Status: pre-release 0.1.0.** Implementation now covers C++ Halcyon through v1.1, plus Db2 block/rowset fetch. Publication is still gated on the complete matrix in [docs/RELEASING.md](docs/RELEASING.md), including live Db2, loom, ThreadSanitizer, docs, package, and platform checks. No crates have been published and no `v0.1.0` tag or release has been created.

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
| `std::future` async over executor threads | feature-gated `AsyncDatabase` over `tokio::task::spawn_blocking`, with closure-scoped transactions and streaming |

## Current surface

- **Db2 data path** — positional and named parameters; exact `Value` mapping; column-wise, memory-bounded `execute_batch`; and rowset-backed `CliDriver::fetch_block` for ordinary `ResultSet`/`QueryResult` cursors.
- **Transactions** — RAII and closure transactions on `Connection` and `Database`. `ConnectionParams::isolation` sets the session default for standalone connections; pooled sessions take theirs from `PoolConfig::isolation` alone (`Database::open` discards `ConnectionParams::isolation`, even when the pool level is `None`). `begin_with_isolation` and `transaction_with_isolation` apply a per-transaction override and restore the configured level, or Cursor Stability when no level was configured.
- **Savepoints** — `savepoint`, `savepoint_named`, and `nested` return/use RAII `Savepoint` guards. Dropping a live guard rolls back and releases it; active names are guarded case-insensitively to match Db2 identifier folding.
- **Large objects** — single-attempt `execute_with_lobs` accepts `BindParam` values plus `lob_file`, `lob_reader`, or `lob_callback` BLOB/CLOB sources. `query_streaming` returns a lending `StreamingRow`; `LobReader` supports chunk reads, `copy_to`, `copy_to_file`, `to_vec`, and `to_string` without letting a row or pool lease escape its cursor.
- **Pool, cache, and retry** — bounded pooling, validation/reconnect, idle/lifetime maintenance, a per-connection statement LRU, and classified read retry. Writes, batches, and streaming/LOB operations are single-attempt.
- **Async facade** — with feature `tokio`, `AsyncDatabase` provides `execute`, `query_as`, `execute_batch`, `execute_with_lobs`, `transaction`, `transaction_with_isolation`, and closure-scoped `query_streaming`. Work runs through `spawn_blocking`; there are deliberately no async `begin` methods, and dropping a future does not cancel in-flight blocking work.
- **Observability** — with feature `observability`, `tracing` spans and `metrics` instruments cover query/execute/transaction, pool, retry, reconnect, and cache paths. The structured event names exactly match the C++ catalog: `connect.ok`, `connect.fail`, `reconnect.attempt`, `reconnect.ok`, `reconnect.fail`, `pool.exhausted`, `pool.reap`, `retry.attempt`, `stmt_cache.evict`, `txn.poisoned`, `savepoint.poisoned`, and `isolation.restore_fail`; `rowset.row_warning` (a benign class-01 fetch warning's diagnostic) is Rust-only.

### Long-column block-fetch divergence

`CliDriver::fetch_block` normally returns multiple row-major `Value` rows. When `Db2CliDriver` detects a long or LOB column, the ordinary Rust path intentionally falls back to **at most one row per `fetch_block` call** rather than materializing many large rows at once as C++ can. The public `Value`s remain complete and correct; lending LOB reads use the separate row-at-a-time `query_streaming`/`fetch_next` path.

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

The clidriver `lib/` directory is embedded as an rpath in test/example binaries, so on **Linux** they run without `LD_LIBRARY_PATH`. On **macOS** the rpath is not consulted — IBM ships `libdb2.dylib` with a bare install name, and rpaths only apply to `@rpath/` names — so set `DYLD_LIBRARY_PATH` to the clidriver `lib/` directory when running binaries. Downstream consumers can read `DEP_DB2_LIB_DIR` in their own build script or set the library path at runtime.

### Live integration test

The DSN-gated suite in [halcyon/tests/db2_roundtrip.rs](halcyon/tests/db2_roundtrip.rs) covers the core/facade path, statement reuse and cancel safety, bounded and long-column block fetch, savepoints and isolation visibility, and deterministic 32 MiB LOB source/output checks. Every test prints a skip and returns when `HALCYON_TEST_DSN` is absent, so a no-DSN harness pass is compile/discovery evidence—not live database proof. The new parity-catch-up cases have not been claimed as live-verified in this pre-release state; the release matrix requires an explicit DSN-backed run.

To run the suite against a local Db2:

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

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs `fmt`, `clippy`, the pure-Rust feature matrix, the Rust 1.75 MSRV lane, Db2-linked Linux/macOS builds, deterministic stress, ThreadSanitizer stress, loom models, and derive UI tests. Its `live-db2` job is configured to stand up Dockerized Db2 and run the DSN-gated sync and async round-trips on pushes to `main` and manual dispatch. Publication still requires the release-commit matrix in [docs/RELEASING.md](docs/RELEASING.md), rather than assuming that configured jobs or skipped tests ran live assertions.

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `db2` | on | Links clidriver and enables the real `Db2CliDriver`. Disable to build/test the pure-Rust layers without Db2 installed. |
| `derive` | on | Enables `#[derive(FromRow)]` (the `halcyon-derive` proc macro). Pure-Rust; disable to drop the proc-macro dependency. |
| `tokio` | off | Enables the async facade (`AsyncDatabase`) over `tokio::task::spawn_blocking`. Pulls in `tokio`; off by default so the pure-Rust/MSRV builds never compile it. |
| `observability` | off | Enables C++-parity `tracing` spans and `metrics` counters/histograms/gauges. Downstream installs a subscriber/recorder (and exporters). Off by default. |
| `stress` | off | Exposes the pure-Rust `halcyon::stress` fake driver, runner, and scenarios used by concurrency tests and benchmarks. |

## Roadmap

- [x] Driver seam, structured errors/SQLSTATE classification, core connections/statements/results, positional/named parameters, and `#[derive(FromRow)]`.
- [x] Column-wise batch binding and Db2 block/rowset fetch, including the documented at-most-one-row long-column fallback.
- [x] Pooled `Database`, reconnect and classified retry, maintenance, per-connection statement LRU, and safe lease/statement poisoning.
- [x] Isolation defaults and overrides, RAII/closure transactions, savepoints, and nested scopes.
- [x] Chunked BLOB/CLOB input sources and lending streaming output cursors/readers.
- [x] Feature-gated `AsyncDatabase` wrappers and C++-catalog observability.
- [x] Pure-Rust mock, stress, loom, ThreadSanitizer, derive UI, benchmark, and DSN-gated integration coverage wired into CI.
- [ ] Run the complete release-commit verification matrix, package all three crates, then publish `halcyon-sys` → `halcyon-derive` → `halcyon`, tag `v0.1.0`, and create the GitHub release—see [docs/RELEASING.md](docs/RELEASING.md).

## Versioning & MSRV

- The public **`halcyon` API is SemVer-stable**; `halcyon-sys` and `halcyon-derive` are implementation details with no stability guarantee of their own (all three crates version in lockstep). Pre-1.0, breaking changes bump the minor version.
- **MSRV is Rust 1.75**, checked in CI. Raising it is a minor version bump, never a patch.

## Deferred features

Explicitly out of scope for 0.1.0 and tracked for later releases:

- Windows and AIX support (the current seam widths are verified for supported non-Windows targets; IBM clidriver CI is x86_64).
- Scrollable cursors.
- LOB parameters in `execute_batch`.
- XML and DECFLOAT mappings.
- Public block-size and LOB-chunk-size tuning knobs.
- Async-native/coroutine streaming rather than closure-scoped `spawn_blocking` wrappers.
- A pool health-statistics accessor beyond the current count/metric surface.
- An optional standalone `Logger` trait if downstream users need non-`tracing` sinks.
- A seam getter for the current isolation level; transaction restoration currently normalizes an unconfigured session to Cursor Stability.

## License

BSD 3-Clause — same as the C++ Halcyon. See [LICENSE](LICENSE).
