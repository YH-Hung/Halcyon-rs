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
- **Pool, cache, and retry** — bounded pooling, opt-in validation on acquire with reconnect (`PoolConfig::validate_on_acquire`, off by default as in C++), idle/lifetime maintenance, a per-connection statement LRU, and classified read retry. Writes, batches, and streaming/LOB operations are single-attempt.
- **Async facade** — with feature `tokio`, `AsyncDatabase` provides `execute`, `query_as`, `execute_batch`, `execute_with_lobs`, `transaction`, `transaction_with_isolation`, closure-scoped `query_streaming`, and `with_connection` (the C++ `async_with_connection`) for several statements on one leased connection. Work runs through `spawn_blocking`; there are deliberately no async `begin` methods, and dropping a future does not cancel in-flight blocking work.
- **Observability** — with feature `observability`, `tracing` spans and `metrics` instruments cover query/execute/transaction, pool, retry, reconnect, and cache paths. The structured event names exactly match the C++ catalog: `connect.ok`, `connect.fail`, `reconnect.attempt`, `reconnect.ok`, `reconnect.fail`, `pool.exhausted`, `pool.reap`, `retry.attempt`, `stmt_cache.evict`, `txn.poisoned`, `savepoint.poisoned`, and `isolation.restore_fail`; `rowset.row_warning` (a benign class-01 fetch warning's diagnostic) is Rust-only.

### Long-column block-fetch divergence

`CliDriver::fetch_block` normally returns multiple row-major `Value` rows. When `Db2CliDriver` detects a long or LOB column, the ordinary Rust path intentionally falls back to **at most one row per `fetch_block` call** rather than materializing many large rows at once as C++ can. The public `Value`s remain complete and correct; lending LOB reads use the separate row-at-a-time `query_streaming`/`fetch_next` path.

## Requirements

- Rust 1.75+
- IBM Db2 CLI driver (**clidriver**) — user-supplied, never redistributed. Point `DB2_CLIDRIVER_ROOT` at it, or vendor it at `third_party/clidriver/`. A sibling checkout of the C++ Halcyon repo works out of the box via [.cargo/config.toml](.cargo/config.toml).

## Quick start

Add the published client to your application (version 0.1.0 is awaiting its
first publication):

```toml
[dependencies]
halcyon = "0.1.0"
```

Install IBM's clidriver for your platform and set `DB2_CLIDRIVER_ROOT` to its
absolute installation directory before building. The default features enable
Db2 and `#[derive(FromRow)]`. Add `features = ["tokio"]` for `AsyncDatabase`.

| Target | Db2 feature | Pure-Rust layers |
|---|---|---|
| Linux x86_64 | Built and tested in CI, including live Db2 | Supported |
| macOS x86_64 | Built and tested in CI | Supported |
| macOS ARM64 | Live-tested locally against Db2 in Docker Desktop; not in CI | Tested locally |
| Windows / AIX | Deferred | No CI support guarantee |

Configure the runtime loader with `LD_LIBRARY_PATH="$DB2_CLIDRIVER_ROOT/lib"`
on Linux or `DYLD_LIBRARY_PATH="$DB2_CLIDRIVER_ROOT/lib"` on macOS. The
repository's rpath setup for its own tests does not configure your application.

```rust
use halcyon::{Database, Error, ErrorCode, Result};

fn main() -> Result<()> {
    let dsn = std::env::var("HALCYON_DSN")
        .map_err(|_| Error::new(ErrorCode::InvalidArgument, "set HALCYON_DSN"))?;
    let db = Database::connect(&dsn)?;
    let rows: Vec<(i64,)> = db.query_as("VALUES CAST(? AS BIGINT)", (42_i64,))?;
    println!("{rows:?}");
    Ok(())
}
```

Set `HALCYON_DSN` to your CLI connection string, for example
`DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=...;PWD=...;`.
For a complete typed query and transaction example using a temporary table, see
[quick_start.rs](halcyon/examples/quick_start.rs). CI compiles the example;
run it against your database with `cargo run -p halcyon --example quick_start`.

## Timeouts and lost connections

Halcyon sets no deadline of its own on statements, validation, or connecting;
`PoolConfig::acquire_timeout` bounds only the wait for pool capacity. To bound
waits on an unresponsive server, add IBM's `ReceiveTimeout=<seconds>` keyword
to the DSN. A statement that times out fails with SQLSTATE `40003` (statement
completion unknown). Halcyon then retires that connection, as it does after any
connection-class failure, even one your code handles, so the pool replaces it.
The error's classification is unchanged: the statement may have completed, so
the failure never makes a write retriable.

Observed with IBM's macOS ARM64 clidriver v12.1.4 against a paused Db2
container, not confirmed on other drivers:

- `ReceiveTimeout=5` bounded every statement path, including the
  `validate_on_acquire` probe, at about 15 seconds.
- Adding `ConnectTimeout=5` to the same DSN disabled that bound: every statement
  waited until the server answered again.
- `ReceiveTimeout` does not bound reconnecting. A reconnect after a failed
  validation, or for a read retried after a lost connection, waits until the
  server answers, unless the pool is full and `acquire_timeout` ends the wait.

## Build & test

```bash
# Full build (links libdb2; needs clidriver)
cargo build
cargo test

# Pure-Rust layers only — no clidriver required (for CI without Db2)
cargo build -p halcyon --no-default-features
cargo test  -p halcyon --no-default-features
```

The clidriver `lib/` directory is embedded as an rpath in test/example binaries, so on **Linux** they run without `LD_LIBRARY_PATH`. On **macOS** the rpath is not consulted — IBM ships `libdb2.dylib` with a bare install name, and rpaths only apply to `@rpath/` names — so set `DYLD_LIBRARY_PATH` to the clidriver `lib/` directory when running binaries. That rpath reaches only this repository's binaries: `DEP_DB2_LIB_DIR` is visible only to direct dependents of `halcyon-sys`, so applications set the library path themselves, as in [Quick start](#quick-start).

### Live integration test

The DSN-gated suite in [halcyon/tests/db2_roundtrip.rs](halcyon/tests/db2_roundtrip.rs) covers the core/facade path, statement reuse and cancel safety, bounded and long-column block fetch, savepoints and isolation visibility, and deterministic 32 MiB LOB source/output checks. Every test prints a skip and returns when `HALCYON_TEST_DSN` is absent, so a no-DSN harness pass is compile/discovery evidence—not live database proof. The new parity-catch-up cases have not been claimed as live-verified in this pre-release state; the release matrix requires an explicit DSN-backed run.

To run the suite against a local Db2:

```bash
docker compose -f docker/docker-compose.yml up -d   # wait until healthy (first boot: a few minutes)
export HALCYON_TEST_DSN='DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=halcyon;'
cargo test --test db2_roundtrip -- --nocapture
```

> On macOS, run the suite from the host through Docker Desktop's port proxy, with `DYLD_LIBRARY_PATH` set as above. This was verified on Apple silicon with IBM's `macarm64` clidriver (v12.1.4) against this compose file's Db2 11.5.9.0 and against Db2 12.1.5.0. The Db2 image is linux/amd64, so Apple silicon emulates it. CI's `live-db2` job runs the same suite on Linux.

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
- With feature `tokio`, any tokio 1.x from 1.52 works. Tokio 1.54 needs Rust 1.85, and Cargo before 1.84 ignores `rust-version` when resolving, so a Rust 1.75 build pins a compatible tokio in its lockfile: `cargo update -p tokio --precise 1.53.3`. Cargo 1.84 and later can pick one itself with `resolver.incompatible-rust-versions = "fallback"`, the default under resolver 3.

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
