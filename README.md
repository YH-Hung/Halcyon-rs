# Halcyon-rs

A Rust client library for **IBM Db2**, built on the IBM Db2 CLI (`libdb2` from clidriver). It is the Rust counterpart of the C++ [Halcyon](https://github.com/YH-Hung/Halcyon) library, with the same layered architecture, error model, and driver seam, expressed in idiomatic Rust.

> **Status: pre-release 0.1.0.** The API is complete for the first release, but no crate is on crates.io yet and no `v0.1.0` tag exists. Publication waits on the release matrix in [docs/RELEASING.md](https://github.com/YH-Hung/Halcyon-rs/blob/main/docs/RELEASING.md).

## Features

- **Pooled, thread-safe `Database`** — cloneable handle with bounded sizing, acquire timeout, idle and lifetime reaping, optional validation on acquire, transparent reconnect, and a per-connection prepared-statement LRU.
- **Safe auto-retry** — reads that fail with a retriable SQLSTATE are replayed with backoff; writes, batches, and streaming/LOB operations run exactly once.
- **Typed results** — map rows into tuples or into structs with `#[derive(FromRow)]`; bind `?` positional or `:name` named parameters.
- **Transactions** — RAII and closure transactions, per-transaction isolation overrides, savepoints, and nested scopes.
- **Large objects** — stream BLOB/CLOB parameters from a file, reader, or callback; read LOB columns in chunks or straight to a file without loading them into memory.
- **Batches and block fetch** — column-wise array binding for `execute_batch` and rowset fetches for ordinary cursors.
- **Async** (feature `tokio`) — `AsyncDatabase` over `tokio::task::spawn_blocking`.
- **Observability** (feature `observability`) — `tracing` spans and `metrics` instruments, with structured event names that match the C++ library.
- **Structured errors** — every fallible call returns `Result<T, halcyon::Error>` carrying an `ErrorCode`, SQLSTATE, native SQLCODE, message, and a `retriable` flag. No public API panics.

## Requirements

- Rust 1.75 or later.
- IBM's Db2 CLI driver (**clidriver**) for your platform. Halcyon never redistributes it.

| Target | Db2 feature | Pure-Rust layers |
|---|---|---|
| Linux x86_64 | Built and tested in CI, including against live Db2 | Supported |
| macOS x86_64 | Built and tested in CI | Supported |
| macOS ARM64 | Tested locally against Db2 in Docker Desktop; not in CI | Tested locally |
| Windows / AIX | Not yet supported | Not tested |

## Installation

Until 0.1.0 is published, depend on the Git repository:

```toml
[dependencies]
halcyon = { git = "https://github.com/YH-Hung/Halcyon-rs" }
# after publication: halcyon = "0.1"
```

Point the build at clidriver, and the runtime loader at its `lib/` directory:

```bash
export DB2_CLIDRIVER_ROOT=/opt/ibm/clidriver
export LD_LIBRARY_PATH="$DB2_CLIDRIVER_ROOT/lib"     # Linux
export DYLD_LIBRARY_PATH="$DB2_CLIDRIVER_ROOT/lib"   # macOS
```

`DB2_CLIDRIVER_ROOT` must be an absolute path. Your application has to set the library path itself: the rpath Halcyon embeds reaches only this repository's own test and example binaries.

## Quick start

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

`HALCYON_DSN` is a CLI connection string, such as `DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=...;PWD=...;`. A complete runnable version that uses a temporary table is in [examples/quick_start.rs](https://github.com/YH-Hung/Halcyon-rs/blob/main/halcyon/examples/quick_start.rs).

## Usage

### Typed rows, parameters, and transactions

```rust
use halcyon::{named, Database, FromRow, Result};

#[derive(Debug, FromRow)]
struct User {
    id: i64,
    name: String,
    email: Option<String>, // NULL-able column
}

fn add_user(db: &Database) -> Result<Vec<User>> {
    db.transaction(|tx| {
        tx.execute(
            "INSERT INTO users(id, name) VALUES (:id, :name)",
            named! { "id" => 7, "name" => "ada" },
        )?;
        // Rolls back to the savepoint if the closure returns an error.
        tx.nested(|tx| tx.execute("UPDATE users SET email = ? WHERE id = ?", ("ada@example.com", 7_i64)))?;
        tx.query_as("SELECT id, name, email FROM users WHERE id = ?", (7_i64,))
    })
}
```

`#[derive(FromRow)]` maps columns to fields by position. The closure commits when it returns `Ok` and rolls back when it returns `Err`. For explicit control, `db.begin()` returns a `ScopedTransaction` guard that rolls back on drop unless committed.

### Pool configuration and isolation

```rust
use std::time::Duration;
use halcyon::{Database, Isolation, PoolConfig, Result};

fn open(dsn: &str) -> Result<Database> {
    Database::connect_with(dsn, PoolConfig {
        max: 16,
        acquire_timeout: Duration::from_secs(2),
        validate_on_acquire: true,
        isolation: Some(Isolation::ReadStability),
        ..PoolConfig::default()
    })
}
```

The defaults are `min: 1`, `max: 8`, a 5-second acquire timeout, a 64-entry statement cache, validation off, and the server's default isolation. Use `db.transaction_with_isolation(Isolation::RepeatableRead, |tx| ...)` to override isolation for one transaction.

### Large objects

```rust
use halcyon::{lob_file, BindParam, Database, Result, Value};

fn store_and_export(db: &Database) -> Result<()> {
    // Streams the file into a BLOB parameter in chunks.
    db.execute_with_lobs(
        "INSERT INTO docs(id, body) VALUES (?, ?)",
        vec![Value::Int(1).into(), BindParam::Lob(lob_file("report.pdf"))],
    )?;

    // Reads the LOB column back in chunks, straight to a file.
    let mut rows = db.query_streaming("SELECT body FROM docs WHERE id = ?", (1_i64,))?;
    while let Some(mut row) = rows.next()? {
        row.lob(0)?.copy_to_file("report-copy.pdf")?;
    }
    Ok(())
}
```

Call `.as_clob()` on a source to bind character data. `lob_reader` and `lob_callback` take any `Read` or pull closure. A `LobReader` also offers `read`, `copy_to`, `to_vec`, and `to_string`.

### Async

```rust
use halcyon::{AsyncDatabase, Result};

async fn count(dsn: &str) -> Result<i64> {
    let db = AsyncDatabase::connect(dsn).await?;
    let rows: Vec<(i64,)> = db.query_as("SELECT COUNT(*) FROM users", ()).await?;
    Ok(rows[0].0)
}
```

Enable it with `features = ["tokio"]`. `AsyncDatabase` offers `execute`, `query_as`, `execute_batch`, `execute_with_lobs`, and closure-scoped `transaction`, `transaction_with_isolation`, `query_streaming`, and `with_connection`. Each call runs on a blocking thread. Transactions and cursors stay inside a closure so they never cross an `.await`, and dropping a future does not cancel database work that has already started.

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `db2` | on | Links clidriver and enables the real `Db2CliDriver`, `Database::connect`, and `AsyncDatabase::connect`. Turn it off to build the pure-Rust layers without clidriver. |
| `derive` | on | `#[derive(FromRow)]`, from the `halcyon-derive` proc macro. |
| `tokio` | off | The `AsyncDatabase` async facade. |
| `observability` | off | `tracing` spans and `metrics` counters, histograms, and gauges. Your application installs the subscriber, recorder, and exporters. |
| `stress` | off | The `halcyon::stress` fake driver and scenarios used by Halcyon's own concurrency tests and benchmarks. |

## Behavior notes

### Timeouts and lost connections

Halcyon sets no deadline of its own on statements, validation, or connecting. `PoolConfig::acquire_timeout` bounds only the wait for a free pool slot. To bound waits on an unresponsive server, add IBM's `ReceiveTimeout=<seconds>` keyword to the DSN. A statement that times out fails with SQLSTATE `40003` (statement completion unknown).

After any failure that reports the connection lost, including a `40003`, Halcyon retires that connection so the pool replaces it, even when your code handles the error. The error's classification does not change: the statement may have completed, so a write never becomes retriable.

Observed with IBM's macOS ARM64 clidriver v12.1.4 against a paused Db2 container, and not confirmed on other drivers:

- `ReceiveTimeout=5` bounded every statement path, including the `validate_on_acquire` probe, at about 15 seconds.
- Adding `ConnectTimeout=5` to the same DSN removed that bound: statements waited until the server answered again.
- `ReceiveTimeout` does not bound reconnecting. A reconnect after a failed validation, or for a read retried after a lost connection, waits for the server unless the pool is full and `acquire_timeout` ends the wait.

### Long and LOB columns in block fetch

Ordinary cursors fetch rows in blocks. When a result has a long or LOB column, Halcyon fetches at most one row per block, where the C++ library can fetch several. The returned values are complete; only the batching differs. Use `query_streaming` to read large columns in chunks.

### Isolation restore

A per-transaction isolation override restores the configured session level afterward. If none was configured, it restores Cursor Stability, Db2's server default.

## Documentation

- **API reference:** [docs.rs/halcyon](https://docs.rs/halcyon) once 0.1.0 is published. Build it locally with `cargo doc -p halcyon --features tokio,observability --open`.
- **Examples:** [halcyon/examples](https://github.com/YH-Hung/Halcyon-rs/tree/main/halcyon/examples).
- **Changes:** [CHANGELOG.md](https://github.com/YH-Hung/Halcyon-rs/blob/main/CHANGELOG.md).
- **Contributing, building, and testing:** [CONTRIBUTING.md](https://github.com/YH-Hung/Halcyon-rs/blob/main/CONTRIBUTING.md).
- **Releasing:** [docs/RELEASING.md](https://github.com/YH-Hung/Halcyon-rs/blob/main/docs/RELEASING.md).

## Architecture

Halcyon is strictly layered around a thin CLI seam. Only `halcyon-sys` and `halcyon::driver::db2` touch the IBM CLI ABI. Everything above the seam is portable, `unsafe`-free Rust (enforced by `#![deny(unsafe_code)]`) and is tested against a mock driver with no database.

```
facade ─► pool ─► core ─► driver (CliDriver trait) ─► halcyon-sys ─► libdb2
```

| Crate | Role |
|---|---|
| `halcyon` | The library you depend on. |
| `halcyon-sys` | Hand-written FFI declarations for the CLI entry points the seam uses. Implementation detail. |
| `halcyon-derive` | The `#[derive(FromRow)]` proc macro, re-exported by `halcyon`. Implementation detail. |

| C++ Halcyon | Halcyon-rs |
|---|---|
| `ICliDriver` interface | `driver::CliDriver` trait |
| `Db2CliDriver` (the only code including `sqlcli1.h`) | `driver::Db2CliDriver` (the only module using `halcyon-sys`) |
| boundary `Value` variant | `Value` enum (`Null`/`Bool`/`Int`/`Double`/`Text`/`Bytes`) |
| `Result<T>` plus throwing overloads | `Result<T, Error>` only |
| `Error` (code, SQLSTATE, SQLCODE, message, retriable) | `Error`, with identical fields and classification |
| `HALCYON_REFLECT` struct mapping | `#[derive(FromRow)]` |
| `std::future` async over executor threads | `AsyncDatabase` over `tokio::task::spawn_blocking` |

## Roadmap

- [x] Driver seam, structured errors and SQLSTATE classification, connections, statements, result sets, parameters, and `#[derive(FromRow)]`.
- [x] Column-wise batch binding and block/rowset fetch.
- [x] Pooled `Database`, reconnect, classified retry, maintenance, and statement cache.
- [x] Isolation defaults and overrides, transactions, savepoints, and nested scopes.
- [x] Chunked BLOB/CLOB input and streaming LOB output.
- [x] `AsyncDatabase` and observability.
- [x] Mock, stress, loom, ThreadSanitizer, derive UI, benchmark, and live Db2 test coverage in CI.
- [ ] Run the release matrix, then publish `halcyon-sys`, `halcyon-derive`, and `halcyon`, tag `v0.1.0`, and create the GitHub release.

Out of scope for 0.1.0 and tracked for later releases:

- Windows and AIX support.
- Scrollable cursors.
- LOB parameters in `execute_batch`.
- XML and DECFLOAT mappings.
- Public tuning knobs for block size and LOB chunk size.
- Async-native streaming, rather than closure-scoped `spawn_blocking` wrappers.
- A pool health-statistics accessor beyond the current counts and metrics.
- A standalone `Logger` trait for sinks other than `tracing`.
- A seam getter for the current isolation level.

## Versioning and MSRV

- The public `halcyon` API follows SemVer. Before 1.0, breaking changes bump the minor version. `halcyon-sys` and `halcyon-derive` have no stability guarantee of their own; all three crates share one version.
- The MSRV is Rust 1.75, checked in CI. Raising it is a minor version bump.
- The `tokio` feature accepts tokio 1.52 or later. Tokio 1.54 needs Rust 1.85, and Cargo before 1.84 ignores `rust-version` when resolving, so a Rust 1.75 build must pin a compatible tokio: `cargo update -p tokio --precise 1.53.3`. Cargo 1.84 and later picks one itself under `resolver.incompatible-rust-versions = "fallback"`, the default with resolver 3.

## License

BSD 3-Clause, the same as C++ Halcyon. See [LICENSE](https://github.com/YH-Hung/Halcyon-rs/blob/main/LICENSE).
