# Halcyon-rs

A modern Rust client library for **IBM Db2**, built on the IBM Db2 CLI (`libdb2`). The Rust counterpart of the C++ [Halcyon](https://github.com/YH-Hung/Halcyon) library — same layered architecture, same error model, same driver-seam philosophy, expressed idiomatically in Rust.

> **Status: early development.** The driver seam is complete; the core/pool/facade layers are being ported. See the roadmap below.

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
| `HALCYON_REFLECT` struct mapping | `#[derive(FromRow)]` proc macro *(planned)* |
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

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `db2` | on | Links clidriver and enables the real `Db2CliDriver`. Disable to build/test the pure-Rust layers without Db2 installed. |

## Roadmap

- [x] Workspace scaffolding (`halcyon`, `halcyon-sys`)
- [x] Error model: `ErrorCode`, `Error`, SQLSTATE→code classification, `retriable` flag
- [x] Boundary `Value` type with ergonomic `From` conversions
- [x] `CliDriver` seam trait (mockable; unit tests need no live database)
- [x] `Db2CliDriver`: connect/prepare/bind/execute/fetch, chunked array-binding batch, transactions — port of the C++ driver seam (not yet exercised against a live Db2)
- [x] Batch shape validation (rectangular, type-homogeneous) enforced inside the driver
- [ ] Core: `Connection`, `Statement`, `ResultSet`, named (`:name`) & positional (`?`) parameters, row → tuple mapping
- [ ] `#[derive(FromRow)]` proc macro (`halcyon-derive`)
- [ ] Pool: min/max bounds, acquire timeout, validation, idle/lifetime reaping, transparent reconnect, safe auto-retry
- [ ] Facade: `Database` entry point, RAII + closure transactions, `execute_batch`
- [ ] Statement cache (per-connection LRU)
- [ ] `tokio` async facade (feature-gated, `spawn_blocking`)
- [ ] Observability: `metrics`/`tracing`-facade adapters (feature-gated)
- [ ] Integration tests against Dockerized Db2 (reuse the C++ repo's compose setup)

## License

BSD 3-Clause — same as the C++ Halcyon.
