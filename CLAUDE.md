# Halcyon-rs

Rust counterpart of the C++ Db2 driver **Halcyon** (`../Halcyon`). The C++ repo is the design reference: when porting a layer, read the corresponding C++ header/source first and preserve its semantics (locking discipline, error classification, retry rules), then express it idiomatically in Rust.

## Hard rules

- **The seam rule:** only `halcyon-sys` and `halcyon/src/driver/db2.rs` may touch the IBM CLI ABI. `#![deny(unsafe_code)]` in `halcyon` enforces this — do not add `allow(unsafe_code)` anywhere else.
- Everything above the seam must be testable against a mock `CliDriver` with no live database and no clidriver installed (`cargo test -p halcyon --no-default-features` must always pass).
- Errors are `Result<T, halcyon::Error>` only — no panicking public APIs. `Error.retriable` drives auto-retry; classification lives in `driver/sqlstate.rs` and must match the C++ `classify_sqlstate`.
- No hard dependencies for optional functionality: Db2 linkage is behind the `db2` feature (default on); async and observability will be feature-gated too.

## Build & test

```bash
cargo test                                    # full (needs clidriver; resolved via DB2_CLIDRIVER_ROOT)
cargo test -p halcyon --no-default-features   # pure-Rust layers, no clidriver
cargo clippy --all-targets                    # keep warning-free
```

`.cargo/config.toml` defaults `DB2_CLIDRIVER_ROOT` to `../Halcyon/third_party/clidriver` (sibling C++ checkout); a real environment variable overrides it.

## Design decisions (settled)

- Sync core; async ships later as a feature-gated `tokio` facade over `spawn_blocking`.
- Hand-written FFI in `halcyon-sys` (no bindgen, no odbc-api): only the ~19 CLI entry points the seam uses, with type widths verified against the vendored clidriver headers (`SQLLEN` = i64, handles = pointers).
- Roadmap and layer mapping live in README.md.
