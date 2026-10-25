# Contributing to Halcyon-rs

Halcyon-rs ports the C++ [Halcyon](https://github.com/YH-Hung/Halcyon) library. When you port or change a layer, read the corresponding C++ header and source first and keep its semantics: locking discipline, error classification, and retry rules. [AGENTS.md](AGENTS.md) lists the hard rules, including the seam rule: only `halcyon-sys` and `halcyon/src/driver/db2.rs` may touch the IBM CLI ABI.

## Build and test

```bash
# Full build and tests (links libdb2; needs clidriver)
cargo build
cargo test

# Pure-Rust layers only, with no clidriver installed
cargo build -p halcyon --no-default-features
cargo test  -p halcyon --no-default-features

cargo clippy --all-targets   # keep warning-free
```

[.cargo/config.toml](.cargo/config.toml) defaults `DB2_CLIDRIVER_ROOT` to `../Halcyon/third_party/clidriver`, so a sibling checkout of the C++ repo works with no setup. A real environment variable overrides it, and `third_party/clidriver/` at this repository's root also works.

The clidriver `lib/` directory is embedded as an rpath in this repository's test and example binaries, so on **Linux** they run without `LD_LIBRARY_PATH`. On **macOS** the rpath is not consulted: IBM ships `libdb2.dylib` with a bare install name, and rpaths apply only to `@rpath/` names. Set `DYLD_LIBRARY_PATH` to the clidriver `lib/` directory to run binaries there.

## Live integration test

The DSN-gated suite in [halcyon/tests/db2_roundtrip.rs](halcyon/tests/db2_roundtrip.rs) covers the core and facade paths, statement reuse and cancel safety, bounded and long-column block fetch, savepoints and isolation visibility, and deterministic 32 MiB LOB checks. Every test prints a skip and returns when `HALCYON_TEST_DSN` is unset, so a pass without a DSN proves only that the suite compiles.

To run it against a local Db2:

```bash
docker compose -f docker/docker-compose.yml up -d   # wait until healthy (first boot takes a few minutes)
export HALCYON_TEST_DSN='DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=halcyon;'
cargo test --test db2_roundtrip -- --nocapture
```

On macOS, run the suite from the host through Docker Desktop's port proxy, with `DYLD_LIBRARY_PATH` set as above. This was verified on Apple silicon with IBM's `macarm64` clidriver (v12.1.4) against this compose file's Db2 11.5.9.0 and against Db2 12.1.5.0. The Db2 image is linux/amd64, so Apple silicon emulates it.

## Derive UI tests

`#[derive(FromRow)]`'s compile errors are covered by [trybuild](halcyon/tests/ui/) snapshots. Because `.stderr` output is specific to a rustc version, they run only under `HALCYON_UI_TESTS=1` (the pinned `derive-ui` CI job) and pass as a no-op otherwise. Regenerate them after an intended change:

```bash
TRYBUILD=overwrite HALCYON_UI_TESTS=1 cargo test -p halcyon --no-default-features --features derive --test derive_ui
```

## Continuous integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs `fmt`, `clippy`, the pure-Rust feature matrix, the Rust 1.75 MSRV lane, Db2-linked Linux and macOS builds, deterministic stress, ThreadSanitizer stress, loom models, derive UI tests, and the release checks in [ci/check-release.sh](ci/check-release.sh). Its `live-db2` job starts Dockerized Db2 and runs the DSN-gated sync and async round-trips on every pull request to `main`, on pushes to `main` and release tags, and on manual dispatch.

## Releasing

Follow [docs/RELEASING.md](docs/RELEASING.md). Record user-visible changes in the `[Unreleased]` section of [CHANGELOG.md](CHANGELOG.md) as you make them.
