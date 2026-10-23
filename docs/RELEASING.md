# Releasing Halcyon-rs

Checklist for publishing `halcyon-sys`, `halcyon-derive`, and `halcyon` to
crates.io. Publish order matters: `halcyon` depends on the other two by
version, so they must exist on the registry first.

## 1. Green matrix

All of the following must pass on the release commit:

```bash
bash ci/check-release.sh
cargo fmt --all --check
cargo clippy --all-targets --features tokio,observability,stress -- -D warnings
cargo clippy -p halcyon --no-default-features --features derive,tokio,observability --all-targets -- -D warnings

# Pure-Rust suite (must pass with no clidriver installed)
cargo test -p halcyon --no-default-features
cargo test -p halcyon --no-default-features --features derive,tokio,observability,stress -- --test-threads=1

# Full suite (needs clidriver; x86_64 only)
cargo test

# Concurrency gate
RUSTFLAGS="--cfg loom" cargo test -p halcyon --no-default-features --release --lib pool::loom_tests
# TSan stress (nightly, x86_64): see .github/workflows/ci.yml `stress-tsan`

# Live Db2 round-trip (Linux x86_64 on the Db2 network; see README)
HALCYON_TEST_DSN=... cargo test --test db2_roundtrip -- --nocapture

# docs.rs simulation
DOCS_RS=1 cargo doc -p halcyon-sys --no-deps
cargo doc -p halcyon --no-default-features --features derive,tokio,observability --no-deps
```

CI (`.github/workflows/ci.yml`) runs the feature, MSRV, native, concurrency,
live, documentation, package-content, and downstream-consumer gates. Confirm
that loom and live Db2 actually ran on the release commit. Driverless checks
using `DOCS_RS=1` are compile/docs evidence, not native runtime verification.
The main crate must still pass registry package verification after its two
dependencies are published.

## 2. Version bump

- Bump `workspace.package.version` in the root [Cargo.toml](../Cargo.toml)
  (all three crates version together).
- Bump the `version =` on the `halcyon-sys` and `halcyon-derive` path
  dependencies in [halcyon/Cargo.toml](../halcyon/Cargo.toml) to match.
- Move the `[Unreleased]` section of [CHANGELOG.md](../CHANGELOG.md) to the new
  version with today's date; add a fresh empty `[Unreleased]`.
- SemVer: breaking changes to the public `halcyon` API require a minor bump
  while pre-1.0 (0.x → 0.x+1). `halcyon-sys`/`halcyon-derive` are
  implementation details, but they version in lockstep anyway.
- MSRV (currently 1.75): raising it is a minor bump, never a patch; update
  `workspace.package.rust-version`, the README, and CI's MSRV job together.

## 3. Package sanity

```bash
cargo package -p halcyon-sys
cargo package -p halcyon-derive
cargo package -p halcyon --no-verify --list   # verify runs after deps publish
```

Check that each lists `LICENSE`, `README.md`, and no stray files.

## 4. Publish (in order)

```bash
cargo publish -p halcyon-sys
cargo publish -p halcyon-derive
# wait for both to be indexed, then:
cargo publish -p halcyon
```

`cargo publish -p halcyon` performs the full verification build against the
now-published deps. docs.rs builds `halcyon` with `--no-default-features
--features derive,tokio,observability` (see `[package.metadata.docs.rs]`) and
`halcyon-sys` with the `DOCS_RS` build-script escape, so neither needs
clidriver.

## 5. Tag + GitHub release

```bash
git tag -a v<version> -m "halcyon <version>"
git push origin main --tags
```

Create a GitHub release from the tag, pasting the CHANGELOG section. Confirm
the `live-db2` CI job on the release commit is green and docs.rs finished
building all three crates.
