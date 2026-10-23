#!/usr/bin/env bash
# Driverless release checks, including compilation as a downstream dependency.
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

DOCS_RS=1 RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
# Check every feature combination together, including the native seam, without
# linking or pretending that a driverless check is a native runtime test.
DOCS_RS=1 cargo check --workspace --all-features --all-targets

for crate in halcyon-sys halcyon-derive halcyon; do
  package_files="$(cargo package -p "$crate" --allow-dirty --list)"
  for required in LICENSE README.md; do
    if ! printf '%s\n' "$package_files" | grep -Fxq "$required"; then
      echo "$crate package is missing $required" >&2
      exit 1
    fi
  done
done
# The main crate's registry dependencies must be published first; its archive
# verification remains a mandatory publishing gate (docs/RELEASING.md).
DOCS_RS=1 cargo package -p halcyon-sys --allow-dirty
cargo package -p halcyon-derive --allow-dirty

consumer_dir="$(mktemp -d)"
trap 'rm -rf "$consumer_dir"' EXIT
cat > "$consumer_dir/Cargo.toml" <<MANIFEST
[package]
name = "halcyon-release-consumer"
version = "0.0.0"
edition = "2021"

[dependencies]
halcyon = { path = "$repo_root/halcyon", default-features = false, features = ["derive", "tokio", "observability"] }
MANIFEST
mkdir -p "$consumer_dir/src"
cat > "$consumer_dir/src/main.rs" <<'RUST'
use halcyon::{AsyncDatabase, Database, FromRow, Result};

#[derive(FromRow)]
struct Record {
    id: i64,
    name: Option<String>,
}

fn sync_api(db: &Database) -> Result<Vec<Record>> {
    db.transaction(|tx| {
        tx.execute("INSERT INTO records(id, name) VALUES (?, ?)", (1_i64, "sample"))?;
        tx.query_as("SELECT id, name FROM records WHERE id = ?", (1_i64,))
    })
}

async fn async_api(db: &AsyncDatabase) -> Result<Vec<Record>> {
    db.query_as("SELECT id, name FROM records WHERE id = ?", (1_i64,)).await
}

fn main() {
    let _ = (sync_api, async_api);
    let record = Record { id: 1, name: None };
    let _ = (record.id, record.name);
}
RUST
cargo check --manifest-path "$consumer_dir/Cargo.toml"
