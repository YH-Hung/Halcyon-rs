//! Compile-fail (UI) tests for `#[derive(FromRow)]`, asserting the derive
//! rejects enums, unions, and unit structs with clear messages.
//!
//! trybuild `.stderr` snapshots are tied to a specific rustc (generated on
//! 1.94.1), so this harness runs only when `HALCYON_UI_TESTS=1` — the pinned
//! `derive-ui` CI job — and is a no-op pass elsewhere, keeping `cargo test`
//! green on any toolchain (the same skip-when-unconfigured pattern as the live
//! Db2 test). The whole file also compiles out when the `derive` feature is off.
//!
//! Regenerate after an intended message or toolchain change with:
//!   TRYBUILD=overwrite HALCYON_UI_TESTS=1 \
//!     cargo test -p halcyon --no-default-features --features derive --test derive_ui
#![cfg(feature = "derive")]

#[test]
fn from_row_rejects_unsupported_inputs() {
    if std::env::var_os("HALCYON_UI_TESTS").is_none() {
        eprintln!("HALCYON_UI_TESTS not set; skipping toolchain-sensitive derive UI tests");
        return;
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
