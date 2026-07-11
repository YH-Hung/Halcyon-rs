use std::env;
use std::path::PathBuf;

/// Locates the IBM Db2 CLI driver (clidriver) and links `libdb2`.
///
/// Resolution order:
/// 1. `DB2_CLIDRIVER_ROOT` environment variable
/// 2. `third_party/clidriver` at the workspace root
fn main() {
    // docs.rs has no clidriver and never runs the resulting rlib; skip locating
    // and linking libdb2 so the published docs build succeeds.
    if env::var("DOCS_RS").is_ok() {
        return;
    }

    println!("cargo:rerun-if-env-changed=DB2_CLIDRIVER_ROOT");

    let root = env::var("DB2_CLIDRIVER_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
        .or_else(|| {
            let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
            let vendored = manifest.join("../third_party/clidriver");
            vendored.is_dir().then_some(vendored)
        });

    let Some(root) = root else {
        panic!(
            "IBM Db2 CLI driver not found. Set DB2_CLIDRIVER_ROOT to your \
             clidriver directory (the one containing lib/libdb2), or vendor it \
             at third_party/clidriver. To build without Db2 (pure-Rust layers \
             only), use: cargo build -p halcyon --no-default-features"
        );
    };

    let lib = root.join("lib");
    if !lib.is_dir() {
        panic!(
            "DB2_CLIDRIVER_ROOT points at {} but it has no lib/ directory",
            root.display()
        );
    }

    println!("cargo:rustc-link-search=native={}", lib.display());
    println!("cargo:rustc-link-lib=dylib=db2");
    // rpath for this package's own test harness…
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib.display());
    // …and exposed to dependents as DEP_DB2_LIB_DIR (via `links = "db2"`) so
    // they can embed the same rpath and run without DYLD/LD_LIBRARY_PATH.
    println!("cargo:lib_dir={}", lib.display());
}
