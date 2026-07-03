use std::env;

/// Embeds the clidriver lib directory as an rpath in this package's linked
/// targets (tests, examples, binaries) so they run without setting
/// DYLD_LIBRARY_PATH / LD_LIBRARY_PATH — mirrors the C++ CMake behavior.
/// `DEP_DB2_LIB_DIR` is published by halcyon-sys and absent when the `db2`
/// feature is off.
fn main() {
    if let Ok(lib) = env::var("DEP_DB2_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{lib}");
    }
}
