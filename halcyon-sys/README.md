# halcyon-sys

Raw FFI bindings to the IBM Db2 CLI (`libdb2` from **clidriver**) for the
[`halcyon`](https://crates.io/crates/halcyon) Db2 client. Hand-written and
deliberately minimal: only the entry points, types, and constants Halcyon's
driver seam uses.

**This crate is an implementation detail of `halcyon`** and carries no
stability guarantee of its own — depend on `halcyon` instead.

## Building

The IBM clidriver is proprietary, user-supplied, and never redistributed by
this crate. The build script locates it via:

1. the `DB2_CLIDRIVER_ROOT` environment variable, or
2. `third_party/clidriver/` at the workspace root.

The clidriver `lib/` directory is exported to dependents as `DEP_DB2_LIB_DIR`
(via `links = "db2"`), and must be resolvable at runtime (rpath or
`LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH`).

## License

BSD 3-Clause — see [LICENSE](LICENSE).
