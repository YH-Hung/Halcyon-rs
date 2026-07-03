//! # Halcyon
//!
//! A high-level, ergonomic IBM Db2 client for Rust, built on the IBM Db2 CLI
//! (`libdb2` from clidriver). Rust counterpart of the C++
//! [Halcyon](https://github.com/YH-Hung/Halcyon) library.
//!
//! **Status: early development.** The target architecture mirrors the C++
//! layering. The driver seam and the core data path ([`Connection`],
//! [`Statement`], [`ResultSet`]/[`Row`], `?`/`:name` params, row → tuple mapping)
//! are implemented; the pool and facade layers are being ported. See the README
//! roadmap:
//!
//! ```text
//! facade ─► pool ─► core ─► driver (CliDriver seam) ─► halcyon-sys ─► libdb2
//! (planned) (planned) (done)     (implemented)
//! ```
//!
//! Only `driver::db2` (feature `db2`) touches the CLI ABI; everything above the
//! seam is portable, mockable, `unsafe`-free Rust.
//!
//! ```ignore
//! use std::sync::Arc;
//! use halcyon::{Connection, driver::{Db2CliDriver, ConnectionParams}};
//!
//! # fn main() -> halcyon::Result<()> {
//! let driver = Arc::new(Db2CliDriver::new());
//! let conn = Connection::open(driver, &ConnectionParams {
//!     connection_string: "DATABASE=sample;HOSTNAME=localhost;...".into(),
//! })?;
//!
//! let mut rows = conn.query("SELECT id, name FROM users WHERE id > ?", (10,))?;
//! while let Some(row) = rows.next()? {
//!     let (id, name): (i64, String) = row.get()?;
//!     println!("{id}: {name}");
//! }
//! # Ok(())
//! # }
//! ```

// The seam rule, enforced: unsafe lives only in driver::db2 (which carries an
// explicit allow) and in the halcyon-sys crate.
#![deny(unsafe_code)]

// Lets `#[derive(FromRow)]`'s generated `::halcyon::…` paths resolve inside this
// crate's own tests, exactly as they do downstream.
extern crate self as halcyon;

mod core;
pub mod driver;
mod error;
mod value;

// Shared, scriptable mock driver for the layers above the seam. Test-only, so
// it never ships in a normal build; every upper-layer unit test builds on it.
#[cfg(test)]
mod testing;

pub use core::{
    Connection, FromColumn, FromRow, IntoParams, Named, Params, QueryMap, ResultSet, Row, Statement,
};
pub use error::{Error, ErrorCode, Result};
pub use value::Value;

/// Derives [`FromRow`] for a struct, mapping a result-set row into its fields by
/// position (feature `derive`, on by default). Field types must implement
/// [`FromColumn`].
#[cfg(feature = "derive")]
pub use halcyon_derive::FromRow;
