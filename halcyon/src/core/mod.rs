//! The portable core data path above the seam.
//!
//! [`Connection`] and [`Statement`] drive the CLI seam through the mockable
//! `CliDriver` trait; [`ResultSet`]/[`Row`] stream results; [`FromColumn`] and
//! [`FromRow`] map columns and rows into Rust types; [`Params`]/[`IntoParams`]
//! and [`Named`] carry positional (`?`) and named (`:name`) parameters. Every
//! type here is portable, `unsafe`-free, and testable against a mock driver
//! with no live database.

mod column;
mod connection;
mod params;
mod result_set;
mod transaction;

#[cfg(all(test, feature = "derive"))]
mod derive_tests;

pub use column::FromColumn;
pub use connection::{Connection, Statement};
pub use params::{IntoParams, Named, Params};
pub use result_set::{FromRow, QueryMap, ResultSet, Row};
pub use transaction::Transaction;
// Shared with the facade's owned pooled cursor / RAII transaction, which live
// outside `core` and so can't reach these through the private submodules.
pub(crate) use transaction::terminate;

use crate::error::{Error, ErrorCode};

/// Constructs a `Mapping`-class error, the class this layer raises for arity,
/// type, and NULL mismatches and unknown named parameters (C++ `mapping_error`).
pub(crate) fn mapping(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Mapping, message)
}
