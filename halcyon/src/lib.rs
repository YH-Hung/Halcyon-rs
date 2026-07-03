//! # Halcyon
//!
//! A high-level, ergonomic IBM Db2 client for Rust, built on the IBM Db2 CLI
//! (`libdb2` from clidriver). Rust counterpart of the C++
//! [Halcyon](https://github.com/YH-Hung/Halcyon) library.
//!
//! **Status: early development.** The target architecture mirrors the C++
//! layering, of which the driver seam is implemented so far — the layers above
//! it (core, pool, facade) are being ported; see the README roadmap:
//!
//! ```text
//! facade ─► pool ─► core ─► driver (CliDriver seam) ─► halcyon-sys ─► libdb2
//! (planned) (planned) (planned)  (implemented)
//! ```
//!
//! Only `driver::db2` (feature `db2`) touches the CLI ABI; everything above the
//! seam is portable, mockable, `unsafe`-free Rust.

// The seam rule, enforced: unsafe lives only in driver::db2 (which carries an
// explicit allow) and in the halcyon-sys crate.
#![deny(unsafe_code)]

pub mod driver;
mod error;
mod value;

pub use error::{Error, ErrorCode, Result};
pub use value::Value;
