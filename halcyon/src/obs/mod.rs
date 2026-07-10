//! Observability helpers — C++ `observability/instrument.hpp` + label helpers.
//!
//! Enabled only with `--features observability`. Emits the Halcyon metric/span
//! catalog via process-global [`tracing`] and [`metrics`] facades.

mod labels;

#[allow(unused_imports)] // wired by later tasks
pub(crate) use labels::{code_label, op_label};

#[cfg(test)]
#[allow(unused_imports)] // exercised by later task tests
mod record;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use record::*;

