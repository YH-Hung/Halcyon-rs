//! Observability helpers — C++ `observability/instrument.hpp` + label helpers.
//!
//! Enabled only with `--features observability`. Emits the Halcyon metric/span
//! catalog via process-global [`tracing`] and [`metrics`] facades.

mod labels;

// Re-exported for facade/pool/cache call sites (wired in later tasks).
#[allow(unused_imports)]
pub(crate) use labels::{code_label, op_label};
