//! Observability helpers — C++ `observability/instrument.hpp` + label helpers.
//!
//! Enabled only with `--features observability`. Emits the Halcyon metric/span
//! catalog via process-global [`tracing`] and [`metrics`] facades.

mod instrument;
mod labels;

pub(crate) use instrument::{
    emit_retry, flush_pool_metrics, instrument_execute, instrument_query,
    instrument_transaction, PendingMetrics,
};
#[allow(unused_imports)] // re-export for pool/cache call sites
pub(crate) use labels::{code_label, op_label};

#[cfg(test)]
mod record;
#[cfg(test)]
pub(crate) use record::*;

#[cfg(test)]
mod tests_facade;
#[cfg(test)]
mod tests_pool;
