//! Observability helpers — C++ `observability/instrument.hpp` + label helpers.
//!
//! With `--features observability`, emits the Halcyon metric/span catalog via
//! process-global [`tracing`] and [`metrics`] facades. Without the feature,
//! thin no-op stubs keep facade/cache call sites as a single body.

#[cfg(feature = "observability")]
mod instrument;
#[cfg(feature = "observability")]
mod labels;
#[cfg(not(feature = "observability"))]
mod stubs;

#[cfg(feature = "observability")]
pub(crate) use instrument::{
    emit_retry, emit_stmt_cache, flush_pool_metrics, instrument_execute, instrument_query,
    instrument_transaction, PendingMetrics,
};
#[cfg(not(feature = "observability"))]
pub(crate) use stubs::{
    emit_retry, emit_stmt_cache, instrument_execute, instrument_query, instrument_transaction,
};

#[cfg(all(test, feature = "observability"))]
mod record;
#[cfg(all(test, feature = "observability"))]
pub(crate) use record::*;

#[cfg(all(test, feature = "observability"))]
mod tests_cache;
#[cfg(all(test, feature = "observability"))]
mod tests_facade;
#[cfg(all(test, feature = "observability"))]
mod tests_pool;
