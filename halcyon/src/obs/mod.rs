//! Observability helpers — C++ `observability/instrument.hpp` + label helpers.
//!
//! With `--features observability`, emits the Halcyon metric/span catalog via
//! process-global [`tracing`] and [`metrics`] facades. Without the feature,
//! thin no-op stubs keep facade/cache call sites as a single body.

#[cfg(feature = "observability")]
mod events;
#[cfg(feature = "observability")]
mod instrument;
#[cfg(feature = "observability")]
mod labels;
#[cfg(not(feature = "observability"))]
mod stubs;

#[cfg(feature = "observability")]
pub(crate) use events::{
    emit_connect_fail, emit_connect_ok, emit_isolation_restore_fail, emit_pool_exhausted,
    emit_pool_reap, emit_reconnect_attempt, emit_reconnect_fail, emit_reconnect_ok,
    emit_savepoint_poisoned, emit_stmt_cache_evict, emit_txn_poisoned,
};
#[cfg(feature = "observability")]
pub(crate) use instrument::{
    emit_retry, emit_stmt_cache, flush_pool_metrics, instrument_execute, instrument_query,
    instrument_transaction, PendingMetrics,
};
#[cfg(not(feature = "observability"))]
pub(crate) use stubs::{
    emit_connect_fail, emit_connect_ok, emit_isolation_restore_fail, emit_pool_exhausted,
    emit_pool_reap, emit_reconnect_attempt, emit_reconnect_fail, emit_reconnect_ok, emit_retry,
    emit_savepoint_poisoned, emit_stmt_cache, emit_stmt_cache_evict, emit_txn_poisoned,
    instrument_execute, instrument_query, instrument_transaction,
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
#[cfg(all(test, feature = "observability"))]
mod tests_transaction;
