//! Pool config presets and the workload builders each scenario runs. The four
//! scenarios that map onto today's Rust surface: pool contention, reconnect +
//! transient faults, transaction churn, reaper race. (Statement-cache and
//! async scenarios are deferred to Phases 5/6 — see the design spec §6.)

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use crate::error::{Error, ErrorCode};
use crate::facade::Database;
use crate::pool::PoolConfig;
use crate::retry::BackoffPolicy;

use super::runner::WorkerCtx;

#[derive(Clone, Copy)]
pub enum Scenario {
    Pool,
    Reconnect,
    Txn,
    Lifecycle,
}

/// The tuned `PoolConfig` for each scenario. `pool_max` is the only sweepable
/// knob the perf harness varies; everything else is scenario-intrinsic. Backoff
/// is zero so retries never really sleep (deterministic).
pub fn config_for(id: Scenario, pool_max: usize) -> PoolConfig {
    let max = pool_max.max(1);
    let min = if max < 2 { max } else { 2 };
    let mut cfg = PoolConfig {
        min,
        max,
        acquire_timeout: Duration::from_millis(5000),
        idle_timeout: Duration::from_millis(60_000),
        max_lifetime: Duration::from_millis(1_800_000),
        validate_on_acquire: false,
        backoff: BackoffPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            max_attempts: 3,
        },
        maintenance_interval: Duration::from_millis(1000),
        start_maintenance_thread: false,
        // Production default: the stress scenarios (and the TSan job) exercise
        // the per-connection statement cache path.
        statement_cache_size: 64,
        isolation: None,
    };
    match id {
        Scenario::Pool => {
            cfg.acquire_timeout = Duration::from_millis(50);
        }
        Scenario::Reconnect => {
            cfg.validate_on_acquire = true;
            cfg.backoff.max_attempts = 3;
        }
        Scenario::Txn => {}
        Scenario::Lifecycle => {
            cfg.start_maintenance_thread = true;
            cfg.maintenance_interval = Duration::from_millis(1);
            cfg.idle_timeout = Duration::from_millis(1);
            cfg.max_lifetime = Duration::from_millis(5);
        }
    }
    cfg
}

/// A canonical scalar SELECT the fake/live encode as `n`.
pub fn select_n(n: i64) -> String {
    format!("SELECT {n} FROM SYSIBM.SYSDUMMY1")
}

// --- Scenario 1: raw pool acquire/release contention (pool mechanics only) ---
pub fn make_pool_contention(db: &Database) -> impl Fn(&mut WorkerCtx) + Sync + '_ {
    move |ctx| {
        let lease = match db.pool().acquire() {
            Ok(l) => l,
            Err(e) => {
                if e.code != ErrorCode::Pool {
                    ctx.fail(format!("unexpected acquire error: {e}"));
                } else {
                    ctx.note_error(); // tolerated acquire-timeout under contention
                }
                return;
            }
        };
        match lease.query_as::<(i64,)>(&select_n(7), ()) {
            Ok(rows) if rows == vec![(7,)] => {}
            Ok(_) => ctx.fail("wrong scalar from pooled query"),
            Err(e) => ctx.fail(format!("query failed: {e}")),
        }
    }
}

// --- Scenario 2: reconnect + transient-fault injection under load ---
pub fn make_reconnect_faults<'a>(
    db: &'a Database,
    recovered: &'a AtomicI64,
) -> impl Fn(&mut WorkerCtx) + Sync + 'a {
    move |ctx| {
        match db.query_as::<(i64,)>(&select_n(7), ()) {
            Ok(rows) if rows == vec![(7,)] => {
                recovered.fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) => ctx.fail("wrong scalar after recovery"),
            Err(e) => {
                // Exhausting a bounded retry budget surfaces a *retriable* error:
                // allowed. A non-retriable error means recovery corrupted the path.
                if !e.retriable {
                    ctx.fail(format!("non-retriable error surfaced: {e}"));
                } else {
                    ctx.note_error();
                }
            }
        }
    }
}

// --- Scenario 3: transaction churn (commit + rollback) ---
pub fn make_txn_churn(db: &Database) -> impl Fn(&mut WorkerCtx) + Sync + '_ {
    move |ctx| {
        let do_commit = (ctx.next_u64() & 1) == 0;
        let r: crate::error::Result<i64> = db.transaction(|tx| {
            let rows = tx.query_as::<(i64,)>(&select_n(7), ())?;
            if rows != vec![(7,)] {
                return Err(Error::new(ErrorCode::Mapping, "wrong scalar in tx"));
            }
            if do_commit {
                Ok(1)
            } else {
                Err(Error::new(ErrorCode::Unknown, "intentional rollback"))
            }
        });
        // A committed tx returns Ok; a rolled-back tx returns our forced error.
        // A Mapping error means the in-tx read was wrong -> real failure.
        if let Err(e) = r {
            if e.code == ErrorCode::Mapping {
                ctx.fail("in-transaction read returned wrong scalar");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_for_sets_min_and_max() {
        let c = config_for(Scenario::Pool, 8);
        assert_eq!(c.max, 8);
        assert_eq!(c.min, 2);
        assert!(!c.start_maintenance_thread);
    }

    #[test]
    fn config_for_clamps_tiny_max() {
        let c = config_for(Scenario::Txn, 0);
        assert_eq!(c.max, 1);
        assert_eq!(c.min, 1);
    }

    #[test]
    fn lifecycle_enables_the_reaper() {
        let c = config_for(Scenario::Lifecycle, 6);
        assert!(c.start_maintenance_thread);
        assert!(c.max_lifetime <= std::time::Duration::from_millis(5));
    }

    #[test]
    fn select_n_embeds_the_value() {
        assert_eq!(select_n(7), "SELECT 7 FROM SYSIBM.SYSDUMMY1");
    }
}
