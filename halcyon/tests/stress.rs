//! Concurrency-correctness scenarios (feature `stress`). Run under
//! ThreadSanitizer in CI. Each drives the pool/facade over the deterministic
//! `ConcurrentFakeDriver` from many threads and asserts the pool invariants.
#![cfg(feature = "stress")]

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use halcyon::driver::ConnectionParams;
use halcyon::stress::{
    config_for, make_pool_contention, make_reconnect_faults, make_txn_churn, run_workload,
    ConcurrentFakeDriver, RunConfig, Scenario, Stop,
};
use halcyon::{Database, Isolation};

fn open(fake: Arc<ConcurrentFakeDriver>, scenario: Scenario, pool_max: usize) -> Database {
    Database::open(
        fake,
        ConnectionParams {
            connection_string: "dsn".into(),
            isolation: None,
        },
        config_for(scenario, pool_max),
    )
    .expect("pool warmup")
}

#[test]
fn pool_contention_stays_consistent() {
    let fake = Arc::new(ConcurrentFakeDriver::new());
    let db = open(fake.clone(), Scenario::Pool, 8);

    let body = make_pool_contention(&db);
    let cfg = RunConfig {
        threads: 32,
        stop: Stop::Iters(20_000),
        warmup: None,
        seed: 1,
    };
    let r = run_workload("pool", &cfg, body);

    assert!(!r.failed, "{:?}", r.first_error);
    assert!(fake.peak_in_flight() <= 8, "never more leases than max");
    assert_eq!(db.pool().active_count(), 0);
    assert_eq!(db.pool().idle_count(), db.pool().total_count());
}

#[test]
fn reconnect_and_retry_recover_under_faults() {
    let fake = Arc::new(ConcurrentFakeDriver::new());
    fake.fail_execute_every.store(7, Ordering::Relaxed); // retriable statement errors
    fake.kill_connection_every.store(11, Ordering::Relaxed); // forces validate reconnects
    let db = open(fake.clone(), Scenario::Reconnect, 6);
    let connects_after_warmup = fake.connect_calls();

    let recovered = AtomicI64::new(0);
    let body = make_reconnect_faults(&db, &recovered);
    let cfg = RunConfig {
        threads: 12,
        stop: Stop::Iters(30_000),
        warmup: None,
        seed: 2,
    };
    let r = run_workload("reconnect", &cfg, body);

    assert!(!r.failed, "{:?}", r.first_error); // no wrong scalar / non-retriable
    assert!(
        fake.connect_calls() > connects_after_warmup,
        "reconnects fired"
    );
    assert!(fake.execute_calls() > r.ops as i64, "retries fired");
    assert!(
        recovered.load(Ordering::Relaxed) > (r.ops as i64) * 9 / 10,
        "vast majority recovered"
    );
    assert_eq!(db.pool().active_count(), 0); // no leaked leases
}

#[test]
fn transaction_churn_commits_and_rollbacks_balance() {
    let fake = Arc::new(ConcurrentFakeDriver::new());
    let db = open(fake.clone(), Scenario::Txn, 6);

    let body = make_txn_churn(&db);
    let cfg = RunConfig {
        threads: 8,
        stop: Stop::Iters(16_000),
        warmup: None,
        seed: 3,
    };
    let r = run_workload("txn", &cfg, body);

    assert!(!r.failed, "{:?}", r.first_error);
    let txns = fake.auto_commit_off(); // each begin() flips off
    assert_eq!(txns, 16_000);
    assert_eq!(fake.commit_calls() + fake.rollback_calls(), txns);
    assert_eq!(fake.auto_commit_on(), txns); // each tx restores it
    assert_eq!(db.pool().active_count(), 0); // no tx leaks a connection
}

#[test]
fn reaper_races_acquire_release_safely() {
    let fake = Arc::new(ConcurrentFakeDriver::new());
    let db = open(fake.clone(), Scenario::Lifecycle, 6); // reaper on, tiny intervals

    let body = make_pool_contention(&db); // acquire/query/release loop
    let cfg = RunConfig {
        threads: 16,
        stop: Stop::Duration(Duration::from_millis(300)), // let the reaper churn
        warmup: None,
        seed: 4,
    };
    let r = run_workload("lifecycle", &cfg, body);

    assert!(!r.failed, "{:?}", r.first_error);
    assert!(
        fake.peak_in_flight() <= 6,
        "reaper never reaps a live lease"
    );
    assert_eq!(db.pool().active_count(), 0);
    assert!(
        db.pool().total_count() >= db.pool().idle_count(),
        "accounting consistent"
    );
}

#[test]
fn concurrent_savepoints_restore_each_connection_session() {
    const THREADS: usize = 8;
    const ITERS: usize = 200;
    const TRANSACTIONS: i64 = (THREADS * ITERS) as i64;

    let fake = Arc::new(ConcurrentFakeDriver::new());
    let db = open(fake.clone(), Scenario::Txn, 8);
    let failures = AtomicI64::new(0);

    std::thread::scope(|scope| {
        for worker in 0..THREADS {
            let db = &db;
            let failures = &failures;
            scope.spawn(move || {
                for iteration in 0..ITERS {
                    let isolation = if iteration % 2 == 0 {
                        Isolation::CursorStability
                    } else {
                        Isolation::RepeatableRead
                    };
                    let result = db.transaction_with_isolation(isolation, |tx| {
                        tx.execute("INSERT INTO t VALUES (?)", (worker as i64,))?;
                        let savepoint = tx.savepoint()?;
                        tx.execute("INSERT INTO t VALUES (?)", (iteration as i64,))?;
                        if iteration % 3 == 0 {
                            savepoint.rollback()?;
                        } else {
                            savepoint.release()?;
                        }
                        tx.nested(|nested| {
                            nested
                                .execute("INSERT INTO t VALUES (?)", ((worker + iteration) as i64,))
                        })
                    });
                    if result.is_err() {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    assert_eq!(failures.load(Ordering::Relaxed), 0);
    assert_eq!(fake.auto_commit_off(), TRANSACTIONS);
    assert_eq!(fake.auto_commit_on(), TRANSACTIONS);
    assert_eq!(fake.set_isolation_calls(), 2 * TRANSACTIONS);
    assert_eq!(fake.commit_calls(), TRANSACTIONS);
    for state in fake.connection_states() {
        assert!(state.autocommit, "connection left with autocommit off");
        assert!(
            state.isolation.is_none() || state.isolation == Some(Isolation::CursorStability),
            "connection left mid-override: {:?}",
            state.isolation
        );
    }
    assert_eq!(db.pool().active_count(), 0);
    assert_eq!(db.pool().idle_count(), db.pool().total_count());
}
