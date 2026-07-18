//! Live Db2 round-trip — the seam exercised against a real database.
//!
//! Gated on `HALCYON_TEST_DSN`: when it is unset the test is a no-op pass, so
//! `cargo test` stays green with no database (mirrors the C++ `HALCYON_TEST_DSN`
//! convention). Point it at a running Db2 to prove the seam end to end:
//!
//! ```sh
//! docker compose -f docker/docker-compose.yml up -d      # wait for healthy
//! export HALCYON_TEST_DSN='DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;\
//!   PROTOCOL=TCPIP;UID=db2inst1;PWD=halcyon;'
//! cargo test --test db2_roundtrip -- --nocapture
//! ```
//!
//! The whole file is `db2`-feature-gated, so `cargo test --no-default-features`
//! compiles it to nothing and the pure-Rust invariant is preserved.
//!
//! Covers the Phase 1 core surface plus the seam's transaction control in
//! [`live_round_trip`] (connect, DDL, positional/named/batch inserts, typed
//! streaming reads including NULL, commit/rollback via the driver primitives),
//! and the Phase 3 [`Database`] facade in [`live_facade_round_trip`] (ergonomic
//! `connect`, closure/RAII [`halcyon::Transaction`] commit and rollback).
//! [`live_statement_cache_reuse`] proves the Phase 5 per-connection statement
//! cache on real Db2: repeated statements reuse their prepared handles across
//! cursor open/close cycles and still return correct results.
#![cfg(feature = "db2")]

use std::sync::Arc;

use halcyon::driver::{CliDriver, ConnectionParams, Db2CliDriver, ParamStreamSource};
use halcyon::{named, Connection, Database, Error, ErrorCode, Value};

fn test_dsn() -> Option<String> {
    std::env::var("HALCYON_TEST_DSN").ok()
}

/// Returns the driver alongside the connection so the test can drive the seam's
/// transaction primitives (`set_auto_commit`/`commit`/`rollback`) directly — the
/// ergonomic facade over them is Phase 3.
fn open(dsn: &str) -> (Arc<Db2CliDriver>, Connection) {
    let driver = Arc::new(Db2CliDriver::new());
    let conn = Connection::open(
        driver.clone(),
        &ConnectionParams {
            connection_string: dsn.to_string(),
            isolation: None,
        },
    )
    .expect("open live connection");
    (driver, conn)
}

#[test]
fn live_round_trip() {
    let Some(dsn) = test_dsn() else {
        eprintln!("HALCYON_TEST_DSN not set; skipping live Db2 round-trip");
        return;
    };
    let (driver, conn) = open(&dsn);

    // Fresh table (ignore the drop error if it does not exist yet).
    let _ = conn.execute("DROP TABLE halcyon_people", ());
    conn.execute(
        "CREATE TABLE halcyon_people(id INT NOT NULL, name VARCHAR(64), age INT)",
        (),
    )
    .expect("create table");

    // Positional insert.
    assert_eq!(
        conn.execute(
            "INSERT INTO halcyon_people VALUES (?, ?, ?)",
            (1, "ada", 36),
        )
        .expect("positional insert"),
        1
    );

    // Named insert, leaving `age` NULL.
    conn.execute(
        "INSERT INTO halcyon_people(id, name) VALUES (:id, :name)",
        named! { "id" => 2, "name" => "bob" },
    )
    .expect("named insert");

    // Batch insert (array binding, validated above the seam).
    let batch = vec![
        vec![Value::Int(3), Value::Text("cy".into()), Value::Int(20)],
        vec![Value::Int(4), Value::Text("di".into()), Value::Int(21)],
    ];
    assert_eq!(
        conn.execute_batch("INSERT INTO halcyon_people VALUES (?, ?, ?)", &batch)
            .expect("batch insert"),
        2
    );

    // Typed streaming read, including a NULL age mapped to None.
    let mut rs = conn
        .query("SELECT id, name, age FROM halcyon_people ORDER BY id", ())
        .expect("query");
    let people: Vec<(i32, String, Option<i32>)> = rs.collect_rows().expect("collect rows");
    assert_eq!(people.len(), 4);
    assert_eq!(people[0], (1, "ada".to_string(), Some(36)));
    assert_eq!(people[1], (2, "bob".to_string(), None));
    assert!(rs.error().is_none(), "clean end of cursor");

    // query_map iterator form with a positional WHERE parameter.
    let names: Vec<String> = conn
        .query(
            "SELECT name FROM halcyon_people WHERE id >= ? ORDER BY id",
            (3,),
        )
        .expect("query where")
        .query_map(|row| row.column::<String>(0))
        .collect::<halcyon::Result<Vec<_>>>()
        .expect("map names");
    assert_eq!(names, vec!["cy".to_string(), "di".to_string()]);

    // --- Transaction commit/rollback, via the seam primitives on this
    // connection's handle (the RAII/closure facade is Phase 3). ---
    let h = conn.handle();
    driver.set_auto_commit(h, false).expect("autocommit off");

    // Rollback discards an insert.
    conn.execute(
        "INSERT INTO halcyon_people VALUES (?, ?, ?)",
        (5, "eve", 40),
    )
    .expect("insert eve");
    driver.rollback(h).expect("rollback");
    let rolled_back: Vec<(i32,)> = conn
        .query_as("SELECT id FROM halcyon_people WHERE id = 5", ())
        .expect("post-rollback query");
    assert!(
        rolled_back.is_empty(),
        "rolled-back insert must not persist"
    );

    // Commit persists an insert.
    conn.execute(
        "INSERT INTO halcyon_people VALUES (?, ?, ?)",
        (6, "fin", 50),
    )
    .expect("insert fin");
    driver.commit(h).expect("commit");
    driver.set_auto_commit(h, true).expect("autocommit on");
    let committed: Vec<(i32, String)> = conn
        .query_as("SELECT id, name FROM halcyon_people WHERE id = 6", ())
        .expect("post-commit query");
    assert_eq!(committed, vec![(6, "fin".to_string())]);

    conn.execute("DROP TABLE halcyon_people", ())
        .expect("drop table");
}

/// Facade round-trip: the ergonomic pooled [`Database`] and the closure/RAII
/// [`halcyon::ScopedTransaction`] against a real Db2 — the facade analog of the
/// seam-primitive transaction coverage in [`live_round_trip`]. Gated on
/// `HALCYON_TEST_DSN` the same way (a no-op pass when unset).
#[test]
fn live_facade_round_trip() {
    let Some(dsn) = test_dsn() else {
        eprintln!("HALCYON_TEST_DSN not set; skipping live Db2 facade round-trip");
        return;
    };
    let db = Database::connect(&dsn).expect("open database");

    let _ = db.execute("DROP TABLE halcyon_facade", ());
    db.execute(
        "CREATE TABLE halcyon_facade(id INT NOT NULL, name VARCHAR(64))",
        (),
    )
    .expect("create table");

    // Closure transaction: commits on Ok.
    db.transaction(|tx| {
        tx.execute("INSERT INTO halcyon_facade VALUES (?, ?)", (1, "ada"))?;
        tx.execute("INSERT INTO halcyon_facade VALUES (?, ?)", (2, "bob"))?;
        Ok(())
    })
    .expect("committed transaction");
    let committed: Vec<(i32, String)> = db
        .query_as("SELECT id, name FROM halcyon_facade ORDER BY id", ())
        .expect("post-commit query");
    assert_eq!(
        committed,
        vec![(1, "ada".to_string()), (2, "bob".to_string())]
    );

    // Closure transaction: rolls back on Err — the insert must not persist.
    let rolled: halcyon::Result<()> = db.transaction(|tx| {
        tx.execute("INSERT INTO halcyon_facade VALUES (?, ?)", (3, "cy"))?;
        Err(Error::new(ErrorCode::Constraint, "abort"))
    });
    assert!(rolled.is_err());
    let after_rollback: Vec<(i32,)> = db
        .query_as("SELECT id FROM halcyon_facade WHERE id = 3", ())
        .expect("post-rollback query");
    assert!(
        after_rollback.is_empty(),
        "rolled-back insert must not persist"
    );

    // RAII transaction: explicit commit persists the insert.
    let mut tx = db.begin().expect("begin");
    tx.execute("INSERT INTO halcyon_facade VALUES (?, ?)", (4, "di"))
        .expect("insert di");
    tx.commit().expect("commit");
    let after_commit: Vec<(i32, String)> = db
        .query_as("SELECT id, name FROM halcyon_facade WHERE id = 4", ())
        .expect("post-commit query");
    assert_eq!(after_commit, vec![(4, "di".to_string())]);

    db.execute("DROP TABLE halcyon_facade", ())
        .expect("drop table");
}

/// Phase 5 exit criterion: repeated statements through the pooled facade (the
/// default config caches 64 per connection) must reuse prepared handles and
/// still return correct results on real Db2 — a parameterized SELECT
/// re-executed with different bindings (cursor open/close/reopen on the cached
/// handle) and a DML re-executed after those cursors.
#[test]
fn live_statement_cache_reuse() {
    let Some(dsn) = test_dsn() else {
        eprintln!("HALCYON_TEST_DSN not set; skipping live statement-cache round-trip");
        return;
    };
    let db = Database::connect(&dsn).expect("open database");

    let _ = db.execute("DROP TABLE halcyon_stmt_cache", ());
    db.execute(
        "CREATE TABLE halcyon_stmt_cache(id INT NOT NULL, name VARCHAR(64))",
        (),
    )
    .expect("create table");

    // Re-execute the same INSERT through the cache with different bindings.
    for (id, name) in [(1, "ada"), (2, "bob"), (3, "cy")] {
        db.execute("INSERT INTO halcyon_stmt_cache VALUES (?, ?)", (id, name))
            .expect("cached insert");
    }

    // Re-execute the same parameterized SELECT; each run opens and closes a
    // cursor on the cached handle (the close_cursor reset-before-reuse
    // discipline, now proven on live Db2).
    for lo in 1..=3i32 {
        let got: Vec<(i32, String)> = db
            .query_as(
                "SELECT id, name FROM halcyon_stmt_cache WHERE id >= ? ORDER BY id",
                (lo,),
            )
            .expect("cached select");
        assert_eq!(got.len() as i32, 4 - lo, "wrong row count for lo={lo}");
        assert_eq!(got[0].0, lo, "wrong first row for lo={lo}");
    }

    // DML re-execution after cursors: the cached INSERT handle still works.
    db.execute("INSERT INTO halcyon_stmt_cache VALUES (?, ?)", (4, "di"))
        .expect("insert after cursor reuse");
    let count: Vec<(i32,)> = db
        .query_as("SELECT COUNT(*) FROM halcyon_stmt_cache", ())
        .expect("count");
    assert_eq!(count[0].0, 4);

    db.execute("DROP TABLE halcyon_stmt_cache", ())
        .expect("drop table");
}

/// Exercises the seam directly so the fast-path block shape and the intentional
/// one-row CLOB fallback are observable (the public collectors deliberately
/// hide driver block boundaries).
#[test]
fn live_rowset_fetch_and_long_column_fallback() {
    let Some(dsn) = test_dsn() else {
        eprintln!("HALCYON_TEST_DSN not set; skipping live Db2 rowset round-trip");
        return;
    };
    let (driver, conn) = open(&dsn);
    let _ = conn.execute("DROP TABLE halcyon_rowset_rs", ());
    conn.execute(
        "CREATE TABLE halcyon_rowset_rs(id BIGINT NOT NULL, name VARCHAR(32), body CLOB)",
        (),
    )
    .expect("create rowset table");
    for id in 1..=5i64 {
        conn.execute(
            "INSERT INTO halcyon_rowset_rs VALUES (?, ?, ?)",
            (
                id,
                format!("name-{id}"),
                format!("lob-{id}-{}", "z".repeat(5000)),
            ),
        )
        .expect("insert rowset row");
    }

    let bounded = driver
        .prepare(
            conn.handle(),
            "SELECT id, name FROM halcyon_rowset_rs ORDER BY id",
        )
        .expect("prepare bounded projection");
    driver.bind_params(bounded, &[]).expect("bind bounded");
    driver.execute(bounded).expect("execute bounded");
    let block = driver.fetch_block(bounded, 10).expect("fetch rowset block");
    assert_eq!(block.len(), 5, "bounded projection should use one rowset");
    assert_eq!(block[0], vec![Value::Int(1), Value::Text("name-1".into())]);
    assert!(driver.fetch_block(bounded, 10).unwrap().is_empty());
    driver.close_cursor(bounded).expect("close bounded cursor");
    driver
        .finalize(bounded)
        .expect("finalize bounded statement");

    let long = driver
        .prepare(
            conn.handle(),
            "SELECT id, body FROM halcyon_rowset_rs ORDER BY id",
        )
        .expect("prepare CLOB projection");
    driver.bind_params(long, &[]).expect("bind CLOB projection");
    driver.execute(long).expect("execute CLOB projection");
    for id in 1..=5i64 {
        let block = driver.fetch_block(long, 10).expect("fetch CLOB fallback");
        assert_eq!(block.len(), 1, "CLOB fallback must return at most one row");
        assert_eq!(block[0][0], Value::Int(id));
        assert_eq!(
            block[0][1],
            Value::Text(format!("lob-{id}-{}", "z".repeat(5000)))
        );
    }
    assert!(driver.fetch_block(long, 10).unwrap().is_empty());
    driver.close_cursor(long).expect("close CLOB cursor");
    driver.finalize(long).expect("finalize CLOB statement");

    conn.execute("DROP TABLE halcyon_rowset_rs", ())
        .expect("drop rowset table");
}

/// Proves `SQLCancel` leaves a statement out of `SQL_NEED_DATA` by reusing the
/// exact direct-seam handle after a source abort. A facade/cache re-prepare
/// would not prove this property, so there is deliberately no portable wrapper
/// between the two `execute_streaming` calls below.
#[test]
fn aborted_streaming_execute_allows_reuse_of_same_statement_handle() {
    let Some(dsn) = test_dsn() else {
        eprintln!("HALCYON_TEST_DSN not set; skipping same-handle SQLCancel live proof");
        return;
    };
    let (driver, conn) = open(&dsn);
    const TABLE: &str = "halcyon_cancel_lob";
    let _ = conn.execute(&format!("DROP TABLE {TABLE}"), ());
    conn.execute(
        &format!("CREATE TABLE {TABLE}(id INT NOT NULL, body BLOB(1M))"),
        (),
    )
    .expect("create SQLCancel proof table");

    let statement = match driver.prepare(
        conn.handle(),
        &format!("INSERT INTO {TABLE}(id, body) VALUES (?, ?)"),
    ) {
        Ok(statement) => statement,
        Err(error) => {
            let _ = conn.execute(&format!("DROP TABLE {TABLE}"), ());
            panic!("prepare SQLCancel proof statement: {error}");
        }
    };

    let proof = (|| -> halcyon::Result<()> {
        let mut pulls = 0usize;
        let abort_message = "intentional LOB source abort after first chunk";
        let aborted = driver.execute_streaming(
            statement,
            &[Value::Int(1), Value::Null],
            vec![ParamStreamSource {
                param_index: 1,
                pull: Box::new(|stage| {
                    pulls += 1;
                    if pulls == 1 {
                        let chunk = b"partial-chunk";
                        if stage.len() < chunk.len() {
                            return Err(Error::new(
                                ErrorCode::Mapping,
                                "Db2 LOB stage unexpectedly too small",
                            ));
                        }
                        stage[..chunk.len()].copy_from_slice(chunk);
                        Ok(chunk.len())
                    } else {
                        Err(Error::new(ErrorCode::Mapping, abort_message))
                    }
                }),
                size_hint: None,
                is_clob: false,
            }],
        );
        match aborted {
            Err(error) if error.code == ErrorCode::Mapping && error.message == abort_message => {}
            Err(error) => {
                return Err(Error::new(
                    ErrorCode::Mapping,
                    format!("streaming abort returned an unexpected error: {error}"),
                ));
            }
            Ok(rows) => {
                return Err(Error::new(
                    ErrorCode::InvalidState,
                    format!("streaming abort unexpectedly succeeded with {rows} rows"),
                ));
            }
        }

        // No finalize or prepare occurs here: `statement` is exactly the same
        // StatementHandle that was left by the aborted NEED_DATA exchange.
        let payload = b"complete-value";
        let mut offset = 0usize;
        let rows = driver.execute_streaming(
            statement,
            &[Value::Int(2), Value::Null],
            vec![ParamStreamSource {
                param_index: 1,
                pull: Box::new(|stage| {
                    let count = stage.len().min(payload.len().saturating_sub(offset));
                    stage[..count].copy_from_slice(&payload[offset..offset + count]);
                    offset += count;
                    Ok(count)
                }),
                size_hint: Some(payload.len() as u64),
                is_clob: false,
            }],
        )?;
        if rows != 1 {
            return Err(Error::new(
                ErrorCode::InvalidState,
                format!("same-handle execute affected {rows} rows instead of 1"),
            ));
        }

        let ids: Vec<(i32,)> = conn.query_as(&format!("SELECT id FROM {TABLE} ORDER BY id"), ())?;
        if ids != vec![(2,)] {
            return Err(Error::new(
                ErrorCode::InvalidState,
                format!("unexpected rows after abort/reuse: {ids:?}"),
            ));
        }
        Ok(())
    })();

    // Run both cleanup steps before surfacing a proof error. Cleanup diagnostics
    // are supplemental and never replace the same-handle assertion result.
    let finalize = driver.finalize(statement);
    let drop_table = conn.execute(&format!("DROP TABLE {TABLE}"), ());
    if let Err(error) = proof {
        panic!(
            "same-handle SQLCancel proof failed: {error}; statement cleanup: {finalize:?}; table cleanup: {drop_table:?}"
        );
    }
    finalize.expect("finalize SQLCancel proof statement");
    drop_table.expect("drop SQLCancel proof table");
}
