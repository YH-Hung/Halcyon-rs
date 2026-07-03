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
//! Covers the Phase 1 public surface plus the seam's transaction control:
//! connect, DDL, positional/named/batch inserts, typed streaming reads including
//! NULL, and commit/rollback via the driver primitives. The RAII/closure
//! transaction facade, struct-mapping `queryAs`, and pool round-trips from the
//! C++ `test_db2_roundtrip.cpp` arrive with Phases 3–4.
#![cfg(feature = "db2")]

use std::sync::Arc;

use halcyon::driver::{CliDriver, ConnectionParams, Db2CliDriver};
use halcyon::{named, Connection, Value};

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
