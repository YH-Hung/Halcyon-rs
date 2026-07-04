//! Live batch-insert throughput benchmark. Requires HALCYON_TEST_DSN; a no-op
//! (exit 0) when unset. Times inserting N rows three ways and prints rows/sec:
//!   1) per-row execute() under autocommit
//!   2) execute_batch (array binding) under autocommit
//!   3) execute_batch wrapped in a transaction (one commit)
//!
//! Run with:
//!      cargo run --release -p halcyon --example batch_bench
use std::time::Instant;

use halcyon::{Database, Value};

fn recreate(db: &Database) {
    let _ = db.execute("DROP TABLE halcyon_bench", ());
    db.execute(
        "CREATE TABLE halcyon_bench(id BIGINT NOT NULL, payload VARCHAR(64))",
        (),
    )
    .expect("create table");
}

fn main() {
    let dsn = match std::env::var("HALCYON_TEST_DSN") {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("HALCYON_TEST_DSN not set; skipping benchmark");
            return;
        }
    };
    let db = Database::connect(&dsn).expect("connect");
    let n: i64 = 50_000;
    let payload = "x".repeat(48);
    let sql = "INSERT INTO halcyon_bench(id,payload) VALUES (?,?)";

    let rows: Vec<Vec<Value>> = (0..n)
        .map(|i| vec![Value::Int(i), Value::Text(payload.clone())])
        .collect();

    // 1) per-row execute under autocommit
    recreate(&db);
    let t0 = Instant::now();
    for i in 0..n {
        db.execute(sql, (i, payload.clone())).expect("insert");
    }
    let per_row = t0.elapsed().as_secs_f64();

    // 2) array binding under autocommit
    recreate(&db);
    let t0 = Instant::now();
    db.execute_batch(sql, &rows).expect("batch");
    let array = t0.elapsed().as_secs_f64();

    // 3) array binding inside one transaction
    recreate(&db);
    let t0 = Instant::now();
    {
        let mut tx = db.begin().expect("begin");
        tx.execute_batch(sql, &rows).expect("tx batch");
        tx.commit().expect("commit");
    }
    let array_txn = t0.elapsed().as_secs_f64();

    let _ = db.execute("DROP TABLE halcyon_bench", ());

    let rps = |s: f64| if s > 0.0 { n as f64 / s } else { 0.0 };
    println!("rows={n}");
    println!("per_row_autocommit : {per_row:.3}s  {:.0} rows/s", rps(per_row));
    println!("array_autocommit   : {array:.3}s  {:.0} rows/s", rps(array));
    println!("array_transaction  : {array_txn:.3}s  {:.0} rows/s", rps(array_txn));
}
