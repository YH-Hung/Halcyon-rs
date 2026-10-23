//! Run with HALCYON_DSN set to a Db2 CLI connection string.
use halcyon::{Database, Error, ErrorCode, FromRow, Result};

#[derive(Debug, FromRow)]
struct Item {
    id: i64,
    name: Option<String>,
}

fn main() -> Result<()> {
    let dsn = std::env::var("HALCYON_DSN")
        .map_err(|_| Error::new(ErrorCode::InvalidArgument, "set HALCYON_DSN"))?;
    let db = Database::connect(&dsn)?;
    // Use a session temporary table so running the example needs no schema
    // setup and leaves no permanent application table behind.
    db.transaction(|tx| {
        tx.execute(
            "DECLARE GLOBAL TEMPORARY TABLE SESSION.halcyon_items \
             (id BIGINT, name VARCHAR(100)) ON COMMIT PRESERVE ROWS NOT LOGGED",
            (),
        )?;
        tx.execute(
            "INSERT INTO SESSION.halcyon_items(id, name) VALUES (?, ?)",
            (1_i64, "sample"),
        )?;
        let items: Vec<Item> = tx.query_as(
            "SELECT id, name FROM SESSION.halcyon_items WHERE id = ?",
            (1_i64,),
        )?;
        for item in items {
            println!("{}: {:?}", item.id, item.name);
        }
        Ok(())
    })
}
