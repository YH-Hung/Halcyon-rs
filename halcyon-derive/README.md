# halcyon-derive

`#[derive(FromRow)]` proc macro for the
[`halcyon`](https://crates.io/crates/halcyon) IBM Db2 client. Generates a
by-position `halcyon::FromRow` implementation — column 0 into the first field,
column 1 into the second, and so on.

**This crate is an implementation detail of `halcyon`** and carries no
stability guarantee of its own — depend on `halcyon` with the `derive` feature
(on by default), which re-exports the macro.

```rust
use halcyon::FromRow;

#[derive(FromRow)]
struct User {
    id: i32,
    name: String,
    email: Option<String>, // NULL-able column
}

let users: Vec<User> = db.query_as("SELECT id, name, email FROM users", ())?;
```

## License

BSD 3-Clause — see [LICENSE](LICENSE).
