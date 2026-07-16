# Halcyon-rs v0.1 Parity Catch-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring Halcyon-rs to C++ v1.1 feature parity (plus deferred block fetch) so the first crates.io publish is `0.1.0` with the enlarged surface.

**Architecture:** Dependency-ordered phases mirroring the approved design: shared error/scaffold → isolation → savepoints → block fetch → LOB streaming → async wrappers → live hardening + release docs. Seam growth lands only in `halcyon-sys` + `driver/db2.rs`; everything above stays mock-tested with `--no-default-features`.

**Tech Stack:** Rust 2021 (MSRV 1.75), existing `db2` / `tokio` / `observability` / `stress` features, hand-written CLI FFI in `halcyon-sys`, TDD against `MockCliDriver`.

**Design reference:** [docs/superpowers/specs/2026-07-16-halcyon-rs-v0.1-parity-catchup-design.md](../specs/2026-07-16-halcyon-rs-v0.1-parity-catchup-design.md) (rev 5). C++: `../Halcyon` v1.1.0 + rowset-fetch specs.

## Global Constraints

- Seam rule: only `halcyon-sys` and `halcyon/src/driver/db2.rs` may use `unsafe` / CLI ABI.
- `cargo test -p halcyon --no-default-features` must stay green after every task.
- Default `db2` feature must **compile** after every task that grows `CliDriver` (use `cargo test -p halcyon --features db2 --no-run`). No `todo!` / `unimplemented!` in `Db2CliDriver` public seam methods.
- Public errors are `Result<T, Error>` only; new codes `InvalidArgument` / `InvalidState`.
- No method overloading: `begin_with_isolation`, `transaction_with_isolation`, `savepoint_named`, `execute_with_lobs`.
- LOB/streaming paths never auto-retry; `Params` / `IntoParams` stay Value-only + `Clone`.
- Async: no lease-holding `begin` across `.await`; closures only for txn/streaming.
- Streaming cursors (locked): `StreamingResultSet<'_>` / `StreamingQueryResult` / `TxStreamingCursor<'_>` / async `FnOnce(&mut StreamingQueryResult)`.
- Connect-time isolation: applied only in `Connection::open_with_cache`; failure → disconnect handle then return.
- Block-fetch long-column fallback returns **≤1 row** per `fetch_block` (intentional C++ divergence).
- Fetch budgets: `FETCH_BLOCK_BYTES = 2 MiB`, `MAX_BLOCK_ROWS = 4096` (not the 16 MiB write batch budget).
- Isolation restore: `configured.unwrap_or(CursorStability)`.
- Sync LOB entry consumes `Vec<BindParam<'_>>` (not `&[BindParam]`).
- Pure stress gate: always `cargo test -p halcyon --no-default-features --features stress` (never bare `--features stress`, which pulls default `db2`).

---

## File Structure

| Path | Responsibility |
|---|---|
| `halcyon/src/error.rs` | Add `InvalidArgument`, `InvalidState` |
| `halcyon/src/isolation.rs` (**new**) | Public `Isolation` enum |
| `halcyon/src/lob.rs` (**new**) | `BindParam` / `LobSource` / `Owned*` / constructors / `LobReader` / `StreamingRow` |
| `halcyon/src/core/streaming.rs` (**new**) | `StreamingResultSet<'_>` (borrowed core cursor) |
| `halcyon/src/core/savepoint.rs` (**new**) | Name gate, registry helpers, `Savepoint` RAII shared by both txn types |
| `halcyon/src/core/transaction.rs` | Interior mutability; isolation restore; savepoint/`nested`; `query_streaming` |
| `halcyon/src/core/connection.rs` | `open_with_cache` applies isolation + disconnect-on-fail; `unusable`; `begin_with_isolation`; `execute_with_lobs` / `query_streaming` |
| `halcyon/src/core/result_set.rs` | Buffer via `fetch_block`; `Row` reads from buffered `Value`s |
| `halcyon/src/core/mod.rs` / `lib.rs` | Module wiring + re-exports |
| `halcyon/src/driver/mod.rs` | `ConnectionParams::isolation`; seam helpers; replace `fetch`/`get_column`; add streaming + `set_isolation` |
| `halcyon/src/driver/db2.rs` | CLI implementations (rowset, isolation, GetData/PutData/Cancel) — **never** `todo!` |
| `halcyon-sys/src/lib.rs` | New FFI entry points / constants only as needed |
| `halcyon/src/pool.rs` | `PoolConfig::isolation`; copy into params on open/reconnect (does **not** call `set_isolation` itself) |
| `halcyon/src/facade.rs` | Isolation + savepoints; `StreamingQueryResult`; `TxStreamingCursor`; LOB + streaming |
| `halcyon/src/async_db.rs` | Closure APIs; `query_streaming(..., FnOnce(&mut StreamingQueryResult))` |
| `halcyon/src/testing.rs` | Scriptable `fetch_block` / chunks / isolation / NEED_DATA |
| `halcyon/src/stress/fake_driver.rs` | Same seam growth for TSan |
| `halcyon/src/obs/labels.rs` | New `code_label` arms |
| `halcyon/src/obs/events.rs` (**new**) | Structured log-catalog `tracing` events |
| `halcyon/tests/db2_roundtrip.rs` | Live savepoints / isolation / LOB / fallback |
| `README.md` / `CHANGELOG.md` | Deferred list + 0.1.0 surface |

---

### Task 1: `ErrorCode::{InvalidArgument, InvalidState}` + obs labels

**Files:**
- Modify: `halcyon/src/error.rs`
- Modify: `halcyon/src/obs/labels.rs`
- Test: inline tests in both files

**Interfaces:**
- Consumes: existing `ErrorCode` / `code_label`
- Produces: two new variants; `as_str` → `"InvalidArgument"` / `"InvalidState"`; `code_label` → `"invalid_argument"` / `"invalid_state"`

- [ ] **Step 1: Write the failing tests**

In `halcyon/src/error.rs` tests:

```rust
#[test]
fn invalid_argument_and_invalid_state_display() {
    assert_eq!(ErrorCode::InvalidArgument.as_str(), "InvalidArgument");
    assert_eq!(ErrorCode::InvalidState.as_str(), "InvalidState");
    assert_eq!(
        Error::new(ErrorCode::InvalidArgument, "bad name").to_string(),
        "InvalidArgument: bad name"
    );
}
```

In `halcyon/src/obs/labels.rs` tests (feature `observability`):

```rust
#[test]
fn code_label_covers_programming_errors() {
    assert_eq!(code_label(ErrorCode::InvalidArgument), "invalid_argument");
    assert_eq!(code_label(ErrorCode::InvalidState), "invalid_state");
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p halcyon --no-default-features invalid_argument_and_invalid_state_display -- --nocapture
cargo test -p halcyon --no-default-features --features observability code_label_covers_programming_errors -- --nocapture
```

Expected: compile error — variants do not exist.

- [ ] **Step 3: Minimal implementation**

Add variants to `ErrorCode`, `as_str` arms, and `code_label` arms. Do not use them elsewhere yet.

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p halcyon --no-default-features
cargo test -p halcyon --no-default-features --features observability
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add halcyon/src/error.rs halcyon/src/obs/labels.rs
git commit -m "$(cat <<'EOF'
feat(error): add InvalidArgument and InvalidState codes.

EOF
)"
```

---

### Task 2: `Isolation` enum + `CliDriver::set_isolation` + real Db2 mapping

**Files:**
- Create: `halcyon/src/isolation.rs`
- Modify: `halcyon/src/lib.rs` (mod + `pub use Isolation`)
- Modify: `halcyon/src/driver/mod.rs` (`ConnectionParams::isolation`, trait method)
- Modify: `halcyon/src/testing.rs`, `halcyon/src/stress/fake_driver.rs`
- Modify: `halcyon-sys/src/lib.rs` — `SQL_ATTR_TXN_ISOLATION` + `SQL_TXN_*` constants as needed
- Modify: `halcyon/src/driver/db2.rs` — **real** `SQLSetConnectAttr` implementation (required this task; no `todo!`)
- Modify: every in-tree `CliDriver` impl (pool loom stubs in `pool.rs` tests)

**Interfaces:**
- Consumes: Task 1 codes (unused here)
- Produces:
  ```rust
  pub enum Isolation {
      UncommittedRead,
      CursorStability,
      ReadStability,
      RepeatableRead,
  }
  // ConnectionParams { connection_string, isolation: Option<Isolation> }
  // CliDriver::set_isolation(&self, conn: ConnectionHandle, level: Isolation) -> Result<()>
  ```

- [ ] **Step 1: Write failing mock tests**

In `halcyon/src/testing.rs` (or a new `isolation` test module):

```rust
#[test]
fn mock_records_set_isolation_calls() {
    let mock = Arc::new(MockCliDriver::new());
    let conn = Connection::open(mock.clone(), &params()).unwrap();
    mock.set_isolation(conn.handle(), Isolation::UncommittedRead).unwrap();
    assert_eq!(mock.isolation_calls(), vec![Isolation::UncommittedRead]);
}
```

Also add a compile-breaking expectation: any existing `CliDriver` impl missing `set_isolation` fails to compile — that is the red signal for Step 2.

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p halcyon --no-default-features mock_records_set_isolation_calls
```

Expected: missing method / type.

- [ ] **Step 3: Implement enum, params field, trait method, mocks, and real Db2**

CLI mapping (`driver/db2.rs` only):

| Variant | CLI |
|---|---|
| `UncommittedRead` | `SQL_TXN_READ_UNCOMMITTED` |
| `CursorStability` | `SQL_TXN_READ_COMMITTED` |
| `ReadStability` | `SQL_TXN_REPEATABLE_READ` |
| `RepeatableRead` | `SQL_TXN_SERIALIZABLE` |

`Db2CliDriver::set_isolation` must call `SQLSetConnectAttr(conn, SQL_ATTR_TXN_ISOLATION, …)` and map failures to `Result` (never panic). Default `ConnectionParams::isolation = None`. Update all test `ConnectionParams { .. }` literals to include `isolation: None` (or add a test helper).

- [ ] **Step 4: Pass (pure + stress without db2 + default feature compiles)**

```bash
cargo test -p halcyon --no-default-features
cargo test -p halcyon --no-default-features --features stress --no-run
cargo test -p halcyon --features db2 --no-run
```

- [ ] **Step 5: Commit**

```bash
git commit -m "$(cat <<'EOF'
feat(driver): add Isolation enum and set_isolation seam.

EOF
)"
```

---

### Task 3: Connect-time isolation apply + `begin_with_isolation` restore / unusable

**Files:**
- Modify: `halcyon/src/pool.rs` (`PoolConfig::isolation`; copy into `ConnectionParams` on open/reconnect — pool does **not** call `set_isolation`)
- Modify: `halcyon/src/core/connection.rs` (`open_with_cache` applies isolation; store `configured_isolation`; `unusable: Cell<bool>`; guard public ops)
- Modify: `halcyon/src/core/transaction.rs` (optional `restore_isolation: Option<Isolation>`; restore in `finish` / Drop)
- Modify: `halcyon/src/facade.rs` (`Database::begin_with_isolation`, `transaction_with_isolation`; `ScopedTransaction` restore)
- Test: unit tests in `transaction.rs`, `connection.rs`, `facade.rs`, `pool.rs`

**Interfaces:**
- Consumes: `set_isolation`, `PoolConfig::isolation`, `ConnectionParams::isolation`
- Produces:
  ```rust
  // restore_target = configured.unwrap_or(Isolation::CursorStability)
  Connection::begin_with_isolation(&self, iso: Isolation) -> Result<Transaction<'_>>
  Database::begin_with_isolation(&self, iso: Isolation) -> Result<ScopedTransaction>
  Database::transaction_with_isolation<T,F>(&self, iso, f) -> Result<T>
  ```

**Connect-time apply (uniform owner — design §3.3):** only in `Connection::open_with_cache`:

```rust
let handle = driver.connect(params)?;
if let Some(level) = params.isolation {
    if let Err(e) = driver.set_isolation(handle, level) {
        let _ = driver.disconnect(handle);
        return Err(e);
    }
}
// store configured_isolation = params.isolation on Connection
```

Begin sequence per design §3.3. Failed compensation: pooled → `lease.mark_broken()`; standalone → `unusable` + later ops `InvalidState`.

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn open_applies_isolation_from_params() {
    // params.isolation = Some(RS); Connection::open; mock saw set_isolation(RS) after connect
}

#[test]
fn open_set_isolation_failure_disconnects_handle() {
    // mock: connect ok, set_isolation fails; open returns Err;
    // assert disconnect_calls()==1 and no usable Connection exists
}

#[test]
fn pool_warmup_isolation_failure_does_not_leak_capacity() {
    // PoolConfig { isolation: Some(UR), min: 1, .. };
    // mock set_isolation fails on first open; pool create/acquire fails;
    // disconnect called; idle/reserved capacity not stuck
}

#[test]
fn pool_reconnect_isolation_failure_disconnects() {
    // Force reconnect path with isolation Some(..); set_isolation fails;
    // disconnect called; broken slot not returned to idle
}

#[test]
fn begin_with_isolation_sets_then_restores_on_commit() {
    // mock records: set(UR), set_auto_commit(false), commit, set_auto_commit(true), set(CS)
    // configured isolation None → restore CS
}

#[test]
fn begin_autocommit_fail_restores_and_marks_standalone_unusable() {
    // set_isolation ok; set_auto_commit(false) fails; best-effort restore;
    // subsequent conn.execute → InvalidState
}

#[test]
fn pooled_begin_compensation_failure_marks_lease_broken() {
    // Database::begin_with_isolation; force restore fail after autocommit fail;
    // drop / failed begin → pool discarded
}

#[test]
fn pool_config_isolation_copied_into_params_at_connect() {
    // PoolConfig { isolation: Some(RS), .. }; open pool; mock saw set_isolation(RS)
    // (applied via open_with_cache, not a second pool-side call)
}
```

- [ ] **Step 2: Run — expect FAIL**

```bash
cargo test -p halcyon --no-default-features open_set_isolation_failure_disconnects_handle
```

- [ ] **Step 3: Implement**

Shared begin helper (private) used by `Connection` and facade:

1. `set_isolation(level)` — on err, return (usable).
2. `set_auto_commit(false)` — on err, best-effort `set_isolation(restore_target)`; then mark unusable / broken as appropriate; return err.
3. On success, store `restore_isolation = Some(restore_target)` on the transaction.

Extend `terminate` (or wrap it) so when `restore_isolation` is `Some`, call `set_isolation` after autocommit restore; failure → poison + ownership rules.

Gate `Connection` public methods: if `unusable`, return `Error::new(InvalidState, "connection unusable after failed isolation/autocommit transition")`.

- [ ] **Step 4: Pass**

```bash
cargo test -p halcyon --no-default-features
cargo test -p halcyon --features db2 --no-run
```

- [ ] **Step 5: Commit**

```bash
git commit -m "$(cat <<'EOF'
feat(tx): isolation overrides with CS-normalized restore.

EOF
)"
```

---

### Task 4: Savepoints on `Transaction` and `ScopedTransaction`

**Files:**
- Create: `halcyon/src/core/savepoint.rs`
- Modify: `halcyon/src/core/transaction.rs`, `halcyon/src/facade.rs`, `halcyon/src/core/mod.rs`, `halcyon/src/lib.rs`
- Test: `savepoint.rs` unit tests + facade tests

**Interfaces:**
- Consumes: existing `execute` for SQL; `InvalidArgument`
- Produces:
  ```rust
  pub struct Savepoint<'a> { /* &Transaction or enum over &ScopedTransaction */ }
  // Transaction / ScopedTransaction:
  fn savepoint(&self) -> Result<Savepoint<'_>>
  fn savepoint_named(&self, name: &str) -> Result<Savepoint<'_>>
  fn nested<T, F>(&self, f: F) -> Result<T>
      where F: FnOnce(&Self) -> Result<T>
  // Savepoint: rollback(), release(); Drop = ROLLBACK TO + RELEASE
  ```

SQL (exact):

```text
SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS
ROLLBACK TO SAVEPOINT <name>
RELEASE SAVEPOINT <name>
```

Name gate: `[A-Za-z_][A-Za-z0-9_]{0,127}`, ASCII-only, case-insensitively not starting with `SYS`. Auto: `halcyon_sp_<n>`. Live registry case-insensitive.

Interior mutability on both txn types for: live names, auto counter, poison flags (and existing active/poisoned where needed so `&self` savepoint works).

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn savepoint_named_rejects_sys_prefix_and_injection() {
    // "SYSFOO", "sysx", "a;drop", "" → InvalidArgument; no execute calls
}

#[test]
fn savepoint_raii_rollback_on_drop() {
    // create SP; drop without release → ROLLBACK TO then RELEASE SQL seen
}

#[test]
fn nested_releases_on_ok_rolls_back_on_err() { /* ... */ }

#[test]
fn live_name_reuse_is_invalid_argument() { /* ... */ }

#[test]
fn scoped_transaction_savepoint_matches_connection_local() {
    // Database::begin(); savepoint_named; release
}
```

Optional trybuild later — type system already prevents `commit` inside `nested` via `&Self`.

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement name validation + Savepoint + methods on both types**

Prefer a private trait or free functions in `savepoint.rs` that take `&dyn TxExec` / callbacks for `execute(sql)` + registry cells, to avoid duplicating logic between `Transaction` and `ScopedTransaction`.

Terminal savepoint failure poisons savepoint **and** owning transaction.

- [ ] **Step 4: Pass**

```bash
cargo test -p halcyon --no-default-features
```

- [ ] **Step 5: Commit**

```bash
git commit -m "$(cat <<'EOF'
feat(tx): RAII savepoints and nested scopes.

EOF
)"
```

---

### Task 5: Seam `fetch_block` + portable cursor buffering

**Files:**
- Modify: `halcyon/src/driver/mod.rs` — remove `fetch`/`get_column`; add `fetch_block(stmt, max_rows) -> Result<Vec<Vec<Value>>>`
- Modify: `halcyon/src/testing.rs`, `stress/fake_driver.rs`, `pool.rs` loom stubs, `driver/db2.rs` (temporary: implement `fetch_block` as a loop of old fetch/get_column **or** real rowset — prefer real rowset if FFI ready; otherwise loop keeps semantics while Task 6 lands rowset)
- Modify: `halcyon/src/core/result_set.rs` and facade cursors (`QueryResult` / `TxCursor`) to consume blocks

**Interfaces:**
- Consumes: existing `Value` column reads
- Produces: empty block = EOF; short block ≠ EOF; portable layer requests up to 4096

**Cursor protocol change:**

```rust
// ResultSet holds:
//   pending: Vec<Vec<Value>>  (or VecDeque)
//   row_idx within block
// next(): if local rows remain, yield; else fetch_block(4096); empty => done
// Row::column: read from buffered Value (no get_column)
```

- [ ] **Step 1: Failing mock tests**

```rust
#[test]
fn fetch_block_empty_means_eof() { /* script empty vec → next() None */ }

#[test]
fn fetch_block_short_block_then_empty() {
    // first block 2 rows, second empty → exactly 2 rows
}

#[test]
fn midstream_fetch_block_error_poisons_cursor() { /* ... */ }
```

Update any tests that used `fail_fetch_on_call` / `fail_get_column_on_call` to block-level knobs.

- [ ] **Step 2: Run — FAIL / compile break on trait**

- [ ] **Step 3: Implement trait + mocks + portable buffering**

`Db2CliDriver::fetch_block` must be a real `Result`-returning implementation this
task (row-at-a-time internal loop is OK if rowset lands in Task 6 — document
which in the commit message). No `todo!`.

- [ ] **Step 4: Pass**

```bash
cargo test -p halcyon --no-default-features
cargo test -p halcyon --no-default-features --features stress --no-run
cargo test -p halcyon --features db2 --no-run
```

- [ ] **Step 5: Commit**

```bash
git commit -m "$(cat <<'EOF'
feat(driver): replace fetch/get_column with fetch_block.

EOF
)"
```

---

### Task 6: Db2 rowset bind + ≤1-row long-column fallback

**Files:**
- Modify: `halcyon-sys/src/lib.rs` (rowset attrs, `SQLBindCol` as needed)
- Modify: `halcyon/src/driver/db2.rs`
- Modify: `halcyon/src/testing.rs` — scriptable “unbounded column” mode returning ≤1 row even when `max_rows > 1`
- Test: mock fallback + live test stub in `halcyon/tests/db2_roundtrip.rs` (can `#[ignore]` until DSN)

**Constants (db2.rs):**

```rust
const FETCH_BLOCK_BYTES: usize = 2 * 1024 * 1024;
const MAX_BLOCK_ROWS: usize = 4096;
const MAX_BOUND_COL_BYTES: usize = 64 * 1024;
```

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn mock_unbounded_fallback_returns_at_most_one_row() {
    // script 5 LOB-bearing rows; fetch_block(10) → len==1; repeat until EOF
}
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement describe-once, rowset bind path, fallback path**

After fetch: unbind / reset `ROW_ARRAY_SIZE=1` so statement cache reuse is safe (match C++).

- [ ] **Step 4: Pass pure tests; compile db2 feature**

```bash
cargo test -p halcyon --no-default-features
cargo test -p halcyon --features db2 --no-run
```

- [ ] **Step 5: Commit**

```bash
git commit -m "$(cat <<'EOF'
feat(db2): rowset fetch_block with one-row long-column fallback.

EOF
)"
```

---

### Task 7: LOB types + streaming cursors + `execute_with_lobs` (mock **and** Db2)

**Files:**
- Create: `halcyon/src/lob.rs` — bind/source types, `LobReader`, `StreamingRow`
- Create: `halcyon/src/core/streaming.rs` — `StreamingResultSet<'_>`
- Modify: `driver/mod.rs` — `GetDataChunk`, `ParamStreamSource`, `fetch_next`, `get_value`, `get_data_chunk`, `execute_streaming`
- Modify: `halcyon-sys/src/lib.rs` — `SQLGetData`, `SQLPutData`, `SQLParamData`, `SQLCancel`, `SQL_NEED_DATA`, CLOB/BLOB types as needed
- Modify: `driver/db2.rs` — **complete** implementations this task (default feature must compile; no `todo!`)
- Modify: `testing.rs` / `fake_driver.rs`
- Modify: `connection.rs`, `transaction.rs`, `facade.rs` (`StreamingQueryResult`, `TxStreamingCursor`)
- Modify: `core/mod.rs`, `lib.rs` re-exports

**Locked public signatures (design §4.4 rev 4):**

```rust
pub enum BindParam<'a> { Value(Value), Lob(LobSource<'a>) }
pub enum LobSource<'a> { File { .. }, Reader { .. }, Callback { .. } }
pub enum OwnedBindParam { Value(Value), Lob(OwnedLobSource) }

// Core / connection-local txn:
Connection::execute_with_lobs(&self, sql: &str, params: Vec<BindParam<'_>>) -> Result<i64>
Connection::query_streaming(&self, sql: &str, params: impl IntoParams) -> Result<StreamingResultSet<'_>>
Transaction::execute_with_lobs(...) -> Result<i64>
Transaction::query_streaming(...) -> Result<StreamingResultSet<'_>>

// Facade:
Database::execute_with_lobs(&self, sql: &str, params: Vec<BindParam<'_>>) -> Result<i64>
Database::query_streaming(&self, sql: &str, params: impl IntoParams) -> Result<StreamingQueryResult>
ScopedTransaction::execute_with_lobs(...) -> Result<i64>
ScopedTransaction::query_streaming(...) -> Result<TxStreamingCursor<'_>>
```

**Drop / poison (mirror existing cursors):**
- `StreamingResultSet`: record driver errors; Drop closes cursor / returns stmt to cache.
- `StreamingQueryResult`: owns lease; Drop poisons stmt on any driver error; `mark_broken` on `ErrorCode::Connection`.
- `TxStreamingCursor`: wraps `StreamingResultSet` + `&Cell<bool>`; Drop sets `conn_broken` on connection-class error so `ScopedTransaction` discards the lease.

Policy: always once (no auto-retry). Any `Lob` → `execute_streaming`. Pull: `FnMut(&mut [u8]) -> Result<usize>` (`Ok(0)` EOF).

**NEED_DATA safety (must be in `Db2CliDriver` this task — design §4.4):**

Extract helpers in `driver/db2.rs` that **`execute_streaming` actually calls**
(not a parallel untested path), e.g.:

```rust
/// Validates size_hint fits SQLLEN/SQLULEN (32-bit on non-Windows).
fn validate_lob_size_hint(hint: Option<u64>) -> Result<Option<i32>>;

/// One NEED_DATA token: pull → (optional) put / empty put; on any failure invoke cancel.
/// `put`/`cancel` are injected so unit tests record calls without MockCliDriver.
fn pump_need_data_source(
    stage: &mut [u8],
    pull: &mut dyn FnMut(&mut [u8]) -> Result<usize>,
    mut put: impl FnMut(&[u8]) -> Result<()>,      // including empty slice for zero-length
    mut cancel: impl FnMut(),
) -> Result<()>;
```

`Db2CliDriver::execute_streaming` must call these helpers for size-hint checks
and every `SQL_NEED_DATA` token (wrapping `SQLPutData` / `SQLCancel`).

Protocol:
1. Source `Err` → cancel + propagate.
2. `n > stage.len()` → cancel + `Mapping` (oversized chunk).
3. Pull panic → `catch_unwind`, cancel, `Mapping`.
4. `size_hint` overflow → `InvalidArgument` before execute.
5. Empty non-NULL LOB → one zero-length put.
6. PutData / unknown-token failure → cancel.

Staging buffer: `LOB_STAGE_BYTES = 256 KiB`.

- [ ] **Step 1: Failing tests**

Portable / mock (routing + lease poison — **not** a substitute for pump tests):

```rust
#[test]
fn execute_with_lobs_routes_to_execute_streaming_once() { /* ... */ }

#[test]
fn execute_with_lobs_failure_poisons_statement_lease() {
    // mirror execute_error_poisons_cached_statement_so_next_use_reprepares:
    // source Err → finalize_calls increases; next same-sql acquire reprepares
}

#[test]
fn lob_reader_chunk_boundaries() { /* empty, NULL, exact multiple, abandon */ }

#[test]
fn ascending_column_violation_is_invalid_state() { /* ... */ }

#[test]
fn streaming_query_result_marks_lease_broken_on_connection_error() { /* facade */ }

#[test]
fn tx_streaming_cursor_sets_conn_broken_on_connection_error() { /* ScopedTransaction */ }
```

**Shared pump / hint tests** (in `driver/db2.rs` `#[cfg(test)]` — drive the
helpers with recording `put`/`cancel` closures; these must fail if the helpers
are unused by `execute_streaming`):

```rust
#[test]
fn pump_source_err_invokes_cancel_and_propagates() { /* ... */ }

#[test]
fn pump_rejects_oversized_pull_before_put() { /* n > stage → cancel; put never called */ }

#[test]
fn pump_cancels_on_pull_panic() { /* catch_unwind path */ }

#[test]
fn validate_lob_size_hint_rejects_overflow() { /* > i32::MAX → InvalidArgument */ }

#[test]
fn pump_empty_non_null_issues_one_zero_length_put() { /* put(&[0;0]) once */ }
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement types + portable APIs + mock seam + complete Db2 using the helpers**

Do **not** extend `IntoParams`. Named params + LOB unsupported. Cursor rule: streaming stmt never calls `fetch_block`. Wire `execute_streaming` through `validate_lob_size_hint` + `pump_need_data_source` so the unit tests cover the real path.

- [ ] **Step 4: Pass (pure + default db2 compiles + db2 lib tests including pump)**

```bash
cargo test -p halcyon --no-default-features
cargo test -p halcyon --no-default-features --features stress --no-run
cargo test -p halcyon --features db2 --no-run
cargo test -p halcyon --features db2 --lib pump_
cargo test -p halcyon --features db2 --lib validate_lob
```

- [ ] **Step 5: Commit**

```bash
git commit -m "$(cat <<'EOF'
feat(lob): streaming bind params, cursors, and Db2 NEED_DATA path.

EOF
)"
```

---

### Task 8: Live same-handle `SQLCancel` proof

**Files:**
- Modify: `halcyon/tests/db2_roundtrip.rs` (or a `#[cfg(feature = "db2")]` module under `driver/db2.rs` if preferred for direct seam access)
- Gate: `HALCYON_TEST_DSN`

**Why not cached same-SQL re-execute:** a failed `execute_streaming` must poison
the `StmtLease`, so the next `Connection`/`Database` call prepares a **new**
handle and can succeed even when `SQLCancel` was omitted. That only proves
lease poison (Task 7 unit), not cancel.

**Required proof:** talk to `Db2CliDriver` with one **persistent**
`StatementHandle`:

- [ ] **Step 1: Write the failing live/seam test**

```rust
#[test]
fn aborted_streaming_execute_allows_reuse_of_same_statement_handle() {
    // HALCYON_TEST_DSN required; skip otherwise.
    // 1. Db2CliDriver::connect + prepare(sql) → keep StatementHandle H.
    // 2. bind + execute_streaming on H with a source that emits ≥1 chunk
    //    then returns Err(_). Assert Err.
    // 3. Without finalize/prepare: bind + execute (or execute_streaming that
    //    succeeds) again on the *same* H.
    // 4. Success ⇒ SQLCancel left H out of SQL_NEED_DATA.
    // Failure modes that must not pass: NEED_DATA / function sequence errors
    // on step 3.
}
```

Keep Task 7’s mock `execute_with_lobs_failure_poisons_statement_lease` as the
separate lease-poison assertion — do not fold it into this live test.

- [ ] **Step 2: Run against live Db2 (or confirm skip without DSN)**

```bash
HALCYON_TEST_DSN=... cargo test -p halcyon --features db2 aborted_streaming_execute_allows_reuse_of_same_statement_handle -- --nocapture
```

- [ ] **Step 3: Fix `Db2CliDriver` if the live test exposes a cancel gap; re-run Task 7 pure + pump suite**

```bash
cargo test -p halcyon --no-default-features
cargo test -p halcyon --features db2 --lib pump_
cargo test -p halcyon --features db2 --lib validate_lob
```

- [ ] **Step 4: Commit**

```bash
git commit -m "$(cat <<'EOF'
test(db2): prove SQLCancel via same StatementHandle reuse after LOB abort.

EOF
)"
```

---

### Task 9: Tracing log catalog (`observability`)

**Files:**
- Create: `halcyon/src/obs/events.rs`
- Modify: `obs/mod.rs`, `stubs.rs`, call sites (`pool`, `stmt_cache`, txn poison paths, connect)
- Test: recording subscriber assertions (same pattern as Phase 7)

**Events (exact names):**

| Event | Level |
|---|---|
| `connect.ok` / `connect.fail` | info / error |
| `reconnect.attempt` / `reconnect.ok` / `reconnect.fail` | warn / info / error |
| `retry.attempt` | warn |
| `pool.exhausted` / `pool.reap` | warn / debug |
| `stmt_cache.evict` | debug |
| `txn.poisoned` / `savepoint.poisoned` | error |
| `isolation.restore_fail` | error |

- [ ] **Step 1: Failing subscriber tests for 2–3 representative events** (`txn.poisoned`, `isolation.restore_fail`, `stmt_cache.evict`)

- [ ] **Step 2: Implement helpers + wire call sites; feature-off stubs compile out**

- [ ] **Step 3: Pass**

```bash
cargo test -p halcyon --no-default-features --features observability
cargo test -p halcyon --no-default-features
```

- [ ] **Step 4: Commit**

```bash
git commit -m "$(cat <<'EOF'
feat(obs): emit C++ structured log catalog as tracing events.

EOF
)"
```

---

### Task 10: Async wrappers

**Files:**
- Modify: `halcyon/src/async_db.rs`
- Test: `#[tokio::test]` in `async_db.rs`

**Interfaces:**

```rust
pub async fn transaction_with_isolation<T, F>(&self, iso: Isolation, f: F) -> Result<T>
where
    F: FnOnce(&ScopedTransaction) -> Result<T> + Send + 'static,
    T: Send + 'static;

pub async fn execute_with_lobs(
    &self,
    sql: impl Into<String>,
    params: Vec<OwnedBindParam>,
) -> Result<i64>;

/// Match existing `execute` / `query_as`: convert params before spawn_blocking.
pub async fn query_streaming<T, F>(
    &self,
    sql: impl Into<String>,
    params: impl IntoParams,
    f: F,
) -> Result<T>
where
    F: FnOnce(&mut StreamingQueryResult) -> Result<T> + Send + 'static,
    T: Send + 'static;
```

Implementation sketch (do **not** require `P: Send + 'static`):

```rust
let sql = sql.into();
let params = params.into_params(); // owned Params
let db = self.inner.clone();
run_blocking(&self.admit, move || {
    let mut rs = db.query_streaming(&sql, params)?;
    f(&mut rs)
})
.await
```

**No** `AsyncDatabase::begin` / `begin_with_isolation`. The closure receives `&mut StreamingQueryResult` (owned pooled cursor), never a borrow that could outlive the blocking task.

- [ ] **Step 1: Failing tokio tests** for each of the three APIs (mock pool)

- [ ] **Step 2: Implement via existing `run_blocking` / admission / obs context propagation**

- [ ] **Step 3: Pass**

```bash
cargo test -p halcyon --no-default-features --features tokio
cargo test -p halcyon --no-default-features --features tokio,observability
```

- [ ] **Step 4: Commit**

```bash
git commit -m "$(cat <<'EOF'
feat(async): isolation, LOB, and streaming closure APIs.

EOF
)"
```

---

### Task 11: Live Db2 + stress coverage

**Files:**
- Modify: `halcyon/tests/db2_roundtrip.rs`
- Modify: `halcyon/tests/stress.rs` (and/or stress helpers)
- Gate: `HALCYON_TEST_DSN`; run on Linux CI / local with clidriver

**Required live cases:**
1. Nested savepoints + name reuse rejection
2. UR sees in-flight dirty row; CS does **not** (Currently Committed — do not assert CS blocks)
3. LOB ≥32 MiB round-trip checksum (file + reader + callback), both directions
4. Ordinary `query` over a LOB column exercises ≤1-row fallback (correct Values)
5. Bounded block-fetch Values match prior behavior for non-LOB tables
6. Task 8’s `aborted_streaming_execute_allows_reuse_of_same_statement_handle` remains green

**Stress:** concurrent txns with savepoints + per-tx isolation; pool always restored or discarded.

- [ ] **Step 1: Write the live/stress tests (they may fail until env present)**

- [ ] **Step 2: Run pure + stress (no default db2)**

```bash
cargo test -p halcyon --no-default-features
cargo test -p halcyon --no-default-features --features stress
```

- [ ] **Step 3: Run live when DSN available**

```bash
HALCYON_TEST_DSN=... cargo test -p halcyon --features db2 --test db2_roundtrip -- --nocapture
```

- [ ] **Step 4: Commit**

```bash
git commit -m "$(cat <<'EOF'
test: live and stress coverage for v0.1 parity catch-up.

EOF
)"
```

---

### Task 12: Docs + release narrative (pre-publish)

**Files:**
- Modify: `README.md` (remove catch-up items from Deferred; note intentional ≤1-row fallback divergence)
- Modify: `CHANGELOG.md` (single **0.1.0** section covering prior surface **plus** catch-up; seam-implementers note for `CliDriver` break)
- Do **not** publish in this task — only docs. Publish follows Task 13 + `docs/RELEASING.md`.

- [ ] **Step 1: Update README deferred list and status blurb**

- [ ] **Step 2: Expand CHANGELOG 0.1.0**

- [ ] **Step 3: Commit**

```bash
git commit -m "$(cat <<'EOF'
docs: record v0.1 parity catch-up in README and CHANGELOG.

EOF
)"
```

---

### Task 13: Release-commit verification gate (non-publish)

Run the full matrix from [`docs/RELEASING.md`](../../RELEASING.md) §1 on the catch-up commit. Every checkbox in Tasks 1–12 can be green while this still fails — do not claim completion until it passes.

- [ ] **Step 1: fmt + clippy**

```bash
cargo fmt --all --check
cargo clippy --all-targets --features tokio,observability,stress -- -D warnings
cargo clippy -p halcyon --no-default-features --features derive,tokio,observability --all-targets -- -D warnings
```

- [ ] **Step 2: Pure-Rust suite**

```bash
cargo test -p halcyon --no-default-features
cargo test -p halcyon --no-default-features --features derive,tokio,observability,stress
```

- [ ] **Step 3: Full suite (needs clidriver)**

```bash
cargo test
```

- [ ] **Step 4: Concurrency + live**

```bash
RUSTFLAGS="--cfg loom" cargo test -p halcyon --no-default-features --release --lib pool::loom
# TSan stress (nightly, x86_64): follow .github/workflows/ci.yml `stress-tsan`
HALCYON_TEST_DSN=... cargo test --test db2_roundtrip -- --nocapture
```

- [ ] **Step 5: docs.rs simulation**

```bash
DOCS_RS=1 cargo doc -p halcyon-sys --no-deps
cargo doc -p halcyon --no-default-features --features derive,tokio,observability --no-deps
```

- [ ] **Step 6: Commit any fixes required to make the matrix green** (or confirm clean tree)

```bash
git commit -m "$(cat <<'EOF'
chore: clear release matrix for v0.1 parity catch-up.

EOF
)"
```

Do **not** `cargo publish` in this plan — that remains a human step after the matrix is green.

---

## Self-review (plan vs spec)

| Spec requirement | Task(s) |
|---|---|
| `InvalidArgument` / `InvalidState` | 1 |
| `Isolation` + real CLI `set_isolation` (no `todo!`) | 2 |
| Connect-time apply in `open_with_cache` + disconnect-on-fail | 3 |
| `PoolConfig::isolation` SoT; CS-normalized restore; begin-failure ownership | 3 |
| Savepoints / `nested` on both txn types | 4 |
| `fetch_block` replaces `fetch`/`get_column` | 5 |
| Rowset bind + ≤1-row fallback + budgets | 6 |
| Streaming cursor types locked; `execute_with_lobs(Vec)`; shared NEED_DATA pump | 7 |
| Same-handle live `SQLCancel`; lease poison unit (Task 7) | 7–8 |
| Tracing log catalog | 9 |
| Async `impl IntoParams` → owned before spawn; `FnOnce(&mut StreamingQueryResult)` | 10 |
| Live + pure stress (`--no-default-features --features stress`) | 11 |
| README / CHANGELOG | 12 |
| Full RELEASING.md matrix (non-publish) | 13 |
| Default `db2` compiles after every seam-growth task | Global + 2, 3, 5–7 |
| No LOB in `execute_batch`; no async lease-holding begin | 7, 10 |
| `ParamStreamSource` → `Result<usize>` (no `npos`) | 7 |

No TBD/placeholder steps remain. Types/names match design rev 5 throughout.
