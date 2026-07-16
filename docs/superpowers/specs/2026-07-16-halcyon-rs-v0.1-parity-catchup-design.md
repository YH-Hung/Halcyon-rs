# Halcyon-rs — v0.1 Parity Catch-up (C++ v1.1 + Block Fetch) — Design

**Date:** 2026-07-16  
**Status:** Approved design (rev 5 — SQLCancel same-handle proof, shared
NEED_DATA pump tests, async IntoParams); implementation plan updated.  
**Baseline:** Halcyon-rs main (feature-complete vs C++ v1.0-ish; crates.io
publish pending).  
**C++ reference:** Halcyon [`v1.1.0`](https://github.com/YH-Hung/Halcyon/releases/tag/v1.1.0)
(2026-07-13) plus the earlier rowset-fetch work that shipped in C++ v1.0.  
**C++ specs:**
`../Halcyon/docs/superpowers/specs/2026-07-12-halcyon-v1.1-transactions-large-data-design.md`,
`../Halcyon/docs/superpowers/specs/2026-06-28-halcyon-rowset-fetch-design.md`.

## 1. Overview

Fold a full C++ feature catch-up into the **first crates.io publish** (`0.1.0`),
rather than shipping today’s surface and catching up as `0.2`. The publish
target is C++ parity through **v1.1**, plus the earlier deferred **block
fetch** (already in C++ v1.0). C++ v1.2 (coroutines / async-native streaming)
stays out of scope.

Four C++ v1.1 workstreams plus block fetch:

1. **Savepoints & nested transactions** — RAII `Savepoint` + `nested`.
2. **Isolation-level API** — Db2 UR/CS/RS/RR as pool default and per-tx override.
3. **LOB streaming** — chunked read (`SQLGetData`) and write (data-at-exec /
   `SQLPutData`), O(chunk) on streaming sinks.
4. **Structured log catalog** — C++ `ILogger` event names as `tracing` events
   under the existing `observability` feature (no new `Logger` trait).
5. **Block fetch** — transparent Db2 CLI rowset binding on the normal read path.

**Approach (user-approved):** dependency-ordered phases mirroring C++ (block
fetch before LOB so the long-column fallback exists first); native CLI mapping
with minimal seam growth; async exposure via `spawn_blocking` without leases
crossing `.await`.

### 1.1 Goals

- Ship the five items above with existing contracts intact: `Result`-only API,
  seam rule, mockable pure-Rust layers, pool poison/restore discipline, retry
  rules (LOB/streaming never auto-retried).
- Keep `cargo test -p halcyon --no-default-features` green after every phase.
- Expose the new sync surface on `AsyncDatabase` via `spawn_blocking` (streaming
  reads as closure APIs so cursors never outlive the blocking task).
- First crates.io publish is **`0.1.0`** with this enlarged surface; changelog
  and README deferred list updated accordingly.

### 1.2 Non-goals

- Windows / AIX.
- Scrollable / positionable cursors.
- LOB support inside `execute_batch`.
- XML / DECFLOAT or other new column types beyond BLOB/CLOB streaming.
- C++ v1.2 coroutine / async-native streaming.
- Public knobs for block or chunk size (fixed internal budgets, like C++).
- A separate pluggable `Logger` trait or in-tree log adapters (use `tracing`).
- Changing MSRV (stays 1.75 unless a hard dependency forces it — not expected).
- Async `begin` that returns a lease-holding transaction across `.await`.

### 1.3 Decisions locked in brainstorming

| Topic | Decision |
|---|---|
| Release sequencing | Fold catch-up into first crates.io publish (delay 0.1.0) |
| Scope | C++ v1.1 **plus** block fetch |
| Logging | C++ event catalog → `tracing` events under `observability` |
| Async | New APIs on `AsyncDatabase` via `spawn_blocking`; streaming reads as closures |
| Implementation shape | Dependency-ordered phases (Approach 1) |
| Version label | Still `0.1.0` (pre-1.0; initial published surface) |

### 1.4 Decisions locked in review rev 2

| Topic | Decision |
|---|---|
| Isolation `None` restore | Normalize restore target to `CursorStability` when no configured default (match C++ `value_or(CS)`); no getter/reset seam method |
| Isolation config SoT | `PoolConfig::isolation` for pooled connects; copied into `ConnectionParams` at connect. Standalone opens set `ConnectionParams` directly |
| Begin-after-override failure | If `set_isolation` succeeds but disabling autocommit fails, best-effort restore to the restore target before returning the error |
| Method names | No overloading: `begin_with_isolation`, `transaction_with_isolation`, `savepoint_named` |
| LOB params | Separate from `IntoParams` / `Params`; see §4.4 |
| Sync LOB entry | `execute_with_lobs(sql, Vec<BindParam<'_>>)` — consuming, single-use (rev 3) |
| Begin-failure ownership | Pooled lease `mark_broken`; standalone `Connection` marked unusable (rev 3) |
| Connect-time isolation | Applied in `Connection::open_with_cache` after `connect`; failure → `disconnect` then return (rev 4) |
| Streaming cursor types | `StreamingResultSet` / `StreamingQueryResult` / `TxStreamingCursor` — mirror `ResultSet` / `QueryResult` / `TxCursor` (rev 4) |
| Async streaming closure | `FnOnce(&mut StreamingQueryResult) -> Result<T>` (rev 4) |
| NEED_DATA safety | Oversized pull / panic / bad size-hint → `SQLCancel`; empty non-NULL → one zero-length `SQLPutData` (rev 4) |
| NEED_DATA test SoT | Shared pump helper used by `Db2CliDriver`; unit tests drive that helper (not only `MockCliDriver`) (rev 5) |
| SQLCancel proof | Same persistent `StatementHandle` abort→re-execute; separately assert streaming errors poison stmt leases (rev 5) |
| Async streaming params | `impl IntoParams` → owned `Params` before `spawn_blocking` (match `execute` / `query_as`) (rev 5) |
| Transaction surface | Savepoints/isolation on both `Transaction` and `ScopedTransaction`; async is closure-only |
| Long-column `fetch_block` | Fallback returns **at most one row** per call |
| `Savepoint` ownership | `Savepoint<'_>` holds `&Transaction` / `&ScopedTransaction`; interior mutability for registry; nested rejects `commit`/`rollback` by types |
| LOB memory claim | O(chunk) for `read` / file / writer sinks; materializing helpers are O(total) |

## 2. Guiding invariants (unchanged)

- **Seam rule.** Only `halcyon-sys` and `halcyon/src/driver/db2.rs` touch the
  IBM CLI ABI. `#![deny(unsafe_code)]` elsewhere.
- **Mockability.** Everything above the seam tested against mock/`Fake`
  `CliDriver` with no live database and no clidriver.
- **Errors.** `Result<T, Error>` only. New codes: `InvalidArgument`,
  `InvalidState`. Wire classification stays in `driver/sqlstate.rs`.
- **Optional deps.** `db2` / `derive` / `tokio` / `observability` feature gates
  unchanged in spirit; no new hard dependencies.

## 3. Seam & shared types

### 3.1 `ErrorCode`

Add `InvalidArgument` and `InvalidState` (and their `as_str` / Display arms).
Used for savepoint name validation, ascending-column violations, and similar
programming errors — not for SQLSTATE classification. Nested
`commit`/`rollback` are rejected by the type system (§4.2), not this code.

### 3.2 `Isolation`

New public enum with **Db2 terminology**:

```text
UncommittedRead | CursorStability | ReadStability | RepeatableRead
```

CLI mapping lives only in `driver/db2.rs`:

| Variant | CLI |
|---|---|
| `UncommittedRead` | `SQL_TXN_READ_UNCOMMITTED` |
| `CursorStability` | `SQL_TXN_READ_COMMITTED` |
| `ReadStability` | `SQL_TXN_REPEATABLE_READ` |
| `RepeatableRead` | `SQL_TXN_SERIALIZABLE` |

### 3.3 Isolation configuration & restore (rev 2)

**Single source of truth for pools:** `PoolConfig::isolation: Option<Isolation>`.

- `None` = do **not** call `set_isolation` at connect (leave the server/session
  default untouched).
- `Some(level)` = call `set_isolation` once after connect for every pooled
  connection (including reconnects).

When opening a pooled connection, the pool copies
`PoolConfig::isolation` into `ConnectionParams::isolation`. Standalone
`Connection::open*` callers set `ConnectionParams` directly — there is no
second competing knob.

**Uniform apply owner (rev 4):** `Connection::open_with_cache` (and `open`,
which delegates to it) is the **only** place that applies
`params.isolation` after a successful `driver.connect`:

1. `handle = driver.connect(params)?`
2. If `params.isolation` is `Some(level)` → `driver.set_isolation(handle, level)`.
   On failure → **best-effort** `driver.disconnect(handle)`, then return the
   isolation error (never leave an open handle or a half-warmed pool slot).
3. On success → construct `Connection` storing `configured_isolation =
   params.isolation` for later restore-target computation.

Pool warmup and reconnect both go through `open_with_cache`, so the same
cleanup applies. Do **not** apply isolation inside `CliDriver::connect` or
in the pool separately.

**Restore target after a per-transaction override** (matches C++
`defaultIsolation_.value_or(Isolation::CursorStability)`):

```text
restore_target = configured_default.unwrap_or(Isolation::CursorStability)
```

Rationale: the seam only has `set_isolation(level)` — there is no
“reset to unknown server default.” Halcyon therefore treats CS as the
normalized restore baseline whenever the configured default was `None`.
Document this clearly: `None` means “don’t set at connect,” not “restore to
an unknowable prior.”

Each connection (or its lease wrapper) remembers its configured default
(`Option<Isolation>`) so overrides know the restore target without re-reading
pool config.

**Begin sequence for `begin_with_isolation(level)`:**

1. `set_isolation(conn, level)`. On failure → return error (autocommit
   untouched; connection still usable).
2. Disable autocommit. On failure → **best-effort**
   `set_isolation(conn, restore_target)`, then return the autocommit error.
   Ownership of a failed compensation (restore itself fails, or we cannot
   trust isolation/autocommit state):
   - **Pooled** (`Database` / `ScopedTransaction` begin path): mark the
     lease broken so the pool **discards** it on drop — never returns a
     wrong-isolation or half-begun connection to the idle list.
   - **Standalone** (`Connection::begin_with_isolation`): mark the
     `Connection` **unusable** via an interior flag (new; today’s
     `Connection` has no broken-state). Subsequent public ops return
     `ErrorCode::InvalidState` (“connection unusable after failed
     isolation/autocommit transition”). Drop still disconnects. Do **not**
     leave the handle silently usable with unknown isolation.
3. On success → store `restore_target` on the transaction; restore on every
   exit path (commit, rollback, Drop). Restore failure → same ownership
   rules as above (poison the transaction **and** mark lease broken /
   standalone connection unusable).

Plain `begin()` (no override) does not touch isolation and does not restore.

### 3.4 `CliDriver` growth

Match current C++ `ICliDriver` (`include/halcyon/detail/cli/driver.hpp`), with
the long-column memory fix in §4.3:

| Change | Role |
|---|---|
| Replace `fetch` + `get_column` with **`fetch_block(stmt, max_rows) -> Vec<Vec<Value>>`** | Normal read path. Empty block = EOF; short block ≠ EOF. Rowset bind when all columns bounded; fallback when any column is long/LOB (§4.3). Statement left clean for cache reuse. |
| **`set_isolation(conn, Isolation)`** | Connect-time default + per-tx override/restore |
| **`fetch_next` / `get_value` / `get_data_chunk`** | LOB / `query_streaming` path only |
| **`execute_streaming(stmt, values, sources)`** | Data-at-exec writes; `SQL_NEED_DATA` loop stays inside `Db2CliDriver` |

**Cursor rule (enforced above the seam):** one statement uses either
`fetch_block` *or* the streaming trio — never both.

Seam-level helpers:

- `GetDataChunk { bytes, done, is_null }`
- `ParamStreamSource` — 0-based param index; pull callback returning
  **`Result<usize>`** (`Ok(0)` = EOF, `Err(_)` = source failure with retained
  detail — **not** a C++-style `npos` sentinel); optional size hint; `is_clob`

### 3.5 `halcyon-sys`

Hand-written FFI only for entry points the new paths need (rowset attrs /
`SQLBindCol` as required, `SQLGetData`, `SQLPutData`, `SQLParamData`,
`SQLCancel`, isolation connect attr). No bindgen. Preserve verified
`SQLLEN`/`SQLULEN` widths (32-bit on supported non-Windows targets).

### 3.6 Mocks

`MockCliDriver` and stress `ConcurrentFakeDriver` implement every new method
with scriptable blocks/chunks so `--no-default-features` coverage stays
complete. Out-of-tree `CliDriver` impls break (pre-1.0; changelog notes “seam
implementers”).

## 4. Workstreams (public API)

Phase order: **ErrorCodes + tracing scaffold → isolation → savepoints → block
fetch → LOB → async wrappers → live hardening + crates.io**.

### 4.1 W1 — Isolation

**Names (no overloading):**

| Existing | New |
|---|---|
| `begin()` | `begin_with_isolation(iso)` |
| `transaction(f)` | `transaction_with_isolation(iso, f)` |

Present on `Connection`, `Database` (facade), and — for the closure form —
`AsyncDatabase` (§4.6). Semantics: §3.3.

### 4.2 W2 — Savepoints

**Surface on both transaction types** (same semantics):

- `core::Transaction<'c>` — borrows a `Connection` (connection-local API).
- `facade::ScopedTransaction` — owns a pool lease (`Database::begin`).

Shared behavior lives in a private helper or duplicated thin wrappers; public
docs present them as one contract.

**Names:**

| Method | Role |
|---|---|
| `savepoint()` | Auto name `halcyon_sp_<n>` |
| `savepoint_named(name)` | Explicit validated name |
| `nested(f)` | Savepoint-backed scope |

**`Savepoint` ownership (rev 2):**

```text
Savepoint<'_>  // holds &Transaction or &ScopedTransaction (shared borrow)
```

- Execute / query on the transaction stay callable while a savepoint lives
  (`&self` methods) — matches C++.
- `commit` / `rollback` remain `&mut self` → **cannot** be called while any
  `Savepoint` (or `nested` closure borrow) is alive — type-system rejection.
- Live-name registry, auto-name counter, and poison flags use interior
  mutability (`Cell` / `RefCell`) so `&self` can create/release savepoints.
- `Savepoint`: `rollback()` / `release()`; Drop without either = rollback-to +
  release. Move-only, one-shot; terminal failure poisons the savepoint **and**
  the owning transaction.

**`nested`:**

```rust
fn nested<T, F>(&self, f: F) -> Result<T>
where
    F: FnOnce(&Self) -> Result<T>;  // &Transaction or &ScopedTransaction
```

`Ok` → release; `Err` / panic → rollback-to, then propagate. Because the
closure receives `&Self`, `commit`/`rollback` (`&mut self`) are uncallable —
no runtime `InvalidState` for that case.

**Names:** auto `halcyon_sp_<n>`; explicit
`[A-Za-z_][A-Za-z0-9_]{0,127}`, ASCII-only, case-insensitively not starting
with `SYS`. Live-name registry case-insensitive; reuse while live →
`InvalidArgument`.

**Implementation:** plain SQL through existing prepare/execute —
`SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS`, `ROLLBACK TO SAVEPOINT`,
`RELEASE SAVEPOINT`. Zero new seam methods.

### 4.3 W3 — Block fetch

- **Transparent:** public `query` / `query_as` / `ResultSet` contracts
  unchanged; portable layer calls `fetch_block` only.
- **Bounded path:** fixed internal byte budget (mirror write-path batch
  budget, ~2 MiB) and max-row clamp (4096). No public knob. Columns bound
  once; one `SQLFetch` fills the block.
- **Long / LOB fallback (rev 2 — stricter than C++):** when any result column
  is unbounded (LOB/long, or width over the bind cap), `fetch_block` returns
  **at most one fully materialized row per call**. The portable layer still
  requests up to `max_rows`; the driver ignores the multi-row request in
  fallback mode. This keeps peak memory at O(one row’s Values), not
  O(`max_rows` × LOB size). (C++ still loops toward `max_rows` in fallback —
  a known gap we do **not** copy.)
- Materializing a single LOB cell into `Value` is still O(that cell’s size) —
  inherent to the non-streaming `query` path. Use `query_streaming` for
  O(chunk) reads.
- Describe once, cache column metadata; Values match the streaming scalar
  path for bounded cells.
- **Live tests** must exercise the long-column fallback itself (a result set
  containing a LOB/long column via ordinary `query` / `query_as`), not only
  bounded-column equivalence.

### 4.4 W4 — LOB streaming

#### Read — cursor ownership (rev 4)

Mirror today’s non-streaming shapes exactly:

| Entry point | Returns | Ownership / poison |
|---|---|---|
| `Connection::query_streaming` | `StreamingResultSet<'_>` | Borrows `Connection` via statement lease (like `ResultSet`). Driver error recorded on the cursor; Drop closes cursor / returns stmt to cache. |
| `Transaction::query_streaming` | `StreamingResultSet<'_>` | Same borrowed cursor; must drop before `commit`/`rollback`. |
| `Database::query_streaming` | `StreamingQueryResult` | Owns `PooledConnection` + stmt lease (like `QueryResult`). Drop: poison stmt on any driver error; `mark_broken` on `ErrorCode::Connection`. |
| `ScopedTransaction::query_streaming` | `TxStreamingCursor<'_>` | Borrows the scoped txn; wraps a `StreamingResultSet` and a `&Cell<bool>` `conn_broken` slot (like `TxCursor`). Drop: if connection-class error, set `conn_broken` so the txn discards the lease. |
| `AsyncDatabase::query_streaming` | closure only | `query_streaming(sql, params, \|rs: &mut StreamingQueryResult\| { … })` — owned pooled cursor lives only inside one `spawn_blocking` task. |

Supporting types (public):

- `StreamingRow<'_>` — lending row view; `get<T: FromColumn>(col)` for scalars;
  `lob(col) -> Result<LobReader<'_>>` for LOB cells.
- `LobReader<'_>` — as below; invalidated by the next `next()` or by moving the
  parent cursor.

Dedicated row-at-a-time path (`fetch_next` + `get_value` /
`get_data_chunk`); **no** block binding so `SQLGetData` is legal.
Ascending column access enforced above the seam → `InvalidState` on
violation.

`LobReader`:
- **O(chunk):** `read(&mut [u8]) -> Result<usize>` (`Ok(0)` = EOF),
  `is_null()`, copy-to-file / copy-to-`Write` sinks.
- **O(total LOB size):** `to_vec()`, `to_string()` — documented as
  materializing helpers, not covered by the O(chunk) guarantee.

Never auto-retried. Mid-stream abandon is safe (cursor close / Drop).

#### Write — separate from `IntoParams` (rev 2/3)

Ordinary `execute` / `IntoParams` / `Params` stay **Value-only and `Clone`**
(required for auto-retry). LOB writes do **not** extend those traits.

```rust
/// One positional bind for a streaming execute.
pub enum BindParam<'a> {
    Value(Value),
    Lob(LobSource<'a>),
}

/// Sync LOB source — borrowed, single-use, not Clone.
pub enum LobSource<'a> {
    File { path: PathBuf, as_clob: bool },
    Reader {
        reader: &'a mut (dyn std::io::Read + Send),
        size_hint: Option<u64>,
        as_clob: bool,
    },
    Callback {
        pull: Box<dyn FnMut(&mut [u8]) -> Result<usize> + 'a>,
        size_hint: Option<u64>,
        as_clob: bool,
    },
}

/// Async / owned LOB source — `Send + 'static`, single-use, not Clone.
pub enum OwnedLobSource {
    File { path: PathBuf, as_clob: bool },
    Callback {
        pull: Box<dyn FnMut(&mut [u8]) -> Result<usize> + Send + 'static>,
        size_hint: Option<u64>,
        as_clob: bool,
    },
    // No borrowed Reader — cannot cross spawn_blocking safely.
}

pub enum OwnedBindParam {
    Value(Value),
    Lob(OwnedLobSource),
}
```

Constructors: `lob_file(path)`, `lob_reader(r, hint)`, `lob_callback(f, hint)`,
each with `.as_clob()`. Packing: build a `Vec<BindParam>` (or a small
builder that yields one). Named parameters + LOB are unsupported
(documented).

**Entry points (rev 3):**

| Sync | Async |
|---|---|
| `execute_with_lobs(sql, Vec<BindParam<'_>>)` | `execute_with_lobs(sql, Vec<OwnedBindParam>)` |

Sync takes an **owned** `Vec` (not `&[BindParam]` / not `&mut [BindParam]`):
`LobSource` holds `&mut dyn Read` and `FnMut` pulls, which require exclusive
mutation during the NEED_DATA loop, and sources are single-use — consuming
the vector matches both constraints (an immutable slice fails to compile
with E0596).

Both always use **once** execution policy (no auto-retry; sources are
single-use and not `Clone`). Routing: any `Lob` variant →
`CliDriver::execute_streaming`; all-`Value` is still allowed on this path but
callers should prefer ordinary `execute` for that case.

Default bind binary (BLOB); `.as_clob()` for CLOB. Driver staging buffer
**256 KiB** (`LOB_STAGE_BYTES`). `execute_batch` does not accept LOB types
(type system).

Seam pull callback: `FnMut(&mut [u8]) -> Result<usize>` — `Ok(0)` EOF,
`Err` aborts with detail retained (no `npos`).

**NEED_DATA safety protocol (rev 4/5 — match C++ `executeStreaming`):**

Extract a **shared pump helper** inside `driver/db2.rs` (e.g.
`pump_need_data_chunk` / `validate_lob_size_hint`) that
`Db2CliDriver::execute_streaming` calls for every `SQL_NEED_DATA` token.
Unit tests must exercise **that helper** (recording put/cancel callbacks) —
`MockCliDriver` sequencing alone is insufficient because it cannot prove the
real FFI path rejects an oversized pull before `SQLPutData`.

For every token / pull:

1. Pull into the staging buffer. On source `Err` → `SQLCancel` + propagate
   (retain source detail, optionally wrap with context).
2. If pull returns `n > stage.len()` → `SQLCancel` + `Mapping` (“oversized
   chunk”) — never pass an oversize length to `SQLPutData` (would FFI
   over-read).
3. If the pull callback **panics** → catch at the seam (`catch_unwind`),
   `SQLCancel`, return `Mapping` (“LOB source pull panicked”). Panics must
   not escape while the statement is in `SQL_NEED_DATA`.
4. Validate `size_hint` before bind (shared helper): must fit in the
   **32-bit** `SQLLEN` / `SQLULEN` used on non-Windows targets; overflow →
   `InvalidArgument` before execute (no NEED_DATA entered).
5. Empty non-NULL LOB (pull returns `Ok(0)` with no prior PutData) → issue
   **one** zero-length `SQLPutData` so the value is present, not omitted.
6. Any `SQLPutData` / unknown-token failure → `SQLCancel` before returning.

**SQLCancel proof (rev 5 — two separate assertions):**

Cached re-execute of the same SQL is **not** a proof: a failed
`execute_streaming` must **poison** the statement lease (like ordinary
`execute` errors), so the next call prepares a **new** handle and can succeed
even if `SQLCancel` was omitted.

1. **Same-handle seam test** (DSN-gated, talks to `Db2CliDriver` directly):
   prepare once → keep that `StatementHandle` → `execute_streaming` that
   emits ≥1 chunk then `Err` → then successfully `execute` /
   `execute_streaming` again on **that exact handle**. Success proves
   `SQLCancel` left the CLI statement reusable (not stuck in `NEED_DATA`).
2. **Lease-poison unit test** (mock): `Connection::execute_with_lobs`
   failure poisons the `StmtLease` so the cache finalizes it and the next
   acquire reprepares (mirror `execute_error_poisons_cached_statement_…`).
### 4.5 W5 — Tracing log catalog

Under feature `observability`, emit the C++ structured-log event names as
`tracing` events (fields mirror C++ `LogField`s). Initial set:

| Event | Level |
|---|---|
| `connect.ok` / `connect.fail` | info / error |
| `reconnect.attempt` / `reconnect.ok` / `reconnect.fail` | warn / info / error |
| `retry.attempt` | warn |
| `pool.exhausted` / `pool.reap` | warn / debug |
| `stmt_cache.evict` | debug |
| `txn.poisoned` / `savepoint.poisoned` | error |
| `isolation.restore_fail` | error |

No new logger trait. Feature off → events compile out. Covered by a recording
`tracing` subscriber in mock tests (same pattern as Phase 7 spans).

### 4.6 W6 — Async (`spawn_blocking`)

**Invariant:** a pool lease must not cross `.await`. Therefore there is **no**
`AsyncDatabase::begin` / `begin_with_isolation` returning a transaction
handle.

| Need | Async API |
|---|---|
| Isolation + savepoints / `nested` | `transaction_with_isolation(iso, f)` only — `f` receives `&ScopedTransaction` and runs entirely inside one `spawn_blocking` task (may call `savepoint` / `nested` / `execute_with_lobs` sync) |
| Plain txn (unchanged) | Existing `transaction(f)` |
| LOB writes | `execute_with_lobs(sql, Vec<OwnedBindParam>)` |
| Streaming reads | `query_streaming(sql, params, f)` — `params: impl IntoParams` converted to owned `Params` **before** `spawn_blocking` (same as `execute` / `query_as`); `f: FnOnce(&mut StreamingQueryResult) -> Result<T>` runs only inside the blocking task |
| Sync escape hatch | `blocking()` for sync streaming / RAII `begin` |

Admission semaphore and observability context propagation across
`spawn_blocking` follow Phase 6/7 rules unchanged. Future-drop still does not
cancel in-flight work (documented).

## 5. Testing strategy

TDD throughout. Full suite including live Db2 before any completion claim
(see README for macOS Docker DRDA caveat — live jobs run on Linux).

- **Unit (mock):** every new seam method; savepoint SQL emission and name gate
  (including `SYS` and injection attempts); `Savepoint` RAII all exits;
  isolation set/restore/poison; begin-failure restore after successful
  override; `None`-default restore uses CS; **standalone connection marked
  unusable** when compensation fails (subsequent ops → `InvalidState`);
  pooled begin path marks lease broken; **connect-time `set_isolation` failure
  disconnects** (standalone, pool warmup, reconnect — no leaked handle);
  `LobReader` chunk boundaries (empty, NULL, exact buffer multiple,
  mid-stream abandon); `execute_streaming` NEED_DATA sequencing via the
  **shared Db2 pump helper** (source-`Err`, oversized pull, panicking pull,
  size-hint overflow, empty non-NULL zero-length PutData) — not solely via
  `MockCliDriver`; ascending-column enforcement; `fetch_block` empty/short/full;
  **fallback returns ≤1 row** when a scripted unbounded column is present;
  tracing events via recording subscriber; compile-fail or type-level check
  that `nested` closures cannot call `commit`/`rollback` (optional trybuild,
  or rely on API shape + review); **`execute_with_lobs` failure poisons the
  statement lease** (next acquire reprepares).
- **Integration (live Db2):** nested savepoints and name reuse; UR sees an
  in-flight row from a second connection while CS does not (Currently
  Committed — do **not** assume CS blocks); LOB round-trips ≥ 32 MiB both
  directions by checksum (file/reader/callback); bounded block-fetch Values
  match prior row-at-a-time behavior; **ordinary `query` over a LOB column**
  exercises the one-row fallback (memory stays bounded per call; correctness
  of Values); **same persistent `StatementHandle`:** abort
  `execute_streaming` then succeed on that exact handle (proves real
  `SQLCancel` — not a cache reprepare).
- **Stress (TSan):** concurrent transactions with savepoints and per-tx
  isolation overrides; pool state always restored or connection discarded.
- **Async:** mock `#[tokio::test]` for `transaction_with_isolation`,
  `execute_with_lobs`, and `query_streaming` closure; DSN-gated live async
  round-trip in `live-db2`.

## 6. Risks & subtleties

- **Db2 savepoint semantics** verified early by live tests (nested support,
  name reuse, `ROLLBACK TO` destroying later savepoints).
- **Cursor Stability + Currently Committed:** with Db2 default `cur_commit=ON`,
  a CS reader does not block on an uncommitted row lock and does not see the
  dirty row — it reads the last committed version. UR still sees the dirty
  row. Tests assert this real behavior.
- **Savepoint name interpolation** is the one place SQL is built from user
  input; the identifier whitelist is a security gate with adversarial unit
  tests.
- **`SQLGetData` vs block bind** is why streaming is a separate statement path.
- **NULL into a LOB column via ordinary typed bind** can fail (`SQL0301N` /
  `07006`) — pre-existing untyped-NULL behavior. Document: use SQL `NULL`
  literal or omit the column. Streaming read of a NULL LOB is fine
  (`is_null()` after first read).
- **Statement cache × data-at-exec:** aborted streaming execute must
  `SQLCancel` **and** poison the statement lease. Prove cancel with a
  same-handle seam re-execute (plan Task 8); prove poison with a mock unit
  test. Cached same-SQL success alone is insufficient.
- **Connect-time isolation failure** must disconnect the new handle before
  returning (§3.3); pool warmup/reconnect share `open_with_cache`.
- **Isolation restore failure** must poison the transaction and mark the
  pooled lease broken / standalone connection unusable (§3.3).
- **`None` default + override:** restore always sets a concrete level (CS if
  unset). A connection that never had an override keeps whatever the server
  had at connect; after the first override+restore it will be CS (or the
  configured default), not a mysterious prior. Callers who need a stable
  non-CS baseline should set `PoolConfig::isolation` explicitly.
- **Trait growth** breaks out-of-tree `CliDriver` impls — called out in
  CHANGELOG under seam implementers.
- **C++ fallback memory gap:** C++ can return many LOB-bearing rows per
  `fetchBlock`; Rust intentionally returns ≤1. Document the intentional
  divergence in the changelog/seam notes.

## 7. Release gate

Replaces the current “feature-complete for v0.1; only publish remains” claim.

1. All phases merged; matrix in [`docs/RELEASING.md`](../../RELEASING.md) green
   (fmt, clippy ± features, pure-Rust tests, full `db2` tests, loom, TSan
   stress, live Db2, `cargo doc`).
2. README: mark catch-up items done; deferred list retains Windows, etc.
3. CHANGELOG: single **0.1.0** section covering prior surface **plus** this
   catch-up.
4. Publish `halcyon-sys` → `halcyon-derive` → `halcyon`; tag `v0.1.0`; GitHub
   release.

## 8. Out-of-scope follow-ups

Recorded so they are not lost: Windows/AIX; scrollable cursors; LOB in
`execute_batch`; XML/DECFLOAT; public block/chunk knobs; C++ v1.2-style
async-native streaming / coroutine layer; pool health-stats accessor; separate
`Logger` trait if downstream ever needs non-`tracing` sinks; seam getter for
current isolation (unnecessary given CS-normalized restore).

## 9. Implementation plan

Phased TDD plan (13 tasks, dependency order from §4):
[`docs/superpowers/plans/2026-07-16-halcyon-rs-v0.1-parity-catchup.md`](../plans/2026-07-16-halcyon-rs-v0.1-parity-catchup.md)
(rev aligned with design rev 5).
