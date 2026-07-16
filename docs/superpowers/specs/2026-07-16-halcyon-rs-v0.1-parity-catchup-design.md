# Halcyon-rs — v0.1 Parity Catch-up (C++ v1.1 + Block Fetch) — Design

**Date:** 2026-07-16  
**Status:** Approved design (rev 2 — review gaps closed); implementation plan
to follow.  
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
`PoolConfig::isolation` into `ConnectionParams::isolation` and the driver
applies it during `connect` (or immediately after). Standalone
`Connection::open*` callers set `ConnectionParams` directly — there is no
second competing knob.

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
   untouched).
2. Disable autocommit. On failure → **best-effort**
   `set_isolation(conn, restore_target)`, then return the autocommit error
   (connection poisoned / discarded if restore itself fails, same as today).
3. On success → store `restore_target` on the transaction; restore on every
   exit path (commit, rollback, Drop). Restore failure → **poison**.

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

#### Read

- `query_streaming` wherever `query` exists (`Connection`, `Transaction`,
  `ScopedTransaction`, `Database`). Facade form returns an owned cursor that
  holds the pool lease (like today’s `QueryResult`).
- Dedicated row-at-a-time path (`fetch_next` + `get_value` /
  `get_data_chunk`); **no** block binding so `SQLGetData` is legal.
- Ascending column access enforced above the seam → `InvalidState` on
  violation.
- `LobReader`:
  - **O(chunk):** `read(&mut [u8]) -> Result<usize>` (`Ok(0)` = EOF),
    `is_null()`, copy-to-file / copy-to-`Write` sinks.
  - **O(total LOB size):** `to_vec()`, `to_string()` — documented as
    materializing helpers, not covered by the O(chunk) guarantee.
- Never auto-retried. Mid-stream abandon is safe (cursor close).

#### Write — separate from `IntoParams` (rev 2)

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
each with `.as_clob()`. Packing: a slice/`Vec` of `BindParam` (or a small
builder). Named parameters + LOB are unsupported (documented).

**Entry points:**

| Sync | Async |
|---|---|
| `execute_with_lobs(sql, &[BindParam<'_>])` | `execute_with_lobs(sql, Vec<OwnedBindParam>)` |

Both always use **once** execution policy (no auto-retry; sources are
single-use and not `Clone`). Routing: any `Lob` variant →
`CliDriver::execute_streaming`; all-`Value` is still allowed on this path but
callers should prefer ordinary `execute` for that case.

Default bind binary (BLOB); `.as_clob()` for CLOB. Driver staging buffer
~256 KiB. Source `Err` → `SQLCancel` + propagate the source error (or wrap
with context) so a cached handle never rejoins in `SQL_NEED_DATA`.
`execute_batch` does not accept LOB types (type system); documenting a clear
error is unnecessary if LOBs cannot be passed.

Seam pull callback: `FnMut(&mut [u8]) -> Result<usize>` — `Ok(0)` EOF,
`Err` aborts with detail retained (no `npos`).

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
| Streaming reads | `query_streaming(sql, params, \|rs\| { … })` — cursor lives only inside the blocking task |
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
  override; `None`-default restore uses CS; `LobReader` chunk boundaries
  (empty, NULL, exact buffer multiple, mid-stream abandon); `execute_streaming`
  NEED_DATA sequencing and source-`Err` cancellation; ascending-column
  enforcement; `fetch_block` empty/short/full; **fallback returns ≤1 row**
  when a scripted unbounded column is present; tracing events via recording
  subscriber; compile-fail or type-level check that `nested` closures cannot
  call `commit`/`rollback` (optional trybuild, or rely on API shape + review).
- **Integration (live Db2):** nested savepoints and name reuse; UR sees an
  in-flight row from a second connection while CS does not (Currently
  Committed — do **not** assume CS blocks); LOB round-trips ≥ 32 MiB both
  directions by checksum (file/reader/callback); bounded block-fetch Values
  match prior row-at-a-time behavior; **ordinary `query` over a LOB column**
  exercises the one-row fallback (memory stays bounded per call; correctness
  of Values).
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
  `SQLCancel` before cache reuse.
- **Isolation restore failure** must poison.
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

## 9. Implementation plan (next)

After this revision is reviewed, invoke the writing-plans skill to produce a
phased implementation plan under `docs/superpowers/plans/` with TDD tasks per
workstream, aligned to the phase order in §4.
