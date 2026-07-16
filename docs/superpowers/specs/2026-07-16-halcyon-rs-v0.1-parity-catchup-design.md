# Halcyon-rs — v0.1 Parity Catch-up (C++ v1.1 + Block Fetch) — Design

**Date:** 2026-07-16  
**Status:** Approved design; implementation plan to follow.  
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

1. **Savepoints & nested transactions** — RAII `Savepoint` + `Transaction::nested`.
2. **Isolation-level API** — Db2 UR/CS/RS/RR as pool default and per-tx override.
3. **LOB streaming** — chunked read (`SQLGetData`) and write (data-at-exec /
   `SQLPutData`), O(chunk) memory.
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

### 1.3 Decisions locked in brainstorming

| Topic | Decision |
|---|---|
| Release sequencing | Fold catch-up into first crates.io publish (delay 0.1.0) |
| Scope | C++ v1.1 **plus** block fetch |
| Logging | C++ event catalog → `tracing` events under `observability` |
| Async | New APIs on `AsyncDatabase` via `spawn_blocking`; streaming reads as closures |
| Implementation shape | Dependency-ordered phases (Approach 1) |
| Version label | Still `0.1.0` (pre-1.0; initial published surface) |

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
Used for savepoint name validation, nested-scope misuse, ascending-column
violations, and similar programming errors — not for SQLSTATE classification.

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

### 3.3 `ConnectionParams`

```rust
pub struct ConnectionParams {
    pub connection_string: String,
    pub isolation: Option<Isolation>, // None = server default
}
```

Applied once at connect for every pooled connection. `PoolConfig` (or the
connect path that builds params) threads the pool-wide default through.

### 3.4 `CliDriver` growth

Match current C++ `ICliDriver` (`include/halcyon/detail/cli/driver.hpp`):

| Change | Role |
|---|---|
| Replace `fetch` + `get_column` with **`fetch_block(stmt, max_rows) -> Vec<Vec<Value>>`** | Normal read path (`query` / `query_as` / `ResultSet`). Empty block = EOF; short block ≠ EOF. Driver chooses rowset bind or long-column row-at-a-time fallback; Values byte-identical either way. Statement left clean for cache reuse. |
| **`set_isolation(conn, Isolation)`** | Pool default + per-tx override/restore |
| **`fetch_next` / `get_value` / `get_data_chunk`** | LOB / `query_streaming` path only |
| **`execute_streaming(stmt, params, sources)`** | Data-at-exec writes; `SQL_NEED_DATA` loop stays inside `Db2CliDriver` |

**Cursor rule (enforced above the seam):** one statement uses either
`fetch_block` *or* the streaming trio — never both.

Seam-level helpers (names idiomatic Rust, semantics = C++):

- `GetDataChunk { bytes, done, is_null }`
- `ParamStreamSource` — 0-based param index, pull callback (`0` = EOF;
  sentinel/`npos` equivalent = source failure), optional size hint, `is_clob`

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

- Pool / connect: optional default `isolation`.
- Overloads: `Connection::begin(iso)`, `Database::begin(iso)`,
  `transaction(iso, f)` (connection + facade).
- Override set immediately before begin (no open txn — CLI rule); restore on
  commit, rollback, and Drop. Restore failure → **poison** (same as autocommit
  restore failure) so a wrong-isolation connection never returns to the pool.

### 4.2 W2 — Savepoints

- `Transaction::savepoint()` / `savepoint(name)` → RAII `Savepoint` with
  `rollback()` / `release()`; Drop without either = rollback-to + release.
  Move-only, one-shot; terminal failure poisons savepoint **and** owning
  transaction.
- `Transaction::nested(|tx| -> Result<T>)`: `Ok` → release; `Err` / panic →
  rollback-to, then propagate. Same `Transaction` ref; calling
  `commit`/`rollback` inside nested → `InvalidState` after `fn`.
- Names: auto `halcyon_sp_<n>`; explicit
  `[A-Za-z_][A-Za-z0-9_]{0,127}`, ASCII-only, case-insensitively not starting
  with `SYS`. Live-name registry case-insensitive; reuse while live →
  `InvalidArgument`.
- Implementation: plain SQL through existing prepare/execute —
  `SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS`, `ROLLBACK TO SAVEPOINT`,
  `RELEASE SAVEPOINT`. Zero new seam methods.

### 4.3 W3 — Block fetch

- **Transparent:** public `query` / `query_as` / `ResultSet` contracts
  unchanged; portable layer calls `fetch_block` only.
- Fixed internal byte budget (mirror write-path batch budget). No public knob.
- Result sets with a LOB/long column: driver-internal row-at-a-time fallback
  (describe once, cache); same `Value` fidelity.
- Eliminates per-cell re-describe waste on the fast path.

### 4.4 W4 — LOB streaming

**Read**

- `query_streaming` wherever `query` exists (connection, transaction, facade).
  Returned `StreamingResultSet` owns the pool lease for its lifetime (like
  today’s owned `QueryResult`).
- Dedicated row-at-a-time path (`fetch_next` + `get_value` / `get_data_chunk`);
  **no** block binding so `SQLGetData` is legal.
- Ascending column access enforced above the seam → `InvalidState` on
  violation.
- `LobReader`: `read(&mut [u8]) -> Result<usize>` (`0` = EOF), `is_null()`,
  convenience sinks (`to_vec`, `to_string`, file/stream helpers as idiomatic
  Rust). Never auto-retried. Mid-stream abandon is safe (cursor close).

**Write**

- `lob_file` / `lob_stream` / `lob_callback` (+ `.as_clob()`) accepted as
  positional parameters alongside ordinary values; binding any LOB source
  routes through `execute_streaming`.
- Named params + LOB unsupported (documented; named path erases to `Value`).
- Default bind binary (BLOB); `.as_clob()` for CLOB targets.
- Driver pulls into a fixed staging buffer (~256 KiB) and `SQLPutData`s.
  Source failure → cancel statement (`SQLCancel`) + mapped error so a cached
  handle never rejoins in `SQL_NEED_DATA`.
- `execute_batch` + LOB → clear `Mapping` / `InvalidArgument` error.

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

**Invariant:** a pool lease must not cross `.await`.

| Sync API | Async shape |
|---|---|
| Isolation + savepoints / `nested` | Available inside async `transaction` / `begin` overloads — whole closure runs in one blocking task |
| LOB writes | `AsyncDatabase::execute` accepts owned/`'static`-friendly LOB sources; entire `execute_streaming` in one blocking task |
| Streaming reads | Closure API, e.g. `query_streaming(sql, params, \|rs\| { … })`, cursor lives only inside `spawn_blocking` |
| Sync escape hatch | `AsyncDatabase::blocking()` for sync streaming remains |

Admission semaphore and observability context propagation across
`spawn_blocking` follow Phase 6/7 rules unchanged. Future-drop still does not
cancel in-flight work (documented).

## 5. Testing strategy

TDD throughout. Full suite including live Db2 before any completion claim
(see README for macOS Docker DRDA caveat — live jobs run on Linux).

- **Unit (mock):** every new seam method; savepoint SQL emission and name gate
  (including `SYS` and injection attempts); `Savepoint` RAII all exits;
  isolation set/restore/poison; `LobReader` chunk boundaries (empty, NULL,
  exact buffer multiple, mid-stream abandon); `execute_streaming` NEED_DATA
  sequencing and source-failure cancellation; ascending-column enforcement;
  `fetch_block` empty/short/full + long-column fallback script; tracing events
  via recording subscriber.
- **Integration (live Db2):** nested savepoints and name reuse; UR sees an
  in-flight row from a second connection while CS does not (Currently
  Committed — do **not** assume CS blocks); LOB round-trips ≥ 32 MiB both
  directions by checksum (file/stream/callback); block-fetch Values match
  row-at-a-time for bounded columns.
- **Stress (TSan):** concurrent transactions with savepoints and per-tx
  isolation overrides; pool state always restored or connection discarded.
- **Async:** mock `#[tokio::test]` for isolation, LOB write, and
  `query_streaming` closure; DSN-gated live async round-trip in `live-db2`.

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
- **Trait growth** breaks out-of-tree `CliDriver` impls — called out in
  CHANGELOG under seam implementers.

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
`Logger` trait if downstream ever needs non-`tracing` sinks.

## 9. Implementation plan (next)

After this spec is reviewed, invoke the writing-plans skill to produce a
phased implementation plan under `docs/superpowers/plans/` with TDD tasks per
workstream, aligned to the phase order in §4.
