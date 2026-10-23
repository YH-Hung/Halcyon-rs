//! Retry policy, exponential backoff, and read-only statement detection.
//!
//! Ported from the C++ `retry.hpp`. [`BackoffPolicy`] and [`ExecPolicy`] are
//! plain, `Clone` data — unlike the C++ originals they carry no injected `sleep`
//! closure. The retry loops (in [`crate::pool`] and the facade) sleep
//! `delay_for(attempt)` directly; unit tests keep the delay at zero so
//! `thread::sleep` returns instantly, which makes them deterministic without a
//! fake clock.
//!
//! `is_read_only` classifies a statement as safe to auto-retry (a read-only
//! `SELECT` / `WITH…SELECT` / `VALUES`), based on the C++ `detail::is_read_only`
//! scanner with SQL-comment handling — the facade uses it to pick a default retry
//! policy per call (writes run once; reads may replay after a transient error).

use std::time::Duration;

/// Exponential backoff with a hard cap. `base_delay * 2^(attempt-1)`, clamped to
/// `max_delay`. Ported from the C++ `BackoffPolicy` (the injected `sleep` is
/// dropped: callers sleep `delay_for` directly).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackoffPolicy {
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub max_attempts: u32,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        BackoffPolicy {
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(2000),
            max_attempts: 3,
        }
    }
}

impl BackoffPolicy {
    /// Delay before a 1-based `attempt`: `base_delay * 2^(attempt-1)`, capped at
    /// `max_delay`. The shift saturates at 2^30 (as in the C++ loop), and the
    /// multiply falls back to `max_delay` on the (clamped-away) overflow, so the
    /// result is always `<= max_delay`.
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(30);
        let mult = 1u32 << shift;
        let scaled = self.base_delay.checked_mul(mult).unwrap_or(self.max_delay);
        scaled.min(self.max_delay)
    }
}

/// Per-call retry policy. Idempotency is opt-in via `max_attempts > 1`; the
/// caller (or pool/facade) decides which statements are safe to replay. Ported
/// from the C++ `ExecPolicy`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffPolicy,
}

impl ExecPolicy {
    /// Run exactly once — no retry. The default for writes.
    pub fn once() -> Self {
        ExecPolicy {
            max_attempts: 1,
            backoff: BackoffPolicy::default(),
        }
    }

    /// Up to `attempts` tries (clamped to at least 1) for an idempotent
    /// statement — the facade's default for reads.
    pub fn idempotent(attempts: u32) -> Self {
        ExecPolicy {
            max_attempts: attempts.max(1),
            backoff: BackoffPolicy::default(),
        }
    }
}

impl Default for ExecPolicy {
    fn default() -> Self {
        ExecPolicy::once()
    }
}

/// True for statements safe to auto-retry: a read-only `SELECT` / `WITH…SELECT` /
/// `VALUES`. Based on the C++ `detail::is_read_only`.
///
/// The leading keyword must be a whole token (so `WITHOUT` / `SELECTED` /
/// `VALUESX` are rejected), and a `WITH` must resolve to a `SELECT` — a `WITH`
/// that feeds a data-change statement is not read-only. A data-change verb
/// (`INSERT`/`UPDATE`/`DELETE`/`MERGE`) as a whole token at *any* parenthesis
/// depth disqualifies the statement, so a Db2 data-change table reference
/// (`SELECT … FROM FINAL TABLE(INSERT …)`) is correctly classified as a write
/// even though it leads with `SELECT`. Quoted strings, delimited identifiers,
/// and SQL comments are skipped so their contents can't perturb the scan.
/// Comment handling hardens the original C++ scanner against hidden writes.
///
/// Operates on ASCII bytes: any non-ASCII byte in a literal or identifier is a
/// non-keyword, non-delimiter byte, exactly as the C++ scanner treats it.
pub(crate) fn is_read_only(sql: &str) -> bool {
    let b = sql.as_bytes();
    let n = b.len();
    let is_ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    // Case-insensitive compare of `a` against an already-uppercase keyword.
    let ieq = |a: &[u8], kw: &[u8]| -> bool {
        a.len() == kw.len() && a.iter().zip(kw).all(|(&x, &k)| x.to_ascii_uppercase() == k)
    };
    let is_data_change =
        |t: &[u8]| ieq(t, b"INSERT") || ieq(t, b"UPDATE") || ieq(t, b"DELETE") || ieq(t, b"MERGE");

    let mut i = 0;
    while i < n && b[i].is_ascii_whitespace() {
        i += 1;
    }
    let kw_start = i;
    while i < n && is_ident(b[i]) {
        i += 1;
    }
    let lead = &b[kw_start..i];
    let lead_select = ieq(lead, b"SELECT") || ieq(lead, b"VALUES");
    let lead_with = ieq(lead, b"WITH");
    if !lead_select && !lead_with {
        return false;
    }

    // Scan the remainder. A data-change verb at any depth means the statement
    // writes → not retryable. For a leading WITH, the main statement's verb is
    // the first depth-0 keyword token and must resolve to SELECT.
    let mut depth: i32 = 0;
    let mut with_main_resolved = false;
    let mut with_main_select = false;
    while i < n {
        let ch = b[i];
        // Comments may contain unmatched quotes or keywords. Skip them before
        // interpreting either, so a comment cannot conceal an embedded write.
        if b[i..].starts_with(b"--") {
            i += 2;
            while i < n && b[i] != b'\n' && b[i] != b'\r' {
                i += 1;
            }
            continue;
        }
        if b[i..].starts_with(b"/*") {
            i += 2;
            let mut nesting = 1usize;
            while i < n && nesting > 0 {
                if b[i..].starts_with(b"/*") {
                    nesting += 1;
                    i += 2;
                } else if b[i..].starts_with(b"*/") {
                    nesting -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if nesting != 0 {
                return false;
            }
            continue;
        }
        if ch == b'\'' || ch == b'"' {
            let quote = ch;
            i += 1;
            while i < n {
                if b[i] == quote {
                    if i + 1 < n && b[i + 1] == quote {
                        i += 2; // doubled quote = escaped, stay in the literal
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if ch == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if ch == b')' {
            if depth > 0 {
                depth -= 1;
            }
            i += 1;
            continue;
        }
        if is_ident(ch) {
            let s = i;
            while i < n && is_ident(b[i]) {
                i += 1;
            }
            let tok = &b[s..i];
            if is_data_change(tok) {
                return false; // write at any depth
            }
            if lead_with && depth == 0 && !with_main_resolved && ieq(tok, b"SELECT") {
                with_main_select = true; // main statement is a SELECT
                with_main_resolved = true;
            }
            continue;
        }
        i += 1;
    }
    // Leading SELECT/VALUES with no embedded write is safe; a WITH is safe only
    // if its main statement resolved to a SELECT.
    if lead_with {
        with_main_select
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_for_doubles_then_caps() {
        let p = BackoffPolicy {
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(2000),
            max_attempts: 10,
        };
        assert_eq!(p.delay_for(1), Duration::from_millis(50)); // 50 * 2^0
        assert_eq!(p.delay_for(2), Duration::from_millis(100)); // 50 * 2^1
        assert_eq!(p.delay_for(3), Duration::from_millis(200)); // 50 * 2^2
        assert_eq!(p.delay_for(6), Duration::from_millis(1600)); // 50 * 2^5
        assert_eq!(p.delay_for(7), Duration::from_millis(2000)); // 50 * 2^6 = 3200 -> capped
        assert_eq!(p.delay_for(50), Duration::from_millis(2000)); // stays capped, no overflow
    }

    #[test]
    fn delay_for_attempt_zero_treated_as_one() {
        let p = BackoffPolicy::default();
        assert_eq!(p.delay_for(0), p.delay_for(1));
    }

    #[test]
    fn zero_base_delay_is_instant_for_deterministic_tests() {
        let p = BackoffPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            max_attempts: 5,
        };
        for attempt in 1..=5 {
            assert_eq!(p.delay_for(attempt), Duration::ZERO);
        }
    }

    #[test]
    fn exec_policy_constructors() {
        assert_eq!(ExecPolicy::once().max_attempts, 1);
        assert_eq!(ExecPolicy::default(), ExecPolicy::once());
        assert_eq!(ExecPolicy::idempotent(3).max_attempts, 3);
        // Clamped to at least one attempt.
        assert_eq!(ExecPolicy::idempotent(0).max_attempts, 1);
    }

    #[test]
    fn read_only_leading_keywords() {
        assert!(is_read_only("SELECT * FROM t"));
        assert!(is_read_only("   select 1 from sysibm.sysdummy1"));
        assert!(is_read_only("VALUES (1)"));
        assert!(is_read_only("values current date"));
        assert!(is_read_only("WITH x AS (SELECT 1) SELECT * FROM x"));
    }

    #[test]
    fn writes_are_not_read_only() {
        assert!(!is_read_only("INSERT INTO t VALUES (1)"));
        assert!(!is_read_only("UPDATE t SET x = 1"));
        assert!(!is_read_only("DELETE FROM t WHERE id = 1"));
        assert!(!is_read_only("MERGE INTO t USING s ON t.id = s.id"));
        assert!(!is_read_only("CALL some_proc()"));
        assert!(!is_read_only(""));
    }

    #[test]
    fn leading_keyword_must_be_a_whole_token() {
        assert!(!is_read_only("WITHOUT ROWID")); // not WITH
        assert!(!is_read_only("SELECTED foo")); // not SELECT
        assert!(!is_read_only("VALUESX (1)")); // not VALUES
    }

    #[test]
    fn embedded_write_anywhere_disqualifies() {
        // Db2 data-change table reference: the write is nested in parens, but the
        // statement still mutates, so it must not be auto-retried.
        assert!(!is_read_only(
            "SELECT * FROM FINAL TABLE (INSERT INTO t VALUES (1))"
        ));
        assert!(!is_read_only(
            "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x"
        ));
        assert!(!is_read_only("WITH x AS (SELECT 1) UPDATE t SET a = 1"));
    }

    #[test]
    fn with_must_resolve_to_a_select() {
        assert!(is_read_only(
            "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a JOIN b"
        ));
        // A WITH whose main statement is not a SELECT is not read-only even with
        // no data-change verb (e.g. a bare CTE feeding nothing SELECT-shaped).
        assert!(!is_read_only("WITH x AS (SELECT 1) DELETE FROM x"));
    }

    #[test]
    fn sql_comments_cannot_hide_embedded_writes() {
        for sql in [
            "SELECT /* ' */ * FROM FINAL TABLE (INSERT INTO t VALUES (1))",
            "SELECT -- \"\n * FROM FINAL TABLE (INSERT INTO t VALUES (1))",
            "SELECT /* outer /* nested */ ' */ * FROM FINAL TABLE (INSERT INTO t VALUES (1))",
        ] {
            assert!(!is_read_only(sql), "mutation classified as read: {sql}");
        }
        assert!(is_read_only("SELECT /* ' INSERT */ 1"));
        assert!(is_read_only("SELECT -- ' INSERT\n 1"));
        assert!(is_read_only("SELECT '/* not a comment */'"));
        assert!(!is_read_only("SELECT /* unterminated"));
    }

    #[test]
    fn quoted_contents_do_not_perturb_the_scan() {
        // INSERT lives inside a string literal, so the statement is still a read.
        assert!(is_read_only("SELECT 'INSERT INTO t' AS note FROM t"));
        // Doubled quote is an escape, not a literal terminator.
        assert!(is_read_only("SELECT 'it''s a note' FROM t"));
        // Delimited identifier containing a keyword-looking word.
        assert!(is_read_only("SELECT \"INSERT\" FROM t"));
    }
}
