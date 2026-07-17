//! Bounded label helpers — port of C++ `detail::obs::op_label` / `code_label`.

use crate::error::ErrorCode;

/// Leading SQL verb → stable `{op}` label (lowercase, fixed set).
pub(crate) fn op_label(sql: &str) -> &'static str {
    let tok = leading_word(sql);
    match tok {
        s if eq_ignore_ascii_case(s, "SELECT") => "select",
        s if eq_ignore_ascii_case(s, "INSERT") => "insert",
        s if eq_ignore_ascii_case(s, "UPDATE") => "update",
        s if eq_ignore_ascii_case(s, "DELETE") => "delete",
        s if eq_ignore_ascii_case(s, "MERGE") => "merge",
        s if eq_ignore_ascii_case(s, "WITH") => "with",
        s if eq_ignore_ascii_case(s, "VALUES") => "values",
        _ => "other",
    }
}

/// Lowercased [`ErrorCode`] for `halcyon_errors_total{code}`.
pub(crate) fn code_label(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::Connection => "connection",
        ErrorCode::Timeout => "timeout",
        ErrorCode::Constraint => "constraint",
        ErrorCode::Syntax => "syntax",
        ErrorCode::Deadlock => "deadlock",
        ErrorCode::Transient => "transient",
        ErrorCode::Mapping => "mapping",
        ErrorCode::Pool => "pool",
        ErrorCode::InvalidArgument => "invalid_argument",
        ErrorCode::InvalidState => "invalid_state",
        ErrorCode::Unknown => "unknown",
    }
}

fn leading_word(sql: &str) -> &str {
    let s = sql.trim_start();
    let end = s
        .char_indices()
        .find(|(_, c)| !c.is_ascii_alphabetic())
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    &s[..end]
}

fn eq_ignore_ascii_case(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .all(|(x, y)| x.to_ascii_uppercase() == y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_label_classifies_verbs() {
        assert_eq!(op_label("  INSERT INTO t VALUES (?)"), "insert");
        assert_eq!(op_label("select 1"), "select");
        assert_eq!(op_label("WITH cte AS (SELECT 1) SELECT 1"), "with");
        assert_eq!(op_label("EXPLAIN SELECT 1"), "other");
    }

    #[test]
    fn code_label_is_lowercase() {
        assert_eq!(code_label(ErrorCode::Syntax), "syntax");
        assert_eq!(code_label(ErrorCode::Deadlock), "deadlock");
    }

    #[test]
    fn code_label_covers_programming_errors() {
        assert_eq!(code_label(ErrorCode::InvalidArgument), "invalid_argument");
        assert_eq!(code_label(ErrorCode::InvalidState), "invalid_state");
    }
}
