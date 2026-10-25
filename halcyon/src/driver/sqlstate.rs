use crate::error::ErrorCode;

/// Result of mapping a raw diagnostic onto Halcyon's error model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Classification {
    /// The error's category.
    pub code: ErrorCode,
    /// Whether a read that failed this way may be retried.
    pub retriable: bool,
}

/// Maps a 5-char SQLSTATE (plus the native Db2 SQLCODE) onto an [`ErrorCode`]
/// and the `retriable` flag that drives transparent auto-retry.
pub fn classify_sqlstate(sqlstate: &str, native_error: i32) -> Classification {
    if sqlstate.starts_with("08") {
        return Classification {
            code: ErrorCode::Connection,
            retriable: true,
        };
    }
    if native_error == -30081 {
        return Classification {
            code: ErrorCode::Transient,
            retriable: true,
        };
    }
    if sqlstate == "40001" {
        return Classification {
            code: ErrorCode::Deadlock,
            retriable: true,
        };
    }
    if sqlstate.starts_with("57") {
        // 57033 lock timeout / resource-not-available class: retriable timeout.
        return Classification {
            code: ErrorCode::Timeout,
            retriable: true,
        };
    }
    if sqlstate.starts_with("23") {
        return Classification {
            code: ErrorCode::Constraint,
            retriable: false,
        };
    }
    if sqlstate.starts_with("42") {
        return Classification {
            code: ErrorCode::Syntax,
            retriable: false,
        };
    }
    Classification {
        code: ErrorCode::Unknown,
        retriable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(sqlstate: &str, native: i32, code: ErrorCode, retriable: bool) {
        let c = classify_sqlstate(sqlstate, native);
        assert_eq!(c.code, code, "sqlstate={sqlstate} native={native}");
        assert_eq!(
            c.retriable, retriable,
            "sqlstate={sqlstate} native={native}"
        );
    }

    #[test]
    fn connection_class_08_is_retriable() {
        check("08001", 0, ErrorCode::Connection, true);
        check("08S01", -30081, ErrorCode::Connection, true); // 08 wins over native
    }

    #[test]
    fn native_30081_is_transient() {
        check("XXXXX", -30081, ErrorCode::Transient, true);
    }

    #[test]
    fn deadlock_40001_is_retriable() {
        check("40001", -911, ErrorCode::Deadlock, true);
    }

    #[test]
    fn class_57_is_retriable_timeout() {
        check("57033", -913, ErrorCode::Timeout, true);
        check("57014", 0, ErrorCode::Timeout, true);
    }

    #[test]
    fn constraint_and_syntax_are_not_retriable() {
        check("23505", -803, ErrorCode::Constraint, false);
        check("42601", -104, ErrorCode::Syntax, false);
    }

    #[test]
    fn anything_else_is_unknown() {
        check("22001", 0, ErrorCode::Unknown, false);
        check("", 0, ErrorCode::Unknown, false);
    }
}
