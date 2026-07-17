use std::fmt;

/// Coarse classification of a Halcyon error, mirroring the C++ `ErrorCode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    Connection,
    Timeout,
    Constraint,
    Syntax,
    Deadlock,
    Transient,
    Mapping,
    Pool,
    InvalidArgument,
    InvalidState,
    Unknown,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::Connection => "Connection",
            ErrorCode::Timeout => "Timeout",
            ErrorCode::Constraint => "Constraint",
            ErrorCode::Syntax => "Syntax",
            ErrorCode::Deadlock => "Deadlock",
            ErrorCode::Transient => "Transient",
            ErrorCode::Mapping => "Mapping",
            ErrorCode::Pool => "Pool",
            ErrorCode::InvalidArgument => "InvalidArgument",
            ErrorCode::InvalidState => "InvalidState",
            ErrorCode::Unknown => "Unknown",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Structured error value returned by all fallible Halcyon operations.
///
/// Carries a typed [`ErrorCode`], the raw 5-char SQLSTATE, the Db2 native
/// SQLCODE, a human-readable message, and a `retriable` flag that drives
/// auto-retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub code: ErrorCode,
    /// Raw 5-char SQLSTATE, e.g. "08001"; empty when not driver-originated.
    pub sqlstate: String,
    /// SQLCODE / native Db2 code; 0 when not driver-originated.
    pub native_error: i32,
    pub message: String,
    /// Transient class — drives transparent auto-retry.
    pub retriable: bool,
}

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Error {
            code,
            sqlstate: String::new(),
            native_error: 0,
            message: message.into(),
            retriable: false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code)?;
        if !self.sqlstate.is_empty() {
            write!(f, " [SQLSTATE {}]", self.sqlstate)?;
        }
        if self.native_error != 0 {
            write!(f, " [native {}]", self.native_error)?;
        }
        write!(f, ": {}", self.message)
    }
}

impl std::error::Error for Error {}

/// Convenience alias used across Halcyon. The C++ dual `Result<T>`/throwing
/// model collapses to this single idiomatic form in Rust.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_sqlstate_and_native_code_when_present() {
        let e = Error {
            code: ErrorCode::Deadlock,
            sqlstate: "40001".into(),
            native_error: -911,
            message: "deadlock or timeout".into(),
            retriable: true,
        };
        assert_eq!(
            e.to_string(),
            "Deadlock [SQLSTATE 40001] [native -911]: deadlock or timeout"
        );
    }

    #[test]
    fn display_is_minimal_for_non_driver_errors() {
        let e = Error::new(ErrorCode::Pool, "acquire timed out");
        assert_eq!(e.to_string(), "Pool: acquire timed out");
    }

    #[test]
    fn invalid_argument_and_invalid_state_display() {
        assert_eq!(ErrorCode::InvalidArgument.as_str(), "InvalidArgument");
        assert_eq!(ErrorCode::InvalidState.as_str(), "InvalidState");
        assert_eq!(
            Error::new(ErrorCode::InvalidArgument, "bad name").to_string(),
            "InvalidArgument: bad name"
        );
    }
}
