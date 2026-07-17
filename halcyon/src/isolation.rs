/// Db2 transaction isolation levels.
///
/// These use Db2's own terminology (UR/CS/RS/RR), rather than SQL-standard
/// names. In particular, Db2 Repeatable Read maps to the CLI serializable
/// isolation value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// Uncommitted Read (UR).
    UncommittedRead,
    /// Cursor Stability (CS), Db2's default.
    CursorStability,
    /// Read Stability (RS).
    ReadStability,
    /// Repeatable Read (RR).
    RepeatableRead,
}
