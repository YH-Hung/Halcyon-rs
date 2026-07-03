//! Batch shape validation, ported from the C++ `detail::validate_batch_rows`.
//! Portable (operates on [`Value`], no CLI types) and unit-testable without a
//! database.

use crate::error::{Error, ErrorCode, Result};
use crate::value::Value;

/// Discriminant used for type-homogeneity checks; `Null` matches any type.
fn type_index(v: &Value) -> Option<usize> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some(0),
        Value::Int(_) => Some(1),
        Value::Double(_) => Some(2),
        Value::Text(_) => Some(3),
        Value::Bytes(_) => Some(4),
    }
}

/// Validates that a batch is rectangular and each column is type-homogeneous,
/// ignoring NULLs. An empty batch is trivially valid.
///
/// Unlike C++ (where this runs only above the seam), `Db2CliDriver` also calls
/// it before any unsafe buffer work: violating the shape precondition from
/// safe code must yield a `Mapping` error, never an out-of-bounds write.
pub(crate) fn validate_batch_rows(rows: &[Vec<Value>]) -> Result<()> {
    let Some(first) = rows.first() else { return Ok(()) };
    let ncols = first.len();
    let mut col_type: Vec<Option<usize>> = vec![None; ncols];
    for (r, row) in rows.iter().enumerate() {
        if row.len() != ncols {
            return Err(Error::new(
                ErrorCode::Mapping,
                format!("batch row {r} has {} columns, expected {ncols}", row.len()),
            ));
        }
        for (c, v) in row.iter().enumerate() {
            let Some(idx) = type_index(v) else { continue }; // null: any type
            match col_type[c] {
                None => col_type[c] = Some(idx),
                Some(t) if t != idx => {
                    return Err(Error::new(
                        ErrorCode::Mapping,
                        format!("batch column {c} mixes value types (first at row {r})"),
                    ));
                }
                Some(_) => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_batch_is_valid() {
        assert!(validate_batch_rows(&[]).is_ok());
    }

    #[test]
    fn rectangular_homogeneous_batch_is_valid() {
        let rows = vec![
            vec![Value::Int(1), Value::Text("a".into())],
            vec![Value::Int(2), Value::Text("b".into())],
        ];
        assert!(validate_batch_rows(&rows).is_ok());
    }

    #[test]
    fn nulls_match_any_type() {
        let rows = vec![
            vec![Value::Null, Value::Null],
            vec![Value::Int(2), Value::Null],
            vec![Value::Null, Value::Text("b".into())],
        ];
        assert!(validate_batch_rows(&rows).is_ok());
    }

    #[test]
    fn ragged_rows_are_rejected() {
        let rows = vec![
            vec![Value::Int(1), Value::Text("a".into())],
            vec![Value::Int(2)],
        ];
        let e = validate_batch_rows(&rows).unwrap_err();
        assert_eq!(e.code, ErrorCode::Mapping);
        assert_eq!(e.message, "batch row 1 has 1 columns, expected 2");
    }

    #[test]
    fn mixed_column_types_are_rejected() {
        // The overflow scenario from review: a column resolved as Int (8-byte
        // slots) later carrying a longer Text value must be rejected up front.
        let rows = vec![
            vec![Value::Int(1)],
            vec![Value::Text("much longer than eight bytes".into())],
        ];
        let e = validate_batch_rows(&rows).unwrap_err();
        assert_eq!(e.code, ErrorCode::Mapping);
        assert_eq!(e.message, "batch column 0 mixes value types (first at row 1)");
    }
}
