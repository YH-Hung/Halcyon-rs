//! Typed column extraction: the reading half of the C++ `TypeBinder`.
//!
//! Binding (Rust value → [`Value`]) is already covered by the `From<T>` impls in
//! [`crate::value`]; this module is the inverse — [`FromColumn`] turns a neutral
//! [`Value`] read up from the seam into a concrete Rust type, preserving the C++
//! `from_value` semantics: NULL into a non-optional target is a `Mapping` error,
//! an out-of-range integer is a `Mapping` error, and a wrong variant is a
//! "type mismatch" `Mapping` error. Wrap the target in [`Option`] to accept NULL.

use crate::core::mapping;
use crate::error::Result;
use crate::value::Value;

/// Reads one neutral [`Value`] into a concrete Rust type.
///
/// Implemented for the primitive types that cross the seam: integers, floats,
/// `bool`, `String`, `Vec<u8>`, and `Option<T>` for nullability. Mirrors the
/// C++ `TypeBinder<T>::from_value`.
pub trait FromColumn: Sized {
    fn from_column(value: Value) -> Result<Self>;
}

// --- integers -------------------------------------------------------------
// Values travel across the seam as i64; narrowing to the target type is
// range-checked via TryFrom (an out-of-range value is a Mapping error, matching
// the C++ `in_range` check). A boolean reads as 1/0, as in C++.
macro_rules! impl_from_column_int {
    ($($t:ty),+ $(,)?) => {$(
        impl FromColumn for $t {
            fn from_column(value: Value) -> Result<Self> {
                match value {
                    Value::Int(i) => <$t>::try_from(i)
                        .map_err(|_| mapping("integer out of range for target type")),
                    Value::Bool(b) => Ok(if b { 1 } else { 0 }),
                    Value::Null => Err(mapping("NULL into non-optional integer")),
                    _ => Err(mapping("type mismatch: expected integer")),
                }
            }
        }
    )+};
}
impl_from_column_int!(i8, i16, i32, i64, u8, u16, u32, u64, isize, usize);

// --- bool -----------------------------------------------------------------
impl FromColumn for bool {
    fn from_column(value: Value) -> Result<Self> {
        match value {
            Value::Bool(b) => Ok(b),
            Value::Int(i) => Ok(i != 0),
            Value::Null => Err(mapping("NULL into non-optional bool")),
            _ => Err(mapping("type mismatch: expected bool")),
        }
    }
}

// --- floating point -------------------------------------------------------
macro_rules! impl_from_column_float {
    ($($t:ty),+ $(,)?) => {$(
        impl FromColumn for $t {
            fn from_column(value: Value) -> Result<Self> {
                match value {
                    Value::Double(d) => Ok(d as $t),
                    Value::Int(i) => Ok(i as $t),
                    Value::Null => Err(mapping("NULL into non-optional floating point")),
                    _ => Err(mapping("type mismatch: expected floating point")),
                }
            }
        }
    )+};
}
impl_from_column_float!(f32, f64);

// --- text -----------------------------------------------------------------
impl FromColumn for String {
    fn from_column(value: Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s),
            Value::Null => Err(mapping("NULL into non-optional string")),
            _ => Err(mapping("type mismatch: expected string")),
        }
    }
}

// --- binary ---------------------------------------------------------------
impl FromColumn for Vec<u8> {
    fn from_column(value: Value) -> Result<Self> {
        match value {
            Value::Bytes(b) => Ok(b),
            Value::Null => Err(mapping("NULL into non-optional binary")),
            _ => Err(mapping("type mismatch: expected binary")),
        }
    }
}

// --- nullability ----------------------------------------------------------
impl<T: FromColumn> FromColumn for Option<T> {
    fn from_column(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            other => T::from_column(other).map(Some),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn integers_read_and_range_check() {
        assert_eq!(i32::from_column(Value::Int(7)).unwrap(), 7);
        assert_eq!(u8::from_column(Value::Int(255)).unwrap(), 255);
        // Out of range for the target narrows to a Mapping error, not a wrap.
        let e = u8::from_column(Value::Int(256)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Mapping);
        assert_eq!(e.message, "integer out of range for target type");
        assert_eq!(
            i8::from_column(Value::Int(-200)).unwrap_err().code,
            ErrorCode::Mapping
        );
    }

    #[test]
    fn bool_and_int_interconvert_like_cpp() {
        assert_eq!(i32::from_column(Value::Bool(true)).unwrap(), 1);
        assert_eq!(i32::from_column(Value::Bool(false)).unwrap(), 0);
        assert!(bool::from_column(Value::Int(5)).unwrap());
        assert!(!bool::from_column(Value::Int(0)).unwrap());
        assert!(bool::from_column(Value::Bool(true)).unwrap());
    }

    #[test]
    fn floats_accept_double_and_int() {
        assert_eq!(f64::from_column(Value::Double(1.5)).unwrap(), 1.5);
        assert_eq!(f64::from_column(Value::Int(3)).unwrap(), 3.0);
        assert_eq!(f32::from_column(Value::Double(2.0)).unwrap(), 2.0_f32);
    }

    #[test]
    fn text_and_binary() {
        assert_eq!(String::from_column(Value::Text("hi".into())).unwrap(), "hi");
        assert_eq!(
            Vec::<u8>::from_column(Value::Bytes(vec![1, 2, 3])).unwrap(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn null_into_non_optional_is_mapping_error() {
        assert_eq!(
            i64::from_column(Value::Null).unwrap_err().code,
            ErrorCode::Mapping
        );
        assert_eq!(
            String::from_column(Value::Null).unwrap_err().code,
            ErrorCode::Mapping
        );
        assert_eq!(
            bool::from_column(Value::Null).unwrap_err().code,
            ErrorCode::Mapping
        );
    }

    #[test]
    fn option_accepts_null_as_none() {
        assert_eq!(Option::<i64>::from_column(Value::Null).unwrap(), None);
        assert_eq!(Option::<i64>::from_column(Value::Int(9)).unwrap(), Some(9));
        assert_eq!(
            Option::<String>::from_column(Value::Text("x".into())).unwrap(),
            Some("x".to_string())
        );
    }

    #[test]
    fn wrong_variant_is_type_mismatch() {
        let e = i64::from_column(Value::Text("nope".into())).unwrap_err();
        assert_eq!(e.code, ErrorCode::Mapping);
        assert_eq!(e.message, "type mismatch: expected integer");
        assert_eq!(
            String::from_column(Value::Int(1)).unwrap_err().message,
            "type mismatch: expected string"
        );
    }
}
