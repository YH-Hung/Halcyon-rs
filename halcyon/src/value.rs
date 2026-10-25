/// CLI-agnostic value exchanged across the driver seam: how the core passes
/// parameters down and reads columns up without leaking CLI C types above the
/// seam. Mirrors the C++ boundary variant: integers travel as `i64`, floating
/// point as `f64`, text as UTF-8, binary as bytes, SQL NULL as `Null`.
/// `Bool` is bind-only-distinct — a boolean column reads back as `Int`.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL NULL.
    Null,
    /// A boolean parameter. Boolean columns read back as [`Int`](Self::Int).
    Bool(bool),
    /// Any integer type (`SMALLINT`, `INTEGER`, `BIGINT`).
    Int(i64),
    /// Any floating-point type (`REAL`, `DOUBLE`).
    Double(f64),
    /// Character data, as UTF-8.
    Text(String),
    /// Binary data.
    Bytes(Vec<u8>),
}

impl Value {
    /// Whether this is [`Null`](Self::Null).
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

macro_rules! impl_from_int {
    ($($t:ty),*) => {$(
        impl From<$t> for Value {
            fn from(v: $t) -> Self {
                Value::Int(i64::from(v))
            }
        }
    )*};
}
impl_from_int!(i8, i16, i32, i64, u8, u16, u32);

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Double(f64::from(v))
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Value::Bytes(v.to_vec())
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_widen_to_int() {
        assert_eq!(Value::from(7i32), Value::Int(7));
        assert_eq!(Value::from(7u16), Value::Int(7));
    }

    #[test]
    fn option_maps_none_to_null() {
        assert_eq!(Value::from(None::<i64>), Value::Null);
        assert_eq!(Value::from(Some("x")), Value::Text("x".into()));
    }
}
