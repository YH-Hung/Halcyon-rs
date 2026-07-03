//! Statement parameters: positional (`?`) and named (`:name`).
//!
//! Positional params are supplied as a tuple of bindable values (`(21, "NYC")`)
//! or a `Vec<Value>`. Named params are supplied as [`Named`] (or the [`named!`]
//! macro) and rewritten to positional `?` by [`bind_named`], a literal-aware
//! scanner ported from the C++ `detail::bind_named`.

use crate::core::mapping;
use crate::error::Result;
use crate::value::Value;

/// Parameters bound to a statement, after erasing concrete types to [`Value`].
pub enum Params {
    /// Positional `?` parameters in order.
    Positional(Vec<Value>),
    /// Named `:name` bindings, resolved by [`bind_named`].
    Named(Vec<(String, Value)>),
}

/// Anything that can be supplied as a statement's parameters.
///
/// Implemented for `()` (no params), tuples of bindable values up to arity 12,
/// `Vec<Value>`, and [`Named`].
pub trait IntoParams {
    fn into_params(self) -> Params;
}

impl IntoParams for Params {
    fn into_params(self) -> Params {
        self
    }
}

impl IntoParams for () {
    fn into_params(self) -> Params {
        Params::Positional(Vec::new())
    }
}

impl IntoParams for Vec<Value> {
    fn into_params(self) -> Params {
        Params::Positional(self)
    }
}

macro_rules! impl_into_params_tuple {
    ($($T:ident $v:ident),+) => {
        impl<$($T: Into<Value>),+> IntoParams for ($($T,)+) {
            fn into_params(self) -> Params {
                let ($($v,)+) = self;
                Params::Positional(vec![$($v.into()),+])
            }
        }
    };
}
impl_into_params_tuple!(A a);
impl_into_params_tuple!(A a, B b);
impl_into_params_tuple!(A a, B b, C c);
impl_into_params_tuple!(A a, B b, C c, D d);
impl_into_params_tuple!(A a, B b, C c, D d, E e);
impl_into_params_tuple!(A a, B b, C c, D d, E e, F f);
impl_into_params_tuple!(A a, B b, C c, D d, E e, F f, G g);
impl_into_params_tuple!(A a, B b, C c, D d, E e, F f, G g, H h);
impl_into_params_tuple!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
impl_into_params_tuple!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
impl_into_params_tuple!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
impl_into_params_tuple!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l);

/// A collection of named bindings: `Named::new().bind("age", 21).bind("city", "NYC")`.
#[derive(Default)]
pub struct Named(Vec<(String, Value)>);

impl Named {
    pub fn new() -> Self {
        Named(Vec::new())
    }

    /// Adds one `:name` binding. A repeated name binds its value at each use.
    #[must_use]
    pub fn bind(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.0.push((name.into(), value.into()));
        self
    }
}

impl IntoParams for Named {
    fn into_params(self) -> Params {
        Params::Named(self.0)
    }
}

/// Builds a [`Named`] parameter set: `named!{ "age" => 21, "city" => "NYC" }`.
#[macro_export]
macro_rules! named {
    ($($name:expr => $value:expr),* $(,)?) => {
        $crate::Named::new()$(.bind($name, $value))*
    };
}

/// SQL with `:name` placeholders rewritten to positional `?`, plus the values in
/// appearance order.
#[derive(Debug)]
pub(crate) struct PreparedSql {
    pub sql: String,
    pub params: Vec<Value>,
}

/// Rewrites `:name` placeholders to `?` in appearance order, resolving each
/// against `named`. A repeated name binds its value again; `::` is left literal.
///
/// The scanner is literal-aware: text inside `'...'` string literals, `"..."`
/// delimited identifiers (both honouring the SQL doubled-quote escape), `--`
/// line comments, and `/* */` block comments is copied through verbatim, so a
/// `:name` appearing there is never mistaken for a placeholder. Ported char-for-
/// char from the C++ `detail::bind_named` (the "correctness trap" the design doc
/// flags).
pub(crate) fn bind_named(sql: &str, named: &[(String, Value)]) -> Result<PreparedSql> {
    let find =
        |name: &str| -> Option<&Value> { named.iter().find(|(n, _)| n == name).map(|(_, v)| v) };

    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut out = String::with_capacity(sql.len());
    let mut params = Vec::new();
    let mut i = 0;
    while i < n {
        let c = chars[i];

        // Quoted string literal / delimited identifier: copy through to the
        // matching close quote, treating a doubled quote as an escaped quote
        // that stays inside the literal.
        if c == '\'' || c == '"' {
            let quote = c;
            out.push(c);
            i += 1;
            while i < n {
                out.push(chars[i]);
                if chars[i] == quote {
                    if i + 1 < n && chars[i + 1] == quote {
                        out.push(chars[i + 1]); // doubled quote = escaped
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // -- line comment: copy through to (but not including) the newline.
        if c == '-' && i + 1 < n && chars[i + 1] == '-' {
            while i < n && chars[i] != '\n' {
                out.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // /* block comment */: copy through to the closing */ (or end of input).
        if c == '/' && i + 1 < n && chars[i + 1] == '*' {
            out.push('/');
            out.push('*');
            i += 2;
            while i < n {
                if chars[i] == '*' && i + 1 < n && chars[i + 1] == '/' {
                    out.push('*');
                    out.push('/');
                    i += 2;
                    break;
                }
                out.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // '::' cast operator: left literal.
        if c == ':' && i + 1 < n && chars[i + 1] == ':' {
            out.push(':');
            out.push(':');
            i += 2;
            continue;
        }

        let starts_name =
            c == ':' && i + 1 < n && (chars[i + 1].is_ascii_alphabetic() || chars[i + 1] == '_');
        if !starts_name {
            out.push(c);
            i += 1;
            continue;
        }

        let mut j = i + 1;
        while j < n && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
            j += 1;
        }
        let name: String = chars[i + 1..j].iter().collect();
        match find(&name) {
            Some(v) => {
                out.push('?');
                params.push(v.clone());
            }
            None => return Err(mapping(format!("unknown named parameter ':{name}'"))),
        }
        i = j;
    }

    Ok(PreparedSql { sql: out, params })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn positional(p: impl IntoParams) -> Vec<Value> {
        match p.into_params() {
            Params::Positional(v) => v,
            Params::Named(_) => panic!("expected positional"),
        }
    }

    #[test]
    fn tuples_pack_positionally_in_order() {
        let v = positional((21, "NYC", true));
        assert_eq!(
            v,
            vec![Value::Int(21), Value::Text("NYC".into()), Value::Bool(true)]
        );
    }

    #[test]
    fn unit_and_vec_are_positional() {
        assert_eq!(positional(()), Vec::<Value>::new());
        assert_eq!(positional(vec![Value::Int(1)]), vec![Value::Int(1)]);
    }

    #[test]
    fn named_macro_and_builder_agree() {
        let a = named! { "age" => 21, "city" => "NYC" };
        let b = Named::new().bind("age", 21).bind("city", "NYC");
        match (a.into_params(), b.into_params()) {
            (Params::Named(x), Params::Named(y)) => assert_eq!(x, y),
            _ => panic!("expected named"),
        }
    }

    // --- bind_named parser (mirrors the C++ test_parameters.cpp cases) ---

    fn rewrite(sql: &str, named: Named) -> Result<PreparedSql> {
        match named.into_params() {
            Params::Named(pairs) => bind_named(sql, &pairs),
            Params::Positional(_) => unreachable!(),
        }
    }

    #[test]
    fn named_rewrite_produces_positional_sql_and_values() {
        let r = rewrite(
            "SELECT id FROM u WHERE age > :age AND city = :city",
            named! { "age" => 21, "city" => "NYC" },
        )
        .unwrap();
        assert_eq!(r.sql, "SELECT id FROM u WHERE age > ? AND city = ?");
        assert_eq!(r.params, vec![Value::Int(21), Value::Text("NYC".into())]);
    }

    #[test]
    fn named_supports_repeated_placeholder() {
        let r = rewrite("VALUES (:x, :x)", named! { "x" => 5 }).unwrap();
        assert_eq!(r.sql, "VALUES (?, ?)");
        assert_eq!(r.params, vec![Value::Int(5), Value::Int(5)]);
    }

    #[test]
    fn unknown_named_param_is_mapping_error() {
        let e = rewrite("WHERE a = :missing", named! { "x" => 1 }).unwrap_err();
        assert_eq!(e.code, ErrorCode::Mapping);
        assert_eq!(e.message, "unknown named parameter ':missing'");
    }

    #[test]
    fn double_colon_is_left_literal() {
        let r = rewrite("SELECT a::int FROM t", Named::new()).unwrap();
        assert_eq!(r.sql, "SELECT a::int FROM t");
        assert!(r.params.is_empty());
    }

    #[test]
    fn skips_single_quoted_string_literal() {
        let r = rewrite(
            "SELECT ':id' AS lit FROM t WHERE id = :id",
            named! { "id" => 7 },
        )
        .unwrap();
        assert_eq!(r.sql, "SELECT ':id' AS lit FROM t WHERE id = ?");
        assert_eq!(r.params, vec![Value::Int(7)]);
    }

    #[test]
    fn handles_doubled_quote_escape_in_string() {
        let r = rewrite("SELECT 'it''s :x' FROM t WHERE a = :a", named! { "a" => 4 }).unwrap();
        assert_eq!(r.sql, "SELECT 'it''s :x' FROM t WHERE a = ?");
        assert_eq!(r.params, vec![Value::Int(4)]);
    }

    #[test]
    fn skips_delimited_identifier() {
        let r = rewrite("SELECT \":id\" FROM t WHERE z = :z", named! { "z" => 3 }).unwrap();
        assert_eq!(r.sql, "SELECT \":id\" FROM t WHERE z = ?");
        assert_eq!(r.params, vec![Value::Int(3)]);
    }

    #[test]
    fn skips_line_comment() {
        let r = rewrite(
            "SELECT 1 -- nope :id here\nFROM t WHERE x = :x",
            named! { "x" => 1 },
        )
        .unwrap();
        assert_eq!(r.sql, "SELECT 1 -- nope :id here\nFROM t WHERE x = ?");
        assert_eq!(r.params, vec![Value::Int(1)]);
    }

    #[test]
    fn skips_block_comment() {
        let r = rewrite(
            "SELECT /* :id and :nope */ x FROM t WHERE y = :y",
            named! { "y" => 2 },
        )
        .unwrap();
        assert_eq!(r.sql, "SELECT /* :id and :nope */ x FROM t WHERE y = ?");
        assert_eq!(r.params, vec![Value::Int(2)]);
    }
}
