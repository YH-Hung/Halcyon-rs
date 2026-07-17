//! `#[derive(FromRow)]` tests (feature `derive`), mirroring the C++
//! `test_reflect.cpp`: by-position struct mapping, NULL → `Option`, and the
//! arity-mismatch `Mapping` error. Kept in a dedicated module so the whole thing
//! compiles out when the `derive` feature is off.

use std::sync::Arc;

use crate::driver::ConnectionParams;
use crate::testing::{MockCliDriver, ScriptedRows};
use crate::value::Value;
use crate::{Connection, ErrorCode, FromRow};

#[derive(FromRow, Debug, PartialEq)]
struct User {
    id: i32,
    name: String,
    age: Option<i32>,
}

fn connect(mock: &Arc<MockCliDriver>) -> Connection {
    Connection::open(
        mock.clone(),
        &ConnectionParams {
            connection_string: "x".into(),
            isolation: None,
        },
    )
    .unwrap()
}

#[test]
fn query_as_maps_rows_to_structs_by_position() {
    let mock = Arc::new(MockCliDriver::new());
    mock.push_result_set(ScriptedRows::new(
        ["id", "name", "age"],
        vec![
            vec![Value::Int(1), Value::Text("ada".into()), Value::Int(36)],
            vec![Value::Int(2), Value::Text("bob".into()), Value::Null],
        ],
    ));
    let conn = connect(&mock);

    let users: Vec<User> = conn
        .query_as("SELECT id, name, age FROM users", ())
        .unwrap();
    assert_eq!(
        users,
        vec![
            User {
                id: 1,
                name: "ada".into(),
                age: Some(36)
            },
            User {
                id: 2,
                name: "bob".into(),
                age: None
            }, // NULL -> None
        ]
    );
}

#[test]
fn arity_mismatch_is_mapping_error() {
    let mock = Arc::new(MockCliDriver::new());
    // 2 columns against a 3-field struct.
    mock.push_result_set(ScriptedRows::new(
        ["id", "name"],
        vec![vec![Value::Int(1), Value::Text("ada".into())]],
    ));
    let conn = connect(&mock);

    let err = conn
        .query_as::<User>("SELECT id, name FROM users", ())
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Mapping);
}

#[test]
fn derived_struct_is_usable_through_row_get_and_query_map() {
    let mock = Arc::new(MockCliDriver::new());
    mock.push_result_set(ScriptedRows::new(
        ["id", "name", "age"],
        vec![vec![Value::Int(7), Value::Text("cy".into()), Value::Null]],
    ));
    let conn = connect(&mock);

    // The derive also plugs into the streaming APIs, not just query_as.
    let got: Vec<User> = conn
        .query("SELECT id, name, age FROM users", ())
        .unwrap()
        .query_map(|row| row.get::<User>())
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        got,
        vec![User {
            id: 7,
            name: "cy".into(),
            age: None
        }]
    );
}

#[test]
fn tuple_struct_maps_by_position() {
    #[derive(FromRow, Debug, PartialEq)]
    struct Pair(i64, String);

    let mock = Arc::new(MockCliDriver::new());
    mock.push_result_set(ScriptedRows::new(
        ["a", "b"],
        vec![vec![Value::Int(5), Value::Text("hi".into())]],
    ));
    let conn = connect(&mock);

    let rows: Vec<Pair> = conn.query_as("SELECT a, b FROM t", ()).unwrap();
    assert_eq!(rows, vec![Pair(5, "hi".into())]);
}
