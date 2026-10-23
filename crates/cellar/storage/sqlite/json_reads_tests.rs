use super::*;
use rusqlite::types::ValueRef;
use serde_json::json;

fn owned_decode(value: ValueRef<'_>) -> anyhow::Result<serde_json::Value> {
    match stored_value(value) {
        StoredValue::Json(text) => Ok(serde_json::from_str(&text)?),
        StoredValue::Structured(_) => {
            anyhow::bail!("stored value is a structured clone; use get_clone")
        }
    }
}

#[test]
fn borrowed_json_preserves_persisted_sqlite_representations() {
    let text = serde_json::to_vec(&json!({"text":"é\"\n☃".repeat(32_768), "n":u64::MAX})).unwrap();
    for value in [
        ValueRef::Null,
        ValueRef::Integer(i64::MIN),
        ValueRef::Integer(i64::MAX),
        ValueRef::Real(-0.0),
        ValueRef::Real(f64::MIN_POSITIVE),
        ValueRef::Real(f64::MAX),
        ValueRef::Real(f64::INFINITY),
        ValueRef::Real(f64::NEG_INFINITY),
        ValueRef::Real(f64::NAN),
        ValueRef::Text(&text),
        ValueRef::Text(b"  [1, null, true, -2.1e-12] \n"),
        ValueRef::Text(b"\"invalid UTF-8: \xff\""),
        ValueRef::Text(b"null"),
        ValueRef::Text(b"[invalid"),
        ValueRef::Text(b"\xff"),
        ValueRef::Blob(b"null"),
    ] {
        match (owned_decode(value), stored_json(value)) {
            (Ok(expected), Ok(actual)) => assert_eq!(actual, expected),
            (Err(expected), Err(actual)) => assert_eq!(actual.to_string(), expected.to_string()),
            results => panic!("JSON decoding contract changed: {results:?}"),
        }
    }
}

#[test]
fn row_json_owns_data_after_the_statement_and_connection_close() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    connection
        .execute_batch("CREATE TABLE items(value); INSERT INTO items VALUES ('{\"n\":null}');")?;
    let value = connection.query_row("SELECT value FROM items", [], |row| {
        Ok(stored_json(row.get_ref(0)?))
    })??;
    drop(connection);
    assert_eq!(value, json!({"n":null}));
    Ok(())
}
