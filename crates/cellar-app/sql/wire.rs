//! Normalize the retained SQL JSON request protocol without executing SQL.
use cellar_native::sql::Error;
use serde_json::Value;

pub fn params(value: Option<&Value>) -> anyhow::Result<Vec<Value>> {
    match value.filter(|value| truthy(Some(value))) {
        None => Ok(Vec::new()),
        Some(Value::Array(values)) => Ok(values.clone()),
        _ => Err(Error::new("SQL_ERROR", "params must be an array").into()),
    }
}

pub fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
        Some(Value::String(value)) => !value.is_empty(),
        _ => true,
    }
}

pub fn js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Object(_)) => "[object Object]".into(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(Some(value))
                }
            })
            .collect::<Vec<_>>()
            .join(","),
    }
}
