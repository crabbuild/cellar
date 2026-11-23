// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::error;
use crate::application::storage::{SqlRow, SqlValue};
use anyhow::Result;

pub(super) fn text(row: &SqlRow, key: &str) -> Result<String> {
    match row.get(key) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        _ => Err(error(format!("invalid stored {key}"))),
    }
}

pub(super) fn number(row: &SqlRow, key: &str) -> Result<i64> {
    match row.get(key) {
        Some(SqlValue::Integer(value)) => Ok(*value),
        _ => Err(error(format!("invalid stored {key}"))),
    }
}

pub(super) fn optional_number(row: &SqlRow, key: &str) -> Result<Option<i64>> {
    match row.get(key) {
        Some(SqlValue::Null) => Ok(None),
        _ => number(row, key).map(Some),
    }
}

pub(super) fn optional_text(row: &SqlRow, key: &str) -> Result<Option<String>> {
    match row.get(key) {
        Some(SqlValue::Null) => Ok(None),
        _ => text(row, key).map(Some),
    }
}

pub(super) fn blob(row: &SqlRow, key: &str) -> Result<Vec<u8>> {
    match row.get(key) {
        Some(SqlValue::Blob(value)) => Ok(value.clone()),
        _ => Err(error(format!("invalid stored {key}"))),
    }
}

pub(super) fn sql_text(value: Option<String>) -> SqlValue {
    value.map(SqlValue::Text).unwrap_or(SqlValue::Null)
}
