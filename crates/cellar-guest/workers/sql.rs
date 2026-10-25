// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::{values::field, Limits};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::{
    storage::{SqlCursor, SqlValue},
    Context,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Mutex};

#[derive(Default)]
pub(super) struct Cursors(Mutex<Resources>);

#[derive(Default)]
struct Resources {
    next: u64,
    cursors: HashMap<u64, SqlCursor>,
}

impl Resources {
    fn buffered_bytes(&self) -> usize {
        self.cursors.values().fold(0_usize, |total, cursor| {
            total.saturating_add(cursor.buffered_bytes())
        })
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Parameter {
    Null,
    Number(String),
    Text(String),
    Blob(String),
}

fn parameter(value: Parameter, limit: usize) -> Result<SqlValue> {
    Ok(match value {
        Parameter::Null => SqlValue::Null,
        Parameter::Number(value) => {
            let value: f64 = value.parse()?;
            // Cloudflare SQL exposes JS numbers, including the documented loss
            // of precision for SQLite int64 columns. Carry numbers as text
            // across JSON so the generic bridge never rounds them implicitly.
            if value.is_finite() && value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_991.0 {
                SqlValue::Integer(value as i64)
            } else {
                SqlValue::Real(value)
            }
        }
        Parameter::Text(value) => {
            ensure!(value.len() <= limit, "SQL text exceeds limit");
            SqlValue::Text(value)
        }
        Parameter::Blob(value) => {
            let bytes = STANDARD.decode(value)?;
            ensure!(bytes.len() <= limit, "SQL blob exceeds limit");
            SqlValue::Blob(bytes)
        }
    })
}

fn value(value: SqlValue) -> Value {
    match value {
        SqlValue::Null => json!({"type":"null"}),
        SqlValue::Integer(value) => json!({"type":"number","value":value.to_string()}),
        SqlValue::Real(value) => {
            json!({"type":"number","value": if value == f64::INFINITY { "Infinity".to_owned() } else if value == f64::NEG_INFINITY { "-Infinity".to_owned() } else { value.to_string() }})
        }
        SqlValue::Text(value) => json!({"type":"text","value":value}),
        SqlValue::Blob(value) => json!({"type":"blob","value":STANDARD.encode(value)}),
    }
}

impl Cursors {
    pub(super) fn execute(
        &self,
        context: &Context,
        name: &str,
        input: Value,
        options: &Limits,
    ) -> Result<Value> {
        let storage = context.storage()?;
        if name == "workers.sql.size" {
            return Ok(json!(storage.sql_database_size()?));
        }
        let mut state = self.0.lock().unwrap();
        if name == "workers.sql.exec" {
            let (sql, parameters) = (|| -> Result<_> {
                let sql = field(&input, "sql")?.to_owned();
                ensure!(sql.len() <= options.max_body_bytes, "SQL exceeds limit");
                let parameters: Vec<Parameter> = serde_json::from_value(input["params"].clone())?;
                ensure!(
                    parameters.len() <= options.max_batch_operations,
                    "SQL parameter limit"
                );
                ensure!(
                    state.cursors.len() < options.max_batch_operations,
                    "SQL cursor limit"
                );
                ensure!(
                    state.next < 9_007_199_254_740_991,
                    "SQL cursor identity exhausted"
                );
                let parameters = parameters
                    .into_iter()
                    .map(|p| parameter(p, options.max_body_bytes))
                    .collect::<Result<Vec<_>>>()?;
                Ok((sql, parameters))
            })()
            .context(crate::failure::Validation)?;
            let cursor = storage.sql_cursor(&sql, &parameters)?;
            // Preparing/stepping can already execute writes. An owned-buffer
            // refusal here must stay an unknown outcome, even for RETURNING.
            ensure!(
                state
                    .buffered_bytes()
                    .saturating_add(cursor.buffered_bytes())
                    <= options.max_body_bytes,
                "SQL cursor buffers exceed limit"
            );
            state.next += 1;
            let id = state.next;
            let result = json!({"id":id,"columns":cursor.column_names(),"rows_read":cursor.rows_read(),"rows_written":cursor.rows_written()});
            state.cursors.insert(id, cursor);
            return Ok(result);
        }
        let id = input["id"]
            .as_u64()
            .context("invalid SQL cursor")
            .context(crate::failure::Validation)?;
        if name == "workers.sql.close" {
            let mut cursor = state
                .cursors
                .remove(&id)
                .context("unknown SQL cursor")
                .context(crate::failure::Validation)?;
            cursor.close()?;
            return Ok(json!(null));
        }
        let cursor = state
            .cursors
            .get_mut(&id)
            .context("unknown SQL cursor")
            .context(crate::failure::Validation)?;
        let row = cursor.next_raw()?;
        let done = row.is_none();
        let result = json!({"row":row.map(|row| row.into_iter().map(value).collect::<Vec<_>>()),
            "rows_read":cursor.rows_read(),"rows_written":cursor.rows_written(),"done":done});
        if done {
            state.cursors.remove(&id);
        }
        if state.buffered_bytes() > options.max_body_bytes {
            state.cursors.remove(&id);
            anyhow::bail!("SQL cursor buffers exceed limit");
        }
        // Result refusal can follow writes. Let the shared failure boundary keep
        // that outcome unknown; never attach the pre-effect validation marker.
        if serde_json::to_vec(&result)?.len() > options.max_body_bytes {
            state.cursors.remove(&id);
            anyhow::bail!("SQL row exceeds limit");
        }
        Ok(result)
    }
}
