//! Bounded execution observations in the scheduling cell's own output gate.
use super::*;
use crate::application::storage::SqlValue;
use anyhow::ensure;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const RETAINED: i64 = 256;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Configuration {
    pub prefix: String,
    pub deployment_version: String,
    pub schedule_revision: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Query {
    pub before: Option<u64>,
    #[serde(default = "page_size")]
    pub limit: u32,
    pub expression: Option<String>,
}

fn page_size() -> u32 {
    50
}

impl Query {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (1..=100).contains(&self.limit),
            "execution page limit must be 1–100"
        );
        ensure!(
            self.before.is_none_or(|v| v <= i64::MAX as u64),
            "invalid execution cursor"
        );
        ensure!(
            self.expression.as_ref().is_none_or(|v| v.len() <= 256),
            "expression is too long"
        );
        Ok(())
    }
}

pub(super) fn begin(storage: &Storage, expression: &str, mut document: Value) -> Result<i64> {
    storage.system_sql_schema("CREATE TABLE IF NOT EXISTS __sys_cron_executions (id INTEGER PRIMARY KEY AUTOINCREMENT, expression TEXT NOT NULL, document TEXT NOT NULL);")?;
    storage.transaction_sync(|_| {
        let row = storage
            .system_sql(
                "INSERT INTO __sys_cron_executions(expression,document) VALUES (?,?) RETURNING id",
                &[expression.to_string().into(), document.to_string().into()],
            )?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("cron execution was not inserted"))?;
        let Some(SqlValue::Integer(id)) = row.get("id") else {
            return Err(anyhow!("invalid cron execution identity"));
        };
        document["id"] = json!(id);
        storage.system_sql(
            "UPDATE __sys_cron_executions SET document=? WHERE id=?",
            &[document.to_string().into(), (*id).into()],
        )?;
        storage.system_sql(
            "DELETE FROM __sys_cron_executions WHERE id NOT IN (SELECT id FROM __sys_cron_executions ORDER BY id DESC LIMIT ?)",
            &[RETAINED.into()],
        )?;
        Ok(*id)
    })
}

pub(super) fn finish(storage: &Storage, id: i64, mut document: Value) -> Result<()> {
    document["id"] = json!(id);
    storage.system_sql(
        "UPDATE __sys_cron_executions SET document=? WHERE id=?",
        &[document.to_string().into(), id.into()],
    )?;
    Ok(())
}

pub(super) fn page(storage: &Storage, query: Query) -> Result<Value> {
    query.validate()?;
    let exists = !storage
        .system_sql(
            "SELECT 1 FROM sqlite_schema WHERE name='__sys_cron_executions'",
            &[],
        )?
        .is_empty();
    let observed = now();
    if !exists {
        return Ok(
            json!({"items":[],"next_cursor":null,"observed_at":observed,"retention_limit":RETAINED,"coverage":"retained","active_alarm":storage.get_alarm()?}),
        );
    }
    let before = query
        .before
        .map(|v| SqlValue::Integer(v as i64))
        .unwrap_or(SqlValue::Null);
    let expression = query
        .expression
        .map(SqlValue::Text)
        .unwrap_or(SqlValue::Null);
    let rows = storage.system_sql(
        "SELECT id,document FROM __sys_cron_executions WHERE (? IS NULL OR id<?) AND (? IS NULL OR expression=?) ORDER BY id DESC LIMIT ?",
        &[before.clone(), before, expression.clone(), expression, (i64::from(query.limit) + 1).into()],
    )?;
    let more = rows.len() > query.limit as usize;
    let mut items = Vec::new();
    for row in rows.into_iter().take(query.limit as usize) {
        let Some(SqlValue::Text(document)) = row.get("document") else {
            return Err(anyhow!("invalid cron execution record"));
        };
        items.push(serde_json::from_str::<Value>(document)?);
    }
    let next = if more {
        items
            .last()
            .and_then(|item| item["id"].as_i64())
            .map(|id| id.to_string())
    } else {
        None
    };
    Ok(
        json!({"items":items,"next_cursor":next,"observed_at":observed,"retention_limit":RETAINED,"coverage":"retained","active_alarm":storage.get_alarm()?}),
    )
}

pub(super) fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string().chars().take(1024).collect()
}

pub(super) fn output_summary(output: &Output) -> Value {
    match output {
        Output::Done => json!({"kind":"done"}),
        Output::Http(response) => {
            json!({"kind":"http","status":response.status,"buffered_bytes":response.body.len(),"streaming":response.stream.is_some()})
        }
        Output::Bytes(bytes) => json!({"kind":"bytes","bytes":bytes.len()}),
        Output::Rpc(_) => json!({"kind":"rpc"}),
        Output::Queue(_) => json!({"kind":"queue"}),
        Output::WebSocket(_) => json!({"kind":"websocket"}),
    }
}
