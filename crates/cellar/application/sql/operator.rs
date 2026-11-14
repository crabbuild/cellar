// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The authenticated operator protocol shared by Fleet and group SQL.

use super::*;

// This contract is deliberately absent from ordinary cell-binding discovery.
// Only the authenticated node operator adapter selects it for ingress.
pub(crate) const METHOD: &str = "__sqlOperator";

impl DatabaseCell {
    pub(super) fn operator(&self, body: &Value) -> anyhow::Result<Value> {
        if body.is_null() {
            return Err(Error::new("SQL_ERROR", "invalid request body").into());
        }
        if let Some(console) = body.get("console") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct ConsoleQuery {
                sql: String,
                readonly: bool,
            }
            let query: ConsoleQuery = serde_json::from_value(console.clone())?;
            return Ok(serde_json::to_value(
                self.0.sql_console(&query.sql, query.readonly)?,
            )?);
        }
        if let Some(history) = body.get("console_migrations") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct History {
                table: String,
                after: u64,
            }
            let query: History = serde_json::from_value(history.clone())?;
            return self.0.sql_console_migrations(&query.table, query.after);
        }
        if let Some(migration) = body.get("migrate") {
            return self.migrate(
                js_string(migration.get("name")),
                js_string(migration.get("sql")),
                if truthy(migration.get("table")) {
                    js_string(migration.get("table"))
                } else {
                    cellar_native::sql::MIGRATIONS_TABLE.into()
                },
            );
        }
        if let Some(history) = body.get("migrations") {
            return self.migrations(
                history
                    .get("table")
                    .map(|table| js_string(Some(table)))
                    .unwrap_or_else(|| cellar_native::sql::MIGRATIONS_TABLE.into()),
            );
        }
        if let Some(exec) = body.get("exec") {
            return self.exec(js_string(exec.get("sql")), truthy(exec.get("rows")));
        }
        self.query(
            body.get("statements")
                .filter(|value| truthy(Some(value)))
                .unwrap_or(&json!([])),
        )
    }
}

pub(crate) fn response(result: anyhow::Result<Value>) -> HttpResponse {
    let (status, body) = match result {
        Ok(value) => (200, json!({ "result": value })),
        Err(error) => (400, json!({ "error": error.to_string() })),
    };
    HttpResponse {
        status,
        body: serde_json::to_vec(&body).expect("SQL result is JSON"),
        stream: None,
        headers: vec![("content-type".into(), "application/json".into())],
        websocket: None,
        write_position: None,
        observed_position: None,
    }
}
