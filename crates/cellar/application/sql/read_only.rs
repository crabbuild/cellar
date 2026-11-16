// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit read requests, distinct from writable prepared statements.

use super::*;
use cellar_native::sql::SqlReadResult;

pub(crate) const METHOD: &str = "__sqlRead";
pub(crate) const OPERATION: &str = "__cellar_sql_read";

pub(crate) fn read_operation(method: &str) -> Option<&'static str> {
    (method == METHOD).then_some(OPERATION)
}

/// One read-only SQL query. It cannot enter a writable statement batch.
/// Raft databases use a fresh quorum read of an already enrolled database;
/// other backends retain their ordinary owner routing. SQLite refuses writes,
/// transaction control and connection-changing PRAGMAs before execution.
#[derive(Clone, Debug)]
pub struct ReadOnlyStatement(PreparedStatement);

impl ReadOnlyStatement {
    pub(super) fn new(database: Database, sql: String) -> Self {
        Self(database.prepare(sql))
    }

    pub fn bind(&self, values: impl IntoIterator<Item = BindValue>) -> Self {
        Self(self.0.bind(values))
    }

    pub fn bind_json(&self, values: impl IntoIterator<Item = Value>) -> Result<Self> {
        Ok(Self(self.0.bind_json(values)?))
    }

    async fn query(&self, context: &Context, first: bool) -> Result<SqlReadResult> {
        let request = &self.0.request;
        let mut results: Vec<SqlReadResult> = self
            .0
            .target
            .database
            .call(
                context,
                METHOD,
                json!([[{ "sql": request.sql, "params": request.params, "first": first }]]),
            )
            .await?;
        if results.len() != 1 {
            return Err(Error::new("SQL_ERROR", "invalid read-only result count"));
        }
        Ok(results.remove(0))
    }

    pub async fn all(&self, context: &Context) -> Result<ReadResultSet> {
        Ok(self.query(context, false).await?.into_public())
    }

    /// None means no row; Some(Null) means a selected SQL NULL.
    pub async fn first(&self, context: &Context, column: Option<&str>) -> Result<Option<Value>> {
        let result = self.query(context, true).await?;
        let Some(row) = result.rows.into_iter().next() else {
            return Ok(None);
        };
        let mut row = shape(&result.columns, row);
        match column {
            None => Ok(Some(Value::Object(row))),
            Some(column) => row.remove(column).map(Some).ok_or_else(|| {
                Error::new(
                    "SQL_COLUMN_NOTFOUND",
                    format!("Column not found ({column})"),
                )
            }),
        }
    }

    pub async fn raw(&self, context: &Context, column_names: bool) -> Result<Vec<Vec<Value>>> {
        let mut result = self.query(context, false).await?;
        if column_names {
            result
                .rows
                .insert(0, result.columns.into_iter().map(Value::String).collect());
        }
        Ok(result.rows)
    }
}

pub(crate) fn read_query(storage: Storage, args: RpcData) -> anyhow::Result<RpcData> {
    DatabaseCell(storage).rpc(METHOD, args)
}

pub(super) fn statements(storage: &Storage, statements: &Value) -> anyhow::Result<Value> {
    let statements = statements
        .as_array()
        .ok_or_else(|| Error::new("SQL_ERROR", "statements must be an array"))?;
    let [statement] = statements.as_slice() else {
        return Err(Error::new("SQL_ERROR", "read-only SQL requires one statement").into());
    };
    let result = storage.sql_read_request(
        js_string(statement.get("sql")),
        params(statement.get("params"))?,
        truthy(statement.get("first")),
    )?;
    Ok(serde_json::to_value(vec![result])?)
}
