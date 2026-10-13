//! Rust SQL bindings and the runtime-supplied database cell. Bindings retain
//! database identity across events and take the executing context per call.
//! Owner lookup, admission and durability remain host responsibilities.

use super::{
    decode_rpc, decode_rpc_result, encode_rpc, Cell, Context, HandlerFuture, Invocation, Output,
    Storage,
};
use crate::engine_api::{HttpResponse, RpcData};
use cellar_native::sql::{
    shape, SqlPreparedRequest, SqlPreparedResult, SqlRequest, SqlResponse, PRIMARY_BOOKMARK,
};
pub use cellar_native::sql::{BindValue, Error, ExecResult, ResultSet};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

type Result<T> = std::result::Result<T, Error>;

fn error(error: impl std::fmt::Display) -> Error {
    Error::from_message(error.to_string())
}

#[derive(Clone, Debug)]
pub struct Database {
    identity: Arc<str>,
}

impl Database {
    pub(crate) fn new(identity: String) -> Self {
        Self {
            identity: identity.into(),
        }
    }

    pub fn prepare(&self, sql: impl Into<String>) -> PreparedStatement {
        Target {
            database: self.clone(),
            bookmark: None,
        }
        .prepare(sql.into())
    }

    pub async fn exec(&self, context: &Context, sql: impl Into<String>) -> Result<ExecResult> {
        self.call(context, "__sqlExec", json!([sql.into()])).await
    }

    pub async fn batch(
        &self,
        context: &Context,
        statements: &[PreparedStatement],
    ) -> Result<Vec<ResultSet>> {
        Target {
            database: self.clone(),
            bookmark: None,
        }
        .batch(context, statements)
        .await
    }

    pub fn with_session(&self, constraint_or_bookmark: Option<&str>) -> Result<Session> {
        let value = constraint_or_bookmark
            .unwrap_or("first-unconstrained")
            .trim();
        if value.is_empty() {
            return Err(Error::new(
                "SQL_SESSION_ERROR",
                "invalid bookmark or constraint",
            ));
        }
        let bookmark = match value {
            "first-primary" | "first-unconstrained" => None,
            value => Some(value.to_string()),
        };
        Ok(Session(Target {
            database: self.clone(),
            bookmark: Some(Arc::new(Mutex::new(bookmark))),
        }))
    }

    /// The retained operator migration protocol: SQL and bookkeeping commit
    /// atomically in the database cell.
    pub async fn migrate(
        &self,
        context: &Context,
        name: &str,
        sql: &str,
        table: &str,
    ) -> Result<ExecResult> {
        self.call(context, "__sqlMigrate", json!([name, sql, table]))
            .await
    }

    /// Read applied migration names through the protected bookkeeping API.
    pub async fn migrations(&self, context: &Context, table: &str) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct History {
            names: Vec<String>,
        }
        let history: History = self
            .call(context, "__sqlMigrations", json!([table]))
            .await?;
        Ok(history.names)
    }

    pub async fn dump(&self) -> Result<Vec<u8>> {
        Err(Error::new(
            "SQL_ERROR",
            "dump() is not implemented in cellar; the database is a SQLite file in your own bucket",
        ))
    }

    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        context: &Context,
        method: &str,
        args: Value,
    ) -> Result<T> {
        let args = cellar_native::clone::StructuredValue::from_json(args).map_err(error)?;
        let response = context
            .rpc_cell(
                crate::engine_api::sql_cell_scope(&self.identity),
                Some(self.identity.to_string()),
                method.to_string(),
                encode_rpc(&args).map_err(error)?,
            )
            .await
            .map_err(error)?;
        serde_json::from_value(
            decode_rpc_result(&response)
                .and_then(|value| value.to_json())
                .map_err(error)?,
        )
        .map_err(error)
    }
}

#[derive(Clone, Debug)]
struct Target {
    database: Database,
    bookmark: Option<Arc<Mutex<Option<String>>>>,
}

impl Target {
    fn advance(&self) {
        if let Some(bookmark) = &self.bookmark {
            *bookmark.lock().unwrap() = Some(PRIMARY_BOOKMARK.into());
        }
    }

    fn prepare(&self, sql: String) -> PreparedStatement {
        PreparedStatement {
            target: self.clone(),
            request: SqlPreparedRequest {
                sql,
                params: Vec::new(),
            },
        }
    }

    async fn query(
        &self,
        context: &Context,
        request: &SqlPreparedRequest,
        first: bool,
    ) -> Result<SqlPreparedResult> {
        let mut results: Vec<SqlPreparedResult> = self
            .database
            .call(
                context,
                "__sqlQuery",
                json!([[{ "sql": request.sql, "params": request.params, "first": first }]]),
            )
            .await?;
        if results.len() != 1 {
            return Err(Error::new(
                "SQL_ERROR",
                "database returned an invalid result count",
            ));
        }
        self.advance();
        Ok(results.remove(0))
    }

    async fn batch(
        &self,
        context: &Context,
        statements: &[PreparedStatement],
    ) -> Result<Vec<ResultSet>> {
        let requests: Vec<_> = statements
            .iter()
            .map(|statement| &statement.request)
            .collect();
        let results: Vec<SqlPreparedResult> = self
            .database
            .call(context, "__sqlBatch", json!([requests]))
            .await?;
        self.advance();
        Ok(results
            .into_iter()
            .map(SqlPreparedResult::into_public)
            .collect())
    }
}

#[derive(Clone, Debug)]
pub struct Session(Target);

impl Session {
    pub fn prepare(&self, sql: impl Into<String>) -> PreparedStatement {
        self.0.prepare(sql.into())
    }

    pub async fn batch(
        &self,
        context: &Context,
        statements: &[PreparedStatement],
    ) -> Result<Vec<ResultSet>> {
        self.0.batch(context, statements).await
    }

    pub fn get_bookmark(&self) -> Option<String> {
        self.0
            .bookmark
            .as_ref()
            .expect("session has a bookmark")
            .lock()
            .unwrap()
            .clone()
    }
}

#[derive(Clone, Debug)]
pub struct PreparedStatement {
    target: Target,
    request: SqlPreparedRequest,
}

impl PreparedStatement {
    /// Binding returns a new statement; the original can be reused unchanged.
    pub fn bind(&self, values: impl IntoIterator<Item = BindValue>) -> Self {
        Self {
            target: self.target.clone(),
            request: SqlPreparedRequest {
                sql: self.request.sql.clone(),
                params: values.into_iter().map(BindValue::into_json).collect(),
            },
        }
    }

    pub fn bind_json(&self, values: impl IntoIterator<Item = Value>) -> Result<Self> {
        Ok(self.bind(
            values
                .into_iter()
                .map(BindValue::from_json)
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    pub async fn all(&self, context: &Context) -> Result<ResultSet> {
        Ok(self
            .target
            .query(context, &self.request, false)
            .await?
            .into_public())
    }

    pub async fn run(&self, context: &Context) -> Result<ResultSet> {
        self.all(context).await
    }

    /// None is an empty result; Some(Null) is a selected SQL NULL. With no
    /// column name the first row is returned as an object.
    pub async fn first(&self, context: &Context, column: Option<&str>) -> Result<Option<Value>> {
        let result = self.target.query(context, &self.request, true).await?;
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
        let mut result = self.target.query(context, &self.request, false).await?;
        if column_names {
            result
                .rows
                .insert(0, result.columns.into_iter().map(Value::String).collect());
        }
        Ok(result.rows)
    }
}

struct DatabaseCell(Storage);

pub(crate) fn create_cell(context: &Context) -> anyhow::Result<Arc<dyn Cell>> {
    Ok(Arc::new(DatabaseCell(context.storage()?)))
}

impl DatabaseCell {
    fn query(&self, statements: &Value) -> anyhow::Result<Value> {
        let statements = statements
            .as_array()
            .ok_or_else(|| Error::new("SQL_ERROR", "statements must be an array"))?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            results.push(self.0.sql_request(&SqlRequest::Prepared {
                sql: js_string(statement.get("sql")),
                params: params(statement.get("params"))?,
                first: truthy(statement.get("first")),
            })?);
        }
        Ok(serde_json::to_value(results)?)
    }

    fn batch(&self, statements: &Value) -> anyhow::Result<Value> {
        let statements = statements
            .as_array()
            .ok_or_else(|| Error::new("SQL_ERROR", "statements must be an array"))?;
        let statements = statements
            .iter()
            .map(|statement| {
                Ok(SqlPreparedRequest {
                    sql: js_string(statement.get("sql")),
                    params: params(statement.get("params"))?,
                })
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(serde_json::to_value(
            self.0.sql_request(&SqlRequest::Batch { statements })?,
        )?)
    }

    fn exec(&self, sql: String, rows: bool) -> anyhow::Result<Value> {
        Ok(serde_json::to_value(
            self.0.sql_request(&SqlRequest::Exec { sql, rows })?,
        )?)
    }

    fn migrate(&self, name: String, sql: String, table: String) -> anyhow::Result<Value> {
        Ok(serde_json::to_value(
            self.0
                .sql_request(&SqlRequest::Migrate { name, sql, table })?,
        )?)
    }

    fn migrations(&self, table: String) -> anyhow::Result<Value> {
        Ok(serde_json::to_value(
            self.0.sql_request(&SqlRequest::Migrations { table })?,
        )?)
    }

    fn rpc(&self, method: &str, args: RpcData) -> anyhow::Result<RpcData> {
        let input = decode_rpc(&args)?.to_json()?;
        let input = input
            .as_array()
            .ok_or_else(|| Error::new("SQL_ERROR", "RPC arguments must be an array"))?;
        let result = match method {
            "__sqlQuery" => self.query(input.first().unwrap_or(&Value::Null))?,
            "__sqlBatch" => self.batch(input.first().unwrap_or(&Value::Null))?,
            "__sqlExec" => {
                let SqlResponse::Exec {
                    count, duration, ..
                } = self.0.sql_request(&SqlRequest::Exec {
                    sql: js_string(input.first()),
                    rows: false,
                })?
                else {
                    unreachable!("exec response")
                };
                json!({ "count": count, "duration": duration })
            }
            "__sqlMigrations" => self.migrations(js_string(input.first()))?,
            "__sqlMigrate" => self.migrate(
                js_string(input.first()),
                js_string(input.get(1)),
                js_string(input.get(2)),
            )?,
            _ => anyhow::bail!("SQL cell has no RPC method {method}"),
        };
        match args {
            RpcData::Json(_) => Ok(RpcData::Json(serde_json::to_string(&result)?)),
            RpcData::Structured(_) => {
                encode_rpc(&cellar_native::clone::StructuredValue::from_json(result)?)
            }
        }
    }

    async fn fetch(&self, context: &Context, body: crate::engine_api::RequestBody) -> HttpResponse {
        let body = async {
            let mut stream = context.take_body(body)?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.try_next().await.map_err(anyhow::Error::msg)? {
                bytes.extend(chunk);
            }
            serde_json::from_slice::<Value>(&bytes).map_err(anyhow::Error::from)
        }
        .await;
        let result = match body {
            Err(_) => Err(anyhow::Error::from(Error::new(
                "SQL_ERROR",
                "invalid request body",
            ))),
            Ok(Value::Null) => Err(anyhow::Error::from(Error::new(
                "SQL_ERROR",
                "invalid request body",
            ))),
            Ok(body) => {
                if let Some(migration) = body.get("migrate") {
                    self.migrate(
                        js_string(migration.get("name")),
                        js_string(migration.get("sql")),
                        if truthy(migration.get("table")) {
                            js_string(migration.get("table"))
                        } else {
                            cellar_native::sql::MIGRATIONS_TABLE.into()
                        },
                    )
                } else if let Some(history) = body.get("migrations") {
                    self.migrations(
                        history
                            .get("table")
                            .map(|table| js_string(Some(table)))
                            .unwrap_or_else(|| cellar_native::sql::MIGRATIONS_TABLE.into()),
                    )
                } else if let Some(exec) = body.get("exec") {
                    self.exec(js_string(exec.get("sql")), truthy(exec.get("rows")))
                } else {
                    self.query(
                        body.get("statements")
                            .filter(|value| truthy(Some(value)))
                            .unwrap_or(&json!([])),
                    )
                }
            }
        };
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
}

impl Cell for DatabaseCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            match invocation {
                Invocation::Fetch(request) => {
                    Ok(Output::Http(self.fetch(&context, request.body).await))
                }
                Invocation::CellRpc { method, args } => {
                    let structured = matches!(args, RpcData::Structured(_));
                    match self.rpc(&method, args) {
                        Ok(response) => Ok(Output::Rpc(response)),
                        Err(error) if structured => {
                            let cause = error.downcast_ref::<Error>().map(Error::cause);
                            Ok(Output::Rpc(RpcData::Structured(
                                cellar_native::rpc::encode_error(
                                    &error.to_string(),
                                    "Error",
                                    cause,
                                )?
                                .into(),
                            )))
                        }
                        Err(error) => Err(error),
                    }
                }
                _ => anyhow::bail!("SQL cell received an unsupported event"),
            }
        })
    }
}

fn params(value: Option<&Value>) -> anyhow::Result<Vec<Value>> {
    match value.filter(|value| truthy(Some(value))) {
        None => Ok(Vec::new()),
        Some(Value::Array(values)) => Ok(values.clone()),
        _ => Err(Error::new("SQL_ERROR", "params must be an array").into()),
    }
}

fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
        Some(Value::String(value)) => !value.is_empty(),
        _ => true,
    }
}

fn js_string(value: Option<&Value>) -> String {
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
