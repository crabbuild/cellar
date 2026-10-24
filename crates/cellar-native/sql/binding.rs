// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SQL wire contracts and value shaping used by the Rust SDK and host.

/// One SQL request. The modes are explicit because their contracts differ:
/// a prepared statement is exactly one statement with binds, an exec is many
/// statements with none, and a migration is an exec plus its bookkeeping row
/// inside one transaction. The first adapter multiplexed all three through
/// the general SQL ops, and the differences lived as conventions in JS.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SqlRequest {
    /// `prepare(sql)` with positional binds: one statement, no more.
    Prepared {
        sql: String,
        #[serde(default)]
        params: Vec<serde_json::Value>,
        /// `first()` keeps one row on this side of the boundary, so a first()
        /// over a large table neither clones the table nor trips a cap.
        #[serde(default)]
        first: bool,
    },
    /// `batch(statements)`: prepared statements in one transaction.
    Batch { statements: Vec<SqlPreparedRequest> },
    /// `exec(sql)`: multi-statement, SQLite decides where statements end.
    Exec {
        sql: String,
        /// The Worker binding's `exec()` reports counts only, as upstream
        /// does; `cellar sql execute` asks for the rows too.
        #[serde(default)]
        rows: bool,
    },
    /// One migration file plus its bookkeeping row, in one transaction.
    Migrate {
        name: String,
        sql: String,
        /// The wrangler bookkeeping table, per binding
        /// (`migrations_table`). Joined into SQL as an identifier, so it is
        /// refused unless it is a plain identifier.
        table: String,
    },
    /// Initialize and read the protected migration ledger.
    Migrations { table: String },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlPreparedRequest {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

/// SQL `meta`, snapshotted inside the same execution that produced the rows.
/// `last_row_id` and `changes` are connection-level in SQLite, so reading
/// them in a second call was only correct while nothing else ran on the
/// cell's connection in between -- an invariant no code enforced.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SqlMeta {
    pub duration: f64,
    pub rows_read: u64,
    pub rows_written: u64,
    pub last_row_id: i64,
    pub changes: u64,
    pub changed_db: bool,
    pub size_after: u64,
    pub served_by: String,
    pub served_by_region: String,
    /// One writer, no replicas: every read is served by the primary, and
    /// saying so honestly beats omitting the field.
    pub served_by_primary: bool,
}

/// One result set: positional rows plus the column names to shape them with.
/// Rows stay positional on the wire so `.all()` and `.raw()` share one copy.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SqlRows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SqlPreparedResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub meta: SqlMeta,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum SqlResponse {
    Prepared(SqlPreparedResult),
    Batch(Vec<SqlPreparedResult>),
    Exec {
        count: u64,
        duration: f64,
        results: Vec<SqlRows>,
    },
    Migrate {
        count: u64,
        duration: f64,
    },
    Migrations {
        names: Vec<String>,
    },
}

pub const MIGRATIONS_TABLE: &str = "__sys_sql_migrations";

/// A custom ledger stays in the SQL migrations subnamespace, so callers cannot
/// use the bookkeeping API to read or alter another primitive's system table.
pub fn valid_migrations_table(table: &str) -> bool {
    (table == MIGRATIONS_TABLE
        || table
            .strip_prefix("__sys_sql_migrations_")
            .is_some_and(|suffix| !suffix.is_empty()))
        && table
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// A typed refusal. `family` is the prefix applications match on
/// (`SQL_ERROR`, `SQL_TYPE_ERROR`, `SQL_EXEC_ERROR`), kept apart from the
/// message so the JS client builds the error and its `.cause` without
/// parsing its own wire format.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Error {
    pub family: String,
    pub message: String,
}

impl Error {
    pub fn new(family: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            family: family.into(),
            message: message.into(),
        }
    }

    /// Keep an existing SQL error family when a host transports its message.
    pub fn from_message(message: impl Into<String>) -> Self {
        let message = message.into();
        if let Some((family, bare)) = message.split_once(": ") {
            let suffix = family.strip_prefix("SQL_");
            if let Some(suffix) = suffix.filter(|suffix| {
                suffix.bytes().all(|c| c.is_ascii_uppercase() || c == b'_')
                    && (suffix.ends_with("ERROR") || suffix.ends_with("NOTFOUND"))
            }) {
                return Self::new(format!("SQL_{suffix}"), bare);
            }
        }
        Self::new("SQL_ERROR", message)
    }

    /// The bare cause exposed by the original SQL binding.
    pub fn cause(&self) -> &str {
        if self.family == "SQL_COLUMN_NOTFOUND" {
            "Column not found"
        } else {
            &self.message
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.family, self.message)
    }
}

impl std::error::Error for Error {}

/// A bind value normalized at the public API boundary. A byte array is a
/// BLOB; booleans bind as 1/0 and non-finite numbers bind as SQL NULL.
#[derive(Clone, Debug, PartialEq)]
pub struct BindValue(serde_json::Value);

impl BindValue {
    pub fn null() -> Self {
        Self(serde_json::Value::Null)
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, Error> {
        match value {
            serde_json::Value::Null
            | serde_json::Value::Number(_)
            | serde_json::Value::String(_) => Ok(Self(value)),
            serde_json::Value::Bool(value) => Ok(Self((value as u8).into())),
            serde_json::Value::Array(ref items)
                if items.iter().all(|value| {
                    value
                        .as_f64()
                        .is_some_and(|byte| (0.0..256.0).contains(&byte))
                }) =>
            {
                Ok(Self(value))
            }
            other => Err(Error::new(
                "SQL_TYPE_ERROR",
                format!("Type 'object' not supported for value '{other}'"),
            )),
        }
    }

    /// Typed numeric views bind their element values converted to Uint8,
    /// rather than their underlying byte representation. This retains the
    /// original Uint16, signed and floating view behavior.
    pub fn typed_elements(values: impl IntoIterator<Item = f64>) -> Self {
        Self::from(
            values
                .into_iter()
                .map(|value| {
                    if value.is_finite() {
                        value.trunc().rem_euclid(256.0) as u8
                    } else {
                        0
                    }
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn into_json(self) -> serde_json::Value {
        self.0
    }
}

impl From<f64> for BindValue {
    fn from(value: f64) -> Self {
        Self(
            serde_json::Number::from_f64(value)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
        )
    }
}

impl From<bool> for BindValue {
    fn from(value: bool) -> Self {
        Self((value as u8).into())
    }
}

impl From<i64> for BindValue {
    fn from(value: i64) -> Self {
        Self(value.into())
    }
}

impl From<String> for BindValue {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for BindValue {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<Vec<u8>> for BindValue {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ResultSet {
    pub success: bool,
    pub meta: SqlMeta,
    pub results: Vec<serde_json::Map<String, serde_json::Value>>,
}

pub fn shape(
    columns: &[String],
    row: Vec<serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    columns.iter().cloned().zip(row).collect()
}

impl SqlPreparedResult {
    pub fn into_public(self) -> ResultSet {
        ResultSet {
            success: true,
            meta: self.meta,
            results: self
                .rows
                .into_iter()
                .map(|row| shape(&self.columns, row))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExecResult {
    pub count: u64,
    pub duration: f64,
}

pub const PRIMARY_BOOKMARK: &str = "cellar:primary";
