// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded operator SQL. Runs only through an activation-bound Storage handle.
use super::*;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

thread_local! {
    static ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn row_limit() -> usize {
    let configured = crate::application::limits::current().sql.max_rows;
    if ACTIVE.with(std::cell::Cell::get) {
        configured.min(1000)
    } else {
        configured
    }
}

pub(super) fn byte_limit() -> usize {
    let configured = crate::application::limits::current().sql.max_result_bytes;
    if ACTIVE.with(std::cell::Cell::get) {
        configured.min(1024 * 1024)
    } else {
        configured
    }
}

fn authorize_read(context: AuthContext<'_>) -> Authorization {
    let read = match context.action {
        AuthAction::Read { .. }
        | AuthAction::Select
        | AuthAction::Function { .. }
        | AuthAction::Recursive => true,
        AuthAction::Pragma { pragma_name, .. } => matches!(
            pragma_name.to_ascii_lowercase().as_str(),
            "table_info"
                | "table_xinfo"
                | "index_list"
                | "index_info"
                | "index_xinfo"
                | "foreign_key_list"
                | "table_list"
                | "schema_version"
        ),
        _ => false,
    };
    if read {
        authorize_sql(context)
    } else {
        Authorization::Deny
    }
}

struct Budget<'a>(&'a Connection, bool);

impl Drop for Budget<'_> {
    fn drop(&mut self) {
        // Cell connections pass fallible hook installation when opened.
        // Handle ownership is immutable for the lifetime of this guard.
        self.0
            .progress_handler(0, None::<fn() -> bool>)
            .expect("console connection owns its SQLite handle");
        self.0
            .authorizer(Some(authorize_sql))
            .expect("console connection owns its SQLite handle");
        ACTIVE.with(|active| active.set(self.1));
    }
}

pub(crate) fn sql_console(
    scope: &str,
    sql: &str,
    readonly: bool,
) -> Result<SqlResponse, SqlFailure> {
    require_sql_healthy(scope).map_err(sql_error)?;
    if sql.trim().is_empty() || sql.len() > 64 * 1024 {
        return Err(sql_error("console SQL must contain 1–65536 bytes"));
    }
    let source = CString::new(sql).map_err(|error| sql_error(error.to_string()))?;
    let started = std::time::Instant::now();
    with(scope, |connection| {
        if !connection.is_autocommit() {
            return Err(sql_error(
                "console SQL cannot start inside an open transaction",
            ));
        }
        // Readers take a deferred snapshot; writes retain the existing atomic exec contract.
        without_sql_authorizer(connection, || {
            connection.execute_batch(if readonly { "BEGIN" } else { "BEGIN IMMEDIATE" })
        })
        .map_err(|error| sql_error(error.to_string()))?;
        let ran = {
            let _budget = Budget(connection, ACTIVE.with(|active| active.replace(true)));
            if readonly {
                connection
                    .authorizer(Some(authorize_read))
                    .expect("console connection owns its SQLite handle");
            }
            let mut steps = 0u32;
            connection
                .progress_handler(
                    1000,
                    Some(move || {
                        steps += 1000;
                        steps >= 1_000_000 || started.elapsed() >= std::time::Duration::from_secs(2)
                    }),
                )
                .expect("console connection owns its SQLite handle");
            as_user_sql(|| {
                // SAFETY: the live connection owns every prepared statement;
                // sql_run_statements finalizes them on every return path.
                unsafe { sql_run_statements(scope, connection, &source, sql, true, sql_error) }
            })
        };
        match ran {
            Ok((count, results)) => {
                sql_transaction_control(
                    scope,
                    connection,
                    SqlTransactionOperation::Commit,
                    sql_error,
                )?;
                Ok(SqlResponse::Exec {
                    count,
                    duration: started.elapsed().as_secs_f64() * 1000.0,
                    results,
                })
            }
            Err(error) => {
                if !connection.is_autocommit() {
                    let _ = sql_transaction_control(
                        scope,
                        connection,
                        SqlTransactionOperation::Rollback,
                        sql_error,
                    );
                }
                Err(error)
            }
        }
    })
    .unwrap_or_else(|| Err(sql_error(format!("no db for {scope}"))))
}

/// A read-only page of protected migration bookkeeping. Unlike the SDK history
/// initializer, inspection must not create an absent bookkeeping table.
pub(crate) fn sql_console_migrations(
    scope: &str,
    table: &str,
    after: u64,
) -> anyhow::Result<serde_json::Value> {
    require_sql_healthy(scope).map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        cellar_native::sql::valid_migrations_table(table),
        "invalid migration table"
    );
    anyhow::ensure!(after <= i64::MAX as u64, "invalid migration cursor");
    with(scope, |connection| {
        without_sql_authorizer(connection, || {
            let exists = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)", [table], |row| row.get::<_, bool>(0))?;
            let mut items = Vec::new();
            let mut bytes = 0usize;
            let mut next_cursor = None;
            if exists {
                let mut statement = connection.prepare(&format!("SELECT id, name, applied_at FROM \"{table}\" WHERE id > ?1 ORDER BY id LIMIT 101"))?;
                let mut rows = statement.query([after as i64])?;
                while let Some(row) = rows.next()? {
                    if items.len() == 100 {
                        next_cursor = items.last().and_then(|item: &serde_json::Value| item["id"].as_u64());
                        break;
                    }
                    let item = serde_json::json!({"id":row.get::<_,i64>(0)?,"name":row.get::<_,String>(1)?,"applied_at":row.get::<_,String>(2)?});
                    bytes = bytes.saturating_add(serde_json::to_vec(&item)?.len());
                    anyhow::ensure!(bytes <= (1024*1024).min(crate::application::limits::current().sql.max_result_bytes) && items.len() < crate::application::limits::current().sql.max_rows, "migration history exceeds result limit");
                    items.push(item);
                }
            }
            Ok(serde_json::json!({"items":items,"next_cursor":next_cursor,"table":table,"exists":exists}))
        })
    }).ok_or_else(||anyhow::anyhow!("no db for {scope}"))?
}
