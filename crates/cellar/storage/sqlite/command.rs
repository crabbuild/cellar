// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Restricted synchronous transactions for the private Raft command image.

use anyhow::{ensure, Context as _, Result};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{ffi, Connection, OpenFlags, Params, Row};
use std::path::Path;

#[path = "command_capacity.rs"]
mod capacity;

/// Host-captured inputs for one command. Followers apply bytes, never rerun it.
#[derive(Clone, Copy, Debug)]
pub struct CommandInputs {
    pub now_unix_millis: u64,
    pub random: [u8; 32],
}

/// A callback can use SQL and captured inputs, but cannot retain or commit the
/// framework transaction, obtain its connection, or start asynchronous work.
pub struct CommandTransaction<'a> {
    connection: &'a Connection,
    inputs: CommandInputs,
}

impl CommandTransaction<'_> {
    pub(crate) fn framework(&self) -> &Connection {
        self.connection
    }

    pub fn inputs(&self) -> CommandInputs {
        self.inputs
    }

    pub fn execute(&self, sql: &str, params: impl Params) -> Result<usize> {
        super::check_sql_rename(sql).map_err(anyhow::Error::msg)?;
        super::as_user_sql(|| self.connection.execute(sql, params)).map_err(Into::into)
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        super::check_sql_rename(sql).map_err(anyhow::Error::msg)?;
        super::as_user_sql(|| self.connection.execute_batch(sql)).map_err(Into::into)
    }

    pub fn query_row<T>(
        &self,
        sql: &str,
        params: impl Params,
        read: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<T> {
        super::check_sql_rename(sql).map_err(anyhow::Error::msg)?;
        super::as_user_sql(|| self.connection.query_row(sql, params, read)).map_err(Into::into)
    }
}

pub(crate) struct CommandDatabase {
    connection: Connection,
}

impl CommandDatabase {
    /// Inspect the current framework-owned transaction before committing its
    /// captured bytes. This never visits application table payloads.
    pub(crate) fn capacity(connection: &Connection, quota: u64) -> Result<capacity::Usage> {
        capacity::measure(connection, quota)
    }

    /// Recheck after capture/checkpoint control writes. The private owner keeps
    /// every writer quiescent, and this read transaction binds all observations
    /// to one SQLite view without modifying the disposable image.
    pub(crate) fn frozen_capacity(&self, quota: u64) -> Result<capacity::Usage> {
        ensure!(
            self.connection.is_autocommit(),
            "capacity inspection requires a quiescent writer"
        );
        super::as_storage_transaction(|| self.connection.execute_batch("BEGIN"))?;
        let result = capacity::measure(&self.connection, quota);
        super::as_storage_transaction(|| self.connection.execute_batch("ROLLBACK"))
            .context("private capacity inspection rollback failed; rebuild is required")?;
        result
    }

    pub(crate) fn open(path: &Path, vfs: Option<&str>, max_image_bytes: u64) -> Result<Self> {
        let connection = match vfs {
            Some(vfs) => Connection::open_with_flags_and_vfs(path, OpenFlags::default(), vfs)?,
            None => Connection::open(path)?,
        };
        // This connection only mutates the disposable Raft working image. The
        // captured command and committed image have separate durability barriers;
        // recovery always discards this SQLite database and its sidecars.
        connection.execute_batch("PRAGMA synchronous=OFF; PRAGMA wal_autocheckpoint=0; PRAGMA temp_store=MEMORY; PRAGMA trusted_schema=OFF; PRAGMA cache_size=-64;")?;
        let page_size: u64 = connection.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        ensure!(
            cellar_ltx::ltx::is_valid_page_size(u32::try_from(page_size)?),
            "invalid command page size"
        );
        ensure!(
            max_image_bytes / page_size > 0,
            "command image limit is smaller than one page"
        );
        connection.pragma_update(None, "max_page_count", max_image_bytes / page_size)?;
        if let Some(limits) = crate::application::limits::current().sql.connection {
            // SAFETY: this live connection is exclusively owned and no statements
            // or transaction have been exposed. Limits do not mutate the image.
            unsafe {
                for (category, limit) in super::sqlite_limit_categories(limits) {
                    ffi::sqlite3_limit(connection.handle(), category, limit);
                }
            }
        }
        // SAFETY: the same exclusive, unused connection receives strict local
        // policies before any callback. No application can lower them through SQL.
        unsafe {
            ffi::sqlite3_limit(connection.handle(), ffi::SQLITE_LIMIT_ATTACHED, 0);
            ffi::sqlite3_limit(connection.handle(), ffi::SQLITE_LIMIT_WORKER_THREADS, 0);
            ensure!(
                ffi::sqlite3_db_config(
                    connection.handle(),
                    ffi::SQLITE_DBCONFIG_DEFENSIVE,
                    1,
                    std::ptr::null_mut::<i32>()
                ) == ffi::SQLITE_OK,
                "cannot enable defensive command SQLite"
            );
        }
        capacity::initialize(&connection)?;
        connection.authorizer(Some(authorize_command))?;
        Ok(Self { connection })
    }

    /// Only fixed framework receipt/schema statements receive this connection.
    pub(crate) fn framework(&self) -> &Connection {
        &self.connection
    }

    pub(crate) fn transaction<T>(
        &self,
        inputs: CommandInputs,
        callback: impl FnOnce(&Connection, &CommandTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let _sql = cellar_ltx::internal::image_timing::span(
            cellar_ltx::internal::image_timing::Phase::Sql,
        );
        super::as_storage_transaction(|| self.connection.execute_batch("BEGIN IMMEDIATE"))?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            callback(
                &self.connection,
                &CommandTransaction {
                    connection: &self.connection,
                    inputs,
                },
            )
        }))
        .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)));
        let result = match result {
            Ok(value) => super::as_storage_transaction(|| self.connection.execute_batch("COMMIT"))
                .map(|()| value)
                .map_err(anyhow::Error::from),
            Err(error) => Err(error),
        };
        if result.is_err() && !self.connection.is_autocommit() {
            super::as_storage_transaction(|| self.connection.execute_batch("ROLLBACK"))
                .context("private command rollback failed; rebuild is required")?;
        }
        result
    }

    pub(crate) fn geometry(&self, max_image_bytes: u64) -> Result<(u64, u32)> {
        ensure!(
            self.connection.is_autocommit(),
            "cannot freeze an active command transaction"
        );
        let size: u32 = self
            .connection
            .query_row("PRAGMA page_size", [], |r| r.get(0))?;
        let pages: u64 = self
            .connection
            .query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let length = pages
            .checked_mul(size as u64)
            .context("image length overflow")?;
        ensure!(
            length > 0 && length <= max_image_bytes,
            "private image exceeds its bound"
        );
        Ok((length, size))
    }
}

fn authorize_command(context: AuthContext<'_>) -> Authorization {
    if super::authorize_sql(context) != Authorization::Allow {
        return Authorization::Deny;
    }
    if !super::USER_SQL.with(std::cell::Cell::get) {
        return Authorization::Allow;
    }
    match context.action {
        AuthAction::Pragma { .. } => Authorization::Deny,
        AuthAction::Function { function_name }
            if matches!(
                function_name.to_ascii_lowercase().as_str(),
                "random"
                    | "randomblob"
                    | "date"
                    | "time"
                    | "datetime"
                    | "julianday"
                    | "strftime"
                    | "unixepoch"
                    | "timediff"
                    | "current_date"
                    | "current_time"
                    | "current_timestamp"
            ) =>
        {
            Authorization::Deny
        }
        _ => Authorization::Allow,
    }
}
