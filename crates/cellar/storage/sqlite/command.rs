// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Restricted synchronous transactions for the private Raft command image.

use anyhow::{ensure, Context as _, Result};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{ffi, Connection, OpenFlags, Params, Row};
use std::cell::{OnceCell, Ref, RefCell};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use crate::application::storage::Resources;
use crate::application::Storage;

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
    frame: &'a CommandFrame,
    inputs: CommandInputs,
}

impl CommandTransaction<'_> {
    pub(crate) fn framework(&self) -> Result<Ref<'_, Connection>> {
        self.frame.connection()
    }

    pub fn inputs(&self) -> CommandInputs {
        self.inputs
    }

    pub fn execute(&self, sql: &str, params: impl Params) -> Result<usize> {
        super::check_sql_rename(sql).map_err(anyhow::Error::msg)?;
        let connection = self.framework()?;
        super::as_user_sql(|| connection.execute(sql, params)).map_err(Into::into)
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        super::check_sql_rename(sql).map_err(anyhow::Error::msg)?;
        let connection = self.framework()?;
        super::as_user_sql(|| connection.execute_batch(sql)).map_err(Into::into)
    }

    pub fn query_row<T>(
        &self,
        sql: &str,
        params: impl Params,
        read: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<T> {
        super::check_sql_rename(sql).map_err(anyhow::Error::msg)?;
        let connection = self.framework()?;
        super::as_user_sql(|| connection.query_row(sql, params, read)).map_err(Into::into)
    }

    /// Open an ordinary Storage view within this private transaction. Its
    /// synchronous operations share the command's commit and durability.
    pub fn storage(&self) -> Result<Storage> {
        let connection = self.framework()?;
        if self.frame.storage.get().is_none() {
            super::as_framework_sql(|| connection.execute_batch(super::KV_SCHEMA))?;
        }
        let resources = self
            .frame
            .storage
            .get_or_init(|| Arc::new(Resources::default()));
        Ok(Storage::raft_command(resources.clone()))
    }
}

struct CommandFrame {
    connection: RefCell<Connection>,
    storage: OnceCell<Arc<Resources>>,
    failure: RefCell<Option<String>>,
}

thread_local! {
    static CURRENT_COMMAND: RefCell<Option<Rc<CommandFrame>>> = const { RefCell::new(None) };
}

impl CommandFrame {
    fn connection(&self) -> Result<Ref<'_, Connection>> {
        let connection = self
            .connection
            .try_borrow()
            .context("private SQLite is busy")?;
        ensure!(
            !connection.is_autocommit(),
            "private command transaction ended"
        );
        if let Some(error) = self.failure.borrow().as_ref() {
            anyhow::bail!("{error}");
        }
        Ok(connection)
    }

    fn run<T>(
        frame: &Rc<Self>,
        inputs: CommandInputs,
        callback: impl FnOnce(&CommandTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        struct Installed;

        impl Drop for Installed {
            fn drop(&mut self) {
                CURRENT_COMMAND.with(|current| current.borrow_mut().take());
            }
        }

        let _installed = CURRENT_COMMAND.with(|current| {
            let mut current = current.borrow_mut();
            ensure!(current.is_none(), "nested Raft command");
            *current = Some(frame.clone());
            Ok::<_, anyhow::Error>(Installed)
        })?;
        super::as_storage_transaction(|| {
            frame.connection.borrow().execute_batch("BEGIN IMMEDIATE")
        })?;
        let result = callback(&CommandTransaction { frame, inputs })?;
        let connection = frame.connection()?;
        super::as_storage_transaction(|| connection.execute_batch("COMMIT"))?;
        Ok(result)
    }
}

pub(super) fn with<T>(read: impl FnOnce(&Connection) -> T) -> Option<T> {
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let connection = current.as_ref()?.connection.try_borrow().ok()?;
        Some(read(&connection))
    })
}

pub(super) fn with_mut<T>(write: impl FnOnce(&mut Connection) -> T) -> Option<T> {
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let mut connection = current.as_ref()?.connection.try_borrow_mut().ok()?;
        Some(write(&mut connection))
    })
}

pub(crate) fn require_storage(resources: &Arc<Resources>, healthy: bool) -> Result<()> {
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let frame = current
            .as_ref()
            .context("storage is outside its Raft command")?;
        ensure!(
            frame
                .storage
                .get()
                .is_some_and(|owner| Arc::ptr_eq(owner, resources)),
            "storage belongs to another Raft command"
        );
        if healthy {
            frame.connection()?;
        }
        Ok(())
    })
}

pub(super) fn failure() -> Option<String> {
    CURRENT_COMMAND.with(|current| current.borrow().as_ref()?.failure.borrow().clone())
}

pub(super) fn poison(error: String) {
    CURRENT_COMMAND.with(|current| {
        if let Some(frame) = current.borrow().as_ref() {
            frame.failure.borrow_mut().get_or_insert(error);
        }
    });
}

pub(crate) struct CommandDatabase {
    connection: Option<Connection>,
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
        let connection = self.framework();
        ensure!(
            connection.is_autocommit(),
            "capacity inspection requires a quiescent writer"
        );
        super::as_storage_transaction(|| connection.execute_batch("BEGIN"))?;
        let result = capacity::measure(connection, quota);
        super::as_storage_transaction(|| connection.execute_batch("ROLLBACK"))
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
        Ok(Self {
            connection: Some(connection),
        })
    }

    /// Only fixed framework receipt/schema statements receive this connection.
    pub(crate) fn framework(&self) -> &Connection {
        self.connection
            .as_ref()
            .expect("working image owns its connection outside a command")
    }

    pub(crate) fn transaction<T>(
        &mut self,
        inputs: CommandInputs,
        callback: impl FnOnce(&CommandTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let frame = Rc::new(CommandFrame {
            connection: RefCell::new(
                self.connection
                    .take()
                    .context("private SQLite is unavailable")?,
            ),
            storage: OnceCell::new(),
            failure: RefCell::new(None),
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            CommandFrame::run(&frame, inputs, callback)
        }))
        .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)));
        // Storage handles retain only their authority, never the connection.
        let frame = Rc::try_unwrap(frame)
            .ok()
            .context("private SQLite is still installed")?;
        self.connection = Some(frame.connection.into_inner());
        let connection = self.framework();
        if result.is_err() && !connection.is_autocommit() {
            super::as_storage_transaction(|| connection.execute_batch("ROLLBACK"))
                .context("private command rollback failed; rebuild is required")?;
        }
        result
    }

    pub(crate) fn geometry(&self, max_image_bytes: u64) -> Result<(u64, u32)> {
        let connection = self.framework();
        ensure!(
            connection.is_autocommit(),
            "cannot freeze an active command transaction"
        );
        let size: u32 = connection.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        let pages: u64 = connection.query_row("PRAGMA page_count", [], |r| r.get(0))?;
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
