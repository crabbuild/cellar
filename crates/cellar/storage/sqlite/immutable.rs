// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A distinct opener for a verified immutable image; no framework schema writes.

use crate::application::{storage::Resources, Storage};
use cellar_ltx::generation::{GenerationPin, ImmutableDatabase, ReadServices};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{ffi, Connection};
use std::cell::{OnceCell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

pub struct ImmutableQueryOptions {
    pub services: ReadServices,
    pub sqlite_vec: bool,
}

pub struct ImmutableQuery {
    database: ImmutableDatabase,
}

impl ImmutableQuery {
    /// Application SQL retains the read-only authorizer and the reservation of
    /// framework tables. Framework receipt lookup uses its separate connection
    /// path; a query callback cannot depend on or expose those private rows.
    pub fn query_row<T>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        read: impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> anyhow::Result<T> {
        super::as_user_sql(|| self.connection().query_row(sql, params, read)).map_err(Into::into)
    }

    /// The borrow retains the generation pin for statements/cursors as well as
    /// the connection. Application query facades expose only read operations.
    pub fn connection(&self) -> &Connection {
        self.database.connection()
    }
}

/// Owns the pinned image only for a synchronous registered query. Cloned
/// Storage views carry the callback identity and cannot retain the image.
pub(crate) struct QueryFrame {
    database: ImmutableQuery,
    storage: OnceCell<Arc<Resources>>,
    kv_present: OnceCell<bool>,
}

thread_local! {
    static CURRENT_QUERY: RefCell<Option<Rc<QueryFrame>>> = const { RefCell::new(None) };
}

impl QueryFrame {
    pub(crate) fn run<T>(
        database: ImmutableQuery,
        callback: impl FnOnce(&Self) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        struct Installed;

        impl Drop for Installed {
            fn drop(&mut self) {
                CURRENT_QUERY.with(|current| current.borrow_mut().take());
            }
        }
        let frame = Rc::new(Self {
            database,
            storage: OnceCell::new(),
            kv_present: OnceCell::new(),
        });
        let _installed = CURRENT_QUERY.with(|current| {
            let mut current = current.borrow_mut();
            anyhow::ensure!(current.is_none(), "nested Raft query");
            *current = Some(frame.clone());
            Ok::<_, anyhow::Error>(Installed)
        })?;
        callback(&frame)
    }

    pub(crate) fn database(&self) -> &ImmutableQuery {
        &self.database
    }

    pub(crate) fn storage(&self) -> Storage {
        let resources = self.storage.get_or_init(|| Arc::new(Resources::default()));
        Storage::raft_query(resources.clone())
    }

    fn kv_present(&self) -> rusqlite::Result<bool> {
        if let Some(present) = self.kv_present.get() {
            return Ok(*present);
        }
        let present = super::as_framework_sql(|| {
            self.database.connection().query_row(
                "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name='__sys_cell_kv')",
                [],
                |row| row.get(0),
            )
        })?;
        Ok(*self.kv_present.get_or_init(|| present))
    }
}

pub(super) fn active() -> bool {
    CURRENT_QUERY.with(|current| current.borrow().is_some())
}

pub(super) fn with<T>(read: impl FnOnce(&Connection) -> T) -> Option<T> {
    CURRENT_QUERY.with(|current| {
        let current = current.borrow();
        Some(read(current.as_ref()?.database.connection()))
    })
}

pub(super) fn has_kv() -> rusqlite::Result<bool> {
    // Images written without SDK KV contain no KV schema.
    CURRENT_QUERY.with(|current| {
        current
            .borrow()
            .as_ref()
            .map_or(Ok(true), |frame| frame.kv_present())
    })
}

pub(crate) fn require_storage(resources: &Arc<Resources>) -> anyhow::Result<()> {
    CURRENT_QUERY.with(|current| {
        anyhow::ensure!(
            current
                .borrow()
                .as_ref()
                .and_then(|frame| frame.storage.get())
                .is_some_and(|owner| Arc::ptr_eq(owner, resources)),
            "storage is outside its committed query"
        );
        Ok(())
    })
}

pub fn open_immutable_generation(
    pin: GenerationPin,
    options: ImmutableQueryOptions,
) -> anyhow::Result<ImmutableQuery> {
    let database = cellar_ltx::generation::open_immutable(pin, options.services)?;
    configure(database, options.sqlite_vec)
}

/// Apply the same SQL limits and authorizer to an independently verified extent
/// view. This opener supplies no schema initialization or native read authority.
pub fn open_immutable_extent_image(
    image: cellar_ltx::internal::ExtentImage,
    options: ImmutableQueryOptions,
) -> anyhow::Result<ImmutableQuery> {
    let database = cellar_ltx::internal::open_extent_image(image, options.services)?;
    configure(database, options.sqlite_vec)
}

fn configure(database: ImmutableDatabase, sqlite_vec: bool) -> anyhow::Result<ImmutableQuery> {
    let connection = database.connection();
    if sqlite_vec {
        super::register_vec0_extension(connection)?;
    }
    let limits = crate::application::limits::current().sql;
    // SAFETY: the opener owns this live, unused connection. These policies
    // cannot create a journal, initialize a schema or write the pinned image.
    unsafe {
        let handle = connection.handle();
        if let Some(connection) = limits.connection {
            for (category, limit) in super::sqlite_limit_categories(connection) {
                ffi::sqlite3_limit(handle, category, limit);
            }
        }
        ffi::sqlite3_limit(handle, ffi::SQLITE_LIMIT_ATTACHED, 0);
        ffi::sqlite3_limit(handle, ffi::SQLITE_LIMIT_WORKER_THREADS, 0);
        let result = ffi::sqlite3_db_config(
            handle,
            ffi::SQLITE_DBCONFIG_DEFENSIVE,
            1,
            std::ptr::null_mut::<i32>(),
        );
        anyhow::ensure!(
            result == ffi::SQLITE_OK,
            "cannot enable defensive immutable SQLite"
        );
    }
    connection.authorizer(Some(authorize_query))?;
    Ok(ImmutableQuery { database })
}

fn authorize_query(context: AuthContext<'_>) -> Authorization {
    if super::authorize_sql(context) != Authorization::Allow {
        return Authorization::Deny;
    }
    match context.action {
        AuthAction::Read { .. }
        | AuthAction::Select
        | AuthAction::Recursive
        | AuthAction::Function { .. } => Authorization::Allow,
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => {
            let name = pragma_name.to_ascii_lowercase();
            let observation = matches!(
                name.as_str(),
                "table_info"
                    | "table_xinfo"
                    | "index_info"
                    | "index_xinfo"
                    | "index_list"
                    | "foreign_key_list"
                    | "foreign_key_check"
                    | "quick_check"
            ) || pragma_value.is_none()
                && matches!(
                    name.as_str(),
                    "page_count"
                        | "freelist_count"
                        | "page_size"
                        | "schema_version"
                        | "user_version"
                );
            if observation {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        _ => Authorization::Deny,
    }
}
