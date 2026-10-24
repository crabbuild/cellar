// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A distinct opener for a verified immutable image; no framework schema writes.

use cellar_ltx::generation::{GenerationPin, ImmutableDatabase, ReadServices};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{ffi, Connection};

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

pub fn open_immutable_generation(
    pin: GenerationPin,
    options: ImmutableQueryOptions,
) -> anyhow::Result<ImmutableQuery> {
    let database = cellar_ltx::generation::open_immutable(pin, options.services)?;
    let connection = database.connection();
    if options.sqlite_vec {
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
    connection.authorizer(Some(authorize_query));
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
