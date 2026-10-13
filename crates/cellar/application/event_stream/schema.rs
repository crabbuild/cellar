//! Transactional, activation-time stream schema migrations. The unreleased
//! first schema had a global unique name; version two permits live-name reuse
//! while retaining deleted subscription identities as tombstones. Version
//! three adds terminal retirement receipts and recoverable bounded cleanup.
//! Version four retains operator identity in partition retirement receipts.

use super::{error::error, store, ErrorCode};
use crate::application::{Result, Storage};

const CURRENT: i64 = 4;
const METADATA: &str = "CREATE TABLE IF NOT EXISTS __sys_stream_schema (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    version INTEGER NOT NULL CHECK(typeof(version)='integer' AND version>0)
); INSERT INTO __sys_stream_schema(singleton,version) VALUES(1,4)
   ON CONFLICT(singleton) DO UPDATE SET version=excluded.version;";

pub(crate) fn open(db: &Storage) -> Result<()> {
    let marker = db.system_sql(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name='__sys_stream_schema'",
        &[],
    )?;
    let version = if marker.is_empty() {
        None
    } else {
        let rows = db.system_sql(
            "SELECT version FROM __sys_stream_schema WHERE singleton=1",
            &[],
        )?;
        rows.first()
            .map(|row| store::number(row, "version"))
            .transpose()?
    };
    if version.is_some_and(|version| !(1..=CURRENT).contains(&version)) {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "unsupported persisted stream schema",
        ));
    }
    let tables = db.system_sql(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND name='__sys_stream_subscriptions'",
        &[],
    )?;
    let legacy = tables
        .first()
        .map(|row| store::text(row, "sql"))
        .transpose()?
        .is_some_and(|sql| {
            sql.split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_uppercase()
                .contains("NAME TEXT NOT NULL UNIQUE")
        });
    if version.is_some_and(|version| version >= 2) && legacy {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "stream schema marker conflicts with its tables",
        ));
    }
    let operator = operator_migration(db, "__sys_stream_retirement", version == Some(CURRENT))?;
    if version == Some(CURRENT) {
        return Ok(());
    }
    let schema = include_str!("schema.sql");
    let ddl = if legacy {
        format!("ALTER TABLE __sys_stream_subscriptions RENAME TO __sys_stream_subscription_migration;
            {schema}
            INSERT INTO __sys_stream_subscriptions SELECT * FROM __sys_stream_subscription_migration;
            DROP TABLE __sys_stream_subscription_migration;
            {schema}
            {METADATA}")
    } else {
        format!("{schema}\n{METADATA}")
    };
    db.system_sql_schema(&format!("{operator}\n{ddl}"))
}

pub(crate) fn operator_migration(db: &Storage, table: &str, current: bool) -> Result<String> {
    let exists = !db
        .system_sql(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name=?",
            &[table.into()],
        )?
        .is_empty();
    let columns = db.system_sql(
        "SELECT name,type FROM pragma_table_info(?) WHERE name='operator'",
        &[table.into()],
    )?;
    if current && (!exists || columns.is_empty()) {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "retirement schema marker requires operator provenance",
        ));
    }
    if let Some(column) = columns.first() {
        if !store::text(column, "type")?.eq_ignore_ascii_case("TEXT") {
            return Err(error(
                ErrorCode::UnsupportedProtocol,
                "invalid retirement operator column",
            ));
        }
    }
    if exists && columns.is_empty() {
        Ok(operator_ddl(table))
    } else {
        Ok(String::new())
    }
}

fn operator_ddl(table: &str) -> String {
    format!("ALTER TABLE {table} ADD COLUMN operator TEXT CHECK(operator IS NULL OR (typeof(operator)='text' AND length(CAST(operator AS BLOB)) BETWEEN 1 AND 256));")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_column_migration_preserves_legacy_receipts_and_bounds_new_provenance() -> Result<()>
    {
        for table in ["__sys_stream_retirement", "__sys_event_catalog_receipts"] {
            let connection = rusqlite::Connection::open_in_memory()?;
            connection.execute_batch(&format!("CREATE TABLE {table}(operation TEXT PRIMARY KEY,request TEXT,result TEXT); INSERT INTO {table} VALUES('retained','original request','original receipt');"))?;
            let before: (String, String, String) = connection.query_row(
                &format!("SELECT operation,request,result FROM {table}"),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            connection.execute_batch(&operator_ddl(table))?;
            let after: (String, String, String, Option<String>) = connection.query_row(
                &format!("SELECT operation,request,result,operator FROM {table}"),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            assert_eq!((after.0, after.1, after.2), before);
            assert_eq!(after.3, None);
            assert!(connection
                .execute(&format!("UPDATE {table} SET operator=''"), [])
                .is_err());
            assert!(connection
                .execute(
                    &format!("UPDATE {table} SET operator=?1"),
                    ["x".repeat(257)]
                )
                .is_err());
            connection.execute(
                &format!("UPDATE {table} SET operator='events-operator'"),
                [],
            )?;
            assert_eq!(
                connection.query_row(&format!("SELECT operator FROM {table}"), [], |row| row
                    .get::<_, String>(
                    0
                ))?,
                "events-operator"
            );
        }
        Ok(())
    }
}
