//! Transactional, activation-time stream schema migrations. The unreleased
//! first schema had a global unique name; version two permits live-name reuse
//! while retaining deleted subscription identities as tombstones. Version
//! three adds terminal retirement receipts and recoverable bounded cleanup.

use super::{error::error, store, ErrorCode};
use crate::application::{Result, Storage};

const CURRENT: i64 = 3;
const METADATA: &str = "CREATE TABLE IF NOT EXISTS __sys_stream_schema (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    version INTEGER NOT NULL CHECK(typeof(version)='integer' AND version>0)
); INSERT INTO __sys_stream_schema(singleton,version) VALUES(1,3)
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
    if version.is_some_and(|version| version != 1 && version != 2 && version != CURRENT) {
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
    db.system_sql_schema(&ddl)
}
