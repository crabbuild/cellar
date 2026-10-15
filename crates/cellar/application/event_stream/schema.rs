//! Fresh version-1 schemas for internal development; no layout migrations.

use super::{error::error, store, ErrorCode};
use crate::application::{Result, Storage};

pub(crate) fn open(db: &Storage) -> Result<()> {
    open_schema(
        db,
        "__sys_stream_schema",
        "__sys_stream",
        "__sys_stream_retirement",
        include_str!("schema.sql"),
    )
}

pub(crate) fn open_schema(
    db: &Storage,
    marker: &str,
    table: &str,
    receipts: &str,
    ddl: &str,
) -> Result<()> {
    let marked = !db
        .system_sql(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name=?",
            &[marker.into()],
        )?
        .is_empty();
    if marked {
        let rows = db.system_sql(&format!("SELECT singleton,version FROM {marker}"), &[])?;
        if rows.len() != 1
            || store::number(&rows[0], "singleton")? != 1
            || store::number(&rows[0], "version")? != 1
        {
            return Err(error(
                ErrorCode::UnsupportedProtocol,
                "unsupported persisted event schema; recreate development state",
            ));
        }
        let columns = db.system_sql(
            "SELECT type FROM pragma_table_info(?) WHERE name='operator'",
            &[receipts.into()],
        )?;
        if columns.len() != 1 || !store::text(&columns[0], "type")?.eq_ignore_ascii_case("TEXT") {
            return Err(error(
                ErrorCode::UnsupportedProtocol,
                "event schema lacks operator provenance; recreate development state",
            ));
        }
        return Ok(());
    }
    if !db
        .system_sql(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name IN (?,?)",
            &[table.into(), receipts.into()],
        )?
        .is_empty()
    {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "unmarked event schema; recreate development state",
        ));
    }
    db.system_sql_schema(&format!("{ddl}
        CREATE TABLE {marker}(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL CHECK(version=1));
        INSERT INTO {marker} VALUES(1,1);"))
}
