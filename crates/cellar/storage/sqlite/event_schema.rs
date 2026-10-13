//! Versioned event storage. Only known unmarked layouts from the unreleased
//! implementation may migrate; a future marker cannot be overwritten by DDL.

use crate::application::event_stream::{DeliveryError, ErrorCode};
use rusqlite::Connection;

pub(super) struct Schema {
    pub marker: &'static str,
    pub ddl: &'static str,
    pub required: &'static [(&'static str, &'static str)],
    pub optional: &'static [(&'static str, &'static str)],
}

pub(super) fn compatible(condition: bool, reason: &str) -> anyhow::Result<()> {
    if !condition {
        return Err(DeliveryError::new(ErrorCode::UnsupportedProtocol, reason).into());
    }
    Ok(())
}

fn present(connection: &Connection, table: &str) -> anyhow::Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
        [table],
        |row| row.get(0),
    )?)
}

pub(super) fn retirement_operator(
    connection: &Connection,
    marker: &str,
    current: i64,
    table: &str,
) -> anyhow::Result<()> {
    if !present(connection, marker)? {
        return Ok(());
    }
    let version: i64 = connection.query_row(
        &format!("SELECT version FROM {marker} WHERE singleton=1"),
        [],
        |row| row.get(0),
    )?;
    if version != current {
        return Ok(());
    }
    let types = connection
        .prepare("SELECT type FROM pragma_table_info(?1) WHERE name='operator'")?
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    compatible(
        types.len() == 1 && types[0].eq_ignore_ascii_case("TEXT"),
        "retirement schema marker requires operator provenance",
    )
}

fn layout(connection: &Connection, table: &str, expected: &str) -> anyhow::Result<()> {
    // Names come only from framework-owned static declarations.
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .take(expected.split(',').count() + 1)
        .collect::<rusqlite::Result<Vec<_>>>()?;
    compatible(
        columns.join(",") == expected,
        "persisted event table has an unsupported layout",
    )
}

pub(super) fn check_version(
    connection: &Connection,
    marker: &str,
    supported: &[i64],
) -> anyhow::Result<bool> {
    if !present(connection, marker)? {
        return Ok(false);
    }
    let (count, version): (i64, Option<i64>) = connection.query_row(
        &format!("SELECT count(*),max(CASE WHEN singleton=1 THEN version END) FROM {marker}"),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    compatible(
        count == 1 && version.is_some_and(|version| supported.contains(&version)),
        "unsupported or missing persisted event schema version",
    )?;
    Ok(true)
}

impl Schema {
    pub(super) fn check(&self, connection: &Connection) -> anyhow::Result<bool> {
        let marked = check_version(connection, self.marker, &[1])?;
        if marked {
            layout(connection, self.marker, "singleton,version")?;
        }
        let mut required = 0;
        let mut optional = 0;
        for (table, expected) in self.required {
            if present(connection, table)? {
                layout(connection, table, expected)?;
                required += 1;
            }
        }
        for (table, expected) in self.optional {
            if present(connection, table)? {
                layout(connection, table, expected)?;
                optional += 1;
            }
        }
        let exists = marked || required != 0 || optional != 0;
        compatible(
            !exists || required == self.required.len(),
            "persisted event schema is incomplete",
        )?;
        compatible(
            !marked || optional == self.optional.len(),
            "versioned event schema lacks protocol tables",
        )?;
        Ok(exists)
    }

    /// The caller owns a transaction/savepoint, so migration and its marker
    /// follow the surrounding emission, inbox, or activation rollback.
    pub(super) fn open(&self, connection: &Connection) -> anyhow::Result<()> {
        compatible(
            !connection.is_autocommit(),
            "event schema migration requires a transaction",
        )?;
        // Activation already validated marked layouts. Avoid rebuilding DDL
        // and walking retained records for every emission/inbox acceptance.
        if check_version(connection, self.marker, &[1])? {
            return Ok(());
        }
        self.check(connection)?;
        connection.execute_batch(self.ddl)?;
        connection.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            version INTEGER NOT NULL CHECK(typeof(version)='integer' AND version>0)
        ); INSERT OR IGNORE INTO {} VALUES(1,1);",
            self.marker, self.marker
        ))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marked_retirement_provenance_is_required_before_base_schema_changes() -> anyhow::Result<()> {
        for (marker, version, table) in [
            ("__sys_stream_schema", 4, "__sys_stream_retirement"),
            (
                "__sys_event_catalog_schema",
                3,
                "__sys_event_catalog_receipts",
            ),
        ] {
            for column in ["", ",operator INTEGER"] {
                let connection = Connection::open_in_memory()?;
                connection.execute_batch(&format!("CREATE TABLE business(value TEXT);INSERT INTO business VALUES('retained');CREATE TABLE {marker}(singleton INTEGER PRIMARY KEY,version INTEGER);INSERT INTO {marker} VALUES(1,{version});CREATE TABLE {table}(operation TEXT{column});"))?;
                let before: i64 =
                    connection.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
                let failure = super::super::schema(&connection).unwrap_err();
                assert_eq!(
                    failure.downcast_ref::<DeliveryError>().unwrap().code,
                    ErrorCode::UnsupportedProtocol
                );
                assert_eq!(
                    connection
                        .query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))?,
                    before
                );
                assert_eq!(
                    connection.query_row("SELECT value FROM business", [], |row| row
                        .get::<_, String>(0))?,
                    "retained"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn future_and_missing_markers_fail_before_any_schema_mutation() -> anyhow::Result<()> {
        for marker in [
            "__sys_outbox_schema",
            "__sys_outbox_control_schema",
            "__sys_inbox_schema",
            "__sys_stream_schema",
            "__sys_event_catalog_schema",
        ] {
            for version in [Some(999), None] {
                let connection = Connection::open_in_memory()?;
                connection.execute_batch(&format!(
                    "CREATE TABLE business(value TEXT); INSERT INTO business VALUES('retained');
                    CREATE TABLE {marker}(singleton INTEGER PRIMARY KEY,version INTEGER NOT NULL);"
                ))?;
                if let Some(version) = version {
                    connection.execute(&format!("INSERT INTO {marker} VALUES(1,?1)"), [version])?;
                }
                let before: i64 =
                    connection.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
                let failure = super::super::schema(&connection).unwrap_err();
                assert_eq!(
                    failure.downcast_ref::<DeliveryError>().unwrap().code,
                    ErrorCode::UnsupportedProtocol
                );
                let after: i64 =
                    connection.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
                assert_eq!(
                    before, after,
                    "unsupported {marker} changed the storage schema"
                );
                assert_eq!(
                    connection.query_row("SELECT value FROM business", [], |row| row
                        .get::<_, String>(0))?,
                    "retained"
                );
            }
        }
        Ok(())
    }
}
