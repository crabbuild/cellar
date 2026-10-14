//! Internal-development event storage. Existing layouts require a version-1 marker.

use crate::application::event_stream::{DeliveryError, ErrorCode};
use rusqlite::Connection;

pub(super) struct Schema {
    pub marker: &'static str,
    pub ddl: &'static str,
    pub required: &'static [(&'static str, &'static str)],
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

pub(super) fn check_version(connection: &Connection, marker: &str) -> anyhow::Result<bool> {
    if !present(connection, marker)? {
        let tables: &[&str] = match marker {
            "__sys_stream_schema" => &[
                "__sys_stream",
                "__sys_stream_retirement",
                "__sys_stream_subscriptions",
            ],
            "__sys_event_catalog_schema" => {
                &["__sys_event_catalog", "__sys_event_catalog_receipts"]
            }
            _ => &[],
        };
        for table in tables {
            compatible(
                !present(connection, table)?,
                "unmarked event schema; recreate development state",
            )?;
        }
        return Ok(false);
    }
    let (count, version): (i64, Option<i64>) = connection.query_row(
        &format!("SELECT count(*),max(CASE WHEN singleton=1 THEN version END) FROM {marker}"),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    compatible(
        count == 1 && version == Some(1),
        "unsupported or missing persisted event schema version",
    )?;
    Ok(true)
}

/// Refuse earlier development layouts before any base-schema mutation.
pub(super) fn stream_state(connection: &Connection) -> anyhow::Result<()> {
    if !check_version(connection, "__sys_stream_schema")? {
        return Ok(());
    }
    layout(connection,"__sys_stream","singleton,definition,revision,lifecycle,next_offset,earliest_offset,bytes,rows,logical_ms,config_operation,config_digest,config_principal,config_grant,byte_position")?;
    layout(
        connection,
        "__sys_stream_events",
        "offset,lane,sequence,envelope,size,append_ms,byte_position",
    )?;
    for (index, expected) in [
        ("__sys_stream_events_age", "append_ms,offset"),
        ("__sys_stream_required_frontier", "checkpoint"),
        ("__sys_stream_subscription_state", "state,id"),
    ] {
        let columns = connection
            .prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")?
            .query_map([index], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        compatible(
            columns.join(",") == expected,
            "stream observation index missing; recreate development state",
        )?;
    }
    let state = connection.prepare("SELECT earliest_offset,next_offset,rows,bytes,byte_position FROM __sys_stream WHERE singleton=1")?
        .query_map([],|row|Ok((row.get::<_,i64>(0)?,row.get::<_,i64>(1)?,row.get::<_,i64>(2)?,row.get::<_,i64>(3)?,row.get::<_,i64>(4)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let Some(&(earliest, end, expected_rows, expected_bytes, tail)) = state.first() else {
        return Ok(());
    };
    compatible(
        earliest > 0
            && end >= earliest
            && expected_rows == end - earliest
            && expected_bytes >= 0
            && tail >= 0,
        "invalid retained stream accounting",
    )?;
    let mut offset = earliest;
    let mut bytes = 0i64;
    let mut prior = None;
    let mut previous_ms = None;
    let mut statement=connection.prepare("SELECT offset,size,length(envelope),append_ms,byte_position FROM __sys_stream_events ORDER BY offset")?;
    let mut records = statement.query([])?;
    while let Some(row) = records.next()? {
        let actual: i64 = row.get(0)?;
        let size: i64 = row.get(1)?;
        let length: i64 = row.get(2)?;
        let append_ms: i64 = row.get(3)?;
        let position: i64 = row.get(4)?;
        compatible(
            actual == offset
                && size > 0
                && size == length
                && append_ms >= 0
                && position >= 0
                && previous_ms.is_none_or(|previous| previous <= append_ms),
            "invalid retained stream event metadata",
        )?;
        if let Some(previous) = prior {
            compatible(
                cellar_logic::event_stream::advance_byte_position(previous, size)? == position,
                "stream byte positions conflict with retained events",
            )?;
        }
        bytes = bytes.checked_add(size).ok_or_else(|| {
            DeliveryError::new(
                ErrorCode::UnsupportedProtocol,
                "stream byte accounting overflow",
            )
        })?;
        offset = offset.checked_add(1).ok_or_else(|| {
            DeliveryError::new(
                ErrorCode::UnsupportedProtocol,
                "stream offset accounting overflow",
            )
        })?;
        prior = Some(position);
        previous_ms = Some(append_ms);
    }
    compatible(
        offset == end && bytes == expected_bytes && prior.is_none_or(|last| last == tail),
        "stream counters conflict with retained events",
    )
}

impl Schema {
    pub(super) fn check(&self, connection: &Connection) -> anyhow::Result<bool> {
        let marked = check_version(connection, self.marker)?;
        if marked {
            layout(connection, self.marker, "singleton,version")?;
        }
        let mut required = 0;
        for (table, expected) in self.required {
            if present(connection, table)? {
                layout(connection, table, expected)?;
                required += 1;
            }
        }
        let exists = marked || required != 0;
        compatible(
            !exists || (marked && required == self.required.len()),
            "persisted event schema is incomplete",
        )?;
        Ok(exists)
    }

    /// The caller owns a transaction/savepoint, so initialization and its marker
    /// follow the surrounding emission, inbox, or activation rollback.
    pub(super) fn open(&self, connection: &Connection) -> anyhow::Result<()> {
        compatible(
            !connection.is_autocommit(),
            "event schema initialization requires a transaction",
        )?;
        // Activation already validated marked layouts. Avoid rebuilding DDL
        // and walking retained records for every emission/inbox acceptance.
        if check_version(connection, self.marker)? {
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

    fn retained_stream() -> anyhow::Result<Connection> {
        let c = Connection::open_in_memory()?;
        c.execute_batch(include_str!("../../application/event_stream/schema.sql"))?;
        c.execute_batch("CREATE TABLE __sys_stream_schema(singleton INTEGER PRIMARY KEY,version INTEGER);INSERT INTO __sys_stream_schema VALUES(1,1);CREATE TABLE business(value TEXT);INSERT INTO business VALUES('retained');")?;
        c.execute("INSERT INTO __sys_stream(singleton,definition,revision,lifecycle,next_offset,earliest_offset,bytes,rows,logical_ms,config_operation,config_digest,config_principal,config_grant,byte_position) VALUES(1,'{}',1,'active',3,1,10,2,100,'operation','digest','principal','grant',6)",[])?;
        c.execute(
            "INSERT INTO __sys_stream_events VALUES(1,'lane',1,?1,3,100,?2)",
            rusqlite::params![b"aaa".as_slice(), i64::MAX],
        )?;
        c.execute(
            "INSERT INTO __sys_stream_events VALUES(2,'lane',2,?1,7,100,6)",
            [b"bbbbbbb".as_slice()],
        )?;
        Ok(c)
    }

    #[test]
    fn retained_byte_windows_survive_wrap_prefix_reclamation_and_restore() -> anyhow::Result<()> {
        let c = retained_stream()?;
        stream_state(&c)?;
        super::super::schema(&c)?;
        c.execute_batch("DELETE FROM __sys_stream_events WHERE offset=1;UPDATE __sys_stream SET earliest_offset=2,rows=1,bytes=7;")?;
        stream_state(&c)?;
        assert_eq!(cellar_logic::event_stream::byte_distance(i64::MAX, 6)?, 7);
        c.execute_batch("DELETE FROM __sys_stream_events;UPDATE __sys_stream SET earliest_offset=next_offset,rows=0,bytes=0;")?;
        stream_state(&c)?;
        assert_eq!(
            c.query_row("SELECT byte_position FROM __sys_stream", [], |row| row
                .get::<_, i64>(0))?,
            6
        );
        Ok(())
    }

    #[test]
    fn stream_observation_layout_and_accounting_fail_before_base_mutation() -> anyhow::Result<()> {
        for mutation in [
            "ALTER TABLE __sys_stream DROP COLUMN byte_position",
            "UPDATE __sys_stream SET bytes=9",
            "UPDATE __sys_stream_events SET byte_position=7 WHERE offset=2",
            "UPDATE __sys_stream_events SET append_ms=50 WHERE offset=2",
            "DROP INDEX __sys_stream_events_age",
        ] {
            let c = retained_stream()?;
            c.execute_batch(mutation)?;
            let before: i64 = c.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
            let failure = super::super::schema(&c).unwrap_err();
            assert_eq!(
                failure.downcast_ref::<DeliveryError>().unwrap().code,
                ErrorCode::UnsupportedProtocol,
                "{mutation}"
            );
            assert_eq!(
                c.query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))?,
                before
            );
            assert_eq!(
                c.query_row("SELECT count(*) FROM __sys_stream_events", [], |row| row
                    .get::<_, i64>(0))?,
                2
            );
            assert_eq!(
                c.query_row("SELECT value FROM business", [], |row| row
                    .get::<_, String>(0))?,
                "retained"
            );
        }
        Ok(())
    }

    #[test]
    fn marked_retirement_provenance_is_required_before_base_schema_changes() -> anyhow::Result<()> {
        for (marker, version, table) in [
            ("__sys_stream_schema", 1, "__sys_stream_retirement"),
            (
                "__sys_event_catalog_schema",
                1,
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
            for version in [Some(0), Some(2), Some(999), None] {
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
