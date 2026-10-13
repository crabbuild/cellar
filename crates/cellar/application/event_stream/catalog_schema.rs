use super::{error::error, store, ErrorCode};
use crate::application::{Result, Storage};

pub(crate) fn open(db: &Storage) -> Result<()> {
    let marker = db.system_sql(
        "SELECT name FROM sqlite_schema WHERE name='__sys_event_catalog_schema' AND type='table'",
        &[],
    )?;
    let version = if marker.is_empty() {
        None
    } else {
        db.system_sql(
            "SELECT version FROM __sys_event_catalog_schema WHERE singleton=1",
            &[],
        )?
        .first()
        .map(|row| store::number(row, "version"))
        .transpose()?
    };
    if version.is_some_and(|version| !(1..=3).contains(&version)) {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "unsupported catalog schema",
        ));
    }
    let operator =
        super::schema::operator_migration(db, "__sys_event_catalog_receipts", version == Some(3))?;
    if version == Some(3) {
        return Ok(());
    }
    let tables = db.system_sql(
        "SELECT sql FROM sqlite_schema WHERE name='__sys_event_catalog' AND type='table'",
        &[],
    )?;
    let old = tables
        .first()
        .map(|row| store::text(row, "sql"))
        .transpose()?
        .is_some_and(|sql| !sql.contains("pending_retirement"));
    let schema = include_str!("catalog.sql");
    let ddl = if old {
        format!("ALTER TABLE __sys_event_catalog RENAME TO __sys_event_catalog_migration;
        {schema}
        INSERT INTO __sys_event_catalog(id,name,lifecycle,definition,pending_definition,pending_authority,operation,digest,operation_authority,retry_at_ms,attempts,last_error)
            SELECT id,name,lifecycle,definition,pending_definition,pending_authority,operation,digest,operation_authority,retry_at_ms,attempts,last_error FROM __sys_event_catalog_migration;
        DROP TABLE __sys_event_catalog_migration;
        {schema}")
    } else {
        schema.into()
    };
    let ddl=format!("{operator}\n{ddl}
        INSERT INTO __sys_event_catalog_receipts(operation,resource,kind,digest,principal,grant_id,result)
            SELECT operation,id,'configure',digest,json_extract(operation_authority,'$.principal'),json_extract(operation_authority,'$.grant'),
                CASE WHEN pending_definition IS NULL THEN json_object('state',lifecycle,'operation',operation,'resource',id,'last_error',last_error) ELSE NULL END
            FROM __sys_event_catalog WHERE pending_retirement IS NULL ON CONFLICT(operation) DO NOTHING;
        CREATE TABLE IF NOT EXISTS __sys_event_catalog_schema(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL CHECK(version>0));
        INSERT INTO __sys_event_catalog_schema VALUES(1,3) ON CONFLICT(singleton) DO UPDATE SET version=excluded.version;");
    db.system_sql_schema(&ddl)
}
