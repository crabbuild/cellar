// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An erasure request is committed in the cell before the external barrier.
use super::*;
use crate::cell_runtime::retirement::{Evidence, Record};
use anyhow::{ensure, Result};

const SCHEMA: event_schema::Schema = event_schema::Schema {
    marker: "__sys_sandbox_erasure_schema",
    ddl: "CREATE TABLE __sys_sandbox_erasure(singleton INTEGER PRIMARY KEY CHECK(singleton=1),record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=16384));",
    required: &[("__sys_sandbox_erasure", "singleton,record")],
};

pub(super) fn read(connection: &Connection) -> Result<Option<Record>> {
    if !SCHEMA.check(connection)? {
        return Ok(None);
    }
    let encoded: Option<String> = connection.query_row("SELECT CASE WHEN typeof(record)='text' AND length(CAST(record AS BLOB))<=16384 THEN record END FROM __sys_sandbox_erasure WHERE singleton=1", [], |row| row.get(0))?;
    let record: Record = serde_json::from_str(
        &encoded.ok_or_else(|| anyhow::anyhow!("invalid sandbox erasure request"))?,
    )?;
    record.validate()?;
    Ok(Some(record))
}

pub(super) fn check_schema(connection: &Connection) -> Result<()> {
    read(connection)?;
    Ok(())
}

pub(crate) fn load(scope: &str) -> Result<Option<Record>> {
    require_sql_healthy(scope).map_err(anyhow::Error::msg)?;
    with(scope, |connection| {
        as_framework_sql(|| {
            let record = read(connection)?;
            ensure!(
                record
                    .as_ref()
                    .is_none_or(|record| record.evidence.scope() == scope),
                "erasure request belongs to another cell"
            );
            Ok(record)
        })
    })
    .ok_or_else(|| anyhow::anyhow!("erasure storage is not local"))?
}

pub(crate) fn prepare(scope: &str) -> Result<Record> {
    use crate::application::containers::SandboxParentState;
    use crate::runtime::sandbox_execution::Phase;
    // Every independently owned resource must have positive terminal evidence.
    ensure!(
        super::container_bindings::list(scope)?
            .iter()
            .all(|(_, checkpoint)| checkpoint.record.is_retired()),
        "provider cleanup remains pending"
    );
    let evidence = if let Some(child) = super::sandbox_children::load(scope)? {
        ensure!(
            child.phase == Phase::Deleted,
            "sandbox child is not deleted"
        );
        Evidence::child(&child.identity)?
    } else {
        let parent = super::sandbox_catalog::parent(scope)?
            .ok_or_else(|| anyhow::anyhow!("sandbox parent missing"))?;
        ensure!(
            parent.state == SandboxParentState::Deleted,
            "sandbox parent cleanup remains pending"
        );
        Evidence::Parent {
            scope: scope.into(),
            incarnation: parent.incarnation,
        }
    };
    let record = Record::new(evidence)?;
    with_application_storage_write(scope, |connection| {
        super::sandbox_catalog::transaction(connection, |connection| {
            ensure!(
                !connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM __sys_cell_facet_names LIMIT 1)",
                    [],
                    |row| row.get::<_, bool>(0)
                )?,
                "remove retained facets before erasing their root"
            );
            let source: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_event_source')", [], |row| row.get(0))?;
            if source {
                let closed = connection
                    .query_row(
                        "SELECT state FROM __sys_outbox_source_lifecycle WHERE singleton=1",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                ensure!(
                    closed.as_deref() == Some("deleted"),
                    "retire the event source before erasing its cell"
                );
            }
            ensure!(!connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_work WHERE reason!='sandbox-cleanup' AND at_ms IS NOT NULL LIMIT 1)", [], |row| row.get::<_, bool>(0))?, "other framework cleanup remains pending");
            let broker: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name IN ('__sys_stream_schema','__sys_event_catalog_schema'))", [], |row| row.get(0))?;
            ensure!(
                !broker,
                "event resources require their separate retirement protocol"
            );
            let raft: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name GLOB '__sys_raft_*')", [], |row| row.get(0))?;
            ensure!(
                !raft,
                "consensus cells require their archive retirement protocol"
            );
            if let Some(prior) = read(connection)? {
                ensure!(prior == record, "sandbox erasure identity changed");
            } else {
                SCHEMA.open(connection)?;
                connection.execute(
                    "INSERT INTO __sys_sandbox_erasure VALUES(1,?1)",
                    [serde_json::to_string(&record)?],
                )?;
            }
            super::work::set_in(
                connection,
                scope,
                cellar_logic::work::Reason::SandboxCleanup,
                0,
            )?;
            Ok(record)
        })
    })
}
