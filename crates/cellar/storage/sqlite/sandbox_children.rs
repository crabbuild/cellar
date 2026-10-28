// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::runtime::sandbox_execution::Record;

const SCHEMA: event_schema::Schema = event_schema::Schema {
    marker: "__sys_sandbox_child_schema",
    ddl: "CREATE TABLE __sys_sandbox_child(singleton INTEGER PRIMARY KEY CHECK(singleton=1),record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=4096));",
    required: &[("__sys_sandbox_child","singleton,record")],
};

fn read(connection: &Connection) -> anyhow::Result<Option<Record>> {
    if !SCHEMA.check(connection)? {
        return Ok(None);
    }
    let encoded: Option<String> = connection.query_row(
        "SELECT CASE WHEN typeof(record)='text' AND length(CAST(record AS BLOB))<=4096 THEN record END FROM __sys_sandbox_child WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    let encoded = encoded.ok_or_else(|| anyhow::anyhow!("invalid sandbox child record"))?;
    let record: Record = serde_json::from_str(&encoded)?;
    record.identity.validate()?;
    if let Some(spec) = &record.container_spec {
        anyhow::ensure!(
            record
                .identity
                .execution_scope
                .split_once(':')
                .is_some_and(|(class, _)| class == spec.class_name),
            "sandbox cleanup specification belongs to another class"
        );
        crate::container::validate_exposed_ports(&spec.exposed_ports)?;
    }
    Ok(Some(record))
}

pub(super) fn check_schema(connection: &Connection) -> anyhow::Result<()> {
    read(connection)?;
    Ok(())
}

pub(crate) fn load(scope: &str) -> anyhow::Result<Option<Record>> {
    require_sql_healthy(scope).map_err(anyhow::Error::msg)?;
    with(scope, |connection| as_framework_sql(|| read(connection)))
        .ok_or_else(|| anyhow::anyhow!("sandbox child storage is not local"))?
}

pub(crate) fn save(scope: &str, record: &Record) -> anyhow::Result<()> {
    record.identity.validate()?;
    anyhow::ensure!(
        record.identity.execution_scope == scope,
        "sandbox child scope changed"
    );
    let encoded = serde_json::to_string(record)?;
    anyhow::ensure!(
        encoded.len() <= 4096,
        "sandbox child record exceeds byte limit"
    );
    with_application_storage_write(scope, |connection| {
        super::sandbox_catalog::transaction(connection, |connection| {
            save_in(connection, record, &encoded)
        })
    })
}

fn save_in(connection: &Connection, record: &Record, encoded: &str) -> anyhow::Result<()> {
    if let Some(prior) = read(connection)? {
        anyhow::ensure!(
            prior.identity == record.identity && prior.container_spec == record.container_spec,
            "sandbox child identity changed"
        );
        use crate::runtime::sandbox_execution::Phase;
        let allowed = prior.phase == record.phase
            || matches!(
                (prior.phase, record.phase),
                (
                    Phase::Unstarted,
                    Phase::Starting | Phase::Deleting | Phase::Deleted
                ) | (
                    Phase::Starting,
                    Phase::Unstarted | Phase::Ready | Phase::Deleting
                ) | (Phase::Ready, Phase::Deleting)
                    | (Phase::Deleting, Phase::Deleted)
            );
        anyhow::ensure!(
            allowed,
            "sandbox child transition cannot reopen or bypass creation intent"
        );
    }
    SCHEMA.open(connection)?;
    connection.execute("INSERT INTO __sys_sandbox_child VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET record=excluded.record",[encoded])?;
    Ok(())
}

#[cfg(test)]
#[path = "sandbox_children_tests.rs"]
mod tests;
