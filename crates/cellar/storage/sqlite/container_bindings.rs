// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reserved provider records; application SQL and deleteAll cannot forge or erase them.
use super::*;
use crate::application::containers::{
    BindingCheckpoint, BindingRecord, MAX_BINDINGS, MAX_BINDING_BYTES,
};

const SCHEMA: event_schema::Schema = event_schema::Schema {
    marker: "__sys_container_binding_schema",
    ddl: include_str!("container_bindings.sql"),
    required: &[(
        "__sys_container_bindings",
        "binding,revision,activation_epoch,record",
    )],
};

pub(crate) fn validate_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !name.is_empty()
            && name.len() <= 128
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid configured container binding name"
    );
    Ok(())
}

pub(super) fn check_schema(connection: &Connection) -> anyhow::Result<bool> {
    let present = SCHEMA.check(connection)?;
    if present {
        let count: usize =
            connection.query_row("SELECT count(*) FROM __sys_container_bindings", [], |row| {
                row.get(0)
            })?;
        event_schema::compatible(
            count <= MAX_BINDINGS,
            "provider binding count exceeds limit",
        )?;
        let invalid: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_container_bindings WHERE typeof(revision)!='integer' OR revision<=0 OR typeof(activation_epoch)!='integer' OR activation_epoch<0 OR typeof(record)!='text' OR length(CAST(record AS BLOB))>16384)", [], |row|row.get(0))?;
        event_schema::compatible(!invalid, "invalid retained provider binding fields")?;
        let mut statement = connection.prepare(
            "SELECT binding,revision,activation_epoch,record FROM __sys_container_bindings",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            validate_name(&row.get::<_, String>(0)?)?;
            let revision: u64 = row.get(1)?;
            let _: u64 = row.get(2)?;
            let encoded: String = row.get(3)?;
            event_schema::compatible(
                revision > 0 && encoded.len() <= MAX_BINDING_BYTES,
                "invalid retained provider binding",
            )?;
            serde_json::from_str::<BindingRecord>(&encoded)?.validate()?;
        }
    }
    Ok(present)
}

pub(super) fn restore_schema(connection: &Connection) -> anyhow::Result<()> {
    if check_schema(connection)? {
        SCHEMA.open(connection)?;
    }
    Ok(())
}

fn read(connection: &Connection, name: &str) -> anyhow::Result<Option<BindingCheckpoint>> {
    if !SCHEMA.check(connection)? {
        return Ok(None);
    }
    let row: Option<(u64,u64,String)> = connection.query_row("SELECT revision,activation_epoch,record FROM __sys_container_bindings WHERE binding=?1",[name],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?;
    row.map(|(revision, activation_epoch, encoded)| {
        anyhow::ensure!(
            revision > 0 && encoded.len() <= MAX_BINDING_BYTES,
            "invalid binding checkpoint"
        );
        let record: BindingRecord = serde_json::from_str(&encoded)?;
        record.validate()?;
        Ok(BindingCheckpoint {
            revision,
            activation_epoch,
            record,
        })
    })
    .transpose()
}

pub(crate) fn load(scope: &str, name: &str) -> anyhow::Result<Option<BindingCheckpoint>> {
    validate_name(name)?;
    require_sql_healthy(scope).map_err(anyhow::Error::msg)?;
    with(scope, |connection| {
        as_framework_sql(|| read(connection, name))
    })
    .ok_or_else(|| anyhow::anyhow!("cell storage is not local"))?
}

fn write(
    connection: &Connection,
    name: &str,
    expected: Option<u64>,
    activation_epoch: u64,
    record: BindingRecord,
) -> anyhow::Result<BindingCheckpoint> {
    validate_name(name)?;
    record.validate()?;
    let encoded = serde_json::to_string(&record)?;
    anyhow::ensure!(
        encoded.len() <= MAX_BINDING_BYTES,
        "binding record exceeds byte limit"
    );
    SCHEMA.open(connection)?;
    let prior = read(connection, name)?;
    anyhow::ensure!(
        prior.as_ref().map(|row| row.revision) == expected,
        "provider binding revision conflict"
    );
    record.follows(prior.as_ref().map(|row| &row.record))?;
    let revision = expected
        .unwrap_or(0)
        .checked_add(1)
        .filter(|revision| *revision <= i64::MAX as u64)
        .ok_or_else(|| anyhow::anyhow!("binding revision exhausted"))?;
    let epoch = i64::try_from(activation_epoch)?;
    if prior.is_none() {
        let count: usize =
            connection.query_row("SELECT count(*) FROM __sys_container_bindings", [], |row| {
                row.get(0)
            })?;
        anyhow::ensure!(count < MAX_BINDINGS, "provider binding count exceeds limit");
    }
    connection.execute("INSERT INTO __sys_container_bindings(binding,revision,activation_epoch,record) VALUES(?1,?2,?3,?4) ON CONFLICT(binding) DO UPDATE SET revision=excluded.revision,activation_epoch=excluded.activation_epoch,record=excluded.record",rusqlite::params![name,revision,epoch,encoded])?;
    Ok(BindingCheckpoint {
        revision,
        activation_epoch,
        record,
    })
}

fn transaction(
    connection: &Connection,
    name: &str,
    expected: Option<u64>,
    epoch: u64,
    record: BindingRecord,
) -> anyhow::Result<BindingCheckpoint> {
    as_framework_sql(|| {
        // A root native event can be in autocommit mode or an implicit batch.
        // SAVEPOINT starts a transaction in the first case and nests in the second.
        as_storage_transaction(|| connection.execute_batch("SAVEPOINT cellar_container_binding;"))?;
        let result = write(connection, name, expected, epoch, record);
        let cleanup = if result.is_err() && connection.is_autocommit() {
            // A SQLite failure may already have rolled back the transaction.
            // Preserve that original failure instead of masking it with a missing savepoint.
            Ok(())
        } else {
            as_storage_transaction(|| {
                if result.is_err() {
                    connection.execute_batch("ROLLBACK TO cellar_container_binding;")?;
                }
                connection.execute_batch("RELEASE cellar_container_binding;")
            })
        };
        match (result, cleanup) {
            (Ok(checkpoint), Ok(())) => Ok(checkpoint),
            (Ok(_), Err(cleanup)) => Err(cleanup.into()),
            (Err(original), Ok(())) => Err(original),
            (Err(original), Err(cleanup)) => {
                Err(original.context(format!("binding savepoint cleanup failed: {cleanup}")))
            }
        }
    })
}

pub(crate) fn compare_exchange(
    scope: &str,
    name: &str,
    expected: Option<u64>,
    epoch: u64,
    record: BindingRecord,
) -> anyhow::Result<BindingCheckpoint> {
    with_application_storage_write(scope, |connection| {
        transaction(connection, name, expected, epoch, record)
    })
}

#[cfg(test)]
#[path = "container_bindings_tests.rs"]
mod tests;
