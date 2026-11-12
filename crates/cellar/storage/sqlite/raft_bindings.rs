// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Framework-owned binding state in the private Raft transaction.

use super::*;
use crate::application::cell_bindings::{Intent, Route, Source};
use crate::application::consensus::{CellHistory, CommandId};
use anyhow::{ensure, Result};
use serde::Serialize;

pub(crate) fn install(connection: &Connection) -> Result<()> {
    as_framework_sql(|| {
        connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS __sys_raft_binding_intents(
          parent_client BLOB NOT NULL,parent_sequence BLOB NOT NULL,ordinal BLOB NOT NULL,
          format INTEGER NOT NULL CHECK(format=1),fingerprint BLOB NOT NULL,body BLOB NOT NULL,args BLOB NOT NULL,
          phase INTEGER NOT NULL CHECK(phase BETWEEN 0 AND 3),selected BLOB,result_digest BLOB,
          PRIMARY KEY(parent_client,parent_sequence,ordinal)) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS __sys_raft_binding_lanes(
          epoch BLOB NOT NULL,lane INTEGER NOT NULL CHECK(lane BETWEEN 0 AND 63),
          sequence BLOB NOT NULL,busy INTEGER NOT NULL CHECK(busy IN(0,1)),
          PRIMARY KEY(epoch,lane)) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS __sys_raft_binding_sequences(
          epoch BLOB NOT NULL,lane INTEGER NOT NULL,cluster BLOB NOT NULL,namespace BLOB NOT NULL,
          cell TEXT NOT NULL,incarnation BLOB NOT NULL,sequence BLOB NOT NULL,
          PRIMARY KEY(epoch,lane,cluster,namespace,cell,incarnation)) WITHOUT ROWID;",
    )
    })?;
    Ok(())
}

#[derive(Serialize)]
struct IntentWire<'a> {
    epoch: [u8; 16],
    lane: u64,
    serial: u64,
    source_durability: u8,
    deadline_unix_ms: u64,
    cluster: [u8; 16],
    namespace: [u8; 16],
    cell: &'a str,
    schema: [u8; 32],
    operation: &'a str,
    contract: [u8; 32],
    durability: u8,
    enroll: bool,
    candidate: [u8; 16],
    creation: [u8; 16],
    child_client: [u8; 16],
    child_sequence: u64,
    retry_until_ms: u64,
}

/// Prepare the durable intent and owned task admission in one synchronous
/// savepoint. The callback may enqueue work, never poll or dispatch it.
pub(crate) fn prepare<T>(
    source: &Source,
    route: Route,
    input: Vec<u8>,
    ordinal: u64,
    schedule: impl FnOnce(Intent) -> Result<T>,
) -> Result<T> {
    command::with(|connection| as_framework_sql(|| {
        control(connection, "SAVEPOINT __sys_raft_binding_prepare")?;
        let result = (|| {
        let epoch = source.history.incarnation.bytes();
        let mut available = None;
        for lane in 0_u64..64 {
            let prior: Option<(Vec<u8>, bool)> = connection.query_row(
                "SELECT sequence,busy FROM __sys_raft_binding_lanes WHERE epoch=?1 AND lane=?2",
                rusqlite::params![epoch.as_slice(), lane], |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional()?;
            let sequence = match prior {
                Some((_, true)) => continue,
                Some((bytes, false)) => {
                    let value = u64::from_be_bytes(bytes.as_slice().try_into()?);
                    let Some(value) = value.checked_add(1) else { continue; };
                    value
                }
                None => 1,
            };
            available = Some((lane, sequence));
            break;
        }
        let (lane, sequence) = available.ok_or(crate::application::consensus::CommandFailure::Overloaded)?;
        let mut intent = Intent::prepare(source, route, input, ordinal, lane, sequence)?;
        let target_sequence = reserve_sequence(connection, &intent, &intent.candidate)?;
        intent.child = CommandId::new(intent.child.client, target_sequence)?;
        connection.execute(
            "INSERT INTO __sys_raft_binding_lanes VALUES(?1,?2,?3,1) ON CONFLICT(epoch,lane) DO UPDATE SET sequence=excluded.sequence,busy=1 WHERE busy=0",
            rusqlite::params![epoch.as_slice(), lane, sequence.to_be_bytes().as_slice()],
        )?;
        let body = wire(&intent)?;
            connection.execute(
                "INSERT INTO __sys_raft_binding_intents VALUES(?1,?2,?3,1,?4,?5,?6,0,NULL,NULL)",
                rusqlite::params![intent.parent.client.bytes().as_slice(), intent.parent.sequence().to_be_bytes().as_slice(),
                    intent.ordinal.to_be_bytes().as_slice(), intent.fingerprint.as_slice(), body, intent.input.as_slice()],
            )?;
            schedule(intent)
        })();
        if result.is_ok() {
            if let Err(error) = control(connection, "RELEASE __sys_raft_binding_prepare") {
                command::poison(format!("binding intent release failed: {error}"));
                return Err(error.into());
            }
        } else if let Err(error) = control(connection, "ROLLBACK TO __sys_raft_binding_prepare; RELEASE __sys_raft_binding_prepare") {
            command::poison(format!("binding intent rollback failed: {error}"));
            return Err(error.into());
        }
        result
    })).ok_or_else(|| anyhow::anyhow!("cell binding has no active command"))?
}

pub(crate) fn select(intent: &mut Intent, history: &CellHistory) -> Result<()> {
    ensure!(
        history.cluster == intent.route.cluster
            && history.namespace == intent.route.namespace
            && history.cell == intent.route.cell,
        "cell binding resolution scope differs"
    );
    let prior = intent.child;
    let result = command::with(|connection| as_framework_sql(|| {
        control(connection, "SAVEPOINT __sys_raft_binding_select")?;
        let result = (|| {
            let sequence = reserve_sequence(connection, intent, history)?;
            intent.child = CommandId::new(intent.child.client, sequence)?;
            let changed = connection.execute(
                "UPDATE __sys_raft_binding_intents SET selected=?5,phase=2,body=?6 WHERE parent_client=?1 AND parent_sequence=?2 AND ordinal=?3 AND fingerprint=?4 AND format=1 AND phase=0",
                rusqlite::params![intent.parent.client.bytes().as_slice(), intent.parent.sequence().to_be_bytes().as_slice(),
                    intent.ordinal.to_be_bytes().as_slice(), intent.fingerprint.as_slice(), history.incarnation.bytes().as_slice(), wire(intent)?],
            )?;
            ensure!(changed == 1, "cell binding selection is stale");
            // The original candidate never received a business command. Its
            // provisional counter must not accumulate one row per failed creator.
            if intent.enroll && intent.candidate != *history && prior.sequence() == 1 {
                connection.execute(
                    "DELETE FROM __sys_raft_binding_sequences WHERE epoch=?1 AND lane=?2 AND cluster=?3 AND namespace=?4 AND cell=?5 AND incarnation=?6 AND sequence=?7",
                    rusqlite::params![intent.epoch.as_slice(), intent.lane, intent.route.cluster.bytes().as_slice(), intent.route.namespace.bytes().as_slice(),
                        intent.route.cell.as_str(), intent.candidate.incarnation.bytes().as_slice(), prior.sequence().to_be_bytes().as_slice()],
                )?;
            }
            Ok(())
        })();
        if result.is_ok() {
            if let Err(error) = control(connection, "RELEASE __sys_raft_binding_select") {
                command::poison(format!("binding selection release failed: {error}"));
                return Err(error.into());
            }
        } else if let Err(error) = control(connection, "ROLLBACK TO __sys_raft_binding_select; RELEASE __sys_raft_binding_select") {
            command::poison(format!("binding selection rollback failed: {error}"));
            return Err(error.into());
        }
        result
    })).ok_or_else(|| anyhow::anyhow!("cell binding has no active command"))?;
    if result.is_err() {
        intent.child = prior;
    }
    result
}

/// The target did not admit the sequence. It cannot be reused for different
/// input or skipped by the next request, so retain both intent and busy lane.
/// Reconciliation must seal that target sequence before physical retirement.
pub(crate) fn refuse(intent: &Intent, history: &CellHistory) -> Result<()> {
    command::with(|connection| as_framework_sql(|| {
        let changed = connection.execute(
            "UPDATE __sys_raft_binding_intents SET phase=1,selected=?5 WHERE parent_client=?1 AND parent_sequence=?2 AND ordinal=?3 AND fingerprint=?4 AND format=1 AND ((phase=2 AND selected=?5) OR (phase=0 AND selected IS NULL AND ?6))",
            rusqlite::params![intent.parent.client.bytes().as_slice(), intent.parent.sequence().to_be_bytes().as_slice(),
                intent.ordinal.to_be_bytes().as_slice(), intent.fingerprint.as_slice(), history.incarnation.bytes().as_slice(), history == &intent.candidate],
        )?;
        ensure!(changed == 1, "cell binding refusal is stale");
        Ok(())
    })).ok_or_else(|| anyhow::anyhow!("cell binding has no active command"))?
}

pub(crate) fn complete(intent: &Intent, history: &CellHistory, result: &[u8]) -> Result<()> {
    use sha2::{Digest, Sha256};
    let digest: [u8; 32] = Sha256::digest(result).into();
    command::with(|connection| as_framework_sql(|| {
        control(connection, "SAVEPOINT __sys_raft_binding_complete")?;
        let result = (|| {
        let changed = connection.execute(
            "UPDATE __sys_raft_binding_intents SET phase=3,result_digest=?6,selected=?5 WHERE parent_client=?1 AND parent_sequence=?2 AND ordinal=?3 AND fingerprint=?4 AND format=1 AND ((phase=2 AND selected=?5) OR (phase=0 AND selected IS NULL AND ?7))",
            rusqlite::params![intent.parent.client.bytes().as_slice(), intent.parent.sequence().to_be_bytes().as_slice(),
                intent.ordinal.to_be_bytes().as_slice(), intent.fingerprint.as_slice(), history.incarnation.bytes().as_slice(), digest.as_slice(), history == &intent.candidate],
        )?;
        ensure!(changed == 1, "cell binding completion is stale");
        let released = connection.execute(
            "UPDATE __sys_raft_binding_lanes SET busy=0 WHERE epoch=?1 AND lane=?2 AND sequence=?3 AND busy=1",
            rusqlite::params![intent.epoch.as_slice(), intent.lane, intent.serial.to_be_bytes().as_slice()],
        )?;
        ensure!(released == 1, "cell binding lane is stale");
        Ok(())
        })();
        if result.is_ok() {
            if let Err(error) = control(connection, "RELEASE __sys_raft_binding_complete") {
                command::poison(format!("binding completion release failed: {error}"));
                return Err(error.into());
            }
        } else if let Err(error) = control(connection, "ROLLBACK TO __sys_raft_binding_complete; RELEASE __sys_raft_binding_complete") {
            command::poison(format!("binding completion rollback failed: {error}"));
            return Err(error.into());
        }
        result
    })).ok_or_else(|| anyhow::anyhow!("cell binding has no active command"))?
}

pub(crate) fn clear_resolved(connection: &Connection, parent: CommandId) -> Result<()> {
    as_framework_sql(|| {
        if present(connection)? {
            connection.execute("DELETE FROM __sys_raft_binding_intents WHERE parent_client=?1 AND parent_sequence=?2 AND phase=3 AND format=1",
                rusqlite::params![parent.client.bytes().as_slice(), parent.sequence().to_be_bytes().as_slice()])?;
        }
        Ok(())
    })
}

pub(crate) fn check_clear(connection: &Connection) -> Result<()> {
    as_framework_sql(|| {
        if present(connection)? {
            let pending: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_raft_binding_intents WHERE phase!=3 OR format!=1)", [], |row| row.get(0))?;
            ensure!(
                !pending,
                crate::application::consensus::CommandFailure::Unavailable
            );
        }
        Ok(())
    })
}

fn present(connection: &Connection) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='__sys_raft_binding_intents')",
        [],
        |row| row.get(0),
    )
}

pub(crate) fn cell_name(connection: &Connection, incoming: Option<&str>) -> Result<Option<String>> {
    as_framework_sql(|| {
        let present: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='__sys_raft_cell_name')",
            [],
            |row| row.get(0),
        )?;
        let prior: Option<(u32, String)> = if present {
            connection
                .query_row(
                    "SELECT format,name FROM __sys_raft_cell_name WHERE id=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
        } else {
            None
        };
        if let Some((format, name)) = prior {
            ensure!(
                format == 1 && name.len() <= 1024,
                "invalid Raft cell name state"
            );
            ensure!(
                incoming.is_none_or(|value| value == name),
                "Raft cell name conflict"
            );
            return Ok(Some(name));
        }
        let Some(name) = incoming else {
            return Ok(None);
        };
        ensure!(name.len() <= 1024, "cell routing name exceeds its bound");
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS __sys_raft_cell_name(id INTEGER PRIMARY KEY CHECK(id=1),format INTEGER NOT NULL CHECK(format=1),name TEXT NOT NULL)",
        )?;
        connection.execute("INSERT INTO __sys_raft_cell_name VALUES(1,1,?1)", [name])?;
        Ok(Some(name.to_owned()))
    })
}

// The authorizer grants transaction control independently from access to the
// framework tables. Only these fixed savepoint statements receive that grant.
fn control(connection: &Connection, statement: &'static str) -> rusqlite::Result<()> {
    as_storage_transaction(|| connection.execute_batch(statement))
}

const MAX_DESTINATION_LANES: u64 = 65_536;

fn reserve_sequence(
    connection: &Connection,
    intent: &Intent,
    history: &CellHistory,
) -> Result<u64> {
    let prior: Option<Vec<u8>> = connection.query_row(
        "SELECT sequence FROM __sys_raft_binding_sequences WHERE epoch=?1 AND lane=?2 AND cluster=?3 AND namespace=?4 AND cell=?5 AND incarnation=?6",
        rusqlite::params![intent.epoch.as_slice(), intent.lane, history.cluster.bytes().as_slice(), history.namespace.bytes().as_slice(), history.cell.as_str(), history.incarnation.bytes().as_slice()],
        |row| row.get(0),
    ).optional()?;
    let sequence = match prior {
        Some(bytes) => u64::from_be_bytes(bytes.as_slice().try_into()?)
            .checked_add(1)
            .ok_or(crate::application::consensus::CommandFailure::Overloaded)?,
        None => {
            let count: u64 = connection.query_row(
                "SELECT COUNT(*) FROM __sys_raft_binding_sequences",
                [],
                |row| row.get(0),
            )?;
            ensure!(
                count < MAX_DESTINATION_LANES,
                crate::application::consensus::CommandFailure::Overloaded
            );
            1
        }
    };
    connection.execute(
        "INSERT INTO __sys_raft_binding_sequences VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(epoch,lane,cluster,namespace,cell,incarnation) DO UPDATE SET sequence=excluded.sequence",
        rusqlite::params![intent.epoch.as_slice(), intent.lane, history.cluster.bytes().as_slice(), history.namespace.bytes().as_slice(), history.cell.as_str(), history.incarnation.bytes().as_slice(), sequence.to_be_bytes().as_slice()],
    )?;
    Ok(sequence)
}

fn wire(intent: &Intent) -> Result<Vec<u8>> {
    use crate::application::consensus::Durability;
    let durability = |value| match value {
        Durability::Quorum => 1,
        Durability::S3 => 2,
    };
    Ok(serde_json::to_vec(&IntentWire {
        epoch: intent.epoch,
        lane: intent.lane,
        serial: intent.serial,
        source_durability: durability(intent.source_durability),
        deadline_unix_ms: intent.deadline_unix_ms,
        cluster: intent.route.cluster.bytes(),
        namespace: intent.route.namespace.bytes(),
        cell: intent.route.cell.as_str(),
        schema: intent.route.schema,
        operation: &intent.route.contract.operation,
        contract: intent.route.contract.digest,
        durability: durability(intent.route.durability),
        enroll: intent.enroll,
        candidate: intent.candidate.incarnation.bytes(),
        creation: intent.creation.bytes(),
        child_client: intent.child.client.bytes(),
        child_sequence: intent.child.sequence(),
        retry_until_ms: intent.retry_until_ms,
    })?)
}
