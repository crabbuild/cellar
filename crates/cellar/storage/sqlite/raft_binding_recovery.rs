// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Recovery operates on committed identities, never by replaying a native frame.

use super::{as_framework_sql, raft_bindings::IntentWire};
use crate::application::consensus::{CellHistory, CommandFailure, Durability, GroupHistory};
use anyhow::{ensure, Result};
use cellar_logic::consensus::{GroupId, IncarnationId};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const CLAIM: &str = "__sys_raft_binding_claim";
pub(crate) const SETTLE: &str = "__sys_raft_binding_settle";
pub(crate) const MAX_METADATA: usize = 16 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Key {
    pub client: [u8; 16],
    pub sequence: u64,
    pub ordinal: u64,
    pub fingerprint: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Pending {
    pub key: Key,
    pub body: IntentWire,
    pub selected: Option<[u8; 16]>,
    pub digest: [u8; 32],
}

impl Pending {
    pub fn minimum(&self) -> Result<Durability> {
        durability(self.body.source_durability)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Settlement {
    pub key: Key,
    pub digest: [u8; 32],
    /// The target proved a different creation already owns this logical name.
    /// Only an unselected, provisional candidate counter can be discarded.
    pub candidate_lost: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct Origin {
    pub group: GroupHistory,
    pub term: u64,
}

pub(crate) fn durability(value: u8) -> Result<Durability> {
    match value {
        1 => Ok(Durability::Quorum),
        2 => Ok(Durability::S3),
        _ => anyhow::bail!(CommandFailure::CorruptState),
    }
}

fn present(connection: &Connection) -> Result<bool> {
    Ok(connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_raft_binding_intents')", [], |row| row.get(0))?)
}

fn read(connection: &Connection, key: &Key) -> Result<Option<(Pending, u8)>> {
    let row = connection.query_row(
        "SELECT format,fingerprint,CASE WHEN typeof(body)='blob' AND length(body)<=?4 THEN body ELSE NULL END,phase,selected FROM __sys_raft_binding_intents WHERE parent_client=?1 AND parent_sequence=?2 AND ordinal=?3",
        params![key.client.as_slice(),key.sequence.to_be_bytes().as_slice(),key.ordinal.to_be_bytes().as_slice(),MAX_METADATA],
        |row| Ok((row.get::<_,u8>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,u8>(3)?,row.get::<_,Option<Vec<u8>>>(4)?)),
    ).optional()?;
    let Some((format, fingerprint, bytes, phase, selected)) = row else {
        return Ok(None);
    };
    ensure!(
        format == 1 && fingerprint.as_slice() == key.fingerprint && phase <= 4,
        CommandFailure::CorruptState
    );
    let body: IntentWire = serde_json::from_slice(&bytes)?;
    ensure!(
        body.lane < 64
            && body.serial != 0
            && body.child_sequence != 0
            && body.child_client.starts_with(b"RCB1")
            && body.cell.len() <= 1024
            && body.operation.len() <= 128,
        CommandFailure::CorruptState
    );
    GroupId::new(body.source_group)?;
    IncarnationId::new(body.source_group_incarnation)?;
    IncarnationId::new(body.candidate)?;
    durability(body.durability)?;
    ensure!(
        body.schema != [0; 32] && body.contract != [0; 32],
        CommandFailure::CorruptState
    );
    let selected = selected
        .map(|bytes| {
            bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!(CommandFailure::CorruptState))
        })
        .transpose()?;
    let mut hash = Sha256::new();
    hash.update(bytes);
    hash.update([u8::from(selected.is_some())]);
    if let Some(selected) = selected {
        hash.update(selected);
    }
    Ok(Some((
        Pending {
            key: key.clone(),
            body,
            selected,
            digest: hash.finalize().into(),
        },
        phase,
    )))
}

/// One indexed due row; payload arguments are never allocated by discovery.
pub(crate) fn due(connection: &Connection, now: u64) -> Result<Option<Pending>> {
    as_framework_sql(|| {
        if !present(connection)? {
            return Ok(None);
        }
        let key=connection.query_row(
            "SELECT parent_client,parent_sequence,ordinal,fingerprint FROM __sys_raft_binding_intents WHERE next_at<=?1 ORDER BY next_at,parent_client,parent_sequence,ordinal LIMIT 1",
            [now.min(i64::MAX as u64)], |row| Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,Vec<u8>>(3)?)),
        ).optional()?;
        let Some((client, sequence, ordinal, fingerprint)) = key else {
            return Ok(None);
        };
        let key = Key {
            client: client
                .try_into()
                .map_err(|_| anyhow::anyhow!(CommandFailure::CorruptState))?,
            sequence: u64::from_be_bytes(
                sequence
                    .try_into()
                    .map_err(|_| anyhow::anyhow!(CommandFailure::CorruptState))?,
            ),
            ordinal: u64::from_be_bytes(
                ordinal
                    .try_into()
                    .map_err(|_| anyhow::anyhow!(CommandFailure::CorruptState))?,
            ),
            fingerprint: fingerprint
                .try_into()
                .map_err(|_| anyhow::anyhow!(CommandFailure::CorruptState))?,
        };
        Ok(read(connection, &key)?.map(|(pending, _)| pending))
    })
}

fn source(
    pending: &Pending,
    history: &CellHistory,
    origin: Origin,
    policy: Durability,
) -> Result<()> {
    let body = &pending.body;
    ensure!(
        body.epoch == history.incarnation.bytes()
            && body.source_cluster == history.cluster.bytes()
            && body.source_namespace == history.namespace.bytes()
            && origin.group.same_namespace(history),
        CommandFailure::WrongIncarnation
    );
    ensure!(
        body.source_term != 0 && origin.term != 0,
        CommandFailure::CorruptState
    );
    if body.source_group == origin.group.group.bytes()
        && body.source_group_incarnation == origin.group.incarnation.bytes()
    {
        ensure!(origin.term >= body.source_term, CommandFailure::Unavailable);
    }
    ensure!(
        policy >= pending.minimum()?,
        CommandFailure::UnsupportedCapability
    );
    Ok(())
}

/// Called by a private system command only after the SQL worker has excluded
/// every same-cell live/parked frame. The resulting receipt must be proven at
/// the original source minimum before target retirement can begin.
pub(crate) fn claim(
    connection: &Connection,
    history: &CellHistory,
    origin: Origin,
    policy: Durability,
    now: u64,
    key: &Key,
) -> Result<Option<Pending>> {
    as_framework_sql(|| {
        ensure!(!connection.is_autocommit(), CommandFailure::CorruptState);
        let Some((pending, phase)) = read(connection, key)? else {
            return Ok(None);
        };
        source(&pending, history, origin, policy)?;
        ensure!(
            now >= pending.body.deadline_unix_ms,
            CommandFailure::Unavailable
        );
        if phase == 3 {
            // A completed lane may already be reused. Delete only this orphaned
            // resolved row, never the lane's current reservation.
            remove(connection, key)?;
            return Ok(None);
        }
        let changed=connection.execute("UPDATE __sys_raft_binding_intents SET phase=4 WHERE parent_client=?1 AND parent_sequence=?2 AND ordinal=?3 AND fingerprint=?4 AND format=1 AND phase!=3",params![key.client.as_slice(),key.sequence.to_be_bytes().as_slice(),key.ordinal.to_be_bytes().as_slice(),key.fingerprint.as_slice()])?;
        ensure!(changed == 1, CommandFailure::CorruptState);
        Ok(Some(pending))
    })
}

pub(crate) fn settle(
    connection: &Connection,
    history: &CellHistory,
    origin: Origin,
    policy: Durability,
    settlement: &Settlement,
) -> Result<()> {
    as_framework_sql(|| {
        ensure!(!connection.is_autocommit(), CommandFailure::CorruptState);
        let Some((pending, phase)) = read(connection, &settlement.key)? else {
            return Ok(());
        };
        source(&pending, history, origin, policy)?;
        ensure!(
            phase == 4 && pending.digest == settlement.digest,
            CommandFailure::CommandConflict
        );
        let body = &pending.body;
        if settlement.candidate_lost {
            ensure!(
                body.enroll && pending.selected.is_none() && body.child_sequence == 1,
                CommandFailure::CommandConflict
            );
            let count=connection.execute("DELETE FROM __sys_raft_binding_sequences WHERE epoch=?1 AND lane=?2 AND cluster=?3 AND namespace=?4 AND cell=?5 AND incarnation=?6 AND sequence=?7",params![body.epoch.as_slice(),body.lane,body.cluster.as_slice(),body.namespace.as_slice(),body.cell,body.candidate.as_slice(),1_u64.to_be_bytes().as_slice()])?;
            ensure!(count == 1, CommandFailure::CorruptState);
        }
        let count=connection.execute("UPDATE __sys_raft_binding_lanes SET busy=0 WHERE epoch=?1 AND lane=?2 AND sequence=?3 AND busy=1",params![body.epoch.as_slice(),body.lane,body.serial.to_be_bytes().as_slice()])?;
        ensure!(count == 1, CommandFailure::CorruptState);
        remove(connection, &settlement.key)
    })
}

fn remove(connection: &Connection, key: &Key) -> Result<()> {
    let count=connection.execute("DELETE FROM __sys_raft_binding_intents WHERE parent_client=?1 AND parent_sequence=?2 AND ordinal=?3 AND fingerprint=?4 AND format=1",params![key.client.as_slice(),key.sequence.to_be_bytes().as_slice(),key.ordinal.to_be_bytes().as_slice(),key.fingerprint.as_slice()])?;
    ensure!(count == 1, CommandFailure::CorruptState);
    Ok(())
}

#[cfg(test)]
#[path = "raft_binding_recovery_tests.rs"]
mod tests;
