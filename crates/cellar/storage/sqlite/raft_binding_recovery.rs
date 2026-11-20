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
pub(crate) const CONFIRM: &str = "__sys_raft_binding_confirm";
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
    pub confirmation: Option<ReceiptReference>,
}

/// An exact private maintenance receipt, never a target business retry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReceiptReference {
    pub client: [u8; 16],
    pub sequence: u64,
    pub retry_until_ms: u64,
    pub durability: u8,
    pub candidate_lost: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Confirmation {
    pub key: Key,
    pub digest: [u8; 32],
    pub receipt: ReceiptReference,
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
        "SELECT format,fingerprint,CASE WHEN typeof(body)='blob' AND length(body)<=?4 THEN body ELSE NULL END,phase,selected,CASE WHEN typeof(settlement)='blob' AND length(settlement)<=1024 THEN settlement ELSE NULL END,settlement_client,settlement_sequence FROM __sys_raft_binding_intents WHERE parent_client=?1 AND parent_sequence=?2 AND ordinal=?3",
        params![key.client.as_slice(),key.sequence.to_be_bytes().as_slice(),key.ordinal.to_be_bytes().as_slice(),MAX_METADATA],
        |row| Ok((row.get::<_,u8>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,u8>(3)?,row.get::<_,Option<Vec<u8>>>(4)?,row.get::<_,Option<Vec<u8>>>(5)?,row.get::<_,Option<Vec<u8>>>(6)?,row.get::<_,Option<Vec<u8>>>(7)?)),
    ).optional()?;
    let Some((
        format,
        fingerprint,
        bytes,
        phase,
        selected,
        confirmation,
        pinned_client,
        pinned_sequence,
    )) = row
    else {
        return Ok(None);
    };
    ensure!(
        format == 1 && fingerprint.as_slice() == key.fingerprint && phase <= 5,
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
    let confirmation = match (phase, confirmation, pinned_client, pinned_sequence) {
        (5, Some(bytes), Some(client), Some(sequence)) => {
            let reference: ReceiptReference = serde_json::from_slice(&bytes)?;
            cellar_logic::consensus::ClientId::new(reference.client)?;
            ensure!(
                durability(reference.durability)? >= durability(body.source_durability)?,
                CommandFailure::CorruptState
            );
            ensure!(
                reference.sequence != 0
                    && reference.retry_until_ms != 0
                    && client.as_slice() == reference.client
                    && sequence.as_slice() == reference.sequence.to_be_bytes(),
                CommandFailure::CorruptState
            );
            Some(reference)
        }
        (5, _, _, _) => anyhow::bail!(CommandFailure::CorruptState),
        (_, None, None, None) => None,
        _ => anyhow::bail!(CommandFailure::CorruptState),
    };
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
            confirmation,
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
        if phase == 5 {
            // Only receipt confirmation may retire this row. A stale claim
            // must not erase the durable rediscovery obligation.
            return Ok(None);
        }
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
        let changed=connection.execute("UPDATE __sys_raft_binding_intents SET phase=4 WHERE parent_client=?1 AND parent_sequence=?2 AND ordinal=?3 AND fingerprint=?4 AND format=1 AND phase IN(0,1,2,4)",params![key.client.as_slice(),key.sequence.to_be_bytes().as_slice(),key.ordinal.to_be_bytes().as_slice(),key.fingerprint.as_slice()])?;
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
    request: &crate::raft::execution::CommandRequest,
) -> Result<()> {
    as_framework_sql(|| {
        ensure!(!connection.is_autocommit(), CommandFailure::CorruptState);
        let Some((pending, phase)) = read(connection, &settlement.key)? else {
            return Ok(());
        };
        source(&pending, history, origin, policy)?;
        ensure!(
            matches!(phase, 4 | 5) && pending.digest == settlement.digest,
            CommandFailure::CommandConflict
        );
        ensure!(
            request.operation == SETTLE && request.durability >= policy,
            CommandFailure::CommandConflict
        );
        let reference = ReceiptReference {
            client: request.id.client.bytes(),
            sequence: request.id.sequence(),
            retry_until_ms: request.retry_until_ms,
            durability: if request.durability == Durability::S3 {
                2
            } else {
                1
            },
            candidate_lost: settlement.candidate_lost,
        };
        if phase == 5 {
            ensure!(
                pending.confirmation.as_ref() == Some(&reference),
                CommandFailure::CommandConflict
            );
            return Ok(());
        }
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
        // Quorum commitment can precede the required S3 response. Retain a
        // bounded, payload-free confirmation obligation across that loss.
        let encoded = serde_json::to_vec(&reference)?;
        ensure!(encoded.len() <= 1024, CommandFailure::CorruptState);
        let count=connection.execute("UPDATE __sys_raft_binding_intents SET phase=5,args=X'',settlement=?5,settlement_client=?6,settlement_sequence=?7 WHERE parent_client=?1 AND parent_sequence=?2 AND ordinal=?3 AND fingerprint=?4 AND phase=4",
            params![settlement.key.client.as_slice(),settlement.key.sequence.to_be_bytes().as_slice(),settlement.key.ordinal.to_be_bytes().as_slice(),settlement.key.fingerprint.as_slice(),encoded,reference.client.as_slice(),reference.sequence.to_be_bytes().as_slice()])?;
        ensure!(count == 1, CommandFailure::CorruptState);
        Ok(())
    })
}

/// The owning adapter must independently prove the referenced receipt first.
/// Cleanup never changes a lane, which may already have a newer reservation.
pub(crate) fn confirm(
    connection: &Connection,
    history: &CellHistory,
    origin: Origin,
    policy: Durability,
    confirmation: &Confirmation,
) -> Result<()> {
    as_framework_sql(|| {
        ensure!(!connection.is_autocommit(), CommandFailure::CorruptState);
        let Some((pending, phase)) = read(connection, &confirmation.key)? else {
            return Ok(());
        };
        source(&pending, history, origin, policy)?;
        ensure!(
            phase == 5
                && pending.digest == confirmation.digest
                && pending.confirmation.as_ref() == Some(&confirmation.receipt),
            CommandFailure::CommandConflict
        );
        remove(connection, &confirmation.key)
    })
}

/// Confirmation owns its exact receipt even after the ordinary retry horizon.
/// The existing scheduler releases this pin only after real output proof.
pub(crate) fn prunable(
    connection: &Connection,
    client: cellar_logic::consensus::ClientId,
    through: u64,
) -> Result<bool> {
    as_framework_sql(|| {
        if !present(connection)? {
            return Ok(true);
        }
        Ok(!connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_raft_binding_intents WHERE phase=5 AND settlement_client=?1 AND settlement_sequence<=?2)", params![client.bytes().as_slice(),through.to_be_bytes().as_slice()], |row| row.get::<_,bool>(0))?)
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
