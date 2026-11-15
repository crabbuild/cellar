// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Trusted confirmed dependencies, distinct from unverified prepared locators.
//! Interrupted turns retain inputs until scheduler retirement. Current values
//! and retained archive images keep independent roots after receipt pruning.

use super::{as_framework_sql, command};
use crate::raft::cell_groups::CellGroupPayloadReference;
use anyhow::{ensure, Result};
use cellar_logic::{cell_group::CellHistory, consensus::CommandId};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const MAX_DEPENDENCIES: usize = 4096;
pub(crate) const RETIRE: &str = "__sys_raft_payload_retire";
const MAX_DESCRIPTOR_BYTES: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WireDependency", into = "WireDependency")]
pub(crate) struct Dependency {
    pub history: CellHistory,
    pub reference: CellGroupPayloadReference,
    pub confirmed: bool,
    pub guard: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDependency {
    format: u8,
    cluster: [u8; 16],
    namespace: [u8; 16],
    cell: String,
    incarnation: [u8; 16],
    reference: CellGroupPayloadReference,
    guard: [u8; 32],
}

impl From<Dependency> for WireDependency {
    fn from(value: Dependency) -> Self {
        Self {
            format: 1,
            cluster: value.history.cluster.bytes(),
            namespace: value.history.namespace.bytes(),
            cell: value.history.cell.as_str().into(),
            incarnation: value.history.incarnation.bytes(),
            reference: value.reference,
            guard: value.guard,
        }
    }
}

impl TryFrom<WireDependency> for Dependency {
    type Error = anyhow::Error;

    fn try_from(value: WireDependency) -> Result<Self> {
        use cellar_logic::consensus::{ClusterId, IncarnationId, NamespaceId};
        ensure!(
            value.format == 1 && value.cell.len() <= 1024,
            "payload dependency identity"
        );
        Ok(Self {
            history: CellHistory {
                cluster: ClusterId::new(value.cluster)?,
                namespace: NamespaceId::new(value.namespace)?,
                cell: cellar_logic::cell_group::CellId::new(value.cell)?,
                incarnation: IncarnationId::new(value.incarnation)?,
            },
            reference: value.reference,
            confirmed: false,
            guard: value.guard,
        })
    }
}

/// Journal before source-proven I/O, then confirm after full-byte verification
/// and writer resumption. Failure aborts the private transaction without omitting a root.
pub(crate) fn record(
    parent: CommandId,
    source: &CellHistory,
    history: CellHistory,
    reference: CellGroupPayloadReference,
    confirmed: bool,
) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash.update(b"cellar-payload-command-handoff-1\0");
    hash.update(source.cluster.bytes());
    hash.update(source.namespace.bytes());
    hash.update((source.cell.as_str().len() as u64).to_be_bytes());
    hash.update(source.cell.as_str());
    hash.update(source.incarnation.bytes());
    hash.update(parent.client.bytes());
    hash.update(parent.sequence().to_be_bytes());
    let guard = hash.finalize().into();
    command::with(|connection| {
        as_framework_sql(|| record_in(connection, parent, history, reference, confirmed, guard))
    })
    .ok_or_else(|| anyhow::anyhow!("payload dependency requires resumed writer"))??;
    Ok(guard)
}

fn record_in(
    connection: &Connection,
    parent: CommandId,
    history: CellHistory,
    reference: CellGroupPayloadReference,
    confirmed: bool,
    guard: [u8; 32],
) -> Result<()> {
    let bytes = serde_json::to_vec(&Dependency {
        history,
        reference,
        confirmed,
        guard,
    })?;
    ensure!(
        bytes.len() <= MAX_DESCRIPTOR_BYTES,
        "payload descriptor bound"
    );
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    connection.execute_batch("CREATE TABLE IF NOT EXISTS __sys_raft_payload_dependencies(
            parent_client BLOB NOT NULL CHECK(typeof(parent_client)='blob' AND length(parent_client)=16),
            parent_sequence BLOB NOT NULL CHECK(typeof(parent_sequence)='blob' AND length(parent_sequence)=8),
            digest BLOB NOT NULL CHECK(typeof(digest)='blob' AND length(digest)=32),
            body BLOB NOT NULL CHECK(typeof(body)='blob' AND length(body)<=2048),
            confirmed INTEGER NOT NULL CHECK(confirmed IN(0,1)),
            retired INTEGER NOT NULL CHECK(retired IN(0,1)),
            PRIMARY KEY(parent_client,parent_sequence,digest)) WITHOUT ROWID")?;
    let retired: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_raft_payload_dependencies WHERE parent_client=?1 AND parent_sequence=?2 AND retired=1)", rusqlite::params![parent.client.bytes().as_slice(), parent.sequence().to_be_bytes().as_slice()], |row| row.get(0))?;
    ensure!(!retired, "payload invocation was retired");
    let prior: Option<Vec<u8>> = connection.query_row("SELECT body FROM __sys_raft_payload_dependencies WHERE parent_client=?1 AND parent_sequence=?2 AND digest=?3", rusqlite::params![parent.client.bytes().as_slice(), parent.sequence().to_be_bytes().as_slice(), digest.as_slice()], |row| row.get(0)).optional()?;
    if let Some(prior) = prior {
        ensure!(prior == bytes, "conflicting payload dependency");
        if confirmed {
            connection.execute("UPDATE __sys_raft_payload_dependencies SET confirmed=1 WHERE parent_client=?1 AND parent_sequence=?2 AND digest=?3", rusqlite::params![parent.client.bytes().as_slice(), parent.sequence().to_be_bytes().as_slice(), digest.as_slice()])?;
        }
        return Ok(());
    }
    let count: usize = connection.query_row(
        "SELECT count(*) FROM __sys_raft_payload_dependencies",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        count < MAX_DEPENDENCIES,
        cellar_logic::consensus::CommandFailure::Overloaded
    );
    connection.execute(
        "INSERT INTO __sys_raft_payload_dependencies VALUES(?1,?2,?3,?4,?5,0)",
        rusqlite::params![
            parent.client.bytes().as_slice(),
            parent.sequence().to_be_bytes().as_slice(),
            digest.as_slice(),
            bytes,
            confirmed
        ],
    )?;
    Ok(())
}

fn exists(connection: &Connection, table: &str) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
        [table],
        |row| row.get(0),
    )?)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Retirement {
    pub client: [u8; 16],
    pub sequence: u64,
}

/// The SQL owner must exclude every live/parked same-cell frame before this
/// operation is selected and again before its command enters. Pending child
/// sequences retain their input dependencies until authenticated settlement.
pub(crate) fn due(connection: &Connection) -> Result<Option<Retirement>> {
    as_framework_sql(|| {
        if !exists(connection, "__sys_raft_payload_dependencies")? {
            return Ok(None);
        }
        let sql = if exists(connection, "__sys_raft_binding_intents")? {
            "SELECT d.parent_client,d.parent_sequence FROM __sys_raft_payload_dependencies d JOIN __sys_raft_receipts r ON r.client_id=d.parent_client AND r.sequence=d.parent_sequence WHERE d.retired=0 AND r.rpc_step IS NOT NULL AND NOT EXISTS(SELECT 1 FROM __sys_raft_binding_intents b WHERE b.parent_client=d.parent_client AND b.parent_sequence=d.parent_sequence AND b.phase!=3) ORDER BY d.parent_client,d.parent_sequence LIMIT 1"
        } else {
            "SELECT d.parent_client,d.parent_sequence FROM __sys_raft_payload_dependencies d JOIN __sys_raft_receipts r ON r.client_id=d.parent_client AND r.sequence=d.parent_sequence WHERE d.retired=0 AND r.rpc_step IS NOT NULL ORDER BY d.parent_client,d.parent_sequence LIMIT 1"
        };
        let row: Option<(Vec<u8>, Vec<u8>)> = connection
            .query_row(sql, [], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;
        row.map(|(client, sequence)| {
            let key = Retirement {
                client: client.as_slice().try_into()?,
                sequence: u64::from_be_bytes(sequence.as_slice().try_into()?),
            };
            cellar_logic::consensus::CommandId::new(
                cellar_logic::consensus::ClientId::new(key.client)?,
                key.sequence,
            )?;
            Ok(key)
        })
        .transpose()
    })
}

pub(crate) fn retire(connection: &Connection, key: &Retirement) -> Result<()> {
    as_framework_sql(|| {
        cellar_logic::consensus::CommandId::new(
            cellar_logic::consensus::ClientId::new(key.client)?,
            key.sequence,
        )?;
        ensure!(
            !connection.is_autocommit(),
            "payload retirement requires command transaction"
        );
        if !exists(connection, "__sys_raft_payload_dependencies")? {
            return Ok(());
        }
        if exists(connection, "__sys_raft_binding_intents")? {
            let pending: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_raft_binding_intents WHERE parent_client=?1 AND parent_sequence=?2 AND phase!=3)", rusqlite::params![key.client.as_slice(), key.sequence.to_be_bytes().as_slice()], |row| row.get(0))?;
            ensure!(
                !pending,
                cellar_logic::consensus::CommandFailure::Unavailable
            );
        }
        connection.execute("UPDATE __sys_raft_payload_dependencies SET retired=1 WHERE parent_client=?1 AND parent_sequence=?2", rusqlite::params![key.client.as_slice(), key.sequence.to_be_bytes().as_slice()])?;
        Ok(())
    })
}

pub(crate) fn prunable(
    connection: &Connection,
    client: cellar_logic::consensus::ClientId,
    through: u64,
) -> Result<bool> {
    if !exists(connection, "__sys_raft_payload_dependencies")? {
        return Ok(true);
    }
    let live: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_raft_payload_dependencies d JOIN __sys_raft_receipts r ON r.client_id=d.parent_client AND r.sequence=d.parent_sequence WHERE d.parent_client=?1 AND d.parent_sequence<=?2 AND d.retired=0 AND r.rpc_step IS NOT NULL)", rusqlite::params![client.bytes().as_slice(), through.to_be_bytes().as_slice()], |row| row.get(0))?;
    if live {
        return Ok(false);
    }
    if exists(connection, "__sys_raft_binding_intents")? {
        let pending: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_raft_binding_intents b JOIN __sys_raft_payload_dependencies d ON d.parent_client=b.parent_client AND d.parent_sequence=b.parent_sequence WHERE b.parent_client=?1 AND b.parent_sequence<=?2 AND b.phase!=3)", rusqlite::params![client.bytes().as_slice(), through.to_be_bytes().as_slice()], |row| row.get(0))?;
        if pending {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn visit(
    connection: &Connection,
    history: &CellHistory,
    visitor: &mut impl FnMut(Dependency) -> Result<()>,
) -> Result<()> {
    let mut count = 0;
    // A live value remains a dependency after the command's receipt expires.
    // Inspect this independently of the journal; never infer it from a list of
    // unconfirmed upload descriptors or drop it with a producer's retry floor.
    let kv: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_kv')",
        [],
        |row| row.get(0),
    )?;
    if kv {
        let mut statement = connection.prepare("SELECT CASE WHEN typeof(blob_id)='text' AND length(CAST(blob_id AS BLOB))<=1024 THEN blob_id ELSE NULL END,size FROM __sys_kv WHERE blob_id IS NOT NULL")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            ensure!(count < MAX_DEPENDENCIES, "payload row inventory bound");
            count += 1;
            let encoded: String = row.get(0)?;
            let reference: CellGroupPayloadReference = serde_json::from_str(&encoded)?;
            let size: u64 = row.get(1)?;
            ensure!(reference.length() == size, "payload row length");
            visitor(Dependency {
                history: history.clone(),
                reference,
                confirmed: true,
                guard: [0; 32],
            })?;
        }
    }
    let exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_raft_payload_dependencies')", [], |row| row.get(0))?;
    if !exists {
        return Ok(());
    }
    let mut statement = connection.prepare("SELECT digest,CASE WHEN typeof(body)='blob' AND length(body)<=2048 THEN body ELSE NULL END,confirmed FROM __sys_raft_payload_dependencies ORDER BY digest")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        ensure!(
            count < MAX_DEPENDENCIES,
            "payload dependency inventory bound"
        );
        count += 1;
        let digest: Vec<u8> = row.get(0)?;
        let bytes: Vec<u8> = row.get(1)?;
        ensure!(
            digest.as_slice() == <[u8; 32]>::from(Sha256::digest(&bytes)),
            "payload dependency digest"
        );
        let mut dependency: Dependency = serde_json::from_slice(&bytes)?;
        let confirmed: i64 = row.get(2)?;
        ensure!(
            matches!(confirmed, 0 | 1) && dependency.guard != [0; 32],
            "payload dependency phase or handoff identity"
        );
        dependency.confirmed = confirmed == 1;
        visitor(dependency)?;
    }
    Ok(())
}

/// Receipt-floor advancement retires this command's input/reply dependencies.
/// Current value rows and every older pinned image retain their own closure.
pub(crate) fn prune(
    connection: &Connection,
    client: cellar_logic::consensus::ClientId,
    through: u64,
) -> Result<()> {
    let exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_raft_payload_dependencies')", [], |row| row.get(0))?;
    if exists {
        connection.execute("DELETE FROM __sys_raft_payload_dependencies WHERE parent_client=?1 AND parent_sequence<=?2", rusqlite::params![client.bytes().as_slice(), through.to_be_bytes().as_slice()])?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "raft_payloads_tests.rs"]
mod tests;
