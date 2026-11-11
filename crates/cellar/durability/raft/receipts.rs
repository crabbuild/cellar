// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Replicated receipt and non-reusable client-sequence floors.

use super::execution::{storage, CommandRequest, ExecutionError, ExecutionLimits, StoredReceipt};
use cellar_logic::consensus::{ClientId, CommandFailure, Durability};
use rusqlite::{params, Connection, OptionalExtension};

#[cfg(test)]
#[path = "receipts_tests.rs"]
mod tests;

/// User-state erasure must retain exact retries and non-reusable sequence floors.
pub(crate) fn is_control_table(name: &str) -> bool {
    matches!(name, "__sys_raft_clients" | "__sys_raft_receipts")
}

pub(super) fn install(connection: &Connection) -> anyhow::Result<()> {
    connection.execute_batch("CREATE TABLE IF NOT EXISTS __sys_raft_clients (
        client_id BLOB PRIMARY KEY CHECK(typeof(client_id)='blob' AND length(client_id)=16),
        last_sequence BLOB NOT NULL CHECK(typeof(last_sequence)='blob' AND length(last_sequence)=8),
        expired_through BLOB NOT NULL CHECK(typeof(expired_through)='blob' AND length(expired_through)=8)
    ) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS __sys_raft_receipts (
        client_id BLOB NOT NULL CHECK(typeof(client_id)='blob' AND length(client_id)=16),
        sequence BLOB NOT NULL CHECK(typeof(sequence)='blob' AND length(sequence)=8),
        operation TEXT NOT NULL CHECK(length(operation) BETWEEN 1 AND 128),
        contract_digest BLOB NOT NULL CHECK(typeof(contract_digest)='blob' AND length(contract_digest)=32),
        request_digest BLOB NOT NULL CHECK(typeof(request_digest)='blob' AND length(request_digest)=32),
        result BLOB NOT NULL CHECK(typeof(result)='blob'),
        minimum_durability INTEGER NOT NULL CHECK(minimum_durability IN (0,1)),
        retry_until BLOB NOT NULL CHECK(typeof(retry_until)='blob' AND length(retry_until)=8),
        rpc_step BLOB CHECK(rpc_step IS NULL OR (typeof(rpc_step)='blob' AND length(rpc_step)=8)),
        PRIMARY KEY(client_id,sequence)
    ) WITHOUT ROWID;")?;
    Ok(())
}

fn corrupt() -> ExecutionError {
    ExecutionError::Refused(CommandFailure::CorruptState)
}

fn number(bytes: Vec<u8>) -> Result<u64, ExecutionError> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| corrupt())?))
}

fn digest(bytes: Vec<u8>) -> Result<[u8; 32], ExecutionError> {
    bytes.try_into().map_err(|_| corrupt())
}

pub(super) fn client(
    connection: &Connection,
    id: ClientId,
) -> Result<Option<(u64, u64)>, ExecutionError> {
    let row: Option<(Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT last_sequence,expired_through FROM __sys_raft_clients WHERE client_id=?1",
            [id.bytes().as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    row.map(|(last, floor)| {
        let last = number(last)?;
        let floor = number(floor)?;
        if last == 0 || floor > last {
            return Err(corrupt());
        }
        Ok((last, floor))
    })
    .transpose()
}

pub(super) fn retained_count(connection: &Connection) -> Result<usize, ExecutionError> {
    connection
        .query_row("SELECT count(*) FROM __sys_raft_receipts", [], |row| {
            row.get(0)
        })
        .map_err(storage)
}

pub(super) fn preflight(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
) -> Result<Option<StoredReceipt>, ExecutionError> {
    let receipt = preflight_identity(connection, request, limits)?;
    if receipt.is_none() && retained_count(connection)? >= limits.max_retained_receipts {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    Ok(receipt)
}

pub(super) fn preflight_identity(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
) -> Result<Option<StoredReceipt>, ExecutionError> {
    match inspect_identity(connection, request, limits)? {
        Identity::Retained(receipt) => Ok(Some(receipt)),
        Identity::Vacant { new_client } => {
            if new_client {
                let count: usize = connection
                    .query_row("SELECT count(*) FROM __sys_raft_clients", [], |r| r.get(0))
                    .map_err(storage)?;
                if count >= limits.max_client_identities {
                    return Err(ExecutionError::Refused(CommandFailure::Overloaded));
                }
            }
            Ok(None)
        }
    }
}

/// Inspect immutable committed state without admitting a new client or receipt.
/// Capacity cannot obscure absence; identity, expiry, sequence and result-size
/// checks still apply. The caller supplies the consensus proof for using absence.
pub(super) fn lookup_identity(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
) -> Result<Option<StoredReceipt>, ExecutionError> {
    match inspect_identity(connection, request, limits)? {
        Identity::Retained(receipt) => Ok(Some(receipt)),
        Identity::Vacant { .. } => Ok(None),
    }
}

/// An initial cell image may precede receipt schema installation. Losing schema
/// after a mutation, or retaining only half of it, is corruption, not absence.
/// A missing receipt never proves that a submitted command cannot still commit.
pub(super) fn lookup_cell_identity(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
    cell_sequence: u64,
) -> Result<Option<StoredReceipt>, ExecutionError> {
    lookup_cell_identity_admitted(connection, request, limits, cell_sequence, |_| Ok(()))
}

/// Reserve a retained result after inspecting its length, before fetching bytes.
/// Admission failure is not absence and consumes no additional operation slot.
pub(super) fn lookup_cell_identity_admitted(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
    cell_sequence: u64,
    mut reserve_result: impl FnMut(usize) -> Result<(), ExecutionError>,
) -> Result<Option<StoredReceipt>, ExecutionError> {
    let tables: u64 = connection.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('__sys_raft_clients','__sys_raft_receipts')",
        [], |row| row.get(0),
    ).map_err(storage)?;
    match tables {
        0 if cell_sequence == 1 => {
            if request.id.sequence() == 1 {
                Ok(None)
            } else {
                Err(ExecutionError::Refused(CommandFailure::SequenceGap {
                    next: 1,
                }))
            }
        }
        2 => match inspect_identity_admitted(connection, request, limits, &mut reserve_result)? {
            Identity::Retained(receipt) => Ok(Some(receipt)),
            Identity::Vacant { .. } => Ok(None),
        },
        _ => Err(corrupt()),
    }
}

enum Identity {
    Retained(StoredReceipt),
    Vacant { new_client: bool },
}

fn inspect_identity(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
) -> Result<Identity, ExecutionError> {
    inspect_identity_admitted(connection, request, limits, &mut |_| Ok(()))
}

fn inspect_identity_admitted(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
    reserve_result: &mut dyn FnMut(usize) -> Result<(), ExecutionError>,
) -> Result<Identity, ExecutionError> {
    inspect_identity_state(connection, request, limits, reserve_result, false)
}

fn inspect_identity_state(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
    reserve_result: &mut dyn FnMut(usize) -> Result<(), ExecutionError>,
    allow_pending: bool,
) -> Result<Identity, ExecutionError> {
    let state = client(connection, request.id.client)?;
    let (last, floor) = state.unwrap_or((0, 0));
    let sequence = request.id.sequence();
    if sequence <= floor {
        return Err(ExecutionError::Refused(CommandFailure::CommandExpired));
    }
    if sequence <= last {
        // Bound the result allocation before fetching it. Exclusive ownership
        // prevents another writer changing the row between these observations.
        let length: Option<i64> = connection
            .query_row(
                "SELECT length(result) FROM __sys_raft_receipts WHERE client_id=?1 AND sequence=?2",
                params![
                    request.id.client.bytes().as_slice(),
                    sequence.to_be_bytes().as_slice()
                ],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        if length.is_none_or(|n| n < 0 || n as u64 > limits.max_result_bytes as u64) {
            return Err(corrupt());
        }
        reserve_result(length.ok_or_else(corrupt)? as usize)?;
        let row=connection.query_row(
            "SELECT operation,contract_digest,request_digest,result,minimum_durability,retry_until,rpc_step
             FROM __sys_raft_receipts WHERE client_id=?1 AND sequence=?2",
            params![request.id.client.bytes().as_slice(),sequence.to_be_bytes().as_slice()],|r|
                Ok((r.get::<_,String>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,Vec<u8>>(2)?,
                    r.get::<_,Vec<u8>>(3)?,r.get::<_,i64>(4)?,r.get::<_,Vec<u8>>(5)?,r.get::<_,Option<Vec<u8>>>(6)?)))
            .map_err(storage)?;
        let contract = digest(row.1)?;
        let request_digest = digest(row.2)?;
        if row.0 != request.operation
            || contract != request.contract_digest
            || request_digest != request.request_digest
        {
            return Err(ExecutionError::Refused(CommandFailure::CommandConflict));
        }
        let minimum_durability = match row.4 {
            0 => Durability::Quorum,
            1 => Durability::S3,
            _ => return Err(corrupt()),
        };
        let rpc_step = row.6.map(number).transpose()?;
        if rpc_step == Some(0) || (rpc_step.is_some() && !row.3.is_empty()) {
            return Err(corrupt());
        }
        if rpc_step.is_some() && !allow_pending {
            return Err(ExecutionError::Uncertain);
        }
        return Ok(Identity::Retained(StoredReceipt {
            id: request.id,
            operation: row.0,
            contract_digest: contract,
            request_digest,
            result: row.3,
            minimum_durability,
            retry_until_ms: number(row.5)?,
            rpc_step,
        }));
    }
    let next = last.checked_add(1).ok_or_else(corrupt)?;
    if sequence != next {
        return Err(ExecutionError::Refused(CommandFailure::SequenceGap {
            next,
        }));
    }
    Ok(Identity::Vacant {
        new_client: state.is_none(),
    })
}

/// Host-only inspection of an exact intermediate mutation. Public receipt
/// lookup treats every unfinished invocation as uncertain, never absent.
pub(super) fn rpc_receipt(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
    step: u64,
) -> Result<StoredReceipt, ExecutionError> {
    if step == 0 {
        return Err(corrupt());
    }
    match lookup_rpc_identity(connection, request, limits)? {
        Some(receipt) if receipt.rpc_step == Some(step) => Ok(receipt),
        _ => Err(ExecutionError::Uncertain),
    }
}

pub(super) fn lookup_rpc_identity(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
) -> Result<Option<StoredReceipt>, ExecutionError> {
    match inspect_identity_state(connection, request, limits, &mut |_| Ok(()), true)? {
        Identity::Retained(receipt) => Ok(Some(receipt)),
        Identity::Vacant { .. } => Ok(None),
    }
}

/// Commit the original invocation identity with each independently applied
/// transaction. Reuse its one admitted receipt/client slot and retention floor.
pub(super) fn record_rpc_step(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
    step: u64,
) -> anyhow::Result<()> {
    if step == 1 {
        record(connection, request, Vec::new())?;
    } else {
        let previous = step
            .checked_sub(1)
            .filter(|step| *step != 0)
            .ok_or_else(|| anyhow::anyhow!("invalid RPC checkpoint sequence"))?;
        rpc_receipt(connection, request, limits, previous)?;
    }
    let changed = connection.execute(
        "UPDATE __sys_raft_receipts SET rpc_step=?3 WHERE client_id=?1 AND sequence=?2",
        params![
            request.id.client.bytes().as_slice(),
            request.id.sequence().to_be_bytes().as_slice(),
            step.to_be_bytes().as_slice()
        ],
    )?;
    anyhow::ensure!(changed == 1, "RPC checkpoint receipt missing");
    Ok(())
}

pub(super) fn complete_rpc(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
    step: u64,
    result: Vec<u8>,
) -> anyhow::Result<()> {
    if step == 0 {
        return record(connection, request, result);
    }
    rpc_receipt(connection, request, limits, step)?;
    let changed = connection.execute(
        "UPDATE __sys_raft_receipts SET result=?3,rpc_step=NULL WHERE client_id=?1 AND sequence=?2",
        params![
            request.id.client.bytes().as_slice(),
            request.id.sequence().to_be_bytes().as_slice(),
            result
        ],
    )?;
    anyhow::ensure!(changed == 1, "RPC completion receipt missing");
    Ok(())
}

pub(super) fn record(
    connection: &Connection,
    request: &CommandRequest,
    result: Vec<u8>,
) -> anyhow::Result<()> {
    let id = request.id.client.bytes();
    let sequence = request.id.sequence().to_be_bytes();
    connection.execute(
        "INSERT INTO __sys_raft_clients(client_id,last_sequence,expired_through)
        VALUES(?1,?2,?3) ON CONFLICT(client_id) DO UPDATE SET last_sequence=excluded.last_sequence",
        params![
            id.as_slice(),
            sequence.as_slice(),
            0_u64.to_be_bytes().as_slice()
        ],
    )?;
    connection.execute("INSERT INTO __sys_raft_receipts
        (client_id,sequence,operation,contract_digest,request_digest,result,minimum_durability,retry_until)
        VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",params![id.as_slice(),sequence.as_slice(),&request.operation,
        request.contract_digest.as_slice(),request.request_digest.as_slice(),result,
        if request.durability==Durability::S3 {1} else {0},request.retry_until_ms.to_be_bytes().as_slice()])?;
    Ok(())
}

pub(super) fn prune(
    connection: &Connection,
    id: ClientId,
    through: u64,
    now: u64,
) -> Result<(), ExecutionError> {
    let (last, floor) =
        client(connection, id)?.ok_or(ExecutionError::Invalid("unknown receipt client"))?;
    if through < floor || through > last {
        return Err(ExecutionError::Invalid("receipt expiry floor"));
    }
    let count: u64 = connection
        .query_row(
            "SELECT count(*) FROM __sys_raft_receipts
        WHERE client_id=?1 AND sequence<=?2",
            params![id.bytes().as_slice(), through.to_be_bytes().as_slice()],
            |r| r.get(0),
        )
        .map_err(storage)?;
    if count != through - floor {
        return Err(corrupt());
    }
    let retained: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_raft_receipts
        WHERE client_id=?1 AND sequence<=?2 AND retry_until>=?3)",
            params![
                id.bytes().as_slice(),
                through.to_be_bytes().as_slice(),
                now.to_be_bytes().as_slice()
            ],
            |r| r.get(0),
        )
        .map_err(storage)?;
    if retained {
        return Err(ExecutionError::RetentionNotExpired);
    }
    connection
        .execute(
            "DELETE FROM __sys_raft_receipts WHERE client_id=?1 AND sequence<=?2",
            params![id.bytes().as_slice(), through.to_be_bytes().as_slice()],
        )
        .map_err(storage)?;
    connection
        .execute(
            "UPDATE __sys_raft_clients SET expired_through=?2 WHERE client_id=?1",
            params![id.bytes().as_slice(), through.to_be_bytes().as_slice()],
        )
        .map_err(storage)?;
    Ok(())
}
