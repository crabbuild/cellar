//! Replicated receipt and non-reusable client-sequence floors.

use super::execution::{storage, CommandRequest, ExecutionError, ExecutionLimits, StoredReceipt};
use cellar_logic::consensus::{ClientId, CommandFailure, Durability};
use rusqlite::{params, Connection, OptionalExtension};

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

fn client(connection: &Connection, id: ClientId) -> Result<Option<(u64, u64)>, ExecutionError> {
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

pub(super) fn preflight(
    connection: &Connection,
    request: &CommandRequest,
    limits: &ExecutionLimits,
) -> Result<Option<StoredReceipt>, ExecutionError> {
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
        let row=connection.query_row(
            "SELECT operation,contract_digest,request_digest,result,minimum_durability,retry_until
             FROM __sys_raft_receipts WHERE client_id=?1 AND sequence=?2",
            params![request.id.client.bytes().as_slice(),sequence.to_be_bytes().as_slice()],|r|
                Ok((r.get::<_,String>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,Vec<u8>>(2)?,
                    r.get::<_,Vec<u8>>(3)?,r.get::<_,i64>(4)?,r.get::<_,Vec<u8>>(5)?)))
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
        return Ok(Some(StoredReceipt {
            id: request.id,
            operation: row.0,
            contract_digest: contract,
            request_digest,
            result: row.3,
            minimum_durability,
            retry_until_ms: number(row.5)?,
        }));
    }
    let next = last.checked_add(1).ok_or_else(corrupt)?;
    if sequence != next {
        return Err(ExecutionError::Refused(CommandFailure::SequenceGap {
            next,
        }));
    }
    if state.is_none() {
        let count: usize = connection
            .query_row("SELECT count(*) FROM __sys_raft_clients", [], |r| r.get(0))
            .map_err(storage)?;
        if count >= limits.max_client_identities {
            return Err(ExecutionError::Refused(CommandFailure::Overloaded));
        }
    }
    let count: usize = connection
        .query_row("SELECT count(*) FROM __sys_raft_receipts", [], |r| r.get(0))
        .map_err(storage)?;
    if count >= limits.max_retained_receipts {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    Ok(None)
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
        WHERE client_id=?1 AND sequence<=?2 AND retry_until>?3)",
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
