// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, read-only selection of expired receipt prefixes for a command's
//! existing private transaction. Scheduling cursors carry no mutation authority.
use super::execution::{storage, ExecutionError};
use super::receipts;
use cellar_logic::consensus::ClientId;
use rusqlite::{params, Connection};

/// Work bounds for opportunistic receipt maintenance near the receipt ceiling.
/// Every client's latest receipt remains protected. No client floor is removed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiptMaintenanceLimits {
    pub max_clients_per_command: usize,
    pub max_rows_per_command: usize,
}

impl Default for ReceiptMaintenanceLimits {
    fn default() -> Self {
        Self {
            max_clients_per_command: 8,
            max_rows_per_command: 128,
        }
    }
}

impl ReceiptMaintenanceLimits {
    pub(crate) fn validate(self) -> Result<(), ExecutionError> {
        if !(1..=64).contains(&self.max_clients_per_command)
            || !(1..=1024).contains(&self.max_rows_per_command)
        {
            return Err(ExecutionError::Invalid("receipt maintenance limits"));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Prefix {
    client: ClientId,
    last: u64,
    floor: u64,
    through: u64,
}

#[derive(Debug)]
pub(super) struct Plan {
    now: u64,
    prefixes: Vec<Prefix>,
    pub next_cursor: Option<ClientId>,
    pub rows_removed: usize,
    pub clients_scanned: usize,
    pub rows_scanned: usize,
}

impl Plan {
    /// Apply only inside the same command transaction and captured input clock.
    /// The normal receipt check revalidates deadlines and contiguous floors.
    pub fn apply(&self, connection: &Connection, now: u64) -> Result<(), ExecutionError> {
        if now != self.now {
            return Err(ExecutionError::Invalid("receipt maintenance clock"));
        }
        if connection.is_autocommit() {
            return Err(ExecutionError::Invalid(
                "receipt maintenance requires transaction",
            ));
        }
        for prefix in &self.prefixes {
            if prefix.through >= prefix.last
                || receipts::client(connection, prefix.client)? != Some((prefix.last, prefix.floor))
            {
                return Err(corrupt());
            }
            receipts::prune(connection, prefix.client, prefix.through, now)?;
        }
        Ok(())
    }
}

fn corrupt() -> ExecutionError {
    ExecutionError::Refused(cellar_logic::consensus::CommandFailure::CorruptState)
}

fn number(value: Vec<u8>) -> Result<u64, ExecutionError> {
    Ok(u64::from_be_bytes(value.try_into().map_err(|_| corrupt())?))
}

struct Client {
    id: ClientId,
    last: u64,
    floor: u64,
}

fn clients(
    connection: &Connection,
    cursor: Option<ClientId>,
    limit: usize,
) -> Result<Vec<Client>, ExecutionError> {
    let mut selected = Vec::with_capacity(limit);
    let after = cursor.map(|id| id.bytes());
    // Both ranges use the primary key and disjoint bounds. A wrapped scan
    // visits each identity at most once, including when the cursor disappeared.
    let sql = if after.is_some() {
        "SELECT client_id,last_sequence,expired_through FROM __sys_raft_clients WHERE client_id>?1 ORDER BY client_id LIMIT ?2"
    } else {
        "SELECT client_id,last_sequence,expired_through FROM __sys_raft_clients WHERE ?1 IS NULL ORDER BY client_id LIMIT ?2"
    };
    read_clients(
        connection,
        sql,
        after.as_ref().map(|id| id.as_slice()),
        limit,
        &mut selected,
    )?;
    if after.is_some() && selected.len() < limit {
        read_clients(connection,
            "SELECT client_id,last_sequence,expired_through FROM __sys_raft_clients WHERE client_id<=?1 ORDER BY client_id LIMIT ?2",
            after.as_ref().map(|id| id.as_slice()), limit - selected.len(), &mut selected)?;
    }
    Ok(selected)
}

fn read_clients(
    connection: &Connection,
    sql: &str,
    bound: Option<&[u8]>,
    limit: usize,
    selected: &mut Vec<Client>,
) -> Result<(), ExecutionError> {
    let mut statement = connection.prepare(sql).map_err(storage)?;
    let mut rows = statement
        .query(params![bound, limit as i64])
        .map_err(storage)?;
    while let Some(row) = rows.next().map_err(storage)? {
        let id: Vec<u8> = row.get(0).map_err(storage)?;
        let id = ClientId::new(id.try_into().map_err(|_| corrupt())?).map_err(|_| corrupt())?;
        let last = number(row.get(1).map_err(storage)?)?;
        let floor = number(row.get(2).map_err(storage)?)?;
        if last == 0 || floor > last {
            return Err(corrupt());
        }
        selected.push(Client { id, last, floor });
    }
    Ok(())
}

pub(super) fn plan(
    connection: &Connection,
    now: u64,
    limits: ReceiptMaintenanceLimits,
    cursor: Option<ClientId>,
) -> Result<Plan, ExecutionError> {
    limits.validate()?;
    let mut plan = Plan {
        now,
        prefixes: Vec::new(),
        next_cursor: cursor,
        rows_removed: 0,
        clients_scanned: 0,
        rows_scanned: 0,
    };
    for client in clients(connection, cursor, limits.max_clients_per_command)? {
        if plan.rows_scanned == limits.max_rows_per_command {
            break;
        }
        plan.clients_scanned += 1;
        plan.next_cursor = Some(client.id);
        let older = client.last.saturating_sub(client.floor).saturating_sub(1);
        if older == 0 {
            continue;
        }
        let maximum = older.min((limits.max_rows_per_command - plan.rows_scanned) as u64) as usize;
        let mut statement = connection.prepare(
            "SELECT sequence,retry_until FROM __sys_raft_receipts WHERE client_id=?1 AND sequence>?2 AND sequence<?3 ORDER BY sequence LIMIT ?4"
        ).map_err(storage)?;
        let mut rows = statement
            .query(params![
                client.id.bytes().as_slice(),
                client.floor.to_be_bytes().as_slice(),
                client.last.to_be_bytes().as_slice(),
                maximum as i64
            ])
            .map_err(storage)?;
        let mut through = client.floor;
        let mut visited = 0;
        let mut retained = false;
        while let Some(row) = rows.next().map_err(storage)? {
            let sequence = number(row.get(0).map_err(storage)?)?;
            let deadline = number(row.get(1).map_err(storage)?)?;
            if through.checked_add(1) != Some(sequence) {
                return Err(corrupt());
            }
            plan.rows_scanned += 1;
            visited += 1;
            if deadline >= now {
                retained = true;
                break;
            }
            through = sequence;
        }
        if !retained && visited != maximum {
            return Err(corrupt());
        }
        if through > client.floor {
            plan.rows_removed += usize::try_from(through - client.floor).map_err(|_| corrupt())?;
            plan.prefixes.push(Prefix {
                client: client.id,
                last: client.last,
                floor: client.floor,
                through,
            });
        }
    }
    Ok(plan)
}

#[cfg(test)]
#[path = "receipt_maintenance_tests.rs"]
mod tests;
