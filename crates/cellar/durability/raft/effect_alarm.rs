//! Transactional alarm generations and exact committed-image discovery.
use super::super::{apply::PublishedImage, budget::Permit};
use crate::application::consensus::{CommandContext, RaftClass};
use anyhow::{ensure, Context as _, Result};
use cellar_logic::consensus::{CommandFailure, CommandId, Durability, LogPosition};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftAlarmLimits {
    pub max_alarms: usize,
    pub max_payload_bytes: usize,
}

impl Default for RaftAlarmLimits {
    fn default() -> Self {
        Self {
            max_alarms: 16,
            max_payload_bytes: 64 * 1024,
        }
    }
}

impl RaftAlarmLimits {
    pub(crate) fn validate(self) -> Result<()> {
        ensure!(
            (1..=32).contains(&self.max_alarms)
                && (1..=1024 * 1024).contains(&self.max_payload_bytes),
            "invalid alarm limits"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RaftAlarmOptions {
    pub interval_ms: u64,
    pub operation_timeout_ms: u64,
    pub retry_window_ms: u64,
    pub metadata_bytes: usize,
}

impl Default for RaftAlarmOptions {
    fn default() -> Self {
        Self {
            interval_ms: 100,
            operation_timeout_ms: 5000,
            retry_window_ms: 60_000,
            metadata_bytes: 64 * 1024,
        }
    }
}

impl RaftAlarmOptions {
    pub(crate) fn validate(self, host: &crate::raft::host::RaftHostOptions) -> Result<()> {
        ensure!(
            (10..=60_000).contains(&self.interval_ms)
                && (1..=60_000).contains(&self.operation_timeout_ms)
                && self
                    .operation_timeout_ms
                    .checked_add(100)
                    .is_some_and(|n| n <= self.retry_window_ms)
                && self.retry_window_ms <= host.execution.max_retry_window_ms
                && (64 * 1024..=8 * 1024 * 1024).contains(&self.metadata_bytes),
            "invalid alarm scheduler bounds"
        );
        Ok(())
    }
}

pub(crate) fn operation(name: &str) -> String {
    format!("__sys_raft_alarm_{name}")
}

fn exists(connection: &Connection) -> Result<bool> {
    Ok(connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_raft_alarms')",[],|row|row.get(0))?)
}

fn install(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS __sys_raft_alarms (
        name TEXT PRIMARY KEY CHECK(typeof(name)='text' AND length(name) BETWEEN 1 AND 64),
        id BLOB NOT NULL CHECK(typeof(id)='blob' AND length(id)=32),
        due INTEGER NOT NULL CHECK(typeof(due)='integer' AND due>=0),
        minimum INTEGER NOT NULL CHECK(minimum IN (0,1)),
        payload BLOB NOT NULL CHECK(typeof(payload)='blob')
    ) WITHOUT ROWID",
    )?;
    Ok(())
}

pub(crate) fn arm(
    context: &CommandContext<'_>,
    durability: Durability,
    name: &str,
    due: u64,
    payload: &[u8],
    limits: RaftAlarmLimits,
) -> Result<()> {
    let connection = context.sql().framework();
    let scope = context.identity();
    let command = context.command();
    ensure!(
        payload.len() <= limits.max_payload_bytes,
        CommandFailure::Overloaded
    );
    let due = i64::try_from(due).map_err(|_| CommandFailure::UnsupportedCapability)?;
    install(connection)?;
    let (count, present):(usize,bool) = connection.query_row("SELECT count(*),EXISTS(SELECT 1 FROM __sys_raft_alarms WHERE name=?1) FROM __sys_raft_alarms",[name],|row|Ok((row.get(0)?,row.get(1)?)))?;
    ensure!(count <= limits.max_alarms, CommandFailure::CorruptState);
    ensure!(
        present || count < limits.max_alarms,
        CommandFailure::Overloaded
    );
    let mut hash = Sha256::new();
    hash.update(b"cellar-raft-alarm-generation-1");
    hash.update(super::system_client(scope)?.bytes());
    hash.update(command.client.bytes());
    hash.update(command.sequence().to_be_bytes());
    hash.update((name.len() as u64).to_be_bytes());
    hash.update(name.as_bytes());
    let id: [u8; 32] = hash.finalize().into();
    connection.execute("INSERT INTO __sys_raft_alarms(name,id,due,minimum,payload) VALUES(?1,?2,?3,?4,?5)
        ON CONFLICT(name) DO UPDATE SET id=excluded.id,due=excluded.due,minimum=excluded.minimum,payload=excluded.payload",
        params![name,id.as_slice(),due,if durability==Durability::S3 {1} else {0},payload])?;
    Ok(())
}

pub(crate) fn cancel(connection: &Connection, name: &str) -> Result<bool> {
    if !exists(connection)? {
        return Ok(false);
    }
    Ok(connection.execute("DELETE FROM __sys_raft_alarms WHERE name=?1", [name])? != 0)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fire {
    id: [u8; 32],
}

pub(crate) fn fire(
    context: &CommandContext<'_>,
    name: &str,
    limits: RaftAlarmLimits,
    input: &[u8],
    callback: &impl for<'a> Fn(&CommandContext<'a>, &[u8]) -> Result<()>,
) -> Result<Vec<u8>> {
    ensure!(input.len() <= 1024, CommandFailure::Overloaded);
    let fire: Fire = serde_json::from_slice(input)?;
    let connection = context.sql().framework();
    if !exists(connection)? {
        return Ok(vec![0]);
    }
    let metadata: Option<(Vec<u8>, i64, i64)> = connection
        .query_row(
            "SELECT CASE WHEN typeof(id)='blob' AND length(id)=32 THEN id ELSE NULL END,due,length(payload) FROM __sys_raft_alarms WHERE name=?1",
            [name],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((id, due, length)) = metadata else {
        return Ok(vec![0]);
    };
    ensure!(
        id.len() == 32
            && due >= 0
            && length >= 0
            && length as u64 <= limits.max_payload_bytes as u64,
        CommandFailure::CorruptState
    );
    if id != fire.id || due as u64 > context.inputs().now_unix_millis {
        return Ok(vec![0]);
    }
    let payload: Vec<u8> = connection.query_row(
        "SELECT payload FROM __sys_raft_alarms WHERE name=?1",
        [name],
        |row| row.get(0),
    )?;
    ensure!(payload.len() as i64 == length, CommandFailure::CorruptState);
    ensure!(
        connection.execute(
            "DELETE FROM __sys_raft_alarms WHERE name=?1 AND id=?2",
            params![name, id]
        )? == 1,
        CommandFailure::CorruptState
    );
    // Rollback restores the generation if the callback fails. Deleting first
    // allows a successful callback to arm a distinct next generation atomically.
    callback(context, &payload)?;
    Ok(vec![1])
}

pub(crate) struct DueAlarm {
    pub operation: String,
    pub input: Vec<u8>,
    pub minimum: Durability,
}

pub(crate) struct AlarmScan {
    pub position: LogPosition,
    pub next: CommandId,
    pub due: Option<DueAlarm>,
    pub _reservation: Permit,
}

pub(crate) fn scan(
    image: PublishedImage,
    class: &RaftClass,
    services: cellar_ltx::generation::ReadServices,
    now: u64,
    reservation: Permit,
) -> Result<AlarmScan> {
    let limits = class
        .alarm_limits()
        .ok_or(CommandFailure::UnsupportedCapability)?;
    let database = crate::storage::open_immutable_generation(
        image.pin(),
        crate::storage::ImmutableQueryOptions {
            services,
            sqlite_vec: false,
        },
    )?;
    let connection = database.connection();
    let client = super::system_client(image.scope())?;
    let last: Option<Vec<u8>> = connection
        .query_row(
            "SELECT CASE WHEN typeof(last_sequence)='blob' AND length(last_sequence)=8 THEN last_sequence ELSE NULL END FROM __sys_raft_clients WHERE client_id=?1",
            [client.bytes().as_slice()],
            |row| row.get(0),
        )
        .optional()?;
    let last = last
        .map(|bytes| {
            bytes
                .try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| anyhow::anyhow!(CommandFailure::CorruptState))
        })
        .transpose()?
        .unwrap_or(0);
    let next = CommandId::new(
        client,
        last.checked_add(1).ok_or(CommandFailure::Overloaded)?,
    )?;
    let due = if exists(connection)? {
        let count: usize =
            connection.query_row("SELECT count(*) FROM __sys_raft_alarms", [], |row| {
                row.get(0)
            })?;
        ensure!(count <= limits.max_alarms, CommandFailure::CorruptState);
        let metadata:Option<(String,Vec<u8>,i64)>=connection.query_row("SELECT CASE WHEN typeof(name)='text' AND length(name) BETWEEN 1 AND 64 THEN name ELSE NULL END,CASE WHEN typeof(id)='blob' AND length(id)=32 THEN id ELSE NULL END,minimum FROM __sys_raft_alarms WHERE due<=?1 ORDER BY due,name LIMIT 1",[i64::try_from(now).context("alarm clock range")?],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?;
        metadata
            .map(|(name, id, minimum)| {
                ensure!(name.len() <= 64, CommandFailure::CorruptState);
                let operation = operation(&name);
                ensure!(
                    class.system_contract(&operation).is_some(),
                    CommandFailure::CorruptState
                );
                let id = id
                    .try_into()
                    .map_err(|_| anyhow::anyhow!(CommandFailure::CorruptState))?;
                let minimum = match minimum {
                    0 => Durability::Quorum,
                    1 => Durability::S3,
                    _ => return Err(CommandFailure::CorruptState.into()),
                };
                Ok(DueAlarm {
                    operation,
                    input: serde_json::to_vec(&Fire { id })?,
                    minimum,
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(AlarmScan {
        position: image.position(),
        next,
        due,
        _reservation: reservation,
    })
}
