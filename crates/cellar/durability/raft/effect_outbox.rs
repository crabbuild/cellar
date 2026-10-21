//! Replicated outbox lanes. A committed attempt precedes every send, and an
//! authenticated receiver receipt precedes conditional replicated settlement.
use super::delivery::{
    self, DeliveryOrigin, RaftDeliveryRequest, RaftEffectSigningKey, RaftEffectSink,
    RaftEffectTarget,
};
use crate::application::consensus::{CommandContext, CommandFailure, Durability};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) const CLAIM: &str = "__sys_raft_outbox_claim";
pub(crate) const COMPLETE: &str = "__sys_raft_outbox_complete";

#[derive(Clone, Copy, Debug)]
pub struct RaftOutboxLimits {
    pub max_lanes: usize,
    pub max_pending: usize,
    pub max_payload_bytes: usize,
    /// Includes actual encoded signed requests, rather than payload bytes alone.
    pub max_total_bytes: usize,
    pub retry_delay_ms: u64,
}

/// Prefix progress from one pinned committed image. Settlement proves the
/// configured receiver receipt was committed at the source's required policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftOutboxProgress {
    pub emitted: u64,
    pub settled: u64,
}

pub(crate) fn progress(
    connection: &Connection,
    outbox: &RaftOutbox,
    lane: &str,
) -> Result<RaftOutboxProgress> {
    ensure!(
        outbox.bindings.contains_key(lane),
        CommandFailure::UnsupportedCapability
    );
    if !exists(connection)? {
        return Ok(RaftOutboxProgress {
            emitted: 0,
            settled: 0,
        });
    }
    let row: Option<(Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT CASE WHEN typeof(emitted)='blob' AND length(emitted)=8 THEN emitted ELSE NULL END,CASE WHEN typeof(settled)='blob' AND length(settled)=8 THEN settled ELSE NULL END FROM __sys_raft_outbox_lanes WHERE lane=?1",
            [lane],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (emitted, settled) = row
        .map(|(emitted, settled)| Ok::<_, anyhow::Error>((number(emitted)?, number(settled)?)))
        .transpose()?
        .unwrap_or((0, 0));
    ensure!(settled <= emitted, CommandFailure::CorruptState);
    Ok(RaftOutboxProgress { emitted, settled })
}

impl Default for RaftOutboxLimits {
    fn default() -> Self {
        Self {
            max_lanes: 16,
            max_pending: 16,
            max_payload_bytes: delivery::MAX_PAYLOAD,
            max_total_bytes: 1024 * 1024,
            retry_delay_ms: 1000,
        }
    }
}

impl RaftOutboxLimits {
    fn validate(self) -> Result<()> {
        ensure!(
            (1..=32).contains(&self.max_lanes)
                && (1..=128).contains(&self.max_pending)
                && (1..=delivery::MAX_PAYLOAD).contains(&self.max_payload_bytes)
                && (delivery::MAX_REQUEST..=16 * 1024 * 1024).contains(&self.max_total_bytes)
                && (100..=60_000).contains(&self.retry_delay_ms),
            "invalid outbox limits"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Binding {
    digest: [u8; 32],
    pub target: RaftEffectTarget,
    pub sink: Arc<dyn RaftEffectSink>,
}

/// A compiled set of lanes and authenticated logical destinations. Routing
/// adapters may change endpoints, but cannot change identity or settle a send
/// without a valid durable receiver receipt.
#[derive(Clone)]
pub struct RaftOutbox {
    pub(crate) limits: RaftOutboxLimits,
    signer: Arc<RaftEffectSigningKey>,
    pub(crate) bindings: BTreeMap<String, Binding>,
}

impl RaftOutbox {
    pub fn new(signer: Arc<RaftEffectSigningKey>, limits: RaftOutboxLimits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            limits,
            signer,
            bindings: BTreeMap::new(),
        })
    }

    pub fn binding(
        mut self,
        name: impl Into<String>,
        digest: [u8; 32],
        target: RaftEffectTarget,
        sink: Arc<dyn RaftEffectSink>,
    ) -> Result<Self> {
        let name = name.into();
        ensure!(
            delivery::valid_name(&name) && digest != [0; 32] && !self.bindings.contains_key(&name),
            "invalid/duplicate outbox binding"
        );
        ensure!(
            self.bindings.len() < self.limits.max_lanes,
            CommandFailure::Overloaded
        );
        self.bindings.insert(
            name,
            Binding {
                digest,
                target,
                sink,
            },
        );
        Ok(self)
    }

    pub(crate) fn identity(&self) -> Result<[u8; 32]> {
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-outbox-1");
        hash.update(delivery::protocol_identity());
        hash.update(self.signer.verification_key().public_bytes());
        for limit in [
            self.limits.max_lanes as u64,
            self.limits.max_pending as u64,
            self.limits.max_payload_bytes as u64,
            self.limits.max_total_bytes as u64,
            self.limits.retry_delay_ms,
        ] {
            hash.update(limit.to_be_bytes());
        }
        hash.update((self.bindings.len() as u64).to_be_bytes());
        for (name, binding) in &self.bindings {
            hash.update((name.len() as u16).to_be_bytes());
            hash.update(name.as_bytes());
            hash.update(binding.digest);
            let target = serde_json::to_vec(&binding.target.wire())?;
            hash.update((target.len() as u64).to_be_bytes());
            hash.update(target);
        }
        Ok(hash.finalize().into())
    }

    fn stored_request(
        &self,
        context: &CommandContext<'_>,
        lane: &str,
        sequence: u64,
        bytes: &[u8],
    ) -> Result<RaftDeliveryRequest> {
        let binding = self
            .bindings
            .get(lane)
            .ok_or(CommandFailure::CorruptState)?;
        delivery::restore_request(
            bytes,
            DeliveryOrigin {
                scope: context.identity(),
                schema: context.schema_digest(),
                binding: lane,
                digest: binding.digest,
                sequence,
            },
            binding.target.clone(),
            &self.signer,
        )
    }

    pub(crate) fn emit(
        &self,
        context: &CommandContext<'_>,
        lane: &str,
        payload: &[u8],
    ) -> Result<u64> {
        let binding = self
            .bindings
            .get(lane)
            .ok_or(CommandFailure::UnsupportedCapability)?;
        ensure!(
            payload.len() <= self.limits.max_payload_bytes,
            CommandFailure::Overloaded
        );
        let connection = context.sql().framework();
        install(connection)?;
        let metadata: (usize, usize) = connection.query_row(
            "SELECT count(*),coalesce(sum(length(request)),0) FROM __sys_raft_outbox",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            metadata.0 <= self.limits.max_pending && metadata.1 <= self.limits.max_total_bytes,
            CommandFailure::CorruptState
        );
        ensure!(
            metadata.0 < self.limits.max_pending,
            CommandFailure::Overloaded
        );
        let last: Option<Vec<u8>> = connection
            .query_row(
                "SELECT CASE WHEN typeof(emitted)='blob' AND length(emitted)=8 THEN emitted ELSE NULL END FROM __sys_raft_outbox_lanes WHERE lane=?1",
                [lane],
                |row| row.get(0),
            )
            .optional()?;
        let last = last.map(number).transpose()?.unwrap_or(0);
        let sequence = last.checked_add(1).ok_or(CommandFailure::Overloaded)?;
        let request = RaftDeliveryRequest::new(
            DeliveryOrigin {
                scope: context.identity(),
                schema: context.schema_digest(),
                binding: lane,
                digest: binding.digest,
                sequence,
            },
            payload,
            binding.target.clone(),
            &self.signer,
        )?;
        ensure!(
            metadata
                .1
                .checked_add(request.encoded().len())
                .is_some_and(|n| n <= self.limits.max_total_bytes),
            CommandFailure::Overloaded
        );
        let now = i64::try_from(context.inputs().now_unix_millis)?;
        connection.execute("INSERT INTO __sys_raft_outbox_lanes(lane,emitted,settled) VALUES(?1,?2,?3) ON CONFLICT(lane) DO UPDATE SET emitted=excluded.emitted",params![lane,sequence.to_be_bytes().as_slice(),0u64.to_be_bytes().as_slice()])?;
        connection.execute("INSERT INTO __sys_raft_outbox(lane,sequence,request,minimum,generation,next_at) VALUES(?1,?2,?3,?4,?5,?6)",params![lane,sequence.to_be_bytes().as_slice(),request.encoded(),delivery::policy(context.durability()),[0u8;32].as_slice(),now])?;
        Ok(sequence)
    }

    pub(crate) fn claim(&self, context: &CommandContext<'_>, input: &[u8]) -> Result<Vec<u8>> {
        ensure!(input.len() <= 1024, CommandFailure::Overloaded);
        let selected: Selected = serde_json::from_slice(input)?;
        let sequence = selected.number()?;
        let connection = context.sql().framework();
        if !exists(connection)? {
            return Ok(vec![0]);
        }
        let Some(StoredIntent {
            bytes,
            minimum,
            next_at,
            ..
        }) = load(connection, &selected.lane, sequence)?
        else {
            return Ok(vec![0]);
        };
        let now = context.inputs().now_unix_millis;
        if next_at > now {
            return Ok(vec![0]);
        }
        ensure!(
            context.durability() >= minimum,
            CommandFailure::UnsupportedCapability
        );
        let settled: Vec<u8> = connection.query_row(
            "SELECT CASE WHEN typeof(settled)='blob' AND length(settled)=8 THEN settled ELSE NULL END FROM __sys_raft_outbox_lanes WHERE lane=?1",
            [&selected.lane],
            |row| row.get(0),
        )?;
        ensure!(
            number(settled)?.checked_add(1) == Some(sequence),
            CommandFailure::CorruptState
        );
        self.stored_request(context, &selected.lane, sequence, &bytes)?;
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-delivery-attempt-1");
        hash.update(super::system_client(context.identity())?.bytes());
        hash.update(context.command().client.bytes());
        hash.update(context.command().sequence().to_be_bytes());
        let generation: [u8; 32] = hash.finalize().into();
        let next_at = i64::try_from(
            now.checked_add(self.limits.retry_delay_ms)
                .ok_or(CommandFailure::Overloaded)?,
        )?;
        connection.execute(
            "UPDATE __sys_raft_outbox SET generation=?3,next_at=?4 WHERE lane=?1 AND sequence=?2",
            params![
                &selected.lane,
                sequence.to_be_bytes().as_slice(),
                generation.as_slice(),
                next_at
            ],
        )?;
        let mut result = Vec::with_capacity(33 + bytes.len());
        result.push(1);
        result.extend_from_slice(&generation);
        result.extend_from_slice(&bytes);
        Ok(result)
    }

    pub(crate) fn complete(&self, context: &CommandContext<'_>, input: &[u8]) -> Result<Vec<u8>> {
        ensure!(input.len() <= 32 * 1024, CommandFailure::Overloaded);
        let completed: Complete = serde_json::from_slice(input)?;
        let sequence = completed.selected.number()?;
        let connection = context.sql().framework();
        if !exists(connection)? {
            return Ok(vec![0]);
        }
        let Some(StoredIntent {
            bytes,
            minimum,
            generation,
            ..
        }) = load(connection, &completed.selected.lane, sequence)?
        else {
            return Ok(vec![0]);
        };
        if generation != completed.generation {
            return Ok(vec![0]);
        }
        ensure!(
            context.durability() >= minimum,
            CommandFailure::UnsupportedCapability
        );
        let request = self.stored_request(context, &completed.selected.lane, sequence, &bytes)?;
        let receipt = URL_SAFE_NO_PAD.decode(&completed.receipt)?;
        ensure!(
            URL_SAFE_NO_PAD.encode(&receipt) == completed.receipt,
            CommandFailure::CommandConflict
        );
        request.verify_receipt(&receipt)?;
        let settled: Vec<u8> = connection.query_row(
            "SELECT CASE WHEN typeof(settled)='blob' AND length(settled)=8 THEN settled ELSE NULL END FROM __sys_raft_outbox_lanes WHERE lane=?1",
            [&completed.selected.lane],
            |row| row.get(0),
        )?;
        ensure!(
            number(settled)?.checked_add(1) == Some(sequence),
            CommandFailure::CorruptState
        );
        ensure!(
            connection.execute(
                "DELETE FROM __sys_raft_outbox WHERE lane=?1 AND sequence=?2 AND generation=?3",
                params![
                    &completed.selected.lane,
                    sequence.to_be_bytes().as_slice(),
                    generation.as_slice()
                ]
            )? == 1,
            CommandFailure::CorruptState
        );
        connection.execute(
            "UPDATE __sys_raft_outbox_lanes SET settled=?2 WHERE lane=?1",
            params![&completed.selected.lane, sequence.to_be_bytes().as_slice()],
        )?;
        Ok(vec![1])
    }
}

fn install(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS __sys_raft_outbox_lanes (
        lane TEXT PRIMARY KEY CHECK(typeof(lane)='text' AND length(lane) BETWEEN 1 AND 64),
        emitted BLOB NOT NULL CHECK(typeof(emitted)='blob' AND length(emitted)=8),
        settled BLOB NOT NULL CHECK(typeof(settled)='blob' AND length(settled)=8)
    ) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS __sys_raft_outbox (
        lane TEXT NOT NULL REFERENCES __sys_raft_outbox_lanes(lane),
        sequence BLOB NOT NULL CHECK(typeof(sequence)='blob' AND length(sequence)=8),
        request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request)<=61440),
        minimum INTEGER NOT NULL CHECK(minimum IN (0,1)),
        generation BLOB NOT NULL CHECK(typeof(generation)='blob' AND length(generation)=32),
        next_at INTEGER NOT NULL CHECK(typeof(next_at)='integer' AND next_at>=0),
        PRIMARY KEY(lane,sequence)
    ) WITHOUT ROWID",
    )?;
    Ok(())
}

fn exists(connection: &Connection) -> Result<bool> {
    Ok(connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_raft_outbox')",[],|row|row.get(0))?)
}

fn number(bytes: Vec<u8>) -> Result<u64> {
    Ok(u64::from_be_bytes(
        bytes.try_into().map_err(|_| CommandFailure::CorruptState)?,
    ))
}

struct StoredIntent {
    bytes: Vec<u8>,
    minimum: Durability,
    generation: [u8; 32],
    next_at: u64,
}

fn load(connection: &Connection, lane: &str, sequence: u64) -> Result<Option<StoredIntent>> {
    let metadata:Option<(usize,i64,Vec<u8>,i64)>=connection.query_row("SELECT length(request),minimum,CASE WHEN typeof(generation)='blob' AND length(generation)=32 THEN generation ELSE NULL END,next_at FROM __sys_raft_outbox WHERE lane=?1 AND sequence=?2",params![lane,sequence.to_be_bytes().as_slice()],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
    let Some((length, minimum, generation, next_at)) = metadata else {
        return Ok(None);
    };
    ensure!(
        length <= delivery::MAX_REQUEST && next_at >= 0,
        CommandFailure::CorruptState
    );
    let minimum = match minimum {
        0 => Durability::Quorum,
        1 => Durability::S3,
        _ => return Err(CommandFailure::CorruptState.into()),
    };
    let generation = generation
        .try_into()
        .map_err(|_| CommandFailure::CorruptState)?;
    let bytes: Vec<u8> = connection.query_row(
        "SELECT request FROM __sys_raft_outbox WHERE lane=?1 AND sequence=?2",
        params![lane, sequence.to_be_bytes().as_slice()],
        |row| row.get(0),
    )?;
    ensure!(bytes.len() == length, CommandFailure::CorruptState);
    Ok(Some(StoredIntent {
        bytes,
        minimum,
        generation,
        next_at: next_at as u64,
    }))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Selected {
    pub lane: String,
    pub sequence: String,
}

impl Selected {
    fn number(&self) -> Result<u64> {
        let sequence: u64 = self.sequence.parse()?;
        ensure!(
            delivery::valid_name(&self.lane)
                && sequence > 0
                && sequence.to_string() == self.sequence,
            CommandFailure::CommandConflict
        );
        Ok(sequence)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Complete {
    selected: Selected,
    generation: [u8; 32],
    receipt: String,
}

pub(crate) fn completion_input(
    selected: Selected,
    generation: [u8; 32],
    receipt: &[u8],
) -> Result<Vec<u8>> {
    ensure!(
        receipt.len() <= delivery::MAX_RECEIPT,
        CommandFailure::Overloaded
    );
    Ok(serde_json::to_vec(&Complete {
        selected,
        generation,
        receipt: URL_SAFE_NO_PAD.encode(receipt),
    })?)
}

pub(crate) fn due(
    connection: &Connection,
    outbox: &RaftOutbox,
    now: u64,
) -> Result<Option<(Selected, Durability)>> {
    if !exists(connection)? {
        return Ok(None);
    }
    let (count,bytes,lanes):(usize,usize,usize)=connection.query_row("SELECT count(*),coalesce(sum(length(request)),0),(SELECT count(*) FROM __sys_raft_outbox_lanes) FROM __sys_raft_outbox",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
    ensure!(
        count <= outbox.limits.max_pending
            && bytes <= outbox.limits.max_total_bytes
            && lanes <= outbox.limits.max_lanes,
        CommandFailure::CorruptState
    );
    let selected:Option<(String,Vec<u8>,i64)>=connection.query_row("SELECT CASE WHEN typeof(a.lane)='text' AND length(CAST(a.lane AS BLOB)) BETWEEN 1 AND 64 THEN a.lane ELSE NULL END,CASE WHEN typeof(a.sequence)='blob' AND length(a.sequence)=8 THEN a.sequence ELSE NULL END,a.minimum FROM __sys_raft_outbox a WHERE a.next_at<=?1 AND NOT EXISTS(SELECT 1 FROM __sys_raft_outbox b WHERE b.lane=a.lane AND b.sequence<a.sequence) ORDER BY a.next_at,a.lane LIMIT 1",[i64::try_from(now)?],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?;
    selected
        .map(|(lane, sequence, minimum)| {
            ensure!(
                delivery::valid_name(&lane) && outbox.bindings.contains_key(&lane),
                CommandFailure::CorruptState
            );
            let selected = Selected {
                lane,
                sequence: number(sequence)?.to_string(),
            };
            selected.number()?;
            let minimum = match minimum {
                0 => Durability::Quorum,
                1 => Durability::S3,
                _ => return Err(CommandFailure::CorruptState.into()),
            };
            Ok((selected, minimum))
        })
        .transpose()
}

pub(crate) struct ClaimedDelivery {
    pub request: RaftDeliveryRequest,
    pub generation: [u8; 32],
    pub sink: Arc<dyn RaftEffectSink>,
}

pub(crate) fn claimed(
    outbox: &RaftOutbox,
    scope: &crate::application::consensus::GroupScope,
    schema: [u8; 32],
    selected: &Selected,
    bytes: &[u8],
) -> Result<Option<ClaimedDelivery>> {
    if bytes == [0] {
        return Ok(None);
    }
    ensure!(
        bytes.first() == Some(&1) && bytes.len() > 33 && bytes.len() <= 33 + delivery::MAX_REQUEST,
        CommandFailure::CorruptState
    );
    let binding = outbox
        .bindings
        .get(&selected.lane)
        .ok_or(CommandFailure::CorruptState)?;
    let request = delivery::restore_request(
        &bytes[33..],
        DeliveryOrigin {
            scope,
            schema,
            binding: &selected.lane,
            digest: binding.digest,
            sequence: selected.number()?,
        },
        binding.target.clone(),
        &outbox.signer,
    )?;
    let generation = bytes[1..33]
        .try_into()
        .map_err(|_| CommandFailure::CorruptState)?;
    Ok(Some(ClaimedDelivery {
        request,
        generation,
        sink: binding.sink.clone(),
    }))
}
