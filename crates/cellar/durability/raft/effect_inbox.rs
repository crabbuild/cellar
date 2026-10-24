// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable receiver receipts and business changes share one command transaction.
use super::delivery::{self, RaftEffectSigningKey, RaftEffectSourceGrant};
use crate::application::consensus::{
    CommandContext, CommandFailure, Durability, OperationContract,
};
use anyhow::{ensure, Result};
use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Explicit receiver authorization and storage bounds. The latest receipt for
/// each source lane is retained; older lane sequences never execute again.
#[derive(Clone)]
pub struct RaftInbox {
    pub(crate) grants: Vec<RaftEffectSourceGrant>,
    pub(crate) signer: Arc<RaftEffectSigningKey>,
    pub(crate) durability: Durability,
    pub(crate) max_sources: usize,
}

impl RaftInbox {
    pub fn new(
        mut grants: Vec<RaftEffectSourceGrant>,
        signer: Arc<RaftEffectSigningKey>,
        durability: Durability,
        max_sources: usize,
    ) -> Result<Self> {
        ensure!(
            !grants.is_empty() && grants.len() <= 32 && (1..=128).contains(&max_sources),
            "invalid inbox limits"
        );
        grants.sort_by_key(RaftEffectSourceGrant::identity);
        ensure!(
            grants
                .windows(2)
                .all(|pair| pair[0].identity() != pair[1].identity()),
            "duplicate inbox grant"
        );
        Ok(Self {
            grants,
            signer,
            durability,
            max_sources,
        })
    }

    pub(crate) fn identity(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-inbox-1");
        hash.update(delivery::protocol_identity());
        hash.update(self.signer.verification_key().public_bytes());
        hash.update([delivery::policy(self.durability)]);
        hash.update((self.max_sources as u64).to_be_bytes());
        hash.update((self.grants.len() as u64).to_be_bytes());
        for grant in &self.grants {
            hash.update(grant.identity());
        }
        hash.finalize().into()
    }

    pub(crate) fn receive(
        &self,
        context: &CommandContext<'_>,
        contract: &OperationContract,
        input: &[u8],
        callback: &impl for<'a> Fn(&CommandContext<'a>, &[u8]) -> Result<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        ensure!(
            context.durability() >= self.durability,
            CommandFailure::UnsupportedCapability
        );
        let incoming = delivery::authenticate(
            input,
            &self.grants,
            context.identity(),
            context.definition_digest(),
            contract,
            self.signer.verification_key(),
            self.durability,
        )?;
        ensure!(
            incoming.command == context.command(),
            CommandFailure::CommandConflict
        );
        let connection = context.sql().framework();
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS __sys_raft_inbox (
            lane BLOB PRIMARY KEY CHECK(typeof(lane)='blob' AND length(lane)=32),
            sequence BLOB NOT NULL CHECK(typeof(sequence)='blob' AND length(sequence)=8),
            digest BLOB NOT NULL CHECK(typeof(digest)='blob' AND length(digest)=32),
            receipt BLOB NOT NULL CHECK(typeof(receipt)='blob' AND length(receipt)<=16384)
        ) WITHOUT ROWID",
        )?;
        let previous: Option<(Vec<u8>, Vec<u8>, usize)> = connection
            .query_row(
                "SELECT CASE WHEN typeof(sequence)='blob' AND length(sequence)=8 THEN sequence ELSE NULL END,CASE WHEN typeof(digest)='blob' AND length(digest)=32 THEN digest ELSE NULL END,length(receipt) FROM __sys_raft_inbox WHERE lane=?1",
                [incoming.lane.as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let next = if let Some((sequence, digest, size)) = previous {
            ensure!(
                size <= delivery::MAX_RECEIPT && digest.len() == 32,
                CommandFailure::CorruptState
            );
            let last = u64::from_be_bytes(
                sequence
                    .try_into()
                    .map_err(|_| CommandFailure::CorruptState)?,
            );
            if incoming.command.sequence() == last {
                ensure!(digest == incoming.digest, CommandFailure::CommandConflict);
                return Ok(connection.query_row(
                    "SELECT receipt FROM __sys_raft_inbox WHERE lane=?1",
                    [incoming.lane.as_slice()],
                    |row| row.get(0),
                )?);
            }
            ensure!(
                incoming.command.sequence() > last,
                CommandFailure::CommandExpired
            );
            last.checked_add(1).ok_or(CommandFailure::Overloaded)?
        } else {
            let count: usize =
                connection.query_row("SELECT count(*) FROM __sys_raft_inbox", [], |row| {
                    row.get(0)
                })?;
            ensure!(count < self.max_sources, CommandFailure::Overloaded);
            1
        };
        ensure!(
            incoming.command.sequence() == next,
            CommandFailure::SequenceGap { next }
        );
        let result = callback(context, &incoming.payload)?;
        let receipt = delivery::receipt(&incoming, &result, &self.signer)?;
        connection.execute("INSERT INTO __sys_raft_inbox(lane,sequence,digest,receipt) VALUES(?1,?2,?3,?4)
            ON CONFLICT(lane) DO UPDATE SET sequence=excluded.sequence,digest=excluded.digest,receipt=excluded.receipt",
            params![incoming.lane.as_slice(),incoming.command.sequence().to_be_bytes().as_slice(),incoming.digest.as_slice(),receipt])?;
        Ok(receipt)
    }
}
