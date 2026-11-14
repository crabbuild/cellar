// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One persistent client stream per database, independent of node incarnations.
//! A consumed fingerprint can prove a competing request owns a sequence. Missing
//! or pruned receipts never establish that a submitted request cannot commit.

use anyhow::{ensure, Result};
use cellar_logic::consensus::ClientId;
use rusqlite::{Connection, OptionalExtension};

pub(super) const OPERATION: &str = "__sqlIngressSequence";

pub(super) fn native_digest() -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(b"cellar-native-cell-ingress-sequence-1").into()
}

const CLIENT: [u8; 16] = *b"SQLIngressLane01";

pub(crate) fn client() -> ClientId {
    ClientId::new(CLIENT).expect("nonzero compiled SQL ingress identity")
}

pub(crate) struct Observation {
    pub next: u64,
    consumed: Option<[u8; 32]>,
}

impl Observation {
    pub fn decode(input: &[u8]) -> Result<Self> {
        ensure!(
            input.len() == 9 || input.len() == 41,
            "SQL sequence reply length"
        );
        let next = u64::from_be_bytes(input[..8].try_into()?);
        ensure!(next != 0, "SQL sequence reply overflow");
        let consumed = match (input[8], input.len()) {
            (0, 9) => None,
            (1, 41) => Some(input[9..].try_into()?),
            _ => anyhow::bail!("SQL sequence reply marker"),
        };
        Ok(Self { next, consumed })
    }

    /// The observation must come from a fresh quorum query of the selected
    /// history. This proof never authorizes retry of an absent/unknown request.
    pub fn consumed_other(&self, sequence: u64, fingerprint: [u8; 32]) -> bool {
        self.next > sequence && self.consumed.is_some_and(|actual| actual != fingerprint)
    }
}

pub(crate) fn observe(
    connection: &Connection,
    cell_sequence: u64,
    input: &[u8],
) -> Result<Vec<u8>> {
    let sequence = u64::from_be_bytes(input.try_into()?);
    let tables: u8 = connection.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('__sys_raft_clients','__sys_raft_receipts')",
        [], |row| row.get(0),
    )?;
    if tables == 0 && cell_sequence == 1 {
        let mut output = 1u64.to_be_bytes().to_vec();
        output.push(0);
        return Ok(output);
    }
    ensure!(tables == 2, super::CommandFailure::CorruptState);
    let state: Option<(Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT last_sequence,expired_through FROM __sys_raft_clients WHERE client_id=?1",
            [CLIENT.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let next = match state {
        None => 1,
        Some((last, floor)) => {
            let last = u64::from_be_bytes(last.as_slice().try_into()?);
            let floor = u64::from_be_bytes(floor.as_slice().try_into()?);
            ensure!(
                last != 0 && floor <= last,
                super::CommandFailure::CorruptState
            );
            last.checked_add(1)
                .ok_or(super::CommandFailure::Overloaded)?
        }
    };
    let consumed: Option<Vec<u8>> =
        if sequence == 0 {
            None
        } else {
            connection.query_row(
            "SELECT request_digest FROM __sys_raft_receipts WHERE client_id=?1 AND sequence=?2",
            rusqlite::params![CLIENT.as_slice(), sequence.to_be_bytes().as_slice()],
            |row| row.get(0),
        ).optional()?
        };
    let mut output = next.to_be_bytes().to_vec();
    output.push(u8::from(consumed.is_some()));
    if let Some(digest) = consumed {
        ensure!(
            digest.len() == 32 && sequence < next,
            super::CommandFailure::CorruptState
        );
        output.extend(digest);
    }
    Ok(output)
}

#[cfg(test)]
#[path = "sql_ingress_identity_tests.rs"]
mod tests;
