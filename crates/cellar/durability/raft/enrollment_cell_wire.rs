// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Creation payloads use a distinct authenticated RPC purpose and exact receipt.
use super::*;
use crate::raft::cell_groups::{CellEnrollmentRequest, EnrollmentRecord};
use crate::storage::CommandInputs;
use cellar_logic::{
    cell_group::CellHistory,
    consensus::{Durability, ProposalId},
};
use cellar_ltx::generation::ImageId;

pub(in crate::raft) struct CellEnrollmentCall {
    pub request: CellEnrollmentRequest,
    pub inputs: CommandInputs,
    pub preview: ImageId,
    pub timeout_ms: u64,
    pub route_epoch: u64,
    pub birth_digest: [u8; 32],
}

impl CellEnrollmentCall {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, TransportError> {
        if self.route_epoch == 0 || self.birth_digest == [0; 32] {
            return Err(TransportError::Identity);
        }
        if !(1..=3_600_000).contains(&self.timeout_ms)
            || self.request.durability != Durability::Quorum
        {
            return Err(TransportError::Protocol);
        }
        let mut body = b"CLGCEQ01".to_vec();
        body.extend_from_slice(&self.request.creation.bytes());
        body.extend_from_slice(&self.route_epoch.to_be_bytes());
        body.extend_from_slice(&self.birth_digest);
        body.push(1);
        body.extend_from_slice(&self.inputs.now_unix_millis.to_be_bytes());
        body.extend_from_slice(&self.inputs.random);
        body.extend_from_slice(&self.preview.bytes());
        body.extend_from_slice(&self.timeout_ms.to_be_bytes());
        if body.len() > limit {
            return Err(TransportError::Limit);
        }
        Ok(body)
    }

    pub fn decode(bytes: &[u8], limit: usize) -> Result<Self, TransportError> {
        if bytes.len() > limit {
            return Err(TransportError::Limit);
        }
        let mut body = Reader(bytes);
        if body.take(8)? != b"CLGCEQ01" {
            return Err(TransportError::Protocol);
        }
        let creation = ProposalId::new(body.array()?).map_err(|_| TransportError::Identity)?;
        let route_epoch = u64::from_be_bytes(body.array()?);
        let birth_digest = body.array()?;
        if body.byte()? != 1 {
            return Err(TransportError::Protocol);
        }
        let inputs = CommandInputs {
            now_unix_millis: u64::from_be_bytes(body.array()?),
            random: body.array()?,
        };
        let preview = ImageId::new(body.array()?).map_err(|_| TransportError::Identity)?;
        let call = Self {
            request: CellEnrollmentRequest {
                creation,
                durability: Durability::Quorum,
            },
            inputs,
            preview,
            route_epoch,
            birth_digest,
            timeout_ms: u64::from_be_bytes(body.array()?),
        };
        if !body.0.is_empty() {
            return Err(TransportError::Protocol);
        }
        // Fixed-size encoding validates the same domain constraints in both directions.
        call.encode(limit)?;
        Ok(call)
    }
}

pub(in crate::raft) fn encode_result(
    result: &Result<EnrollmentRecord, CommandFailure>,
    limit: usize,
) -> Result<Vec<u8>, TransportError> {
    let mut body = b"CLGCES01".to_vec();
    match result {
        Ok(record) => {
            let record = record.encode().map_err(|_| TransportError::Protocol)?;
            body.push(0);
            body.extend_from_slice(&(record.len() as u32).to_be_bytes());
            body.extend_from_slice(&record);
        }
        Err(CommandFailure::CommandConflict) => body.push(9),
        Err(CommandFailure::NotLeader { leader }) => {
            body.push(12);
            body.extend_from_slice(&leader.map_or(0, MemberId::get).to_be_bytes());
        }
        Err(failure) => body.push(failure_code(failure)),
    }
    if body.len() > limit {
        return Err(TransportError::Limit);
    }
    Ok(body)
}

/// Only the authenticated exchange may call this decoder. A record alone is
/// creation data, never a signed version token or proof of the current cell image.
pub(in crate::raft) fn decode_result(
    bytes: &[u8],
    creation: ProposalId,
    cell: &CellHistory,
    schema: [u8; 32],
    limit: usize,
) -> Result<Result<EnrollmentRecord, CommandFailure>, TransportError> {
    if bytes.len() > limit {
        return Err(TransportError::Limit);
    }
    let mut body = Reader(bytes);
    if body.take(8)? != b"CLGCES01" {
        return Err(TransportError::Protocol);
    }
    let result = match body.byte()? {
        0 => {
            let length = u32::from_be_bytes(body.array()?) as usize;
            let record = EnrollmentRecord::decode(body.take(length)?)
                .map_err(|_| TransportError::Protocol)?;
            if record.creation() != creation || record.cell() != cell || record.schema() != schema {
                return Err(TransportError::Authentication);
            }
            Ok(record)
        }
        9 => Err(CommandFailure::CommandConflict),
        12 => {
            let member = u64::from_be_bytes(body.array()?);
            Err(CommandFailure::NotLeader {
                leader: if member == 0 {
                    None
                } else {
                    Some(MemberId::new(member).map_err(|_| TransportError::Identity)?)
                },
            })
        }
        status => Err(decode_failure(status)?),
    };
    if !body.0.is_empty() {
        return Err(TransportError::Protocol);
    }
    Ok(result)
}
