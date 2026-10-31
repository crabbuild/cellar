// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receipt inspection has a distinct authenticated purpose and never stages a command.
use super::*;
use crate::raft::cell_groups::{CellReceiptRequest, CellReceiptResponse};
use cellar_logic::{
    cell_group::CellHistory,
    consensus::{CommandId, Durability},
};

pub(in crate::raft) struct CellReceiptCall {
    pub request: CellReceiptRequest,
    pub timeout_ms: u64,
    pub route_epoch: u64,
    pub birth_digest: [u8; 32],
}

pub(in crate::raft) struct CellReceiptValue {
    pub retained: bool,
    pub value: command::CellCommandValue,
}

impl CellReceiptCall {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, TransportError> {
        let r = &self.request;
        if self.route_epoch == 0 || self.birth_digest == [0; 32] {
            return Err(TransportError::Identity);
        }
        if r.contract.kind != OperationKind::Command
            || r.contract.operation.is_empty()
            || r.contract.operation.len() > 128
            || r.contract.operation.chars().any(char::is_control)
            || !(1..=3_600_000).contains(&self.timeout_ms)
            || r.durability != Durability::Quorum
        {
            return Err(TransportError::Protocol);
        }
        let mut body = b"CLGCRQ01".to_vec();
        body.extend_from_slice(&r.id.client.bytes());
        body.extend_from_slice(&r.id.sequence().to_be_bytes());
        body.extend_from_slice(&self.route_epoch.to_be_bytes());
        body.extend_from_slice(&self.birth_digest);
        string(&mut body, &r.contract.operation)?;
        body.extend_from_slice(&r.contract.digest);
        body.push(1); // Quorum; never silently lower an S3 request.
        body.extend_from_slice(&self.timeout_ms.to_be_bytes());
        let length = u32::try_from(r.input.len()).map_err(|_| TransportError::Limit)?;
        if body
            .len()
            .checked_add(4)
            .and_then(|n| n.checked_add(r.input.len()))
            .is_none_or(|n| n > limit)
        {
            return Err(TransportError::Limit);
        }
        body.extend_from_slice(&length.to_be_bytes());
        body.extend_from_slice(&r.input);
        Ok(body)
    }

    pub fn decode(bytes: &[u8], limit: usize) -> Result<Self, TransportError> {
        if bytes.len() > limit {
            return Err(TransportError::Limit);
        }
        let mut body = Reader(bytes);
        if body.take(8)? != b"CLGCRQ01" {
            return Err(TransportError::Protocol);
        }
        let id = command::command_id(&mut body)?;
        let route_epoch = u64::from_be_bytes(body.array()?);
        let birth_digest = body.array()?;
        if route_epoch == 0 || birth_digest == [0; 32] {
            return Err(TransportError::Identity);
        }
        let operation = body.string(128)?;
        if operation.is_empty() || operation.chars().any(char::is_control) {
            return Err(TransportError::Protocol);
        }
        let contract = OperationContract {
            operation,
            kind: OperationKind::Command,
            digest: body.array()?,
        };
        if body.byte()? != 1 {
            return Err(TransportError::Protocol);
        }
        let timeout_ms = u64::from_be_bytes(body.array()?);
        if !(1..=3_600_000).contains(&timeout_ms) {
            return Err(TransportError::Protocol);
        }
        let length = u32::from_be_bytes(body.array()?) as usize;
        let input = body.take(length)?.to_vec();
        if !body.0.is_empty() {
            return Err(TransportError::Protocol);
        }
        Ok(Self {
            request: CellReceiptRequest {
                id,
                contract,
                input,
                durability: Durability::Quorum,
            },
            timeout_ms,
            route_epoch,
            birth_digest,
        })
    }
}

pub(in crate::raft) fn encode_result(
    result: &Result<CellReceiptResponse, CommandFailure>,
    limit: usize,
) -> Result<Vec<u8>, TransportError> {
    let mut body = b"CLGCRS01".to_vec();
    match result {
        Ok(response) => {
            body.push(if response.receipt().is_some() { 0 } else { 13 });
            body.extend_from_slice(&response.command().client.bytes());
            body.extend_from_slice(&response.command().sequence().to_be_bytes());
            string(&mut body, response.token().as_str())?;
            if let Some(receipt) = response.receipt() {
                body.extend_from_slice(&receipt.retry_until_ms().to_be_bytes());
                let len =
                    u32::try_from(receipt.result().len()).map_err(|_| TransportError::Limit)?;
                if body
                    .len()
                    .checked_add(4)
                    .and_then(|n| n.checked_add(receipt.result().len()))
                    .is_none_or(|n| n > limit)
                {
                    return Err(TransportError::Limit);
                }
                body.extend_from_slice(&len.to_be_bytes());
                body.extend_from_slice(receipt.result());
            }
        }
        Err(CommandFailure::CommandConflict) => body.push(9),
        Err(CommandFailure::CommandExpired) => body.push(10),
        Err(CommandFailure::SequenceGap { next }) => {
            body.push(11);
            body.extend_from_slice(&next.to_be_bytes());
        }
        Err(CommandFailure::NotLeader { leader }) => {
            body.push(12);
            body.extend_from_slice(&leader.map_or(0, MemberId::get).to_be_bytes());
        }
        Err(error) => body.push(failure_code(error)),
    }
    if body.len() > limit {
        return Err(TransportError::Limit);
    }
    Ok(body)
}

pub(in crate::raft) fn decode_result(
    bytes: &[u8],
    expected: (CommandId, &CellHistory),
    schema: [u8; 32],
    keys: &VersionKeys,
    now: u64,
    limit: usize,
    served_by: MemberId,
) -> Result<Result<CellReceiptValue, CommandFailure>, TransportError> {
    if bytes.len() > limit {
        return Err(TransportError::Limit);
    }
    let mut body = Reader(bytes);
    if body.take(8)? != b"CLGCRS01" {
        return Err(TransportError::Protocol);
    }
    let result = match body.byte()? {
        status @ (0 | 13) => {
            let command = command::command_id(&mut body)?;
            if command != expected.0 {
                return Err(TransportError::Authentication);
            }
            let token = crate::raft::version::CellVersionToken::from_encoded(&body.string(4096)?)
                .map_err(|_| TransportError::Authentication)?;
            let observation = keys
                .verify_cell(&token, expected.1, schema, now)
                .map_err(|_| TransportError::Authentication)?;
            let retained = status == 0;
            let (retry_until_ms, result) = if retained {
                let retry = u64::from_be_bytes(body.array()?);
                let len = u32::from_be_bytes(body.array()?) as usize;
                (retry, body.take(len)?.to_vec())
            } else {
                (0, vec![])
            };
            Ok(CellReceiptValue {
                retained,
                value: command::CellCommandValue {
                    command,
                    retry_until_ms,
                    value: cell::CellQueryValue {
                        result,
                        observation,
                        token,
                        served_by,
                    },
                },
            })
        }
        9 => Err(CommandFailure::CommandConflict),
        10 => Err(CommandFailure::CommandExpired),
        11 => Err(CommandFailure::SequenceGap {
            next: u64::from_be_bytes(body.array()?),
        }),
        12 => {
            let id = u64::from_be_bytes(body.array()?);
            Err(CommandFailure::NotLeader {
                leader: if id == 0 {
                    None
                } else {
                    Some(MemberId::new(id).map_err(|_| TransportError::Identity)?)
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
