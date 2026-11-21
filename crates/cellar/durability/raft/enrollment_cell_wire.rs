// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Creation payloads use a distinct authenticated RPC purpose and exact receipt.
use super::*;
use crate::raft::cell_groups::{
    CellDispatchError, CellEnrollmentError, CellEnrollmentRequest, EnrollmentRecord,
};
use crate::raft::completion::RuntimeError;
use crate::raft::execution::ExecutionError;
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

#[derive(Debug, PartialEq, Eq)]
pub(in crate::raft) struct CellEnrollmentValue {
    pub record: EnrollmentRecord,
    pub durability: Durability,
}

impl CellEnrollmentCall {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, TransportError> {
        if self.route_epoch == 0 || self.birth_digest == [0; 32] {
            return Err(TransportError::Identity);
        }
        if !(1..=3_600_000).contains(&self.timeout_ms) {
            return Err(TransportError::Protocol);
        }
        let mut body = b"CLGCEQ01".to_vec();
        body.extend_from_slice(&self.request.creation.bytes());
        body.extend_from_slice(&self.route_epoch.to_be_bytes());
        body.extend_from_slice(&self.birth_digest);
        body.push(durability_code(self.request.durability));
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
        let durability = read_durability(&mut body)?;
        let inputs = CommandInputs {
            now_unix_millis: u64::from_be_bytes(body.array()?),
            random: body.array()?,
        };
        let preview = ImageId::new(body.array()?).map_err(|_| TransportError::Identity)?;
        let call = Self {
            request: CellEnrollmentRequest {
                creation,
                durability,
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
    result: &Result<(&EnrollmentRecord, Durability), &CellEnrollmentError>,
    limit: usize,
) -> Result<Vec<u8>, TransportError> {
    let mut body = b"CLGCES01".to_vec();
    match result {
        Ok((record, durability)) => {
            let record = record.encode().map_err(|_| TransportError::Protocol)?;
            body.push(0);
            body.push(durability_code(*durability));
            body.extend_from_slice(&(record.len() as u32).to_be_bytes());
            body.extend_from_slice(&record);
        }
        Err(error) => {
            body.push(1);
            body.extend_from_slice(&error.creation.bytes());
            body.push(u8::from(error.ambiguous));
            encode_cause(error.cause.as_ref(), &mut body);
        }
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
    expected: (ProposalId, Durability),
    cell: &CellHistory,
    schema: [u8; 32],
    limit: usize,
) -> Result<Result<CellEnrollmentValue, CellEnrollmentError>, TransportError> {
    if bytes.len() > limit {
        return Err(TransportError::Limit);
    }
    let mut body = Reader(bytes);
    if body.take(8)? != b"CLGCES01" {
        return Err(TransportError::Protocol);
    }
    let result = match body.byte()? {
        0 => {
            let durability = read_durability(&mut body)?;
            if durability < expected.1 {
                return Err(TransportError::Authentication);
            }
            let length = u32::from_be_bytes(body.array()?) as usize;
            let record = EnrollmentRecord::decode(body.take(length)?)
                .map_err(|_| TransportError::Protocol)?;
            if record.creation() != expected.0 || record.cell() != cell || record.schema() != schema
            {
                return Err(TransportError::Authentication);
            }
            Ok(CellEnrollmentValue { record, durability })
        }
        1 => {
            let creation = ProposalId::new(body.array()?).map_err(|_| TransportError::Identity)?;
            if creation != expected.0 {
                return Err(TransportError::Authentication);
            }
            let ambiguous = match body.byte()? {
                0 => false,
                1 => true,
                _ => return Err(TransportError::Protocol),
            };
            Err(CellEnrollmentError {
                creation,
                ambiguous,
                cause: Box::new(decode_cause(&mut body)?),
            })
        }
        _ => return Err(TransportError::Protocol),
    };
    if !body.0.is_empty() {
        return Err(TransportError::Protocol);
    }
    Ok(result)
}

// The authenticated request/response MAC binds the route, purpose and original
// request bytes. The explicit creation binding additionally prevents a returned
// non-entry assertion from being consumed for another enrollment proposal.
fn encode_cause(cause: &CellDispatchError, body: &mut Vec<u8>) {
    let runtime = match cause {
        CellDispatchError::Runtime(RuntimeError::NotReady) => Some(1),
        CellDispatchError::Runtime(RuntimeError::NotFound) => Some(2),
        CellDispatchError::Runtime(RuntimeError::NotLeader) => Some(3),
        CellDispatchError::Runtime(RuntimeError::Overloaded) => Some(4),
        CellDispatchError::Runtime(RuntimeError::DeadlineExceeded) => Some(5),
        CellDispatchError::Runtime(RuntimeError::Closed) => Some(6),
        CellDispatchError::Runtime(RuntimeError::StaleCompletion) => Some(7),
        _ => None,
    };
    if let Some(code) = runtime {
        body.extend_from_slice(&[0, code]);
        return;
    }
    let failure = match cause {
        CellDispatchError::Execution(ExecutionError::Refused(failure)) => failure.clone(),
        CellDispatchError::Registration(_) => CommandFailure::UnsupportedCapability,
        _ => CommandFailure::Unavailable,
    };
    body.push(1);
    match failure {
        CommandFailure::CommandConflict => body.push(9),
        CommandFailure::NotLeader { leader } => {
            body.push(12);
            body.extend_from_slice(&leader.map_or(0, MemberId::get).to_be_bytes());
        }
        failure => body.push(failure_code(&failure)),
    }
}

fn decode_cause(body: &mut Reader<'_>) -> Result<CellDispatchError, TransportError> {
    match body.byte()? {
        0 => Ok(CellDispatchError::Runtime(match body.byte()? {
            1 => RuntimeError::NotReady,
            2 => RuntimeError::NotFound,
            3 => RuntimeError::NotLeader,
            4 => RuntimeError::Overloaded,
            5 => RuntimeError::DeadlineExceeded,
            6 => RuntimeError::Closed,
            7 => RuntimeError::StaleCompletion,
            _ => return Err(TransportError::Protocol),
        })),
        1 => {
            let failure = match body.byte()? {
                9 => CommandFailure::CommandConflict,
                12 => {
                    let member = u64::from_be_bytes(body.array()?);
                    CommandFailure::NotLeader {
                        leader: if member == 0 {
                            None
                        } else {
                            Some(MemberId::new(member).map_err(|_| TransportError::Identity)?)
                        },
                    }
                }
                status => decode_failure(status)?,
            };
            Ok(CellDispatchError::Execution(ExecutionError::Refused(
                failure,
            )))
        }
        _ => Err(TransportError::Protocol),
    }
}
