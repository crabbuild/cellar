// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Logical cell query payloads on the existing authenticated query framing.
use super::*;
use crate::raft::cell_groups::{CellQueryConsistency, CellQueryRequest, CellQueryResponse};
use crate::raft::version::{CellVersionObservation, CellVersionToken};
use cellar_logic::{cell_group::CellHistory, consensus::ProposalId};

pub(in crate::raft) struct CellQueryCall {
    pub request: CellQueryRequest,
    pub timeout_ms: u64,
    pub route_epoch: u64,
    pub birth_digest: [u8; 32],
}

pub(in crate::raft) struct CellQueryValue {
    pub result: Vec<u8>,
    pub observation: CellVersionObservation,
    pub token: CellVersionToken,
    pub served_by: MemberId,
}

impl CellQueryCall {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, TransportError> {
        let request = &self.request;
        if self.route_epoch == 0 || self.birth_digest == [0; 32] {
            return Err(TransportError::Identity);
        }
        if request.contract.kind != OperationKind::Query
            || request.contract.operation.is_empty()
            || request.contract.operation.len() > 128
            || request.contract.operation.chars().any(char::is_control)
            || !(1..=3_600_000).contains(&self.timeout_ms)
        {
            return Err(TransportError::Protocol);
        }
        let mut body = b"CLGCQQ01".to_vec();
        body.extend_from_slice(&request.invocation.bytes());
        body.extend_from_slice(&self.route_epoch.to_be_bytes());
        body.extend_from_slice(&self.birth_digest);
        string(&mut body, &request.contract.operation)?;
        body.extend_from_slice(&request.contract.digest);
        match &request.consistency {
            CellQueryConsistency::Stale => body.push(0),
            CellQueryConsistency::Linearizable => body.push(1),
            CellQueryConsistency::AtLeast(token) => {
                body.push(2);
                string(&mut body, token.as_str())?;
            }
        }
        body.extend_from_slice(&self.timeout_ms.to_be_bytes());
        let length = u32::try_from(request.input.len()).map_err(|_| TransportError::Limit)?;
        if body
            .len()
            .checked_add(4)
            .and_then(|n| n.checked_add(request.input.len()))
            .is_none_or(|n| n > limit)
        {
            return Err(TransportError::Limit);
        }
        body.extend_from_slice(&length.to_be_bytes());
        body.extend_from_slice(&request.input);
        Ok(body)
    }

    pub fn decode(bytes: &[u8], limit: usize) -> Result<Self, TransportError> {
        if bytes.len() > limit {
            return Err(TransportError::Limit);
        }
        let mut body = Reader(bytes);
        if body.take(8)? != b"CLGCQQ01" {
            return Err(TransportError::Protocol);
        }
        let invocation = ProposalId::new(body.array()?).map_err(|_| TransportError::Identity)?;
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
            kind: OperationKind::Query,
            digest: body.array()?,
        };
        let consistency = match body.byte()? {
            0 => CellQueryConsistency::Stale,
            1 => CellQueryConsistency::Linearizable,
            2 => CellQueryConsistency::AtLeast(
                CellVersionToken::from_encoded(&body.string(4096)?)
                    .map_err(|_| TransportError::Authentication)?,
            ),
            _ => return Err(TransportError::Protocol),
        };
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
            request: CellQueryRequest {
                invocation,
                contract,
                input,
                consistency,
            },
            timeout_ms,
            route_epoch,
            birth_digest,
        })
    }
}

pub(in crate::raft) fn encode_cell_result(
    result: &Result<CellQueryResponse, CommandFailure>,
    limit: usize,
) -> Result<Vec<u8>, TransportError> {
    let mut body = b"CLGCQS01".to_vec();
    match result {
        Ok(value) => {
            body.push(0);
            string(&mut body, value.token().as_str())?;
            let length = u32::try_from(value.result().len()).map_err(|_| TransportError::Limit)?;
            if body
                .len()
                .checked_add(4)
                .and_then(|n| n.checked_add(value.result().len()))
                .is_none_or(|n| n > limit)
            {
                return Err(TransportError::Limit);
            }
            body.extend_from_slice(&length.to_be_bytes());
            body.extend_from_slice(value.result());
        }
        Err(error) => body.push(failure_code(error)),
    }
    if body.len() > limit {
        return Err(TransportError::Limit);
    }
    Ok(body)
}

pub(in crate::raft) fn decode_cell_result(
    bytes: &[u8],
    cell: &CellHistory,
    schema: [u8; 32],
    keys: &VersionKeys,
    now: u64,
    limit: usize,
    served_by: MemberId,
) -> Result<Result<CellQueryValue, CommandFailure>, TransportError> {
    if bytes.len() > limit {
        return Err(TransportError::Limit);
    }
    let mut body = Reader(bytes);
    if body.take(8)? != b"CLGCQS01" {
        return Err(TransportError::Protocol);
    }
    let status = body.byte()?;
    let result = if status == 0 {
        let token = CellVersionToken::from_encoded(&body.string(4096)?)
            .map_err(|_| TransportError::Authentication)?;
        let observation = keys
            .verify_cell(&token, cell, schema, now)
            .map_err(|_| TransportError::Authentication)?;
        let length = u32::from_be_bytes(body.array()?) as usize;
        Ok(CellQueryValue {
            result: body.take(length)?.to_vec(),
            observation,
            token,
            served_by,
        })
    } else {
        Err(decode_failure(status)?)
    };
    if !body.0.is_empty() {
        return Err(TransportError::Protocol);
    }
    Ok(result)
}
