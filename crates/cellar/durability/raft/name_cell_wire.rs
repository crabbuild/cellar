// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Logical-name discovery returns enrollment metadata, never invocation authority.

use super::*;
use crate::raft::cell_groups::EnrollmentRecord;
use cellar_logic::cell_group::CellHistory;

pub(in crate::raft) struct CellNameCall {
    pub timeout_ms: u64,
    pub route_epoch: u64,
    pub birth_digest: [u8; 32],
}

impl CellNameCall {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, TransportError> {
        if self.route_epoch == 0 || self.birth_digest == [0; 32] {
            return Err(TransportError::Identity);
        }
        if !(1..=60_000).contains(&self.timeout_ms) {
            return Err(TransportError::Protocol);
        }
        let mut bytes = b"CLGCNQ01".to_vec();
        bytes.extend_from_slice(&self.timeout_ms.to_be_bytes());
        bytes.extend_from_slice(&self.route_epoch.to_be_bytes());
        bytes.extend_from_slice(&self.birth_digest);
        if bytes.len() > limit {
            return Err(TransportError::Limit);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8], limit: usize) -> Result<Self, TransportError> {
        if bytes.len() > limit {
            return Err(TransportError::Limit);
        }
        let mut reader = Reader(bytes);
        if reader.take(8)? != b"CLGCNQ01" {
            return Err(TransportError::Protocol);
        }
        let call = Self {
            timeout_ms: u64::from_be_bytes(reader.array()?),
            route_epoch: u64::from_be_bytes(reader.array()?),
            birth_digest: reader.array()?,
        };
        if !reader.0.is_empty() {
            return Err(TransportError::Protocol);
        }
        call.encode(limit)?;
        Ok(call)
    }
}

pub(in crate::raft) fn encode_result(
    result: &Result<Option<EnrollmentRecord>, CommandFailure>,
    limit: usize,
) -> Result<Vec<u8>, TransportError> {
    let mut bytes = b"CLGCNS01".to_vec();
    match result {
        Ok(None) => bytes.push(0),
        Ok(Some(record)) => {
            bytes.push(9);
            let record = record.encode().map_err(|_| TransportError::Protocol)?;
            bytes.extend_from_slice(&(record.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&record);
        }
        Err(error) => bytes.push(failure_code(error)),
    }
    if bytes.len() > limit {
        return Err(TransportError::Limit);
    }
    Ok(bytes)
}

/// The authenticated route binds the logical name; its candidate incarnation
/// deliberately need not equal the selected, potentially recovered incarnation.
/// Consumers must obtain fresh command/query authority for the returned history.
pub(in crate::raft) fn decode_result(
    bytes: &[u8],
    cell: &CellHistory,
    schema: [u8; 32],
    limit: usize,
) -> Result<Result<Option<EnrollmentRecord>, CommandFailure>, TransportError> {
    if bytes.len() > limit {
        return Err(TransportError::Limit);
    }
    let mut reader = Reader(bytes);
    if reader.take(8)? != b"CLGCNS01" {
        return Err(TransportError::Protocol);
    }
    let result = match reader.byte()? {
        0 => Ok(None),
        9 => {
            let length = u32::from_be_bytes(reader.array()?) as usize;
            let record = EnrollmentRecord::decode(reader.take(length)?)
                .map_err(|_| TransportError::Protocol)?;
            let selected = record.cell();
            if selected.cluster != cell.cluster
                || selected.namespace != cell.namespace
                || selected.cell != cell.cell
                || record.schema() != schema
            {
                return Err(TransportError::Authentication);
            }
            Ok(Some(record))
        }
        status => Err(decode_failure(status)?),
    };
    if !reader.0.is_empty() {
        return Err(TransportError::Protocol);
    }
    Ok(result)
}
