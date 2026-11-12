// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private idempotent retirement has its own authenticated request purpose.
use super::*;
use crate::raft::cell_groups::CellCommandResponse;
use crate::raft::execution::binding_seal;
use crate::raft::leader_advice::MutationDisposition;
use cellar_logic::{
    cell_group::CellHistory,
    consensus::{ClientId, CommandId},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft) struct Call {
    pub request: binding_seal::Request,
    pub timeout_ms: u64,
    pub route_epoch: u64,
    pub birth_digest: [u8; 32],
}

impl Call {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, TransportError> {
        self.validate()?;
        let mut bytes = b"CLGBRQ01".to_vec();
        bytes.extend(serde_json::to_vec(self).map_err(|_| TransportError::Protocol)?);
        if bytes.len() > limit.min(8192) {
            return Err(TransportError::Limit);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8], limit: usize) -> Result<Self, TransportError> {
        if bytes.len() > limit.min(8192) {
            return Err(TransportError::Limit);
        }
        if !bytes.starts_with(b"CLGBRQ01") {
            return Err(TransportError::Protocol);
        }
        let value: Self =
            serde_json::from_slice(&bytes[8..]).map_err(|_| TransportError::Protocol)?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), TransportError> {
        if self.route_epoch == 0
            || self.birth_digest == [0; 32]
            || !(1..=60_000).contains(&self.timeout_ms)
            || self.request.format != 1
            || !self.request.client.starts_with(b"RCB1")
            || self.request.sequence == 0
            || !(1..=2).contains(&self.request.durability)
            || self.request.operation.is_empty()
            || self.request.operation.len() > 128
        {
            return Err(TransportError::Protocol);
        }
        Ok(())
    }
}

fn digest(request: &binding_seal::Request) -> Result<[u8; 32], TransportError> {
    Ok(Sha256::digest(serde_json::to_vec(request).map_err(|_| TransportError::Protocol)?).into())
}

pub(in crate::raft) fn encode_reply(
    result: &Result<CellCommandResponse, CommandFailure>,
    request: &binding_seal::Request,
    limit: usize,
) -> Result<Vec<u8>, TransportError> {
    let mut bytes = b"CLGBRS01".to_vec();
    bytes.extend_from_slice(&digest(request)?);
    let id = match result {
        Ok(response) => response
            .local_receipt()
            .ok_or(TransportError::Protocol)?
            .receipt
            .command(),
        Err(_) => CommandId::new(
            ClientId::new(request.client).map_err(|_| TransportError::Protocol)?,
            request.sequence,
        )
        .map_err(|_| TransportError::Protocol)?,
    };
    bytes.extend_from_slice(&id.client.bytes());
    bytes.extend_from_slice(&id.sequence().to_be_bytes());
    let remaining = limit
        .checked_sub(bytes.len())
        .ok_or(TransportError::Limit)?;
    bytes.extend(super::command::encode_reply(
        result,
        if result.is_ok() {
            MutationDisposition::Applied
        } else {
            MutationDisposition::AdmittedOrUnknown
        },
        None,
        remaining,
    )?);
    Ok(bytes)
}

pub(in crate::raft) fn decode_reply(
    bytes: &[u8],
    context: (
        &binding_seal::Request,
        &CellHistory,
        [u8; 32],
        &VersionKeys,
        u64,
        usize,
        MemberId,
    ),
) -> Result<super::command::CellCommandValue, TransportError> {
    let (request, cell, schema, keys, now, limit, served_by) = context;
    if bytes.len() > limit {
        return Err(TransportError::Limit);
    }
    let mut reader = Reader(bytes);
    if reader.take(8)? != b"CLGBRS01" || reader.array::<32>()? != digest(request)? {
        return Err(TransportError::Authentication);
    }
    let id = super::command::command_id(&mut reader)?;
    let minimum = match request.durability {
        1 => Durability::Quorum,
        2 => Durability::S3,
        _ => return Err(TransportError::Protocol),
    };
    let reply = super::command::decode_reply(
        reader.0,
        (id, cell, minimum),
        schema,
        keys,
        now,
        limit,
        served_by,
    )?;
    let value = reply.result.map_err(TransportError::Command)?;
    if id.client != crate::raft::effects::system_client(cell).map_err(TransportError::Command)?
        || value.value.result.len() != 1
        || value.value.result[0] > 2
    {
        return Err(TransportError::Authentication);
    }
    Ok(value)
}
