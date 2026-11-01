// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An authenticated reply owns its S3 output until the physical writer finishes.
use super::TransportError;
use crate::application::consensus::CommandFailure;
use crate::raft::cell_groups::{CellCommandResponse, CellReceiptResponse, CellSqlEnrollment};
use crate::raft::query_wire as wire;
use cellar_logic::consensus::Durability;

enum Owner {
    Command { _response: Box<CellCommandResponse> },
    Enrollment { _response: Box<CellSqlEnrollment> },
    Receipt { _response: Box<CellReceiptResponse> },
}

pub(super) struct Reply {
    bytes: Vec<u8>,
    _owner: Option<Owner>,
}

impl Reply {
    pub(super) fn plain(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            _owner: None,
        }
    }

    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn command(
        result: Result<CellCommandResponse, CommandFailure>,
        limit: usize,
    ) -> Result<Self, TransportError> {
        let bytes = wire::command::encode_result(&result, limit)?;
        let owner = result
            .ok()
            .filter(|r| r.durability() == Durability::S3)
            .map(|response| Owner::Command {
                _response: Box::new(response),
            });
        Ok(Self {
            bytes,
            _owner: owner,
        })
    }

    pub(super) fn enrollment(
        result: Result<CellSqlEnrollment, CommandFailure>,
        limit: usize,
    ) -> Result<Self, TransportError> {
        let encoded = result
            .as_ref()
            .map(|r| (r.record(), r.durability()))
            .map_err(Clone::clone);
        let bytes = wire::enrollment::encode_result(&encoded, limit)?;
        let owner = result
            .ok()
            .filter(|r| r.durability() == Durability::S3)
            .map(|response| Owner::Enrollment {
                _response: Box::new(response),
            });
        Ok(Self {
            bytes,
            _owner: owner,
        })
    }

    pub(super) fn receipt(
        result: Result<CellReceiptResponse, CommandFailure>,
        limit: usize,
    ) -> Result<Self, TransportError> {
        let bytes = wire::receipt::encode_result(&result, limit)?;
        let owner = result
            .ok()
            .filter(|r| r.durability() == Durability::S3)
            .map(|response| Owner::Receipt {
                _response: Box::new(response),
            });
        Ok(Self {
            bytes,
            _owner: owner,
        })
    }
}

/// Fields drop in declaration order. On cancellation or task destruction, close
/// the real TLS/socket stream before refunding the proof/coverage/hold. On success
/// the caller takes the stream only after flush, then drops the consumed reply.
pub(super) struct Write<S> {
    pub stream: S,
    pub reply: Reply,
}
