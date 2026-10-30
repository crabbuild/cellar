// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use raft::prelude::{ConfState, Snapshot};
use serde::{Deserialize, Serialize};

const DOMAIN: &str = "cellar-cell-group-checkpoint-1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Transition {
    Active,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Header {
    pub domain: String,
    pub root: GroupManifest,
    pub conf: Vec<u8>,
    pub membership: GroupMembership,
    pub transition: Transition,
}

impl Header {
    pub(super) fn capture(
        root: GroupManifest,
        conf: &ConfState,
        membership: GroupMembership,
    ) -> Result<Self> {
        Ok(Self {
            domain: DOMAIN.into(),
            root,
            conf: crate::raft::manifest::encode_conf(conf)
                .map_err(|_| CellGroupCheckpointError::Invalid("configuration encoding"))?,
            membership,
            transition: Transition::Active,
        })
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > HEADER_LIMIT {
            return Err(CellGroupCheckpointError::Limit);
        }
        let header: Self = serde_json::from_slice(bytes)
            .map_err(|_| CellGroupCheckpointError::Invalid("checkpoint header"))?;
        if header.domain != DOMAIN
            || header.root.membership_revision() != header.membership.revision().get()
        {
            return Err(CellGroupCheckpointError::Invalid(
                "checkpoint domain/membership",
            ));
        }
        header.root.check()?;
        header
            .membership
            .validate(&header.configuration()?, header.root.position().index.get())?;
        Ok(header)
    }

    pub(super) fn configuration(&self) -> Result<ConfState> {
        crate::raft::manifest::decode_conf(&self.conf)
            .map_err(|_| CellGroupCheckpointError::Invalid("checkpoint configuration"))
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self)
            .map_err(|_| CellGroupCheckpointError::Invalid("header encoding"))?;
        if bytes.len() > HEADER_LIMIT {
            return Err(CellGroupCheckpointError::Limit);
        }
        Ok(bytes)
    }

    pub(super) fn snapshot(&self) -> Result<Snapshot> {
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = self.root.position().index.get();
        snapshot.mut_metadata().term = self.root.position().term.get();
        snapshot
            .mut_metadata()
            .set_conf_state(self.configuration()?);
        snapshot.data = self.encode()?.into();
        Ok(snapshot)
    }
}
