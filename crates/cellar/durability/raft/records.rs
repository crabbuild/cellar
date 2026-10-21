//! Marker-1 local metadata, atomically stored with its Raft log changes.

use super::cache::{CachedGroup, CommittedImage, DurableApply};
use super::storage::{GroupBootstrap, LogError};
use crate::application::consensus::protocol::WireScope;
use ::raft::prelude::{ConfState, HardState, Snapshot};
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, LogIndex, LogPosition, NodeId, Term};
use protobuf::Message;
use serde::{Deserialize, Serialize};

pub(crate) const ROOT_KEY: &[u8] = b"cellar-root";
pub(crate) const GROUP_KEY: &[u8] = b"cellar-group";
pub(crate) const MAX_RECORD_BYTES: usize = 512 * 1024;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeRecord {
    pub format: u32,
    pub node: String,
    pub disk: String,
    pub storage_instance: [u8; 16],
}

impl NodeRecord {
    pub fn new(node: NodeId, disk: DiskLineage, instance: EngineInstanceId) -> Self {
        Self {
            format: 1,
            node: hex::encode(node.bytes()),
            disk: hex::encode(disk.bytes()),
            storage_instance: instance.bytes(),
        }
    }

    pub fn instance(&self, node: NodeId, disk: DiskLineage) -> Result<EngineInstanceId, LogError> {
        if self.format != 1
            || self.node != hex::encode(node.bytes())
            || self.disk != hex::encode(disk.bytes())
        {
            return Err(LogError::WrongIdentity);
        }
        EngineInstanceId::new(self.storage_instance).map_err(|_| LogError::WrongIdentity)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RootRecord {
    pub identity: NodeRecord,
    pub next_key: u64,
    pub sequence: u64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MappingRecord {
    pub format: u32,
    pub key: u64,
    pub scope: WireScope,
    pub member: u64,
    pub birth_digest: [u8; 32],
    pub initial_conf: Vec<u8>,
}

impl MappingRecord {
    pub fn new(key: u64, bootstrap: &GroupBootstrap, conf: &ConfState) -> Result<Self, LogError> {
        Ok(Self {
            format: 1,
            key,
            scope: WireScope::from(&bootstrap.scope),
            member: bootstrap.member.get(),
            birth_digest: bootstrap.birth_digest,
            initial_conf: conf.write_to_bytes().map_err(corrupt)?,
        })
    }

    pub fn catalog_key(&self) -> Result<Vec<u8>, LogError> {
        let scope = self.scope.clone().decode().map_err(corrupt)?;
        let mut key = b"g:".to_vec();
        key.extend_from_slice(&scope.group.bytes());
        key.extend_from_slice(&scope.incarnation.bytes());
        Ok(key)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupRecord {
    pub mapping: MappingRecord,
    pub hard_state: Vec<u8>,
    pub conf_state: Vec<u8>,
    pub membership: super::member_state::GroupMembership,
    pub snapshot: Vec<u8>,
    pub last_index: u64,
    pub applied_index: u64,
    pub applied_term: u64,
    pub image: Option<CommittedImage>,
}

impl GroupRecord {
    pub fn from_cache(mapping: MappingRecord, cache: &CachedGroup) -> Result<Self, LogError> {
        Ok(Self {
            mapping,
            hard_state: cache.hard_state.write_to_bytes().map_err(corrupt)?,
            conf_state: cache.conf_state.write_to_bytes().map_err(corrupt)?,
            membership: cache.membership.clone(),
            snapshot: cache.snapshot.write_to_bytes().map_err(corrupt)?,
            last_index: cache.last_index(),
            applied_index: cache.applied.position.index.get(),
            applied_term: cache.applied.position.term.get(),
            image: cache.applied.image.clone(),
        })
    }

    pub fn metadata_cache(&self) -> Result<CachedGroup, LogError> {
        if self.mapping.format != 1 {
            return Err(LogError::Corrupt("unsupported group marker".into()));
        }
        Ok(CachedGroup {
            hard_state: HardState::parse_from_bytes(&self.hard_state).map_err(corrupt)?,
            conf_state: ConfState::parse_from_bytes(&self.conf_state).map_err(corrupt)?,
            membership: self.membership.clone(),
            snapshot: Snapshot::parse_from_bytes(&self.snapshot).map_err(corrupt)?,
            entries: Default::default(),
            applied: DurableApply {
                position: LogPosition {
                    index: LogIndex::new(self.applied_index),
                    term: Term::new(self.applied_term),
                },
                image: self.image.clone(),
            },
        })
    }
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, LogError> {
    let bytes = serde_json::to_vec(value).map_err(corrupt)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(LogError::Overloaded);
    }
    Ok(bytes)
}

pub(crate) fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, LogError> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(LogError::Corrupt("oversized Raft metadata".into()));
    }
    serde_json::from_slice(bytes).map_err(corrupt)
}

pub(crate) fn corrupt(error: impl std::fmt::Display) -> LogError {
    LogError::Corrupt(error.to_string())
}
