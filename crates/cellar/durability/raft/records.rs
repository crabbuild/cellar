// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Marker-1 local metadata, atomically stored with its Raft log changes.

use super::cache::{CachedGroup, CommittedImage, DurableApply};
use super::storage::{GroupBootstrap, LogError};
use ::raft::prelude::{ConfState, HardState, Snapshot};
use cellar_logic::cell_group::GroupHistory;
use cellar_logic::consensus::{
    ClusterId, DiskLineage, EngineInstanceId, GroupId, GroupScope, IncarnationId, LogIndex,
    LogPosition, NamespaceId, NodeId, Term,
};
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
    domain: MappingDomain,
    history: PhysicalHistory,
    // Temporary application adapter metadata, never the physical registry key.
    // Removed with the remaining single-cell host consumers at public cutover.
    application_cell: String,
    pub member: u64,
    pub birth_digest: [u8; 32],
    pub initial_conf: Vec<u8>,
}

impl MappingRecord {
    pub fn new(key: u64, bootstrap: &GroupBootstrap, conf: &ConfState) -> Result<Self, LogError> {
        Ok(Self {
            format: 1,
            key,
            domain: MappingDomain::Group,
            history: PhysicalHistory::from(GroupHistory::from(&bootstrap.scope)),
            application_cell: bootstrap.scope.cell.clone(),
            member: bootstrap.member.get(),
            birth_digest: bootstrap.birth_digest,
            initial_conf: conf.write_to_bytes().map_err(corrupt)?,
        })
    }

    pub fn application_scope(&self) -> Result<GroupScope, LogError> {
        let history = self.history.decode()?;
        let scope = GroupScope {
            cluster: history.cluster,
            namespace: history.namespace,
            group: history.group,
            incarnation: history.incarnation,
            cell: self.application_cell.clone(),
        };
        scope.validate().map_err(corrupt)?;
        Ok(scope)
    }

    pub fn catalog_key(&self) -> Result<Vec<u8>, LogError> {
        let scope = self.history.decode()?;
        let mut key = b"g:".to_vec();
        key.extend_from_slice(&scope.group.bytes());
        key.extend_from_slice(&scope.incarnation.bytes());
        Ok(key)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
enum MappingDomain {
    #[serde(rename = "cellar-group-mapping-1")]
    Group,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalHistory {
    cluster: [u8; 16],
    namespace: [u8; 16],
    group: [u8; 16],
    incarnation: [u8; 16],
}

impl From<GroupHistory> for PhysicalHistory {
    fn from(history: GroupHistory) -> Self {
        Self {
            cluster: history.cluster.bytes(),
            namespace: history.namespace.bytes(),
            group: history.group.bytes(),
            incarnation: history.incarnation.bytes(),
        }
    }
}

impl PhysicalHistory {
    fn decode(&self) -> Result<GroupHistory, LogError> {
        Ok(GroupHistory {
            cluster: ClusterId::new(self.cluster).map_err(corrupt)?,
            namespace: NamespaceId::new(self.namespace).map_err(corrupt)?,
            group: GroupId::new(self.group).map_err(corrupt)?,
            incarnation: IncarnationId::new(self.incarnation).map_err(corrupt)?,
        })
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
    pub application: ApplicationRecord,
}

// Explicit domains refuse the old bare-image record shape. This staged image
// variant is removed with legacy runtime consumers at the public hard cutover.
#[derive(Serialize, Deserialize)]
#[serde(tag = "domain", content = "state", deny_unknown_fields)]
pub(crate) enum ApplicationRecord {
    #[serde(rename = "cellar-image-apply-1")]
    Image(Option<CommittedImage>),
    #[serde(rename = "cellar-group-apply-1")]
    Group(Box<super::cell_groups::GroupManifest>),
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
            application: match &cache.group_manifest {
                Some(marker) => ApplicationRecord::Group(Box::new(marker.clone())),
                None => ApplicationRecord::Image(cache.applied.image.clone()),
            },
        })
    }

    pub fn metadata_cache(&self) -> Result<CachedGroup, LogError> {
        if self.mapping.format != 1 {
            return Err(LogError::Corrupt("unsupported group marker".into()));
        }
        let (image, group_manifest) = match &self.application {
            ApplicationRecord::Image(image) => (image.clone(), None),
            ApplicationRecord::Group(marker) => {
                let scope = self.mapping.application_scope()?;
                if !marker.matches_group(&scope, self.mapping.birth_digest) {
                    return Err(LogError::WrongIdentity);
                }
                (None, Some(marker.as_ref().clone()))
            }
        };
        Ok(CachedGroup {
            hard_state: HardState::parse_from_bytes(&self.hard_state).map_err(corrupt)?,
            conf_state: ConfState::parse_from_bytes(&self.conf_state).map_err(corrupt)?,
            membership: self.membership.clone(),
            snapshot: Snapshot::parse_from_bytes(&self.snapshot).map_err(corrupt)?,
            entries: Default::default(),
            group_manifest,
            manifest_pins: None,
            applied: DurableApply {
                position: LogPosition {
                    index: LogIndex::new(self.applied_index),
                    term: Term::new(self.applied_term),
                },
                image,
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
