// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Canonical, bounded marker-1 recovery descriptions. Object names include
//! their content digest; the mutable head selects a complete immutable set.

use super::archive::{ArchiveError, ArchiveLimits};
use super::cache::validate_conf;
use crate::application::consensus::protocol::WireScope;
use ::raft::prelude::ConfState;
use cellar_logic::consensus::{GroupScope, LogIndex, LogPosition, Term};
use cellar_ltx::apply::ImageState;
use cellar_ltx::TXID;
use protobuf::Message;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectRef {
    pub kind: ObjectKind,
    pub digest: [u8; 32],
    pub length: u64,
    pub range: Option<PositionRange>,
    pub bundle: Option<super::archive::bundle::Reference>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PositionRange {
    pub first: Position,
    pub last: Position,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ObjectKind {
    Snapshot,
    Tail,
    Dependency,
    Manifest,
}

impl ObjectRef {
    pub fn new(kind: ObjectKind, bytes: &[u8], range: Option<PositionRange>) -> Self {
        Self {
            kind,
            digest: digest(bytes),
            length: bytes.len() as u64,
            range,
            bundle: None,
        }
    }

    pub fn name(&self) -> String {
        let kind = match self.kind {
            ObjectKind::Snapshot => "snapshots",
            ObjectKind::Tail => "tails",
            ObjectKind::Dependency => "dependencies",
            ObjectKind::Manifest => "manifests",
        };
        let range = self.range.map_or_else(String::new, |r| {
            format!(
                "{}-{}-{}-{}/",
                r.first.index, r.first.term, r.last.index, r.last.term
            )
        });
        format!("{kind}/{range}{}-{}", self.length, hex::encode(self.digest))
    }

    pub fn verify(&self, bytes: &[u8]) -> Result<(), ArchiveError> {
        if self.length != bytes.len() as u64 || self.digest != digest(bytes) {
            return Err(ArchiveError::Corrupt("archive object digest/length"));
        }
        Ok(())
    }

    pub fn validate_location(&self) -> Result<(), ArchiveError> {
        if let Some(bundle) = &self.bundle {
            if !matches!(self.kind, ObjectKind::Snapshot | ObjectKind::Tail) {
                return Err(ArchiveError::Corrupt("archive bundle object domain"));
            }
            bundle.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Position {
    pub index: u64,
    pub term: u64,
}

impl From<LogPosition> for Position {
    fn from(value: LogPosition) -> Self {
        Self {
            index: value.index.get(),
            term: value.term.get(),
        }
    }
}

impl Position {
    pub fn decode(self) -> Result<LogPosition, ArchiveError> {
        if self.index == 0 || self.index == u64::MAX || self.term == 0 {
            return Err(ArchiveError::Corrupt("archive position"));
        }
        Ok(LogPosition {
            index: LogIndex::new(self.index),
            term: Term::new(self.term),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Image {
    pub epoch: u64,
    pub txid: u64,
    pub checksum: u64,
    pub page_size: u32,
    pub length: u64,
    pub digest: [u8; 32],
}

impl From<ImageState> for Image {
    fn from(value: ImageState) -> Self {
        Self {
            epoch: value.epoch,
            txid: value.txid.0,
            checksum: value.checksum,
            page_size: value.page_size,
            length: value.length,
            digest: value.digest,
        }
    }
}

impl Image {
    pub fn decode(self) -> Result<ImageState, ArchiveError> {
        let state = ImageState {
            epoch: self.epoch,
            txid: TXID(self.txid),
            checksum: self.checksum,
            page_size: self.page_size,
            length: self.length,
            digest: self.digest,
        };
        state
            .validate(&super::archive::apply_limits())
            .map_err(|_| ArchiveError::Corrupt("archive image state"))?;
        if self.txid == 0 {
            return Err(ArchiveError::Corrupt("archive lacks a database image"));
        }
        Ok(state)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Snapshot {
    pub position: Position,
    #[serde(deserialize_with = "required_option")]
    pub image: Option<Image>,
    pub conf_state: Vec<u8>,
    pub membership: super::member_state::GroupMembership,
    #[serde(deserialize_with = "required_option")]
    pub object: Option<ObjectRef>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Tail {
    pub first: Position,
    pub last: Position,
    pub entries: usize,
    pub object: ObjectRef,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub scope: WireScope,
    pub root_digest: [u8; 32],
    pub birth_digest: [u8; 32],
    pub schema_digest: [u8; 32],
    pub snapshot: Snapshot,
    pub tails: Vec<Tail>,
    pub position: Position,
    #[serde(deserialize_with = "required_option")]
    pub image: Option<Image>,
    pub dependencies: Vec<ObjectRef>,
}

impl Manifest {
    pub fn validate(
        &self,
        scope: &GroupScope,
        root: [u8; 32],
        birth: [u8; 32],
        schema: [u8; 32],
        limits: ArchiveLimits,
        initial: &super::member_state::GroupMembership,
    ) -> Result<(), ArchiveError> {
        if self
            .scope
            .clone()
            .decode()
            .map_err(|_| ArchiveError::Corrupt("archive scope"))?
            != *scope
            || self.root_digest != root
            || self.birth_digest != birth
            || self.schema_digest != schema
            || !self.snapshot.membership.same_birth(initial)
        {
            return Err(ArchiveError::Corrupt("archive history/schema binding"));
        }
        self.snapshot.position.decode()?;
        self.position.decode()?;
        self.snapshot.image.map(Image::decode).transpose()?;
        self.image.map(Image::decode).transpose()?;
        if self
            .snapshot
            .image
            .is_some_and(|image| image.length > limits.max_image_bytes)
            || self
                .image
                .is_some_and(|image| image.length > limits.max_image_bytes)
        {
            return Err(ArchiveError::Limit);
        }
        let conf = decode_conf(&self.snapshot.conf_state)?;
        self.snapshot
            .membership
            .validate(&conf, self.snapshot.position.index)
            .map_err(|_| ArchiveError::Corrupt("archive member bindings"))?;
        self.snapshot
            .membership
            .validate_scope(scope)
            .map_err(|_| ArchiveError::Corrupt("archive fleet binding"))?;
        // Explicit nulls denote the canonical pre-schema state. Missing fields
        // are rejected by required_option; partial pairs or retained payloads
        // cannot be reinterpreted as an empty archive.
        let object = match (&self.snapshot.image, &self.snapshot.object, &self.image) {
            (None, None, None)
                if self.tails.is_empty()
                    && self.dependencies.is_empty()
                    && self.snapshot.position == self.position =>
            {
                return Ok(())
            }
            (Some(_), Some(object), Some(_)) => object,
            _ => return Err(ArchiveError::Corrupt("archive empty/image binding")),
        };
        object.validate_location()?;
        if self.tails.len() > limits.max_tails
            || self.dependencies.len() > limits.max_dependencies
            || object.kind != ObjectKind::Snapshot
        {
            return Err(ArchiveError::Limit);
        }
        if object.range
            != Some(PositionRange {
                first: self.snapshot.position,
                last: self.snapshot.position,
            })
        {
            return Err(ArchiveError::Corrupt("archive snapshot object range"));
        }
        let mut position = self.snapshot.position;
        let mut entries = 0usize;
        let mut total = object.length;
        for tail in &self.tails {
            tail.object.validate_location()?;
            tail.first.decode()?;
            tail.last.decode()?;
            if position.index.checked_add(1) != Some(tail.first.index)
                || tail.first.term < position.term
                || tail.last.index < tail.first.index
                || tail.last.term < tail.first.term
                || tail.last.index - tail.first.index + 1 != tail.entries as u64
                || tail.object.kind != ObjectKind::Tail
                || tail.object.range
                    != Some(PositionRange {
                        first: tail.first,
                        last: tail.last,
                    })
            {
                return Err(ArchiveError::Corrupt("archive tail coverage"));
            }
            position = tail.last;
            entries = entries
                .checked_add(tail.entries)
                .ok_or(ArchiveError::Limit)?;
            total = total
                .checked_add(tail.object.length)
                .ok_or(ArchiveError::Limit)?;
        }
        if position != self.position
            || entries > limits.max_entries
            || self.tails.is_empty() && self.image != self.snapshot.image
        {
            return Err(ArchiveError::Corrupt("archive tip coverage"));
        }
        let mut previous = None;
        for dependency in &self.dependencies {
            dependency.validate_location()?;
            if dependency.kind != ObjectKind::Dependency
                || dependency.range.is_some()
                || previous.is_some_and(|p| p >= dependency.digest)
            {
                return Err(ArchiveError::Corrupt("archive dependency order/domain"));
            }
            previous = Some(dependency.digest);
            total = total
                .checked_add(dependency.length)
                .ok_or(ArchiveError::Limit)?;
        }
        if object.length > limits.max_object_bytes as u64
            || self
                .tails
                .iter()
                .any(|r| r.object.length > limits.max_object_bytes as u64)
            || self
                .dependencies
                .iter()
                .any(|r| r.length > limits.max_object_bytes as u64)
            || total > limits.max_set_bytes as u64
        {
            return Err(ArchiveError::Limit);
        }
        Ok(())
    }
}

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

pub(crate) fn decode_conf(bytes: &[u8]) -> Result<ConfState, ArchiveError> {
    if bytes.len() > 16 * 1024 {
        return Err(ArchiveError::Limit);
    }
    let conf = ConfState::parse_from_bytes(bytes)
        .map_err(|_| ArchiveError::Corrupt("archive configuration"))?;
    if encode_conf(&conf)? != bytes {
        return Err(ArchiveError::Corrupt("noncanonical archive configuration"));
    }
    Ok(conf)
}

pub(crate) fn encode_conf(conf: &ConfState) -> Result<Vec<u8>, ArchiveError> {
    validate_conf(conf).map_err(ArchiveError::Log)?;
    if conf.get_unknown_fields().iter().next().is_some() {
        return Err(ArchiveError::Corrupt(
            "unknown archive configuration fields",
        ));
    }
    // raft-rs emits these sets in hash iteration order. Normalize archive
    // bytes so independently applied equivalent configurations compare equal.
    let mut conf = conf.clone();
    conf.voters.sort_unstable();
    conf.voters_outgoing.sort_unstable();
    conf.learners.sort_unstable();
    conf.learners_next.sort_unstable();
    conf.write_to_bytes()
        .map_err(|_| ArchiveError::Corrupt("archive configuration"))
}

const MAGIC: &[u8; 8] = b"CLRARC01";

pub(crate) fn encode(
    kind: u8,
    value: &impl Serialize,
    limit: usize,
) -> Result<Vec<u8>, ArchiveError> {
    let body = serde_json::to_vec(value).map_err(|_| ArchiveError::Corrupt("archive encoding"))?;
    if body.len().checked_add(41).is_none_or(|n| n > limit) {
        return Err(ArchiveError::Limit);
    }
    let mut bytes = Vec::with_capacity(body.len() + 41);
    bytes.extend_from_slice(MAGIC);
    bytes.push(kind);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&digest(&bytes));
    Ok(bytes)
}

pub(crate) fn decode<T: DeserializeOwned + Serialize>(
    kind: u8,
    bytes: &[u8],
    limit: usize,
) -> Result<T, ArchiveError> {
    if bytes.len() > limit {
        return Err(ArchiveError::Limit);
    }
    if bytes.len() < 41
        || &bytes[..8] != MAGIC
        || bytes[8] != kind
        || digest(&bytes[..bytes.len() - 32])[..] != bytes[bytes.len() - 32..]
    {
        return Err(ArchiveError::Corrupt("archive framing/domain/digest"));
    }
    let value = serde_json::from_slice(&bytes[9..bytes.len() - 32])
        .map_err(|_| ArchiveError::Corrupt("archive payload"))?;
    if encode(kind, &value, limit)? != bytes {
        return Err(ArchiveError::Corrupt("noncanonical archive record"));
    }
    Ok(value)
}

pub(crate) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_configuration_set_order_is_canonical() -> Result<(), ArchiveError> {
        let mut first = ConfState {
            voters: vec![9, 1, 5],
            voters_outgoing: vec![3, 9, 1],
            learners: vec![11, 7],
            learners_next: vec![3],
            auto_leave: true,
            ..Default::default()
        };
        let encoded = encode_conf(&first)?;
        let raw = first
            .write_to_bytes()
            .map_err(|_| ArchiveError::Corrupt("test configuration"))?;
        assert!(decode_conf(&raw).is_err());
        first.voters.reverse();
        first.voters_outgoing.reverse();
        first.learners.reverse();
        first.learners_next.reverse();
        assert_eq!(encoded, encode_conf(&first)?);
        let canonical = decode_conf(&encoded)?;
        assert_eq!(canonical.voters, vec![1, 5, 9]);
        assert_eq!(canonical.voters_outgoing, vec![1, 3, 9]);
        assert_eq!(canonical.learners, vec![7, 11]);
        assert_eq!(canonical.learners_next, vec![3]);
        Ok(())
    }

    #[test]
    fn archive_configuration_unknown_and_duplicate_members_refused() {
        let mut conf = ConfState {
            voters: vec![1, 1],
            ..Default::default()
        };
        assert!(encode_conf(&conf).is_err());
        conf.voters.pop();
        conf.mut_unknown_fields().add_varint(99, 1);
        assert!(encode_conf(&conf).is_err());
    }

    fn empty_manifest() -> (GroupScope, Manifest) {
        use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};
        let scope = GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
            cell: "Counter:empty".into(),
        };
        let conf = ConfState {
            voters: vec![1, 2, 3],
            ..Default::default()
        };
        let position = Position { index: 1, term: 1 };
        let manifest = Manifest {
            scope: WireScope::from(&scope),
            root_digest: [5; 32],
            birth_digest: [6; 32],
            schema_digest: [7; 32],
            snapshot: Snapshot {
                position,
                image: None,
                object: None,
                conf_state: encode_conf(&conf).unwrap(),
                membership: super::super::member_state::GroupMembership::initial(&conf),
            },
            tails: vec![],
            position,
            image: None,
            dependencies: vec![],
        };
        (scope, manifest)
    }

    #[test]
    fn archive_empty_manifest_requires_explicit_null_fields() {
        let (_, manifest) = empty_manifest();
        let value = serde_json::to_value(&manifest).unwrap();
        assert!(serde_json::from_value::<Manifest>(value.clone()).is_ok());
        for field in ["image", "object"] {
            let mut missing = value.clone();
            missing["snapshot"].as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<Manifest>(missing).is_err());
        }
        let mut missing = value;
        missing.as_object_mut().unwrap().remove("image");
        assert!(serde_json::from_value::<Manifest>(missing).is_err());
    }

    #[test]
    fn archive_empty_manifest_refuses_partial_state_and_payloads() {
        let (scope, manifest) = empty_manifest();
        let validate = |m: &Manifest| {
            m.validate(
                &scope,
                [5; 32],
                [6; 32],
                [7; 32],
                ArchiveLimits::default(),
                &manifest.snapshot.membership,
            )
        };
        assert!(validate(&manifest).is_ok());
        let mut invalid = manifest.clone();
        invalid.position.index += 1;
        assert!(validate(&invalid).is_err());
        let mut invalid = manifest.clone();
        invalid.snapshot.object = Some(ObjectRef::new(ObjectKind::Snapshot, b"", None));
        assert!(validate(&invalid).is_err());
        let mut invalid = manifest.clone();
        invalid
            .dependencies
            .push(ObjectRef::new(ObjectKind::Dependency, b"data", None));
        assert!(validate(&invalid).is_err());
        let mut invalid = manifest.clone();
        invalid.image = Some(Image {
            epoch: 1,
            txid: 0,
            checksum: 0,
            page_size: 0,
            length: 0,
            digest: [0; 32],
        });
        assert!(validate(&invalid).is_err());
    }
}
