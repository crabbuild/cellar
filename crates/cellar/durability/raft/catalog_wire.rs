// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded marker-1 catalog records; independent domains for each immutable key.

use super::catalog::{CatalogError, InitialMember, NamespaceConfig, VoterPolicy};
use crate::application::consensus::protocol::WireScope;
use cellar_logic::consensus::{ClusterId, MemberId, NamespaceId, NodeId};
use cellar_logic::NamespaceMode;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"CLRCAT01";
const PREFIX: usize = 13;
const TRAILER: usize = 32;
pub(super) const ROOT: u8 = 1;
pub(super) const BIRTH: u8 = 2;
pub(super) const CLAIM: u8 = 3;
pub(super) const RESERVATION: u8 = 4;
pub(super) const VERSION_KEYS: u8 = 5;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct VersionKeys {
    pub root_digest: [u8; 32],
    pub keys: Vec<VersionKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct VersionKey {
    pub public: [u8; 32],
    pub verify_until_ms: u64,
}

pub(super) fn encode(
    kind: u8,
    value: &impl Serialize,
    limit: usize,
) -> Result<Vec<u8>, CatalogError> {
    let payload =
        serde_json::to_vec(value).map_err(|_| CatalogError::Corrupt("catalog serialization"))?;
    let length = PREFIX
        .checked_add(payload.len())
        .and_then(|n| n.checked_add(TRAILER))
        .ok_or(CatalogError::Limit)?;
    if length > limit {
        return Err(CatalogError::Limit);
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(MAGIC);
    bytes.push(kind);
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&Sha256::digest(&bytes));
    Ok(bytes)
}

pub(super) fn decode<T: DeserializeOwned + Serialize>(
    kind: u8,
    bytes: &[u8],
    limit: usize,
) -> Result<T, CatalogError> {
    if bytes.len() > limit {
        return Err(CatalogError::Limit);
    }
    if bytes.len() < PREFIX + TRAILER || &bytes[..8] != MAGIC || bytes[8] != kind {
        return Err(CatalogError::Corrupt("catalog framing/domain"));
    }
    let length = u32::from_be_bytes(
        bytes[9..13]
            .try_into()
            .map_err(|_| CatalogError::Corrupt("catalog length"))?,
    ) as usize;
    let end = PREFIX.checked_add(length).ok_or(CatalogError::Limit)?;
    if end.checked_add(TRAILER) != Some(bytes.len())
        || Sha256::digest(&bytes[..end])[..] != bytes[end..]
    {
        return Err(CatalogError::Corrupt("catalog length/digest"));
    }
    let value: T = serde_json::from_slice(&bytes[PREFIX..end])
        .map_err(|_| CatalogError::Corrupt("catalog payload"))?;
    if encode(kind, &value, limit)? != bytes {
        return Err(CatalogError::Corrupt("noncanonical catalog record"));
    }
    Ok(value)
}

pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Mode {
    Fleet,
    Raft,
}

impl From<NamespaceMode> for Mode {
    fn from(mode: NamespaceMode) -> Self {
        match mode {
            NamespaceMode::Fleet => Self::Fleet,
            NamespaceMode::Raft => Self::Raft,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Root {
    pub cluster: [u8; 16],
    pub namespace: [u8; 16],
    pub mode: Mode,
    pub schema_digest: [u8; 32],
    pub protocol_digest: [u8; 32],
    pub storage_prefix: String,
    pub minimum_s3: bool,
    pub single_voter_development: bool,
    #[serde(deserialize_with = "required_nullable")]
    pub managed_fleet: Option<ManagedOwner>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManagedOwner {
    fleet: [u8; 16],
    catalog: [u8; 32],
}

impl Root {
    pub fn new(config: &NamespaceConfig, prefix: String) -> Self {
        Self {
            cluster: config.cluster.bytes(),
            namespace: config.namespace.bytes(),
            mode: config.mode.into(),
            schema_digest: config.schema_digest,
            protocol_digest: crate::application::consensus::protocol::protocol_identity(),
            storage_prefix: prefix,
            minimum_s3: config.minimum_durability == cellar_logic::consensus::Durability::S3,
            single_voter_development: config.voters == VoterPolicy::SingleVoterDevelopment,
            managed_fleet: config.managed_fleet.as_ref().map(|owner| ManagedOwner {
                fleet: owner.fleet,
                catalog: owner.catalog,
            }),
        }
    }

    pub fn validate(&self) -> Result<(), CatalogError> {
        ClusterId::new(self.cluster).map_err(|_| CatalogError::Corrupt("root cluster"))?;
        NamespaceId::new(self.namespace).map_err(|_| CatalogError::Corrupt("root namespace"))?;
        if let Some(owner) = &self.managed_fleet {
            cellar_logic::consensus::IncarnationId::new(owner.fleet)
                .map_err(|_| CatalogError::Corrupt("root fleet"))?;
            if owner.catalog == [0; 32] || self.mode != Mode::Raft || self.single_voter_development
            {
                return Err(CatalogError::Corrupt("managed namespace policy"));
            }
        }
        if self.schema_digest == [0; 32]
            || self.protocol_digest != crate::application::consensus::protocol::protocol_identity()
        {
            return Err(CatalogError::Corrupt("namespace schema/protocol"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_refuses_earlier_protocols() {
        let config = NamespaceConfig::new(
            ClusterId::new([1; 16]).unwrap(),
            NamespaceId::new([2; 16]).unwrap(),
            cellar_logic::NamespaceMode::Raft,
            [3; 32],
        );
        let mut root = Root::new(&config, "test".into());
        root.validate().unwrap();
        for protocol in [
            "cellar-raft-ltx-1",
            "cellar-raft-ltx-1/members-1",
            "cellar-raft-ltx-1/members-1/seeds-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1/fleet-stop-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1/capacity-readiness-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1/capacity-readiness-1/resource-reservations-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1/capacity-readiness-1/resource-reservations-1/physical-inventory-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1/capacity-readiness-1/resource-reservations-1/physical-inventory-1/fleet-removal-1",
            "cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1/fleet-1/enrollment-1/node-probes-1/birth-admission-1/managed-genesis-1/group-reservations-1/birth-inventory-1/namespace-import-1/fleet-namespaces-1/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1/capacity-readiness-1/resource-reservations-1/physical-inventory-1/fleet-removal-1/resource-movement-1",
        ] {
            root.protocol_digest = Sha256::digest(protocol.as_bytes()).into();
            let encoded = encode(ROOT, &root, 64 * 1024).unwrap();
            let decoded: Root = decode(ROOT, &encoded, 64 * 1024).unwrap();
            assert!(matches!(
                decoded.validate(),
                Err(CatalogError::Corrupt("namespace schema/protocol"))
            ));
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Member {
    pub member: u64,
    pub node: [u8; 16],
}

impl Member {
    pub fn encode(value: InitialMember) -> Self {
        Self {
            member: value.member.get(),
            node: value.node.bytes(),
        }
    }

    pub fn decode(&self) -> Result<InitialMember, CatalogError> {
        Ok(InitialMember {
            member: MemberId::new(self.member)
                .map_err(|_| CatalogError::Corrupt("member identity"))?,
            node: NodeId::new(self.node).map_err(|_| CatalogError::Corrupt("member node"))?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Birth {
    pub root_digest: [u8; 32],
    pub scope: WireScope,
    pub voters: Vec<Member>,
    pub learners: Vec<Member>,
    pub empty_digest: [u8; 32],
    #[serde(deserialize_with = "required_nullable")]
    pub managed_owner: Option<super::catalog::ManagedBirthOwner>,
    #[serde(deserialize_with = "required_nullable")]
    pub cancelled_by: Option<[u8; 16]>,
}

fn required_nullable<'de, T, D>(decoder: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Option::<T>::deserialize(decoder)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Reservation {
    pub root_digest: [u8; 32],
    pub scope: WireScope,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Claim {
    pub root_digest: [u8; 32],
    pub scope: WireScope,
    pub member: u64,
    pub disk: [u8; 16],
    pub birth_digest: [u8; 32],
}
