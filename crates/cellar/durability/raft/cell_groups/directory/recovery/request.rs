// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistable operator intent. No mutable-head selection or authority is
//! reconstructed from a fence receipt or an untrusted archive locator.
use super::*;
use crate::raft::cell_groups::CellGroupArchiveHold;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    namespace: [u8; 32],
    source: Vec<u8>,
    replacement: Vec<u8>,
    mutation: Vec<u8>,
    hold: CellGroupArchiveHold,
}

/// Persist before submission, including the dedicated remote hold. Never
/// release that hold while the intent or any recovery user remains live.
/// Replacement cell histories use the replacement group's fresh incarnation;
/// old witnesses remain provenance, not authority in the replacement history.
pub struct CellGroupRecoveryRequest {
    bytes: Vec<u8>,
    source: CellGroupBirthRequest,
    replacement: CellGroupBirthRequest,
    mutation: DirectoryMutation,
    hold: CellGroupArchiveHold,
}

impl CellGroupRecoveryRequest {
    pub fn encode(&self) -> &[u8] {
        &self.bytes
    }

    pub fn digest(&self) -> [u8; 32] {
        record::digest(&self.bytes)
    }

    pub fn source(&self) -> &CellGroupBirthRequest {
        &self.source
    }

    pub fn replacement(&self) -> &CellGroupBirthRequest {
        &self.replacement
    }

    pub fn hold(&self) -> &CellGroupArchiveHold {
        &self.hold
    }

    pub fn operation(&self) -> ProposalId {
        self.mutation.operation()
    }

    pub(super) fn mutation(&self) -> &DirectoryMutation {
        &self.mutation
    }
}

/// Whole-group binding for `DisasterFenceAuthority`. It is created only after
/// complete archive verification and a durable topology/namespace claim.
pub struct CellGroupDisasterFenceRequest {
    recovery: CellGroupRecoveryRequest,
}

impl CellGroupDisasterFenceRequest {
    pub fn recovery(&self) -> &CellGroupRecoveryRequest {
        &self.recovery
    }

    pub fn digest(&self) -> [u8; 32] {
        self.recovery.digest()
    }

    pub(super) fn new(recovery: CellGroupRecoveryRequest) -> Self {
        Self { recovery }
    }

    pub(super) fn into_recovery(self) -> CellGroupRecoveryRequest {
        self.recovery
    }
}

impl RangeDirectory {
    /// Prepare one exact static-group rollback. This allocates no metadata and
    /// grants no fence, startup or serving permission. The hold must cover the
    /// selected birth; callers explicitly choose its recovery point.
    pub fn prepare_group_recovery(
        &self,
        view: &DirectoryView,
        operation: ProposalId,
        source: &CellGroupBirth,
        replacement: &CellGroupBirthRequest,
        hold: CellGroupArchiveHold,
    ) -> Result<CellGroupRecoveryRequest, CatalogError> {
        self.unmanaged_birth()?;
        self.check_view(view)?;
        if !Arc::ptr_eq(&source.owner, &self.namespace.inner.owner) {
            return Err(CatalogError::Conflict("foreign recovery source birth"));
        }
        if view.head.recovery.is_some() {
            return Err(CatalogError::Fenced);
        }
        let mutation = self.prepare_replacement(
            view,
            operation,
            std::slice::from_ref(source.route()),
            std::slice::from_ref(replacement.route()),
        )?;
        let bytes = record::encode(
            wire::GROUP_RECOVERY,
            &Request {
                namespace: self.namespace.root_digest,
                source: source.request.encode().to_vec(),
                replacement: replacement.encode().to_vec(),
                mutation: mutation.encode().to_vec(),
                hold,
            },
            wire::LIMIT,
        )?;
        self.decode_group_recovery(&bytes)
    }

    /// Decode exactly the persisted intent under this namespace. A new catalog
    /// owner may resume it; it cannot reinterpret or rebase its selected cut.
    pub fn decode_group_recovery(
        &self,
        bytes: &[u8],
    ) -> Result<CellGroupRecoveryRequest, CatalogError> {
        self.unmanaged_birth()?;
        let value: Request = record::decode(wire::GROUP_RECOVERY, bytes, wire::LIMIT)?;
        if value.namespace != self.namespace.root_digest {
            return Err(CatalogError::Conflict("group recovery namespace"));
        }
        let source = self.decode_group_birth(&value.source)?;
        let replacement = self.decode_group_birth(&value.replacement)?;
        let mutation = self.decode_mutation(&value.mutation)?;
        let old = source.route();
        let new = replacement.route();
        if mutation.request.expected.is_none()
            || mutation.request.parents.len() != 1
            || mutation.request.children.len() != 1
            || mutation.request.parents[0].decode(self)? != *old
            || mutation.request.children[0].decode(self)? != *new
            || value.hold.marker().route() != old
            || old.range != new.range
            || old.history.group == new.history.group
            || old.history.incarnation == new.history.incarnation
            || source.creation() == replacement.creation()
        {
            return Err(CatalogError::Conflict("group recovery identities or cut"));
        }
        for member in replacement.voters().iter().chain(replacement.learners()) {
            if source
                .voters()
                .iter()
                .chain(source.learners())
                .any(|old| old.node == member.node || old.member == member.member)
            {
                return Err(CatalogError::Conflict(
                    "reused initial recovery member or node",
                ));
            }
        }
        Ok(CellGroupRecoveryRequest {
            bytes: bytes.to_vec(),
            source,
            replacement,
            mutation,
            hold: value.hold,
        })
    }
}
