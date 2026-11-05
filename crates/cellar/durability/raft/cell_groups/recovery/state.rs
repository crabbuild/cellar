// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Applied recovery lifecycle. A source intent is provenance, never authority
//! to serve its replacement. Only replicated activation opens admission.
use crate::raft::cell_groups::{GroupManifest, ManifestError};
use cellar_logic::{cell_group::TransitionId, consensus::*};
use serde::{Deserialize, Serialize};

/// The immutable request commits to the held source archive and all its original
/// cell witnesses. Retain that source hold while this provenance is referenced;
/// no old witness is installed as authority in the replacement history.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft::cell_groups) struct RecoveryState {
    request: [u8; 32],
    operation: [u8; 16],
    #[serde(deserialize_with = "required_activation")]
    activation: Option<Activation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft::cell_groups) struct Activation {
    publication: [u8; 32],
    manifest: [u8; 32],
    index: u64,
    term: u64,
}

impl Activation {
    pub(in crate::raft::cell_groups) fn new(
        publication: [u8; 32],
        manifest: [u8; 32],
        position: LogPosition,
    ) -> Self {
        Self {
            publication,
            manifest,
            index: position.index.get(),
            term: position.term.get(),
        }
    }

    pub(in crate::raft::cell_groups) fn parts(&self) -> ([u8; 32], [u8; 32], LogPosition) {
        (
            self.publication,
            self.manifest,
            LogPosition {
                index: LogIndex::new(self.index),
                term: Term::new(self.term),
            },
        )
    }

    pub(in crate::raft::cell_groups) fn validate(
        &self,
        position: LogPosition,
    ) -> Result<(), ManifestError> {
        if self.publication == [0; 32]
            || self.manifest == [0; 32]
            || self.index <= 1
            || self.term <= 1
            || self.index > position.index.get()
            || self.term > position.term.get()
            || (self.index == position.index.get() && self.term != position.term.get())
        {
            return Err(ManifestError::Invalid("activation position"));
        }
        Ok(())
    }
}

fn required_activation<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Activation>, D::Error> {
    Option::deserialize(decoder)
}

impl RecoveryState {
    pub(in crate::raft::cell_groups) fn pending(
        request: [u8; 32],
        operation: ProposalId,
    ) -> Result<Self, ManifestError> {
        if request == [0; 32] {
            return Err(ManifestError::Invalid("recovery request digest"));
        }
        Ok(Self {
            request,
            operation: operation.bytes(),
            activation: None,
        })
    }

    pub(in crate::raft::cell_groups) fn request(&self) -> [u8; 32] {
        self.request
    }

    pub(in crate::raft::cell_groups) fn active(&self) -> bool {
        self.activation.is_some()
    }

    pub(in crate::raft::cell_groups) fn activation_manifest(&self) -> Option<[u8; 32]> {
        self.activation
            .as_ref()
            .map(|activation| activation.manifest)
    }

    pub(in crate::raft::cell_groups) fn activation(
        &self,
    ) -> Option<([u8; 32], [u8; 32], LogPosition)> {
        self.activation.as_ref().map(Activation::parts)
    }

    pub(in crate::raft::cell_groups) fn succeeds(&self, previous: &Self) -> bool {
        self.request == previous.request
            && self.operation == previous.operation
            && previous
                .activation
                .as_ref()
                .is_none_or(|activation| self.activation.as_ref() == Some(activation))
    }

    pub(in crate::raft::cell_groups) fn validate(
        &self,
        position: LogPosition,
    ) -> Result<(), ManifestError> {
        if self.request == [0; 32] || ProposalId::new(self.operation).is_err() {
            return Err(ManifestError::Invalid("recovery identity"));
        }
        if let Some(activation) = &self.activation {
            activation.validate(position)?;
        }
        Ok(())
    }

    pub(in crate::raft::cell_groups) fn activate(
        &mut self,
        transition: TransitionId,
        publication: [u8; 32],
        manifest: [u8; 32],
        expected_manifest: [u8; 32],
        position: LogPosition,
    ) -> Result<(), ManifestError> {
        if transition.bytes() != self.operation
            || publication == [0; 32]
            || manifest == [0; 32]
            || manifest != expected_manifest
        {
            return Err(ManifestError::Invalid("recovery activation binding"));
        }
        if let Some(existing) = &self.activation {
            if existing.publication != publication || existing.manifest != manifest {
                return Err(ManifestError::Conflict);
            }
            return self.validate(position);
        }
        let next = Self {
            activation: Some(Activation::new(publication, manifest, position)),
            ..self.clone()
        };
        next.validate(position)?;
        *self = next;
        Ok(())
    }

    pub(in crate::raft::cell_groups) fn seed_commitment(
        &self,
        marker: &GroupManifest,
    ) -> Result<[u8; 32], ManifestError> {
        use sha2::{Digest, Sha256};
        if self.active()
            || marker.membership_revision() != 1
            || marker.recovery.as_deref() != Some(self)
        {
            return Err(ManifestError::Invalid("recovery seed commitment phase"));
        }
        let mut genesis = marker.clone();
        genesis.position = LogPosition {
            index: LogIndex::new(1),
            term: Term::new(1),
        };
        genesis.check()?;
        let bytes = serde_json::to_vec(&genesis)
            .map_err(|_| ManifestError::Invalid("recovery seed encoding"))?;
        let mut digest = Sha256::new();
        digest.update(b"cellar-cell-group-recovery-genesis-1\0");
        digest.update(bytes);
        Ok(digest.finalize().into())
    }
}
