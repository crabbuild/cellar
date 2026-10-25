// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical seeding for one fresh, live-authorized learner.

use super::*;

/// A fresh learner's physical download. This cannot be constructed from an
/// archive, a cached membership observation, or a caller-supplied snapshot.
pub(crate) struct SeedDownload {
    grant: crate::raft::transport::seeds::SeedGrant,
    descriptor: Descriptor,
    initial: ::raft::prelude::ConfState,
    limits: SnapshotLimits,
    bytes: Vec<u8>,
}

impl SeedDownload {
    pub(crate) fn scope(&self) -> &GroupScope {
        &self.grant.local().scope
    }

    pub(crate) fn new(
        grant: crate::raft::transport::seeds::SeedGrant,
        birth: &crate::raft::catalog::GroupBirth,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        let mut initial = ::raft::prelude::ConfState::default();
        initial.set_voters(birth.voters().iter().map(|m| m.member.get()).collect());
        initial.set_learners(birth.learners().iter().map(|m| m.member.get()).collect());
        let descriptor = Descriptor::decode_bound(
            birth.scope(),
            birth.digest(),
            &crate::raft::member_state::GroupMembership::initial(&initial),
            schema,
            grant.snapshot(),
            limits,
        )?;
        let local = grant.local();
        let conf = grant.snapshot().get_metadata().get_conf_state();
        let joined = descriptor
            .membership
            .joined(local.member)?
            .ok_or(SnapshotError::Invalid("seed member not joined"))?;
        if local.scope != *birth.scope()
            || joined.1 > grant.barrier()
            || !descriptor.membership.permits(local)
            || !conf.learners.contains(&local.member.get())
            || conf.voters.contains(&local.member.get())
            || conf.voters_outgoing.contains(&local.member.get())
            || conf.learners_next.contains(&local.member.get())
            || descriptor.index < grant.barrier().get()
        {
            return Err(SnapshotError::Invalid("seed learner/barrier binding"));
        }
        Ok(Self {
            grant,
            descriptor,
            initial,
            limits,
            bytes: Vec::new(),
        })
    }

    pub(crate) fn encoded_length(&self) -> usize {
        self.descriptor.encoded_length
    }

    pub(crate) fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        if offset != self.bytes.len()
            || bytes.is_empty()
            || bytes.len() > self.limits.max_chunk_bytes
            || offset
                .checked_add(bytes.len())
                .is_none_or(|end| end > self.descriptor.encoded_length)
        {
            return Err(SnapshotError::Limit);
        }
        self.bytes
            .try_reserve_exact(self.descriptor.encoded_length - self.bytes.len())
            .map_err(|_| SnapshotError::Limit)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn finish(
        self,
        images: &GenerationStore,
        reservation: crate::raft::budget::Permit,
    ) -> Result<PreparedSeed, SnapshotError> {
        let pin = seal_image(&self.descriptor, &self.bytes, self.limits, images)?;
        Ok(PreparedSeed {
            grant: SeedPermit::Peer(self.grant),
            descriptor: self.descriptor,
            initial: self.initial,
            pin,
            _reservation: std::sync::Arc::new(reservation),
        })
    }
}

/// Image bytes are already synced. The engine owner must still check the opaque
/// live grant, persist the complete checkpoint atomically, and reconcile the
/// permanent member claim before exposing any runtime attachment.
enum SeedPermit {
    Peer(crate::raft::transport::seeds::SeedGrant),
    S3(crate::raft::transport::seeds::ArchiveSeedGrant),
}

impl SeedPermit {
    fn local(&self) -> &cellar_logic::consensus::ReplicaIdentity {
        match self {
            Self::Peer(grant) => grant.local(),
            Self::S3(grant) => grant.local(),
        }
    }

    fn authorized(
        &self,
        owner: &std::sync::Arc<()>,
        identity: &cellar_logic::consensus::ReplicaIdentity,
    ) -> bool {
        match self {
            Self::Peer(grant) => grant.authorized(owner, identity),
            Self::S3(grant) => grant.authorized(owner, identity),
        }
    }
}

pub(crate) struct PreparedSeed {
    grant: SeedPermit,
    descriptor: Descriptor,
    initial: ::raft::prelude::ConfState,
    pin: Option<GenerationPin>,
    _reservation: std::sync::Arc<crate::raft::budget::Permit>,
}

impl PreparedSeed {
    pub(crate) fn membership(&self) -> &crate::raft::member_state::GroupMembership {
        &self.descriptor.membership
    }

    pub(crate) fn local(&self) -> &cellar_logic::consensus::ReplicaIdentity {
        self.grant.local()
    }

    pub(crate) fn authorized(
        &self,
        owner: &std::sync::Arc<()>,
        identity: &cellar_logic::consensus::ReplicaIdentity,
    ) -> bool {
        self.grant.authorized(owner, identity)
    }

    pub(crate) fn bootstrap(&self) -> Result<crate::raft::storage::GroupBootstrap, LogError> {
        Ok(crate::raft::storage::GroupBootstrap {
            scope: self.local().scope.clone(),
            member: self.local().member,
            birth_digest: self.descriptor.birth,
            voters: self
                .initial
                .voters
                .iter()
                .map(|id| {
                    cellar_logic::consensus::MemberId::new(*id).map_err(|_| LogError::WrongIdentity)
                })
                .collect::<Result<Vec<_>, _>>()?,
            learners: self
                .initial
                .learners
                .iter()
                .map(|id| {
                    cellar_logic::consensus::MemberId::new(*id).map_err(|_| LogError::WrongIdentity)
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    pub(crate) fn initial(&self) -> &::raft::prelude::ConfState {
        &self.initial
    }

    pub(crate) fn cache(&self) -> Result<crate::raft::cache::CachedGroup, SnapshotError> {
        let snapshot = self.descriptor.snapshot()?;
        let hard_state = ::raft::prelude::HardState {
            term: self.descriptor.term,
            commit: self.descriptor.index,
            ..Default::default()
        };
        Ok(crate::raft::cache::CachedGroup {
            hard_state,
            conf_state: snapshot.get_metadata().get_conf_state().clone(),
            membership: self.descriptor.membership.clone(),
            snapshot,
            entries: std::collections::BTreeMap::new(),
            applied: crate::raft::cache::DurableApply {
                position: LogPosition {
                    index: LogIndex::new(self.descriptor.index),
                    term: Term::new(self.descriptor.term),
                },
                image: self.pin.as_ref().map(marker),
            },
        })
    }

    pub(crate) fn retain_pending(&self) -> Result<(), SnapshotError> {
        if let Some(pin) = &self.pin {
            if !pin.retain_reference(PENDING_SNAPSHOT_REFERENCE, self.descriptor.index)? {
                return Err(SnapshotError::Invalid("stale seed image"));
            }
        }
        Ok(())
    }

    pub(crate) fn retain(&self) -> Result<(), SnapshotError> {
        if let Some(pin) = &self.pin {
            pin.retain_reference(SNAPSHOT_REFERENCE, self.descriptor.index)?;
            pin.release_reference(PENDING_SNAPSHOT_REFERENCE, self.descriptor.index)?;
        }
        Ok(())
    }
}

impl PreparedSeed {
    pub(crate) fn from_archive(
        grant: crate::raft::transport::seeds::ArchiveSeedGrant,
        set: crate::raft::archive::ArchiveRecoverySet,
        birth: &crate::raft::catalog::GroupBirth,
        schema: [u8; 32],
        limits: SnapshotLimits,
        images: &GenerationStore,
        reservation: std::sync::Arc<crate::raft::budget::Permit>,
    ) -> Result<Self, SnapshotError> {
        limits.validate()?;
        if set.reference() != *grant.reference()
            || set.scope() != birth.scope()
            || set.schema_digest() != schema
            || set.birth_digest() != birth.digest()
            || grant.local().scope != *birth.scope()
        {
            return Err(SnapshotError::Invalid(
                "archive seed reference/history/schema",
            ));
        }
        let position = set.position();
        let conf = set
            .configuration()
            .map_err(|_| SnapshotError::Invalid("archive seed configuration"))?;
        let membership = set.membership();
        membership.validate(&conf, position.index.get())?;
        if position.index < grant.barrier()
            || !membership.permits(grant.local())
            || membership
                .joined(grant.local().member)?
                .is_none_or(|(_, at)| at > grant.barrier())
            || !conf.learners.contains(&grant.local().member.get())
            || conf.voters.contains(&grant.local().member.get())
            || conf.voters_outgoing.contains(&grant.local().member.get())
            || conf.learners_next.contains(&grant.local().member.get())
        {
            return Err(SnapshotError::Invalid(
                "archive seed learner/barrier binding",
            ));
        }
        let mut initial = ::raft::prelude::ConfState::default();
        initial.set_voters(birth.voters().iter().map(|m| m.member.get()).collect());
        initial.set_learners(birth.learners().iter().map(|m| m.member.get()).collect());
        if !membership.same_birth(&crate::raft::member_state::GroupMembership::initial(
            &initial,
        )) {
            return Err(SnapshotError::Invalid("archive seed birth members"));
        }
        let mut hash = Sha256::new();
        hash.update(b"cellar-archive-seed-image-1");
        hash.update(
            serde_json::to_vec(grant.reference())
                .map_err(|_| SnapshotError::Invalid("archive seed reference encoding"))?,
        );
        hash.update(grant.local().node.bytes());
        hash.update(grant.local().disk.bytes());
        hash.update(grant.local().process.bytes());
        let digest: [u8; 32] = hash.finalize().into();
        let nonce = cellar_ltx::generation::ImageId::new(
            digest[..16]
                .try_into()
                .map_err(|_| SnapshotError::Invalid("archive seed image nonce"))?,
        )?;
        let pin = set
            .restore_state(images, nonce)
            .map_err(SnapshotError::Archive)?;
        let bytes = pin
            .as_ref()
            .map(|pin| encode_image(pin, limits))
            .transpose()?
            .unwrap_or_default();
        let descriptor = Descriptor {
            format: 1,
            scope: WireScope::from(birth.scope()),
            birth: birth.digest(),
            schema,
            index: position.index.get(),
            term: position.term.get(),
            conf: manifest::encode_conf(&conf)
                .map_err(|_| SnapshotError::Invalid("archive seed configuration encoding"))?,
            membership,
            image: pin.as_ref().map(marker),
            encoded_length: bytes.len(),
            encoded_digest: Sha256::digest(&bytes).into(),
        };
        Descriptor::decode_bound(
            birth.scope(),
            birth.digest(),
            &crate::raft::member_state::GroupMembership::initial(&initial),
            schema,
            &descriptor.snapshot()?,
            limits,
        )?;
        Ok(Self {
            grant: SeedPermit::S3(grant),
            descriptor,
            initial,
            pin,
            _reservation: reservation,
        })
    }
}
