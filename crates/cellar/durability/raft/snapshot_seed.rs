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
            grant: self.grant,
            descriptor: self.descriptor,
            initial: self.initial,
            pin,
            _reservation: reservation,
        })
    }
}

/// Image bytes are already synced. The engine owner must still check the opaque
/// live grant, persist the complete checkpoint atomically, and reconcile the
/// permanent member claim before exposing any runtime attachment.
pub(crate) struct PreparedSeed {
    grant: crate::raft::transport::seeds::SeedGrant,
    descriptor: Descriptor,
    initial: ::raft::prelude::ConfState,
    pin: Option<GenerationPin>,
    _reservation: crate::raft::budget::Permit,
}

impl PreparedSeed {
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
