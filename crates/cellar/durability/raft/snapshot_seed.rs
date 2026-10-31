// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical seeding for one fresh, live-authorized learner.

use super::*;
#[path = "snapshot_disaster.rs"]
mod disaster;
pub(crate) use disaster::DisasterSeed;

/// A fresh learner's physical download. This cannot be constructed from an
/// archive, a cached membership observation, or a caller-supplied snapshot.
pub(crate) struct SeedDownload {
    grant: crate::raft::transport::seeds::SeedGrant,
    descriptor: Descriptor,
    initial: ::raft::prelude::ConfState,
    payload: SnapshotPayload,
}

impl SeedDownload {
    /// Transport admission covers codec buffers until sealing or discard.
    pub(crate) fn with_io_profile(mut self, profile: stream_snapshot::IoProfile) -> Self {
        self.payload.set_io_profile(profile);
        self
    }

    pub(crate) fn scope(&self) -> &GroupScope {
        &self.grant.local().scope
    }

    pub(crate) fn new(
        grant: crate::raft::transport::seeds::SeedGrant,
        birth: &crate::raft::catalog::GroupBirth,
        schema: [u8; 32],
        limits: SnapshotLimits,
        storage: DownloadStorage<'_>,
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
        let payload = if let Some(store) = storage.extents.filter(|_| descriptor.image.is_some()) {
            SnapshotPayload::in_extents(&descriptor, limits, store)?
        } else {
            SnapshotPayload::new(
                &descriptor,
                limits,
                storage.filesystem,
                storage.directory,
                storage.scratch,
            )?
        };
        Ok(Self {
            grant,
            payload,
            descriptor,
            initial,
        })
    }

    pub(crate) fn transfer_length(&self) -> usize {
        self.descriptor.transfer_length
    }

    pub(crate) fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        self.payload.append(offset, bytes)
    }

    pub(crate) fn finish(
        self,
        images: &GenerationStore,
        reservation: crate::raft::budget::Permit,
    ) -> Result<PreparedSeed, SnapshotError> {
        let pin = seal_image(&self.descriptor, self.payload.finish()?, images)?;
        Ok(PreparedSeed {
            grant: SeedPermit::Peer(self.grant),
            descriptor: self.descriptor,
            initial: self.initial,
            pin: pin.map(SnapshotPin::Materialized),
            _reservation: std::sync::Arc::new(reservation),
        })
    }

    pub(crate) fn finish_root(
        self,
        store: &cellar_ltx::internal::NodeExtentStore,
        reservation: crate::raft::budget::Permit,
    ) -> Result<PreparedSeed, SnapshotError> {
        let pin = self.payload.finish_root(store)?;
        if pin.as_ref().map(SnapshotPin::marker) != self.descriptor.image {
            return Err(SnapshotError::Invalid("seed root marker binding"));
        }
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
    Disaster(crate::raft::catalog::DisasterSeedPermit),
}

impl SeedPermit {
    fn local(&self) -> &cellar_logic::consensus::ReplicaIdentity {
        match self {
            Self::Peer(grant) => grant.local(),
            Self::S3(grant) => grant.local(),
            Self::Disaster(grant) => &grant.local,
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
            Self::Disaster(grant) => grant.authorized(owner, identity),
        }
    }
}

pub(crate) struct PreparedSeed {
    grant: SeedPermit,
    descriptor: Descriptor,
    initial: ::raft::prelude::ConfState,
    pin: Option<SnapshotPin>,
    _reservation: std::sync::Arc<crate::raft::budget::Permit>,
}

impl PreparedSeed {
    pub(crate) fn membership(&self) -> &crate::raft::member_state::GroupMembership {
        &self.descriptor.membership
    }

    pub(crate) fn from_root_archive(
        grant: crate::raft::transport::seeds::ArchiveSeedGrant,
        set: crate::raft::archive::ArchiveRecoverySet,
        birth: &crate::raft::catalog::GroupBirth,
        schema: [u8; 32],
        limits: SnapshotLimits,
        store: &cellar_ltx::internal::NodeExtentStore,
        reservation: std::sync::Arc<crate::raft::budget::Permit>,
    ) -> Result<Self, SnapshotError> {
        let binding = validate_archive_seed(&grant, &set, birth, schema, limits)?;
        if set.is_canonical_empty().map_err(SnapshotError::Archive)? {
            // The fresh grant and committed learner/barrier above remain
            // mandatory. An empty history has no physical image to convert.
            let descriptor = Descriptor {
                format: 1,
                scope: WireScope::from(birth.scope()),
                birth: birth.digest(),
                schema,
                index: binding.position.index.get(),
                term: binding.position.term.get(),
                conf: manifest::encode_conf(&binding.conf)?,
                membership: binding.membership,
                image: None,
                encoded_length: 0,
                encoded_digest: Sha256::digest([]).into(),
                transfer_length: 0,
            };
            Descriptor::decode_bound(
                birth.scope(),
                birth.digest(),
                &crate::raft::member_state::GroupMembership::initial(&binding.initial),
                schema,
                &descriptor.snapshot()?,
                limits,
            )?;
            return Ok(Self {
                grant: SeedPermit::S3(grant),
                descriptor,
                initial: binding.initial,
                pin: None,
                _reservation: reservation,
            });
        }
        let (snapshot, image) = set.into_root(store).map_err(SnapshotError::Archive)?;
        let descriptor = Descriptor::decode_bound(
            birth.scope(),
            birth.digest(),
            &crate::raft::member_state::GroupMembership::initial(&binding.initial),
            schema,
            &snapshot,
            limits,
        )?;
        if descriptor.index != binding.position.index.get()
            || descriptor.term != binding.position.term.get()
            || descriptor.membership != binding.membership
            || descriptor.conf != manifest::encode_conf(&binding.conf)?
            || descriptor.image.as_ref() != Some(&crate::raft::rooted::marker::encode(&image))
            || !store.owns_image(&image)
        {
            return Err(SnapshotError::Invalid("archive root seed binding"));
        }
        Ok(Self {
            grant: SeedPermit::S3(grant),
            descriptor,
            initial: binding.initial,
            pin: Some(SnapshotPin::Rooted {
                image,
                store: store.clone(),
            }),
            _reservation: reservation,
        })
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
            group_manifest: None,
            manifest_pins: None,
            checkpoint_manifest: None,
            group_seed: None,
            applied: crate::raft::cache::DurableApply {
                position: LogPosition {
                    index: LogIndex::new(self.descriptor.index),
                    term: Term::new(self.descriptor.term),
                },
                image: self.pin.as_ref().map(SnapshotPin::marker),
            },
        })
    }

    pub(crate) fn retain_pending(&self) -> Result<(), SnapshotError> {
        if let Some(pin) = &self.pin {
            pin.retain_pending_for(
                &self.local().scope,
                self.descriptor.birth,
                self.descriptor.index,
            )?;
        }
        Ok(())
    }

    pub(crate) fn retain(&self) -> Result<(), SnapshotError> {
        if let Some(pin) = &self.pin {
            pin.retain_for(
                &self.local().scope,
                self.descriptor.birth,
                self.descriptor.index,
            )?;
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
        let ArchiveSeedBinding {
            position,
            conf,
            membership,
            initial,
        } = validate_archive_seed(&grant, &set, birth, schema, limits)?;
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
            .map(|pin| encode_image(pin, limits, stream_snapshot::IoProfile::Compact))
            .transpose()?;
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
            encoded_length: bytes
                .as_ref()
                .map_or(0, |file| file.metadata().encoded_length as usize),
            encoded_digest: bytes.as_ref().map_or_else(
                || Sha256::digest([]).into(),
                |file| file.metadata().encoded_digest,
            ),
            transfer_length: bytes
                .as_ref()
                .map_or(0, |file| file.metadata().encoded_length as usize),
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
            pin: pin.map(SnapshotPin::Materialized),
            _reservation: reservation,
        })
    }
}

struct ArchiveSeedBinding {
    position: LogPosition,
    conf: ::raft::prelude::ConfState,
    membership: crate::raft::member_state::GroupMembership,
    initial: ::raft::prelude::ConfState,
}

fn validate_archive_seed(
    grant: &crate::raft::transport::seeds::ArchiveSeedGrant,
    set: &crate::raft::archive::ArchiveRecoverySet,
    birth: &crate::raft::catalog::GroupBirth,
    schema: [u8; 32],
    limits: SnapshotLimits,
) -> Result<ArchiveSeedBinding, SnapshotError> {
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
    Ok(ArchiveSeedBinding {
        position,
        conf,
        membership,
        initial,
    })
}
