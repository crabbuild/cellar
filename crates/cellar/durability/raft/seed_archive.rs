//! Exact-reference S3 download. Only an authenticated fresh grant can reach
//! physical initialization; the referenced bytes supply no authority by themselves.

use super::*;
use crate::raft::{
    archive::{ArchiveRecoverySet, CellArchive},
    catalog::GroupBirth,
    snapshot::{PreparedSeed, SnapshotError, SnapshotLimits},
};

pub(crate) struct ReceivedArchiveSeed {
    grant: ArchiveSeedGrant,
    set: ArchiveRecoverySet,
    birth: GroupBirth,
    schema: [u8; 32],
    limits: SnapshotLimits,
    reservation: Arc<Permit>,
}

impl ReceivedArchiveSeed {
    pub(crate) fn scope(&self) -> &GroupScope {
        &self.grant.local().scope
    }

    pub(crate) fn finish(
        self,
        images: &cellar_ltx::generation::GenerationStore,
    ) -> Result<PreparedSeed, SnapshotError> {
        PreparedSeed::from_archive(
            self.grant,
            self.set,
            &self.birth,
            self.schema,
            self.limits,
            images,
            self.reservation,
        )
    }
}

impl RaftTransport {
    pub(crate) async fn seed_from_archive(
        &self,
        local: ReplicaIdentity,
        source: RaftPeer,
        birth: &GroupBirth,
        schema: [u8; 32],
        limits: SnapshotLimits,
        archive: CellArchive,
    ) -> anyhow::Result<ReceivedArchiveSeed> {
        let deadline = self.seed_deadline()?;
        let grant = loop {
            match self
                .request_archive_seed(local.clone(), source.clone(), schema, deadline)
                .await
            {
                Err(
                    TransportError::Overloaded
                    | TransportError::Timeout
                    | TransportError::Io(_)
                    | TransportError::StalePeer,
                ) => {
                    let state = &self.handle.0;
                    let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
                    if now >= deadline {
                        return Err(TransportError::Timeout.into());
                    }
                    state
                        .seed_until(deadline, async {
                            state
                                .domain
                                .sleep_until(now.saturating_add(50).min(deadline))
                                .map_err(|_| TransportError::Closed)?
                                .await;
                            Ok(())
                        })
                        .await?;
                }
                result => break result?,
            }
        };
        let state = &self.handle.0;
        if !grant.authorized(&state.runtime.log().owner(), grant.local()) {
            return Err(TransportError::Identity.into());
        }
        let set_bytes = archive.max_set_bytes();
        if set_bytes > 256 * 1024 * 1024 {
            return Err(TransportError::Limit.into());
        }
        let reservation = Arc::new(
            state
                .snapshot_receive_budget
                .acquire(super::super::snapshots::memory_bound(&state.options) + set_bytes)
                .ok_or(TransportError::Overloaded)?,
        );
        let archive = archive.with_read_retention(reservation.clone());
        let set = state
            .seed_until(grant.deadline(), async {
                Ok(archive.recovery_set_at(grant.reference()).await)
            })
            .await??;
        if set.reference() != *grant.reference() {
            return Err(TransportError::Authentication.into());
        }
        Ok(ReceivedArchiveSeed {
            grant,
            set,
            birth: birth.clone(),
            schema,
            limits,
            reservation,
        })
    }
}
