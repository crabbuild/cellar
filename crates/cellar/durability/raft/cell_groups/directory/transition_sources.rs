// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently replay each held parent closure before rooting the sealed set.
use super::*;
use crate::raft::cell_groups::{
    CellGroupArchiveCheckpointReceiver, CellGroupArchiveCoverage, CellGroupArchiveExecution,
    CellGroupArchiveHold, CellGroupArchivePool, CellGroupArchiveTailPool, GroupManifest,
};
use crate::raft::fleet_catalog::ManagedTransitionPermit;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const SOURCE_HOLD: u8 = 199;
const SOURCE_SET: u8 = 200;
const HOLDS: &str = "transition-source-holds";
const SETS: &str = "transition-source-sets";

type Result<T> = std::result::Result<T, CellGroupArchiveBindingError>;

/// The explicit archive hold belongs to the transition owner until retirement.
/// The receiver must have its own physical recovery admission and stores.
pub struct CellGroupTransitionSource {
    pub birth: CellGroupBirth,
    pub hold: CellGroupArchiveHold,
    pub receiver: CellGroupArchiveCheckpointReceiver,
}

/// Independently recovered source images and their selected catalog decision.
/// This grants no child boot, installation, publication or serving authority.
#[derive(Clone)]
pub struct VerifiedCellGroupTransitionSources {
    operation: ProposalId,
    plan: [u8; 32],
    journal: TransitionJournal,
    sources: Vec<CellGroupArchiveCoverage>,
    children: Vec<GroupRoute>,
}

impl VerifiedCellGroupTransitionSources {
    pub(crate) fn transition_plan(&self) -> (ProposalId, [u8; 32]) {
        (self.operation, self.plan)
    }

    pub fn journal(&self) -> &TransitionJournal {
        &self.journal
    }

    pub fn sources(&self) -> &[CellGroupArchiveCoverage] {
        &self.sources
    }

    pub fn children(&self) -> &[GroupRoute] {
        &self.children
    }
}

#[derive(Clone, Copy)]
pub(super) enum TransitionSourceCut {
    Active,
    Sealed,
}

pub(super) struct VerifiedTransitionSourceSet {
    pub(super) journal: TransitionJournal,
    pub(super) digest: [u8; 32],
    pub(super) sources: Vec<CellGroupArchiveCoverage>,
    pub(super) children: Vec<GroupRoute>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceSet {
    namespace: [u8; 32],
    request: [u8; 32],
    holds: Vec<[u8; 32]>,
}

impl RangeDirectory {
    fn check_source_marker(
        &self,
        mutation: &DirectoryMutation,
        journal: &TransitionJournal,
        route: &GroupRoute,
        marker: &GroupManifest,
        cut: TransitionSourceCut,
    ) -> std::result::Result<(), CatalogError> {
        if marker.route() != route {
            return Err(CatalogError::Fenced);
        }
        if matches!(cut, TransitionSourceCut::Active) {
            return if marker.is_active() {
                Ok(())
            } else {
                Err(CatalogError::Fenced)
            };
        }
        let (transition, authorization, entry) =
            self.source_seal_entry(mutation, journal, route)?;
        let payload = Sha256::digest(&entry).into();
        if marker
            .source_seal()
            .is_none_or(|seal| !seal.matches(transition, authorization, payload))
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }

    /// Verify every exact held parent cut in plan order, then select the whole
    /// set in the namespace CAS. Persist the hold allocations before calling.
    /// Keep those remote holds until the transition owner retires all users.
    /// Retry with the identical set after a lost response and independently
    /// verify the restored holds again before using their data. On reopen,
    /// materialize the selected births with this directory before retrying.
    pub async fn verify_and_record_sealed_sources(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        mutation: &DirectoryMutation,
        sources: Vec<CellGroupTransitionSource>,
    ) -> Result<VerifiedCellGroupTransitionSources> {
        self.unmanaged_birth()?;
        self.verify_and_record_sealed_sources_inner(
            domain, archives, tails, mutation, sources, None,
        )
        .await
    }

    pub(crate) async fn verify_and_record_managed_sealed_sources(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        mutation: &DirectoryMutation,
        sources: Vec<CellGroupTransitionSource>,
        permit: &ManagedTransitionPermit,
    ) -> Result<VerifiedCellGroupTransitionSources> {
        permit.check(self, mutation)?;
        self.verify_and_record_sealed_sources_inner(
            domain,
            archives,
            tails,
            mutation,
            sources,
            Some(permit),
        )
        .await
    }

    async fn verify_and_record_sealed_sources_inner(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        mutation: &DirectoryMutation,
        sources: Vec<CellGroupTransitionSource>,
        permit: Option<&ManagedTransitionPermit>,
    ) -> Result<VerifiedCellGroupTransitionSources> {
        domain
            .run(Box::pin(async {
                let verified = Box::pin(self.verify_transition_source_set(
                    domain,
                    archives,
                    tails,
                    mutation,
                    sources,
                    TransitionSourceCut::Sealed,
                ))
                .await?;
                let journal = self
                    .record_verified_sources(mutation, verified.digest, permit)
                    .await?;
                Ok(VerifiedCellGroupTransitionSources {
                    operation: mutation.operation,
                    plan: mutation.digest,
                    journal,
                    sources: verified.sources,
                    children: verified.children,
                })
            }))
            .await
            .map_err(|_| crate::raft::cell_groups::CellGroupArchiveError::Stopped)?
    }

    pub(super) async fn verify_transition_source_set(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        mutation: &DirectoryMutation,
        sources: Vec<CellGroupTransitionSource>,
        cut: TransitionSourceCut,
    ) -> Result<VerifiedTransitionSourceSet> {
        let (parents, children) = self.transition_routes(mutation)?;
        if sources.len() != parents.len() {
            return Err(CatalogError::Invalid("incomplete transition source set").into());
        }
        let (view, journal) = self
            .bounded(async {
                let (view, _) = self.current().await?;
                let journal = self
                    .matching_transition(&view.head, mutation)
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                match cut {
                    TransitionSourceCut::Active
                        if !matches!(
                            journal.phase(),
                            TransitionPhase::Planned | TransitionPhase::Prepared
                        ) =>
                    {
                        return Err(CatalogError::Fenced)
                    }
                    TransitionSourceCut::Sealed => {
                        journal.seal_authorization()?;
                    }
                    TransitionSourceCut::Active => (),
                }
                Ok((view, journal))
            })
            .await?;
        let mut encoded = Vec::with_capacity(sources.len());
        let mut admitted = Vec::with_capacity(sources.len());
        for (route, source) in parents.iter().zip(sources) {
            if source.birth.route() != route {
                return Err(CatalogError::Fenced.into());
            }
            self.bounded(self.selected_route_in(&view, route)).await?;
            source.hold.check_reference()?;
            self.check_source_marker(mutation, &journal, route, source.hold.marker(), cut)?;
            let bytes = record::encode(SOURCE_HOLD, &source.hold, wire::LIMIT)?;
            encoded.push((record::digest(&bytes), bytes));
            admitted.push(source);
        }
        let set = SourceSet {
            namespace: self.namespace.root_digest,
            request: mutation.digest,
            holds: encoded.iter().map(|(digest, _)| *digest).collect(),
        };
        let bytes = record::encode(SOURCE_SET, &set, wire::LIMIT)?;
        let digest = record::digest(&bytes);
        if journal
            .sealed_sources()
            .is_some_and(|selected| selected != digest)
        {
            return Err(CatalogError::Conflict("transition source set changed").into());
        }
        let mut verified = Vec::with_capacity(admitted.len());
        let mut registries = Vec::with_capacity(admitted.len());
        for source in admitted {
            let registry = self
                .open_group_archive(domain, archives, &source.birth)
                .await?;
            let coverage = registry
                .verify_hold(domain, &source.hold, tails, source.receiver)
                .await?;
            self.check_source_marker(
                mutation,
                &journal,
                source.birth.route(),
                coverage.marker(),
                cut,
            )?;
            verified.push(coverage);
            registries.push(registry);
        }
        self.bounded(async {
            for (id, bytes) in encoded {
                self.immutable(HOLDS, id, &bytes).await?;
            }
            self.immutable(SETS, digest, &bytes).await
        })
        .await?;
        for (registry, coverage) in registries.iter().zip(&verified) {
            registry
                .require_retained_hold(domain, coverage.hold())
                .await?;
        }
        Ok(VerifiedTransitionSourceSet {
            journal,
            digest,
            sources: verified,
            children,
        })
    }

    /// Reopen the original hold descriptors after a lost controller. They are
    /// references to verify again, never recovered images or install authority.
    pub async fn sealed_source_holds(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<Vec<CellGroupArchiveHold>> {
        self.check_transition_plan(mutation)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, mutation)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let digest = journal.sealed_sources().ok_or(CatalogError::Fenced)?;
            self.read_transition_source_holds(
                mutation,
                &journal,
                digest,
                TransitionSourceCut::Sealed,
            )
            .await
        })
        .await
        .map_err(Into::into)
    }

    pub(super) async fn read_transition_source_holds(
        &self,
        mutation: &DirectoryMutation,
        journal: &TransitionJournal,
        digest: [u8; 32],
        cut: TransitionSourceCut,
    ) -> std::result::Result<Vec<CellGroupArchiveHold>, CatalogError> {
        let bytes = self
            .read(&format!("{SETS}/{}", hex::encode(digest)))
            .await?
            .ok_or(CatalogError::Corrupt("missing transition source set"))?;
        if record::digest(&bytes) != digest {
            return Err(CatalogError::Corrupt("transition source set digest"));
        }
        let set: SourceSet = record::decode(SOURCE_SET, &bytes, wire::LIMIT)?;
        if set.namespace != self.namespace.root_digest
            || set.request != mutation.digest
            || set.holds.len() != mutation.request.parents.len()
        {
            return Err(CatalogError::Corrupt("transition source set binding"));
        }
        let mut holds = Vec::with_capacity(set.holds.len());
        for (parent, id) in mutation.request.parents.iter().zip(set.holds) {
            let bytes = self
                .read(&format!("{HOLDS}/{}", hex::encode(id)))
                .await?
                .ok_or(CatalogError::Corrupt("missing transition source hold"))?;
            if record::digest(&bytes) != id {
                return Err(CatalogError::Corrupt("transition source hold digest"));
            }
            let hold: CellGroupArchiveHold = record::decode(SOURCE_HOLD, &bytes, wire::LIMIT)?;
            hold.check_reference()
                .map_err(|_| CatalogError::Corrupt("transition source hold"))?;
            self.check_source_marker(mutation, journal, &parent.decode(self)?, hold.marker(), cut)?;
            holds.push(hold);
        }
        Ok(holds)
    }
}
