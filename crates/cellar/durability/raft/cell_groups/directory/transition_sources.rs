// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently replay each held parent closure before rooting the sealed set.
use super::{transition_source_slots::ArchiveSlot, *};
use crate::raft::cell_groups::{
    CellGroupArchiveCheckpointReceiver, CellGroupArchiveCoverage, CellGroupArchiveExecution,
    CellGroupArchiveHold, CellGroupArchiveIntent, CellGroupArchivePool, CellGroupArchiveTailPool,
    GroupManifest,
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
    // Called only after complete archive replay and exact selected-set checks.
    pub(super) fn from_replay(
        plan: &DirectoryMutation,
        verified: VerifiedTransitionSourceSet,
    ) -> Self {
        Self {
            operation: plan.operation(),
            plan: plan.digest(),
            journal: verified.journal,
            sources: verified.sources,
            children: verified.children,
        }
    }

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
    PublishedBaseline,
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

struct EncodedSourceSet {
    bytes: Vec<u8>,
    holds: Vec<([u8; 32], Vec<u8>)>,
}

impl RangeDirectory {
    pub(super) fn check_source_marker(
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

    /// Select the exact active hold set before opening any physical receivers.
    /// The caller supplies its own holds and retains responsibility for their
    /// cleanup if the plan is cancelled. Selection and cancellation share a CAS.
    pub(crate) async fn stage_managed_preparation_holds(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        plan: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
        holds: &[CellGroupArchiveHold],
        acquires: Option<[u8; 32]>,
    ) -> Result<TransitionJournal> {
        permit.check(self, plan)?;
        let (parents, _) = self.transition_routes(plan)?;
        if parents.len() != holds.len() {
            return Err(CatalogError::Invalid("incomplete transition preparation inputs").into());
        }
        let (view, journal) = self
            .bounded(async {
                let (view, _) = self.current().await?;
                let journal = self
                    .matching_transition(&view.head, plan)
                    .await?
                    .filter(|journal| {
                        matches!(
                            journal.phase(),
                            TransitionPhase::Planned | TransitionPhase::Prepared
                        )
                    })
                    .ok_or(CatalogError::Fenced)?;
                Ok((view, journal))
            })
            .await?;
        if journal.preparation_acquires() != acquires {
            return Err(CatalogError::Conflict("transition preparation hold owner changed").into());
        }
        let set = self.encode_transition_source_set(plan, holds.iter())?;
        let digest = record::digest(&set.bytes);
        if journal
            .preparation_sources()
            .is_some_and(|selected| selected != digest)
        {
            return Err(CatalogError::Conflict("transition preparation inputs changed").into());
        }
        for (route, hold) in parents.iter().zip(holds) {
            self.bounded(self.selected_route_in(&view, route)).await?;
            self.check_source_marker(
                plan,
                &journal,
                route,
                hold.marker(),
                TransitionSourceCut::Active,
            )?;
            let birth = self.materialize_selected_history(route.history).await?;
            if birth.route() != route {
                return Err(CatalogError::Fenced.into());
            }
            let registry = self.open_group_archive(domain, archives, &birth).await?;
            registry.require_retained_hold(domain, hold).await?;
        }
        self.persist_transition_source_set(digest, set).await?;
        self.record_managed_preparation_sources(plan, permit, digest, acquires)
            .await
            .map_err(Into::into)
    }

    /// Reopen the staged input descriptors, including after cancellation.
    /// A descriptor alone supplies neither a physical pin nor seal authority.
    pub(crate) async fn staged_managed_preparation_holds(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<Option<Vec<CellGroupArchiveHold>>> {
        if self.namespace_config().managed_fleet.is_none() {
            return Err(CatalogError::ManagedAdmissionRequired.into());
        }
        self.check_transition_plan(plan)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let Some(digest) = journal.preparation_sources() else {
                return Ok(None);
            };
            self.read_transition_source_holds(plan, &journal, digest, TransitionSourceCut::Active)
                .await
                .map(Some)
        })
        .await
        .map_err(Into::into)
    }

    fn encode_transition_source_set<'a>(
        &self,
        plan: &DirectoryMutation,
        holds: impl Iterator<Item = &'a CellGroupArchiveHold>,
    ) -> Result<EncodedSourceSet> {
        let holds = holds
            .map(|hold| {
                hold.check_reference()?;
                let bytes = record::encode(SOURCE_HOLD, hold, wire::LIMIT)?;
                Ok((record::digest(&bytes), bytes))
            })
            .collect::<Result<Vec<_>>>()?;
        let set = SourceSet {
            namespace: self.namespace.root_digest,
            request: plan.digest,
            holds: holds.iter().map(|(digest, _)| *digest).collect(),
        };
        Ok(EncodedSourceSet {
            bytes: record::encode(SOURCE_SET, &set, wire::LIMIT)?,
            holds,
        })
    }

    async fn persist_transition_source_set(
        &self,
        digest: [u8; 32],
        set: EncodedSourceSet,
    ) -> std::result::Result<(), CatalogError> {
        self.bounded(async {
            for (id, bytes) in set.holds {
                self.immutable(HOLDS, id, &bytes).await?;
            }
            self.immutable(SETS, digest, &set.bytes).await
        })
        .await
    }

    /// Select a single verified parent hold for this irreversible operation.
    /// Independent slots allow different parents to finish in either order.
    pub(crate) async fn stage_managed_sealed_source(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        plan: &DirectoryMutation,
        source: (CellGroupTransitionSource, Option<CellGroupArchiveIntent>),
        permit: &ManagedTransitionPermit,
    ) -> Result<()> {
        let (source, intent) = source;
        permit.check(self, plan)?;
        let (parents, _) = self.transition_routes(plan)?;
        let route = source.birth.route();
        let index = parents
            .iter()
            .position(|parent| parent == route)
            .ok_or(CatalogError::Fenced)?;
        let journal = self
            .bounded(async {
                let (view, _) = self.current().await?;
                let journal = self
                    .matching_transition(&view.head, plan)
                    .await?
                    .filter(|journal| journal.phase() == TransitionPhase::SealAuthorized)
                    .ok_or(CatalogError::Fenced)?;
                self.selected_route_in(&view, route).await?;
                Ok(journal)
            })
            .await?;
        source.hold.check_reference()?;
        self.check_source_marker(
            plan,
            &journal,
            route,
            source.hold.marker(),
            TransitionSourceCut::Sealed,
        )?;
        let registry = self
            .open_group_archive(domain, archives, &source.birth)
            .await?;
        // Keep the nested archive replay out of the staging future's
        // stack frame while the object-store HTTP client is being polled.
        let coverage =
            Box::pin(registry.verify_hold(domain, &source.hold, tails, source.receiver)).await?;
        self.check_source_marker(
            plan,
            &journal,
            route,
            coverage.marker(),
            TransitionSourceCut::Sealed,
        )?;
        registry
            .require_retained_hold(domain, coverage.hold())
            .await?;
        self.bounded(self.claim_staged_source_hold(plan, index, coverage.hold(), intent.as_ref()))
            .await?;
        Ok(())
    }

    /// Bounded plan-order progress for an already admitted managed operation.
    pub(crate) async fn managed_staged_source_holds(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<Vec<(GroupRoute, Option<CellGroupArchiveHold>)>> {
        if self.namespace_config().managed_fleet.is_none() {
            return Err(CatalogError::ManagedAdmissionRequired.into());
        }
        let (parents, _) = self.transition_routes(plan)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            journal.seal_authorization()?;
            let mut holds = Vec::with_capacity(parents.len());
            for (index, route) in parents.into_iter().enumerate() {
                let hold = match self.read_source_slot(plan, index).await? {
                    Some((ArchiveSlot::Verified(hold), _)) => {
                        self.check_sealed_hold(plan, &journal, &route, &hold)?;
                        Some(hold)
                    }
                    Some((ArchiveSlot::Acquiring(intent), _)) => {
                        self.check_sealed_intent(plan, &journal, &route, &intent)
                            .map_err(|_| CatalogError::Corrupt("staged source acquire"))?;
                        None
                    }
                    None => None,
                };
                holds.push((route, hold));
            }
            Ok(holds)
        })
        .await
        .map_err(Into::into)
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

    pub(super) async fn verify_and_record_sealed_sources_inner(
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
                if permit.is_some() && verified.journal.phase() == TransitionPhase::SealAuthorized {
                    self.bounded(async {
                        for (index, coverage) in verified.sources.iter().enumerate() {
                            self.claim_staged_source_hold(mutation, index, coverage.hold(), None)
                                .await?;
                        }
                        Ok(())
                    })
                    .await?;
                }
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
                    TransitionSourceCut::Sealed | TransitionSourceCut::PublishedBaseline => {
                        journal.seal_authorization()?;
                        if matches!(cut, TransitionSourceCut::PublishedBaseline) {
                            let publication = self
                                .confirmed_transfer_publication(mutation)
                                .await?
                                .ok_or(CatalogError::Fenced)?;
                            if journal.phase() != TransitionPhase::Published
                                || journal.parent_holds_released()
                                || publication.transfer_archive() != journal.transfer_archive()
                            {
                                return Err(CatalogError::Fenced);
                            }
                        }
                    }
                    TransitionSourceCut::Active => (),
                }
                Ok((view, journal))
            })
            .await?;
        let selected_routes = if matches!(cut, TransitionSourceCut::PublishedBaseline) {
            &children
        } else {
            &parents
        };
        for route in selected_routes {
            self.bounded(self.selected_route_in(&view, route)).await?;
        }
        for (route, source) in parents.iter().zip(&sources) {
            if source.birth.route() != route {
                return Err(CatalogError::Fenced.into());
            }
            self.check_source_marker(mutation, &journal, route, source.hold.marker(), cut)?;
        }
        let set =
            self.encode_transition_source_set(mutation, sources.iter().map(|source| &source.hold))?;
        let digest = record::digest(&set.bytes);
        let selected = match cut {
            TransitionSourceCut::Active => journal.preparation_sources(),
            TransitionSourceCut::Sealed | TransitionSourceCut::PublishedBaseline => {
                journal.sealed_sources()
            }
        };
        if matches!(cut, TransitionSourceCut::PublishedBaseline) && selected.is_none() {
            return Err(CatalogError::Fenced.into());
        }
        if selected.is_some_and(|selected| selected != digest) {
            return Err(CatalogError::Conflict("transition source set changed").into());
        }
        let mut verified = Vec::with_capacity(sources.len());
        let mut registries = Vec::with_capacity(sources.len());
        for source in sources {
            let registry = if matches!(cut, TransitionSourceCut::PublishedBaseline) {
                self.open_retired_source_archive(domain, archives, source.birth.route())
                    .await?
            } else {
                self.open_group_archive(domain, archives, &source.birth)
                    .await?
            };
            let coverage = registry
                .verify_hold(domain, &source.hold, tails, source.receiver)
                .await?;
            let coverage = if tails.sequential_sources() {
                coverage.retain_transition_source()?
            } else {
                coverage
            };
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
        if !matches!(cut, TransitionSourceCut::PublishedBaseline) {
            self.persist_transition_source_set(digest, set).await?;
        }
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
