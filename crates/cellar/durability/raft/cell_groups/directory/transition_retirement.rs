// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
};
use crate::raft::{
    cache::GroupLog,
    cell_groups::{
        CellGroupArchiveError, CellGroupArchiveExecution, CellGroupArchiveHold,
        CellGroupArchivePool, GroupManifest, GroupSourceRetirement,
    },
    completion::RuntimeError,
    fleet_catalog::{FleetGroupAuthorization, FleetHostAuthorization, ManagedTransitionPermit},
    fleet_stop::BootBinding,
    storage::ReplicaLog,
};
use cellar_logic::consensus::{EngineInstanceId, LogIndex, LogPosition, Term};
use serde::{Deserialize, Serialize};

/// A durable parent tombstone witnessed by its engine-bound Raft owner.
#[derive(Clone, Debug)]
pub struct CellGroupSourceRetirementAck {
    route: GroupRoute,
    seal: LogPosition,
    tombstone: GroupSourceRetirement,
}

impl CellGroupSourceRetirementAck {
    pub fn tombstone(&self) -> &GroupSourceRetirement {
        &self.tombstone
    }
}

/// One sealed parent and the exact replacement certificate, bound to its
/// existing voter engine. A permit does not authorize releasing any holds.
pub struct CellGroupSourceRetirementPermit {
    group: GroupLog<GroupHistory>,
    engine: EngineInstanceId,
    sealed: GroupManifest,
    publication: [u8; 32],
    deadline: u64,
    entry: Vec<u8>,
    managed: Option<(BootBinding, u64)>,
}

struct ManagedRetirementAdmission<'a> {
    permit: &'a ManagedTransitionPermit,
    host: &'a FleetHostAuthorization,
    authority: &'a FleetGroupAuthorization,
    deadline: u64,
}

impl CellGroupSourceRetirementPermit {
    pub(crate) fn history(&self) -> GroupHistory {
        self.group.identity().scope
    }

    pub(crate) fn check(
        &self,
        log: &ReplicaLog,
        group: &GroupLog<GroupHistory>,
    ) -> Result<(), RuntimeError> {
        if !Arc::ptr_eq(&group.owner, &self.group.owner)
            || group.identity() != self.group.identity()
            || log.storage_instance()? != self.engine
            || log.domain().mono_ms().map_err(|_| RuntimeError::Closed)? >= self.deadline
        {
            return Err(RuntimeError::StaleCompletion);
        }
        if let Some((boot, revision)) = &self.managed {
            if log
                .lifecycle_guard()?
                .is_none_or(|guard| !guard.admits_group_start(boot))
            {
                return Err(RuntimeError::StaleCompletion);
            }
            let cache = group.read()?;
            if cache.membership.revision().get() != *revision
                || !cache
                    .conf_state
                    .voters
                    .contains(&group.identity().member.get())
            {
                return Err(RuntimeError::StaleCompletion);
            }
        } else {
            drop(group.read()?);
        }
        Ok(())
    }

    pub(crate) fn acknowledge(
        &self,
        group: &GroupLog<GroupHistory>,
        marker: &GroupManifest,
    ) -> Result<Option<CellGroupSourceRetirementAck>, RuntimeError> {
        if marker.route() != self.sealed.route() || !marker.preserves_source_seal(&self.sealed) {
            return Err(RuntimeError::StaleCompletion);
        }
        let seal = marker.source_seal().ok_or(RuntimeError::StaleCompletion)?;
        let Some(retired) = seal.retirement() else {
            return Ok(None);
        };
        if retired.publication() != self.publication {
            return Err(RuntimeError::StaleCompletion);
        }
        let cache = group.read()?;
        if cache.group_manifest.as_ref() != Some(marker)
            || cache.hard_state.commit < retired.position().index.get()
        {
            return Err(RuntimeError::NotReady);
        }
        Ok(Some(CellGroupSourceRetirementAck {
            route: marker.route().clone(),
            seal: seal.position(),
            tombstone: retired.clone(),
        }))
    }

    pub(crate) fn into_entry(self) -> Vec<u8> {
        self.entry
    }
}

impl RangeDirectory {
    /// Discover a retained source after its ranges have been replaced. The
    /// historical birth is usable only for this sealed replica's retirement.
    pub(crate) async fn discover_managed_sealed_source(
        &self,
        marker: &GroupManifest,
    ) -> Result<Option<(CellGroupBirth, DirectoryMutation)>, CatalogError> {
        self.bounded(async {
            let Some((plan, journal)) = self.active_transition().await? else {
                return Ok(None);
            };
            if !matches!(
                journal.phase(),
                TransitionPhase::Published | TransitionPhase::Active | TransitionPhase::Retired
            ) || !self.transition_routes(&plan)?.0.contains(marker.route())
            {
                return Ok(None);
            }
            let publication = self
                .confirmed_transfer_publication(&plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            if journal.transfer_archive() != publication.transfer_archive() {
                return Err(CatalogError::Fenced);
            }
            let sealed = self
                .transfer_source_marker(&plan, &journal, marker.route().history)
                .await?;
            if !marker.preserves_source_seal(&sealed) {
                return Err(CatalogError::Fenced);
            }
            let birth = self.open_historical_birth(marker.route()).await?;
            Ok(Some((birth, plan)))
        })
        .await
    }

    pub(crate) async fn bootstrap_managed_sealed_source(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
        host: &FleetHostAuthorization,
    ) -> Result<CellGroupBootstrapPermit, CatalogError> {
        self.bounded(async {
            let source = birth.route().history;
            let group = log.cell_group(&source).map_err(CatalogError::Log)?;
            let marker = group
                .group_manifest()
                .map_err(CatalogError::Log)?
                .ok_or(CatalogError::LostStableState)?;
            let (historical, plan) = self
                .discover_managed_sealed_source(&marker)
                .await?
                .ok_or(CatalogError::Fenced)?;
            if historical.route() != birth.route() {
                return Err(CatalogError::Fenced);
            }
            let timeout = Duration::from_millis(self.limits.operation_timeout_ms);
            let transition = host
                .stop_catalog()
                .reopen_managed_transition(self, plan.operation(), timeout)
                .await
                .map_err(CatalogError::from)?
                .ok_or(CatalogError::Fenced)?;
            if transition.plan.digest() != plan.digest() {
                return Err(CatalogError::Fenced);
            }
            let (permit, authority) = host
                .managed_source_authority(&transition, self, source, timeout)
                .await
                .map_err(CatalogError::from)?;
            permit.check(self, &plan)?;
            self.require_managed_source_voter(log, &group, &historical, host, &authority)
                .await?;
            Ok(CellGroupBootstrapPermit::managed_sealed_source(
                group,
                historical,
                self.clone(),
                log.clone(),
                host.clone(),
            ))
        })
        .await
    }

    async fn require_managed_source_voter(
        &self,
        log: &ReplicaLog,
        group: &GroupLog<GroupHistory>,
        birth: &CellGroupBirth,
        host: &FleetHostAuthorization,
        authority: &FleetGroupAuthorization,
    ) -> Result<(BootBinding, u64), CatalogError> {
        let boot = self.require_managed_group_boot(log, host).await?;
        let cache = group.read().map_err(CatalogError::Log)?;
        let marker = cache
            .group_manifest
            .as_ref()
            .ok_or(CatalogError::LostStableState)?;
        if !authority.check_birth(self, birth)
            || marker.route() != birth.route()
            || marker
                .source_seal()
                .is_none_or(|seal| cache.hard_state.commit < seal.position().index.get())
            || cache
                .membership
                .fleet_status()
                .is_none_or(|status| &status.binding != authority.binding())
            || !authority.matches_replica(group.identity(), &cache.membership)
            || !cache
                .conf_state
                .voters
                .contains(&group.identity().member.get())
        {
            return Err(CatalogError::Fenced);
        }
        Ok((boot, marker.membership_revision()))
    }

    async fn transfer_source_marker(
        &self,
        plan: &DirectoryMutation,
        journal: &TransitionJournal,
        source: GroupHistory,
    ) -> Result<GroupManifest, CatalogError> {
        self.read_transition_source_holds(
            plan,
            journal,
            journal.sealed_sources().ok_or(CatalogError::Fenced)?,
            super::transition_sources::TransitionSourceCut::Sealed,
        )
        .await?
        .into_iter()
        .find(|hold| hold.marker().route().history == source)
        .map(|hold| hold.marker().clone())
        .ok_or(CatalogError::Fenced)
    }
}

impl RangeDirectory {
    /// Admit the exact source tombstone only after every child activation has
    /// been selected. The source remains sealed while the entry is pending.
    pub async fn prepare_source_retirement(
        &self,
        plan: &DirectoryMutation,
        source: GroupHistory,
        log: &ReplicaLog,
    ) -> Result<CellGroupSourceRetirementPermit, CatalogError> {
        self.unmanaged_birth()?;
        self.prepare_source_retirement_inner(plan, source, log, None)
            .await
    }

    pub(crate) async fn prepare_managed_source_retirement(
        &self,
        plan: &DirectoryMutation,
        source: GroupHistory,
        log: &ReplicaLog,
        host: &FleetHostAuthorization,
        authority: (&ManagedTransitionPermit, &FleetGroupAuthorization),
        deadline: u64,
    ) -> Result<CellGroupSourceRetirementPermit, CatalogError> {
        self.prepare_source_retirement_inner(
            plan,
            source,
            log,
            Some(ManagedRetirementAdmission {
                permit: authority.0,
                host,
                authority: authority.1,
                deadline,
            }),
        )
        .await
    }

    async fn prepare_source_retirement_inner(
        &self,
        plan: &DirectoryMutation,
        source: GroupHistory,
        log: &ReplicaLog,
        managed: Option<ManagedRetirementAdmission<'_>>,
    ) -> Result<CellGroupSourceRetirementPermit, CatalogError> {
        self.check_transition_plan(plan)?;
        self.bounded(async {
            if managed.is_none() {
                self.namespace.require_cell_group_unmanaged().await?;
            }
            self.transfer_activations_inner(
                plan,
                managed.as_ref().map(|admission| admission.permit),
            )
            .await?
            .ok_or(CatalogError::Fenced)?;
            let publication = self
                .confirmed_transfer_publication(plan)
                .await?
                .ok_or(CatalogError::Fenced)?
                .journal_digest();
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, plan)
                .await?
                .filter(|journal| journal.phase() == TransitionPhase::Active)
                .ok_or(CatalogError::Fenced)?;
            let sealed = self.transfer_source_marker(plan, &journal, source).await?;
            let route = sealed.route();
            let group = log.cell_group(&source).map_err(CatalogError::Log)?;
            let marker = group
                .group_manifest()
                .map_err(CatalogError::Log)?
                .ok_or(CatalogError::LostStableState)?;
            if marker.route() != route || !marker.preserves_source_seal(&sealed) {
                return Err(CatalogError::Fenced);
            }
            super::bootstrap::check_member(&group)?;
            let engine = log.storage_instance().map_err(CatalogError::Log)?;
            let identity = group.identity();
            self.namespace
                .verify_node_storage_identity(identity.node, identity.disk, engine)
                .await?;
            let transition = TransitionId::new(plan.operation().bytes())
                .map_err(|_| CatalogError::Invalid("transfer operation"))?;
            let entry = CellGroupEntry {
                binding: GroupEntryBinding {
                    group: route.history,
                    route_epoch: route.epoch,
                    birth_digest: route.birth_digest,
                    schema_digest: route.schema_digest,
                    proposal: plan.operation(),
                },
                command: GroupCommand::Retire {
                    transition,
                    publication,
                },
            }
            .encode(route, GroupEntryLimits::default())
            .map_err(|_| CatalogError::Invalid("source retirement entry"))?;
            let now = log
                .domain()
                .mono_ms()
                .map_err(|_| CatalogError::Unconfirmed)?;
            let limit = now
                .checked_add(self.limits.operation_timeout_ms)
                .ok_or(CatalogError::Limit)?;
            let (managed, deadline) = if let Some(admission) = managed {
                let birth = self.open_historical_birth(route).await?;
                let voter = self
                    .require_managed_source_voter(
                        log,
                        &group,
                        &birth,
                        admission.host,
                        admission.authority,
                    )
                    .await?;
                (Some(voter), limit.min(admission.deadline))
            } else {
                (None, limit)
            };
            if deadline <= now {
                return Err(CatalogError::Unconfirmed);
            }
            Ok(CellGroupSourceRetirementPermit {
                group,
                engine,
                sealed,
                publication,
                deadline,
                entry,
                managed,
            })
        })
        .await
    }
}

const RETIREMENTS: u8 = 211;
const SETS: &str = "transfer-parent-retirement-sets";

struct RetirementBasis {
    journal: TransitionJournal,
    publication: [u8; 32],
    child_archives: [u8; 32],
    parents: Vec<CellGroupArchiveHold>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RetirementSet {
    namespace: [u8; 32],
    plan: [u8; 32],
    publication: [u8; 32],
    child_archives: [u8; 32],
    parents: Vec<RetirementReceipt>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RetirementReceipt {
    group: [u8; 16],
    incarnation: [u8; 16],
    seal_index: u64,
    seal_term: u64,
    index: u64,
    term: u64,
}

impl RetirementReceipt {
    fn from_ack(ack: CellGroupSourceRetirementAck) -> Self {
        let position = ack.tombstone.position();
        Self {
            group: ack.route.history.group.bytes(),
            incarnation: ack.route.history.incarnation.bytes(),
            seal_index: ack.seal.index.get(),
            seal_term: ack.seal.term.get(),
            index: position.index.get(),
            term: position.term.get(),
        }
    }

    fn validate(&self, held: &GroupManifest) -> Result<(), CatalogError> {
        let history = held.route().history;
        let seal = held.source_seal().ok_or(CatalogError::Fenced)?.position();
        let position = LogPosition {
            index: LogIndex::new(self.index),
            term: Term::new(self.term),
        };
        if self.group != history.group.bytes()
            || self.incarnation != history.incarnation.bytes()
            || self.seal_index != seal.index.get()
            || self.seal_term != seal.term.get()
            || position.index <= held.position().index
            || position.term < held.position().term
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }
}

impl RangeDirectory {
    /// Record the durable parent tombstones after confirming the selected
    /// child coverage is still held. Keep topology owned through hold release.
    pub async fn settle_transfer_retirement(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        plan: &DirectoryMutation,
        acknowledgements: Vec<CellGroupSourceRetirementAck>,
    ) -> Result<TransitionJournal, CellGroupArchiveBindingError> {
        self.unmanaged_birth()?;
        self.check_transition_plan(plan)?;
        if acknowledgements.len() != plan.request.parents.len() {
            return Err(CatalogError::Invalid("incomplete transfer parent retirement").into());
        }
        domain
            .run(Box::pin(async {
                self.namespace.require_cell_group_unmanaged().await?;
                let basis = self.bounded(self.retirement_basis(plan)).await?;
                let mut parents = Vec::with_capacity(basis.parents.len());
                for (ack, hold) in acknowledgements.into_iter().zip(&basis.parents) {
                    if ack.tombstone.publication() != basis.publication
                        || &ack.route != hold.marker().route()
                    {
                        return Err(CatalogError::Fenced.into());
                    }
                    let receipt = RetirementReceipt::from_ack(ack);
                    receipt.validate(hold.marker())?;
                    parents.push(receipt);
                }
                let set = RetirementSet {
                    namespace: self.namespace.root_digest,
                    plan: plan.digest(),
                    publication: basis.publication,
                    child_archives: basis.child_archives,
                    parents,
                };
                let bytes = record::encode(RETIREMENTS, &set, wire::LIMIT)?;
                let digest = record::digest(&bytes);
                if let Some(selected) = basis.journal.retired_parents() {
                    if selected != digest {
                        return Err(
                            CatalogError::Conflict("transfer retirement set changed").into()
                        );
                    }
                    self.bounded(self.require_retirement_set(plan, &basis, selected))
                        .await?;
                    return Ok(basis.journal);
                }
                self.require_live_retirement_archives(domain, pool, plan)
                    .await?;
                self.bounded(self.immutable(SETS, digest, &bytes)).await?;
                self.record_verified_retirement(
                    plan,
                    basis.publication,
                    basis.child_archives,
                    digest,
                )
                .await
                .map_err(Into::into)
            }))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    /// Release the exact transfer-owned parent holds after the Retired CAS.
    /// Each archive CAS is idempotent. Retain topology ownership until all
    /// releases are observed, then record completion and reopen the lane.
    pub async fn release_retired_parent_holds(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        plan: &DirectoryMutation,
    ) -> Result<TransitionJournal, CellGroupArchiveBindingError> {
        self.unmanaged_birth()?;
        self.check_transition_plan(plan)?;
        domain
            .run(Box::pin(async {
                self.namespace.require_cell_group_unmanaged().await?;
                let basis = self.bounded(self.retirement_basis(plan)).await?;
                let digest = basis
                    .journal
                    .retired_parents()
                    .ok_or(CatalogError::Fenced)?;
                self.bounded(self.require_retirement_set(plan, &basis, digest))
                    .await?;
                if basis.journal.parent_holds_released() {
                    return Ok(basis.journal);
                }
                let released = async {
                    self.require_live_retirement_archives(domain, pool, plan)
                        .await?;
                    for hold in &basis.parents {
                        let registry = self
                            .open_retired_source_archive(domain, pool, hold.marker().route())
                            .await?;
                        registry.release(domain, hold).await?;
                    }
                    self.record_released_parent_holds(plan, digest)
                        .await
                        .map_err(Into::into)
                }
                .await;
                if released.is_err() {
                    // Another controller may have completed cleanup and a later
                    // topology owner may already have replaced a child.
                    if let Ok(current) = self.bounded(self.retirement_basis(plan)).await {
                        if current.journal.parent_holds_released()
                            && current.journal.retired_parents() == Some(digest)
                        {
                            return Ok(current.journal);
                        }
                    }
                }
                released
            }))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    async fn retirement_basis(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<RetirementBasis, CatalogError> {
        let (view, _) = self.current().await?;
        let journal = self
            .matching_transition(&view.head, plan)
            .await?
            .filter(|journal| {
                matches!(
                    journal.phase(),
                    TransitionPhase::Active | TransitionPhase::Retired
                )
            })
            .ok_or(CatalogError::Fenced)?;
        let publication = self
            .confirmed(&view.head, plan)
            .await?
            .filter(|publication| publication.transfer_archive().is_some())
            .ok_or(CatalogError::Fenced)?
            .journal_digest();
        let child_archives = journal.child_archives().ok_or(CatalogError::Fenced)?;
        let parents = self
            .read_transition_source_holds(
                plan,
                &journal,
                journal.sealed_sources().ok_or(CatalogError::Fenced)?,
                super::transition_sources::TransitionSourceCut::Sealed,
            )
            .await?;
        Ok(RetirementBasis {
            journal,
            publication,
            child_archives,
            parents,
        })
    }

    async fn require_retirement_set(
        &self,
        plan: &DirectoryMutation,
        basis: &RetirementBasis,
        digest: [u8; 32],
    ) -> Result<(), CatalogError> {
        let bytes = self
            .read(&format!("{SETS}/{}", hex::encode(digest)))
            .await?
            .ok_or(CatalogError::Corrupt("missing transfer retirement set"))?;
        if record::digest(&bytes) != digest {
            return Err(CatalogError::Corrupt("transfer retirement set digest"));
        }
        let set: RetirementSet = record::decode(RETIREMENTS, &bytes, wire::LIMIT)?;
        if set.namespace != self.namespace.root_digest
            || set.plan != plan.digest()
            || set.publication != basis.publication
            || set.child_archives != basis.child_archives
            || set.parents.len() != basis.parents.len()
        {
            return Err(CatalogError::Corrupt("transfer retirement set binding"));
        }
        for (receipt, hold) in set.parents.iter().zip(&basis.parents) {
            receipt.validate(hold.marker())?;
        }
        Ok(())
    }

    async fn require_live_retirement_archives(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        plan: &DirectoryMutation,
    ) -> Result<(), CellGroupArchiveBindingError> {
        let children = self
            .retirement_child_holds(plan)
            .await?
            .ok_or(CatalogError::Fenced)?;
        for hold in children {
            let birth = self
                .materialize_selected_history(hold.marker().route().history)
                .await?;
            let registry = self.open_group_archive(domain, pool, &birth).await?;
            registry.require_retained_hold(domain, &hold).await?;
        }
        Ok(())
    }
}
