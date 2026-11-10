// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
};
use crate::raft::{
    cache::GroupLog,
    cell_groups::GroupManifest,
    completion::RuntimeError,
    fleet_catalog::{
        FleetHostAuthorization, ManagedTransitionPermit, ReopenedManagedCellGroupTransition,
        TransferVoterAdmission,
    },
    fleet_stop::BootBinding,
    storage::ReplicaLog,
};
use cellar_logic::consensus::{EngineInstanceId, LogIndex, LogPosition, Term};
use serde::{Deserialize, Serialize};

const ACTIVATIONS: u8 = 208;
const SETS: &str = "transfer-activation-sets";
const STAGED_ACTIVATION: u8 = 212;

/// Historical proof from an engine-bound Raft owner after durable apply.
#[derive(Clone, Debug)]
pub struct CellGroupTransferActivationAck {
    route: GroupRoute,
    publication: [u8; 32],
    manifest: [u8; 32],
    position: LogPosition,
}

impl CellGroupTransferActivationAck {
    pub fn position(&self) -> LogPosition {
        self.position
    }
}

/// Exact selected publication and seed, bound to one open voter engine.
pub struct CellGroupTransferActivationPermit {
    group: GroupLog<GroupHistory>,
    engine: EngineInstanceId,
    publication: [u8; 32],
    manifest: [u8; 32],
    deadline: u64,
    entry: Vec<u8>,
    boot: Option<BootBinding>,
}

impl CellGroupTransferActivationPermit {
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
        if let Some(boot) = &self.boot {
            if log
                .lifecycle_guard()?
                .is_none_or(|guard| !guard.admits_group_start(boot))
            {
                return Err(RuntimeError::StaleCompletion);
            }
            let cache = group.read()?;
            if cache.membership.revision().get() != 1
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
    ) -> Result<Option<CellGroupTransferActivationAck>, RuntimeError> {
        if marker.route().history != self.history()
            || marker
                .transfer_commitment()
                .map_err(|e| RuntimeError::Invalid(e.to_string()))?
                != Some(self.manifest)
        {
            return Err(RuntimeError::StaleCompletion);
        }
        match marker.transfer_activation() {
            Some((publication, manifest, position))
                if publication == self.publication && manifest == self.manifest =>
            {
                let cache = group.read()?;
                if cache.group_manifest.as_ref() != Some(marker)
                    || cache.hard_state.commit < position.index.get()
                {
                    return Err(RuntimeError::NotReady);
                }
                Ok(Some(CellGroupTransferActivationAck {
                    route: marker.route().clone(),
                    publication,
                    manifest,
                    position,
                }))
            }
            Some(_) => Err(RuntimeError::StaleCompletion),
            None => Ok(None),
        }
    }

    pub(crate) fn into_entry(self) -> Vec<u8> {
        self.entry
    }
}

impl RangeDirectory {
    /// Bind the child voter and its retained seed to the selected publication.
    /// Admission requires a separate current-term barrier and committed apply.
    pub async fn prepare_transfer_activation(
        &self,
        plan: &DirectoryMutation,
        history: GroupHistory,
        log: &ReplicaLog,
    ) -> Result<CellGroupTransferActivationPermit, CatalogError> {
        self.unmanaged_birth()?;
        let bootstrap = self
            .bootstrap_pending_transfer_voter(plan, history, log)
            .await?;
        self.prepare_transfer_activation_inner(
            plan,
            bootstrap.route(),
            bootstrap.group().clone(),
            log,
            None,
        )
        .await
    }

    pub(crate) async fn prepare_managed_transfer_activation(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        history: GroupHistory,
        log: &ReplicaLog,
        host: &FleetHostAuthorization,
        deadline: u64,
    ) -> Result<CellGroupTransferActivationPermit, CatalogError> {
        let (_, children) = self.transition_routes(&transition.plan)?;
        let route = children
            .iter()
            .find(|route| route.history == history)
            .ok_or(CatalogError::Fenced)?;
        let (group, _, boot) = self
            .require_managed_transfer_voter(
                transition,
                route,
                log,
                host,
                TransferVoterAdmission::Install,
                deadline,
            )
            .await?;
        self.prepare_transfer_activation_inner(
            &transition.plan,
            route,
            group,
            log,
            Some((boot, deadline)),
        )
        .await
    }

    async fn prepare_transfer_activation_inner(
        &self,
        plan: &DirectoryMutation,
        route: &GroupRoute,
        group: GroupLog<GroupHistory>,
        log: &ReplicaLog,
        managed: Option<(BootBinding, u64)>,
    ) -> Result<CellGroupTransferActivationPermit, CatalogError> {
        self.bounded(async {
            if managed.is_some() {
                let (view, _) = self.current().await?;
                self.selected_route_in(&view, route).await?;
            }
            let publication = self
                .publication(plan)
                .await?
                .filter(|publication| publication.transfer_archive().is_some())
                .ok_or(CatalogError::Fenced)?;
            let marker = group
                .group_manifest()
                .map_err(CatalogError::Log)?
                .ok_or(CatalogError::LostStableState)?;
            let manifest = marker
                .transfer_commitment()
                .map_err(|_| CatalogError::Corrupt("transfer seed commitment"))?
                .ok_or(CatalogError::Fenced)?;
            let transition = TransitionId::new(plan.operation().bytes())
                .map_err(|_| CatalogError::Invalid("transfer operation"))?;
            let publication = publication.journal_digest();
            let entry = CellGroupEntry {
                binding: GroupEntryBinding {
                    group: route.history,
                    route_epoch: route.epoch,
                    birth_digest: route.birth_digest,
                    schema_digest: route.schema_digest,
                    proposal: plan.operation(),
                },
                command: GroupCommand::Activate {
                    transition,
                    publication,
                    manifest,
                },
            }
            .encode(route, GroupEntryLimits::default())
            .map_err(|_| CatalogError::Invalid("transfer activation entry"))?;
            let now = log
                .domain()
                .mono_ms()
                .map_err(|_| CatalogError::Unconfirmed)?;
            let limit = now
                .checked_add(self.limits.operation_timeout_ms)
                .ok_or(CatalogError::Limit)?;
            let (boot, deadline) = managed.map_or((None, limit), |(boot, deadline)| {
                (Some(boot), deadline.min(limit))
            });
            if deadline <= now {
                return Err(CatalogError::Unconfirmed);
            }
            Ok(CellGroupTransferActivationPermit {
                group,
                engine: log.storage_instance().map_err(CatalogError::Log)?,
                publication,
                manifest,
                deadline,
                entry,
                boot,
            })
        })
        .await
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ActivationSet {
    namespace: [u8; 32],
    plan: [u8; 32],
    publication: [u8; 32],
    installed: [u8; 32],
    children: Vec<ActivationReceipt>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StagedActivation {
    namespace: [u8; 32],
    plan: [u8; 32],
    publication: [u8; 32],
    installed: [u8; 32],
    child: ActivationReceipt,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ActivationReceipt {
    group: [u8; 16],
    incarnation: [u8; 16],
    manifest: [u8; 32],
    index: u64,
    term: u64,
}

impl ActivationReceipt {
    fn new(ack: &CellGroupTransferActivationAck) -> Self {
        Self {
            group: ack.route.history.group.bytes(),
            incarnation: ack.route.history.incarnation.bytes(),
            manifest: ack.manifest,
            index: ack.position.index.get(),
            term: ack.position.term.get(),
        }
    }

    fn validate(&self, installed: &GroupManifest) -> Result<LogPosition, CatalogError> {
        let history = installed.route().history;
        let position = LogPosition {
            index: LogIndex::new(self.index),
            term: Term::new(self.term),
        };
        let commitment = installed
            .transfer_commitment()
            .map_err(|_| CatalogError::Corrupt("installed child seed commitment"))?;
        if self.group != history.group.bytes()
            || self.incarnation != history.incarnation.bytes()
            || commitment != Some(self.manifest)
            || position.index <= installed.position().index
            || position.term < installed.position().term
        {
            return Err(CatalogError::Fenced);
        }
        Ok(position)
    }
}

impl RangeDirectory {
    /// Select a complete set of durable child activations in plan order.
    /// Keep the topology lane owned until the parents can safely retire.
    pub async fn record_transfer_activations(
        &self,
        plan: &DirectoryMutation,
        acknowledgements: Vec<CellGroupTransferActivationAck>,
    ) -> Result<TransitionJournal, CatalogError> {
        self.unmanaged_birth()?;
        self.record_transfer_activations_inner(plan, acknowledgements, None)
            .await
    }

    pub(crate) async fn record_managed_transfer_activations(
        &self,
        plan: &DirectoryMutation,
        acknowledgements: Vec<CellGroupTransferActivationAck>,
        permit: &ManagedTransitionPermit,
    ) -> Result<TransitionJournal, CatalogError> {
        permit.check(self, plan)?;
        self.record_transfer_activations_inner(plan, acknowledgements, Some(permit))
            .await
    }

    /// Persist a local applied acknowledgment and select Active only after
    /// every installed child has an exact staged acknowledgment.
    pub(crate) async fn resume_managed_transfer_activation(
        &self,
        plan: &DirectoryMutation,
        history: GroupHistory,
        acknowledgement: Option<CellGroupTransferActivationAck>,
        permit: &ManagedTransitionPermit,
    ) -> Result<Option<(LogPosition, TransitionJournal)>, CatalogError> {
        permit.check(self, plan)?;
        let (journal, position, complete) = self
            .bounded(async {
                let (view, _) = self.current().await?;
                let journal = self
                    .matching_transition(&view.head, plan)
                    .await?
                    .filter(|journal| {
                        matches!(
                            journal.phase(),
                            TransitionPhase::Published | TransitionPhase::Active
                        )
                    })
                    .ok_or(CatalogError::Fenced)?;
                let (publication, installed) = self
                    .transfer_activation_dependencies(plan, &view.head, &journal)
                    .await?;
                let markers = self
                    .installed_transfer_children_inner(plan, Some(permit))
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                let marker = markers
                    .iter()
                    .find(|marker| marker.route().history == history)
                    .ok_or(CatalogError::Fenced)?;
                if let Some(ack) = acknowledgement {
                    let receipt = checked_activation(&ack, marker, publication)?;
                    self.claim_staged_activation(plan, publication, installed, receipt)
                        .await?;
                }

                let mut position = None;
                let mut children = Vec::with_capacity(markers.len());
                for marker in &markers {
                    let child = marker.route().history;
                    let key =
                        staged_activation_key(plan, child.group.bytes(), child.incarnation.bytes());
                    let Some(bytes) = self.read(&key).await? else {
                        continue;
                    };
                    let staged: StagedActivation =
                        record::decode(STAGED_ACTIVATION, &bytes, wire::LIMIT)?;
                    if staged.namespace != self.namespace.root_digest
                        || staged.plan != plan.digest()
                        || staged.publication != publication
                        || staged.installed != installed
                    {
                        return Err(CatalogError::Corrupt("staged transfer activation binding"));
                    }
                    let applied = staged.child.validate(marker)?;
                    if child == history {
                        position = Some(applied);
                    }
                    children.push(staged.child);
                }
                let complete = if children.len() == markers.len() {
                    let digest = self
                        .store_activation_set(plan, publication, installed, children)
                        .await?;
                    Some((publication, installed, digest))
                } else {
                    None
                };
                Ok((journal, position, complete))
            })
            .await?;
        let Some(position) = position else {
            return Ok(None);
        };
        let journal = if let Some((publication, installed, digest)) = complete {
            self.record_verified_transfer_activations(
                plan,
                publication,
                installed,
                digest,
                Some(permit),
            )
            .await?
        } else {
            journal
        };
        Ok(Some((position, journal)))
    }

    async fn claim_staged_activation(
        &self,
        plan: &DirectoryMutation,
        publication: [u8; 32],
        installed: [u8; 32],
        child: ActivationReceipt,
    ) -> Result<(), CatalogError> {
        let key = self.key(&staged_activation_key(plan, child.group, child.incarnation));
        let value = StagedActivation {
            namespace: self.namespace.root_digest,
            plan: plan.digest(),
            publication,
            installed,
            child,
        };
        let bytes = record::encode(STAGED_ACTIVATION, &value, wire::LIMIT)?;
        let observed = self
            .namespace
            .inner
            .create_and_observe_bounded(&key, &bytes, wire::LIMIT)
            .await?;
        if observed != bytes {
            return Err(CatalogError::Conflict("staged transfer activation changed"));
        }
        Ok(())
    }

    async fn store_activation_set(
        &self,
        plan: &DirectoryMutation,
        publication: [u8; 32],
        installed: [u8; 32],
        children: Vec<ActivationReceipt>,
    ) -> Result<[u8; 32], CatalogError> {
        let set = ActivationSet {
            namespace: self.namespace.root_digest,
            plan: plan.digest(),
            publication,
            installed,
            children,
        };
        let bytes = record::encode(ACTIVATIONS, &set, wire::LIMIT)?;
        let digest = record::digest(&bytes);
        self.immutable(SETS, digest, &bytes).await?;
        Ok(digest)
    }

    async fn record_transfer_activations_inner(
        &self,
        plan: &DirectoryMutation,
        acknowledgements: Vec<CellGroupTransferActivationAck>,
        permit: Option<&ManagedTransitionPermit>,
    ) -> Result<TransitionJournal, CatalogError> {
        self.check_transition_plan(plan)?;
        if acknowledgements.len() != plan.request.children.len() {
            return Err(CatalogError::Invalid("incomplete transfer activation set"));
        }
        let (publication, installed, digest) = self
            .bounded(async {
                let (view, _) = self.current().await?;
                let journal = self
                    .matching_transition(&view.head, plan)
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                let (publication, installed) = self
                    .transfer_activation_dependencies(plan, &view.head, &journal)
                    .await?;
                let markers = self
                    .installed_transfer_children_inner(plan, permit)
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                let children = acknowledgements
                    .iter()
                    .zip(&markers)
                    .map(|(ack, marker)| checked_activation(ack, marker, publication))
                    .collect::<Result<Vec<_>, _>>()?;
                if permit.is_some() && journal.phase() == TransitionPhase::Published {
                    for receipt in &children {
                        self.claim_staged_activation(plan, publication, installed, receipt.clone())
                            .await?;
                    }
                }
                let digest = self
                    .store_activation_set(plan, publication, installed, children)
                    .await?;
                Ok((publication, installed, digest))
            })
            .await?;
        self.record_verified_transfer_activations(plan, publication, installed, digest, permit)
            .await
    }

    /// Reopen the historical applied positions selected by the Active CAS.
    pub async fn transfer_activations(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<Option<Vec<(GroupHistory, LogPosition)>>, CatalogError> {
        self.unmanaged_birth()?;
        self.transfer_activations_inner(plan, None).await
    }

    pub(super) async fn transfer_activations_inner(
        &self,
        plan: &DirectoryMutation,
        permit: Option<&ManagedTransitionPermit>,
    ) -> Result<Option<Vec<(GroupHistory, LogPosition)>>, CatalogError> {
        if let Some(permit) = permit {
            permit.check(self, plan)?;
        }
        self.check_transition_plan(plan)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let Some(digest) = journal.activated_children() else {
                return Ok(None);
            };
            let bytes = self
                .read(&format!("{SETS}/{}", hex::encode(digest)))
                .await?
                .ok_or(CatalogError::Corrupt("missing transfer activation set"))?;
            if record::digest(&bytes) != digest {
                return Err(CatalogError::Corrupt("transfer activation set digest"));
            }
            let set: ActivationSet = record::decode(ACTIVATIONS, &bytes, wire::LIMIT)?;
            let (publication, installed) = self
                .transfer_activation_dependencies(plan, &view.head, &journal)
                .await?;
            if set.namespace != self.namespace.root_digest
                || set.plan != plan.digest()
                || set.publication != publication
                || set.installed != installed
                || set.children.len() != plan.request.children.len()
            {
                return Err(CatalogError::Corrupt("transfer activation set binding"));
            }
            let markers = self
                .installed_transfer_children_inner(plan, permit)
                .await?
                .ok_or(CatalogError::Corrupt("missing activated child install set"))?;
            set.children
                .into_iter()
                .zip(markers)
                .map(|(receipt, marker)| Ok((marker.route().history, receipt.validate(&marker)?)))
                .collect::<Result<Vec<_>, CatalogError>>()
                .map(Some)
        })
        .await
    }

    async fn transfer_activation_dependencies(
        &self,
        plan: &DirectoryMutation,
        head: &wire::Head,
        journal: &TransitionJournal,
    ) -> Result<([u8; 32], [u8; 32]), CatalogError> {
        let publication = self
            .confirmed(head, plan)
            .await?
            .filter(|publication| {
                publication.transfer_archive() == journal.transfer_archive()
                    && publication.transfer_archive().is_some()
            })
            .ok_or(CatalogError::Fenced)?;
        Ok((
            publication.journal_digest(),
            journal.installed_children().ok_or(CatalogError::Fenced)?,
        ))
    }
}

fn checked_activation(
    ack: &CellGroupTransferActivationAck,
    installed: &GroupManifest,
    publication: [u8; 32],
) -> Result<ActivationReceipt, CatalogError> {
    if ack.publication != publication || &ack.route != installed.route() {
        return Err(CatalogError::Fenced);
    }
    let receipt = ActivationReceipt::new(ack);
    receipt.validate(installed)?;
    Ok(receipt)
}

fn staged_activation_key(
    plan: &DirectoryMutation,
    group: [u8; 16],
    incarnation: [u8; 16],
) -> String {
    format!(
        "transfer-activation-progress/{}/{}/{}",
        hex::encode(plan.digest()),
        hex::encode(group),
        hex::encode(incarnation)
    )
}
