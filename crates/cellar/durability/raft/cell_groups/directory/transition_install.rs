// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A selected sealed-source decision can install a child on its exact engine.
use super::*;
use crate::raft::{
    cache::GroupLog,
    cell_groups::{
        GroupManifest, PreparedCellGroupTransferChild, PreparedGroupManifest, TransferSeed,
    },
    fleet_catalog::{FleetHostAuthorization, ReopenedManagedCellGroupTransition},
    fleet_stop::{BootBinding, Guard},
    member_state::FleetGroupBinding,
    storage::{CellGroupBootstrap, LogError, ReplicaLog},
};
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, MemberId, NodeId, ReplicaIdentity};
use serde::{Deserialize, Serialize};

const BINDING: u8 = 203;
const INTENT: u8 = 204;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InstallBinding {
    namespace: [u8; 32],
    genesis: GroupManifest,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InstallIntent {
    binding: [u8; 32],
    member: u64,
    node: [u8; 16],
    disk: [u8; 16],
    engine: [u8; 16],
}

pub(crate) struct TransferSeedPermit {
    pub(crate) owner: Arc<()>,
    pub(crate) bootstrap: CellGroupBootstrap,
    pub(crate) genesis: GroupManifest,
    local: ReplicaIdentity<GroupHistory>,
    engine: EngineInstanceId,
    managed: Option<ManagedSeed>,
}

struct ManagedSeed {
    group: FleetGroupBinding,
    boot: BootBinding,
    deadline: u64,
}

enum InstallData<'a> {
    Prepared(&'a PreparedCellGroupTransferChild),
    Retained {
        marker: &'a GroupManifest,
        selected: &'a GroupManifest,
        closure: Option<&'a PreparedGroupManifest>,
    },
}

impl InstallData<'_> {
    fn genesis(&self) -> Result<&GroupManifest, CatalogError> {
        match self {
            Self::Prepared(child) => {
                child
                    .genesis()
                    .map(|root| root.marker())
                    .ok_or(CatalogError::Conflict(
                        "transfer requires final child genesis",
                    ))
            }
            Self::Retained { marker, .. } => Ok(marker),
        }
    }
}

struct ManagedInstall<'a> {
    transition: &'a ReopenedManagedCellGroupTransition,
    host: &'a FleetHostAuthorization,
    deadline: u64,
}

impl ManagedInstall<'_> {
    fn remaining(&self, log: &ReplicaLog) -> Result<Duration, CatalogError> {
        let now = log
            .domain()
            .mono_ms()
            .map_err(|_| CatalogError::Unconfirmed)?;
        self.deadline
            .checked_sub(now)
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .ok_or(CatalogError::Unconfirmed)
    }
}

impl TransferSeedPermit {
    pub(crate) fn check(
        &self,
        owner: &Arc<()>,
        local: &ReplicaIdentity<GroupHistory>,
        engine: EngineInstanceId,
    ) -> Result<(), LogError> {
        if !Arc::ptr_eq(owner, &self.owner) || local != &self.local || engine != self.engine {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }

    pub(crate) fn managed_binding(
        &self,
        guard: Option<&Guard>,
        now: u64,
    ) -> Result<Option<&FleetGroupBinding>, LogError> {
        let Some(managed) = &self.managed else {
            return Ok(None);
        };
        if now >= managed.deadline
            || guard.is_none_or(|guard| !guard.admits_group_start(&managed.boot))
            || !managed.group.matches_scope(&self.local.scope)
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(Some(&managed.group))
    }
}

/// One reserved voter whose engine sync has an exact durable receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstalledTransferVoter {
    pub member: MemberId,
    pub node: NodeId,
    pub disk: DiskLineage,
    pub engine: EngineInstanceId,
}

/// The selected sealed-source child and its confirmed physical voter majority.
/// Startup and current-term Raft commitment require separate proof.
pub struct TransferInstallQuorum {
    pub genesis: GroupManifest,
    pub voters: Vec<InstalledTransferVoter>,
}

fn install_prefix(route: &GroupRoute) -> String {
    format!(
        "transfer-installs/{}/{}",
        hex::encode(route.history.group.bytes()),
        hex::encode(route.history.incarnation.bytes()),
    )
}

fn pending_genesis(marker: &GroupManifest) -> Result<GroupManifest, CatalogError> {
    let origin = marker.transfer_origin().ok_or(CatalogError::Fenced)?;
    if marker.transfer_activation().is_some() || marker.membership_revision() != 1 {
        return Err(CatalogError::Fenced);
    }
    GroupManifest::transfer_genesis(marker.route().clone(), marker.root(), origin.clone())
        .map_err(|_| CatalogError::Corrupt("pending transfer genesis"))
}

struct InstallRecords {
    prefix: String,
    member: String,
    binding: Vec<u8>,
    intent: Vec<u8>,
}

impl RangeDirectory {
    fn install_records(
        &self,
        genesis: &GroupManifest,
        local: &ReplicaIdentity<GroupHistory>,
        engine: EngineInstanceId,
    ) -> Result<InstallRecords, CatalogError> {
        let prefix = install_prefix(genesis.route());
        let binding = record::encode(
            BINDING,
            &InstallBinding {
                namespace: self.namespace.root_digest,
                genesis: genesis.clone(),
            },
            wire::LIMIT,
        )?;
        let intent = record::encode(
            INTENT,
            &InstallIntent {
                binding: record::digest(&binding),
                member: local.member.get(),
                node: local.node.bytes(),
                disk: local.disk.bytes(),
                engine: engine.bytes(),
            },
            wire::LIMIT,
        )?;
        let member = format!("{prefix}/{:016x}", local.member.get());
        Ok(InstallRecords {
            prefix,
            member,
            binding,
            intent,
        })
    }

    async fn require_install_intent(&self, records: &InstallRecords) -> Result<(), CatalogError> {
        if self
            .read(&format!("{}/binding", records.prefix))
            .await?
            .as_deref()
            != Some(records.binding.as_slice())
            || self
                .read(&format!("{}/intent", records.member))
                .await?
                .as_deref()
                != Some(records.intent.as_slice())
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }

    /// Before a managed boot can park a pending child, recheck the exact
    /// operation and durable install intent. The receipt may still be missing
    /// after a lost reply. This path issues no permit and verifies no images.
    pub(crate) async fn require_parked_managed_transfer(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        group: &GroupLog<GroupHistory>,
        marker: &GroupManifest,
        engine: EngineInstanceId,
    ) -> Result<(), CatalogError> {
        let owner = self
            .namespace
            .config
            .managed_fleet
            .as_ref()
            .ok_or(CatalogError::ManagedAdmissionRequired)?;
        let local = group.identity();
        if marker.route().history != local.scope
            || group
                .membership()
                .map_err(CatalogError::Log)?
                .fleet_status()
                .is_none_or(|status| {
                    status.binding.fleet() != owner.fleet
                        || !status.binding.matches_scope(&local.scope)
                })
        {
            return Err(CatalogError::Fenced);
        }
        self.bounded(async {
            let genesis = pending_genesis(marker)?;
            self.require_open_transfer_install(&transition.plan, &genesis)
                .await?;
            let birth = self
                .require_ordinary_birth_reservation(marker.route())
                .await?;
            if !birth
                .voters()
                .iter()
                .any(|voter| voter.node == local.node && voter.member == local.member)
            {
                return Err(CatalogError::NotInitialMember);
            }
            let records = self.install_records(&genesis, local, engine)?;
            self.require_install_intent(&records).await?;
            if self
                .read(&format!("{}/receipt", records.member))
                .await?
                .is_some_and(|receipt| receipt != records.intent)
            {
                return Err(CatalogError::Fenced);
            }
            Ok(())
        })
        .await
    }

    /// Install one reserved voter from the final sealed-source partition. The
    /// returned engine mapping grants no startup, voting or serving authority.
    /// Retrying the exact disk and engine reconciles an unknown result.
    pub async fn install_transfer_voter(
        &self,
        plan: &DirectoryMutation,
        child: &PreparedCellGroupTransferChild,
        log: &ReplicaLog,
    ) -> Result<GroupLog<GroupHistory>, CatalogError> {
        self.unmanaged_birth()?;
        self.install_transfer_voter_inner(plan, InstallData::Prepared(child), log, None)
            .await
    }

    /// Sync a prepared child into its reserved managed voter engine.
    /// The receiver must own the destination's retained page and image stores.
    /// A durable install receipt grants no attachment or serving authority.
    pub async fn install_managed_transfer_voter(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        child: &PreparedCellGroupTransferChild,
        log: &ReplicaLog,
        host: &FleetHostAuthorization,
        timeout: Duration,
    ) -> Result<InstalledTransferVoter, CatalogError> {
        if timeout.as_millis() == 0 || timeout > Duration::from_secs(60) {
            return Err(CatalogError::Invalid("managed transfer install timeout"));
        }
        let deadline = log
            .domain()
            .mono_ms()
            .map_err(|_| CatalogError::Unconfirmed)?
            .checked_add(timeout.as_millis() as u64)
            .ok_or(CatalogError::Limit)?;
        self.sync_managed_transfer_voter(
            transition,
            InstallData::Prepared(child),
            log,
            host,
            deadline,
        )
        .await
    }

    pub(crate) async fn reconcile_managed_transfer_voter(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        route: &GroupRoute,
        log: &ReplicaLog,
        host: &FleetHostAuthorization,
        deadline: u64,
        closure: Option<&PreparedGroupManifest>,
    ) -> Result<Option<InstalledTransferVoter>, CatalogError> {
        let group = match log.cell_group(&route.history) {
            Ok(group) => group,
            Err(LogError::NotFound) => return Ok(None),
            Err(error) => return Err(CatalogError::Log(error)),
        };
        let selected = group
            .group_manifest()
            .map_err(CatalogError::Log)?
            .filter(|marker| marker.route() == route)
            .ok_or(CatalogError::Fenced)?;
        let marker = pending_genesis(&selected)?;
        self.sync_managed_transfer_voter(
            transition,
            InstallData::Retained {
                marker: &marker,
                selected: &selected,
                closure,
            },
            log,
            host,
            deadline,
        )
        .await
        .map(Some)
    }

    async fn sync_managed_transfer_voter(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        data: InstallData<'_>,
        log: &ReplicaLog,
        host: &FleetHostAuthorization,
        deadline: u64,
    ) -> Result<InstalledTransferVoter, CatalogError> {
        let managed = ManagedInstall {
            transition,
            host,
            deadline,
        };
        let group = log
            .domain()
            .run(Box::pin(crate::asyncrt::timeout_at(
                deadline,
                Box::pin(self.install_transfer_voter_inner(
                    &transition.plan,
                    data,
                    log,
                    Some(managed),
                )),
            )))
            .await
            .map_err(|_| CatalogError::Unconfirmed)?
            .map_err(|_| CatalogError::Unconfirmed)??;
        let local = group.identity();
        Ok(InstalledTransferVoter {
            member: local.member,
            node: local.node,
            disk: local.disk,
            engine: log.storage_instance().map_err(CatalogError::Log)?,
        })
    }

    async fn install_transfer_voter_inner(
        &self,
        plan: &DirectoryMutation,
        data: InstallData<'_>,
        log: &ReplicaLog,
        managed: Option<ManagedInstall<'_>>,
    ) -> Result<GroupLog<GroupHistory>, CatalogError> {
        let genesis = data.genesis()?;
        self.bounded(async {
            let authority = if let Some(managed) = &managed {
                let boot = self.require_managed_group_boot(log, managed.host).await?;
                let authority = managed
                    .host
                    .managed_transfer_install_authority(
                        managed.transition,
                        self,
                        genesis.route(),
                        managed.remaining(log)?,
                    )
                    .await
                    .map_err(CatalogError::from)?;
                authority.transition.check(self, plan)?;
                if authority.boot != boot {
                    return Err(CatalogError::Fenced);
                }
                Some((managed, authority))
            } else {
                self.namespace.require_cell_group_unmanaged().await?;
                None
            };
            self.require_open_transfer_install(plan, genesis).await?;
            let birth = self
                .require_ordinary_birth_reservation(genesis.route())
                .await?;
            let voter = birth
                .voters()
                .iter()
                .find(|member| member.node == log.limits().node)
                .ok_or(CatalogError::NotInitialMember)?;
            self.namespace.require_node_storage(log).await?;
            let engine = log.storage_instance().map_err(CatalogError::Log)?;
            let route = genesis.route();
            let local = ReplicaIdentity {
                scope: route.history,
                member: voter.member,
                node: voter.node,
                disk: log.limits().disk,
                process: log.limits().process,
            };
            let records = self.install_records(genesis, &local, engine)?;
            match &data {
                InstallData::Prepared(_) => {
                    self.exact_install_record(
                        &format!("{}/binding", records.prefix),
                        &records.binding,
                    )
                    .await?;
                    self.exact_install_record(
                        &format!("{}/intent", records.member),
                        &records.intent,
                    )
                    .await?;
                }
                InstallData::Retained { .. } => self.require_install_intent(&records).await?,
            }
            let receipt = format!("{}/receipt", records.member);
            if let Some(observed) = self.read(&receipt).await? {
                if observed != records.intent {
                    return Err(CatalogError::Conflict("transfer install receipt"));
                }
                match log.cell_group(&route.history) {
                    Ok(_) => (),
                    Err(LogError::NotFound) => return Err(CatalogError::LostStableState),
                    Err(error) => return Err(CatalogError::Log(error)),
                }
            }
            self.require_open_transfer_install(plan, genesis).await?;
            let managed = if let Some((managed, original)) = authority {
                let authority = managed
                    .host
                    .managed_transfer_install_authority(
                        managed.transition,
                        self,
                        route,
                        managed.remaining(log)?,
                    )
                    .await
                    .map_err(CatalogError::from)?;
                authority.transition.check(self, plan)?;
                if authority.group != original.group || authority.boot != original.boot {
                    return Err(CatalogError::Fenced);
                }
                Some(ManagedSeed {
                    group: authority.group,
                    boot: authority.boot,
                    deadline: managed.deadline,
                })
            } else {
                None
            };
            let permit = TransferSeedPermit {
                owner: log.owner(),
                bootstrap: CellGroupBootstrap {
                    route: route.clone(),
                    member: voter.member,
                    voters: birth.voters().iter().map(|m| m.member).collect(),
                    learners: birth.learners().iter().map(|m| m.member).collect(),
                },
                genesis: genesis.clone(),
                local,
                engine,
                managed,
            };
            let group = match &data {
                InstallData::Prepared(child) => {
                    let seed = TransferSeed::new(permit, child).map_err(CatalogError::Log)?;
                    log.install_transfer_seed(seed)
                        .await
                        .map_err(CatalogError::Log)?
                }
                InstallData::Retained {
                    selected, closure, ..
                } => {
                    let group = log.cell_group(&route.history).map_err(CatalogError::Log)?;
                    let guard = log.lifecycle_guard().map_err(CatalogError::Log)?;
                    let now = log
                        .domain()
                        .mono_ms()
                        .map_err(|_| CatalogError::Unconfirmed)?;
                    let binding = permit
                        .managed_binding(guard.as_ref(), now)
                        .map_err(CatalogError::Log)?
                        .ok_or(CatalogError::ManagedAdmissionRequired)?;
                    let cache = group.read().map_err(CatalogError::Log)?;
                    if group.identity() != &permit.local
                        || cache.group_manifest.as_ref() != Some(*selected)
                        || cache
                            .membership
                            .fleet_status()
                            .is_none_or(|status| &status.binding != binding)
                    {
                        return Err(CatalogError::Fenced);
                    }
                    let reopened = closure.is_some_and(|verified| {
                        Arc::ptr_eq(&verified.engine_owner, &group.owner)
                            && verified.marker() == *selected
                            && verified.images.root() == genesis.root()
                    });
                    if !reopened
                        && cache
                            .manifest_pins
                            .as_ref()
                            .is_none_or(|pins| pins.root() != genesis.root())
                    {
                        return Err(CatalogError::Runtime(
                            crate::raft::completion::RuntimeError::NotReady,
                        ));
                    }
                    drop(cache);
                    group
                }
            };
            self.require_open_transfer_install(plan, genesis).await?;
            self.exact_install_record(&receipt, &records.intent).await?;
            Ok(group)
        })
        .await
    }

    /// Observe an exact majority of synced installs for the selected child.
    /// Missing receipts leave a child below quorum. Corrupt receipts, stale
    /// storage claims or two receipts sharing physical identity fail closed.
    pub async fn observe_transfer_install_quorum(
        &self,
        plan: &DirectoryMutation,
        history: GroupHistory,
    ) -> Result<Option<TransferInstallQuorum>, CatalogError> {
        self.unmanaged_birth()?;
        self.observe_transfer_installs(plan, history, false).await
    }

    pub(crate) async fn observe_managed_transfer_install_quorum(
        &self,
        plan: &DirectoryMutation,
        permit: &crate::raft::fleet_catalog::ManagedTransitionPermit,
        history: GroupHistory,
    ) -> Result<Option<TransferInstallQuorum>, CatalogError> {
        permit.check(self, plan)?;
        self.observe_transfer_installs(plan, history, true).await
    }

    async fn observe_transfer_installs(
        &self,
        plan: &DirectoryMutation,
        history: GroupHistory,
        managed: bool,
    ) -> Result<Option<TransferInstallQuorum>, CatalogError> {
        let (_, children) = self.transition_routes(plan)?;
        let route = children
            .iter()
            .find(|route| route.history == history)
            .ok_or(CatalogError::Conflict("transfer child outside plan"))?;
        self.bounded(async {
            if !managed {
                self.namespace.require_cell_group_unmanaged().await?;
            }
            let prefix = install_prefix(route);
            let Some(binding) = self.read(&format!("{prefix}/binding")).await? else {
                return Ok(None);
            };
            let selected: InstallBinding = record::decode(BINDING, &binding, wire::LIMIT)?;
            if selected.namespace != self.namespace.root_digest || selected.genesis.route() != route
            {
                return Err(CatalogError::Corrupt("transfer install genesis binding"));
            }
            let phase = self
                .require_transfer_install(plan, &selected.genesis)
                .await?;
            if managed && phase == TransitionPhase::Retired {
                return Err(CatalogError::Fenced);
            }
            let birth = self.require_ordinary_birth_reservation(route).await?;
            let digest = record::digest(&binding);
            let mut voters = Vec::with_capacity(birth.voters().len());
            let mut disks = BTreeSet::new();
            let mut engines = BTreeSet::new();
            for voter in birth.voters() {
                let member = format!("{prefix}/{:016x}", voter.member.get());
                let Some(receipt) = self.read(&format!("{member}/receipt")).await? else {
                    continue;
                };
                if self.read(&format!("{member}/intent")).await?.as_ref() != Some(&receipt) {
                    return Err(CatalogError::Corrupt(
                        "transfer install receipt without exact intent",
                    ));
                }
                let intent: InstallIntent = record::decode(INTENT, &receipt, wire::LIMIT)?;
                if intent.binding != digest
                    || intent.member != voter.member.get()
                    || intent.node != voter.node.bytes()
                {
                    return Err(CatalogError::Corrupt("transfer install voter identity"));
                }
                let disk = DiskLineage::new(intent.disk)
                    .map_err(|_| CatalogError::Corrupt("transfer install disk"))?;
                let engine = EngineInstanceId::new(intent.engine)
                    .map_err(|_| CatalogError::Corrupt("transfer install engine"))?;
                if !disks.insert(disk) || !engines.insert(engine) {
                    return Err(CatalogError::Conflict("shared transfer install storage"));
                }
                self.namespace
                    .verify_node_storage_identity(voter.node, disk, engine)
                    .await?;
                voters.push(InstalledTransferVoter {
                    member: voter.member,
                    node: voter.node,
                    disk,
                    engine,
                });
            }
            let phase = self
                .require_transfer_install(plan, &selected.genesis)
                .await?;
            if managed && phase == TransitionPhase::Retired {
                return Err(CatalogError::Fenced);
            }
            Ok(
                (voters.len() > birth.voters().len() / 2).then_some(TransferInstallQuorum {
                    genesis: selected.genesis,
                    voters,
                }),
            )
        })
        .await
    }

    /// Admit a physically installed voter to the pending child Raft group.
    /// The complete local image must still be recovered before attachment.
    /// This grants no routing, application or activation authority.
    pub fn bootstrap_pending_transfer_voter<'a>(
        &'a self,
        plan: &'a DirectoryMutation,
        history: GroupHistory,
        log: &'a ReplicaLog,
    ) -> futures_util::future::BoxFuture<'a, Result<CellGroupBootstrapPermit, CatalogError>> {
        Box::pin(async move {
            let proof = self
                .observe_transfer_install_quorum(plan, history)
                .await?
                .ok_or(CatalogError::Unconfirmed)?;
            let (group, birth) = self.pending_transfer_member(plan, &proof, log).await?;
            Ok(CellGroupBootstrapPermit::pending_transfer(
                group,
                birth,
                self.clone(),
                log.clone(),
                super::bootstrap::PendingTransfer::Unmanaged(Arc::new(
                    self.decode_mutation(plan.encode())?,
                )),
            ))
        })
    }

    pub(crate) fn bootstrap_managed_pending_transfer_voter<'a>(
        &'a self,
        transition: Arc<ReopenedManagedCellGroupTransition>,
        route: &'a GroupRoute,
        log: &'a ReplicaLog,
        host: &'a FleetHostAuthorization,
        deadline: u64,
    ) -> futures_util::future::BoxFuture<'a, Result<CellGroupBootstrapPermit, CatalogError>> {
        Box::pin(async move {
            let domain = log.domain();
            let remaining = || {
                let now = domain.mono_ms().map_err(|_| CatalogError::Unconfirmed)?;
                deadline
                    .checked_sub(now)
                    .filter(|ms| *ms > 0)
                    .map(Duration::from_millis)
                    .ok_or(CatalogError::Runtime(
                        crate::raft::completion::RuntimeError::DeadlineExceeded,
                    ))
            };
            let proof = host
                .stop_catalog()
                .observe_managed_transfer_install_quorum(&transition, self, route, remaining()?)
                .await
                .map_err(CatalogError::from)?
                .ok_or(CatalogError::Unconfirmed)?;
            let (group, birth) = self
                .pending_transfer_member(&transition.plan, &proof, log)
                .await?;
            let authority = host
                .managed_transfer_install_authority(&transition, self, route, remaining()?)
                .await
                .map_err(CatalogError::from)?;
            let boot = self.require_managed_group_boot(log, host).await?;
            remaining()?;
            let cache = group.read().map_err(CatalogError::Log)?;
            if boot != authority.boot
                || cache
                    .membership
                    .fleet_status()
                    .is_none_or(|status| status.binding != authority.group)
                || cache
                    .group_manifest
                    .as_ref()
                    .is_none_or(|marker| marker.transfer_activation().is_some())
            {
                return Err(CatalogError::Fenced);
            }
            drop(cache);
            Ok(CellGroupBootstrapPermit::pending_transfer(
                group,
                birth,
                self.clone(),
                log.clone(),
                super::bootstrap::PendingTransfer::Managed {
                    transition,
                    host: Box::new(host.clone()),
                    deadline,
                },
            ))
        })
    }

    async fn pending_transfer_member(
        &self,
        plan: &DirectoryMutation,
        proof: &TransferInstallQuorum,
        log: &ReplicaLog,
    ) -> Result<(GroupLog<GroupHistory>, CellGroupBirthRequest), CatalogError> {
        self.bounded(async {
            let local = log.limits();
            let voter = proof
                .voters
                .iter()
                .find(|receipt| receipt.node == local.node)
                .ok_or(CatalogError::NotInitialMember)?;
            let engine = log.storage_instance().map_err(CatalogError::Log)?;
            if voter.disk != local.disk || voter.engine != engine {
                return Err(CatalogError::LostStableState);
            }
            let group =
                log.cell_group(&proof.genesis.route().history)
                    .map_err(|error| match error {
                        LogError::NotFound => CatalogError::LostStableState,
                        error => CatalogError::Log(error),
                    })?;
            let birth = self
                .require_ordinary_birth_reservation(proof.genesis.route())
                .await?;
            let activation = {
                let cache = group.read().map_err(CatalogError::Log)?;
                let marker = cache
                    .group_manifest
                    .as_ref()
                    .ok_or(CatalogError::LostStableState)?;
                let expected_voters = birth.voters().iter().map(|member| member.member.get());
                let expected_learners = birth.learners().iter().map(|member| member.member.get());
                if group.identity().member != voter.member
                    || marker.route() != proof.genesis.route()
                    || !marker.preserves_transfer(&proof.genesis)
                {
                    return Err(CatalogError::LostStableState);
                }
                let activation = marker.transfer_activation();
                if activation.is_none()
                    && (cache.membership.revision().get() != 1
                        || !cache.conf_state.voters.iter().copied().eq(expected_voters)
                        || !cache
                            .conf_state
                            .learners
                            .iter()
                            .copied()
                            .eq(expected_learners)
                        || !cache.conf_state.voters_outgoing.is_empty()
                        || !cache.conf_state.learners_next.is_empty()
                        || cache.conf_state.auto_leave)
                {
                    return Err(CatalogError::LostStableState);
                }
                activation
            };
            if let Some((certificate, _, _)) = activation {
                let published = self.publication(plan).await?.ok_or(CatalogError::Fenced)?;
                if published.transfer_archive().is_none()
                    || published.journal_digest() != certificate
                {
                    return Err(CatalogError::Fenced);
                }
            }
            super::bootstrap::check_member(&group)?;
            let phase = self.require_transfer_install(plan, &proof.genesis).await?;
            if phase == TransitionPhase::Retired || activation.is_some() {
                let (view, _) = self.current().await?;
                self.selected_route_in(&view, proof.genesis.route()).await?;
            }
            Ok((group, birth))
        })
        .await
    }

    async fn require_open_transfer_install(
        &self,
        plan: &DirectoryMutation,
        genesis: &GroupManifest,
    ) -> Result<(), CatalogError> {
        if self.require_transfer_install(plan, genesis).await? == TransitionPhase::Retired {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }

    async fn require_transfer_install(
        &self,
        plan: &DirectoryMutation,
        genesis: &GroupManifest,
    ) -> Result<TransitionPhase, CatalogError> {
        let (_, children) = self.transition_routes(plan)?;
        let (view, _) = self.current().await?;
        let journal = self
            .matching_transition(&view.head, plan)
            .await?
            .ok_or(CatalogError::Fenced)?;
        let origin = genesis.transfer_origin().ok_or(CatalogError::Fenced)?;
        if !genesis.is_transfer_genesis()
            || !matches!(
                journal.phase(),
                TransitionPhase::Sealed
                    | TransitionPhase::Installed
                    | TransitionPhase::Archived
                    | TransitionPhase::Published
                    | TransitionPhase::Active
                    | TransitionPhase::Retired
            )
            || journal.sealed_sources() != Some(origin.sealed_sources())
            || origin.operation() != plan.operation().bytes()
            || origin.plan() != plan.digest()
            || !children.contains(genesis.route())
        {
            return Err(CatalogError::Fenced);
        }
        Ok(journal.phase())
    }

    async fn exact_install_record(&self, path: &str, expected: &[u8]) -> Result<(), CatalogError> {
        let observed = self
            .namespace
            .inner
            .create_and_observe_bounded(&self.key(path), expected, wire::LIMIT)
            .await?;
        if observed != expected {
            return Err(CatalogError::Conflict("transfer install binding"));
        }
        Ok(())
    }
}
