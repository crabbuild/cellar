// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A selected sealed-source decision can install a child on its exact engine.
use super::*;
use crate::raft::{
    cache::GroupLog,
    cell_groups::{GroupManifest, PreparedCellGroupTransferChild, TransferSeed},
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

impl RangeDirectory {
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
        let genesis = child
            .genesis()
            .ok_or(CatalogError::Conflict(
                "transfer requires final child genesis",
            ))?
            .marker();
        self.bounded(async {
            self.namespace.require_cell_group_unmanaged().await?;
            self.require_transfer_install(plan, genesis).await?;
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
            let prefix = install_prefix(route);
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
                    member: voter.member.get(),
                    node: voter.node.bytes(),
                    disk: local.disk.bytes(),
                    engine: engine.bytes(),
                },
                wire::LIMIT,
            )?;
            let member = format!("{prefix}/{:016x}", voter.member.get());
            self.exact_install_record(&format!("{prefix}/binding"), &binding)
                .await?;
            self.exact_install_record(&format!("{member}/intent"), &intent)
                .await?;
            let receipt = format!("{member}/receipt");
            if let Some(observed) = self.read(&receipt).await? {
                if observed != intent {
                    return Err(CatalogError::Conflict("transfer install receipt"));
                }
                match log.cell_group(&route.history) {
                    Ok(_) => (),
                    Err(LogError::NotFound) => return Err(CatalogError::LostStableState),
                    Err(error) => return Err(CatalogError::Log(error)),
                }
            }
            self.require_transfer_install(plan, genesis).await?;
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
            };
            let seed = TransferSeed::new(permit, child).map_err(CatalogError::Log)?;
            let group = log
                .install_transfer_seed(seed)
                .await
                .map_err(CatalogError::Log)?;
            self.require_transfer_install(plan, genesis).await?;
            self.exact_install_record(&receipt, &intent).await?;
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
        let (_, children) = self.transition_routes(plan)?;
        let route = children
            .iter()
            .find(|route| route.history == history)
            .ok_or(CatalogError::Conflict("transfer child outside plan"))?;
        self.bounded(async {
            self.namespace.require_cell_group_unmanaged().await?;
            let prefix = install_prefix(route);
            let Some(binding) = self.read(&format!("{prefix}/binding")).await? else {
                return Ok(None);
            };
            let selected: InstallBinding = record::decode(BINDING, &binding, wire::LIMIT)?;
            if selected.namespace != self.namespace.root_digest || selected.genesis.route() != route
            {
                return Err(CatalogError::Corrupt("transfer install genesis binding"));
            }
            self.require_transfer_install(plan, &selected.genesis)
                .await?;
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
            self.require_transfer_install(plan, &selected.genesis)
                .await?;
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
                let group = log.cell_group(&history).map_err(|error| match error {
                    LogError::NotFound => CatalogError::LostStableState,
                    error => CatalogError::Log(error),
                })?;
                let birth = self
                    .require_ordinary_birth_reservation(proof.genesis.route())
                    .await?;
                {
                    let cache = group.read().map_err(CatalogError::Log)?;
                    let expected_voters = birth.voters().iter().map(|member| member.member.get());
                    let expected_learners =
                        birth.learners().iter().map(|member| member.member.get());
                    if group.identity().member != voter.member
                        || cache.group_manifest.as_ref().is_none_or(|marker| {
                            marker.route() != proof.genesis.route()
                                || marker.is_active()
                                || !marker.preserves_transfer(&proof.genesis)
                        })
                        || cache.membership.revision().get() != 1
                        || !cache.conf_state.voters.iter().copied().eq(expected_voters)
                        || !cache
                            .conf_state
                            .learners
                            .iter()
                            .copied()
                            .eq(expected_learners)
                        || !cache.conf_state.voters_outgoing.is_empty()
                        || !cache.conf_state.learners_next.is_empty()
                        || cache.conf_state.auto_leave
                    {
                        return Err(CatalogError::LostStableState);
                    }
                }
                super::bootstrap::check_member(&group)?;
                self.require_transfer_install(plan, &proof.genesis).await?;
                Ok(CellGroupBootstrapPermit::pending_transfer(
                    group,
                    birth,
                    self.clone(),
                    log.clone(),
                    self.decode_mutation(plan.encode())?,
                ))
            })
            .await
        })
    }

    async fn require_transfer_install(
        &self,
        plan: &DirectoryMutation,
        genesis: &GroupManifest,
    ) -> Result<(), CatalogError> {
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
            )
            || journal.sealed_sources() != Some(origin.sealed_sources())
            || origin.operation() != plan.operation().bytes()
            || origin.plan() != plan.digest()
            || !children.contains(genesis.route())
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
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
