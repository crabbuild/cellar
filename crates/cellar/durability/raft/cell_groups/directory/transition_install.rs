// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A selected sealed-source decision can install a child on its exact engine.
use super::*;
use crate::raft::{
    cache::GroupLog,
    cell_groups::{GroupManifest, PreparedCellGroupTransferChild, TransferSeed},
    storage::{CellGroupBootstrap, LogError, ReplicaLog},
};
use cellar_logic::consensus::{EngineInstanceId, ReplicaIdentity};
use serde::Serialize;

const BINDING: u8 = 203;
const INTENT: u8 = 204;

#[derive(Serialize)]
struct InstallBinding<'a> {
    namespace: [u8; 32],
    genesis: &'a GroupManifest,
}

#[derive(Serialize)]
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
            let prefix = format!(
                "transfer-installs/{}/{}",
                hex::encode(route.history.group.bytes()),
                hex::encode(route.history.incarnation.bytes()),
            );
            let binding = record::encode(
                BINDING,
                &InstallBinding {
                    namespace: self.namespace.root_digest,
                    genesis,
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
            || journal.phase() != TransitionPhase::Sealed
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
