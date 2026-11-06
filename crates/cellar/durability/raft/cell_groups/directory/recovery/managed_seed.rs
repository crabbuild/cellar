// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One managed voter seed. The Fleet reservation and directory recovery origin
//! must both exist before the writer can sync a non-serving genesis.
use super::{
    seed::{ManagedRecoverySeed, RecoverySeedPermit},
    *,
};
use crate::raft::{
    cell_groups::{CellGroupApplier, GroupManifest, PreparedRecoveryData, RecoverySeed},
    fleet_catalog::ManagedRecoverySeedAuthority,
    fleet_stop::BootBinding,
    storage::{LogError, ReplicaLog},
};
use cellar_logic::consensus::MemberId;

const BINDING: u8 = 226;
const INTENT: u8 = 227;
const COMPLETE: u8 = 228;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    plan: [u8; 32],
    genesis: GroupManifest,
    commitment: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    binding: [u8; 32],
    member: u64,
    boot: BootBinding,
}

/// Exact durable receipt for a managed recovery voter. Publication and
/// activation require independent topology and quorum proofs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedRecoveryVoterSeed {
    pub route: GroupRoute,
    pub plan_digest: [u8; 32],
    pub genesis_commitment: [u8; 32],
    pub member: MemberId,
    pub boot: BootBinding,
}

pub(crate) struct PendingManagedRecoverySeed {
    proof: ManagedRecoveryVoterSeed,
    binding: [u8; 32],
}

impl RangeDirectory {
    pub(crate) async fn seed_managed_recovery_voter(
        &self,
        domain: &impl CellGroupArchiveExecution,
        target: &crate::raft::catalog::CellGroupBirthRequest,
        prepared: PreparedRecoveryData,
        authority: ManagedRecoverySeedAuthority,
        log: &ReplicaLog,
        deadline: u64,
    ) -> Result<PendingManagedRecoverySeed, CellGroupRecoveryError> {
        let marker = prepared.manifest.marker();
        let plan = marker.recovery_request().ok_or(CatalogError::Fenced)?;
        if !marker.is_recovery_genesis()
            || marker.route() != target.route()
            || prepared.store.route() != target.route()
            || marker
                .recovery_commitment()
                .map_err(|_| CatalogError::Corrupt("managed seed commitment"))?
                != Some(prepared.commitment)
            || !authority.group.matches_scope(&target.route().history)
            || authority.boot.node != log.limits().node.bytes()
            || authority.boot.disk != log.limits().disk.bytes()
            || authority.boot.process != log.limits().process.bytes()
            || authority.boot.engine != log.storage_instance().map_err(CatalogError::Log)?.bytes()
        {
            return Err(CatalogError::Fenced.into());
        }
        self.require_managed_recovery_identity(target, plan).await?;
        let _node = self.namespace.require_node_storage(log).await?;
        let member = target
            .voters()
            .iter()
            .find(|member| member.node == log.limits().node)
            .ok_or(CatalogError::NotInitialMember)?;
        let binding_bytes = record::encode(
            BINDING,
            &Binding {
                plan,
                genesis: marker.clone(),
                commitment: prepared.commitment,
            },
            wire::LIMIT,
        )?;
        let binding = record::digest(&binding_bytes);
        let proof = ManagedRecoveryVoterSeed {
            route: target.route().clone(),
            plan_digest: plan,
            genesis_commitment: prepared.commitment,
            member: member.member,
            boot: authority.boot.clone(),
        };
        let pending = PendingManagedRecoverySeed { proof, binding };
        let (intent, complete) = pending.receipts()?;
        self.bounded(async {
            self.put_exact_managed_seed(
                &self.managed_seed_key(target.route(), "binding"),
                binding_bytes,
            )
            .await?;
            let key = self.managed_seed_key(target.route(), &pending.member_key("complete"));
            if let Some((bytes, _)) = self
                .namespace
                .inner
                .read_versioned_bounded(&key, wire::LIMIT)
                .await?
            {
                if bytes != complete {
                    return Err(CatalogError::Conflict("managed recovery seed receipt"));
                }
                log.cell_group(&target.route().history)
                    .map_err(|error| match error {
                        LogError::NotFound => CatalogError::LostStableState,
                        error => CatalogError::Log(error),
                    })?;
            }
            self.put_exact_managed_seed(
                &self.managed_seed_key(target.route(), &pending.member_key("intent")),
                intent,
            )
            .await
        })
        .await?;

        let permit = RecoverySeedPermit::for_local(
            target,
            marker,
            prepared.commitment,
            log,
            Some(ManagedRecoverySeed {
                group: authority.group,
                boot: authority.boot,
                deadline,
            }),
        )?;
        let genesis = marker.clone();
        let seed = RecoverySeed::new(permit, prepared.clone()).map_err(CatalogError::Log)?;
        let group = Box::pin(log.install_recovery_seed(seed))
            .await
            .map_err(CatalogError::Log)?;
        domain
            .blocking(move || CellGroupApplier::recover_seeded(group, &prepared.store, &genesis))
            .map_err(|_| CellGroupArchiveError::Stopped)?
            .await
            .map_err(CellGroupArchiveError::Worker)?
            .map_err(CellGroupArchiveError::Replay)?;
        Ok(pending)
    }

    pub(crate) async fn confirm_managed_recovery_voter(
        &self,
        target: &crate::raft::catalog::CellGroupBirthRequest,
        pending: PendingManagedRecoverySeed,
        authority: &ManagedRecoverySeedAuthority,
    ) -> Result<ManagedRecoveryVoterSeed, CatalogError> {
        let proof = &pending.proof;
        if proof.route != *target.route()
            || proof.boot != authority.boot
            || !authority.group.matches_scope(&proof.route.history)
        {
            return Err(CatalogError::Fenced);
        }
        self.require_managed_recovery_identity(target, proof.plan_digest)
            .await?;
        let (_, complete) = pending.receipts()?;
        self.bounded(self.put_exact_managed_seed(
            &self.managed_seed_key(&proof.route, &pending.member_key("complete")),
            complete,
        ))
        .await?;
        Ok(pending.proof)
    }

    fn managed_seed_key(&self, route: &GroupRoute, leaf: &str) -> Path {
        self.key(&format!(
            "managed-recovery-seeds/{}/{}/{leaf}",
            hex::encode(route.history.group.bytes()),
            hex::encode(route.history.incarnation.bytes())
        ))
    }

    async fn put_exact_managed_seed(&self, key: &Path, bytes: Vec<u8>) -> Result<(), CatalogError> {
        let actual = self
            .namespace
            .inner
            .create_and_observe_bounded(key, &bytes, wire::LIMIT)
            .await?;
        if actual != bytes {
            return Err(CatalogError::Conflict("managed recovery seed identity"));
        }
        Ok(())
    }
}

impl PendingManagedRecoverySeed {
    fn member_key(&self, leaf: &str) -> String {
        format!("voters/{:016x}/{leaf}", self.proof.member.get())
    }

    fn receipts(&self) -> Result<(Vec<u8>, Vec<u8>), CatalogError> {
        let receipt = Receipt {
            binding: self.binding,
            member: self.proof.member.get(),
            boot: self.proof.boot.clone(),
        };
        Ok((
            record::encode(INTENT, &receipt, wire::LIMIT)?,
            record::encode(COMPLETE, &receipt, wire::LIMIT)?,
        ))
    }
}
