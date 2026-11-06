// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One managed voter seed. The Fleet reservation and directory recovery origin
//! must both exist before the writer can sync a non-serving genesis.
use super::{
    seed::{ManagedRecoverySeed, RecoverySeedPermit},
    *,
};
use crate::raft::{
    cache::GroupLog,
    cell_groups::{CellGroupApplier, GroupManifest, PreparedRecoveryData, RecoverySeed},
    fleet_catalog::ManagedRecoverySeedAuthority,
    fleet_stop::BootBinding,
    member_state::FleetGroupBinding,
    storage::{LogError, ReplicaLog},
};
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, MemberId};

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

/// The immutable genesis and completed voter set for one replacement range.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedRecoverySeedQuorum {
    birth: [u8; 32],
    binding: [u8; 32],
    commitment: [u8; 32],
    members: Vec<u64>,
}

impl ManagedRecoverySeedQuorum {
    pub fn birth_digest(&self) -> [u8; 32] {
        self.birth
    }

    pub fn genesis_commitment(&self) -> [u8; 32] {
        self.commitment
    }

    pub fn members(&self) -> &[u64] {
        &self.members
    }
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
    ) -> Result<(PendingManagedRecoverySeed, CellGroupApplier), CellGroupRecoveryError> {
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
        let (applier, _) = domain
            .blocking(move || CellGroupApplier::recover_seeded(group, &prepared.store, &genesis))
            .map_err(|_| CellGroupArchiveError::Stopped)?
            .await
            .map_err(CellGroupArchiveError::Worker)?
            .map_err(CellGroupArchiveError::Replay)?;
        Ok((pending, applier))
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

    /// Read the immutable receipts for a quorum of the planned voters.
    /// Reopening a selected set never silently includes a later completion.
    pub(crate) async fn observe_managed_recovery_seed_quorum(
        &self,
        target: &crate::raft::catalog::CellGroupBirthRequest,
        plan: [u8; 32],
        boots: &[BootBinding],
        selected: Option<&ManagedRecoverySeedQuorum>,
    ) -> Result<ManagedRecoverySeedQuorum, CatalogError> {
        self.bounded(async {
            self.require_managed_recovery_identity(target, plan).await?;
            let route = target.route();
            let voters = target.voters();
            if voters.len() != 3
                || boots.len() != voters.len()
                || !target.learners().is_empty()
                || selected.is_some_and(|value| {
                    value.birth != route.birth_digest
                        || !(2..=voters.len()).contains(&value.members.len())
                        || value.members.windows(2).any(|pair| pair[0] >= pair[1])
                })
            {
                return Err(CatalogError::Fenced);
            }
            let (binding, digest) = self.managed_seed_binding(route, plan).await?;
            let mut disks = BTreeSet::new();
            let mut engines = BTreeSet::new();
            let mut members = Vec::with_capacity(voters.len());
            for (voter, boot) in voters.iter().zip(boots) {
                if boot.node != voter.node.bytes() {
                    return Err(CatalogError::Corrupt("managed seed voter boot binding"));
                }
                let member = voter.member.get();
                if selected.is_some_and(|value| value.members.binary_search(&member).is_err()) {
                    continue;
                }
                if !self
                    .has_managed_seed_receipt(route, member, boot, digest)
                    .await?
                {
                    if selected.is_some() {
                        return Err(CatalogError::Unconfirmed);
                    }
                    continue;
                }
                boot.validate().map_err(CatalogError::Log)?;
                let disk = DiskLineage::new(boot.disk)
                    .map_err(|_| CatalogError::Corrupt("managed seed disk"))?;
                let engine = EngineInstanceId::new(boot.engine)
                    .map_err(|_| CatalogError::Corrupt("managed seed engine"))?;
                if !disks.insert(disk) || !engines.insert(engine) {
                    return Err(CatalogError::Conflict("duplicate managed seed storage"));
                }
                self.namespace
                    .verify_node_storage_identity(voter.node, disk, engine)
                    .await?;
                members.push(member);
            }
            if members.len() <= voters.len() / 2 {
                return Err(CatalogError::Unconfirmed);
            }
            let quorum = ManagedRecoverySeedQuorum {
                birth: route.birth_digest,
                binding: digest,
                commitment: binding.commitment,
                members,
            };
            if selected.is_some_and(|expected| expected != &quorum) {
                return Err(CatalogError::Corrupt("managed seed quorum binding"));
            }
            Ok(quorum)
        })
        .await
    }

    pub(super) async fn require_published_managed_voter_seed(
        &self,
        target: &CellGroupBirthRequest,
        plan: [u8; 32],
        selected: &ManagedRecoverySeedQuorum,
        voter: MemberId,
        boot: &BootBinding,
    ) -> Result<GroupManifest, CatalogError> {
        self.require_managed_recovery_identity(target, plan).await?;
        let route = target.route();
        let (binding, digest) = self.managed_seed_binding(route, plan).await?;
        if selected.birth != route.birth_digest
            || selected.binding != digest
            || selected.commitment != binding.commitment
        {
            return Err(CatalogError::Corrupt("published recovery seed binding"));
        }
        // A voter may have completed its seed without being needed in the
        // selected majority. It must still present its own exact receipt.
        if !self
            .has_managed_seed_receipt(route, voter.get(), boot, digest)
            .await?
        {
            return Err(CatalogError::LostStableState);
        }
        Ok(binding.genesis)
    }

    /// A crash may leave a synced genesis with only its seed intent. Retain
    /// that owner, but require a complete receipt separately before voting.
    pub(super) async fn require_retained_managed_seed_intent(
        &self,
        group: &GroupLog<GroupHistory>,
        route: &GroupRoute,
        plan: [u8; 32],
        fleet: &FleetGroupBinding,
        engine: EngineInstanceId,
    ) -> Result<(GroupManifest, [u8; 32]), CatalogError> {
        let local = group.identity();
        let (binding, digest) = self.managed_seed_binding(route, plan).await?;
        let member = local.member.get();
        let intent = self.read_managed_seed_intent(route, member).await?;
        let receipt: Receipt = record::decode(INTENT, &intent, wire::LIMIT)?;
        let boot = &receipt.boot;
        boot.validate().map_err(CatalogError::Log)?;
        if receipt.binding != digest
            || receipt.member != member
            || boot.catalog != fleet.catalog()
            || boot.fleet != fleet.fleet()
            || boot.node != local.node.bytes()
            || boot.disk != local.disk.bytes()
            || boot.engine != engine.bytes()
        {
            return Err(CatalogError::LostStableState);
        }
        let expected = record::encode(COMPLETE, &receipt, wire::LIMIT)?;
        if self
            .read_managed_seed_complete(route, member)
            .await?
            .is_some_and(|complete| complete != expected)
        {
            return Err(CatalogError::Corrupt("retained managed seed receipt"));
        }
        self.namespace
            .verify_node_storage_identity(local.node, local.disk, engine)
            .await?;
        Ok((binding.genesis, binding.commitment))
    }

    async fn managed_seed_binding(
        &self,
        route: &GroupRoute,
        plan: [u8; 32],
    ) -> Result<(Binding, [u8; 32]), CatalogError> {
        let binding_key = self.managed_seed_key(route, "binding");
        let (bytes, _) = self
            .namespace
            .inner
            .read_versioned_bounded(&binding_key, wire::LIMIT)
            .await?
            .ok_or(CatalogError::Unconfirmed)?;
        let binding: Binding = record::decode(BINDING, &bytes, wire::LIMIT)?;
        if binding.plan != plan
            || !binding.genesis.is_recovery_genesis()
            || binding.genesis.route() != route
            || binding.genesis.recovery_request() != Some(plan)
            || binding
                .genesis
                .recovery_commitment()
                .map_err(|_| CatalogError::Corrupt("managed seed genesis"))?
                != Some(binding.commitment)
        {
            return Err(CatalogError::Corrupt("managed seed genesis binding"));
        }
        let digest = record::digest(&bytes);
        Ok((binding, digest))
    }

    async fn has_managed_seed_receipt(
        &self,
        route: &GroupRoute,
        member: u64,
        boot: &BootBinding,
        digest: [u8; 32],
    ) -> Result<bool, CatalogError> {
        let receipt = Receipt {
            binding: digest,
            member,
            boot: boot.clone(),
        };
        let Some(complete) = self.read_managed_seed_complete(route, member).await? else {
            return Ok(false);
        };
        let intent = self.read_managed_seed_intent(route, member).await?;
        if complete != record::encode(COMPLETE, &receipt, wire::LIMIT)?
            || intent != record::encode(INTENT, &receipt, wire::LIMIT)?
        {
            return Err(CatalogError::Corrupt("managed seed receipt binding"));
        }
        Ok(true)
    }

    async fn read_managed_seed_complete(
        &self,
        route: &GroupRoute,
        member: u64,
    ) -> Result<Option<Vec<u8>>, CatalogError> {
        let key = self.managed_seed_key(route, &seed_member_key(member, "complete"));
        Ok(self
            .namespace
            .inner
            .read_versioned_bounded(&key, wire::LIMIT)
            .await?
            .map(|(bytes, _)| bytes))
    }

    async fn read_managed_seed_intent(
        &self,
        route: &GroupRoute,
        member: u64,
    ) -> Result<Vec<u8>, CatalogError> {
        let key = self.managed_seed_key(route, &seed_member_key(member, "intent"));
        self.namespace
            .inner
            .read_versioned_bounded(&key, wire::LIMIT)
            .await?
            .map(|(bytes, _)| bytes)
            .ok_or(CatalogError::Corrupt("missing managed seed intent"))
    }

    pub(super) fn managed_seed_key(&self, route: &GroupRoute, leaf: &str) -> Path {
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
        seed_member_key(self.proof.member.get(), leaf)
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

fn seed_member_key(member: u64, leaf: &str) -> String {
    format!("voters/{member:016x}/{leaf}")
}
