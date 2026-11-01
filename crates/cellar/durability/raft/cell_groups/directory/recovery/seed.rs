// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A permanent recovery origin and stable seed receipts precede topology
//! publication. Losing any response never authorizes a new disk or empty root.
use super::*;
use crate::raft::{
    cell_groups::{
        CellGroupApplier, GroupManifest, PreparedCellGroupRecovery, RecoverySeed,
        SeededCellGroupRecovery,
    },
    storage::{CellGroupBootstrap, LogError, ReplicaLog},
};
use cellar_logic::consensus::{EngineInstanceId, ReplicaIdentity};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    pub(super) request: Vec<u8>,
    pub(super) genesis: GroupManifest,
    pub(super) commitment: [u8; 32],
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct MemberSeed {
    binding: [u8; 32],
    member: u64,
    node: [u8; 16],
    disk: [u8; 16],
    engine: [u8; 16],
}

impl MemberSeed {
    pub(super) fn disk(&self) -> Result<cellar_logic::consensus::DiskLineage, CatalogError> {
        cellar_logic::consensus::DiskLineage::new(self.disk)
            .map_err(|_| CatalogError::Corrupt("recovery seed disk"))
    }

    pub(super) fn engine(&self) -> Result<EngineInstanceId, CatalogError> {
        EngineInstanceId::new(self.engine)
            .map_err(|_| CatalogError::Corrupt("recovery seed engine"))
    }

    pub(super) fn check(
        &self,
        selected: &crate::raft::catalog::InitialMember,
        binding: [u8; 32],
    ) -> Result<(), CatalogError> {
        if self.binding != binding
            || self.member != selected.member.get()
            || self.node != selected.node.bytes()
        {
            return Err(CatalogError::Corrupt("recovery seed identity"));
        }
        self.disk()?;
        self.engine()?;
        Ok(())
    }

    pub(super) fn check_log(&self, log: &ReplicaLog) -> Result<(), CatalogError> {
        if self.node != log.limits().node.bytes()
            || self.disk()? != log.limits().disk
            || self.engine()? != log.storage_instance().map_err(CatalogError::Log)?
        {
            return Err(CatalogError::LostStableState);
        }
        Ok(())
    }
}

/// Process-local engine grant, minted only after the durable fence/origin and
/// exact seed intent are confirmed. It is not a startup or serving permission.
pub(crate) struct RecoverySeedPermit {
    owner: Arc<()>,
    local: ReplicaIdentity<GroupHistory>,
    engine: EngineInstanceId,
    request: Arc<CellGroupRecoveryRequest>,
    bootstrap: CellGroupBootstrap,
    genesis: GroupManifest,
    commitment: [u8; 32],
}

impl RecoverySeedPermit {
    pub(crate) fn owner(&self) -> &Arc<()> {
        &self.owner
    }

    pub(crate) fn request(&self) -> &CellGroupRecoveryRequest {
        &self.request
    }

    pub(crate) fn bootstrap(&self) -> &CellGroupBootstrap {
        &self.bootstrap
    }

    pub(crate) fn genesis(&self) -> &GroupManifest {
        &self.genesis
    }

    pub(crate) fn commitment(&self) -> [u8; 32] {
        self.commitment
    }

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
    pub(in crate::raft::catalog::range_directory::recovery) fn recovery_binding_key(
        &self,
        route: &GroupRoute,
    ) -> Path {
        self.key(&format!(
            "recovery-bindings/{}/{}",
            hex::encode(route.history.group.bytes()),
            hex::encode(route.history.incarnation.bytes())
        ))
    }

    pub(super) fn recovery_seed_key(
        &self,
        request: &CellGroupRecoveryRequest,
        member: u64,
        leaf: &str,
    ) -> Path {
        self.recovery_key(request, &format!("seeds/{member:016x}/{leaf}"))
    }

    async fn require_locked_recovery(
        &self,
        request: &CellGroupRecoveryRequest,
    ) -> Result<(), CatalogError> {
        let (view, _) = self.current().await?;
        self.check_recovery_view(&view, request)?;
        if view.head.recovery.is_none() {
            return Err(CatalogError::Fenced);
        }
        let (_, origin) = self
            .birth_reservation(request.replacement().route())
            .await?;
        if origin != Some(request.digest()) {
            return Err(CatalogError::Corrupt("recovery birth origin"));
        }
        self.recovery_source(request).await?;
        Ok(())
    }

    pub(super) async fn verify_recovery_fence(
        &self,
        request: &CellGroupRecoveryRequest,
        authority: &dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>,
    ) -> Result<(), CatalogError> {
        let request = self.decode_group_recovery(request.encode())?;
        let fence = CellGroupDisasterFenceRequest::new(request);
        let bytes = self
            .namespace
            .inner
            .read_versioned_bounded(&self.recovery_key(fence.recovery(), "fence"), wire::LIMIT)
            .await?
            .ok_or(CatalogError::Corrupt("missing recovery fence"))?
            .0;
        let complete: Complete = record::decode(wire::GROUP_FENCE, &bytes, wire::LIMIT)?;
        if complete.request != fence.digest() {
            return Err(CatalogError::Corrupt("recovery fence identity"));
        }
        check_evidence(&complete.evidence)?;
        authority
            .verify(&fence, &complete.evidence)
            .await
            .map_err(|e| CatalogError::Fence(e.to_string()))
    }

    /// Sync the imported complete root as a fresh Raft genesis and confirm its
    /// permanent physical seed receipt. The exact preparation can be cloned for
    /// retry after a lost reply. The replacement remains closed to startup.
    pub async fn seed_group_recovery(
        &self,
        domain: &impl CellGroupArchiveExecution,
        prepared: PreparedCellGroupRecovery,
        log: &ReplicaLog,
        authority: &dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>,
    ) -> Result<SeededCellGroupRecovery, CellGroupRecoveryError> {
        domain
            .run(async {
                let request = Arc::new(self.decode_group_recovery(prepared.request().encode())?);
                self.bounded(self.require_locked_recovery(&request)).await?;
                self.bounded(self.verify_recovery_fence(&request, authority))
                    .await?;
                if !prepared.manifest().marker().is_recovery_genesis()
                    || prepared.manifest().marker().route() != request.replacement().route()
                    || prepared.manifest().marker().recovery_request() != Some(request.digest())
                    || prepared
                        .manifest()
                        .marker()
                        .recovery_commitment()
                        .map_err(|_| CatalogError::Corrupt("seed commitment"))?
                        != Some(prepared.commitment())
                {
                    return Err(CatalogError::Conflict("recovery preparation identity").into());
                }
                let target = request.replacement();
                let member = target
                    .voters()
                    .iter()
                    .chain(target.learners())
                    .find(|member| member.node == log.limits().node)
                    .ok_or(CatalogError::NotInitialMember)?;
                if prepared
                    .source()
                    .coverage()
                    .recovery()
                    .membership()
                    .has_disk_binding(log.limits().disk)
                {
                    return Err(CatalogError::Conflict("reused archived recovery disk").into());
                }
                let nodes = request
                    .source()
                    .voters()
                    .iter()
                    .chain(request.source().learners())
                    .map(|member| member.node)
                    .collect::<Vec<_>>();
                self.bounded(self.namespace.verify_recovery_storage_fresh(log, &nodes))
                    .await?;
                let _node = self.namespace.require_node_storage(log).await?;
                let binding_bytes = record::encode(
                    wire::GROUP_RECOVERY_BINDING,
                    &Binding {
                        request: request.encode().to_vec(),
                        genesis: prepared.manifest().marker().clone(),
                        commitment: prepared.commitment(),
                    },
                    wire::LIMIT,
                )?;
                let binding = record::digest(&binding_bytes);
                self.bounded(async {
                    let observed = self
                        .namespace
                        .inner
                        .create_and_observe_bounded(
                            &self.recovery_binding_key(target.route()),
                            &binding_bytes,
                            wire::LIMIT,
                        )
                        .await?;
                    if observed != binding_bytes {
                        return Err(CatalogError::Conflict("recovery genesis binding"));
                    }
                    Ok(())
                })
                .await?;
                let member_seed = MemberSeed {
                    binding,
                    member: member.member.get(),
                    node: member.node.bytes(),
                    disk: log.limits().disk.bytes(),
                    engine: log.storage_instance().map_err(CatalogError::Log)?.bytes(),
                };
                let intent =
                    record::encode(wire::GROUP_RECOVERY_SEED_INTENT, &member_seed, wire::LIMIT)?;
                let complete = record::encode(
                    wire::GROUP_RECOVERY_SEED_COMPLETE,
                    &member_seed,
                    wire::LIMIT,
                )?;
                self.bounded(async {
                    let existing = self
                        .namespace
                        .inner
                        .read_versioned_bounded(
                            &self.recovery_seed_key(&request, member_seed.member, "complete"),
                            wire::LIMIT,
                        )
                        .await?;
                    if let Some((bytes, _)) = existing {
                        if bytes != complete {
                            return Err(CatalogError::Conflict("recovery seed receipt"));
                        }
                        log.cell_group(&target.route().history)
                            .map_err(|error| match error {
                                LogError::NotFound => CatalogError::LostStableState,
                                error => CatalogError::Log(error),
                            })?;
                    }
                    let observed = self
                        .namespace
                        .inner
                        .create_and_observe_bounded(
                            &self.recovery_seed_key(&request, member_seed.member, "intent"),
                            &intent,
                            wire::LIMIT,
                        )
                        .await?;
                    if observed != intent {
                        return Err(CatalogError::Conflict("recovery seed identity"));
                    }
                    Ok(())
                })
                .await?;
                let local = ReplicaIdentity {
                    scope: target.route().history,
                    member: member.member,
                    node: member.node,
                    disk: log.limits().disk,
                    process: log.limits().process,
                };
                let permit = RecoverySeedPermit {
                    owner: log.owner(),
                    local,
                    engine: log.storage_instance().map_err(CatalogError::Log)?,
                    request: request.clone(),
                    bootstrap: CellGroupBootstrap {
                        route: target.route().clone(),
                        member: member.member,
                        voters: target.voters().iter().map(|member| member.member).collect(),
                        learners: target
                            .learners()
                            .iter()
                            .map(|member| member.member)
                            .collect(),
                    },
                    genesis: prepared.manifest().marker().clone(),
                    commitment: prepared.commitment(),
                };
                let seed =
                    RecoverySeed::new(permit, prepared.clone()).map_err(CatalogError::Log)?;
                let group = log
                    .install_recovery_seed(seed)
                    .await
                    .map_err(CatalogError::Log)?;
                let store = prepared.store().clone();
                let genesis = prepared.manifest().marker().clone();
                let selected = group.clone();
                let retained = prepared.clone();
                let (applier, root) = domain
                    .blocking(move || {
                        let _retained = retained;
                        CellGroupApplier::recover_seeded(selected, &store, &genesis)
                    })
                    .map_err(|_| CellGroupArchiveError::Stopped)?
                    .await
                    .map_err(CellGroupArchiveError::Worker)?
                    .map_err(CellGroupArchiveError::Replay)?;
                self.bounded(self.require_locked_recovery(&request)).await?;
                self.bounded(self.verify_recovery_fence(&request, authority))
                    .await?;
                self.bounded(async {
                    let observed = self
                        .namespace
                        .inner
                        .create_and_observe_bounded(
                            &self.recovery_seed_key(&request, member_seed.member, "complete"),
                            &complete,
                            wire::LIMIT,
                        )
                        .await?;
                    if observed != complete {
                        return Err(CatalogError::Conflict("recovery seed receipt"));
                    }
                    self.require_locked_recovery(&request).await?;
                    Ok(())
                })
                .await?;
                Ok(SeededCellGroupRecovery::new(
                    request, group, applier, root, binding,
                ))
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}
