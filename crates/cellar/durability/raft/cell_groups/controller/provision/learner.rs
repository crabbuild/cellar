// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed learners keep one physical preparation through grant, install and
//! attachment. Neither a reservation nor a staged mapping can create genesis.
use super::*;
use crate::application::consensus::client::LearnerActivation;
use crate::raft::{
    cache::GroupLog,
    cell_groups::CellGroupBirth,
    fleet_catalog::{FleetHostAuthorization, FleetPermitAuthorization},
    member_state::{members, FleetPermitPhase},
    storage::LogError,
    transport::{CellGroupSeedError, CellGroupSeedSession, GroupRoute as PhysicalRoute, RaftPeer},
};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{MemberId, NodeId, ReplicaIdentity},
};

#[derive(Clone)]
pub(super) struct Admission {
    authorization: FleetPermitAuthorization,
    pub member: MemberId,
    source: MemberId,
    source_node: NodeId,
    from_s3: bool,
}

impl Admission {
    pub fn new(
        route: &GroupRoute,
        host: &FleetHostAuthorization,
        authorization: FleetPermitAuthorization,
        activation: LearnerActivation,
    ) -> Result<Self, CellGroupControllerError> {
        let replacement = authorization
            .permit()
            .replacement()
            .map_err(|e| catalog_error(CatalogError::Log(e)))?;
        let boot = host
            .boot_binding()
            .map_err(CatalogError::from)
            .map_err(catalog_error)?;
        if !authorization.matches_scope(&route.history)
            || authorization.permit().binding().fleet() != host.fleet()
            || authorization.target_boot() != Some(&boot)
            || replacement.member != activation.member
            || replacement.node != host.node()
            || replacement.disk != host.disk()
            || activation.member == activation.source
            || activation.source_node == host.node()
        {
            return Err(CellGroupControllerError::Invalid(
                "managed learner admission",
            ));
        }
        Ok(Self {
            authorization,
            member: activation.member,
            source: activation.source,
            source_node: activation.source_node,
            from_s3: activation.from_s3,
        })
    }

    pub(super) fn check_installed(
        &self,
        group: &GroupLog<GroupHistory>,
    ) -> Result<(), CellGroupControllerError> {
        let cache = group.read().map_err(RuntimeError::from)?;
        let status = cache
            .membership
            .fleet_status()
            .ok_or(CellGroupControllerError::Invalid(
                "missing learner fleet guard",
            ))?;
        let permit = self.authorization.permit();
        if group.identity().member != self.member
            || status.permit.as_ref() != Some(permit)
            || &status.binding != permit.binding()
            || status.consumed_sequence != permit.sequence()
            || !matches!(
                status.phase,
                Some(
                    FleetPermitPhase::Seeding
                        | FleetPermitPhase::Joint
                        | FleetPermitPhase::Complete
                )
            )
        {
            return Err(CellGroupControllerError::Invalid(
                "installed learner permit",
            ));
        }
        Ok(())
    }
}

pub(super) fn same_admission(a: Option<&Admission>, b: Option<&Admission>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            // A source endpoint is a fresh-attempt hint, not destination
            // ownership. Leader failure must not strand the exact retained
            // boot/member/permit. A running attempt keeps its original hint;
            // the next attempt reauthorizes its source quorum independently.
            a.authorization.permit() == b.authorization.permit()
                && a.authorization.target_boot() == b.authorization.target_boot()
                && a.member == b.member
                && a.from_s3 == b.from_s3
        }
        _ => false,
    }
}

pub(super) struct Staged {
    pub prepared: prepare::Prepared,
    session: CellGroupSeedSession,
    birth: CellGroupBirth,
    config: Arc<Config>,
    transport: RaftTransport,
    admission: Admission,
    _parallel: tokio::sync::OwnedSemaphorePermit,
}

impl Staged {
    pub async fn install(self) -> Result<CellGroupRegistration, CellGroupControllerError> {
        let (permit, current) = self
            .session
            .install(self.prepared.applier.clone())
            .await
            .map_err(seed_error)?;
        self.admission.check_installed(permit.group())?;
        let route = physical_route(
            &self.birth,
            permit.group(),
            &self.config,
            &self.transport,
            self.admission.authorization.minimum_trust_revision(),
        )?;
        Ok(CellGroupRegistration {
            permit,
            current,
            applier: self.prepared.applier,
            transport: route,
        })
    }
}

pub(super) async fn recover(
    directory: &RangeDirectory,
    birth: &CellGroupBirth,
    config: &Arc<Config>,
    runtime: &CellGroupRuntime,
    transport: &RaftTransport,
    host: &FleetHostAuthorization,
    admission: &Admission,
    deadline: u64,
    resources: prepare::Resources,
    parallel: tokio::sync::OwnedSemaphorePermit,
) -> Result<prepare::Recovery, CellGroupControllerError> {
    let prepare::Resources { existing, archive } = resources;
    let log = runtime.log();
    let installed = match log.cell_group(&birth.route().history) {
        Ok(group) => group
            .read()
            .map_err(RuntimeError::from)?
            .group_seed
            .is_some(),
        Err(LogError::NotFound) => false,
        Err(error) => return Err(RuntimeError::from(error).into()),
    };
    let images = config.images(directory)?;
    if installed {
        // A lost install response cannot authorize downloading/reinitializing a
        // different cut. The durable receipt and permanent claim select restart.
        let permit = directory
            .bootstrap_managed_joined_group_member(birth, admission.member, log, host)
            .await
            .map_err(catalog_error)?;
        admission.check_installed(permit.group())?;
        let prepared =
            prepare::recover_pages(config, runtime, permit.group().clone(), images, existing)
                .await?;
        let route = physical_route(
            birth,
            permit.group(),
            config,
            transport,
            admission.authorization.minimum_trust_revision(),
        )?;
        return Ok(prepare::Recovery::Ready(CellGroupRegistration {
            permit,
            current: prepared.current,
            applier: prepared.applier,
            transport: route,
        }));
    }
    let peer = transport
        .managed_group_peer(
            &birth.route().history,
            admission.source_node,
            admission.authorization.minimum_trust_revision(),
        )
        .map_err(CellGroupControllerError::Transport)?;
    let local = log.limits();
    let target = ReplicaIdentity {
        scope: birth.route().history,
        member: admission.member,
        node: local.node,
        disk: local.disk,
        process: local.process,
    };
    let source = RaftPeer {
        replica: ReplicaIdentity {
            scope: target.scope,
            member: admission.source,
            node: peer.node,
            disk: peer.disk,
            process: peer.process,
        },
        label: peer.label,
        server_name: peer.server_name,
        control_address: peer.control_address,
        data_address: peer.data_address,
        query_address: peer.query_address,
    };
    let session = if admission.from_s3 {
        let archive = archive.ok_or(CellGroupControllerError::Invalid(
            "S3 learner resources unavailable",
        ))?;
        transport
            .prepare_managed_group_archive_seed(
                target,
                source,
                directory.clone(),
                birth.clone(),
                deadline,
                crate::raft::transport::ManagedArchiveSeed {
                    host: host.clone(),
                    authorization: admission.authorization.clone(),
                    archive,
                },
            )
            .await
    } else {
        transport
            .prepare_managed_group_seed(
                target,
                source,
                directory.clone(),
                birth.clone(),
                deadline,
                host.clone(),
                admission.authorization.clone(),
            )
            .await
    }
    .map_err(seed_error)?;
    let prepared =
        prepare::recover_pages(config, runtime, session.group().clone(), images, existing).await?;
    Ok(prepare::Recovery::Seed(Staged {
        prepared,
        session,
        birth: birth.clone(),
        config: config.clone(),
        transport: transport.clone(),
        admission: admission.clone(),
        _parallel: parallel,
    }))
}

fn physical_route(
    birth: &CellGroupBirth,
    group: &GroupLog<GroupHistory>,
    config: &Config,
    transport: &RaftTransport,
    minimum_revision: u64,
) -> Result<PhysicalRoute<GroupHistory>, CellGroupControllerError> {
    let bindings = {
        let cache = group.read().map_err(RuntimeError::from)?;
        members(&cache.conf_state)
            .into_iter()
            .map(|id| {
                let member = MemberId::new(id)
                    .map_err(|_| CellGroupControllerError::Invalid("member id"))?;
                let binding = cache
                    .membership
                    .joined(member)
                    .map_err(RuntimeError::from)?
                    .map(|(b, _)| (b.node, b.disk));
                let node = binding
                    .map(|b| b.0)
                    .or_else(|| {
                        birth
                            .voters()
                            .iter()
                            .chain(birth.learners())
                            .find(|b| b.member == member)
                            .map(|b| b.node)
                    })
                    .ok_or(CellGroupControllerError::Invalid("member node binding"))?;
                Ok((member, node, binding.map(|b| b.1)))
            })
            .collect::<Result<Vec<_>, CellGroupControllerError>>()?
    };
    // Resolve after the group read guard drops. Each endpoint is independently
    // checked against rooted trust again by physical registration.
    let mut peers = Vec::new();
    for (member, node, disk) in bindings {
        let peer =
            match transport.managed_group_peer(&birth.route().history, node, minimum_revision) {
                Ok(peer) => peer,
                Err(TransportError::StalePeer | TransportError::Refused)
                    if node != group.identity().node =>
                {
                    continue
                }
                Err(error) => return Err(CellGroupControllerError::Transport(error)),
            };
        if disk.is_some_and(|disk| disk != peer.disk) {
            return Err(CellGroupControllerError::Invalid(
                "joined member disk changed",
            ));
        }
        peers.push(RaftPeer {
            replica: ReplicaIdentity {
                scope: birth.route().history,
                member,
                node,
                disk: peer.disk,
                process: peer.process,
            },
            label: peer.label,
            server_name: peer.server_name,
            control_address: peer.control_address,
            data_address: peer.data_address,
            query_address: peer.query_address,
        });
    }
    Ok(PhysicalRoute {
        scope: birth.route().history,
        schema_digest: birth.route().schema_digest,
        revision: config.settings.peer_revision,
        peers,
    })
}

fn seed_error(error: CellGroupSeedError) -> CellGroupControllerError {
    match error {
        CellGroupSeedError::Transport(e) => CellGroupControllerError::Transport(e),
        CellGroupSeedError::Catalog(e) => catalog_error(e),
        CellGroupSeedError::Log(e) => RuntimeError::from(e).into(),
        CellGroupSeedError::Checkpoint(e) => CellGroupControllerError::Checkpoint(Arc::new(e)),
        CellGroupSeedError::Archive(e) => {
            CellGroupControllerError::Archive(Arc::new(CellGroupArchiveControlError::Archive(*e)))
        }
    }
}
