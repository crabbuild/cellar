// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Catalog observation resolves group placement. It does not bypass member
//! claims, create per-cell births or grant a destination leader authority.
use super::*;
use crate::raft::catalog::{Catalog, CatalogError};
use cellar_logic::{consensus::ReplicaIdentity, NamespaceMode};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) async fn prepare(
    config: &mut RaftCellsConfig,
    application: &dyn crate::application::Application,
    store: Arc<dyn object_store::ObjectStore>,
    keys: &VersionKeys,
) -> anyhow::Result<Prepared> {
    config.host.validate()?;
    if let Some(partition) = &config.host.automatic_partitions {
        anyhow::ensure!(
            config.shared.archive.is_some() && config.shared.checkpoints.is_some(),
            "automatic partitions require shared archives and checkpoints"
        );
        anyhow::ensure!(
            partition.service.max_groups <= config.host.max_groups
                && partition.recovery.max_operations == 1
                && partition.recovery.max_retained_bytes
                    <= config.shared.provisioning.max_preparation_bytes
                && partition.recovery.max_cells <= config.shared.manifest.max_cells
                && partition.recovery.max_image_bytes <= config.host.images.max_bytes,
            "automatic partition resources exceed this node's installed bounds"
        );
    }
    if config.host.automatic_topology.is_some() {
        anyhow::ensure!(
            config.shared.archive.is_some() && config.shared.checkpoints.is_some(),
            "automatic topology requires shared archives and checkpoints"
        );
    }
    if let Some(managed) = &config.host.managed {
        anyhow::ensure!(
            managed.authorization.node() == config.infrastructure.log.node
                && managed.authorization.disk() == config.infrastructure.log.disk,
            CatalogError::ManagedAdmissionRequired
        );
        // Validate the actual startup declarations, not just their duplicated
        // copy inside managed options, before any namespace or boot mutation.
        anyhow::ensure!(
            config.namespaces.len() == managed.namespaces.len()
                && config.namespaces.iter().all(|n| n.initialize.is_none()
                    && managed
                        .namespaces
                        .iter()
                        .any(|d| d.namespace == n.namespace && d.class == n.class)),
            "managed startup declarations disagree with rooted capabilities"
        );
    } else {
        anyhow::ensure!(
            config
                .namespaces
                .iter()
                .all(|n| n.namespace.managed_fleet.is_none()),
            CatalogError::ManagedAdmissionRequired
        );
    }
    config.infrastructure.transport.validate()?;
    config.shared.entries.validate()?;
    config
        .shared
        .provisioning
        .validate(config.shared.controller.max_groups)?;
    anyhow::ensure!(
        !config.namespaces.is_empty()
            && config.namespaces.len() <= config.shared.namespaces.max_namespaces
            && config.groups.len() <= config.shared.controller.max_groups
            && config.shared.controller.max_groups <= config.infrastructure.log.max_groups
            && config.shared.controller.max_groups == config.host.max_groups
            && config.shared.dispatch.max_input_bytes == config.host.max_input_bytes
            && config.shared.entries.envelope == config.host.execution.envelope
            && config.shared.manifest.max_cells
                <= config.infrastructure.consensus.max_cells_per_group,
        "incoherent shared host capacities"
    );
    anyhow::ensure!(
        config.shared.archive.is_none() || config.shared.checkpoints.is_some(),
        "archive requires checkpoint admission"
    );
    anyhow::ensure!(
        config.shared.controller.checkpoints.is_none() || config.shared.checkpoints.is_some(),
        "checkpoint policy requires physical source admission"
    );
    // Until these consumers are converted they cannot silently start a legacy
    // placement/controller or publish a misleading managed node inventory.
    anyhow::ensure!(
        config.host.controller.is_none() && config.host.inspection_origins.is_empty(),
        "whole-group placement and inspection integration is required"
    );
    let mut definitions = Vec::new();
    let mut seen = BTreeSet::new();
    let mut cron_scopes = BTreeSet::new();
    for registration in &config.namespaces {
        anyhow::ensure!(
            seen.insert((
                registration.namespace.cluster,
                registration.namespace.namespace
            )),
            "duplicate namespace capability"
        );
        anyhow::ensure!(
            registration.namespace.mode == NamespaceMode::Raft,
            "namespace is not Raft"
        );
        anyhow::ensure!(
            !application.supports_cell(&registration.class)
                && !application.supports_workflow(&registration.class),
            crate::application::consensus::RegistrationError::UnsupportedFactory(
                registration.class.clone()
            )
        );
        let class =
            application.resolve_raft_namespace(&registration.namespace, &registration.class)?;
        if let Some(scope) = class.cron_scope() {
            anyhow::ensure!(
                cron_scopes.insert(scope),
                "cron schedule namespace is ambiguous"
            );
        }
        if class.name() == crate::deploy::KV_CLASS {
            let limits = config
                .shared
                .payloads
                .ok_or_else(|| anyhow::anyhow!("KV requires node-owned payload admission"))?;
            super::CellGroupPayloadPool::new(limits)?;
            anyhow::ensure!(
                limits.max_payload_bytes >= crate::application::kv::raft_wire::MAX_BYTES
                    && limits.max_retained_bytes
                        >= crate::application::kv::raft_wire::MIN_RETAINED_BYTES
                    && config.host.max_input_bytes >= 8192
                    && config.host.execution.max_result_bytes >= 8192,
                "KV requires full bounded external-wire and descriptor capacity"
            );
        }
        class.validate_shared_host(&config.host)?;
        anyhow::ensure!(
            class.schema_digest() == registration.namespace.schema_digest,
            "compiled namespace schema mismatch"
        );
        anyhow::ensure!(
            registration.namespace.image_storage
                != super::super::catalog::RaftImageStorage::Extents
                || config.host.extents.is_some(),
            "extent namespace requires node storage"
        );
        anyhow::ensure!(
            registration.namespace.minimum_durability != cellar_logic::consensus::Durability::S3
                || config.shared.archive.is_some(),
            "S3 minimum requires an archive owner"
        );
        definitions.push(class);
    }
    let mut peers = BTreeMap::new();
    for peer in &config.peers {
        anyhow::ensure!(
            peers.insert(peer.node, peer).is_none(),
            "duplicate physical node endpoint"
        );
    }
    let catalog = Catalog::new(store, config.catalog.clone())?;
    let mut namespaces = Vec::new();
    for (registration, class) in config.namespaces.drain(..).zip(definitions) {
        let namespace = if registration.initialize.is_some() {
            catalog.open_namespace(registration.namespace).await?
        } else {
            catalog.existing_namespace(registration.namespace).await?
        };
        namespace
            .require_version_keys(keys, u64::try_from(crate::asyncrt::wall_ms())?)
            .await?;
        let directory = RangeDirectory::new(namespace, config.shared.directory)?;
        let owner = directory.namespace().cell_group_owner().await?;
        if let Some(managed) = &config.host.managed {
            let bound = managed
                .authorization
                .stop_catalog()
                .bind_namespace(directory.namespace_config().clone())?;
            anyhow::ensure!(
                owner == bound.managed_fleet,
                CatalogError::ManagedAdmissionRequired
            );
        } else {
            anyhow::ensure!(owner.is_none(), CatalogError::ManagedAdmissionRequired);
        }
        if let Some(initialization) = registration.initialize {
            provisioning::initialize(
                &directory,
                initialization,
                config.shared.directory.max_groups,
            )
            .await?;
        }
        // A declared managed standby may precede the first range publication.
        // It attaches no group and invents no empty root or serving authority.
        // Every explicit placement below still requires a selected full birth.
        if config.host.managed.is_none() {
            directory.observe().await?;
        }
        namespaces.push(Namespace { directory, class });
    }
    if let Some(managed) = &config.host.managed {
        let declarations = namespaces
            .iter()
            .map(|n| crate::raft::host::NamespaceRegistration {
                namespace: n.directory.namespace().clone(),
                class: n.class.clone(),
            })
            .collect::<Vec<_>>();
        let timeout = config
            .host
            .node_directory
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("managed directory required"))?
            .timeout_ms;
        managed
            .authorization
            .check_declared_namespaces(&declarations, std::time::Duration::from_millis(timeout))
            .await?;
    }
    let mut groups = Vec::new();
    let mut seen = BTreeSet::new();
    for assignment in &config.groups {
        anyhow::ensure!(
            seen.insert(assignment.history),
            "duplicate physical group placement"
        );
        let mut group = select_assignment(&namespaces, *assignment).await?;
        let mut members = Vec::new();
        for member in group.birth.voters().iter().chain(group.birth.learners()) {
            let endpoint = match peers.get(&member.node) {
                Some(peer) => peer,
                None if config.host.managed.is_some()
                    || config.infrastructure.log.mode
                        == crate::raft::options::LogOpenMode::Resume =>
                {
                    continue
                }
                None => anyhow::bail!("missing trusted group peer endpoint"),
            };
            members.push(super::super::transport::RaftPeer {
                replica: ReplicaIdentity {
                    scope: assignment.history,
                    member: member.member,
                    node: endpoint.node,
                    disk: endpoint.disk,
                    process: endpoint.process,
                },
                label: endpoint.label.clone(),
                server_name: endpoint.server_name.clone(),
                control_address: endpoint.control_address,
                data_address: endpoint.data_address,
                query_address: endpoint.query_address,
            });
        }
        group.transport.peers = members;
        groups.push(group);
    }
    Ok(Prepared {
        namespaces,
        peers: std::mem::take(&mut config.peers),
        groups,
        query_origins: std::mem::take(&mut config.query_origins),
        command_origins: std::mem::take(&mut config.command_origins),
        isolated_peers: None,
    })
}

async fn select_assignment(
    namespaces: &[Namespace],
    assignment: RaftGroupAssignment,
) -> anyhow::Result<Assignment> {
    let namespace = namespaces
        .iter()
        .position(|n| {
            let config = n.directory.namespace_config();
            config.cluster == assignment.history.cluster
                && config.namespace == assignment.history.namespace
        })
        .ok_or_else(|| anyhow::anyhow!("group is outside namespace capabilities"))?;
    let birth = namespaces[namespace]
        .directory
        .materialize_selected_history(assignment.history)
        .await?;
    Ok(assignment_from_birth(
        namespace,
        assignment.placement,
        birth,
    ))
}

fn assignment_from_birth(
    namespace: usize,
    placement: RaftGroupPlacement,
    birth: CellGroupBirth,
) -> Assignment {
    Assignment {
        namespace,
        placement,
        pending_transfer_archive: false,
        sealed_transfer_source: false,
        transport: super::super::transport::GroupRoute {
            scope: birth.route().history,
            schema_digest: birth.route().schema_digest,
            revision: birth.route().epoch.get(),
            peers: Vec::new(),
        },
        birth,
    }
}

/// Recover local placement from the opened engine and selected namespace
/// histories. Boot, permanent claims and membership are checked at attachment.
#[derive(Default)]
pub(super) struct RetainedPlacements {
    pub(super) parked: Vec<(usize, crate::raft::cache::GroupLog<GroupHistory>)>,
    pub(super) retired_sources: Vec<GroupHistory>,
    pub(super) removed_members: Vec<GroupHistory>,
}

pub(super) async fn discover_retained_placements(
    prepared: &mut Prepared,
    log: &crate::raft::storage::ReplicaLog,
    fleet: Option<&crate::raft::fleet_catalog::FleetCatalog>,
    limits: CellGroupNodeLimits,
) -> anyhow::Result<RetainedPlacements> {
    let configured: BTreeMap<_, _> = prepared
        .groups
        .iter()
        .map(|assignment| (assignment.birth.route().history, assignment.placement))
        .collect();
    let mut placements = RetainedPlacements::default();
    for retained in log.cell_groups()? {
        let history = retained.identity().scope;
        match configured.get(&history) {
            Some(RaftGroupPlacement::Replica) => continue,
            Some(RaftGroupPlacement::QueryRouter) => {
                anyhow::bail!("retained replica conflicts with router placement");
            }
            None => (),
        }
        anyhow::ensure!(
            prepared
                .groups
                .len()
                .saturating_add(placements.parked.len())
                < limits.controller.max_groups,
            "retained group capacity exceeded"
        );
        let namespace = || {
            prepared
                .namespaces
                .iter()
                .position(|namespace| {
                    let config = namespace.directory.namespace_config();
                    config.cluster == history.cluster && config.namespace == history.namespace
                })
                .ok_or(CatalogError::ManagedAdmissionRequired)
        };
        let Some(fleet) = fleet else {
            // Explicit unmanaged assignments control serving. An omitted
            // replica in a declared namespace still owns its durable images.
            placements.parked.push((namespace()?, retained));
            continue;
        };
        let marker = retained.group_manifest()?;
        if let Some(marker) = marker
            .as_ref()
            .filter(|marker| marker.recovery_request().is_some())
        {
            let namespace = namespace()?;
            let directory = &prepared.namespaces[namespace].directory;
            let timeout = std::time::Duration::from_secs(5);
            let snapshot = fleet.read(timeout).await?;
            let birth = fleet.history(&snapshot, &history, timeout).await?;
            if Box::pin(directory.park_retained_managed_recovery(
                &retained,
                marker,
                &birth,
                log.storage_instance()?,
            ))
            .await?
            {
                placements.parked.push((namespace, retained));
                continue;
            }
        }
        if let Some(marker) = marker.as_ref().filter(|marker| {
            marker.transfer_origin().is_some() && marker.transfer_activation().is_none()
        }) {
            let namespace = namespace()?;
            let directory = &prepared.namespaces[namespace].directory;
            let origin = marker.transfer_origin().ok_or(CatalogError::Fenced)?;
            let operation = cellar_logic::consensus::ProposalId::new(origin.operation())?;
            let transition = fleet
                .reopen_managed_transition(directory, operation, std::time::Duration::from_secs(5))
                .await?
                .ok_or(CatalogError::Fenced)?;
            directory
                .require_parked_managed_transfer(
                    &transition,
                    &retained,
                    marker,
                    log.storage_instance()?,
                )
                .await?;
            placements.parked.push((namespace, retained));
            continue;
        }
        if let Some(marker) = marker
            .as_ref()
            .filter(|marker| marker.source_seal().is_some())
        {
            let namespace = namespace()?;
            let directory = &prepared.namespaces[namespace].directory;
            if let Some(seal) = marker
                .source_seal()
                .filter(|seal| seal.retirement().is_some())
            {
                let operation = cellar_logic::consensus::ProposalId::new(seal.transition())?;
                let transition = fleet
                    .reopen_managed_transition(
                        directory,
                        operation,
                        std::time::Duration::from_secs(5),
                    )
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                if transition
                    .journal
                    .as_ref()
                    .is_some_and(|journal| journal.retired_parents().is_some())
                {
                    directory
                        .park_managed_retired_source(&transition.plan, &retained, marker)
                        .await?;
                    placements.retired_sources.push(history);
                    placements.parked.push((namespace, retained));
                    continue;
                }
            }
            if let Some((birth, _)) = directory.discover_managed_sealed_source(marker).await? {
                let mut assignment =
                    assignment_from_birth(namespace, RaftGroupPlacement::Replica, birth);
                assignment.sealed_transfer_source = true;
                prepared.groups.push(assignment);
                continue;
            }
        }
        if fleet
            .parked_removed_member(log, &retained, std::time::Duration::from_secs(5))
            .await?
        {
            placements.removed_members.push(history);
            placements.parked.push((namespace()?, retained));
            continue;
        }
        let group = select_assignment(
            &prepared.namespaces,
            RaftGroupAssignment {
                history,
                placement: RaftGroupPlacement::Replica,
            },
        )
        .await?;
        prepared.groups.push(group);
    }
    Ok(placements)
}

/// Resolve the complete physical placement after managed discovery has accepted
/// current process views, before genesis, attachment or route publication.
pub(super) fn refresh_routes(
    prepared: &mut Prepared,
    log: &crate::raft::storage::ReplicaLog,
    managed: bool,
) -> anyhow::Result<()> {
    for assignment in &mut prepared.groups {
        let managed_router = managed && assignment.placement == RaftGroupPlacement::QueryRouter;
        let retained = if assignment.placement == RaftGroupPlacement::Replica {
            match log.cell_group(&assignment.birth.route().history) {
                Ok(group) => Some(group),
                Err(crate::raft::storage::LogError::NotFound) => None,
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };
        let members = if let Some(group) = &retained {
            let cache = group.read()?;
            let members = crate::raft::member_state::members(&cache.conf_state);
            anyhow::ensure!(
                members.contains(&group.identity().member.get()),
                CatalogError::Fenced
            );
            members
                .into_iter()
                .map(|id| {
                    let member = cellar_logic::consensus::MemberId::new(id)?;
                    let node = if let Some((binding, _)) = cache.membership.joined(member)? {
                        binding.node
                    } else {
                        assignment
                            .birth
                            .voters()
                            .iter()
                            .chain(assignment.birth.learners())
                            .find(|m| m.member == member)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "retained membership has no birth or joined binding"
                                )
                            })?
                            .node
                    };
                    Ok((member, node))
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        } else {
            let learners = if managed_router {
                &[]
            } else {
                assignment.birth.learners()
            };
            assignment
                .birth
                .voters()
                .iter()
                .chain(learners)
                .map(|m| (m.member, m.node))
                .collect()
        };
        if managed_router
            && members
                .iter()
                .any(|(_, node)| !prepared.peers.iter().any(|peer| peer.node == *node))
        {
            // An offline router may return after every birth voter retired.
            // Its directory must first observe the current quorum.
            assignment.transport.peers.clear();
            continue;
        }
        assignment.transport.peers = members
            .into_iter()
            .filter(|(_, node)| {
                // Keep the full durable ConfState. Only a retained managed
                // member may omit a route, and only with a verified permanent
                // isolation receipt from the startup trust cut. Missing or
                // unreachable peers alone never authorize this exception.
                !(managed
                    && retained.is_some()
                    && prepared
                        .isolated_peers
                        .as_ref()
                        .is_some_and(|peers| peers.contains(*node)))
            })
            .map(|(member, node)| {
                let endpoint = prepared
                    .peers
                    .iter()
                    .find(|peer| peer.node == node)
                    .ok_or_else(|| anyhow::anyhow!("missing trusted group peer endpoint"))?;
                Ok(super::super::transport::RaftPeer {
                    replica: ReplicaIdentity {
                        scope: assignment.birth.route().history,
                        member,
                        node: endpoint.node,
                        disk: endpoint.disk,
                        process: endpoint.process,
                    },
                    label: endpoint.label.clone(),
                    server_name: endpoint.server_name.clone(),
                    control_address: endpoint.control_address,
                    data_address: endpoint.data_address,
                    query_address: endpoint.query_address,
                })
            })
            .collect::<anyhow::Result<_>>()?;
        if let Some(group) = retained {
            let membership = group.membership()?;
            anyhow::ensure!(
                assignment
                    .transport
                    .peers
                    .iter()
                    .all(|p| membership.permits_group(&p.replica)),
                CatalogError::Fenced
            );
        }
    }
    Ok(())
}
