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
        let class = application.resolve_raft_class(&registration.class)?;
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
        let namespace = namespaces
            .iter()
            .position(|n| {
                let c = n.directory.namespace_config();
                c.cluster == assignment.history.cluster
                    && c.namespace == assignment.history.namespace
            })
            .ok_or_else(|| anyhow::anyhow!("group is outside namespace capabilities"))?;
        let birth = namespaces[namespace]
            .directory
            .materialize_selected_history(assignment.history)
            .await?;
        let mut members = Vec::new();
        for member in birth.voters().iter().chain(birth.learners()) {
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
        groups.push(Assignment {
            namespace,
            placement: assignment.placement,
            transport: super::super::transport::GroupRoute {
                scope: assignment.history,
                schema_digest: birth.route().schema_digest,
                revision: birth.route().epoch.get(),
                peers: members,
            },
            birth,
        });
    }
    Ok(Prepared {
        namespaces,
        peers: std::mem::take(&mut config.peers),
        groups,
        query_origins: std::mem::take(&mut config.query_origins),
        command_origins: std::mem::take(&mut config.command_origins),
    })
}

/// Resolve the complete physical placement after managed discovery has accepted
/// current process views, before genesis, attachment or route publication.
pub(super) fn refresh_routes(
    prepared: &mut Prepared,
    log: &crate::raft::storage::ReplicaLog,
) -> anyhow::Result<()> {
    for assignment in &mut prepared.groups {
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
            assignment
                .birth
                .voters()
                .iter()
                .chain(assignment.birth.learners())
                .map(|m| (m.member, m.node))
                .collect()
        };
        assignment.transport.peers = members
            .into_iter()
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
