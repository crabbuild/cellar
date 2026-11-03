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
    // Managed authority must be converted as a whole-group protocol. Never
    // bootstrap an old managed namespace through the unmanaged permit path.
    anyhow::ensure!(
        config.host.managed.is_none()
            && config
                .namespaces
                .iter()
                .all(|n| n.namespace.managed_fleet.is_none()),
        CatalogError::ManagedAdmissionRequired
    );
    config.host.validate()?;
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
        config.host.controller.is_none()
            && config.host.node_directory.is_none()
            && config.host.inspection_origins.is_empty(),
        "whole-group placement and node directory integration is required"
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
        if let Some(initialization) = registration.initialize {
            provisioning::initialize(
                &directory,
                initialization,
                config.shared.directory.max_groups,
            )
            .await?;
        }
        directory.observe().await?;
        namespaces.push(Namespace { directory, class });
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
            let endpoint = peers
                .get(&member.node)
                .ok_or_else(|| anyhow::anyhow!("missing trusted group peer endpoint"))?;
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
