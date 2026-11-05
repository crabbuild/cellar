// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::{ManifestPageStore, PreparedGroupManifest};
use crate::raft::transport::{GroupRoute as PhysicalRoute, RaftPeer};
use cellar_logic::consensus::ReplicaIdentity;

#[derive(Clone)]
pub(super) struct Prepared {
    pub applier: CellGroupApplier,
    pub current: PreparedGroupManifest,
}

pub(super) struct Resources {
    pub existing: Option<Prepared>,
    pub archive: Option<CellGroupArchiveRegistration>,
}

pub(super) enum Recovery {
    Ready(CellGroupRegistration),
    Seed(learner::Staged),
}

pub(super) async fn recover(
    directory: RangeDirectory,
    route: GroupRoute,
    config: Arc<Config>,
    runtime: CellGroupRuntime,
    transport: RaftTransport,
    authority: Authority,
    resources: Resources,
) -> Result<Recovery, CellGroupControllerError> {
    let Resources { existing, archive } = resources;
    authority.check_live(&runtime)?;
    let images = config.images(&directory)?;
    let _parallel = config
        .parallel
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| CellGroupControllerError::Closed)?;
    let birth = resolve(&directory, &route, &runtime, &authority).await?;
    if let Some(ManagedAuthority {
        host,
        deadline,
        learner: Some(admission),
    }) = authority.managed()
    {
        return learner::Recovery {
            directory: &directory,
            birth: &birth,
            config: &config,
            runtime: &runtime,
            transport: &transport,
            host,
            admission,
            deadline: *deadline,
        }
        .recover(Resources { existing, archive }, _parallel)
        .await;
    }
    // Check complete trusted placement before creating genesis or page files.
    let mut peers = Vec::new();
    for member in birth.voters().iter().chain(birth.learners()) {
        let discovered;
        let node = if matches!(&authority, Authority::Managed(_)) {
            discovered = transport
                .observed_peer(member.node)
                .map_err(CellGroupControllerError::Transport)?;
            discovered
                .as_ref()
                .ok_or(CellGroupControllerError::Invalid(
                    "undiscovered managed birth member",
                ))?
        } else {
            config
                .settings
                .peers
                .iter()
                .find(|p| p.node == member.node)
                .ok_or(CellGroupControllerError::Invalid(
                    "unconfigured birth member",
                ))?
        };
        peers.push(RaftPeer {
            replica: ReplicaIdentity {
                scope: route.history,
                member: member.member,
                node: node.node,
                disk: node.disk,
                process: node.process,
            },
            label: node.label.clone(),
            server_name: node.server_name.clone(),
            control_address: node.control_address,
            data_address: node.data_address,
            query_address: node.query_address,
        });
    }
    let permit = bootstrap(&directory, &birth, &runtime, &authority).await?;
    let prepared =
        recover_pages(&config, &runtime, permit.group().clone(), images, existing).await?;
    Ok(Recovery::Ready(CellGroupRegistration {
        permit,
        current: prepared.current,
        applier: prepared.applier,
        transport: PhysicalRoute {
            scope: route.history,
            schema_digest: route.schema_digest,
            revision: config.settings.peer_revision,
            peers,
        },
    }))
}

pub(super) async fn resolve(
    directory: &RangeDirectory,
    route: &GroupRoute,
    runtime: &CellGroupRuntime,
    authority: &Authority,
) -> Result<crate::raft::cell_groups::CellGroupBirth, CellGroupControllerError> {
    authority.check_live(runtime)?;
    if let Some(ManagedAuthority { host, .. }) = authority.managed() {
        let boot = host
            .boot_binding()
            .map_err(CatalogError::from)
            .map_err(catalog_error)?;
        let owner = directory
            .namespace()
            .cell_group_owner()
            .await
            .map_err(catalog_error)?;
        if owner.is_none_or(|o| o.fleet != host.fleet() || o.catalog != boot.catalog) {
            return Err(catalog_error(CatalogError::ManagedAdmissionRequired));
        }
        authority.check_live(runtime)?;
    }
    let birth = match authority {
        Authority::Initial => directory.materialize_selected_group(route).await,
        Authority::Managed(_) => directory.materialize_selected_history(route.history).await,
    }
    .map_err(catalog_error)?;
    if birth.route() != route {
        return Err(CellGroupControllerError::Invalid("provision birth route"));
    }
    authority.check_live(runtime)?;
    Ok(birth)
}

async fn bootstrap(
    directory: &RangeDirectory,
    birth: &crate::raft::cell_groups::CellGroupBirth,
    runtime: &CellGroupRuntime,
    authority: &Authority,
) -> Result<crate::raft::cell_groups::CellGroupBootstrapPermit, CellGroupControllerError> {
    authority.check_live(runtime)?;
    let permit = match authority.managed() {
        None => directory.bootstrap_group_member(birth, runtime.log()).await,
        Some(ManagedAuthority {
            host,
            learner: Some(admission),
            ..
        }) => {
            directory
                .bootstrap_managed_joined_group_member(birth, admission.member, runtime.log(), host)
                .await
        }
        Some(ManagedAuthority {
            host,
            learner: None,
            ..
        }) => {
            directory
                .bootstrap_managed_group_member(birth, runtime.log(), host)
                .await
        }
    }
    .map_err(catalog_error)?;
    authority.check_live(runtime)?;
    Ok(permit)
}

pub(super) async fn recover_pages(
    config: &Arc<Config>,
    runtime: &CellGroupRuntime,
    group: crate::raft::cache::GroupLog<cellar_logic::cell_group::GroupHistory>,
    images: crate::raft::cell_groups::CellGroupApplyImages,
    existing: Option<Prepared>,
) -> Result<Prepared, CellGroupControllerError> {
    let config = config.clone();
    runtime
        .log()
        .domain()
        .blocking(move || {
            if let Some(mut prepared) = existing {
                // Reuse the retained complete closure only while its exact
                // selection/owners are live and publication is settled. A lost
                // response with a changed selection recovers on this same owner.
                prepared.current = prepared
                    .applier
                    .recover_or_reuse_selected(&prepared.current)?;
                return Ok(prepared);
            }
            let settings = &config.settings;
            let path =
                ManifestPageStore::group_pages_path(&settings.directory, group.identity().scope);
            let pages = ManifestPageStore::open(
                &path,
                settings.filesystem.clone(),
                settings.max_pages,
                settings.max_page_bytes,
                settings.max_retained_roots,
            )
            .map_err(crate::raft::execution::storage)?;
            let (applier, current) = CellGroupApplier::recover(
                group,
                pages,
                images,
                settings.manifest,
                settings.entries,
                settings.max_effect_rows,
            )?;
            Ok::<_, ExecutionError>(Prepared { applier, current })
        })
        .map_err(|_| CellGroupControllerError::Closed)?
        .await
        .map_err(|_| CellGroupControllerError::Closed)?
        .map_err(apply_error)
}

/// An attached retry rechecks catalog/boot authority without opening a second
/// page owner or trying to attach its already claimed consensus slot again.
pub(super) async fn validate(
    directory: RangeDirectory,
    route: &GroupRoute,
    config: Arc<Config>,
    runtime: CellGroupRuntime,
    authority: Authority,
) -> Result<(), CellGroupControllerError> {
    let _parallel = config
        .parallel
        .acquire()
        .await
        .map_err(|_| CellGroupControllerError::Closed)?;
    let birth = resolve(&directory, route, &runtime, &authority).await?;
    let permit = bootstrap(&directory, &birth, &runtime, &authority).await?;
    if let Some(ManagedAuthority {
        learner: Some(admission),
        ..
    }) = authority.managed()
    {
        admission.check_installed(permit.group())?;
    }
    Ok(())
}
