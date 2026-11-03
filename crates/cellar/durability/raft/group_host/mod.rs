// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Canonical startup uses namespace capabilities and physical group placement.
//! Logical cells are enrolled through the controller, never through node config.
mod archive;
mod directory;
mod provisioning;
mod registration;
pub use provisioning::{RaftGroupArchiveInitialization, RaftNamespaceProvisioning};
mod resources;
#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "../host_startup_tests.rs"]
mod startup_tests;
#[cfg(test)]
pub(crate) mod test_support;

use super::cell_groups::*;
use crate::application::consensus::{RaftCells, RaftClass, VersionKeys};
use cellar_logic::cell_group::GroupHistory;
use std::{path::PathBuf, sync::Arc};

/// An explicit, immutable initial range plan. Retrying preserves every birth
/// and the directory operation identity; startup never allocates a group per cell.
pub struct RaftNamespaceInitialization {
    pub operation: cellar_logic::consensus::ProposalId,
    pub groups: Vec<CellGroupBirthSpec>,
}

pub struct RaftNamespaceConfig {
    pub namespace: super::catalog::NamespaceConfig,
    pub class: String,
    /// None opens an existing namespace only. A published initialization is
    /// idempotent; it cannot replace a later topology or recovery incarnation.
    pub initialize: Option<RaftNamespaceInitialization>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftGroupPlacement {
    Replica,
    QueryRouter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftGroupAssignment {
    pub history: GroupHistory,
    pub placement: RaftGroupPlacement,
}

/// Explicit shared capacities. Physical SQL, archive and image admissions are
/// node-wide; manifest directories additionally have per-group disk ceilings.
#[derive(Clone, Copy)]
pub struct CellGroupNodeLimits {
    pub namespaces: CellGroupHostLimits,
    pub controller: CellGroupControllerLimits,
    pub sql: CellSqlLimits,
    pub provisioning: CellGroupProvisionLimits,
    pub directory: DirectoryLimits,
    pub dispatch: CellDispatchLimits,
    pub manifest: ManifestLimits,
    pub entries: crate::application::consensus::group_protocol::GroupEntryLimits,
    pub manifest_pages: usize,
    pub manifest_bytes: u64,
    pub retained_roots: usize,
    pub max_effect_rows: u32,
    pub checkpoints: Option<CellGroupCheckpointLimits>,
    /// Bindings must have been durably provisioned in the namespace catalog.
    /// Missing archive identity/counters are never initialized during restart.
    pub archive: Option<CellGroupNodeArchiveOptions>,
}

#[derive(Clone, Copy)]
pub struct CellGroupNodeArchiveOptions {
    pub io: CellGroupArchiveLimits,
    pub uploads: CellGroupArchiveUploadLimits,
    pub tails: CellGroupArchiveTailLimits,
}

pub struct RaftCellsConfig {
    pub infrastructure: crate::daemon::NodeRaftConfig,
    pub catalog: super::catalog::CatalogOptions,
    pub namespaces: Vec<RaftNamespaceConfig>,
    pub groups: Vec<RaftGroupAssignment>,
    pub peers: Vec<super::transport_options::RaftPeerNode>,
    pub query_origins: Vec<super::transport_options::NamespaceQueryOrigin>,
    pub command_origins: Vec<super::transport_options::NamespaceCommandOrigin>,
    pub host: super::host::RaftHostOptions,
    pub shared: CellGroupNodeLimits,
}

pub(crate) struct Namespace {
    directory: RangeDirectory,
    class: Arc<RaftClass>,
}

pub(crate) struct Assignment {
    namespace: usize,
    birth: CellGroupBirth,
    placement: RaftGroupPlacement,
    transport: super::transport::GroupRoute<GroupHistory>,
}

pub(crate) struct Prepared {
    namespaces: Vec<Namespace>,
    peers: Vec<super::transport_options::RaftPeerNode>,
    groups: Vec<Assignment>,
    query_origins: Vec<super::transport_options::NamespaceQueryOrigin>,
    command_origins: Vec<super::transport_options::NamespaceCommandOrigin>,
}

pub(crate) use registration::prepare;

/// Own infrastructure while assembling resources. Any error or cancelled startup
/// schedules joined closure on the same domain; SQL outlives its physical work.
struct Startup {
    service: Option<Infrastructure>,
    sql: Option<CellGroupSqlPool>,
    controller: Option<CellGroupController>,
}

struct Infrastructure {
    log: super::storage::ReplicaLog,
    runtime: CellGroupRuntime,
    transport: super::transport::RaftTransport,
}

impl Startup {
    fn cleanup(
        &mut self,
    ) -> Result<Option<crate::asyncrt::TaskHandle<()>>, crate::asyncrt::DomainClosed> {
        let Some(service) = self.service.take() else {
            return Ok(None);
        };
        let domain = service.log.domain();
        let sql = self.sql.take();
        let controller = self.controller.take();
        domain
            .spawn_cleanup(async move {
                if let Some(controller) = controller {
                    let _ = controller.close().await;
                } else {
                    let _ = service.runtime.close().await;
                    service.transport.close().await;
                    if let Some(sql) = sql {
                        let _ = sql.close().await;
                    }
                }
                let _ = service.log.close().await;
            })
            .map(Some)
    }
}

impl Drop for Startup {
    fn drop(&mut self) {
        if let Ok(Some(task)) = self.cleanup() {
            task.detach();
        }
    }
}

pub(crate) async fn start(
    service: crate::daemon::NodeRaftService,
    mut prepared: Prepared,
    directory: PathBuf,
    mut options: super::host::RaftHostOptions,
    limits: CellGroupNodeLimits,
    keys: VersionKeys,
) -> anyhow::Result<RaftCells> {
    let crate::daemon::NodeRaftService {
        log,
        runtime,
        events,
        transport,
    } = service;
    let mut startup = Startup {
        service: Some(Infrastructure {
            log: log.clone(),
            runtime: runtime.clone(),
            transport: transport.clone(),
        }),
        sql: None,
        controller: None,
    };
    // This initialization owns several nested catalog/recovery futures. Keep
    // that state off callers' native stacks, including nested node startup.
    let result = Box::pin(async {
        let discovery = directory::prepare(&mut prepared, &mut options, &log, &transport).await?;
        registration::refresh_routes(&mut prepared, &log)?;
        let sql = CellGroupSqlPool::start(&log, limits.sql)?;
        startup.sql = Some(sql.clone());
        let checkpoint = limits
            .checkpoints
            .map(CellGroupCheckpointPool::new)
            .transpose()?;
        if let Some(pool) = &checkpoint {
            transport.enable_group_checkpoints(pool.clone())?;
        }
        let images =
            resources::Images::open(&log, directory.clone(), &options, &prepared.namespaces)
                .await?;
        let mut appliers = Vec::new();
        for assignment in &prepared.groups {
            match assignment.placement {
                RaftGroupPlacement::QueryRouter => {
                    // Publish every route together after all local roots attach.
                }
                RaftGroupPlacement::Replica => {
                    let namespace = &prepared.namespaces[assignment.namespace];
                    let retained = match log.cell_group(&assignment.birth.route().history) {
                        Ok(group) => Some(group),
                        Err(super::storage::LogError::NotFound) => None,
                        Err(error) => return Err(error.into()),
                    };
                    let joined = retained
                        .as_ref()
                        .map(|group| {
                            group
                                .membership()?
                                .joined(group.identity().member)
                                .map(|binding| binding.map(|_| group.identity().member))
                        })
                        .transpose()?
                        .flatten();
                    let permit = if let Some(member) = joined {
                        if let Some(managed) = &options.managed {
                            namespace
                                .directory
                                .bootstrap_managed_joined_group_member(
                                    &assignment.birth,
                                    member,
                                    &log,
                                    &managed.authorization,
                                )
                                .await?
                        } else {
                            namespace
                                .directory
                                .bootstrap_joined_group_member(&assignment.birth, member, &log)
                                .await?
                        }
                    } else if let Some(managed) = &options.managed {
                        namespace
                            .directory
                            .bootstrap_managed_group_member(
                                &assignment.birth,
                                &log,
                                &managed.authorization,
                            )
                            .await?
                    } else {
                        namespace
                            .directory
                            .bootstrap_group_member(&assignment.birth, &log)
                            .await?
                    };
                    let group = permit.group().clone();
                    let pages = ManifestPageStore::group_pages_path(
                        &directory,
                        assignment.birth.route().history,
                    );
                    let backend = images.apply(namespace.directory.namespace_config(), &options)?;
                    let (applier, current) = log
                        .domain()
                        .blocking(move || {
                            let pages = resources::pages(&pages, limits)?;
                            Ok::<_, anyhow::Error>(CellGroupApplier::recover(
                                group,
                                pages,
                                backend,
                                limits.manifest,
                                limits.entries,
                                limits.max_effect_rows,
                            )?)
                        })?
                        .await??;
                    runtime.attach(permit, current).await?;

                    appliers.push(applier);
                }
            }
        }
        // Refuse incomplete inventories before any collector can consider unpinned
        // files. Every retained group must have an explicitly recovered assignment.
        for group in log.cell_groups()? {
            anyhow::ensure!(
                appliers
                    .iter()
                    .any(|a| a.history() == group.identity().scope),
                "retained group is absent from the node placement inventory"
            );
        }
        // No physical peer or query route is reachable while a later local group
        // still waits on its permanent claim or physical recovery. The attached
        // owners retain bounded Ready work until the controller starts draining it.
        for assignment in &prepared.groups {
            match assignment.placement {
                RaftGroupPlacement::Replica => {
                    transport.install_physical_group(assignment.transport.clone())?
                }
                RaftGroupPlacement::QueryRouter => {
                    transport.install_cell_query_group(assignment.transport.clone())?
                }
            }
        }
        let controller = CellGroupController::start(
            runtime.clone(),
            events,
            transport.clone(),
            sql.clone(),
            appliers,
            limits.controller,
        )
        .await?;
        startup.controller = Some(controller.clone());
        let admission = Arc::new(crate::application::consensus::admission::Admission::default());
        controller.bind_public_admission(
            admission.clone(),
            crate::asyncrt::services().application_work_registry(),
        )?;
        let namespaces = prepared
            .namespaces
            .iter()
            .map(|namespace| {
                Ok(CellGroupProvisionNamespace {
                    directory: namespace.directory.clone(),
                    images: images.apply(namespace.directory.namespace_config(), &options)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        controller.configure_provisioning(CellGroupProvisioning {
            directory: directory.clone(),
            filesystem: crate::asyncrt::fs(),
            namespaces,
            manifest: limits.manifest,
            entries: limits.entries,
            max_effect_rows: limits.max_effect_rows,
            max_pages: limits.manifest_pages,
            max_page_bytes: limits.manifest_bytes,
            max_retained_roots: limits.retained_roots,
            peers: prepared.peers.clone(),
            peer_revision: limits.provisioning.peer_revision,
            max_preparations: limits.provisioning.max_preparations,
            max_preparation_bytes: limits.provisioning.max_preparation_bytes,
            max_parallel_preparations: limits.provisioning.max_parallel_preparations,
            max_waiters: limits.provisioning.max_waiters,
        })?;
        let archive = archive::Pools::new(limits.archive, checkpoint)?;
        let archive_images = if let Some(archive) = archive {
            Some(
                archive
                    .register(&prepared, &controller, &log, &directory, limits, &options)
                    .await?,
            )
        } else {
            None
        };
        let mut registrations = Vec::new();
        let mut effects = Vec::new();
        let mut dispatchers = Vec::new();
        for namespace in &prepared.namespaces {
            let dispatcher = CellGroupDispatcher::from_domain(
                log.domain(),
                namespace.directory.clone(),
                namespace.class.clone(),
                keys.clone(),
                limits.dispatch,
            )?
            .with_s3(&controller)?;
            let config = namespace.directory.namespace_config();
            let storage = CellGroupNamespaceStorage::new(images.sql(config, &options)?)?;
            if namespace.class.has_system_commands() {
                let resources = storage.clone();
                effects.push(CellGroupEffectRegistration::new(
                    dispatcher.clone(),
                    options
                        .effects
                        .ok_or_else(|| anyhow::anyhow!("effects require a scheduler"))?,
                    move |cell| Ok(resources.for_cell(cell)),
                )?);
            }
            dispatchers.push((dispatcher.clone(), storage.clone()));
            registrations.push(CellGroupHostRegistration {
                dispatcher,
                storage,
            });
        }
        if !effects.is_empty() {
            controller.configure_effects(effects)?;
        }
        let host = CellGroupHost::bind(
            &runtime,
            &sql,
            &transport,
            controller.clone(),
            registrations,
            limits.namespaces,
        )?;
        let resources = crate::application::consensus::group_client::resources::Resources::new(
            sql,
            &transport,
            images.observer(),
            archive_images,
        );
        let service = RaftCells::from_shared(
            host,
            controller.clone(),
            runtime,
            log.clone(),
            admission,
            resources,
            transport.clone(),
            &options,
        )?;
        // Admission/work accounting is bound before gateways or grants become
        // externally reachable. Returned capabilities own these weak registrations.
        for (dispatcher, storage) in dispatchers {
            service.retain_gateways(
                transport.install_cell_query_gateway(
                    dispatcher.clone(),
                    controller.clone(),
                    storage.clone(),
                )?,
                transport.install_cell_command_gateway(dispatcher, controller.clone(), storage)?,
            )?;
        }
        for origin in prepared.query_origins {
            transport.grant_query_origin(origin)?;
        }
        for origin in prepared.command_origins {
            transport.grant_command_origin(origin)?;
        }
        if let Some(discovery) = discovery {
            discovery.start(&service, log.clone(), transport.clone())?;
        }
        startup.service.take();
        startup.sql.take();
        startup.controller.take();
        Ok(service)
    })
    .await;
    if result.is_err() {
        // An explicit startup error is a physical join boundary even when the
        // caller retains log/runtime/transport clones. Cancellation detaches
        // this same cleanup task, whose dependencies remain node-owned.
        if let Some(task) = startup.cleanup()? {
            task.await?;
        }
    }
    result
}
