// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Canonical startup uses namespace capabilities and physical group placement.
//! Logical cells are enrolled through the controller, never through node config.
mod archive;
mod collection;
mod directory;
pub(crate) use directory::require_local as require_local_fleet_boot;
mod router_refresh;
pub(crate) use router_refresh::{
    RouterRefreshClient, RouterRefreshKey, RouterRefreshOutcome, RouterRefreshRequest,
    RouterRefreshTarget,
};
mod partition;
mod provisioning;
mod recovery;
mod registration;
mod router_directory;
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
    /// Explicit routes and placements. A managed host discovers retained local
    /// replicas within its declared namespaces from the opened engine. Its joined
    /// directory worker also discovers bounded read-only routes for selected
    /// remote groups; those routes grant no namespace application permission.
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
    pending_transfer_archive: bool,
    sealed_transfer_source: bool,
    transport: super::transport::GroupRoute<GroupHistory>,
}

pub(crate) struct Prepared {
    namespaces: Vec<Namespace>,
    peers: Vec<super::transport_options::RaftPeerNode>,
    groups: Vec<Assignment>,
    query_origins: Vec<super::transport_options::NamespaceQueryOrigin>,
    command_origins: Vec<super::transport_options::NamespaceCommandOrigin>,
    isolated_peers: Option<crate::raft::fleet_catalog::FleetIsolatedTransportPeers>,
}

pub(crate) use registration::prepare;

/// Own infrastructure while assembling resources. Any error or cancelled startup
/// schedules joined closure on the same domain; SQL outlives its physical work.
struct Startup {
    service: Option<Infrastructure>,
    public: Option<RaftCells>,
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
        let public = self.public.take();
        let sql = self.sql.take();
        let controller = self.controller.take();
        domain
            .spawn_cleanup(async move {
                // A late admission/grant failure can follow entered control
                // work. Join that owner before tearing down its dependencies.
                if let Some(public) = public {
                    let _ = public.close().await;
                }
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
    recovery_authority: Option<
        Arc<dyn super::catalog::DisasterFenceAuthority<CellGroupDisasterFenceRequest>>,
    >,
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
        public: None,
        sql: None,
        controller: None,
    };
    // This initialization owns several nested catalog/recovery futures. Keep
    // that state off callers' native stacks, including nested node startup.
    let result = Box::pin(async {
        let fleet = options
            .managed
            .as_ref()
            .map(|managed| managed.authorization.stop_catalog());
        let retained =
            registration::discover_retained_placements(&mut prepared, &log, fleet.as_ref(), limits)
                .await?;
        // Boot binding commits the Fleet root through the object-store client.
        // Keep its nested futures off the caller's startup polling frame.
        let discovery = Box::pin(directory::prepare(
            &mut prepared,
            &mut options,
            &log,
            &transport,
        ))
        .await?;
        registration::refresh_routes(&mut prepared, &log, options.managed.is_some())?;
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
        let mut recovering = Vec::new();
        let mut recovery_bytes = 4096usize;
        for assignment in &mut prepared.groups {
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
                    let recovered = retained
                        .as_ref()
                        .map(|group| group.recovery_genesis())
                        .transpose()?
                        .flatten()
                        .is_some();
                    let activated_transfer = retained
                        .as_ref()
                        .map(|group| group.group_manifest())
                        .transpose()?
                        .flatten()
                        .is_some_and(|marker| marker.transfer_activation().is_some());
                    let permit = if assignment.sealed_transfer_source {
                        let managed = options.managed.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("sealed source needs managed boot authority")
                        })?;
                        namespace
                            .directory
                            .bootstrap_managed_sealed_source(
                                &assignment.birth,
                                &log,
                                &managed.authorization,
                            )
                            .await?
                    } else if recovered && options.managed.is_none() {
                        let authority = recovery_authority.clone().ok_or_else(|| {
                            anyhow::anyhow!(
                                "recovered startup requires an infrastructure fencing authority"
                            )
                        })?;
                        namespace
                            .directory
                            .bootstrap_selected_recovered_group_member(
                                &assignment.birth,
                                &log,
                                authority,
                            )
                            .await?
                    } else if let Some(member) = joined {
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
                        if activated_transfer {
                            namespace
                                .directory
                                .bootstrap_managed_activated_transfer_voter(
                                    &assignment.birth,
                                    &log,
                                    &managed.authorization,
                                )
                                .await?
                        } else {
                            namespace
                                .directory
                                .bootstrap_managed_group_member(
                                    &assignment.birth,
                                    &log,
                                    &managed.authorization,
                                )
                                .await?
                        }
                    } else {
                        namespace
                            .directory
                            .bootstrap_group_member(&assignment.birth, &log)
                            .await?
                    };
                    assignment.pending_transfer_archive = permit.transfer_archive_pending();
                    let group = permit.group().clone();
                    if let Some(request) = permit.recovery_request() {
                        // Charge the original bytes plus decoded records and
                        // both startup/activation references against the
                        // existing node control bank. No new bank is added.
                        recovery_bytes =
                            recovery_bytes
                                .checked_add(
                                    request.encode().len().checked_mul(16).ok_or_else(|| {
                                        anyhow::anyhow!("recovery metadata overflow")
                                    })?,
                                )
                                .ok_or_else(|| anyhow::anyhow!("recovery metadata overflow"))?;
                        anyhow::ensure!(
                            recovery_bytes <= options.max_control_bytes,
                            "recovery metadata exceeds node control admission"
                        );
                        recovering.push(recovery::Registration {
                            directory: namespace.directory.clone(),
                            request: request.clone(),
                            authority: permit
                                .recovery_authority()
                                .ok_or_else(|| anyhow::anyhow!("recovery authority missing"))?
                                .clone(),
                        });
                    }
                    let (applier, current) = images
                        .recover_group(
                            &log,
                            group,
                            namespace.directory.namespace_config(),
                            &options,
                            limits,
                        )
                        .await?;
                    runtime.attach(permit, current).await?;

                    appliers.push(applier);
                }
            }
        }
        // Recover parked images before clearing conservative references.
        // Parked replicas stay out of the serving roster and transport.
        let mut parked_replicas = Vec::with_capacity(retained.parked.len());
        for (namespace, group) in retained.parked {
            parked_replicas.push(
                images
                    .recover_group(
                        &log,
                        group,
                        prepared.namespaces[namespace].directory.namespace_config(),
                        &options,
                        limits,
                    )
                    .await?,
            );
        }
        let mut collectible = Vec::new();
        collectible.extend(
            images
                .adopt_recovered(&log, &appliers, &parked_replicas)
                .await?,
        );
        // No physical peer or query route is reachable while a later local group
        // still waits on its permanent claim or physical recovery. The attached
        // owners retain bounded Ready work until the controller starts draining it.
        for assignment in &prepared.groups {
            match assignment.placement {
                RaftGroupPlacement::Replica => {
                    transport.install_physical_group(assignment.transport.clone())?
                }
                RaftGroupPlacement::QueryRouter => {
                    let mut route = assignment.transport.clone();
                    if options.managed.is_some() {
                        // Router revisions track the physical configuration log.
                        route.revision = 1;
                        if route.peers.is_empty() {
                            continue;
                        }
                    }
                    let installed = if options.managed.is_some() {
                        transport.install_cell_query_group(route)
                    } else {
                        transport.install_initial_cell_query_group(route, &assignment.birth)
                    };
                    match installed {
                        Ok(()) => (),
                        Err(
                            super::transport::TransportError::Refused
                            | super::transport::TransportError::StalePeer,
                        ) if options.managed.is_some() => (),
                        Err(error) => return Err(error.into()),
                    }
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
        controller.retain_parked_retired_sources(retained.retired_sources)?;
        controller.retain_parked_removed_members(retained.removed_members)?;
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
        let archive = archive::Pools::new(limits.archive, checkpoint)?;
        let (archive_images, archive_provisioning) = if let Some(archive) = archive {
            let (observer, inventory, provisioning) = archive
                .register(&prepared, &controller, &log, &directory, limits, &options)
                .await?;
            collectible.extend(inventory);
            (Some(observer), Some(provisioning))
        } else {
            (None, None)
        };
        let archive_io = archive_provisioning
            .as_ref()
            .map(|archive| archive.io.clone());
        let automatic_topology = if let Some(mut config) = options.automatic_topology.clone() {
            // Use the actual node's private replay stores and shared archive
            // pools, including when callers supplied an external verifier.
            if let Some(recovery) = &mut config.recovery {
                recovery.recovery.archive = partition::archive_verifier(
                    archive_provisioning.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("automatic topology requires archive resources")
                    })?,
                    &log,
                    &directory,
                    limits,
                )?;
            }
            config.movement.reclaim_removed_members |= options.member_cleanup.is_some();
            Some(super::fleet_catalog::ConfiguredAutomaticTopology {
                catalog: options
                    .managed
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("automatic topology enrollment"))?
                    .authorization
                    .stop_catalog()
                    .owned_client(log.domain()),
                namespaces: prepared
                    .namespaces
                    .iter()
                    .map(|n| n.directory.clone())
                    .collect(),
                options: config,
            })
        } else {
            None
        };
        let automatic = if options.automatic_partitions.is_some() {
            partition::prepare(
                &prepared,
                archive_provisioning.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("automatic partitions require archive resources")
                })?,
                &log,
                &directory,
                limits,
                &options,
            )?
        } else {
            None
        };
        controller.configure_provisioning_with_archives(
            CellGroupProvisioning {
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
            },
            archive_provisioning,
            parked_replicas,
        )?;
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
        let collection = collection::Collection::new(collectible)?;
        let resources = crate::application::consensus::group_client::resources::Resources::new(
            sql,
            &transport,
            images.observer(),
            archive_images,
            collection.as_ref().map(collection::Collection::observer),
        );
        let service = RaftCells::from_shared(
            host,
            controller.clone(),
            crate::application::consensus::group_client::Infrastructure {
                runtime: runtime.clone(),
                log: log.clone(),
                transport: transport.clone(),
                archive_io,
            },
            admission,
            resources,
            &options,
        )?;
        startup.public = Some(service.clone());
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
        recovery::start(
            &service,
            log.clone(),
            runtime,
            recovering,
            recovery_bytes,
            options.poll_ms,
        )?;
        if let Some(collection) = collection {
            collection.start(&service, options.poll_ms)?;
        }
        if let Some(cleanup) = options
            .member_cleanup
            .as_ref()
            .filter(|_| automatic_topology.is_none())
        {
            let managed = options
                .managed
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("member cleanup requires managed enrollment"))?;
            Box::pin(
                managed
                    .authorization
                    .stop_catalog()
                    .start_node_member_cleanup(&service, cleanup.clone()),
            )
            .await?;
        }
        if let Some(config) = automatic_topology {
            service.configure_automatic_topology(config)?;
            Box::pin(service.resume_automatic_topology()).await?;
        }
        if let (Some(control), Some(config)) = (automatic, &options.automatic_partitions) {
            service.configure_automatic_partitions(control, config.clone())?;
            Box::pin(service.resume_automatic_partitions(config.service.turn_timeout)).await?;
        }
        startup.public.take();
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
