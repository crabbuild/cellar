// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-owned command producers and Raft/SQL effect dispatch. Waiters never own
//! a submitted producer, its private image or its progress through publication.
pub use super::host_archive::RaftArchiveOptions;
use super::{
    archive::{ArchiveError, CellArchive},
    budget::{Budget, Permit},
    cache::GroupLog,
    catalog::*,
    completion::*,
    execution::*,
    host_sql::{self as sql, Action, ImageView, Pool, Value},
    mailbox,
    query::{Minimum, PendingRead, Phase as ReadPhase, ReadId},
    runtime::{RaftRuntime, RuntimeEvents},
    transport::{GroupRoute, RaftPeer, RaftTransport},
};
use crate::application::consensus::client::{
    self, Binding, ControlRequest, Kind, NodeKind, NodeRequest, Request, Response,
};
use crate::application::consensus::{command_request_digest, OperationKind, RaftCells, RaftClass};
use cellar_logic::consensus::*;
use cellar_logic::raft_command::CommandState;
use cellar_ltx::generation::{GenerationLimits, ImageId, ReadServices};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use rand::RngCore;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::{mpsc, Notify};

#[path = "host_image_limits.rs"]
mod image_limits;

#[path = "host_extents.rs"]
mod extents;
pub use extents::{RaftExtentCacheLimits, RaftExtentLimits, RaftExtentOptions};

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "host_startup_tests.rs"]
mod startup_tests;

#[cfg(test)]
use super::storage_fault;

#[path = "host_admission.rs"]
mod admission;

#[path = "host_controller.rs"]
mod controller;
#[path = "host_drain.rs"]
mod drain;
#[path = "host_effects.rs"]
mod effect_scheduler;
pub use controller::{RaftControllerOptions, RaftControllerStatus};
#[path = "host_directory.rs"]
mod directory;
#[path = "host_enrollment.rs"]
mod enrollment;
pub(crate) use enrollment::NamespaceRegistration;
pub use enrollment::{RaftManagedHostOptions, RaftNamespaceRegistration};
#[path = "host_inspection.rs"]
mod inspection;
#[path = "host_leader_load.rs"]
mod load;
#[path = "host_readiness.rs"]
mod readiness;
pub(crate) use load::GroupResourceObserver;
pub use load::RaftInspectionScanOptions;
#[path = "host_fleet_admission.rs"]
mod fleet_admission;
#[path = "host_fleet.rs"]
mod fleet_control;
#[path = "host_fleet_observation.rs"]
mod fleet_observation;
pub use fleet_observation::{FleetMembershipConfiguration, FleetMembershipObservation};
#[path = "host_fleet_cut.rs"]
mod fleet_cut;
pub(crate) use fleet_cut::Publication;
pub use fleet_cut::{FleetImageState, FleetPublishedCut, FleetPublishedImage};
#[path = "host_fleet_inspection.rs"]
mod fleet_inspection;
pub use super::fleet_inspection_wire::RaftPeerFleetMembershipObservation;
#[path = "host_initial.rs"]
mod initial;
#[path = "host_node.rs"]
mod node_control;
#[path = "host_registration.rs"]
mod registration;
use node_control::reply_node;
pub use node_control::{
    RaftHostEnrollmentObservation, RaftHostObservation, RaftHostStartupBounds,
    RaftPeerNodeObservation,
};
#[path = "host_fleet_execution.rs"]
mod fleet_execution;
#[path = "host_fleet_stop.rs"]
mod fleet_stop;
#[path = "host_membership.rs"]
mod membership;
#[path = "host_node_inspection.rs"]
mod node_inspection;
pub use directory::RaftNodeDirectoryOptions;
#[path = "host_seed.rs"]
mod seed;
#[path = "host_snapshot.rs"]
mod snapshots;
#[path = "host_standby.rs"]
mod standby;

#[derive(Clone, Debug)]
pub struct RaftHostOptions {
    /// Shared inventory and caches for namespaces selecting extent images.
    pub extents: Option<RaftExtentOptions>,
    pub close_report: super::operations::RaftCloseReportLimits,
    /// Explicit committed alarm/outbox scheduling. Definitions refuse activation when
    /// this capability is absent; ordinary classes do not start a scheduler.
    pub effects: Option<super::effects::RaftEffectOptions>,
    /// Opt in to bounded automatic repair and optional leadership balancing.
    pub controller: Option<RaftControllerOptions>,
    /// Bounded complete node-wide resource scans. Peer inspection still requires
    /// its independent explicit profile and grants.
    pub inspection_scan: RaftInspectionScanOptions,
    /// Explicit inbound metadata permissions. Discovery and query grants do not
    /// enable inspection. Each peer must match the configured trusted endpoint.
    pub inspection_origins: Vec<super::transport_options::NamespaceInspectionOrigin>,
    /// Enable read-only placement inspection with explicit capacity declarations.
    pub node_profile: Option<super::inspection::RaftNodeProfile>,
    /// Publish and refresh configured physical endpoints, or the complete rooted
    /// node set for managed hosts. Requires a trusted self endpoint and storage.
    pub node_directory: Option<RaftNodeDirectoryOptions>,
    /// Explicit rooted node enrollment and compiled namespace capabilities.
    /// Capacity readiness/activation and complete inventory are separate proofs.
    pub managed: Option<RaftManagedHostOptions>,
    /// Enable native snapshot recovery and its larger image admission floor.
    pub enable_snapshots: bool,
    /// Applied entries between automatic checkpoints. Shared log-cache pressure
    /// can trigger an earlier checkpoint; zero disables automatic compaction.
    pub checkpoint_entries: u64,
    pub archive: RaftArchiveOptions,
    pub max_groups: usize,
    pub blocking_workers: usize,
    pub worker_queue: usize,
    /// Node-wide physical SDK query-reader reservations (1..=16). Each remains
    /// owned until its callback and immutable connection exit, even if cancelled.
    pub max_query_pins: usize,
    pub max_requests: usize,
    /// Independent administrative/inspection admission, including retained waiters.
    pub max_control_requests: usize,
    pub max_control_bytes: usize,
    pub max_pending_bytes: usize,
    pub max_input_bytes: usize,
    pub poll_ms: u64,
    pub stale_source_timeout_ms: u64,
    pub images: GenerationLimits,
    pub execution: ExecutionLimits,
}

impl Default for RaftHostOptions {
    fn default() -> Self {
        let images = GenerationLimits {
            apply: super::snapshot::SnapshotLimits::default().apply(),
            ..Default::default()
        };
        Self {
            extents: None,
            close_report: super::operations::RaftCloseReportLimits::default(),
            effects: None,
            controller: None,
            inspection_scan: RaftInspectionScanOptions::default(),
            inspection_origins: Vec::new(),
            node_directory: None,
            node_profile: None,
            managed: None,
            enable_snapshots: true,
            checkpoint_entries: 256,
            archive: RaftArchiveOptions::default(),
            max_groups: 128,
            blocking_workers: 2,
            worker_queue: 32,
            max_query_pins: 16,
            max_requests: 128,
            max_control_requests: 16,
            max_control_bytes: 16 * 1024 * 1024,
            max_pending_bytes: 64 * 1024 * 1024,
            max_input_bytes: 1024 * 1024,
            poll_ms: 25,
            stale_source_timeout_ms: 5000,
            images,
            execution: ExecutionLimits::default(),
        }
    }
}

impl RaftHostOptions {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        self.close_report.validate()?;
        if let Some(managed) = &self.managed {
            managed.validate()?;
            anyhow::ensure!(
                self.max_control_bytes
                    >= 4 * 1024 * 1024 + super::inspection::RESPONSE_RESERVATION + 4096
                    && self.max_control_requests >= 3,
                "managed discovery requires bounded metadata and control capacity"
            );
            anyhow::ensure!(
                self.node_profile.is_some()
                    && self.node_directory.is_some()
                    && self.controller.is_none(),
                "managed enrollment requires a profile/directory; disable the legacy controller"
            );
        }
        if let Some(directory) = &self.node_directory {
            directory.validate()?;
        }
        anyhow::ensure!(
            self.inspection_origins.is_empty()
                || (self.node_profile.is_some() && self.node_directory.is_some()),
            "peer inspection requires a placement profile and physical node directory"
        );
        anyhow::ensure!(
            self.inspection_origins.len() <= 4096,
            "too many inspection grants"
        );
        self.inspection_scan.validate()?;
        if let Some(controller) = &self.controller {
            controller.validate(self)?;
        }
        if let Some(alarms) = self.effects {
            alarms.validate(self)?;
        }
        self.archive.validate()?;
        self.execution.validate()?;
        if let Some(extents) = &self.extents {
            extents.validate(self.execution)?;
        }
        anyhow::ensure!(
            self.archive.limits.envelope == self.execution.envelope,
            "archive and execution envelope limits must agree"
        );
        // Snapshot/current/private/apply/incoming and an older source transfer
        // may coexist. Preparation reserves two copies. Additional reader or
        // archive pins may still apply bounded backpressure.
        let (generations, copies) = if self.enable_snapshots {
            (6, 8)
        } else {
            (3, 4)
        };
        anyhow::ensure!(
            self.images.max_generations >= generations
                && self.images.max_bytes >= self.execution.max_image_bytes.saturating_mul(copies),
            "image admission cannot cover configured snapshot/private/apply ownership"
        );
        let apply = self.images.apply;
        let envelope = self.execution.envelope;
        let snapshot_bound =
            cellar_ltx::ltx::full_snapshot_bound(self.execution.max_image_bytes, 512)
                .ok_or_else(|| anyhow::anyhow!("snapshot encoded bound overflow"))?;
        anyhow::ensure!(
            self.archive.limits.max_snapshot_bytes >= snapshot_bound as u64
                && self.archive.limits.max_image_bytes >= self.execution.max_image_bytes,
            "snapshot image and archive admission must cover full-LTX encoding overhead"
        );
        anyhow::ensure!(
            apply.decode.max_image_bytes >= self.execution.max_image_bytes
                && apply.max_files >= envelope.max_files
                && apply.max_total_encoded_bytes >= envelope.max_payload_bytes
                && apply.decode.max_encoded_bytes >= envelope.max_payload_bytes
                && apply.max_total_decoded_bytes >= envelope.max_decoded_bytes
                && apply.decode.max_decoded_bytes >= envelope.max_decoded_bytes
                && apply.max_total_pages as u64 >= envelope.max_pages
                && apply.decode.max_pages as u64 >= envelope.max_pages,
            "image application limits cannot cover admitted execution envelopes"
        );
        anyhow::ensure!(
            self.max_groups > 0
                && self.max_groups <= 4096
                && (1..=16).contains(&self.blocking_workers)
                && (1..=256).contains(&self.worker_queue)
                && (1..=16).contains(&self.max_query_pins)
                && (1..=4096).contains(&self.max_requests)
                && (1..=256).contains(&self.max_control_requests)
                && (4096..=64 * 1024 * 1024).contains(&self.max_control_bytes)
                && (4096..=256 * 1024 * 1024).contains(&self.max_pending_bytes)
                && self.max_input_bytes > 0
                && self.max_input_bytes <= self.execution.envelope.max_payload_bytes
                && self.max_input_bytes + 4096 <= self.max_pending_bytes
                && (1..=1000).contains(&self.poll_ms),
            "invalid Raft host limits"
        );
        anyhow::ensure!(
            (1..=3_600_000).contains(&self.stale_source_timeout_ms),
            "invalid stale-source deadline"
        );
        if let Some(profile) = &self.node_profile {
            profile.validate(self.max_groups, self.images.max_bytes)?;
            anyhow::ensure!(
                self.max_control_requests >= 2
                    && self.max_control_bytes >= super::inspection::RESPONSE_RESERVATION + 4096,
                "Raft inspection requires independent request and response capacity"
            );
        }
        Ok(())
    }
}

use super::transport_options::NamespaceQueryOrigin;
pub use super::transport_options::RaftPeerNode;

/// Local participation is explicit. A router never creates or attaches a
/// member and never initializes a SQLite cell image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftCellPlacement {
    Replica,
    /// Register an eligible local standby without creating a group or image.
    /// A retained handle gains local admission only after authorized learner
    /// initialization in this node's existing engine and SQL pool.
    Standby,
    /// Resume a stable joined member or seed a new committed learner from the
    /// specified trusted peer. A claimed member with lost state is refused.
    /// Fresh managed learners require authorized activation on a `Standby`;
    /// this placement can still resume their intact retained engines.
    Joining {
        member: MemberId,
        source: MemberId,
        source_node: NodeId,
    },
    /// Seed a fresh learner from an exact S3 manifest selected by a live peer
    /// read barrier. Existing stable joined state resumes without reseeding.
    /// Fresh managed learners use authorized `Standby` activation instead.
    JoiningFromS3 {
        member: MemberId,
        source: MemberId,
        source_node: NodeId,
    },
    QueryRouter {
        leader_hint: Option<MemberId>,
    },
}

#[derive(Clone, Debug)]
pub struct RaftCellRegistration {
    pub placement: RaftCellPlacement,
    pub namespace: NamespaceConfig,
    pub class: String,
    pub birth: BirthProposal,
}

pub struct RaftCellsConfig {
    pub infrastructure: crate::daemon::NodeRaftConfig,
    pub catalog: CatalogOptions,
    pub cells: Vec<RaftCellRegistration>,
    pub peers: Vec<RaftPeerNode>,
    pub query_origins: Vec<NamespaceQueryOrigin>,
    pub host: RaftHostOptions,
}

pub struct RaftCellsServices {
    pub listeners: crate::daemon::NodeRaftListeners,
    pub tls: super::transport_options::RaftTls,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub application: Arc<dyn crate::application::Application>,
    pub versions: super::version::VersionKeys,
}

pub(crate) struct Registration {
    pub placement: RaftCellPlacement,
    pub namespace: NamespaceCatalog,
    pub birth: GroupBirth,
    pub class: Arc<RaftClass>,
}

pub(crate) struct Registrations {
    pub cells: Vec<Registration>,
    pub namespaces: Vec<NamespaceRegistration>,
    pub peers: Vec<RaftPeerNode>,
    pub origins: Vec<NamespaceQueryOrigin>,
}

pub(crate) async fn start(
    service: crate::daemon::NodeRaftService,
    registrations: Registrations,
    directory: PathBuf,
    label: String,
    options: RaftHostOptions,
    versions: super::version::VersionKeys,
) -> anyhow::Result<RaftCells> {
    let log = service.log.clone();
    let runtime = service.runtime.clone();
    let transport = service.transport.clone();
    let result = Box::pin(initialize(
        service,
        registrations,
        directory,
        label,
        options,
        versions,
    ))
    .await;
    if result.is_err() {
        // This includes preflight and pool-construction errors before the
        // initialized-cell cleanup exists. Retained handles must observe real
        // closure; dropping the last caller is not a physical join boundary.
        let _ = runtime.close().await;
        transport.close().await;
        let _ = log.close().await;
    }
    result
}

async fn initialize(
    service: crate::daemon::NodeRaftService,
    registrations: Registrations,
    directory: PathBuf,
    label: String,
    mut options: RaftHostOptions,
    versions: super::version::VersionKeys,
) -> anyhow::Result<RaftCells> {
    let Registrations {
        cells: registrations,
        namespaces: declared_namespaces,
        peers,
        origins,
    } = registrations;
    options.validate()?;
    for registration in &registrations {
        registration.class.validate_host(&options)?;
    }
    let snapshot_limits = service
        .transport
        .snapshot_limits(options.execution.max_image_bytes)?;
    anyhow::ensure!(
        (!registrations.is_empty() || options.managed.is_some())
            && registrations.len() <= options.max_groups,
        "invalid registered group count"
    );
    let domain = service.log.domain();
    let budget = Budget::new(options.max_pending_bytes, options.max_requests);
    let archive_capture_budget = Budget::new(
        options.archive.limits.max_capture_bytes * options.archive.max_producers,
        options.archive.max_producers,
    );
    let query_pins = Budget::new(0, options.max_query_pins);
    let mut pool = Pool::start(
        domain.clone(),
        options.blocking_workers,
        options.worker_queue,
        options.max_groups,
        super::host_sql::Budgets {
            business: budget.clone(),
            archive_capture: archive_capture_budget.clone(),
            query_pins: query_pins.clone(),
        },
        service.log.performance(),
    )?;
    let mut cells = BTreeMap::new();
    let mut standbys = BTreeMap::new();
    let mut bindings = BTreeMap::new();
    let mut routes = Vec::new();
    let mut archive_owner = None;
    let mut node_directory = None;
    let mut extent_owner = None;
    let peer_count = peers.len();
    let mut peers: BTreeMap<_, _> = peers.into_iter().map(|peer| (peer.node, peer)).collect();
    let initialized = async {
        let needs_extents = declared_namespaces.iter().any(|registration| {
            registration.namespace.config().image_storage == RaftImageStorage::Extents
        }) || registrations.iter().any(|registration| {
            registration.namespace.config().image_storage == RaftImageStorage::Extents
                && !matches!(
                    registration.placement,
                    RaftCellPlacement::QueryRouter { .. }
                )
        });
        extent_owner = extents::Owner::open(
            &service.log,
            directory.clone(),
            ltx_host(),
            options.extents,
            needs_extents,
        )
        .await?;
        anyhow::ensure!(
            peer_count == peers.len() && peer_count <= service.transport.peer_limit(),
            "invalid peer discovery count"
        );
        let authorizations = enrollment::prepare(
            &mut options,
            &service,
            &registrations,
            &declared_namespaces,
            &peers,
            &label,
        )
        .await?;
        let mut grants = BTreeSet::new();
        for origin in origins {
            anyhow::ensure!(
                grants.insert((origin.cluster, origin.namespace, origin.peer.node)),
                "duplicate namespace query origin"
            );
            service.transport.grant_query_origin(origin)?;
        }
        node_directory = directory::prepare(
            options.node_directory.as_ref(),
            &registrations,
            &declared_namespaces,
            options.managed.as_mut().map(|m| &mut m.authorization),
            &mut peers,
            &service,
            &label,
        )
        .await?;
        let namespaces: BTreeSet<_> = registrations
            .iter()
            .map(|registration| {
                (
                    registration.namespace.config().cluster,
                    registration.namespace.config().namespace,
                )
            })
            .chain(declared_namespaces.iter().map(|registration| {
                (
                    registration.namespace.config().cluster,
                    registration.namespace.config().namespace,
                )
            }))
            .collect();
        let mut grants = BTreeSet::new();
        for origin in &options.inspection_origins {
            anyhow::ensure!(
                namespaces.contains(&(origin.cluster, origin.namespace))
                    && peers.get(&origin.peer.node) == Some(&origin.peer)
                    && grants.insert((origin.cluster, origin.namespace, origin.peer.node)),
                "inspection origin must match a unique registered namespace and trusted peer"
            );
            service.transport.grant_inspection_origin(origin.clone())?;
        }
        for (index, registration) in registrations.into_iter().enumerate() {
            let Registration {
                mut placement,
                namespace,
                birth,
                class,
            } = registration;
            namespace.require_mode(cellar_logic::NamespaceMode::Raft)?;
            anyhow::ensure!(
                namespace.config().schema_digest == class.schema_digest(),
                "registered schema disagrees with namespace"
            );
            let scope = birth.scope().clone();
            let worker = index % options.blocking_workers;
            let cell_directory = directory
                .join(hex::encode(scope.namespace.bytes()))
                .join(hex::encode(scope.group.bytes()))
                .join(hex::encode(scope.incarnation.bytes()));
            let rooted = match namespace.config().image_storage {
                RaftImageStorage::Materialized => None,
                RaftImageStorage::Extents
                    if matches!(placement, RaftCellPlacement::QueryRouter { .. }) =>
                {
                    None
                }
                RaftImageStorage::Extents => Some(
                    extent_owner
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("missing node extent owner"))?
                        .cell_storage(&cell_directory),
                ),
            };
            let storage = sql::CellStorage {
                directory: cell_directory,
                host: ltx_host(),
                services: read_services(),
                images: options.images,
                rooted,
                snapshot_scratch: service.log.snapshot_scratch(),
                execution: options.execution,
            };
            if placement == RaftCellPlacement::Standby {
                anyhow::ensure!(
                    options.enable_snapshots,
                    "standby activation requires snapshots"
                );
                anyhow::ensure!(
                    !birth
                        .voters()
                        .iter()
                        .chain(birth.learners())
                        .any(|m| m.node == service.log.limits().node),
                    "standby is an initial member"
                );
                anyhow::ensure!(
                    !standbys.contains_key(&scope)
                        && !bindings.contains_key(&(scope.namespace, scope.cell.clone())),
                    "duplicate standby registration"
                );
                namespace.require_node_storage(&service.log).await?;
                let existing = match service.log.group(&scope) {
                    Ok(group) => Some(group),
                    Err(super::storage::LogError::NotFound) => None,
                    Err(error) => return Err(error.into()),
                };
                let resume = existing.as_ref().is_some_and(|group| {
                    group.read().is_ok_and(|cache| {
                        cache.membership.permits(group.identity())
                            && super::member_state::members(&cache.conf_state)
                                .contains(&group.identity().member.get())
                    })
                });
                standbys.insert(
                    scope.clone(),
                    standby::Dormant {
                        namespace: namespace.clone(),
                        birth: birth.clone(),
                        class: class.clone(),
                        storage: storage.clone(),
                        worker,
                        failed: existing.is_some() && !resume,
                    },
                );
                if resume {
                    let group = existing
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("standby resume state missing"))?;
                    let source = birth
                        .voters()
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("standby birth has no voter"))?;
                    placement = RaftCellPlacement::Joining {
                        member: group.identity().member,
                        source: source.member,
                        source_node: source.node,
                    };
                } else {
                    bindings.insert(
                        (scope.namespace, scope.cell.clone()),
                        Binding {
                            scope,
                            class,
                            minimum: namespace.minimum_durability(),
                            member: client::LocalMember::new(None),
                            leader_hint: None,
                        },
                    );
                    continue;
                }
            }
            if let RaftCellPlacement::QueryRouter { leader_hint } = placement {
                let scope = birth.scope().clone();
                let local = service.log.limits().node;
                anyhow::ensure!(
                    !birth
                        .voters()
                        .iter()
                        .chain(birth.learners())
                        .any(|member| member.node == local),
                    "query router is an initial group member"
                );
                anyhow::ensure!(
                    matches!(
                        service.log.group(&scope),
                        Err(super::storage::LogError::NotFound)
                    ),
                    "query router has local member state"
                );
                anyhow::ensure!(
                    leader_hint.is_none_or(|hint| birth
                        .voters()
                        .iter()
                        .any(|member| member.member == hint)),
                    "invalid query leader hint"
                );
                anyhow::ensure!(
                    !bindings.contains_key(&(scope.namespace, scope.cell.clone())),
                    "duplicate registered cell"
                );
                let routes = birth
                    .voters()
                    .iter()
                    .chain(birth.learners())
                    .map(|member| {
                        let peer = peers
                            .get(&member.node)
                            .ok_or_else(|| anyhow::anyhow!("missing query peer route"))?;
                        Ok(RaftPeer {
                            replica: ReplicaIdentity {
                                scope: scope.clone(),
                                node: peer.node,
                                member: member.member,
                                disk: peer.disk,
                                process: peer.process,
                            },
                            label: peer.label.clone(),
                            server_name: peer.server_name.clone(),
                            control_address: peer.control_address,
                            data_address: peer.data_address,
                            query_address: peer.query_address,
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                service.transport.install_query_group(GroupRoute {
                    scope: scope.clone(),
                    schema_digest: class.schema_digest(),
                    revision: 1,
                    peers: routes,
                })?;
                bindings.insert(
                    (scope.namespace, scope.cell.clone()),
                    Binding {
                        scope,
                        class,
                        minimum: namespace.minimum_durability(),
                        member: client::LocalMember::new(None),
                        leader_hint,
                    },
                );
                continue;
            }
            anyhow::ensure!(
                !cells.contains_key(&scope)
                    && !bindings.contains_key(&(scope.namespace, scope.cell.clone())),
                "duplicate registered cell"
            );
            let permit = match placement {
                RaftCellPlacement::Replica => {
                    if birth.managed_owner().is_some() {
                        let authorization = authorizations.get(&scope).ok_or_else(|| {
                            anyhow::anyhow!("managed initial host authorization required")
                        })?;
                        namespace
                            .bootstrap_managed_member(&birth, &service.log, authorization)
                            .await?
                    } else {
                        namespace.bootstrap_member(&birth, &service.log).await?
                    }
                }
                RaftCellPlacement::Joining {
                    member,
                    source,
                    source_node,
                }
                | RaftCellPlacement::JoiningFromS3 {
                    member,
                    source,
                    source_node,
                } => {
                    let from_s3 = matches!(placement, RaftCellPlacement::JoiningFromS3 { .. });
                    anyhow::ensure!(
                        options.enable_snapshots,
                        "joining requires snapshot recovery"
                    );
                    match namespace
                        .resume_joined(&birth, &service.log, member)
                        .await?
                    {
                        Some(permit) => permit,
                        None => {
                            let peer = peers
                                .get(&source_node)
                                .ok_or_else(|| anyhow::anyhow!("missing seed source endpoint"))?;
                            peer.validate_endpoint()?;
                            let local = service.log.limits();
                            let identity = ReplicaIdentity {
                                scope: scope.clone(),
                                node: local.node,
                                member,
                                disk: local.disk,
                                process: local.process,
                            };
                            let source = RaftPeer {
                                replica: ReplicaIdentity {
                                    scope: scope.clone(),
                                    node: peer.node,
                                    member: source,
                                    disk: peer.disk,
                                    process: peer.process,
                                },
                                label: peer.label.clone(),
                                server_name: peer.server_name.clone(),
                                control_address: peer.control_address,
                                data_address: peer.data_address,
                                query_address: peer.query_address,
                            };
                            let limits = service
                                .transport
                                .snapshot_limits(options.execution.max_image_bytes)?;
                            let action = if from_s3 {
                                let archive = namespace
                                    .cell_archive(
                                        &birth,
                                        storage.host.clone(),
                                        options.archive.limits,
                                    )?
                                    .with_scratch_directory(service.log.limits().directory.clone())
                                    .with_snapshot_scratch(service.log.snapshot_scratch());
                                let archive = match &storage.rooted {
                                    Some(rooted) => archive.with_root_store(rooted.store.clone()),
                                    None => archive,
                                };
                                let received = service
                                    .transport
                                    .seed_from_archive(
                                        identity,
                                        source,
                                        &birth,
                                        class.schema_digest(),
                                        limits,
                                        archive,
                                    )
                                    .await?;
                                Action::SealArchiveSeed {
                                    received: Box::new(received),
                                    storage: storage.clone(),
                                }
                            } else {
                                let received = service
                                    .transport
                                    .seed_from_peer(
                                        identity,
                                        source,
                                        &birth,
                                        class.schema_digest(),
                                        limits,
                                        storage.rooted.as_ref().map(|root| root.store.clone()),
                                    )
                                    .await?;
                                Action::SealSeed {
                                    received: Box::new(received),
                                    storage: storage.clone(),
                                }
                            };
                            let output =
                                pool.call(worker, scope.clone(), action, None, None).await?;
                            let Value::Seeded(seed) = output.value else {
                                anyhow::bail!("SQL seed response")
                            };
                            // Managed fresh learners use registered standby activation
                            // with an opaque catalog authorization. Retained joined
                            // state above resumes without creating a new membership.
                            standby::check_seed_guard(
                                seed.membership(),
                                seed.local(),
                                None,
                                false,
                            )?;
                            service
                                .log
                                .performance()
                                .measure(
                                    super::performance::Operation::RecoveryInstall,
                                    service.log.create_seeded(*seed),
                                )
                                .await?;
                            namespace
                                .resume_joined(&birth, &service.log, member)
                                .await?
                                .ok_or_else(|| anyhow::anyhow!("seed state missing after fence"))?
                        }
                    }
                }
                RaftCellPlacement::QueryRouter { .. } | RaftCellPlacement::Standby => {
                    unreachable!("non-replica handled above")
                }
            };
            let group = service.log.group(&scope)?;
            let extent_store = storage.rooted.as_ref().map(|root| root.store.clone());
            let output = pool
                .call(
                    worker,
                    scope.clone(),
                    Action::Initialize {
                        group: group.clone(),
                        class: class.clone(),
                        storage,
                    },
                    None,
                    None,
                )
                .await?;
            let Value::Initialized(image) = output.value else {
                anyhow::bail!("SQL initialization response")
            };
            routes.push(membership::route(
                &group,
                &birth,
                &peers,
                class.schema_digest(),
                &label,
                &service.transport,
            )?);
            bindings.insert(
                (scope.namespace, scope.cell.clone()),
                Binding {
                    scope: scope.clone(),
                    class: class.clone(),
                    minimum: namespace.minimum_durability(),
                    member: client::LocalMember::new(Some(group.identity().member)),
                    leader_hint: None,
                },
            );
            let archive = namespace
                .cell_archive(&birth, ltx_host(), options.archive.limits)?
                .with_scratch_directory(service.log.limits().directory.clone())
                .with_snapshot_scratch(service.log.snapshot_scratch())
                .with_publication_coordination(
                    super::host_archive::publication_owner(group.identity().node_identity()),
                    options.archive.publication_stale_ms,
                    64 * 1024 * 1024,
                )?;
            cells.insert(
                scope.clone(),
                Cell::recovered(
                    group,
                    birth.clone(),
                    archive,
                    image,
                    worker,
                    namespace.minimum_durability(),
                    class.schema_digest(),
                )?
                .with_extents(extent_store),
            );
            permit.attach(&service.runtime).await?;
        }
        if !cells.is_empty() || !standbys.is_empty() || options.managed.is_some() {
            let mut nonce = [0; 16];
            crate::asyncrt::rng("raft-archive-verifier").fill_bytes(&mut nonce);
            let owner = super::host_archive::Owner::open(
                directory.join("archive-verifier"),
                ltx_host(),
                ImageId::new(nonce)?,
                options.images.apply,
                pool.image_accounting(),
                service.log.snapshot_scratch(),
            )
            .await?;
            for cell in cells.values_mut() {
                cell.archive = cell
                    .archive
                    .clone()
                    .with_cancellation(owner.cancellation.subscribe());
            }
            archive_owner = Some(owner);
        }
        // The host cannot drain runtime events until initialization completes.
        // Publishing earlier lets peer traffic retain the ordinary request
        // budget behind that drain and reject later group attachments. Keep
        // routes private until every local group and the archive owner are ready.
        for route in routes {
            service.transport.install_group(route)?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = initialized {
        let _ = service.runtime.close().await;
        service.transport.close().await;
        let _ = pool.join().await;
        let _ = service.log.close().await;
        // A failed join can still own sealed roots in cells or standby storage.
        // Release their final directory locks on a joined blocking owner too.
        let cleanup = service.log.domain().blocking(move || {
            drop((cells, standbys, extent_owner, archive_owner));
        });
        let cleanup = match cleanup {
            Ok(job) => job.await.map_err(anyhow::Error::from),
            Err(error) => Err(error.into()),
        };
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => {
                Err(error.context(format!("physical startup cleanup failed: {cleanup}")))
            }
        };
    }
    let (sender, requests) = mailbox::channel(options.max_requests);
    let (control_sender, controls) = mailbox::channel(options.max_control_requests);
    let control_budget = Budget::new(options.max_control_bytes, options.max_control_requests);
    let seed_archive_work = Budget::new(0, options.max_control_requests);
    let (close, closes) = mpsc::unbounded_channel();
    let closed = Arc::new(AtomicBool::new(false));
    let closing = Arc::new(AtomicBool::new(false));
    let terminal = Arc::new(client::Terminal {
        result: Mutex::new(None),
        changed: Notify::new(),
    });
    let controller_bindings = if options.controller.is_some() {
        bindings.values().cloned().collect()
    } else {
        Vec::new()
    };
    let has_effects = bindings
        .values()
        .any(|b| b.class.alarm_limits().is_some() || b.class.outbox_definition().is_some());
    let handle = Arc::new(client::Handle {
        performance: service.log.performance(),
        requests: sender,
        control_requests: control_sender,
        control_budget: control_budget.clone(),
        close,
        domain: domain.clone(),
        closed: closed.clone(),
        closing: closing.clone(),
        admission: Arc::new(crate::application::consensus::admission::Admission::default()),
        terminal: terminal.clone(),
        budget: budget.clone(),
        archive_capture_budget,
        query_pins,
        sql: Some(pool.resource_observer()),
        extents: extent_owner.as_ref().map(|owner| owner.store.observer()),
        groups: load::GroupResourceObserver::default(),
        controller: Mutex::new(None),
        fleet_restart_service: Mutex::new(None),
        fleet_movement_service: Mutex::new(None),
        transport: service.transport.resource_observer(),
        max_input_bytes: options.max_input_bytes,
        bindings: client::Bindings::new(bindings, options.max_groups),
        work: crate::asyncrt::services().application_work_registry(),
    });
    let installation = (|| -> anyhow::Result<()> {
        if options.managed.is_some() {
            crate::asyncrt::services()
                .raft_lifecycle()
                .bind_startup_admission(&handle.admission)?;
        }
        service.transport.install_query_gateway(handle.clone())?;
        Ok(())
    })();
    if let Err(error) = installation {
        let _ = service.runtime.close().await;
        service.transport.close().await;
        let _ = pool.join().await;
        let _ = service.log.close().await;
        return Err(error);
    }
    let local = handle.clone();
    let crate::daemon::NodeRaftService {
        log,
        runtime,
        events,
        transport,
    } = service;
    domain.spawn_detached("raft-cell-host", async move {
        struct Closed(Arc<AtomicBool>, Arc<client::Terminal>);

        impl Drop for Closed {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
                self.1.changed.notify_waiters();
            }
        }
        let _closed = Closed(closed, terminal.clone());
        let mut rng = crate::asyncrt::rng("raft-cell-commands");
        let mut read_nonce = [0; 16];
        rng.fill_bytes(&mut read_nonce);
        let mut driver = Driver {
            drain: None,
            effect_scheduler: options
                .effects
                .filter(|_| has_effects)
                .map(effect_scheduler::Scheduler::new),
            controller: options
                .controller
                .as_ref()
                .map(|options| controller::Controller::new(options.clone(), controller_bindings)),
            leader_load: Some(load::Scanner::new(options.inspection_scan.clone())),
            directory: node_directory,
            peers,
            label,
            archive_owner,
            extents: extent_owner,
            log,
            runtime,
            events,
            transport,
            pool,
            cells,
            standbys,
            declared_namespaces,
            history_registration: None,
            directory_path: directory,
            activating: None,
            active: BTreeSet::new(),
            applying: BTreeSet::new(),
            uncertain: BTreeMap::new(),
            jobs: FuturesUnordered::new(),
            options,
            rng,
            closing,
            versions,
            reads: BTreeMap::new(),
            read_nonce,
            read_sequence: 0,
            budget,
            control_budget,
            seed_archive_work,
            local,
            snapshot_limits,
            snapshot_source: None,
            maintenance_at: 0,
            admission_at: 0,
            admission_waiters: VecDeque::new(),
        };
        driver.run(requests, controls, closes).await;
        let result = driver.shutdown().await;
        if let Ok(mut target) = terminal.result.lock() {
            *target = Some(result);
        }
        terminal.changed.notify_waiters();
    })?;
    Ok(RaftCells { handle })
}

fn ltx_host() -> cellar_ltx::LtxHost {
    let fs = crate::asyncrt::fs();
    let age = fs.clone();
    let read = fs.clone();
    cellar_ltx::LtxHost::new(
        crate::asyncrt::wall_ms,
        move |path| {
            Ok(std::time::Duration::from_millis(
                crate::asyncrt::wall_ms()
                    .saturating_sub(age.metadata(path)?.modified_unix_millis)
                    .max(0) as u64,
            ))
        },
        move |path| {
            let fs = read.clone();
            async move {
                crate::asyncrt::try_blocking(move || fs.read(&path))
                    .map_err(|e| std::io::Error::other(e.to_string()))?
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?
            }
        },
        |job| async move {
            crate::asyncrt::try_blocking(job)
                .map_err(|e| cellar_ltx::HostTaskError::new(e.to_string()))?
                .await
                .map_err(|e| cellar_ltx::HostTaskError::new(e.to_string()))
        },
    )
    .with_filesystem(fs)
}

fn read_services() -> ReadServices {
    ReadServices {
        now_unix_millis: Arc::new(crate::asyncrt::wall_ms),
        fill_random: Arc::new(|bytes| crate::asyncrt::rng("raft-image-read").fill_bytes(bytes)),
    }
}

fn progress(
    group: &GroupLog,
    image: Option<&ImageView>,
    archived: LogIndex,
) -> Result<Option<ReplicaProgress>, RuntimeError> {
    let cache = group.read()?;
    if cache.applied.image.as_ref() != image.map(|image| &image.image) {
        return Ok(None);
    }
    let mut progress = ReplicaProgress::checkpoint(
        group.identity().clone(),
        cache.applied.position.index,
        archived,
    )
    .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
    progress
        .persist(LogIndex::new(cache.last_index()))
        .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
    progress
        .commit(LogIndex::new(cache.hard_state.commit))
        .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
    Ok(Some(progress))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    AwaitAdmission,
    Start,
    AwaitStart,
    Staging,
    BeforeSubmit,
    AwaitSubmit,
    Marking,
    Proposing,
    Waiting,
    Finish,
    AwaitFinish,
    Discard,
    Retiring,
}

struct Pending {
    allow_new: bool,
    // Unknown until the callback's physical preview is verified. Keep this
    // bound after proposal submission takes `prepared`, through proof/reply.
    archive_bytes: Option<u64>,
    // Only transient admission deferral is timed here, until grant/refusal.
    // Dropping a cancelled/retired waiter records abandonment, never a grant.
    admission_wait: Option<super::performance::Timer>,
    system: bool,
    proof: Option<(LogPosition, [u8; 32])>,
    request: Request,
    options: client::CommandOptions,
    contract: crate::application::consensus::OperationContract,
    input: Option<Vec<u8>>,
    identity: Option<CommandIdentity>,
    prepared: Option<PreparedCommand>,
    result: Option<CommittedReceipt>,
    duplicate: bool,
    phase: Phase,
    _bytes: Option<Permit>,
}

struct Cell {
    extents: Option<cellar_ltx::internal::NodeExtentStore>,
    birth: GroupBirth,
    route_revision: LogIndex,
    configuration: Option<membership::PendingConfiguration>,
    retired: bool,
    archive: CellArchive,
    archived: LogIndex,
    archive_retained: admission::Retained,
    archive_retry_at: u64,
    group: GroupLog,
    schema_digest: [u8; 32],
    worker: usize,
    core: CommandState,
    image: Option<ImageView>,
    pending: Option<Pending>,
    sequence: u64,
    status_busy: bool,
    prefer_delivery: bool,
    sql_busy: bool,
    apply: Option<RuntimeEvent>,
    publication: Option<ApplyTicket>,
    apply_ticket: Option<ApplyTicket>,
    apply_retry_at: u64,
    failed: bool,
    incoming: Option<snapshots::Incoming>,
    source_request: Option<snapshots::SourceRequest>,
    apply_inflight: bool,
    checkpoint_busy: bool,
    checkpoint_retry_at: u64,
}

enum StatusPurpose {
    Start,
    Submit,
    Finish,
    Poll,
    Observe(Box<Request>),
}

enum SqlPurpose {
    EffectScan(u64),
    Seed(ReadId),
    Checkpoint,
    SnapshotSource(Box<::raft::prelude::Message>),
    SnapshotPrepare(ApplyTicket),
    SnapshotPublish(ApplyTicket),
    CaptureArchive,
    Stage,
    Mark,
    Discard,
    Retire,
    Prepare(RuntimeEvent),
    Publish(ApplyTicket),
    Query(ReadId),
}

enum Job {
    FleetMovementServiceStopped,
    FleetRestartServiceStopped,
    FleetAction(Box<fleet_execution::Completed>),
    PeerFleetStop(Box<fleet_stop::Completed>),
    PeerFleetInspection(Box<fleet_inspection::PeerCompleted>),
    LocalFleetInspection(Box<fleet_inspection::LocalCompleted>),
    FleetAdmission(Box<fleet_admission::Completed>),
    NodeInspection(Box<node_inspection::Completed>),
    HistoryRegistered(Box<registration::Completed>),
    Drained(Box<drain::Completed>),
    Effect(Box<effect_scheduler::Completed>),
    Controller(Box<controller::Completed>),
    LeaderLoad(Box<load::Completed>),
    Inspection(Box<inspection::Completed>),
    PeerInspection(Box<inspection::PeerCompleted>),
    Directory(Box<directory::Observation>),
    Standby(Box<standby::Completed>),
    Leadership {
        request: Box<Request>,
        result: Result<Response, RuntimeError>,
    },
    SeedArchive {
        id: ReadId,
        result: Box<Result<Option<super::archive::ArchiveMetadata>, ArchiveError>>,
    },
    Configured {
        scope: GroupScope,
        result: Result<AcceptedProposal, RuntimeError>,
    },
    SnapshotSent {
        lease: u64,
        result: Result<(), super::transport::TransportError>,
    },
    SnapshotFetched {
        ticket: ApplyTicket,
        result: Box<
            Result<super::transport::snapshots::ReceivedSnapshot, super::transport::TransportError>,
        >,
    },
    SnapshotAuthorized {
        ticket: ApplyTicket,
        result: Result<(), RuntimeError>,
    },
    SnapshotAbort {
        ticket: ApplyTicket,
        result: Result<(), RuntimeError>,
    },
    Checkpoint {
        scope: GroupScope,
        result: Result<(), RuntimeError>,
    },
    Archived(Vec<super::host_archive::Completed>),
    ArchiveBatchJoined,
    Routed {
        request: Box<Request>,
        result: Result<super::query_route::RoutedOutput, CommandFailure>,
    },
    ReadStarted {
        id: ReadId,
        attempt: u64,
        result: Result<(), RuntimeError>,
    },
    ReadSource {
        id: ReadId,
        result: Result<GroupStatus, RuntimeError>,
    },
    ReadLeader {
        id: ReadId,
        check: u64,
        result: Result<GroupStatus, RuntimeError>,
    },
    Status {
        scope: GroupScope,
        sequence: u64,
        purpose: StatusPurpose,
        result: Result<GroupStatus, RuntimeError>,
    },
    Sql {
        scope: GroupScope,
        purpose: SqlPurpose,
        result: Box<Result<sql::Output, ExecutionError>>,
        work: Option<Arc<client::Work>>,
    },
    SnapshotEncoded {
        scope: GroupScope,
        purpose: SqlPurpose,
        result: Box<Result<sql::Output, ExecutionError>>,
    },
    Proposed {
        scope: GroupScope,
        identity: CommandIdentity,
        result: Result<AcceptedProposal, RuntimeError>,
    },
    Installed {
        scope: GroupScope,
        result: Result<(), RuntimeError>,
    },
    Hint,
    ExtentGc(anyhow::Result<usize>),
}

struct Driver {
    maintenance_at: u64,
    admission_at: u64,
    admission_waiters: VecDeque<GroupScope>,
    drain: Option<drain::Drain>,
    effect_scheduler: Option<effect_scheduler::Scheduler>,
    controller: Option<controller::Controller>,
    leader_load: Option<load::Scanner>,
    directory: Option<directory::Directory>,
    standbys: BTreeMap<GroupScope, standby::Dormant>,
    declared_namespaces: Vec<NamespaceRegistration>,
    history_registration: Option<Arc<client::AttemptControl>>,
    directory_path: PathBuf,
    activating: Option<GroupScope>,
    peers: BTreeMap<NodeId, RaftPeerNode>,
    label: String,
    snapshot_limits: super::snapshot::SnapshotLimits,
    snapshot_source: Option<super::transport::snapshots::SnapshotSource>,
    archive_owner: Option<super::host_archive::Owner>,
    extents: Option<extents::Owner>,
    local: Arc<client::Handle>,
    log: super::storage::ReplicaLog,
    runtime: RaftRuntime,
    events: RuntimeEvents,
    transport: RaftTransport,
    pool: Pool,
    cells: BTreeMap<GroupScope, Cell>,
    active: BTreeSet<GroupScope>,
    applying: BTreeSet<GroupScope>,
    uncertain: BTreeMap<GroupScope, CommandId>,
    jobs: FuturesUnordered<BoxFuture<'static, Job>>,
    options: RaftHostOptions,
    rng: crate::asyncrt::rng::Stream,
    closing: Arc<AtomicBool>,
    versions: super::version::VersionKeys,
    reads: BTreeMap<ReadId, PendingRead>,
    read_nonce: [u8; 16],
    read_sequence: u64,
    budget: Arc<Budget>,
    control_budget: Arc<Budget>,
    seed_archive_work: Arc<Budget>,
}

fn reply(request: &mut Request, result: Result<Response, CommandFailure>) {
    reply_charged(request, result, None);
}

fn reply_charged(
    request: &mut Request,
    result: Result<Response, CommandFailure>,
    bytes: Option<Permit>,
) {
    if let Some(reply) = request.reply.take() {
        let _ = reply.send(result.map(|response| client::ChargedResponse {
            response,
            _bytes: bytes,
        }));
    }
}

fn failure(error: ExecutionError) -> CommandFailure {
    match error {
        ExecutionError::Refused(failure) => failure,
        ExecutionError::Registration(_) => CommandFailure::UnsupportedCapability,
        _ => CommandFailure::Unavailable,
    }
}

impl Driver {
    fn finish_read(&mut self, id: &ReadId, result: Result<Response, CommandFailure>) {
        if let Some(mut read) = self.reads.remove(id) {
            if result.is_err() {
                read.request.control.cancel();
            }
            if let Some(timer) = read.archive_wait.take() {
                timer.finish(result.is_ok());
            }
            reply(&mut read.request, result);
        }
    }

    fn pump_reads(&mut self) {
        let now = crate::asyncrt::mono_ms();
        for id in self.reads.keys().copied().collect::<Vec<_>>() {
            let Some(read) = self.reads.get_mut(&id) else {
                continue;
            };
            let scope = read.request.scope.clone();
            let Some(cell) = self.cells.get(&scope) else {
                self.finish_read(&id, Err(CommandFailure::Unavailable));
                continue;
            };
            if read.expired() || cell.failed || cell.retired {
                self.finish_read(&id, Err(CommandFailure::VersionUnavailable));
                continue;
            }
            if cell.incoming.is_some() {
                continue;
            }
            if now < read.retry_at {
                continue;
            }
            match read.phase {
                ReadPhase::Source => {
                    read.phase = ReadPhase::Probing;
                    let runtime = self.runtime.clone();
                    self.jobs.push(Box::pin(async move {
                        Job::ReadSource {
                            id,
                            result: runtime.status(&scope).await,
                        }
                    }));
                }
                ReadPhase::StartBarrier => {
                    let Some(check) = read.leader_check.checked_add(1) else {
                        self.finish_read(&id, Err(CommandFailure::Overloaded));
                        continue;
                    };
                    read.leader_check = check;
                    read.phase = ReadPhase::CheckingLeader;
                    let runtime = self.runtime.clone();
                    self.jobs.push(Box::pin(async move {
                        Job::ReadLeader {
                            id,
                            check,
                            result: runtime.status(&scope).await,
                        }
                    }));
                }
                ReadPhase::Barrier if !read.checking_leader => {
                    let Some(check) = read.leader_check.checked_add(1) else {
                        self.finish_read(&id, Err(CommandFailure::Overloaded));
                        continue;
                    };
                    read.leader_check = check;
                    read.checking_leader = true;
                    let runtime = self.runtime.clone();
                    self.jobs.push(Box::pin(async move {
                        Job::ReadLeader {
                            id,
                            check,
                            result: runtime.status(&scope).await,
                        }
                    }));
                }
                ReadPhase::Image => {
                    if matches!(&read.operation, super::query::ReadOperation::Fleet { .. }) {
                        self.finish_fleet_observation(id);
                        continue;
                    }
                    let metadata_only = matches!(
                        &read.operation,
                        super::query::ReadOperation::Seed {
                            kind: super::transport::seeds::SeedKind::S3,
                            ..
                        }
                    );
                    if !metadata_only
                        && (cell.sql_busy || cell.apply.is_some() || cell.publication.is_some())
                    {
                        continue;
                    }
                    if let super::query::ReadOperation::Seed {
                        target,
                        kind,
                        charge,
                    } = &read.operation
                    {
                        let Some(Minimum::ReadIndex(barrier)) = read.minimum.as_ref() else {
                            self.finish_read(&id, Err(CommandFailure::CorruptState));
                            continue;
                        };
                        if !seed::valid_target(cell, target, &self.peers) {
                            self.finish_read(&id, Err(CommandFailure::Unavailable));
                            continue;
                        }
                        let ready = !cell.apply_inflight
                            && cell.group.read().is_ok_and(|cache| {
                                cache.applied.position.index >= *barrier
                                    && cache.applied.image.as_ref()
                                        == cell.image.as_ref().map(|image| &image.image)
                            });
                        if !ready {
                            continue;
                        }
                        if *kind == super::transport::seeds::SeedKind::S3 {
                            // One owner spans all metadata retries and waiting
                            // for the archive to cover this exact ReadIndex.
                            read.archive_wait.get_or_insert_with(|| {
                                self.local
                                    .performance
                                    .timer(super::performance::Operation::RecoveryArchiveWait)
                            });
                            let archive = cell.archive.clone().with_read_retention(charge.clone());
                            let Some(work) = self.seed_archive_work.acquire(0) else {
                                self.finish_read(&id, Err(CommandFailure::Overloaded));
                                continue;
                            };
                            read.phase = ReadPhase::Executing;
                            self.jobs.push(Box::pin(async move {
                                // Ownership survives expiry/removal of the read waiter.
                                let _work = work;
                                Job::SeedArchive {
                                    id,
                                    result: Box::new(archive.recovery_metadata().await),
                                }
                            }));
                            continue;
                        }
                        let reservation = match self.transport.reserve_snapshot_source() {
                            Ok(reservation) => reservation,
                            Err(_) => {
                                self.finish_read(&id, Err(CommandFailure::Overloaded));
                                continue;
                            }
                        };
                        read.phase = ReadPhase::Executing;
                        self.sql(
                            &scope,
                            SqlPurpose::Seed(id),
                            Action::CaptureSeed {
                                limits: self.snapshot_limits,
                                reservation,
                            },
                        );
                        continue;
                    }
                    let Some(image) = cell.image.as_ref() else {
                        continue;
                    };
                    let candidate = cell.group.read().ok().and_then(|cache| {
                        (cache.applied.image.as_ref() == Some(&image.image)).then(|| {
                            CommittedVersion {
                                scope: scope.clone(),
                                position: cache.applied.position,
                            }
                        })
                    });
                    let Some(candidate) = candidate else {
                        continue;
                    };
                    let Some(minimum) = read.minimum.clone() else {
                        self.finish_read(&id, Err(CommandFailure::CorruptState));
                        continue;
                    };
                    match minimum.satisfied(&candidate, &scope) {
                        Ok(false) => continue,
                        Err(error) => {
                            self.finish_read(&id, Err(error));
                            continue;
                        }
                        Ok(true) => (),
                    }
                    read.phase = ReadPhase::Executing;
                    let super::query::ReadOperation::Query { contract, input } = &read.operation
                    else {
                        continue;
                    };
                    let action = Action::Query {
                        contract: contract.clone(),
                        input: input.clone(),
                        minimum,
                        control: read.request.control.clone(),
                        source_deadline: read.source_deadline,
                    };
                    self.sql(&scope, SqlPurpose::Query(id), action);
                }
                _ => (),
            }
        }
    }

    fn on_read_source(&mut self, id: ReadId, result: Result<GroupStatus, RuntimeError>) {
        let Some(read) = self.reads.get_mut(&id) else {
            return;
        };
        if read.phase != ReadPhase::Probing {
            return;
        }
        let now = crate::asyncrt::mono_ms();
        match result {
            Err(RuntimeError::NotReady | RuntimeError::Overloaded) => {
                read.phase = ReadPhase::Source;
                read.retry_at = now.saturating_add(self.options.poll_ms);
            }
            Ok(status)
                if self
                    .cells
                    .get(&read.request.scope)
                    .is_some_and(|cell| status.replica == *cell.group.identity()) =>
            {
                if let Some(deadline) = super::query::recent_source_deadline(
                    status.source_observed_ms,
                    now,
                    self.options.stale_source_timeout_ms,
                ) {
                    read.source_deadline = Some(deadline);
                    read.phase = ReadPhase::Image;
                } else {
                    self.finish_read(&id, Err(CommandFailure::VersionUnavailable));
                }
            }
            _ => self.finish_read(&id, Err(CommandFailure::VersionUnavailable)),
        }
    }

    fn on_read_leader(
        &mut self,
        id: ReadId,
        check: u64,
        result: Result<GroupStatus, RuntimeError>,
    ) {
        let Some(read) = self.reads.get_mut(&id) else {
            return;
        };
        if check != read.leader_check
            || !matches!(read.phase, ReadPhase::CheckingLeader | ReadPhase::Barrier)
        {
            return;
        }
        read.checking_leader = false;
        read.retry_at = crate::asyncrt::mono_ms().saturating_add(self.options.poll_ms);
        let status = match result {
            Ok(status)
                if self
                    .cells
                    .get(&read.request.scope)
                    .is_some_and(|cell| status.replica == *cell.group.identity()) =>
            {
                status
            }
            Err(RuntimeError::NotReady | RuntimeError::Overloaded) => {
                if read.phase == ReadPhase::CheckingLeader {
                    read.phase = ReadPhase::StartBarrier;
                }
                return;
            }
            _ => {
                self.finish_read(&id, Err(CommandFailure::VersionUnavailable));
                return;
            }
        };
        let leader = (status.term, status.leader, status.is_leader);
        if read.phase == ReadPhase::Barrier {
            // RawNode may clear a pending read on role change, or its reply
            // may be lost without any leadership change. Retry the same
            // invocation after a checked observation and bounded delay. The
            // original deadline remains fixed; status never supplies proof.
            if read.leader != Some(leader) || crate::asyncrt::mono_ms() >= read.barrier_retry_at {
                read.phase = ReadPhase::StartBarrier;
            }
            return;
        }
        if status.leader.is_none()
            || status.applied.term != status.term
            || (status.is_leader && !status.protocol_ready)
        {
            read.phase = ReadPhase::StartBarrier;
            return;
        }
        let Some(attempt) = read.barrier_attempt.checked_add(1) else {
            self.finish_read(&id, Err(CommandFailure::Overloaded));
            return;
        };
        read.barrier_attempt = attempt;
        read.leader = Some(leader);
        read.phase = ReadPhase::Barrier;
        read.barrier_retry_at = crate::asyncrt::mono_ms()
            .saturating_add(self.options.poll_ms.saturating_mul(4).clamp(100, 1000));
        let scope = read.request.scope.clone();
        let runtime = self.runtime.clone();
        self.jobs.push(Box::pin(async move {
            Job::ReadStarted {
                id,
                attempt,
                result: runtime.read_index(&scope, id.to_vec()).await,
            }
        }));
    }

    fn status(&mut self, scope: &GroupScope, purpose: StatusPurpose) {
        let Some(cell) = self.cells.get_mut(scope) else {
            return;
        };
        if cell.status_busy {
            if let StatusPurpose::Observe(mut request) = purpose {
                reply(&mut request, Err(CommandFailure::Overloaded));
            }
            return;
        }
        let Some(sequence) = cell.sequence.checked_add(1) else {
            self.fail(scope);
            return;
        };
        cell.sequence = sequence;
        cell.status_busy = true;
        let runtime = self.runtime.clone();
        let scope = scope.clone();
        self.jobs.push(Box::pin(async move {
            let result = runtime.status(&scope).await;
            Job::Status {
                scope,
                sequence,
                purpose,
                result,
            }
        }));
    }

    fn sql(&mut self, scope: &GroupScope, purpose: SqlPurpose, action: Action) {
        let Some(cell) = self.cells.get_mut(scope) else {
            return;
        };
        cell.sql_busy = true;
        let work = match &purpose {
            SqlPurpose::Query(id) | SqlPurpose::Seed(id) => {
                self.reads.get(id).map(|read| read.request.work.clone())
            }
            _ => cell.pending.as_ref().map(|p| p.request.work.clone()),
        };
        let cancel = match &purpose {
            SqlPurpose::Query(id) | SqlPurpose::Seed(id) => {
                self.reads.get(id).map(|read| read.request.control.clone())
            }
            SqlPurpose::Stage => cell
                .pending
                .as_ref()
                .map(|pending| pending.request.control.clone()),
            _ => None,
        };
        let encoding_work = work.clone();
        let job = self
            .pool
            .call(cell.worker, scope.clone(), action, work, cancel);
        let scope = scope.clone();
        self.jobs.push(Box::pin(async move {
            Job::Sql {
                scope,
                purpose,
                result: Box::new(job.await),
                work: encoding_work,
            }
        }));
    }

    fn finish_pending(&mut self, scope: &GroupScope, result: Result<Response, CommandFailure>) {
        self.finish_pending_charged(scope, result, None);
    }

    fn finish_pending_charged(
        &mut self,
        scope: &GroupScope,
        result: Result<Response, CommandFailure>,
        bytes: Option<Permit>,
    ) {
        if let Some(cell) = self.cells.get_mut(scope) {
            if let Some(mut pending) = cell.pending.take() {
                let result = if pending.request.control.cancelled()
                    && (result.is_ok()
                        || pending.duplicate && matches!(result, Err(CommandFailure::Unavailable)))
                {
                    Err(
                        if pending.request.control.submitted() || pending.duplicate {
                            CommandFailure::Ambiguous {
                                command: pending.options.id,
                            }
                        } else {
                            CommandFailure::Unavailable
                        },
                    )
                } else {
                    result
                };
                reply_charged(&mut pending.request, result, bytes);
            }
        }
        self.active.remove(scope);
        self.admission_waiters.retain(|queued| queued != scope);
        self.admission_at = 0;
    }

    #[track_caller]
    fn fail(&mut self, scope: &GroupScope) {
        if let Some(cell) = self.cells.get_mut(scope) {
            if !cell.failed {
                tracing::error!(group = ?scope.group, location = %std::panic::Location::caller(), "Raft host closed group admission after a failure");
            }
            cell.failed = true;
            cell.core.close();
            if let Some(pending) = cell.pending.as_mut() {
                if pending.request.control.submitted() {
                    self.uncertain.insert(scope.clone(), pending.options.id);
                }
            }
        }
        let result = self
            .cells
            .get(scope)
            .and_then(|c| c.pending.as_ref())
            .map(|p| {
                if p.request.control.submitted() {
                    CommandFailure::Ambiguous {
                        command: p.options.id,
                    }
                } else {
                    CommandFailure::CorruptState
                }
            })
            .unwrap_or(CommandFailure::CorruptState);
        self.finish_pending(scope, Err(result));
    }

    fn refuse(&mut self, scope: &GroupScope, error: CommandFailure) {
        if let Some(pending) = self
            .cells
            .get_mut(scope)
            .and_then(|cell| cell.pending.as_mut())
        {
            if pending.request.control.submitted() {
                reply(
                    &mut pending.request,
                    Err(CommandFailure::Ambiguous {
                        command: pending.options.id,
                    }),
                );
                pending.phase = Phase::Waiting;
            } else {
                reply(&mut pending.request, Err(error));
                pending.phase = Phase::Discard;
            }
        }
    }

    fn on_routed_query(&mut self, mut request: Request) {
        let scope = request.scope.clone();
        let Ok(binding) = self
            .local
            .bindings
            .get(&(scope.namespace, scope.cell.clone()))
            .and_then(|binding| {
                if binding.scope == scope {
                    Ok(binding)
                } else {
                    Err(CommandFailure::CommandConflict)
                }
            })
        else {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        };
        let Kind::Query {
            contract,
            input,
            options,
        } = std::mem::replace(&mut request.kind, Kind::Status)
        else {
            reply(&mut request, Err(CommandFailure::UnsupportedCapability));
            return;
        };
        if contract.kind != OperationKind::Query {
            reply(&mut request, Err(CommandFailure::UnsupportedCapability));
            return;
        }
        let minimum = if let client::QueryConsistency::AtLeast(token) = &options.consistency {
            match u64::try_from(crate::asyncrt::wall_ms())
                .map_err(|_| super::version::VersionError::Expired)
                .and_then(|now| {
                    self.versions
                        .verify(token, &scope, binding.class.schema_digest(), now)
                }) {
                Ok(version) => Some(version),
                Err(error) => {
                    let failure = if error == super::version::VersionError::WrongHistory {
                        CommandFailure::WrongIncarnation
                    } else {
                        CommandFailure::VersionUnavailable
                    };
                    reply(&mut request, Err(failure));
                    return;
                }
            }
        } else {
            None
        };
        let context = super::query_route::RouteContext {
            member: binding.member(),
            leader_hint: binding.leader_hint,
            local: crate::application::consensus::RaftCellHandle {
                handle: self.local.clone(),
                binding,
            },
            runtime: self.runtime.clone(),
            transport: self.transport.clone(),
            keys: self.versions.clone(),
            control: request.control.clone(),
            minimum,
        };
        self.jobs.push(Box::pin(async move {
            Job::Routed {
                request: Box::new(request),
                result: context.run(contract, input, options).await,
            }
        }));
    }

    fn on_request(&mut self, mut request: Request) {
        self.reconcile_membership(&request.scope);
        let scope = request.scope.clone();
        if matches!(&request.kind, Kind::Inspect | Kind::InspectPeer(_)) {
            if self.closing.load(Ordering::Acquire) {
                reply(&mut request, Err(CommandFailure::Unavailable));
            } else if !request.control.cancelled() {
                match request.kind {
                    Kind::InspectPeer(peer) if peer != self.log.limits().node => {
                        self.start_peer_inspection(request, peer)
                    }
                    _ => self.start_inspection(request),
                }
            }
            return;
        }
        let system = matches!(&request.kind, Kind::SystemCommand { .. });
        let admission = if matches!(
            &request.kind,
            Kind::Command { .. } | Kind::SystemCommand { .. }
        ) {
            match self.archive_admission() {
                admission::Decision::Granted if !self.admission_waiters.is_empty() => {
                    admission::Decision::Deferred
                }
                decision => decision,
            }
        } else {
            admission::Decision::Refused
        };
        if self.closing.load(Ordering::Acquire)
            || self
                .cells
                .get(&scope)
                .is_some_and(|cell| cell.failed || cell.retired)
        {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        }
        if request.control.cancelled() {
            return;
        }
        if matches!(
            &request.kind,
            Kind::ActivateLearner(_) | Kind::ActivateManaged { .. } | Kind::ActivateInitial(_)
        ) {
            match std::mem::replace(&mut request.kind, Kind::Status) {
                Kind::ActivateLearner(activation) => {
                    self.activate_learner(request, activation, None)
                }
                Kind::ActivateManaged {
                    activation,
                    authorization,
                } => self.activate_learner(request, activation, Some(*authorization)),
                Kind::ActivateInitial(authorization) => {
                    self.activate_initial(request, *authorization)
                }
                _ => reply(&mut request, Err(CommandFailure::CorruptState)),
            }
            return;
        }
        if matches!(&request.kind, Kind::Seed { .. }) {
            if let Kind::Seed {
                target,
                kind,
                charge,
            } = std::mem::replace(&mut request.kind, Kind::Status)
            {
                self.start_seed(request, target, kind, charge);
            }
            return;
        }
        if matches!(&request.kind, Kind::Query { options, .. } if !matches!(options.routing, client::QueryRouting::Local))
        {
            self.on_routed_query(request);
            return;
        }
        if matches!(
            &request.kind,
            Kind::BeginRepair { .. } | Kind::AdvanceRepair { .. } | Kind::Fleet(_)
        ) {
            match std::mem::replace(&mut request.kind, Kind::Status) {
                Kind::BeginRepair { plan } => self.begin_repair(request, plan, None),
                Kind::AdvanceRepair {
                    id,
                    step,
                    expected_revision,
                } => self.advance_repair(request, id, step, expected_revision, None),
                Kind::Fleet(action) => self.fleet_configuration(request, *action),
                _ => reply(&mut request, Err(CommandFailure::CorruptState)),
            }
            return;
        }
        if matches!(&request.kind, Kind::AddLearner { .. }) {
            if let Kind::AddLearner {
                member,
                expected_revision,
            } = std::mem::replace(&mut request.kind, Kind::Status)
            {
                self.add_learner(request, member, expected_revision);
            }
            return;
        }
        let Some(cell) = self.cells.get_mut(&scope) else {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        };
        if cell.failed || self.closing.load(Ordering::Acquire) {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        }
        if cell.incoming.is_some()
            && matches!(
                &request.kind,
                Kind::Command { .. } | Kind::SystemCommand { .. }
            )
        {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        }
        if request.control.cancelled() {
            return;
        }
        match std::mem::replace(&mut request.kind, Kind::Status) {
            Kind::ReplicationStatus => {
                let runtime = self.runtime.clone();
                self.jobs.push(Box::pin(async move {
                    let result = runtime
                        .replication_status(&scope)
                        .await
                        .map(Response::Replication);
                    Job::Leadership {
                        request: Box::new(request),
                        result,
                    }
                }));
            }
            Kind::TransferLeadership(transfer) => {
                if cell.pending.is_some() || cell.configuration.is_some() || cell.incoming.is_some()
                {
                    reply(&mut request, Err(CommandFailure::Unavailable));
                    return;
                }
                if !request.control.begin_submission() {
                    return;
                }
                let runtime = self.runtime.clone();
                self.jobs.push(Box::pin(async move {
                    let result = if request.control.cancelled() {
                        Err(RuntimeError::Closed)
                    } else {
                        runtime
                            .transfer_leadership(&scope, transfer)
                            .await
                            .map(|()| Response::LeadershipRequested)
                    };
                    Job::Leadership {
                        request: Box::new(request),
                        result,
                    }
                }));
            }
            Kind::RestartLeadership(mut check) => {
                if check.transfer.is_some()
                    && (cell.pending.is_some()
                        || cell.configuration.is_some()
                        || cell.incoming.is_some())
                {
                    reply(&mut request, Err(CommandFailure::Unavailable));
                    return;
                }
                if !request.control.begin_submission() {
                    return;
                }
                check.deadline = check.deadline.min(request.control.deadline());
                let runtime = self.runtime.clone();
                self.jobs.push(Box::pin(async move {
                    let result = if request.control.cancelled() {
                        Err(RuntimeError::Closed)
                    } else {
                        runtime
                            .restart_leadership(&scope, check)
                            .await
                            .map(|()| Response::LeadershipRequested)
                    };
                    Job::Leadership {
                        request: Box::new(request),
                        result,
                    }
                }));
            }
            Kind::Inspect
            | Kind::InspectPeer(_)
            | Kind::AddLearner { .. }
            | Kind::ActivateLearner(_)
            | Kind::ActivateManaged { .. }
            | Kind::ActivateInitial(_)
            | Kind::Fleet(_)
            | Kind::Seed { .. }
            | Kind::BeginRepair { .. }
            | Kind::AdvanceRepair { .. } => {
                reply(&mut request, Err(CommandFailure::UnsupportedCapability))
            }
            Kind::Status => self.status(&scope, StatusPurpose::Observe(Box::new(request))),
            Kind::RepairStatus => {
                let result = cell
                    .group
                    .membership()
                    .map_err(|_| CommandFailure::CorruptState)
                    .and_then(|membership| {
                        membership
                            .repair_status()
                            .map_err(|_| CommandFailure::CorruptState)
                    })
                    .map(Response::Repair);
                reply(&mut request, result);
            }
            Kind::FleetStatus => {
                let result = cell
                    .group
                    .membership()
                    .map(|membership| Response::Fleet(membership.fleet_status().map(Box::new)))
                    .map_err(|_| CommandFailure::CorruptState);
                reply(&mut request, result);
            }
            Kind::ObserveFleet(authorization) => {
                self.start_fleet_observation(request, *authorization);
            }
            Kind::Query {
                contract,
                input,
                options,
            } => {
                if contract.kind != OperationKind::Query {
                    reply(&mut request, Err(CommandFailure::UnsupportedCapability));
                    return;
                }
                let minimum = if let client::QueryConsistency::AtLeast(token) = &options.consistency
                {
                    let decoded = u64::try_from(crate::asyncrt::wall_ms())
                        .map_err(|_| super::version::VersionError::Expired)
                        .and_then(|now| {
                            self.versions.verify(token, &scope, cell.schema_digest, now)
                        });
                    match decoded {
                        Ok(version) => Some(version),
                        Err(error) => {
                            let failure = if error == super::version::VersionError::WrongHistory {
                                CommandFailure::WrongIncarnation
                            } else {
                                CommandFailure::VersionUnavailable
                            };
                            reply(&mut request, Err(failure));
                            return;
                        }
                    }
                } else {
                    None
                };
                let Some(sequence) = self.read_sequence.checked_add(1) else {
                    reply(&mut request, Err(CommandFailure::Overloaded));
                    return;
                };
                self.read_sequence = sequence;
                let mut id = [0; 32];
                id[..8].copy_from_slice(b"CLRRD001");
                id[8..24].copy_from_slice(&self.read_nonce);
                id[24..].copy_from_slice(&sequence.to_be_bytes());
                self.reads.insert(
                    id,
                    PendingRead::new(request, contract, input, &options, minimum),
                );
            }
            Kind::Command {
                contract,
                input,
                options,
            }
            | Kind::SystemCommand {
                contract,
                input,
                options,
            } => {
                // Background SQL does not consume the one business-command slot.
                // Keep the bounded request until the physical worker completes;
                // pump() rechecks its original deadline before staging.
                if cell.pending.is_some() {
                    reply(&mut request, Err(CommandFailure::Overloaded));
                    return;
                }
                cell.pending = Some(Pending {
                    allow_new: admission == admission::Decision::Granted,
                    archive_bytes: None,
                    admission_wait: (admission == admission::Decision::Deferred).then(|| {
                        self.local
                            .performance
                            .timer(super::performance::Operation::ArchiveAdmissionWait)
                    }),
                    system,
                    proof: None,
                    request,
                    options,
                    contract,
                    input: Some(input),
                    identity: None,
                    prepared: None,
                    result: None,
                    duplicate: false,
                    phase: if admission == admission::Decision::Deferred {
                        Phase::AwaitAdmission
                    } else {
                        Phase::Start
                    },
                    _bytes: None,
                });
                let sql_idle = !cell.sql_busy;
                self.active.insert(scope.clone());
                if admission == admission::Decision::Deferred {
                    self.admission_waiters.push_back(scope);
                } else if sql_idle {
                    self.status(&scope, StatusPurpose::Start);
                }
            }
        }
    }

    fn on_status(
        &mut self,
        scope: GroupScope,
        sequence: u64,
        purpose: StatusPurpose,
        result: Result<GroupStatus, RuntimeError>,
    ) {
        let Some(cell) = self.cells.get_mut(&scope) else {
            return;
        };
        cell.status_busy = false;
        let accepted = result.and_then(|status| {
            if status.replica != *cell.group.identity()
                || cell.group.read()?.hard_state.term > status.term.get()
            {
                return Err(RuntimeError::NotReady);
            }
            if let Some(progress) = progress(&cell.group, cell.image.as_ref(), cell.archived)? {
                cell.core
                    .observe_progress(progress)
                    .map_err(|_| RuntimeError::StaleCompletion)?;
            }
            let change = cell
                .core
                .observe_leadership(status.leadership_observation(sequence)?)
                .map_err(|_| RuntimeError::StaleCompletion)?;
            if change.retired.is_some() {
                if let Some(pending) = cell.pending.as_mut() {
                    let error = if pending.request.control.submitted() {
                        CommandFailure::Ambiguous {
                            command: pending.options.id,
                        }
                    } else {
                        CommandFailure::NotLeader {
                            leader: status.leader.and_then(|id| MemberId::new(id).ok()),
                        }
                    };
                    reply(&mut pending.request, Err(error));
                }
            }
            Ok(status)
        });
        if let StatusPurpose::Observe(mut request) = purpose {
            let result = accepted.and_then(|status| {
                let now = self
                    .log
                    .domain()
                    .mono_ms()
                    .map_err(|_| RuntimeError::Closed)?;
                let registered_queries = self
                    .local
                    .bindings
                    .get(&(scope.namespace, scope.cell.clone()))
                    .map_err(|_| RuntimeError::Closed)?
                    .class
                    .has_queries();
                let (role, queries) = readiness::capture(
                    cell,
                    &status,
                    now,
                    self.options.stale_source_timeout_ms,
                    self.local.admission.is_draining(),
                    registered_queries,
                )?;
                Ok(Response::Status(client::CellStatus {
                    role,
                    queries,
                    leader: status.leader.and_then(|id| MemberId::new(id).ok()),
                    term: status.term,
                    command_ready: role == crate::application::consensus::RaftReplicaRole::Leader
                        && cell.pending.is_none()
                        && cell.incoming.is_none()
                        && cell.core.authority().is_some()
                        && status.protocol_ready
                        && !status.awaiting_apply
                        && !status.awaiting_ready
                        && status.persisted == status.committed
                        && status.committed == status.applied.index,
                    version: cell.image.as_ref().map(|image| CommittedVersion {
                        scope: scope.clone(),
                        position: image.position,
                    }),
                    persisted: status.persisted,
                    committed: status.committed,
                    applied: status.applied,
                    checkpoint: status.checkpoint,
                    archived: cell.archived,
                    membership_revision: status.membership_revision,
                    member_high_water: status.member_high_water,
                }))
            });
            reply(
                &mut request,
                result.map_err(|_| CommandFailure::Unavailable),
            );
            return;
        }
        let status = match accepted {
            Ok(status) => status,
            Err(_) => {
                if matches!(purpose, StatusPurpose::Start | StatusPurpose::Submit) {
                    self.refuse(&scope, CommandFailure::Unavailable);
                }
                return;
            }
        };
        let Some(pending) = cell.pending.as_mut() else {
            return;
        };
        if pending.request.control.cancelled() && !pending.request.control.submitted() {
            pending.phase = Phase::Discard;
            return;
        }
        match purpose {
            StatusPurpose::Start => {
                // A reader or archive capture may acquire SQL while status is
                // in flight. Keep one bounded command and recheck authority
                // after that work physically completes.
                if cell.sql_busy {
                    pending.phase = Phase::Start;
                    return;
                }
                let Some(authority) = cell.core.authority().cloned() else {
                    if status.is_leader {
                        pending.phase = Phase::AwaitStart;
                        return;
                    }
                    self.refuse(
                        &scope,
                        CommandFailure::NotLeader {
                            leader: status.leader.and_then(|id| MemberId::new(id).ok()),
                        },
                    );
                    return;
                };
                let mut bytes = [0; 16];
                self.rng.fill_bytes(&mut bytes);
                let Ok(proposal) = ProposalId::new(bytes) else {
                    self.refuse(&scope, CommandFailure::Unavailable);
                    return;
                };
                let identity = match cell.core.admit(
                    &authority,
                    pending.options.id,
                    proposal,
                    pending.options.durability,
                ) {
                    Ok(identity) => identity,
                    Err(Refusal::LeaderNotReady) => {
                        pending.phase = Phase::AwaitStart;
                        return;
                    }
                    Err(_) => {
                        self.refuse(&scope, CommandFailure::Unavailable);
                        return;
                    }
                };
                pending.identity = Some(identity.clone());
                let Some(input) = pending.input.take() else {
                    self.refuse(&scope, CommandFailure::Unavailable);
                    return;
                };
                let mut preview = [0; 16];
                self.rng.fill_bytes(&mut preview);
                let Ok(preview) = ImageId::new(preview) else {
                    self.refuse(&scope, CommandFailure::Unavailable);
                    return;
                };
                let durability = match cell.core.receipt_durability(&identity) {
                    Ok(value) => value,
                    Err(_) => {
                        self.refuse(&scope, CommandFailure::Unavailable);
                        return;
                    }
                };
                let mut random = [0; 32];
                self.rng.fill_bytes(&mut random);
                let attempt = CommandAttempt {
                    request: CommandRequest {
                        id: pending.options.id,
                        operation: pending.contract.operation.clone(),
                        contract_digest: pending.contract.digest,
                        request_digest: command_request_digest(&input),
                        durability,
                        retry_until_ms: pending.options.retry_until_ms,
                    },
                    proposal,
                    preview,
                    inputs: crate::storage::CommandInputs {
                        now_unix_millis: crate::asyncrt::wall_ms().max(0) as u64,
                        random,
                    },
                };
                pending.phase = Phase::Staging;
                let allow_new = pending.allow_new;
                let system = pending.system;
                self.sql(
                    &scope,
                    SqlPurpose::Stage,
                    Action::Stage {
                        identity,
                        status: Box::new(status),
                        attempt,
                        input,
                        allow_new,
                        system,
                    },
                );
            }
            StatusPurpose::Submit => {
                let Some(identity) = pending.identity.clone() else {
                    self.refuse(&scope, CommandFailure::Unavailable);
                    return;
                };
                if !pending.request.control.begin_submission() {
                    pending.phase = Phase::Discard;
                    return;
                }
                if let Err(reason) = cell.core.mark_submitted(&identity) {
                    pending.request.control.refuse_preflight();
                    if reason == Refusal::LeaderNotReady {
                        // Ready persistence or protocol-only work may be in
                        // flight between two status requests. Keep the frozen
                        // private result; retry the authority check on a tick,
                        // under the original attempt deadline, before any RPC.
                        tracing::debug!(?reason, "Raft private submission preflight deferred");
                        pending.phase = Phase::AwaitSubmit;
                        return;
                    }
                    self.refuse(&scope, CommandFailure::Unavailable);
                    return;
                }
                pending.phase = Phase::Marking;
                self.sql(&scope, SqlPurpose::Mark, Action::Submit { identity });
            }
            StatusPurpose::Finish => {
                let (Some(identity), Some(receipt)) =
                    (pending.identity.as_ref(), pending.result.as_ref())
                else {
                    return;
                };
                let result = if pending.duplicate {
                    cell.core.recover_receipt(
                        identity,
                        receipt.receipt().command(),
                        CommittedVersion {
                            scope: scope.clone(),
                            position: receipt.position(),
                        },
                        receipt.receipt().minimum_durability(),
                    )
                } else {
                    cell.core.verify_receipt(
                        identity,
                        receipt.receipt().command(),
                        receipt.position(),
                        receipt.receipt().minimum_durability(),
                    )
                };
                if let Err(reason) = result {
                    if reason == Refusal::LeaderNotReady {
                        pending.phase = Phase::AwaitFinish;
                        return;
                    }
                    self.refuse(&scope, CommandFailure::Unavailable);
                    return;
                }
                if cell.archived >= receipt.position().index {
                    if let Some((position, entry_digest)) = pending.proof {
                        if cell
                            .core
                            .observe_proof(&OutputProof::Raft(Box::new(RaftProof {
                                command: identity.clone(),
                                position,
                                entry_digest,
                                stage: ProofStage::Archived,
                            })))
                            .is_err()
                        {
                            self.fail(&scope);
                            return;
                        }
                    }
                }
                match cell.core.finish(identity) {
                    Ok(permit) => {
                        let token = u64::try_from(crate::asyncrt::wall_ms())
                            .ok()
                            .and_then(|now| {
                                self.versions
                                    .issue(permit.version(), cell.schema_digest, now)
                                    .ok()
                            });
                        let Some(version_token) = token else {
                            let id = permit.command();
                            self.finish_pending(
                                &scope,
                                Err(CommandFailure::Ambiguous { command: id }),
                            );
                            return;
                        };
                        let Some(bytes) = self.budget.acquire(receipt.receipt().result().len())
                        else {
                            let id = permit.command();
                            self.finish_pending(
                                &scope,
                                Err(CommandFailure::Ambiguous { command: id }),
                            );
                            return;
                        };
                        let response = client::CommandReceipt {
                            id: permit.command(),
                            version: permit.version().clone(),
                            durability: permit.durability(),
                            result: receipt.receipt().result().to_vec(),
                            version_token,
                        };
                        self.finish_pending_charged(
                            &scope,
                            Ok(Response::Command(response)),
                            Some(bytes),
                        );
                    }
                    Err(Refusal::Unproven) => pending.phase = Phase::AwaitFinish,
                    Err(_) => self.refuse(&scope, CommandFailure::Unavailable),
                }
            }
            StatusPurpose::Poll => {
                if let Some(identity) = pending.identity.clone() {
                    if identity.authority.term < status.term {
                        if let Ok(barrier) = cell.core.begin_retirement(&identity) {
                            pending.phase = Phase::Retiring;
                            self.sql(
                                &scope,
                                SqlPurpose::Retire,
                                Action::Retire { identity, barrier },
                            );
                        }
                        return;
                    }
                }
                if pending.result.is_some() {
                    pending.phase = Phase::Finish;
                }
            }
            StatusPurpose::Observe(_) => (),
        }
    }

    fn on_sql(
        &mut self,
        scope: GroupScope,
        purpose: SqlPurpose,
        result: Result<sql::Output, ExecutionError>,
        work: Option<Arc<client::Work>>,
    ) {
        let result = match result {
            Ok(sql::Output {
                value:
                    Value::SnapshotCapture {
                        capture,
                        reservation,
                    },
                _bytes,
            }) => {
                let Some(cell) = self.cells.get_mut(&scope) else {
                    return;
                };
                cell.sql_busy = false;
                if !matches!(
                    &purpose,
                    SqlPurpose::Seed(_) | SqlPurpose::Checkpoint | SqlPurpose::SnapshotSource(_)
                ) {
                    self.fail(&scope);
                    return;
                }
                self.encode_snapshot(scope, purpose, capture, reservation, work, _bytes);
                return;
            }
            result => result,
        };
        if let SqlPurpose::EffectScan(sequence) = purpose {
            self.finish_effect_scan(scope, sequence, result);
            return;
        }
        if let SqlPurpose::Seed(id) = purpose {
            self.seed_sql(scope, id, result);
            return;
        }
        if matches!(
            &purpose,
            SqlPurpose::Checkpoint
                | SqlPurpose::SnapshotSource(_)
                | SqlPurpose::SnapshotPrepare(_)
                | SqlPurpose::SnapshotPublish(_)
        ) {
            self.on_snapshot_sql(scope, purpose, result);
            return;
        }
        let Some(cell) = self.cells.get_mut(&scope) else {
            return;
        };
        cell.sql_busy = false;
        // A scan while this cell was busy cannot predict when SQL finishes.
        // Wake archive selection for newly idle retained work instead of
        // waiting a maintenance interval and missing the collection window.
        if !matches!(purpose, SqlPurpose::CaptureArchive)
            && cell
                .group
                .applied()
                .is_ok_and(|applied| applied.position.index > cell.archived)
        {
            if let Some(owner) = self.archive_owner.as_mut() {
                if !owner.inflight.contains(&scope) {
                    owner.scan_at = 0;
                }
            }
        }
        if let SqlPurpose::CaptureArchive = purpose {
            let Some(owner) = self.archive_owner.as_mut() else {
                return;
            };
            if self.closing.load(Ordering::Acquire) {
                owner.inflight.remove(&scope);
                return;
            }
            match result {
                Ok(sql::Output {
                    value: Value::ArchiveCut(Some(cut)),
                    _bytes,
                }) => {
                    if owner.pending.is_empty() {
                        owner.publish_at = crate::asyncrt::mono_ms()
                            .saturating_add(super::archive::bundle::COALESCE_MS);
                    }
                    owner.pending.push(super::host_archive::Captured {
                        scope,
                        archive: cell.archive.clone(),
                        cut: *cut,
                        reservation: _bytes,
                    });
                }
                _ => {
                    owner.inflight.remove(&scope);
                    owner.scan_at = 0;
                    cell.archive_retry_at =
                        crate::asyncrt::mono_ms().saturating_add(self.options.archive.retry_ms);
                }
            }
            return;
        }
        if let SqlPurpose::Query(id) = purpose {
            let Some(mut read) = self.reads.remove(&id) else {
                return;
            };
            if read.expired() {
                read.request.control.cancel();
                reply(&mut read.request, Err(CommandFailure::VersionUnavailable));
                return;
            }
            if matches!(
                &result,
                Err(ExecutionError::Refused(CommandFailure::VersionUnavailable))
            ) && !self.closing.load(Ordering::Acquire)
            {
                read.phase = ReadPhase::Image;
                read.retry_at = crate::asyncrt::mono_ms().saturating_add(self.options.poll_ms);
                self.reads.insert(id, read);
                return;
            }
            let mut bytes = None;
            let result = match result {
                Ok(sql::Output {
                    value: Value::Queried(value),
                    _bytes,
                }) => {
                    bytes = _bytes;
                    if read.minimum.as_ref().is_some_and(|minimum| {
                        minimum.satisfied(&value.version, &scope) == Ok(true)
                    }) {
                        u64::try_from(crate::asyncrt::wall_ms())
                            .ok()
                            .and_then(|now| {
                                self.versions
                                    .issue(&value.version, cell.schema_digest, now)
                                    .ok()
                            })
                            .map(|version_token| {
                                Response::Query(client::QueryResult {
                                    result: value.result,
                                    version: value.version,
                                    version_token,
                                    served_by: cell.group.identity().member,
                                })
                            })
                            .ok_or(CommandFailure::VersionUnavailable)
                    } else {
                        Err(CommandFailure::CorruptState)
                    }
                }
                Err(error) => Err(failure(error)),
                _ => Err(CommandFailure::CorruptState),
            };
            if !self.closing.load(Ordering::Acquire) {
                reply_charged(&mut read.request, result, bytes);
            }
            return;
        }
        let sql::Output { value, _bytes } = match result {
            Ok(result) => result,
            Err(error) => {
                tracing::debug!(event = "raft_sql_failure", cell = %scope.cell,
                    operation = match &purpose { SqlPurpose::Stage => "stage", SqlPurpose::Prepare(_) => "apply", SqlPurpose::Publish(_) => "publish", _ => "lifecycle" },
                    error = ?error);
                if let (
                    SqlPurpose::Prepare(event),
                    ExecutionError::Refused(CommandFailure::Overloaded),
                ) = (&purpose, &error)
                {
                    let Some(retry) = event.clone_apply() else {
                        self.fail(&scope);
                        return;
                    };
                    cell.apply = Some(retry);
                    cell.apply_retry_at =
                        crate::asyncrt::mono_ms().saturating_add(self.options.poll_ms);
                    self.applying.insert(scope);
                    return;
                }
                if matches!(
                    purpose,
                    SqlPurpose::Discard | SqlPurpose::Prepare(_) | SqlPurpose::Publish(_)
                ) {
                    tracing::error!(group = ?scope.group, ?error, "Raft image operation failed");
                    self.fail(&scope);
                } else {
                    self.refuse(&scope, failure(error));
                }
                return;
            }
        };
        match (purpose, value) {
            (SqlPurpose::Stage, Value::Staged(outcome)) => {
                self.admission_at = 0;
                if let Some(pending) = cell.pending.as_mut() {
                    pending._bytes = _bytes;
                    match outcome {
                        StageOutcome::Prepared(prepared) => {
                            pending.archive_bytes = Some(prepared.archive_bytes());
                            pending.prepared = Some(prepared);
                            pending.phase = Phase::BeforeSubmit;
                        }
                        StageOutcome::Duplicate(receipt) => {
                            pending.request.control.observe_committed();
                            pending.result = Some(receipt);
                            pending.duplicate = true;
                            pending.phase = Phase::Finish;
                        }
                    }
                }
            }
            (SqlPurpose::Mark, Value::Submitted) => {
                let Some(pending) = cell.pending.as_mut() else {
                    return;
                };
                let (Some(prepared), Some(identity)) =
                    (pending.prepared.take(), pending.identity.clone())
                else {
                    self.fail(&scope);
                    return;
                };
                pending.phase = Phase::Proposing;
                let runtime = self.runtime.clone();
                self.jobs.push(Box::pin(async move {
                    let result = runtime
                        .propose(&scope, prepared.context(), prepared.bytes().to_vec())
                        .await;
                    Job::Proposed {
                        scope,
                        identity,
                        result,
                    }
                }));
            }
            (SqlPurpose::Discard, Value::Discarded) => {
                if let Some(identity) = cell.pending.as_ref().and_then(|p| p.identity.as_ref()) {
                    if cell.core.discard_completed(identity).is_err() {
                        self.fail(&scope);
                        return;
                    }
                }
                self.finish_pending(&scope, Err(CommandFailure::Unavailable));
            }
            (SqlPurpose::Retire, Value::Retired(barrier)) => {
                if let Some(identity) = cell
                    .pending
                    .as_ref()
                    .and_then(|pending| pending.identity.as_ref())
                {
                    if cell.core.retirement_completed(identity, barrier).is_err() {
                        self.fail(&scope);
                        return;
                    }
                }
                self.finish_pending(&scope, Err(CommandFailure::Unavailable));
            }
            (SqlPurpose::Prepare(_), Value::Prepared(completion)) => {
                cell.apply_ticket = Some(completion.ticket.clone());
                let runtime = self.runtime.clone();
                self.jobs.push(Box::pin(async move {
                    Job::Installed {
                        scope,
                        result: runtime.complete_apply(completion).await,
                    }
                }));
            }
            (SqlPurpose::Publish(ticket), Value::Published { view, receipt }) => {
                cell.image = Some(view);
                // The runtime can deliver the next committed entry while this
                // image is being published. Publication consumed the scheduler
                // slot, so restore it for that queued entry after SQL joins.
                cell.apply_inflight = cell.apply.is_some();
                if cell.apply_inflight {
                    self.applying.insert(scope.clone());
                }
                cell.apply_ticket = None;
                // New durable work invalidates an earlier empty inventory.
                // Keep per-cell failure backoff and the single producer; only
                // idle scanning should wait for the maintenance interval.
                if let Some(owner) = &mut self.archive_owner {
                    owner.scan_at = 0;
                }
                if let Ok(Some(progress)) =
                    progress(&cell.group, cell.image.as_ref(), cell.archived)
                {
                    let _ = cell.core.observe_progress(progress);
                }
                if let Some((identity, receipt)) = receipt {
                    if cell.pending.as_ref().and_then(|p| p.identity.as_ref()) == Some(&identity) {
                        let proven = cell
                            .core
                            .bind_entry(&identity, ticket.position(), ticket.entry_digest())
                            .and_then(|()| {
                                for stage in [
                                    ProofStage::Committed,
                                    ProofStage::Applied,
                                    ProofStage::Queryable,
                                ] {
                                    cell.core.observe_proof(&OutputProof::Raft(Box::new(
                                        RaftProof {
                                            command: identity.clone(),
                                            entry_digest: ticket.entry_digest(),
                                            position: ticket.position(),
                                            stage,
                                        },
                                    )))?;
                                }
                                Ok(())
                            });
                        if proven.is_ok() {
                            if let Some(pending) = cell.pending.as_mut() {
                                pending.request.control.observe_committed();
                                pending.proof = Some((ticket.position(), ticket.entry_digest()));
                                pending.result = Some(receipt);
                                pending.phase = Phase::Finish;
                            }
                        } else {
                            self.refuse(&scope, CommandFailure::Unavailable);
                        }
                    }
                }
            }
            _ => self.fail(&scope),
        }
    }

    fn on_event(&mut self, event: RuntimeEvent) {
        match &event.kind {
            RuntimeEventKind::Message { replica, message } => {
                if message.get_msg_type() == ::raft::prelude::MessageType::MsgSnapshot {
                    self.send_snapshot(&replica.scope, message.clone());
                    return;
                }
                if self
                    .transport
                    .submit(&replica.scope, message.clone())
                    .is_err()
                {
                    let runtime = self.runtime.clone();
                    let scope = replica.scope.clone();
                    let member = message.to;
                    let snapshot =
                        message.get_msg_type() == ::raft::prelude::MessageType::MsgSnapshot;
                    self.jobs.push(Box::pin(async move {
                        let _ = runtime.admit_unreachable(&scope, member).await;
                        if snapshot {
                            let _ = runtime.admit_snapshot_status(&scope, member, false).await;
                        }
                        Job::Hint
                    }));
                }
            }
            RuntimeEventKind::Apply { ticket, .. } => {
                let scope = ticket.replica().scope.clone();
                if let Some(cell) = self.cells.get_mut(&scope) {
                    if cell.apply.is_some() {
                        self.fail(&scope);
                    } else {
                        cell.apply_inflight = true;
                        cell.apply = Some(event);
                        self.applying.insert(scope);
                    }
                }
            }
            RuntimeEventKind::Applied { ticket, .. } => {
                let scope = ticket.replica().scope.clone();
                if let Some(cell) = self.cells.get_mut(&scope) {
                    let protocol = cell.group.read().ok().is_some_and(|cache| {
                        cache
                            .entries
                            .get(&ticket.position().index.get())
                            .is_some_and(|entry| {
                                (entry.get_entry_type() != raft::prelude::EntryType::EntryNormal
                                    || entry.data.is_empty())
                                    && entry.index == ticket.position().index.get()
                                    && entry.term == ticket.position().term.get()
                                    && super::completion::entry_digest(entry).ok()
                                        == Some(ticket.entry_digest())
                            })
                    });
                    if protocol && ticket.replica() == cell.group.identity() {
                        if cell.image.is_some() {
                            if let Some(owner) = &mut self.archive_owner {
                                owner.scan_at = 0;
                            }
                        }
                    } else if cell.apply_ticket.as_ref() == Some(ticket) {
                        cell.publication = Some(ticket.clone());
                        self.applying.insert(scope);
                    } else {
                        self.fail(&scope);
                    }
                }
            }
            RuntimeEventKind::Failed { replica, error } => {
                tracing::error!(group = ?replica.scope.group, ?error, "Raft runtime failed");
                self.fail(&replica.scope);
            }
            RuntimeEventKind::Snapshot {
                ticket,
                snapshot,
                source,
            } => self.receive_snapshot(ticket.clone(), snapshot.clone(), *source),
            RuntimeEventKind::SnapshotInstalled { ticket, .. } => self.snapshot_installed(ticket),
            RuntimeEventKind::SnapshotAborted { ticket, .. } => self.snapshot_aborted(ticket),
            RuntimeEventKind::Checkpointed { ticket } => {
                if let Some(cell) = self.cells.get_mut(&ticket.replica().scope) {
                    cell.checkpoint_busy = false;
                }
            }
            RuntimeEventKind::ReadIndex {
                replica,
                index,
                context,
            } => {
                let Ok(id) = <ReadId>::try_from(context.as_slice()) else {
                    return;
                };
                let Some(read) = self.reads.get_mut(&id) else {
                    return;
                };
                if read.request.scope == replica.scope
                    && self
                        .cells
                        .get(&replica.scope)
                        .is_some_and(|cell| replica == cell.group.identity())
                    && matches!(
                        read.phase,
                        ReadPhase::StartBarrier | ReadPhase::CheckingLeader | ReadPhase::Barrier
                    )
                {
                    if index.get() == 0 {
                        self.finish_read(&id, Err(CommandFailure::VersionUnavailable));
                    } else {
                        read.minimum = Some(Minimum::ReadIndex(*index));
                        read.phase = ReadPhase::Image;
                    }
                }
            }
            _ => (),
        }
    }

    fn pump(&mut self) {
        self.pump_directory();
        self.pump_admission();
        // Inventory scans are node maintenance, not per-frame work. Repeating
        // them for every heartbeat makes N groups cost O(N²) per tick.
        let now = crate::asyncrt::mono_ms();
        if now >= self.maintenance_at {
            self.maintenance_at = now.saturating_add(self.options.poll_ms);
            self.pump_extent_gc();
            self.pump_membership();
            self.pump_snapshots();
        }
        for scope in self.applying.clone() {
            let Some(cell) = self.cells.get_mut(&scope) else {
                continue;
            };
            if cell.failed {
                self.applying.remove(&scope);
                continue;
            }
            if cell
                .incoming
                .as_ref()
                .is_some_and(|incoming| !incoming.allows_apply())
            {
                continue;
            }
            if cell.sql_busy || cell.apply_retry_at > crate::asyncrt::mono_ms() {
                continue;
            }
            if let Some(ticket) = cell.publication.take() {
                self.sql(
                    &scope,
                    SqlPurpose::Publish(ticket.clone()),
                    Action::Publish { ticket },
                );
                self.applying.remove(&scope);
            } else if let Some(event) = cell.apply.take() {
                let mut bytes = [0; 16];
                self.rng.fill_bytes(&mut bytes);
                let Ok(image) = ImageId::new(bytes) else {
                    self.fail(&scope);
                    continue;
                };
                let Some(retry) = event.clone_apply() else {
                    self.fail(&scope);
                    continue;
                };
                self.sql(
                    &scope,
                    SqlPurpose::Prepare(retry),
                    Action::Prepare { event, image },
                );
                self.applying.remove(&scope);
            }
        }
        for scope in self.active.clone() {
            let Some(cell) = self.cells.get_mut(&scope) else {
                continue;
            };
            if cell.failed || cell.sql_busy || cell.status_busy || cell.incoming.is_some() {
                continue;
            }
            let Some(pending) = cell.pending.as_mut() else {
                self.active.remove(&scope);
                continue;
            };
            if pending.request.control.cancelled() && !pending.request.control.submitted() {
                pending.phase = Phase::Discard;
            }
            match pending.phase {
                Phase::Start => self.status(&scope, StatusPurpose::Start),
                Phase::BeforeSubmit => self.status(&scope, StatusPurpose::Submit),
                Phase::Finish => self.status(&scope, StatusPurpose::Finish),
                Phase::Discard => {
                    if let Some(identity) = pending.identity.clone() {
                        self.sql(&scope, SqlPurpose::Discard, Action::Discard { identity });
                    } else {
                        self.finish_pending(&scope, Err(CommandFailure::Unavailable));
                    }
                }
                _ => (),
            }
        }
        self.pump_archive();
        self.pump_reads();
        self.pump_leader_load();
        self.pump_controller();
        self.publish_controller_resources();
        self.pump_effects();
        self.pump_node_drain();
    }

    fn pump_archive(&mut self) {
        if self.closing.load(Ordering::Acquire) {
            return;
        }
        let now = crate::asyncrt::mono_ms();
        // Already captured cuts must be published even when every slot is
        // occupied. A partial batch's deadline cannot be renewed by slow SQL.
        let publish = self.archive_owner.as_ref().is_some_and(|owner| {
            !owner.pending.is_empty()
                && (owner.pending.len()
                    >= self
                        .options
                        .archive
                        .max_batch_groups
                        .min(self.options.archive.max_producers)
                    || now >= owner.publish_at)
        });
        if publish {
            self.publish_archive_batch();
        }
        let Some(owner) = self.archive_owner.as_mut() else {
            return;
        };
        if owner.inflight.len() >= self.options.archive.max_producers {
            return;
        }
        if now < owner.scan_at {
            return;
        }
        let eligible = |cell: &Cell| {
            !cell.failed
                && !cell.sql_busy
                && cell.archive_retry_at <= now
                && cell
                    .group
                    .applied()
                    .is_ok_and(|applied| applied.position.index > cell.archived)
        };
        let mut captures = Vec::new();
        while owner.inflight.len() < self.options.archive.max_producers {
            let next = super::host_archive::next_scope(
                &self.cells,
                owner.cursor.as_ref(),
                |scope, cell| !owner.inflight.contains(scope) && eligible(cell),
            )
            .cloned();
            let Some(scope) = next else {
                owner.scan_at = now.saturating_add(self.options.poll_ms);
                break;
            };
            owner.inflight.insert(scope.clone());
            // Advance once per selected cell, independently of how completed
            // captures are partitioned into compatible upload batches.
            owner.cursor = Some(scope.clone());
            captures.push(scope);
        }
        let limits = self.options.archive.limits;
        for scope in captures {
            self.sql(
                &scope,
                SqlPurpose::CaptureArchive,
                Action::CaptureArchive { limits },
            );
        }
    }

    fn publish_archive_batch(&mut self) {
        let Some(owner) = self.archive_owner.as_mut() else {
            return;
        };
        while !owner.pending.is_empty() {
            let mut pending = vec![owner.pending.remove(0)];
            while pending.len() < self.options.archive.max_batch_groups {
                let next = owner
                    .pending
                    .iter()
                    .position(|candidate| pending[0].archive.can_batch_with(&candidate.archive));
                let Some(next) = next else {
                    break;
                };
                pending.push(owner.pending.remove(next));
            }
            // Each cut has one bounded completion channel. The physical batch
            // remains in jobs through joined completion, while a finished cell
            // can release its own slot without waiting for another history.
            let mut senders = Vec::with_capacity(pending.len());
            for capture in &pending {
                let scope = capture.scope.clone();
                let position = capture.cut.position();
                let (send, receive) = tokio::sync::oneshot::channel();
                senders.push(Some(send));
                self.jobs.push(Box::pin(async move {
                    let completed = receive.await.unwrap_or(super::host_archive::Completed {
                        scope,
                        position,
                        result: Err(super::archive::ArchiveError::Unconfirmed),
                    });
                    Job::Archived(vec![completed])
                }));
            }
            let verifier = owner.verifier.clone();
            let prepared = self.transport.prepared_snapshot_cache();
            let timer = self
                .local
                .performance
                .timer(super::performance::Operation::ArchivePublicationBatch);
            self.jobs.push(Box::pin(async move {
                let succeeded = super::host_archive::publish(
                    pending,
                    verifier,
                    prepared,
                    move |number, completed| {
                        if let Some(sender) = senders.get_mut(number).and_then(Option::take) {
                            let _ = sender.send(completed);
                        }
                    },
                )
                .await;
                timer.finish(succeeded);
                Job::ArchiveBatchJoined
            }));
        }
    }

    fn on_job(&mut self, job: Job) {
        match job {
            Job::FleetMovementServiceStopped | Job::FleetRestartServiceStopped => (),
            Job::FleetAction(completed) => self.finish_fleet_action(*completed),
            Job::PeerFleetStop(completed) => self.finish_peer_fleet_stop(*completed),
            Job::FleetAdmission(completed) => self.finish_fleet_admission(*completed),
            Job::HistoryRegistered(completed) => self.finish_history_registration(*completed),
            Job::ExtentGc(result) => self.extent_gc_completed(result),
            Job::Drained(completed) => self.finish_node_drain(*completed),
            Job::Controller(completed) => self.finish_controller(*completed),
            Job::Effect(completed) => self.finish_effect(*completed),
            Job::LeaderLoad(completed) => self.finish_leader_load(*completed),
            Job::Inspection(completed) => self.finish_inspection(*completed),
            Job::PeerInspection(completed) => self.finish_peer_inspection(*completed),
            Job::NodeInspection(completed) => self.finish_node_inspection(*completed),
            Job::PeerFleetInspection(completed) => self.finish_peer_fleet_inspection(*completed),
            Job::LocalFleetInspection(completed) => self.finish_local_fleet_inspection(*completed),
            Job::Directory(observation) => self.on_directory(*observation),
            Job::Standby(completed) => self.on_standby_job(*completed),
            Job::Leadership {
                mut request,
                result,
            } => {
                let result = if request.control.cancelled() || self.closing.load(Ordering::Acquire)
                {
                    Err(CommandFailure::Unavailable)
                } else {
                    result.map_err(|error| match error {
                        RuntimeError::NotLeader => CommandFailure::NotLeader { leader: None },
                        RuntimeError::Overloaded | RuntimeError::NotReady => {
                            CommandFailure::Overloaded
                        }
                        _ => CommandFailure::Unavailable,
                    })
                };
                reply(&mut request, result);
            }
            Job::SeedArchive { id, result } => self.seed_archive(id, *result),
            Job::Configured { scope, result } => self.configured(scope, result),
            job @ (Job::SnapshotSent { .. }
            | Job::SnapshotFetched { .. }
            | Job::SnapshotAuthorized { .. }
            | Job::SnapshotAbort { .. }
            | Job::Checkpoint { .. }) => self.on_snapshot_job(job),
            Job::ArchiveBatchJoined => (),
            Job::Archived(completed) => {
                self.admission_at = 0;
                if let Some(owner) = self.archive_owner.as_mut() {
                    owner.scan_at = 0;
                    for result in &completed {
                        owner.inflight.remove(&result.scope);
                    }
                }
                for super::host_archive::Completed {
                    scope,
                    position,
                    result,
                } in completed
                {
                    use super::performance::Counter;
                    if let Err(error) = &result {
                        // Expected publication contention is common. Keep the
                        // typed cause available for diagnosis without warning
                        // on each advisory deferral or changing retry policy.
                        tracing::debug!(
                            event = "raft_archive_publication_failure",
                            node = ?self.log.limits().node,
                            group = ?scope.group,
                            index = position.index.get(),
                            error = ?error,
                            "Raft archive publication did not confirm the cut"
                        );
                    }
                    self.local.performance.increment(
                        if result.is_ok() {
                            Counter::ArchivePublicationCutSucceeded
                        } else {
                            Counter::ArchivePublicationCutFailed
                        },
                        1,
                    );
                    let Some(cell) = self.cells.get_mut(&scope) else {
                        if result.is_ok() {
                            self.local
                                .performance
                                .increment(Counter::ArchiveCoverageMissingCell, 1);
                        }
                        continue;
                    };
                    if result.is_ok() && position.index > cell.archived {
                        // The proof names the captured local cut, even if another
                        // replica has already archived a later cut of this history.
                        // Never import that replica's later applied progress.
                        match progress(&cell.group, cell.image.as_ref(), position.index) {
                            Ok(Some(progress)) => {
                                if cell.core.observe_progress(progress).is_ok() {
                                    self.local.performance.increment(
                                        Counter::ArchiveCoverageIndexAdvance,
                                        position.index.get() - cell.archived.get(),
                                    );
                                    self.local
                                        .performance
                                        .increment(Counter::ArchiveCoverageAdvanced, 1);
                                    cell.archived = position.index;
                                    // The confirmed cut resolves this wait. Re-run
                                    // the normal fresh authority/output checks now;
                                    // it is not itself permission to send a result.
                                    if let Some(pending) = cell.pending.as_mut() {
                                        if pending.phase == Phase::AwaitFinish
                                            && pending.result.as_ref().is_some_and(|receipt| {
                                                receipt.position().index <= cell.archived
                                            })
                                        {
                                            pending.phase = Phase::Finish;
                                        }
                                    }
                                } else {
                                    self.local
                                        .performance
                                        .increment(Counter::ArchiveCoverageRejected, 1);
                                }
                            }
                            Ok(None) => self
                                .local
                                .performance
                                .increment(Counter::ArchiveCoverageImagePending, 1),
                            Err(_) => self
                                .local
                                .performance
                                .increment(Counter::ArchiveCoverageRejected, 1),
                        }
                    } else if result.is_ok() {
                        self.local
                            .performance
                            .increment(Counter::ArchiveCoverageUnchanged, 1);
                    }
                    // A confirmed cut may already have newer work waiting. Keep it
                    // eligible for the next fair scan; back off failures or a proof
                    // that the local state machine could not accept.
                    let now = crate::asyncrt::mono_ms();
                    cell.archive_retry_at = if result.is_ok() && position.index <= cell.archived {
                        now
                    } else {
                        now.saturating_add(self.options.archive.retry_ms)
                    };
                }
            }
            Job::Routed {
                mut request,
                result,
            } => {
                if request.control.cancelled() || self.closing.load(Ordering::Acquire) {
                    reply(&mut request, Err(CommandFailure::VersionUnavailable));
                } else {
                    match result {
                        Ok(output) => reply_charged(
                            &mut request,
                            Ok(Response::Query(output.result)),
                            Some(output.bytes),
                        ),
                        Err(error) => reply(&mut request, Err(error)),
                    }
                }
            }
            Job::ReadSource { id, result } => self.on_read_source(id, result),
            Job::ReadLeader { id, check, result } => self.on_read_leader(id, check, result),
            Job::ReadStarted {
                id,
                attempt,
                result,
            } => {
                if let Some(read) = self.reads.get_mut(&id) {
                    // A barrier event can arrive before the RPC acknowledgement.
                    // That late reply cannot regress an executing read.
                    if read.phase == ReadPhase::Barrier && read.barrier_attempt == attempt {
                        match result {
                            Ok(()) => (),
                            Err(RuntimeError::NotReady | RuntimeError::Overloaded) => {
                                read.phase = ReadPhase::StartBarrier;
                                read.checking_leader = false;
                                read.retry_at =
                                    crate::asyncrt::mono_ms().saturating_add(self.options.poll_ms);
                            }
                            Err(_) => {
                                self.finish_read(&id, Err(CommandFailure::VersionUnavailable))
                            }
                        }
                    }
                }
            }
            Job::Status {
                scope,
                sequence,
                purpose,
                result,
            } => self.on_status(scope, sequence, purpose, result),
            Job::Sql {
                scope,
                purpose,
                result,
                work,
            } => self.on_sql(scope, purpose, *result, work),
            Job::SnapshotEncoded {
                scope,
                purpose,
                result,
            } => match purpose {
                SqlPurpose::Seed(id) => self.seed_encoded(scope, id, *result),
                SqlPurpose::Checkpoint | SqlPurpose::SnapshotSource(_) => {
                    self.on_snapshot_encoded(scope, purpose, *result)
                }
                _ => self.fail(&scope),
            },
            Job::Installed { scope, result } => {
                if let Err(error) = result {
                    tracing::error!(group = ?scope.group, ?error, "Raft durable apply completion failed");
                    self.fail(&scope);
                }
            }
            Job::Hint => (),
            Job::Proposed {
                scope,
                identity,
                result,
            } => {
                let Some(cell) = self.cells.get_mut(&scope) else {
                    return;
                };
                if cell.pending.as_ref().and_then(|p| p.identity.as_ref()) != Some(&identity) {
                    return;
                }
                // Publication or a newer-term retirement may finish before this
                // RPC reply. It cannot put that producer back into Waiting.
                let waiting = cell
                    .pending
                    .as_ref()
                    .is_some_and(|p| p.phase == Phase::Proposing);
                if waiting {
                    if let Some(pending) = cell.pending.as_mut() {
                        pending.phase = Phase::Waiting;
                    }
                }
                match result {
                    Ok(accepted) => {
                        if accepted.replica != identity.authority.replica {
                            self.fail(&scope);
                        } else {
                            // A retired gate may refuse this late observation.
                            // Only the verified committed receipt can release it.
                            let _ = cell.core.bind_entry(
                                &identity,
                                accepted.position,
                                accepted.entry_digest,
                            );
                        }
                    }
                    Err(_) if waiting => self.refuse(&scope, CommandFailure::Unavailable),
                    Err(_) => (),
                }
            }
        }
    }

    async fn run(
        &mut self,
        mut requests: mailbox::Receiver<Request>,
        mut controls: mailbox::Receiver<ControlRequest>,
        mut closes: mpsc::UnboundedReceiver<()>,
    ) {
        let mut requests_open = true;
        let mut controls_open = true;
        let mut closes_open = true;
        let Ok(now) = self.log.domain().mono_ms() else {
            return;
        };
        let mut poll_at = now.saturating_add(self.options.poll_ms);
        loop {
            if self.closing.load(Ordering::Acquire) {
                return;
            }
            // Continue an admitted command before another administrative
            // observer can occupy its serialized authority-status check.
            self.pump();
            // Give admitted control work a bounded turn even while business
            // traffic is continuously ready. The main select still services
            // business, completions and consensus between these small batches.
            for _ in 0..4 {
                match controls.try_recv() {
                    Ok(request) => self.on_control(request),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        controls_open = false;
                        break;
                    }
                }
            }
            self.pump();
            let wake_at = self
                .archive_owner
                .as_ref()
                .filter(|owner| !owner.pending.is_empty())
                .map_or(poll_at, |owner| poll_at.min(owner.publish_at));
            let Ok(timer) = self.log.domain().sleep_until(wake_at) else {
                return;
            };
            crate::asyncrt::select! {
                request=controls.recv(),if controls_open=>match request{Some(request)=>self.on_control(request),None=>controls_open=false},
                request=requests.recv(),if requests_open=>match request{Some(request)=>self.on_request(request),None=>requests_open=false},
                close=closes.recv(),if closes_open=>if close.is_some(){return;}else{closes_open=false;},
                event=self.events.recv()=>match event{Some(event)=>self.on_event(event),None=>return},
                job=self.jobs.next(),if !self.jobs.is_empty()=>if let Some(job)=job{self.on_job(job)},
                _=timer=>{let Ok(now)=self.log.domain().mono_ms() else{return;};
                    if now < poll_at { continue; }
                    poll_at=now.saturating_add(self.options.poll_ms);
                    for scope in self.active.clone(){
                        let purpose=self.cells.get(&scope).filter(|cell|!cell.status_busy).and_then(|cell|cell.pending.as_ref()).and_then(|pending|match pending.phase {
                            Phase::AwaitStart=>Some(StatusPurpose::Start),Phase::AwaitSubmit=>Some(StatusPurpose::Submit),Phase::AwaitFinish=>Some(StatusPurpose::Finish),Phase::Waiting=>Some(StatusPurpose::Poll),_=>None,
                        });
                        if let Some(purpose)=purpose {self.status(&scope,purpose);}
                    }},
            }
        }
    }

    async fn shutdown(&mut self) -> Result<client::RaftCloseReport, CommandFailure> {
        self.closing.store(true, Ordering::Release);
        RaftCells {
            handle: self.local.clone(),
        }
        .stop_fleet_movement_service();
        RaftCells {
            handle: self.local.clone(),
        }
        .stop_fleet_restart_service();
        if let Some(control) = self.history_registration.take() {
            control.cancel();
        }
        self.snapshot_source.take();
        if let Some(controller) = &self.controller {
            controller.stop();
        }
        self.close_configurations();
        if let Some(owner) = &mut self.archive_owner {
            let _ = owner.cancellation.send(true);
            owner.pending.clear();
        }
        for (_, mut read) in std::mem::take(&mut self.reads) {
            read.request.control.cancel();
            reply(&mut read.request, Err(CommandFailure::Unavailable));
        }
        let mut unfinished = self
            .uncertain
            .iter()
            .map(|(scope, id)| client::UnfinishedCommand {
                scope: scope.clone(),
                id: *id,
            })
            .collect::<Vec<_>>();
        for (scope, cell) in &mut self.cells {
            cell.core.close();
            if let Some(pending) = cell.pending.as_mut() {
                let submitted = pending.request.control.submitted();
                if submitted {
                    unfinished.push(client::UnfinishedCommand {
                        scope: scope.clone(),
                        id: pending.options.id,
                    });
                }
                let error = if submitted {
                    CommandFailure::Ambiguous {
                        command: pending.options.id,
                    }
                } else {
                    CommandFailure::Unavailable
                };
                reply(&mut pending.request, Err(error));
            }
        }
        self.pool.stop();
        let runtime = self.runtime.close().await;
        self.transport.close().await;
        while self.jobs.next().await.is_some() {}
        let sql = self.pool.join().await;
        self.archive_owner.take();
        let node = NodeIdentity {
            node: self.log.limits().node,
            disk: self.log.limits().disk,
            process: self.log.limits().process,
        };
        let instance = self.log.storage_instance();
        let observations = (|| -> Result<_, CommandFailure> {
            let mut groups = Vec::new();
            let mut groups_with_archive_lag = 0;
            let mut bytes = 0usize;
            for (scope, cell) in &self.cells {
                let cache = cell
                    .group
                    .read()
                    .map_err(|_| CommandFailure::CorruptState)?;
                let committed = LogIndex::new(cache.hard_state.commit);
                if cell.archived < committed {
                    groups_with_archive_lag += 1;
                }
                let charge = scope
                    .cell
                    .len()
                    .checked_add(2048)
                    .ok_or(CommandFailure::CorruptState)?;
                if groups.len() < self.options.close_report.max_groups
                    && bytes
                        .checked_add(charge)
                        .is_some_and(|n| n <= self.options.close_report.max_bytes)
                {
                    bytes += charge;
                    groups.push(super::operations::RaftGroupCloseReport {
                        scope: scope.clone(),
                        member: cell.group.identity().member,
                        committed,
                        applied: cache.applied.position,
                        archived: cell.archived,
                        retired: cell.retired,
                        failed: cell.failed,
                    });
                }
            }
            Ok((groups, groups_with_archive_lag))
        })();
        let groups_total = self
            .log
            .group_count()
            .map_err(|_| CommandFailure::CorruptState);
        let attached = self.cells.len();
        self.cells.clear();
        self.standbys.clear();
        let log = self.log.close().await;
        let closed_extents = if let Some(extents) = self.extents.take() {
            match self.log.domain().blocking(move || drop(extents)) {
                Ok(job) => job.await.is_ok(),
                Err(_) => false,
            }
        } else {
            true
        };
        if runtime.is_err() || sql.is_err() || log.is_err() || !closed_extents {
            return Err(CommandFailure::CorruptState);
        }
        if self.local.admission.is_poisoned() {
            return Err(CommandFailure::Unavailable);
        }
        let (groups, groups_with_archive_lag) = observations?;
        let groups_total = groups_total?;
        let groups_with_unknown_archive = groups_total
            .checked_sub(attached)
            .ok_or(CommandFailure::CorruptState)?;
        let omitted_groups = groups_total - groups.len();
        Ok(client::RaftCloseReport {
            node,
            storage_instance: instance.map_err(|_| CommandFailure::CorruptState)?,
            groups,
            groups_total,
            omitted_groups,
            groups_with_archive_lag,
            groups_with_unknown_archive,
            unfinished,
        })
    }
}
