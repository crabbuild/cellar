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
    query::{Minimum, PendingRead, Phase as ReadPhase, ReadId},
    runtime::{RaftRuntime, RuntimeEvents},
    transport::{GroupRoute, RaftPeer, RaftTransport},
};
use crate::application::consensus::client::{self, Binding, Kind, Request, Response};
use crate::application::consensus::{command_request_digest, OperationKind, RaftCells, RaftClass};
use cellar_logic::consensus::*;
use cellar_logic::raft_command::CommandState;
use cellar_ltx::generation::{GenerationLimits, ImageId, ReadServices};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use rand::RngCore;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::{mpsc, Notify};

#[path = "host_membership.rs"]
mod membership;
#[path = "host_seed.rs"]
mod seed;
#[path = "host_snapshot.rs"]
mod snapshots;

#[derive(Clone, Debug)]
pub struct RaftHostOptions {
    /// Enable native snapshot recovery and its larger image admission floor.
    pub enable_snapshots: bool,
    /// Applied entries between automatic checkpoints; zero disables compaction.
    pub checkpoint_entries: u64,
    pub archive: RaftArchiveOptions,
    pub max_groups: usize,
    pub blocking_workers: usize,
    pub worker_queue: usize,
    pub max_requests: usize,
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
            apply: super::archive::apply_limits(),
            ..Default::default()
        };
        Self {
            enable_snapshots: true,
            checkpoint_entries: 256,
            archive: RaftArchiveOptions::default(),
            max_groups: 128,
            blocking_workers: 2,
            worker_queue: 32,
            max_requests: 128,
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
        self.archive.validate()?;
        self.execution.validate()?;
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
            apply.decode.max_encoded_bytes >= snapshot_bound
                && apply.max_total_encoded_bytes >= snapshot_bound
                && self.archive.limits.max_object_bytes >= snapshot_bound,
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
                && (1..=4096).contains(&self.max_requests)
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
    /// Resume a stable joined member or seed a new committed learner from the
    /// specified trusted peer. A claimed member with lost state is refused.
    Joining {
        member: MemberId,
        source: MemberId,
        source_node: NodeId,
    },
    /// Seed a fresh learner from an exact S3 manifest selected by a live peer
    /// read barrier. Existing stable joined state resumes without reseeding.
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
    let Registrations {
        cells: registrations,
        peers,
        origins,
    } = registrations;
    options.validate()?;
    let snapshot_limits = service
        .transport
        .snapshot_limits(options.execution.max_image_bytes)?;
    anyhow::ensure!(
        !registrations.is_empty() && registrations.len() <= options.max_groups,
        "invalid registered group count"
    );
    let domain = service.log.domain();
    let budget = Budget::new(options.max_pending_bytes, options.max_requests);
    let mut pool = Pool::start(
        domain.clone(),
        options.blocking_workers,
        options.worker_queue,
        options.max_groups,
        budget.clone(),
        Budget::new(options.archive.limits.max_capture_bytes, 1),
    )?;
    let mut cells = BTreeMap::new();
    let mut bindings = BTreeMap::new();
    let mut archive_owner = None;
    let peer_count = peers.len();
    let peers: BTreeMap<_, _> = peers.into_iter().map(|peer| (peer.node, peer)).collect();
    let initialized = async {
        anyhow::ensure!(
            peer_count == peers.len() && peer_count <= service.transport.peer_limit(),
            "invalid peer discovery count"
        );
        let mut grants = BTreeSet::new();
        for origin in origins {
            anyhow::ensure!(
                grants.insert((origin.cluster, origin.namespace, origin.peer.node)),
                "duplicate namespace query origin"
            );
            service.transport.grant_query_origin(origin)?;
        }
        for (index, registration) in registrations.into_iter().enumerate() {
            let Registration {
                placement,
                namespace,
                birth,
                class,
            } = registration;
            namespace.require_mode(cellar_logic::NamespaceMode::Raft)?;
            anyhow::ensure!(
                namespace.config().schema_digest == class.schema_digest(),
                "registered schema disagrees with namespace"
            );
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
                        member: None,
                        leader_hint,
                    },
                );
                continue;
            }
            let scope = birth.scope().clone();
            anyhow::ensure!(
                !cells.contains_key(&scope)
                    && !bindings.contains_key(&(scope.namespace, scope.cell.clone())),
                "duplicate registered cell"
            );
            let worker = index % options.blocking_workers;
            let storage = sql::CellStorage {
                directory: directory
                    .join(hex::encode(scope.namespace.bytes()))
                    .join(hex::encode(scope.group.bytes()))
                    .join(hex::encode(scope.incarnation.bytes())),
                host: ltx_host(),
                services: read_services(),
                images: options.images,
                execution: options.execution,
            };
            let permit = match placement {
                RaftCellPlacement::Replica => {
                    namespace.bootstrap_member(&birth, &service.log).await?
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
                                let archive = namespace.cell_archive(
                                    &birth,
                                    storage.host.clone(),
                                    options.archive.limits,
                                )?;
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
                            service.log.create_seeded(*seed).await?;
                            namespace
                                .resume_joined(&birth, &service.log, member)
                                .await?
                                .ok_or_else(|| anyhow::anyhow!("seed state missing after fence"))?
                        }
                    }
                }
                RaftCellPlacement::QueryRouter { .. } => unreachable!("router handled above"),
            };
            let group = service.log.group(&scope)?;
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
            let progress = progress(&group, image.as_ref(), LogIndex::new(0))?
                .ok_or_else(|| anyhow::anyhow!("unverified recovered image"))?;
            let core = CommandState::new(progress, namespace.minimum_durability())?;
            service.transport.install_group(membership::route(
                &group,
                &birth,
                &peers,
                class.schema_digest(),
                &label,
                &service.transport,
            )?)?;
            bindings.insert(
                (scope.namespace, scope.cell.clone()),
                Binding {
                    scope: scope.clone(),
                    class: class.clone(),
                    minimum: namespace.minimum_durability(),
                    member: Some(group.identity().member),
                    leader_hint: None,
                },
            );
            cells.insert(
                scope.clone(),
                Cell {
                    birth: birth.clone(),
                    route_revision: group.membership()?.revision(),
                    configuration: None,
                    retired: false,
                    archive: namespace.cell_archive(&birth, ltx_host(), options.archive.limits)?,
                    archived: LogIndex::new(0),
                    archive_retry_at: 0,
                    group,
                    schema_digest: class.schema_digest(),
                    worker,
                    core,
                    image,
                    pending: None,
                    sequence: 0,
                    status_busy: false,
                    sql_busy: false,
                    apply: None,
                    publication: None,
                    apply_ticket: None,
                    apply_retry_at: 0,
                    failed: false,
                    incoming: None,
                    source_request: None,
                    apply_inflight: false,
                    checkpoint_busy: false,
                    checkpoint_retry_at: 0,
                },
            );
            permit.attach(&service.runtime).await?;
        }
        if !cells.is_empty() {
            let mut nonce = [0; 16];
            crate::asyncrt::rng("raft-archive-verifier").fill_bytes(&mut nonce);
            let owner = super::host_archive::Owner::open(
                directory.join("archive-verifier"),
                ltx_host(),
                ImageId::new(nonce)?,
                options.images.apply,
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
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = initialized {
        let _ = service.runtime.close().await;
        service.transport.close().await;
        let _ = pool.join().await;
        let _ = service.log.close().await;
        return Err(error);
    }
    let (sender, requests) = mpsc::channel(options.max_requests);
    let (close, closes) = mpsc::unbounded_channel();
    let closed = Arc::new(AtomicBool::new(false));
    let closing = Arc::new(AtomicBool::new(false));
    let terminal = Arc::new(client::Terminal {
        result: Mutex::new(None),
        changed: Notify::new(),
    });
    let handle = Arc::new(client::Handle {
        requests: sender,
        close,
        domain: domain.clone(),
        closed: closed.clone(),
        closing: closing.clone(),
        terminal: terminal.clone(),
        budget: budget.clone(),
        max_input_bytes: options.max_input_bytes,
        bindings,
        work: crate::asyncrt::services().application_work_registry(),
    });
    if let Err(error) = service.transport.install_query_gateway(handle.clone()) {
        let _ = service.runtime.close().await;
        service.transport.close().await;
        let _ = pool.join().await;
        let _ = service.log.close().await;
        return Err(error.into());
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
            peers,
            label,
            archive_owner,
            log,
            runtime,
            events,
            transport,
            pool,
            cells,
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
            local,
            snapshot_limits,
            snapshot_source: None,
        };
        driver.run(requests, closes).await;
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
    birth: GroupBirth,
    route_revision: LogIndex,
    configuration: Option<membership::PendingConfiguration>,
    retired: bool,
    archive: CellArchive,
    archived: LogIndex,
    archive_retry_at: u64,
    group: GroupLog,
    schema_digest: [u8; 32],
    worker: usize,
    core: CommandState,
    image: Option<ImageView>,
    pending: Option<Pending>,
    sequence: u64,
    status_busy: bool,
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
    Archived {
        scope: GroupScope,
        position: LogPosition,
        result: Result<(), ArchiveError>,
    },
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
}

struct Driver {
    peers: BTreeMap<NodeId, RaftPeerNode>,
    label: String,
    snapshot_limits: super::snapshot::SnapshotLimits,
    snapshot_source: Option<super::transport::snapshots::SnapshotSource>,
    archive_owner: Option<super::host_archive::Owner>,
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
                            let archive = cell.archive.clone().with_read_retention(charge.clone());
                            read.phase = ReadPhase::Executing;
                            self.jobs.push(Box::pin(async move {
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
                let deadline = status
                    .source_observed_ms
                    .filter(|at| *at <= now)
                    .and_then(|at| at.checked_add(self.options.stale_source_timeout_ms));
                if deadline.is_some_and(|deadline| now < deadline) {
                    read.source_deadline = deadline;
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
        let job = self
            .pool
            .call(cell.worker, scope.clone(), action, work, cancel);
        let scope = scope.clone();
        self.jobs.push(Box::pin(async move {
            Job::Sql {
                scope,
                purpose,
                result: Box::new(job.await),
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
    }

    fn fail(&mut self, scope: &GroupScope) {
        if let Some(cell) = self.cells.get_mut(scope) {
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
        let Some(binding) = self
            .local
            .bindings
            .get(&(scope.namespace, scope.cell.clone()))
            .cloned()
            .filter(|binding| binding.scope == scope)
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
            member: binding.member,
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
        let scope = request.scope.clone();
        let allow_new = matches!(&request.kind, Kind::Command { .. }) && self.archive_admission();
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
            Kind::BeginRepair { .. } | Kind::AdvanceRepair { .. }
        ) {
            match std::mem::replace(&mut request.kind, Kind::Status) {
                Kind::BeginRepair { plan } => self.begin_repair(request, plan),
                Kind::AdvanceRepair {
                    id,
                    step,
                    expected_revision,
                } => self.advance_repair(request, id, step, expected_revision),
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
        if cell.incoming.is_some() && matches!(&request.kind, Kind::Command { .. }) {
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
            Kind::AddLearner { .. }
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
            } => {
                if cell.pending.is_some() || cell.sql_busy || cell.status_busy {
                    reply(&mut request, Err(CommandFailure::Overloaded));
                    return;
                }
                cell.pending = Some(Pending {
                    allow_new,
                    proof: None,
                    request,
                    options,
                    contract,
                    input: Some(input),
                    identity: None,
                    prepared: None,
                    result: None,
                    duplicate: false,
                    phase: Phase::Start,
                    _bytes: None,
                });
                self.active.insert(scope.clone());
                self.status(&scope, StatusPurpose::Start);
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
            let result = accepted.map(|status| {
                Response::Status(client::CellStatus {
                    leader: status.leader.and_then(|id| MemberId::new(id).ok()),
                    term: status.term,
                    command_ready: cell.pending.is_none()
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
                })
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
                self.sql(
                    &scope,
                    SqlPurpose::Stage,
                    Action::Stage {
                        identity,
                        status: Box::new(status),
                        attempt,
                        input,
                        allow_new,
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
    ) {
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
        if let SqlPurpose::CaptureArchive = purpose {
            let Some(owner) = self.archive_owner.as_mut() else {
                return;
            };
            match result {
                Ok(sql::Output {
                    value: Value::ArchiveCut(Some(cut)),
                    _bytes,
                }) => {
                    let position = cut.position();
                    let archive = cell.archive.clone();
                    let verifier = owner.verifier.clone();
                    self.jobs.push(Box::pin(async move {
                        let _bytes = _bytes;
                        let result = archive.publish(*cut, &verifier).await.and_then(|set| {
                            if set.scope() != &scope
                                || set.position().index < position.index
                                || set.position().index == position.index
                                    && set.position().term != position.term
                            {
                                return Err(ArchiveError::Corrupt("archive output coverage"));
                            }
                            Ok(())
                        });
                        Job::Archived {
                            scope,
                            position,
                            result,
                        }
                    }));
                }
                _ => {
                    owner.busy = false;
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
                    self.fail(&scope);
                } else {
                    self.refuse(&scope, failure(error));
                }
                return;
            }
        };
        match (purpose, value) {
            (SqlPurpose::Stage, Value::Staged(outcome)) => {
                if let Some(pending) = cell.pending.as_mut() {
                    pending._bytes = _bytes;
                    match outcome {
                        StageOutcome::Prepared(prepared) => {
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
                cell.apply_inflight = false;
                cell.apply_ticket = None;
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
                    } else if cell.apply_ticket.as_ref() == Some(ticket) {
                        cell.publication = Some(ticket.clone());
                        self.applying.insert(scope);
                    } else {
                        self.fail(&scope);
                    }
                }
            }
            RuntimeEventKind::Failed { replica, .. } => self.fail(&replica.scope),
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
        self.pump_membership();
        self.pump_snapshots();
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
    }

    fn archive_admission(&self) -> bool {
        let reservation = self
            .options
            .execution
            .max_image_bytes
            .saturating_add(self.options.execution.envelope.max_payload_bytes as u64)
            .saturating_add(self.options.execution.envelope.max_header_bytes as u64)
            .saturating_add(1024);
        let mut bytes = reservation;
        let mut entries = 1u64;
        for cell in self.cells.values() {
            let Ok(cache) = cell.group.read() else {
                return false;
            };
            let after = cell.archived.get().max(1);
            if cache.applied.position.index > cell.archived {
                if let Some(image) = &cache.applied.image {
                    bytes = bytes.saturating_add(image.length);
                }
            }
            for (_, entry) in cache.entries.range(after.saturating_add(1)..) {
                bytes =
                    bytes.saturating_add(u64::from(protobuf::Message::compute_size(entry)) + 128);
                if entry.get_entry_type() == ::raft::prelude::EntryType::EntryNormal
                    && !entry.data.is_empty()
                {
                    entries = entries.saturating_add(1);
                }
            }
            if cell
                .pending
                .as_ref()
                .is_some_and(|pending| pending.allow_new)
            {
                bytes = bytes.saturating_add(reservation);
                entries = entries.saturating_add(1);
            }
            if bytes > self.options.archive.max_unarchived_bytes
                || entries > self.options.archive.max_unarchived_entries as u64
            {
                return false;
            }
        }
        bytes <= self.options.archive.max_unarchived_bytes
            && entries <= self.options.archive.max_unarchived_entries as u64
    }

    fn pump_archive(&mut self) {
        let Some(owner) = self.archive_owner.as_mut() else {
            return;
        };
        if owner.busy || self.closing.load(Ordering::Acquire) {
            return;
        }
        let now = crate::asyncrt::mono_ms();
        let eligible = |cell: &&Cell| {
            !cell.failed
                && !cell.sql_busy
                && cell.image.is_some()
                && cell.archive_retry_at <= now
                && cell
                    .group
                    .applied()
                    .is_ok_and(|applied| applied.position.index > cell.archived)
        };
        let next = self
            .cells
            .iter()
            .filter(|(_, cell)| eligible(cell))
            .find(|(scope, _)| owner.cursor.as_ref().is_none_or(|cursor| *scope > cursor))
            .or_else(|| self.cells.iter().find(|(_, cell)| eligible(cell)))
            .map(|(scope, _)| scope.clone());
        let Some(scope) = next else {
            return;
        };
        owner.busy = true;
        owner.cursor = Some(scope.clone());
        let limits = self.options.archive.limits;
        self.sql(
            &scope,
            SqlPurpose::CaptureArchive,
            Action::CaptureArchive { limits },
        );
    }

    fn on_job(&mut self, job: Job) {
        match job {
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
                        RuntimeError::Overloaded => CommandFailure::Overloaded,
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
            Job::Archived {
                scope,
                position,
                result,
            } => {
                if let Some(owner) = self.archive_owner.as_mut() {
                    owner.busy = false;
                }
                let Some(cell) = self.cells.get_mut(&scope) else {
                    return;
                };
                cell.archive_retry_at =
                    crate::asyncrt::mono_ms().saturating_add(self.options.archive.retry_ms);
                if result.is_ok() && position.index > cell.archived {
                    // The proof names the captured local cut, even if another
                    // replica has already archived a later cut of this history.
                    // Never import that replica's later applied progress.
                    if let Ok(Some(progress)) =
                        progress(&cell.group, cell.image.as_ref(), position.index)
                    {
                        if cell.core.observe_progress(progress).is_ok() {
                            cell.archived = position.index;
                        }
                    }
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
            } => self.on_sql(scope, purpose, *result),
            Job::Installed { scope, result } => {
                if result.is_err() {
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
        mut requests: mpsc::Receiver<Request>,
        mut closes: mpsc::UnboundedReceiver<()>,
    ) {
        let mut requests_open = true;
        let mut closes_open = true;
        let Ok(now) = self.log.domain().mono_ms() else {
            return;
        };
        let mut poll_at = now.saturating_add(self.options.poll_ms);
        loop {
            self.pump();
            let Ok(timer) = self.log.domain().sleep_until(poll_at) else {
                return;
            };
            crate::asyncrt::select! {
                request=requests.recv(),if requests_open=>match request{Some(request)=>self.on_request(request),None=>requests_open=false},
                close=closes.recv(),if closes_open=>if close.is_some(){return;}else{closes_open=false;},
                event=self.events.recv()=>match event{Some(event)=>self.on_event(event),None=>return},
                job=self.jobs.next(),if !self.jobs.is_empty()=>if let Some(job)=job{self.on_job(job)},
                _=timer=>{let Ok(now)=self.log.domain().mono_ms() else{return;};poll_at=now.saturating_add(self.options.poll_ms);
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
        self.snapshot_source.take();
        self.close_configurations();
        if let Some(owner) = &self.archive_owner {
            let _ = owner.cancellation.send(true);
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
        self.cells.clear();
        let log = self.log.close().await;
        if runtime.is_err() || sql.is_err() || log.is_err() {
            return Err(CommandFailure::CorruptState);
        }
        Ok(client::RaftCloseReport { unfinished })
    }
}
