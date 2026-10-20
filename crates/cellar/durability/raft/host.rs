//! Node-owned command producers and Raft/SQL effect dispatch. Waiters never own
//! a submitted producer, its private image or its progress through publication.
use super::{
    budget::{Budget, Permit},
    cache::GroupLog,
    catalog::*,
    completion::*,
    execution::*,
    host_sql::{self as sql, Action, ImageView, Pool, Value},
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
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::{mpsc, Notify};

#[derive(Clone, Debug)]
pub struct RaftHostOptions {
    pub max_groups: usize,
    pub blocking_workers: usize,
    pub worker_queue: usize,
    pub max_requests: usize,
    pub max_pending_bytes: usize,
    pub max_input_bytes: usize,
    pub poll_ms: u64,
    pub images: GenerationLimits,
    pub execution: ExecutionLimits,
}

impl Default for RaftHostOptions {
    fn default() -> Self {
        Self {
            max_groups: 128,
            blocking_workers: 2,
            worker_queue: 32,
            max_requests: 128,
            max_pending_bytes: 64 * 1024 * 1024,
            max_input_bytes: 1024 * 1024,
            poll_ms: 25,
            images: GenerationLimits::default(),
            execution: ExecutionLimits::default(),
        }
    }
}

impl RaftHostOptions {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
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
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RaftPeerNode {
    pub node: NodeId,
    pub disk: DiskLineage,
    pub process: ProcessGeneration,
    pub label: String,
    pub server_name: String,
    pub control_address: SocketAddr,
    pub data_address: SocketAddr,
}

#[derive(Clone, Debug)]
pub struct RaftCellRegistration {
    pub namespace: NamespaceConfig,
    pub class: String,
    pub birth: BirthProposal,
}

pub struct RaftCellsConfig {
    pub infrastructure: crate::daemon::NodeRaftConfig,
    pub catalog: CatalogOptions,
    pub cells: Vec<RaftCellRegistration>,
    pub peers: Vec<RaftPeerNode>,
    pub host: RaftHostOptions,
}

pub struct RaftCellsServices {
    pub listeners: crate::daemon::NodeRaftListeners,
    pub tls: super::transport_options::RaftTls,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub application: Arc<dyn crate::application::Application>,
}

pub(crate) struct Registration {
    pub namespace: NamespaceCatalog,
    pub birth: GroupBirth,
    pub class: Arc<RaftClass>,
}

pub(crate) async fn start(
    service: crate::daemon::NodeRaftService,
    registrations: Vec<Registration>,
    peers: Vec<RaftPeerNode>,
    directory: PathBuf,
    label: String,
    options: RaftHostOptions,
) -> anyhow::Result<RaftCells> {
    options.validate()?;
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
    )?;
    let mut cells = BTreeMap::new();
    let mut bindings = BTreeMap::new();
    let initialized = async {
        let peer_count = peers.len();
        let peers: BTreeMap<_, _> = peers.into_iter().map(|p| (p.node, p)).collect();
        anyhow::ensure!(peer_count == peers.len(), "duplicate peer node");
        for (index, registration) in registrations.into_iter().enumerate() {
            let Registration {
                namespace,
                birth,
                class,
            } = registration;
            namespace.require_mode(cellar_logic::NamespaceMode::Raft)?;
            anyhow::ensure!(
                namespace.config().schema_digest == class.schema_digest(),
                "registered schema disagrees with namespace"
            );
            let permit = namespace.bootstrap_member(&birth, &service.log).await?;
            let scope = birth.scope().clone();
            anyhow::ensure!(
                !cells.contains_key(&scope)
                    && !bindings.contains_key(&(scope.namespace, scope.cell.clone())),
                "duplicate registered cell"
            );
            let group = service.log.group(&scope)?;
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
            let progress = progress(&group, image.as_ref())?
                .ok_or_else(|| anyhow::anyhow!("unverified recovered image"))?;
            let core = CommandState::new(progress, namespace.minimum_durability())?;
            let mut routes = Vec::new();
            for member in birth.voters().iter().chain(birth.learners()) {
                routes.push(if member.node == group.identity().node {
                    RaftPeer {
                        replica: group.identity().clone(),
                        label: label.clone(),
                        server_name: "localhost".into(),
                        control_address: service.transport.control_address(),
                        data_address: service.transport.data_address(),
                    }
                } else {
                    let peer = peers
                        .get(&member.node)
                        .ok_or_else(|| anyhow::anyhow!("missing initial peer route"))?;
                    RaftPeer {
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
                    }
                });
            }
            service.transport.install_group(GroupRoute {
                scope: scope.clone(),
                schema_digest: class.schema_digest(),
                revision: 1,
                peers: routes,
            })?;
            bindings.insert(
                (scope.namespace, scope.cell.clone()),
                Binding {
                    scope: scope.clone(),
                    class: class.clone(),
                    minimum: namespace.minimum_durability(),
                },
            );
            cells.insert(
                scope.clone(),
                Cell {
                    group,
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
                    failed: false,
                },
            );
            permit.attach(&service.runtime).await?;
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
        budget,
        max_input_bytes: options.max_input_bytes,
        bindings,
        work: crate::asyncrt::services().application_work_registry(),
    });
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
        let mut driver = Driver {
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
            rng: crate::asyncrt::rng("raft-cell-commands"),
            closing,
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
) -> Result<Option<ReplicaProgress>, RuntimeError> {
    let cache = group.read()?;
    if cache.applied.image.as_ref() != image.map(|image| &image.image) {
        return Ok(None);
    }
    let mut progress = ReplicaProgress::checkpoint(
        group.identity().clone(),
        cache.applied.position.index,
        LogIndex::new(0),
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
    group: GroupLog,
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
    failed: bool,
}

enum StatusPurpose {
    Start,
    Submit,
    Finish,
    Poll,
    Observe(Box<Request>),
}

enum SqlPurpose {
    Stage,
    Mark,
    Discard,
    Retire,
    Prepare,
    Publish(ApplyTicket),
    Query(Request),
}

enum Job {
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
}

fn reply(request: &mut Request, result: Result<Response, CommandFailure>) {
    if let Some(reply) = request.reply.take() {
        let _ = reply.send(result);
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
            SqlPurpose::Query(request) => Some(request.work.clone()),
            _ => cell.pending.as_ref().map(|p| p.request.work.clone()),
        };
        let cancel = match &purpose {
            SqlPurpose::Query(request) => Some(request.control.clone()),
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
        if let Some(cell) = self.cells.get_mut(scope) {
            if let Some(mut pending) = cell.pending.take() {
                reply(&mut pending.request, result);
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

    fn on_request(&mut self, mut request: Request) {
        let scope = request.scope.clone();
        let Some(cell) = self.cells.get_mut(&scope) else {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        };
        if cell.failed || self.closing.load(Ordering::Acquire) {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        }
        if request.control.cancelled() {
            return;
        }
        match std::mem::replace(&mut request.kind, Kind::Status) {
            Kind::Status => self.status(&scope, StatusPurpose::Observe(Box::new(request))),
            Kind::Query {
                contract,
                input,
                options,
            } => {
                if options.consistency != client::QueryConsistency::Stale
                    || contract.kind != OperationKind::Query
                {
                    reply(&mut request, Err(CommandFailure::UnsupportedConsistency));
                    return;
                }
                if cell.sql_busy {
                    reply(&mut request, Err(CommandFailure::Overloaded));
                    return;
                }
                self.sql(
                    &scope,
                    SqlPurpose::Query(request),
                    Action::Query { contract, input },
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
                if options.durability == Durability::S3 {
                    reply(&mut request, Err(CommandFailure::UnsupportedCapability));
                    return;
                }
                cell.pending = Some(Pending {
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
            if let Some(progress) = progress(&cell.group, cell.image.as_ref())? {
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
                self.sql(
                    &scope,
                    SqlPurpose::Stage,
                    Action::Stage {
                        identity,
                        status: Box::new(status),
                        attempt,
                        input,
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
                match cell.core.finish(identity) {
                    Ok(permit) => {
                        let response = client::CommandReceipt {
                            id: permit.command(),
                            version: permit.version().clone(),
                            durability: permit.durability(),
                            result: receipt.receipt().result().to_vec(),
                        };
                        self.finish_pending(&scope, Ok(Response::Command(response)));
                    }
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
        let Some(cell) = self.cells.get_mut(&scope) else {
            return;
        };
        cell.sql_busy = false;
        if let SqlPurpose::Query(mut request) = purpose {
            let result = match result {
                Ok(sql::Output {
                    value: Value::Queried(value),
                    ..
                }) => Ok(Response::Query(client::QueryResult {
                    result: value.result,
                    version: value.version,
                })),
                Err(error) => Err(failure(error)),
                _ => Err(CommandFailure::CorruptState),
            };
            if !self.closing.load(Ordering::Acquire) {
                reply(&mut request, result);
            }
            return;
        }
        let sql::Output { value, _bytes } = match result {
            Ok(result) => result,
            Err(error) => {
                if matches!(
                    purpose,
                    SqlPurpose::Discard | SqlPurpose::Prepare | SqlPurpose::Publish(_)
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
            (SqlPurpose::Prepare, Value::Prepared(completion)) => {
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
                cell.apply_ticket = None;
                if let Ok(Some(progress)) = progress(&cell.group, cell.image.as_ref()) {
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
                if self
                    .transport
                    .submit(&replica.scope, message.clone())
                    .is_err()
                {
                    let runtime = self.runtime.clone();
                    let scope = replica.scope.clone();
                    let member = message.to;
                    self.jobs.push(Box::pin(async move {
                        let _ = runtime.report_unreachable(&scope, member).await;
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
            RuntimeEventKind::Snapshot { ticket, .. } => self.fail(&ticket.replica().scope),
            _ => (),
        }
    }

    fn pump(&mut self) {
        for scope in self.applying.clone() {
            let Some(cell) = self.cells.get_mut(&scope) else {
                continue;
            };
            if cell.failed {
                self.applying.remove(&scope);
                continue;
            }
            if cell.sql_busy {
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
                self.sql(
                    &scope,
                    SqlPurpose::Prepare,
                    Action::Prepare { event, image },
                );
                self.applying.remove(&scope);
            }
        }
        for scope in self.active.clone() {
            let Some(cell) = self.cells.get_mut(&scope) else {
                continue;
            };
            if cell.failed || cell.sql_busy || cell.status_busy {
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
    }

    fn on_job(&mut self, job: Job) {
        match job {
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
        self.cells.clear();
        let log = self.log.close().await;
        if runtime.is_err() || sql.is_err() || log.is_err() {
            return Err(CommandFailure::CorruptState);
        }
        Ok(client::RaftCloseReport { unfinished })
    }
}
