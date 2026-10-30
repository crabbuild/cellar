// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One domain-owned engine worker. Every successful mutation has crossed fsync.

use super::cache::{
    genesis, validate_conf, CachedGroup, GroupLog, GroupWrite, SnapshotChange, StoredGroup,
};
use super::filesystem::EngineFileSystem;
use super::fleet_stop::{Guard, LifecycleRecord, LifecycleUpdate};
use super::options::{LogOpenMode, ReplicaLogOptions};
use super::performance::{
    Counter as MeasuredCounter, Operation as Measured, Registry as Performance,
};
use super::records::{
    self, GroupRecord, MappingRecord, NodeRecord, RootRecord, GROUP_KEY, ROOT_KEY,
};
use ::raft::prelude::{ConfState, Entry};
use cellar_logic::cell_group::{GroupHistory, GroupRoute};
use cellar_logic::consensus::{GroupScope, MemberId, OperationId, ReplicaIdentity};
use cellar_ltx::{FileSystem, HostFileLock};
use protobuf::Message;
use raft_engine::{Command, Engine, LogBatch, MessageExt, ReadableSize, RecoveryMode};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};

#[cfg(test)]
#[path = "storage_cell_genesis_tests.rs"]
mod cell_genesis_tests;

#[cfg(test)]
#[path = "storage_genesis_tests.rs"]
mod genesis_tests;

#[cfg(test)]
#[path = "storage_lifecycle_tests.rs"]
mod lifecycle_tests;

const LIFECYCLE_KEY: &[u8] = b"cellar-node-lifecycle";

#[derive(Clone)]
struct EngineCaches {
    usage: Arc<AtomicUsize>,
    lifecycle: Arc<Mutex<LifecycleRecord>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogError {
    Invalid(String),
    Io(String),
    Corrupt(String),
    WrongIdentity,
    LostStableState,
    AlreadyExists,
    NotFound,
    Overloaded,
    Closed,
}

impl fmt::Display for LogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Raft storage: {self:?}")
    }
}

impl std::error::Error for LogError {}

impl From<std::io::Error> for LogError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

fn engine_error(error: raft_engine::Error) -> LogError {
    match error {
        raft_engine::Error::Corruption(_)
        | raft_engine::Error::Codec(_)
        | raft_engine::Error::Protobuf(_) => records::corrupt(error),
        _ => LogError::Io(error.to_string()),
    }
}

impl LogError {
    fn fatal(&self) -> bool {
        matches!(self, Self::Io(_) | Self::Corrupt(_) | Self::LostStableState)
    }
}

#[derive(Clone, Debug)]
pub struct GroupBootstrap {
    pub scope: GroupScope,
    pub member: MemberId,
    pub birth_digest: [u8; 32],
    pub voters: Vec<MemberId>,
    pub learners: Vec<MemberId>,
}

impl GroupBootstrap {
    fn configuration(&self) -> Result<ConfState, LogError> {
        self.scope
            .validate()
            .map_err(|error| LogError::Invalid(error.to_string()))?;
        initial_configuration(self.member, &self.voters, &self.learners)
    }
}

/// Physical genesis for a whole cell range. Construction supplies no membership
/// claim or serving authority; those require the catalog bootstrap protocol.
#[derive(Clone, Debug)]
pub struct CellGroupBootstrap {
    pub route: GroupRoute,
    pub member: MemberId,
    pub voters: Vec<MemberId>,
    pub learners: Vec<MemberId>,
}

impl CellGroupBootstrap {
    fn configuration(&self) -> Result<ConfState, LogError> {
        super::cell_groups::GroupManifest::genesis(self.route.clone())
            .map_err(|error| LogError::Invalid(error.to_string()))?;
        initial_configuration(self.member, &self.voters, &self.learners)
    }
}

fn initial_configuration(
    member: MemberId,
    voters: &[MemberId],
    learners: &[MemberId],
) -> Result<ConfState, LogError> {
    let mut conf = ConfState::default();
    conf.set_voters(voters.iter().map(|id| id.get()).collect());
    conf.set_learners(learners.iter().map(|id| id.get()).collect());
    validate_conf(&conf).map_err(|error| LogError::Invalid(error.to_string()))?;
    super::member_state::GroupMembership::initial(&conf)
        .validate(&conf, 1)
        .map_err(|error| LogError::Invalid(error.to_string()))?;
    if !conf.voters.contains(&member.get()) && !conf.learners.contains(&member.get()) {
        return Err(LogError::WrongIdentity);
    }
    Ok(conf)
}

/// Identity-bound observation of one successfully synced batch.
#[derive(Clone, Debug)]
pub struct SyncReceipt<S = GroupScope> {
    pub operation: OperationId,
    pub sequence: u64,
    pub groups: Vec<ReplicaIdentity<S>>,
}

type Catalog = Arc<RwLock<BTreeMap<GroupHistory, StoredGroup>>>;
type Reply<T> = oneshot::Sender<Result<T, LogError>>;

#[derive(Clone)]
pub struct ReplicaLog {
    inner: Arc<LogHandle>,
}

struct LogHandle {
    snapshot_scratch: cellar_ltx::stream_snapshot::ScratchPool,
    performance: Arc<Performance>,
    instance: std::sync::OnceLock<cellar_logic::consensus::EngineInstanceId>,
    owner: Arc<()>,
    sender: mpsc::SyncSender<Request>,
    domain: crate::asyncrt::DomainToken,
    groups: Catalog,
    pending: Arc<AtomicUsize>,
    cache_usage: Arc<AtomicUsize>,
    lifecycle_cache: Arc<Mutex<LifecycleRecord>>,
    options: ReplicaLogOptions,
    closing: Arc<AtomicBool>,
    runtime_claimed: AtomicBool,
    abort: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    terminal: Arc<Terminal>,
}

struct Terminal {
    result: Mutex<Option<Result<(), LogError>>>,
    changed: Notify,
}

impl Terminal {
    async fn wait(&self) -> Result<(), LogError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let result = self
                .result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(result) = result {
                return result;
            }
            changed.await;
        }
    }
}

impl Drop for LogHandle {
    fn drop(&mut self) {
        self.abort.store(true, Ordering::Release);
    }
}

struct Permit {
    pending: Arc<AtomicUsize>,
    bytes: usize,
}

impl Permit {
    fn acquire(pending: Arc<AtomicUsize>, bytes: usize, maximum: usize) -> Result<Self, LogError> {
        pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(bytes).filter(|&total| total <= maximum)
            })
            .map_err(|_| LogError::Overloaded)?;
        Ok(Self { pending, bytes })
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.pending.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Request {
    queued: super::performance::Timer,
    work: Work,
    _permit: Permit,
}

enum Work {
    NodeFence(Reply<u64>),
    Lifecycle(Box<LifecycleUpdate>, Reply<Guard>),
    Create(
        GroupBootstrap,
        Option<Box<super::member_state::FleetGroupBinding>>,
        Reply<GroupLog>,
    ),
    CreateCells(CellGroupBootstrap, Reply<GroupLog<GroupHistory>>),
    Seeded(Box<super::snapshot::PreparedSeed>, Reply<GroupLog>),
    Persist(OperationId, Vec<GroupWrite>, Reply<SyncReceipt>),
    PersistCells(
        OperationId,
        Vec<GroupWrite<GroupHistory>>,
        Reply<SyncReceipt<GroupHistory>>,
    ),
    Purge(Reply<()>),
}

/// Exclusive consensus ownership; normal release follows a final storage fence.
pub(crate) struct RuntimeLease(Arc<LogHandle>);

impl RuntimeLease {
    /// Dropping an unfenced worker deliberately leaves the claim poisoned.
    pub(crate) fn release(self) {
        self.0.runtime_claimed.store(false, Ordering::Release);
    }
}

impl ReplicaLog {
    pub(crate) fn performance(&self) -> Arc<Performance> {
        self.inner.performance.clone()
    }

    /// Cumulative observations for this engine and its owning host, if present.
    pub fn performance_snapshot(&self) -> crate::application::consensus::RaftPerformanceSnapshot {
        self.inner.performance.snapshot()
    }

    /// Stable engine-creation identity, not a vote or group membership proof.
    pub fn storage_instance(&self) -> Result<cellar_logic::consensus::EngineInstanceId, LogError> {
        self.admit()?;
        self.inner.instance.get().copied().ok_or(LogError::Closed)
    }

    /// Allocate a durable node-local directory revision through a nonempty
    /// synced root fence. This cannot settle a Raft group persistence ticket.
    pub async fn reserve_node_revision(&self) -> Result<u64, LogError> {
        self.request(1024, Work::NodeFence).await
    }

    pub(crate) async fn update_lifecycle(
        &self,
        update: LifecycleUpdate,
    ) -> Result<Guard, LogError> {
        self.request(8192, |reply| Work::Lifecycle(Box::new(update), reply))
            .await
    }

    /// Last fsynced local lifecycle state, retained after the engine joins.
    /// This observation alone supplies no physical closure proof.
    pub(crate) fn lifecycle_guard(&self) -> Result<Option<Guard>, LogError> {
        Ok(self
            .inner
            .lifecycle_cache
            .lock()
            .map_err(|_| LogError::Closed)?
            .guard
            .clone())
    }

    pub(crate) fn claim_runtime(&self) -> Result<RuntimeLease, LogError> {
        self.admit()?;
        self.inner
            .runtime_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| LogError::AlreadyExists)?;
        Ok(RuntimeLease(self.inner.clone()))
    }

    pub(crate) fn domain(&self) -> crate::asyncrt::DomainToken {
        self.inner.domain.clone()
    }

    pub(crate) fn owner(&self) -> Arc<()> {
        self.inner.owner.clone()
    }

    pub(crate) fn limits(&self) -> &ReplicaLogOptions {
        &self.inner.options
    }

    /// Advisory maintenance signal. The writer still enforces the hard bound
    /// against the complete candidate batch before crossing its sync boundary.
    pub(crate) fn cache_under_pressure(&self) -> bool {
        self.inner.cache_usage.load(Ordering::Acquire) >= self.inner.options.max_cache_bytes / 2
    }

    pub async fn open(
        options: ReplicaLogOptions,
        filesystem: Arc<dyn FileSystem>,
    ) -> Result<Self, LogError> {
        Self::open_observed(options, filesystem, None).await
    }

    pub(crate) fn snapshot_scratch(&self) -> cellar_ltx::stream_snapshot::ScratchPool {
        self.inner.snapshot_scratch.clone()
    }

    pub(crate) async fn open_observed(
        options: ReplicaLogOptions,
        filesystem: Arc<dyn FileSystem>,
        performance: Option<Arc<Performance>>,
    ) -> Result<Self, LogError> {
        options.validate()?;
        let snapshot_scratch =
            cellar_ltx::stream_snapshot::ScratchPool::new(options.snapshot_scratch)
                .map_err(|e| LogError::Invalid(e.to_string()))?;
        let files = StorageFiles {
            filesystem,
            snapshot_scratch: snapshot_scratch.clone(),
        };
        let domain = crate::asyncrt::services()
            .domain_token()
            .ok_or(LogError::Closed)?;
        let performance = performance.unwrap_or_else(|| Performance::new(domain.clone()));
        let (sender, receiver) = mpsc::sync_channel(options.queue_capacity);
        let (ready, wait) = oneshot::channel();
        let groups = Arc::new(RwLock::new(BTreeMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let abort = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(Terminal {
            result: Mutex::new(None),
            changed: Notify::new(),
        });
        let closing = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(());
        let cache_usage = Arc::new(AtomicUsize::new(0));
        let lifecycle_cache = Arc::new(Mutex::new(LifecycleRecord::empty()));
        let handle = Self {
            inner: Arc::new(LogHandle {
                snapshot_scratch,
                performance: performance.clone(),
                instance: std::sync::OnceLock::new(),
                owner: owner.clone(),
                sender,
                domain: domain.clone(),
                groups: groups.clone(),
                pending: Arc::new(AtomicUsize::new(0)),
                cache_usage: cache_usage.clone(),
                lifecycle_cache: lifecycle_cache.clone(),
                options: options.clone(),
                closing: closing.clone(),
                runtime_claimed: AtomicBool::new(false),
                abort: abort.clone(),
                closed: closed.clone(),
                terminal: terminal.clone(),
            }),
        };
        let worker_domain = domain.clone();
        // The task registry joins begun blocking work on domain shutdown even
        // if the open waiter or public handle is dropped.
        let _worker = domain
            .blocking(move || {
                struct ClosedOnDrop(Arc<AtomicBool>, Arc<Terminal>);

                impl Drop for ClosedOnDrop {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::Release);
                        // Worker/resources unwind before this outer guard. A
                        // panic may close admission early, but cannot fake join.
                        let mut terminal = self
                            .1
                            .result
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if terminal.is_none() {
                            *terminal = Some(Err(LogError::Closed));
                        }
                        drop(terminal);
                        self.1.changed.notify_waiters();
                    }
                }
                let _guard = ClosedOnDrop(closed.clone(), terminal.clone());
                let result = Worker::open(
                    options,
                    files,
                    groups,
                    owner,
                    closed.clone(),
                    EngineCaches {
                        usage: cache_usage,
                        lifecycle: lifecycle_cache,
                    },
                    performance,
                );
                let mut worker = match result {
                    Ok(worker) => worker,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                let instance = worker
                    .root
                    .identity
                    .instance(worker.options.node, worker.options.disk);
                if ready.send(instance).is_err() {
                    return;
                }
                let result = worker.run(receiver, &worker_domain, &abort, &closing);
                closed.store(true, Ordering::Release);
                // Engine drop joins its metrics thread; directory locks are owned
                // by this same worker and cannot outlive its completion.
                drop(worker);
                *terminal
                    .result
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(result);
                terminal.changed.notify_waiters();
            })
            .map_err(|_| LogError::Closed)?;
        let instance = wait.await.map_err(|_| LogError::Closed)??;
        handle
            .inner
            .instance
            .set(instance)
            .map_err(|_| LogError::WrongIdentity)?;
        handle.admit()?;
        Ok(handle)
    }

    pub(crate) fn group_count(&self) -> Result<usize, LogError> {
        self.group_count_with_pending(None)
    }

    /// Count an admitted slot once while ownership transfers to the durable
    /// catalog. One lock prevents a sync racing two independent observations.
    pub(crate) fn group_count_with_pending(
        &self,
        pending: Option<&GroupScope>,
    ) -> Result<usize, LogError> {
        self.admit()?;
        let groups = self.inner.groups.read().map_err(|_| LogError::Closed)?;
        let additional = match pending {
            Some(scope) => {
                scope.validate().map_err(|_| LogError::WrongIdentity)?;
                match groups.get(&GroupHistory::from(scope)) {
                    Some(group) => {
                        if &group.as_cell()?.identity.scope != scope {
                            return Err(LogError::WrongIdentity);
                        }
                        false
                    }
                    None => true,
                }
            }
            None => false,
        };
        groups
            .len()
            .checked_add(usize::from(additional))
            .ok_or(LogError::Overloaded)
    }

    pub fn groups(&self) -> Result<Vec<GroupLog>, LogError> {
        self.admit()?;
        self.inner
            .groups
            .read()
            .map_err(|_| LogError::Closed)?
            .values()
            .map(|group| group.as_cell().cloned())
            .collect()
    }

    pub fn cell_groups(&self) -> Result<Vec<GroupLog<GroupHistory>>, LogError> {
        self.admit()?;
        self.inner
            .groups
            .read()
            .map_err(|_| LogError::Closed)?
            .values()
            .map(|group| group.as_cells().cloned())
            .collect()
    }

    pub fn cell_group(&self, history: &GroupHistory) -> Result<GroupLog<GroupHistory>, LogError> {
        self.admit()?;
        self.inner
            .groups
            .read()
            .map_err(|_| LogError::Closed)?
            .get(history)
            .ok_or(LogError::NotFound)?
            .as_cells()
            .cloned()
    }

    pub async fn create_cell_group(
        &self,
        bootstrap: CellGroupBootstrap,
    ) -> Result<GroupLog<GroupHistory>, LogError> {
        bootstrap.configuration()?;
        self.request(16 * 1024, |reply| Work::CreateCells(bootstrap, reply))
            .await
    }

    /// Read occupied identities and locally durable completed membership under
    /// the engine catalog lock and short per-group cache reads. This catalog is
    /// append-only for an engine instance; retired bindings remain.
    pub(crate) fn physical_inventory(
        &self,
        pending: Option<&GroupScope>,
    ) -> Result<super::physical_inventory::RaftPhysicalInventory, LogError> {
        self.admit()?;
        let groups = self.inner.groups.read().map_err(|_| LogError::Closed)?;
        super::physical_inventory::capture(&groups, pending)
    }

    /// Complete physical occupancy for a cell-group engine, including retained
    /// histories and an optional initializing group. This is not serving or
    /// membership authority. Legacy application engines refuse this accessor.
    pub fn cell_group_inventory(
        &self,
        pending: Option<&GroupHistory>,
    ) -> Result<super::physical_inventory::RaftPhysicalInventory, LogError> {
        self.admit()?;
        let groups = self.inner.groups.read().map_err(|_| LogError::Closed)?;
        super::physical_inventory::capture_cells(&groups, pending)
    }

    pub fn group(&self, scope: &GroupScope) -> Result<GroupLog, LogError> {
        self.admit()?;
        scope.validate().map_err(|_| LogError::WrongIdentity)?;
        let groups = self.inner.groups.read().map_err(|_| LogError::Closed)?;
        let group = groups
            .get(&GroupHistory::from(scope))
            .ok_or(LogError::NotFound)?
            .as_cell()?;
        if &group.identity.scope != scope {
            return Err(LogError::WrongIdentity);
        }
        Ok(group.clone())
    }

    pub async fn create_group(&self, bootstrap: GroupBootstrap) -> Result<GroupLog, LogError> {
        bootstrap.configuration()?;
        self.request(4096, |reply| Work::Create(bootstrap, None, reply))
            .await
    }

    pub(crate) async fn create_managed_group(
        &self,
        bootstrap: GroupBootstrap,
        binding: super::member_state::FleetGroupBinding,
    ) -> Result<GroupLog, LogError> {
        let conf = bootstrap.configuration()?;
        if !binding.matches_scope(&bootstrap.scope) {
            return Err(LogError::WrongIdentity);
        }
        super::member_state::GroupMembership::managed_initial(&conf, binding.clone())?;
        self.request(24 * 1024, |reply| {
            Work::Create(bootstrap, Some(Box::new(binding)), reply)
        })
        .await
    }

    pub(crate) async fn create_seeded(
        &self,
        seed: super::snapshot::PreparedSeed,
    ) -> Result<GroupLog, LogError> {
        if !seed.authorized(&self.inner.owner, seed.local()) {
            return Err(LogError::WrongIdentity);
        }
        self.request(96 * 1024, |reply| Work::Seeded(Box::new(seed), reply))
            .await
    }

    pub async fn persist_cells(
        &self,
        operation: OperationId,
        writes: Vec<GroupWrite<GroupHistory>>,
    ) -> Result<SyncReceipt<GroupHistory>, LogError> {
        let bytes = self.write_bytes(operation, &writes)?;
        self.request(bytes, |reply| Work::PersistCells(operation, writes, reply))
            .await
    }

    pub async fn persist(
        &self,
        operation: OperationId,
        writes: Vec<GroupWrite>,
    ) -> Result<SyncReceipt, LogError> {
        let bytes = self.write_bytes(operation, &writes)?;
        self.request(bytes, |reply| Work::Persist(operation, writes, reply))
            .await
    }

    fn write_bytes<S: Clone>(
        &self,
        operation: OperationId,
        writes: &[GroupWrite<S>],
    ) -> Result<usize, LogError> {
        if operation.get() == 0 {
            return Err(LogError::Invalid("zero persistence operation".into()));
        }
        let mut bytes = 1024_usize;
        for write in writes {
            bytes = bytes
                .checked_add(write.estimated_bytes()?)
                .ok_or(LogError::Overloaded)?;
        }
        if bytes > self.inner.options.max_batch_bytes {
            return Err(LogError::Overloaded);
        }
        Ok(bytes)
    }

    /// Reclaim obsolete shared engine files, retaining every group's live log,
    /// votes and checkpoint. This does not authorize logical compaction.
    pub async fn purge(&self) -> Result<(), LogError> {
        self.request(1024, Work::Purge).await
    }

    /// Close admission, drain submitted work, fence and join physical storage
    /// ownership. Concurrent/repeated waiters observe the same terminal result;
    /// dropping one waiter does not abandon shutdown. Errors also wait for the
    /// engine and directory locks to be released.
    pub async fn close(&self) -> Result<(), LogError> {
        // The worker owns draining/fencing, independently of every close
        // waiter. Admission-closed is distinct from physically joined.
        self.inner.closing.store(true, Ordering::Release);
        self.inner.terminal.wait().await
    }

    fn admit(&self) -> Result<(), LogError> {
        if self.inner.closing.load(Ordering::Acquire)
            || self.inner.closed.load(Ordering::Acquire)
            || self.inner.domain.mono_ms().is_err()
        {
            return Err(LogError::Closed);
        }
        Ok(())
    }

    async fn request<T>(
        &self,
        bytes: usize,
        build: impl FnOnce(Reply<T>) -> Work,
    ) -> Result<T, LogError> {
        self.admit()?;
        let permit = Permit::acquire(
            self.inner.pending.clone(),
            bytes,
            self.inner.options.max_pending_bytes,
        )?;
        let (reply, wait) = oneshot::channel();
        self.inner
            .sender
            .try_send(Request {
                queued: self.inner.performance.timer(Measured::EngineQueue),
                work: build(reply),
                _permit: permit,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => LogError::Overloaded,
                mpsc::TrySendError::Disconnected(_) => LogError::Closed,
            })?;
        let result = wait.await.map_err(|_| LogError::Closed)?;
        if self.inner.domain.mono_ms().is_err() {
            return Err(LogError::Closed);
        }
        result
    }
}

pub(crate) struct EntryCodec;

impl MessageExt for EntryCodec {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

struct Worker {
    filesystem: Arc<dyn FileSystem>,
    snapshot_scratch: cellar_ltx::stream_snapshot::ScratchPool,
    performance: Arc<Performance>,
    engine: Engine<EngineFileSystem>,
    _directory_lock: HostFileLock,
    options: ReplicaLogOptions,
    root: RootRecord,
    mappings: BTreeMap<u64, MappingRecord>,
    groups: Catalog,
    owner: Arc<()>,
    closed: Arc<AtomicBool>,
    cache_bytes: usize,
    cache_usage: Arc<AtomicUsize>,
    lifecycle: LifecycleRecord,
    lifecycle_cache: Arc<Mutex<LifecycleRecord>>,
    purge_pending: bool,
}

struct StorageFiles {
    filesystem: Arc<dyn FileSystem>,
    snapshot_scratch: cellar_ltx::stream_snapshot::ScratchPool,
}

impl Worker {
    fn open(
        options: ReplicaLogOptions,
        files: StorageFiles,
        groups: Catalog,
        owner: Arc<()>,
        closed: Arc<AtomicBool>,
        caches: EngineCaches,
        performance: Arc<Performance>,
    ) -> Result<Self, LogError> {
        let StorageFiles {
            filesystem: fs,
            snapshot_scratch,
        } = files;
        match options.mode {
            LogOpenMode::Create => create_directory_durably(fs.as_ref(), &options.directory)?,
            LogOpenMode::Resume => match fs.metadata(&options.directory) {
                Ok(metadata) if metadata.is_dir => (),
                Ok(_) => return Err(LogError::LostStableState),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(LogError::LostStableState)
                }
                Err(error) => return Err(error.into()),
            },
        }
        let directory_lock = fs
            .lock_file(&options.directory.join("CELLAR_LOCK"))
            .map_err(|error| LogError::Io(format!("node directory lock: {error}")))?;
        let marker = options.directory.join("node.json");
        let log_directory = options.directory.join("log");
        let expected = match options.mode {
            LogOpenMode::Create => {
                for entry in fs.read_dir_iter(&options.directory)? {
                    if entry?.file_name != "CELLAR_LOCK" {
                        return Err(LogError::AlreadyExists);
                    }
                }
                let mut nonce = [0; 16];
                use rand::RngCore as _;
                crate::asyncrt::rng("raft-storage-instance").fill_bytes(&mut nonce);
                let instance = cellar_logic::consensus::EngineInstanceId::new(nonce)
                    .map_err(|_| LogError::Invalid("empty storage instance identity".into()))?;
                let expected = NodeRecord::new(options.node, options.disk, instance);
                let mut file = fs.create_new(&marker)?;
                file.write_all(&records::encode(&expected)?)?;
                file.sync_all()?;
                fs.sync_all(&options.directory)?;
                expected
            }
            LogOpenMode::Resume => {
                let mut file = fs.open(&marker).map_err(|_| LogError::LostStableState)?;
                let length = file.file_len()?;
                if length > 4096 {
                    return Err(LogError::Corrupt("oversized node marker".into()));
                }
                let found: NodeRecord = records::decode(&file.read_exact_at(0, length as usize)?)?;
                found.instance(options.node, options.disk)?;
                if !fs
                    .metadata(&log_directory)
                    .map_err(|_| LogError::LostStableState)?
                    .is_dir
                {
                    return Err(LogError::LostStableState);
                }
                found
            }
        };
        let config = raft_engine::Config {
            dir: log_directory
                .to_str()
                .ok_or_else(|| LogError::Invalid("non-UTF-8 log directory".into()))?
                .to_owned(),
            recovery_mode: RecoveryMode::AbsoluteConsistency,
            recovery_threads: 1,
            target_file_size: ReadableSize(options.target_file_bytes),
            purge_threshold: ReadableSize(options.purge_threshold_bytes),
            enable_log_recycle: false,
            prefill_for_recycle: false,
            ..Default::default()
        };
        let engine = Engine::open_with_file_system(
            config,
            Arc::new(EngineFileSystem(fs.clone(), performance.clone())),
        )
        .map_err(engine_error)?;
        let root = match options.mode {
            LogOpenMode::Create => {
                let root = RootRecord {
                    identity: expected,
                    next_key: 1,
                    sequence: 0,
                };
                let mut batch = LogBatch::default();
                batch
                    .put(0, ROOT_KEY.to_vec(), records::encode(&root)?)
                    .map_err(engine_error)?;
                batch
                    .put(
                        0,
                        LIFECYCLE_KEY.to_vec(),
                        records::encode(&LifecycleRecord::empty())?,
                    )
                    .map_err(engine_error)?;
                engine.write(&mut batch, true).map_err(engine_error)?;
                fs.sync_all(&log_directory)?;
                fs.sync_all(&options.directory)?;
                root
            }
            LogOpenMode::Resume => {
                let root: RootRecord =
                    records::decode(&engine.get(0, ROOT_KEY).ok_or(LogError::LostStableState)?)?;
                if root.identity != expected || root.next_key == 0 {
                    return Err(LogError::WrongIdentity);
                }
                root
            }
        };
        let bytes = engine
            .get(0, LIFECYCLE_KEY)
            .ok_or(LogError::LostStableState)?;
        if bytes.len() > 4096 {
            return Err(LogError::Corrupt("oversized node lifecycle record".into()));
        }
        let lifecycle: LifecycleRecord = records::decode(&bytes)?;
        lifecycle.validate().map_err(|error| {
            LogError::Corrupt(format!("invalid node lifecycle record: {error}"))
        })?;
        if lifecycle.guard.as_ref().is_some_and(|guard| {
            guard.boot.node != options.node.bytes()
                || guard.boot.disk != options.disk.bytes()
                || guard.boot.engine != root.identity.storage_instance
        }) {
            return Err(LogError::WrongIdentity);
        }
        *caches.lifecycle.lock().map_err(|_| LogError::Closed)? = lifecycle.clone();
        let mut worker = Self {
            filesystem: fs.clone(),
            snapshot_scratch,
            performance,
            engine,
            _directory_lock: directory_lock,
            options,
            root,
            mappings: BTreeMap::new(),
            groups,
            owner,
            closed,
            cache_bytes: 0,
            cache_usage: caches.usage,
            lifecycle,
            lifecycle_cache: caches.lifecycle,
            // A previous process may have synced compaction without purging.
            purge_pending: true,
        };
        worker.recover()?;
        worker
            .cache_usage
            .store(worker.cache_bytes, Ordering::Release);
        Ok(worker)
    }

    fn recover(&mut self) -> Result<(), LogError> {
        let mut mappings = Vec::new();
        let mut failure = None;
        self.engine
            .scan_raw_messages(0, Some(b"g:"), Some(b"g;"), false, |key, bytes| {
                if mappings.len() >= self.options.max_groups {
                    failure = Some(LogError::Overloaded);
                    return false;
                }
                let mapping: MappingRecord = match records::decode(bytes) {
                    Ok(mapping) => mapping,
                    Err(error) => {
                        failure = Some(error);
                        return false;
                    }
                };
                match mapping.catalog_key() {
                    Ok(expected) if expected == key => mappings.push(mapping),
                    _ => {
                        failure = Some(LogError::Corrupt("invalid catalog key".into()));
                        return false;
                    }
                }
                true
            })
            .map_err(engine_error)?;
        if let Some(error) = failure {
            return Err(error);
        }
        let mut known = BTreeSet::from([0]);
        let mut application = None;
        for mapping in mappings {
            if application
                .replace(mapping.is_cells())
                .is_some_and(|kind| kind != mapping.is_cells())
            {
                return Err(LogError::Corrupt("mixed engine application domains".into()));
            }
            if mapping.key == 0 || mapping.key >= self.root.next_key || !known.insert(mapping.key) {
                return Err(LogError::Corrupt(
                    "reused or unallocated numeric group key".into(),
                ));
            }
            let bytes = self
                .engine
                .get(mapping.key, GROUP_KEY)
                .ok_or(LogError::LostStableState)?;
            let record: GroupRecord = records::decode(&bytes)?;
            if record.mapping != mapping {
                return Err(LogError::Corrupt("group/catalog identity mismatch".into()));
            }
            let mut cache = record.metadata_cache()?;
            if record.last_index < cache.snapshot.get_metadata().index
                || record.last_index - cache.snapshot.get_metadata().index
                    > self.options.max_entries_per_group as u64
            {
                return Err(LogError::Corrupt("invalid retained log range".into()));
            }
            for index in cache.first_index()..=record.last_index {
                let entry = self
                    .engine
                    .get_entry::<EntryCodec>(mapping.key, index)
                    .map_err(engine_error)?
                    .ok_or_else(|| LogError::Corrupt("missing retained entry".into()))?;
                cache.entries.insert(index, entry);
                if cache.bytes() > self.options.max_cache_bytes {
                    return Err(LogError::Overloaded);
                }
            }
            if self
                .engine
                .last_index(mapping.key)
                .is_some_and(|index| index != record.last_index)
            {
                return Err(LogError::Corrupt("log tail disagrees with metadata".into()));
            }
            cache.validate(self.options.max_entries_per_group)?;
            let conf =
                ConfState::parse_from_bytes(&mapping.initial_conf).map_err(records::corrupt)?;
            validate_conf(&conf)?;
            self.cache_bytes = self
                .cache_bytes
                .checked_add(cache.bytes())
                .ok_or(LogError::Overloaded)?;
            if self.cache_bytes > self.options.max_cache_bytes {
                return Err(LogError::Overloaded);
            }
            let handle = self.handle(&mapping, cache)?;
            if self
                .groups
                .write()
                .map_err(|_| LogError::Closed)?
                .insert(handle.history(), handle)
                .is_some()
            {
                return Err(LogError::Corrupt("duplicate logical group mapping".into()));
            }
            self.mappings.insert(mapping.key, mapping);
        }
        if self
            .engine
            .raft_groups()
            .into_iter()
            .any(|key| !known.contains(&key))
        {
            return Err(LogError::Corrupt("orphan engine group".into()));
        }
        Ok(())
    }

    fn handle(&self, mapping: &MappingRecord, cache: CachedGroup) -> Result<StoredGroup, LogError> {
        let initial =
            ConfState::parse_from_bytes(&mapping.initial_conf).map_err(records::corrupt)?;
        validate_conf(&initial)?;
        if initial.get_unknown_fields().iter().next().is_some() {
            return Err(LogError::Corrupt(
                "unknown initial configuration fields".into(),
            ));
        }
        if !cache
            .membership
            .same_birth(&super::member_state::GroupMembership::initial(&initial))
        {
            return Err(LogError::Corrupt("member binding birth mismatch".into()));
        }
        mapping.validate_cache(&cache)?;
        if mapping.is_cells() {
            Ok(StoredGroup::Cells(self.scoped_handle(
                mapping,
                cache,
                mapping.history()?,
            )?))
        } else {
            Ok(StoredGroup::Cell(self.scoped_handle(
                mapping,
                cache,
                mapping.application_scope()?,
            )?))
        }
    }

    fn scoped_handle<S>(
        &self,
        mapping: &MappingRecord,
        cache: CachedGroup,
        scope: S,
    ) -> Result<GroupLog<S>, LogError> {
        Ok(GroupLog {
            consensus_claimed: Arc::new(AtomicBool::new(false)),
            scratch_directory: self.options.directory.clone(),
            snapshot_scratch: self.snapshot_scratch.clone(),
            filesystem: self.filesystem.clone(),
            key: mapping.key,
            birth_digest: mapping.birth_digest,
            identity: ReplicaIdentity {
                scope,
                node: self.options.node,
                member: MemberId::new(mapping.member).map_err(records::corrupt)?,
                disk: self.options.disk,
                process: self.options.process,
            },
            owner: self.owner.clone(),
            closed: self.closed.clone(),
            cache: Arc::new(RwLock::new(cache)),
            cache_revision: Arc::new(AtomicU64::new(self.root.sequence)),
        })
    }

    fn fence(&mut self, batch: &mut LogBatch) -> Result<u64, LogError> {
        let sequence = self
            .root
            .sequence
            .checked_add(1)
            .ok_or_else(|| LogError::Corrupt("sync sequence exhausted".into()))?;
        let mut root = self.root.clone();
        root.sequence = sequence;
        // A nonempty marker is required: upstream Engine::sync passes an empty
        // batch which does not flush. No false flush proof may escape here.
        batch
            .put(0, ROOT_KEY.to_vec(), records::encode(&root)?)
            .map_err(engine_error)?;
        self.engine.write(batch, true).map_err(engine_error)?;
        self.root = root;
        self.purge_pending = true;
        Ok(sequence)
    }

    fn change_lifecycle(&mut self, update: LifecycleUpdate) -> Result<Guard, LogError> {
        let boot = update.boot();
        if boot.node != self.options.node.bytes()
            || boot.disk != self.options.disk.bytes()
            || boot.process != self.options.process.bytes()
            || boot.engine != self.root.identity.storage_instance
        {
            return Err(LogError::WrongIdentity);
        }
        let next = self.lifecycle.change(update)?;
        let guard = next.guard.clone().ok_or(LogError::WrongIdentity)?;
        if next == self.lifecycle {
            return Ok(guard);
        }
        let bytes = records::encode(&next)?;
        if bytes.len() > 4096 {
            return Err(LogError::Overloaded);
        }
        let mut batch = LogBatch::default();
        batch
            .put(0, LIFECYCLE_KEY.to_vec(), bytes)
            .map_err(engine_error)?;
        self.fence(&mut batch)?;
        self.lifecycle = next;
        *self.lifecycle_cache.lock().map_err(|_| LogError::Closed)? = self.lifecycle.clone();
        Ok(guard)
    }

    fn create(
        &mut self,
        bootstrap: GroupBootstrap,
        binding: Option<super::member_state::FleetGroupBinding>,
    ) -> Result<GroupLog, LogError> {
        let conf = bootstrap.configuration()?;
        if let Some(group) = self
            .groups
            .read()
            .map_err(|_| LogError::Closed)?
            .get(&GroupHistory::from(&bootstrap.scope))
        {
            let group = group.as_cell()?;
            let mapping = self
                .mappings
                .get(&group.key)
                .ok_or(LogError::LostStableState)?;
            if group.identity.scope == bootstrap.scope
                && mapping.member == bootstrap.member.get()
                && mapping.birth_digest == bootstrap.birth_digest
                && mapping.initial_conf == conf.write_to_bytes().map_err(records::corrupt)?
            {
                if let Some(binding) = &binding {
                    let cache = group.read()?;
                    if !cache
                        .membership
                        .fleet_status()
                        .is_some_and(|status| status.binding == *binding)
                        || !super::member_state::members(&cache.conf_state)
                            .contains(&bootstrap.member.get())
                        || !cache.membership.permits(group.identity())
                    {
                        return Err(LogError::WrongIdentity);
                    }
                }
                return Ok(group.clone());
            }
            return Err(LogError::WrongIdentity);
        }
        let mut cache = genesis(conf.clone());
        if let Some(binding) = binding {
            if !binding.matches_scope(&bootstrap.scope) {
                return Err(LogError::WrongIdentity);
            }
            cache.membership =
                super::member_state::GroupMembership::managed_initial(&conf, binding)?;
        }
        self.create_cache(bootstrap, conf, cache, None)
    }

    fn seeded(&mut self, seed: super::snapshot::PreparedSeed) -> Result<GroupLog, LogError> {
        let identity = ReplicaIdentity {
            scope: seed.local().scope.clone(),
            member: seed.local().member,
            node: self.options.node,
            disk: self.options.disk,
            process: self.options.process,
        };
        if !seed.authorized(&self.owner, &identity) {
            return Err(LogError::WrongIdentity);
        }
        if let Some(group) = self
            .groups
            .read()
            .map_err(|_| LogError::Closed)?
            .get(&GroupHistory::from(&identity.scope))
        {
            return Err(if group.as_cell()?.identity.scope == identity.scope {
                LogError::AlreadyExists
            } else {
                LogError::WrongIdentity
            });
        }
        let bootstrap = seed.bootstrap()?;
        let conf = seed.initial().clone();
        let cache = seed.cache().map_err(|e| LogError::Invalid(e.to_string()))?;
        self.create_cache(bootstrap, conf, cache, Some(&seed))
    }

    fn create_cache(
        &mut self,
        bootstrap: GroupBootstrap,
        conf: ConfState,
        cache: CachedGroup,
        seed: Option<&super::snapshot::PreparedSeed>,
    ) -> Result<GroupLog, LogError> {
        let mapping = MappingRecord::new(self.root.next_key, &bootstrap, &conf)?;
        self.insert_mapping(mapping, cache, seed)?
            .as_cell()
            .cloned()
    }

    fn create_cells(
        &mut self,
        bootstrap: CellGroupBootstrap,
    ) -> Result<GroupLog<GroupHistory>, LogError> {
        let conf = bootstrap.configuration()?;
        if let Some(group) = self
            .groups
            .read()
            .map_err(|_| LogError::Closed)?
            .get(&bootstrap.route.history)
        {
            let group = group.as_cells()?;
            let expected = MappingRecord::new_cells(group.key, &bootstrap, &conf)?;
            if self.mappings.get(&group.key) != Some(&expected) {
                return Err(LogError::WrongIdentity);
            }
            return Ok(group.clone());
        }
        let mapping = MappingRecord::new_cells(self.root.next_key, &bootstrap, &conf)?;
        let mut cache = genesis(conf);
        cache.group_manifest = Some(
            super::cell_groups::GroupManifest::genesis(bootstrap.route)
                .map_err(|error| LogError::Invalid(error.to_string()))?,
        );
        self.insert_mapping(mapping, cache, None)?
            .as_cells()
            .cloned()
    }

    fn insert_mapping(
        &mut self,
        mapping: MappingRecord,
        cache: CachedGroup,
        seed: Option<&super::snapshot::PreparedSeed>,
    ) -> Result<StoredGroup, LogError> {
        if self.mappings.len() >= self.options.max_groups {
            return Err(LogError::Overloaded);
        }
        let history = mapping.history()?;
        {
            let groups = self.groups.read().map_err(|_| LogError::Closed)?;
            if groups.keys().any(|h| h.group == history.group)
                || groups
                    .values()
                    .any(|group| group.is_cells() != mapping.is_cells())
            {
                return Err(LogError::WrongIdentity);
            }
        }
        let key = self.root.next_key;
        if mapping.key != key {
            return Err(LogError::WrongIdentity);
        }
        let next_key = key.checked_add(1).ok_or(LogError::Overloaded)?;
        cache.validate(self.options.max_entries_per_group)?;
        let bytes = cache.bytes();
        if self
            .cache_bytes
            .checked_add(bytes)
            .is_none_or(|total| total > self.options.max_cache_bytes)
        {
            return Err(LogError::Overloaded);
        }
        let mut batch = LogBatch::default();
        batch
            .put(0, mapping.catalog_key()?, records::encode(&mapping)?)
            .map_err(engine_error)?;
        batch
            .put(
                key,
                GROUP_KEY.to_vec(),
                records::encode(&GroupRecord::from_cache(mapping.clone(), &cache)?)?,
            )
            .map_err(engine_error)?;
        if let Some(seed) = seed {
            seed.retain_pending()
                .map_err(|e| LogError::Io(e.to_string()))?;
        }
        let previous_key = self.root.next_key;
        self.root.next_key = next_key;
        if let Err(error) = self.fence(&mut batch) {
            self.root.next_key = previous_key;
            return Err(error);
        }
        if let Some(seed) = seed {
            seed.retain().map_err(|e| LogError::Io(e.to_string()))?;
        }
        let handle = self.handle(&mapping, cache)?;
        self.groups
            .write()
            .map_err(|_| LogError::Closed)?
            .insert(history, handle.clone());
        self.mappings.insert(key, mapping);
        self.cache_bytes += bytes;
        self.cache_usage.store(self.cache_bytes, Ordering::Release);
        Ok(handle)
    }

    fn persist<S: Clone>(
        &mut self,
        operation: OperationId,
        writes: Vec<GroupWrite<S>>,
    ) -> Result<SyncReceipt<S>, LogError> {
        let mut unique = BTreeSet::new();
        let mut candidates = Vec::with_capacity(writes.len());
        let mut next_bytes = self.cache_bytes;
        let mut batch = LogBatch::default();
        for write in &writes {
            if !Arc::ptr_eq(&write.group.owner, &self.owner) || !unique.insert(write.group.key) {
                return Err(LogError::WrongIdentity);
            }
            let mapping = self
                .mappings
                .get(&write.group.key)
                .ok_or(LogError::WrongIdentity)?;
            let old = write.group.read()?;
            let next = old.changed(
                write,
                self.options.max_entries_per_group,
                mapping.history()?,
            )?;
            next_bytes = next_bytes
                .checked_sub(old.bytes())
                .and_then(|size| size.checked_add(next.bytes()))
                .ok_or(LogError::Overloaded)?;
            if next_bytes > self.options.max_cache_bytes {
                return Err(LogError::Overloaded);
            }
            drop(old);
            if let Some(change) = &write.snapshot {
                match change {
                    SnapshotChange::Install { .. } => {
                        batch.add_command(write.group.key, Command::Clean)
                    }
                    SnapshotChange::Checkpoint(snapshot) => batch.add_command(
                        write.group.key,
                        Command::Compact {
                            index: snapshot.get_metadata().index + 1,
                        },
                    ),
                }
            }
            batch
                .add_entries::<EntryCodec>(write.group.key, &write.entries)
                .map_err(engine_error)?;
            batch
                .put(
                    write.group.key,
                    GROUP_KEY.to_vec(),
                    records::encode(&GroupRecord::from_cache(mapping.clone(), &next)?)?,
                )
                .map_err(engine_error)?;
            candidates.push(next);
        }
        // Retain both closures before any engine I/O. Failed preflight drops
        // unarmed reservations; once armed, an unknown outcome keeps both roots
        // and quarantines collection until recovery reconstructs the selection.
        let mut publications = Vec::new();
        for write in &writes {
            if let Some(prepared) = &write.group_manifest {
                let previous = write.group.group_manifest()?;
                publications.push(
                    prepared
                        .begin_publication(&write.group, previous.as_ref())
                        .map_err(|e| LogError::Invalid(e.to_string()))?,
                );
            }
        }
        for publication in &mut publications {
            publication.arm();
        }
        let sequence = self.fence(&mut batch)?;
        // No cache write lock is held during engine I/O. Readers may continue
        // to observe the previous durable prefix while a sync is stalled.
        for (write, next) in writes.iter().zip(candidates) {
            let mut cache = write
                .group
                .cache
                .write()
                .map_err(|_| LogError::Corrupt("cache publication lock poisoned".into()))?;
            *cache = next;
            // Publish invalidation while still holding the cache write lock.
            // A reader observing this revision can only acquire the new cache;
            // an older revision observed before this publication is refreshed
            // on its next lookup. Failed syncs never advance the revision.
            write
                .group
                .cache_revision
                .store(sequence, Ordering::Release);
        }
        for publication in publications {
            publication
                .commit()
                .map_err(|e| LogError::Corrupt(e.to_string()))?;
        }
        self.cache_bytes = next_bytes;
        self.cache_usage.store(self.cache_bytes, Ordering::Release);
        self.performance
            .increment(MeasuredCounter::PersistGroups, writes.len() as u64);
        for write in &writes {
            self.performance
                .increment(MeasuredCounter::PersistEntries, write.entries.len() as u64);
            for entry in &write.entries {
                self.performance.increment(
                    MeasuredCounter::PersistEntryBytes,
                    u64::from(entry.compute_size()),
                );
            }
        }
        Ok(SyncReceipt {
            operation,
            sequence,
            groups: writes
                .into_iter()
                .map(|write| write.group.identity)
                .collect(),
        })
    }

    fn purge(&mut self) -> Result<(), LogError> {
        let timer = self.performance.timer(Measured::EnginePurge);
        let result = (|| {
            // Engine rewriting preserves live entries for every group. Returned
            // candidates are hints, never authority to compact unapplied history.
            let candidates = self.engine.purge_expired_files().map_err(engine_error)?;
            self.fence(&mut LogBatch::default())?;
            // Revisit cold groups the engine could not yet rewrite. Otherwise
            // an idle engine does no further periodic I/O until another write.
            self.purge_pending = !candidates.is_empty();
            Ok(())
        })();
        timer.finish(result.is_ok());
        result
    }

    fn run(
        &mut self,
        receiver: mpsc::Receiver<Request>,
        domain: &crate::asyncrt::DomainToken,
        abort: &AtomicBool,
        closing: &AtomicBool,
    ) -> Result<(), LogError> {
        let mut purge_at = domain
            .mono_ms()
            .map_err(|_| LogError::Closed)?
            .saturating_add(self.options.purge_interval_ms.unwrap_or(0));
        loop {
            if abort.load(Ordering::Acquire)
                || self.closed.load(Ordering::Acquire)
                || domain.mono_ms().is_err()
            {
                return Err(LogError::Closed);
            }
            if !closing.load(Ordering::Acquire) && self.purge_pending {
                if let Some(interval) = self.options.purge_interval_ms {
                    let now = domain.mono_ms().map_err(|_| LogError::Closed)?;
                    if now >= purge_at {
                        self.purge()?;
                        purge_at = domain
                            .mono_ms()
                            .map_err(|_| LogError::Closed)?
                            .saturating_add(interval);
                    }
                }
            }
            let request = match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(request) => request,
                Err(mpsc::RecvTimeoutError::Timeout) if closing.load(Ordering::Acquire) => {
                    return self.fence(&mut LogBatch::default()).map(|_| ())
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return Err(LogError::Closed),
            };
            request.queued.finish(true);
            let fatal = match request.work {
                Work::NodeFence(reply) => {
                    let result = self.fence(&mut LogBatch::default());
                    let fatal = result.as_ref().err().filter(|error| error.fatal()).cloned();
                    if fatal.is_some() {
                        self.closed.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                    fatal
                }
                Work::Lifecycle(update, reply) => {
                    let result = match domain.mono_ms() {
                        Ok(now) if now < update.deadline() => self.change_lifecycle(*update),
                        Ok(_) => Err(LogError::Invalid("expired node lifecycle command".into())),
                        Err(_) => Err(LogError::Closed),
                    };
                    let fatal = result.as_ref().err().filter(|error| error.fatal()).cloned();
                    if fatal.is_some() {
                        self.closed.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                    fatal
                }
                Work::Create(bootstrap, binding, reply) => {
                    let result = self.create(bootstrap, binding.map(|binding| *binding));
                    let fatal = result.as_ref().err().filter(|error| error.fatal()).cloned();
                    if fatal.is_some() {
                        self.closed.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                    fatal
                }
                Work::CreateCells(bootstrap, reply) => {
                    let result = self.create_cells(bootstrap);
                    let fatal = result.as_ref().err().filter(|error| error.fatal()).cloned();
                    if fatal.is_some() {
                        self.closed.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                    fatal
                }
                Work::PersistCells(operation, writes, reply) => {
                    let timer = self.performance.timer(Measured::EnginePersist);
                    let result = self.persist(operation, writes);
                    timer.finish(result.is_ok());
                    let fatal = result.as_ref().err().filter(|error| error.fatal()).cloned();
                    if fatal.is_some() {
                        self.closed.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                    fatal
                }
                Work::Seeded(seed, reply) => {
                    let result = self.seeded(*seed);
                    let fatal = result.as_ref().err().filter(|error| error.fatal()).cloned();
                    if fatal.is_some() {
                        self.closed.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                    fatal
                }
                Work::Persist(operation, writes, reply) => {
                    let timer = self.performance.timer(Measured::EnginePersist);
                    let result = self.persist(operation, writes);
                    timer.finish(result.is_ok());
                    let fatal = result.as_ref().err().filter(|error| error.fatal()).cloned();
                    if fatal.is_some() {
                        self.closed.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                    fatal
                }
                Work::Purge(reply) => {
                    let result = self.purge();
                    purge_at = domain
                        .mono_ms()
                        .map_err(|_| LogError::Closed)?
                        .saturating_add(self.options.purge_interval_ms.unwrap_or(0));
                    let fatal = result.as_ref().err().cloned();
                    if fatal.is_some() {
                        self.closed.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                    fatal
                }
            };
            if let Some(error) = fatal {
                return Err(error);
            }
        }
    }
}

fn create_directory_durably(
    fs: &dyn FileSystem,
    directory: &std::path::Path,
) -> Result<(), LogError> {
    let mut missing = Vec::new();
    let mut path = directory;
    loop {
        match fs.metadata(path) {
            Ok(metadata) if metadata.is_dir => break,
            Ok(_) => {
                return Err(LogError::Invalid(
                    "Raft directory ancestor is not a directory".into(),
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(path.to_path_buf())
            }
            Err(error) => return Err(error.into()),
        }
        let parent = parent_directory(path);
        if parent == path || missing.len() > 256 {
            return Err(LogError::Invalid(
                "Raft directory has no usable ancestor".into(),
            ));
        }
        path = parent;
    }
    fs.create_dir_all(directory)?;
    // A directory fsync covers its entries, not the name linking it from its
    // parent. Every newly created ancestor needs both sides of that boundary.
    for path in missing {
        fs.sync_all(&path)?;
        fs.sync_all(parent_directory(&path))?;
    }
    Ok(())
}

fn parent_directory(path: &std::path::Path) -> &std::path::Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."))
}
