// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A fresh quorum seed owns one journaled upload generation through handoff.
//! Transfer data never publishes a recovery head or supplies S3 output proof.
use super::*;
use crate::raft::cell_groups::controller::provision::Config;
use std::sync::{atomic::AtomicBool, Weak};

#[derive(Clone)]
pub(crate) struct Client(Weak<Shared>);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Descriptor {
    pub ticket: CellGroupArchiveUploadTicket,
    pub checkpoint: CellGroupArchiveReference,
}

pub(crate) struct Source {
    authority: CellGroupSeedAuthority,
    snapshot: ::raft::prelude::Snapshot,
    descriptor: Descriptor,
    lease: Arc<Lease>,
    jobs: Arc<Jobs>,
}

impl Source {
    pub(crate) fn check(&self) -> Result<()> {
        if self.jobs.stopping.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        self.authority.check().map_err(Error::Runtime)
    }

    pub(crate) fn snapshot(&self) -> &::raft::prelude::Snapshot {
        &self.snapshot
    }

    pub(crate) fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    pub(crate) fn barrier(&self) -> LogIndex {
        self.authority.barrier().index
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.lease.released.store(true, Ordering::Release);
        self.lease.changed.notify_waiters();
    }
}

#[derive(Default)]
struct Lease {
    released: AtomicBool,
    changed: Notify,
}

impl Lease {
    async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Default)]
struct State {
    active: usize,
    groups: BTreeMap<GroupHistory, Arc<tokio::sync::Mutex<()>>>,
    cleanup_errors: BTreeMap<GroupHistory, Arc<Error>>,
}

#[derive(Default)]
pub(in crate::raft::cell_groups::controller) struct Jobs {
    stopping: AtomicBool,
    state: Mutex<State>,
    finished: Notify,
}

struct Entered(Arc<Jobs>);

impl Drop for Entered {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active -= 1;
        self.0.finished.notify_waiters();
    }
}

impl Jobs {
    fn enter(
        self: &Arc<Self>,
        history: GroupHistory,
        max_groups: usize,
    ) -> Result<(Entered, Arc<tokio::sync::Mutex<()>>)> {
        let mut state = self.state.lock().map_err(|_| Error::Stopped)?;
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if !state.groups.contains_key(&history) && state.groups.len() >= max_groups {
            return Err(Error::Runtime(RuntimeError::Overloaded));
        }
        let group = state.groups.entry(history).or_default().clone();
        state.active += 1;
        Ok((Entered(self.clone()), group))
    }

    pub(in crate::raft::cell_groups::controller) fn stop(&self) {
        let _gate = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.stopping.store(true, Ordering::Release);
    }

    pub(in crate::raft::cell_groups::controller) async fn join(&self) {
        loop {
            let finished = self.finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            if self.state.lock().unwrap_or_else(|e| e.into_inner()).active == 0 {
                return;
            }
            finished.await;
        }
    }

    pub(in crate::raft::cell_groups::controller) fn usage(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        (state.active, state.cleanup_errors.len())
    }
}

impl Client {
    pub(in crate::raft::cell_groups::controller) fn new(shared: &Arc<Shared>) -> Self {
        Self(Arc::downgrade(shared))
    }

    /// The private transport caller retains its already-admitted source lane,
    /// wire workspace and metadata reservation throughout this owned operation.
    pub(crate) async fn prepare(
        &self,
        authority: CellGroupSeedAuthority,
        admitted_transport: Arc<dyn Send + Sync>,
    ) -> std::result::Result<Source, Arc<Error>> {
        let shared = self.0.upgrade().ok_or_else(|| Arc::new(Error::Stopped))?;
        let admitted = (|| -> Result<_> {
            if shared.closing.load(Ordering::Acquire)
                || !authority.belongs_to_engine(&shared.runtime.log().owner())
            {
                return Err(Error::Stopped);
            }
            let history = authority.target().scope;
            if !shared.groups.contains(&history).map_err(Error::Runtime)? {
                return Err(Error::Runtime(RuntimeError::NotFound));
            }
            let config = shared
                .provisioning
                .lock()
                .map_err(|_| Error::Stopped)?
                .clone()
                .ok_or(Error::Invalid("archive seed provisioning unavailable"))?;
            if config.archives.is_none() {
                return Err(Error::Invalid("archive seed resources unavailable"));
            }
            let permit = shared
                .s3
                .acquire(8192)
                .ok_or(Error::Runtime(RuntimeError::Overloaded))?;
            let (entered, group) = shared
                .archive_seeds
                .enter(history, shared.limits.max_groups)?;
            Ok((config, permit, entered, group, history))
        })()
        .map_err(Arc::new)?;
        let (config, permit, entered, group, history) = admitted;
        let runtime = shared.runtime.clone();
        let jobs = shared.archive_seeds.clone();
        let domain = shared.domain.clone();
        let (sender, receiver) = oneshot::channel();
        domain
            .spawn_cleanup(async move {
                let _entered = entered;
                let _permit = permit;
                let _transport = admitted_transport;
                let _group = group.lock_owned().await;
                let execution = ControllerExecution::new(runtime.log().domain());
                let outcome = std::panic::AssertUnwindSafe(async {
                    let result = Box::pin(prepare(&config, &runtime, authority, &execution)).await;
                    // A cancelled network waiter cannot release an entered producer or
                    // journal write. Join before reading its final intent for cleanup.
                    execution.join().await;
                    match result {
                        Ok((authority, snapshot, descriptor, journal, registry)) => {
                            let lease = Arc::new(Lease::default());
                            let source = Source {
                                authority,
                                snapshot,
                                descriptor,
                                lease: lease.clone(),
                                jobs: jobs.clone(),
                            };
                            let _ = sender.send(Ok(source));
                            lease.wait().await;
                            let cleanup = retire(&journal, &registry, &execution).await;
                            record_cleanup(&jobs, history, cleanup);
                        }
                        Err(failure) => {
                            let (error, journal) = *failure;
                            let _ = sender.send(Err(Arc::new(error)));
                            if let Some((journal, registry)) = journal {
                                let cleanup = retire(&journal, &registry, &execution).await;
                                record_cleanup(&jobs, history, cleanup);
                            }
                        }
                    }
                })
                .catch_unwind()
                .await;
                execution.join().await;
                if outcome.is_err() {
                    record_cleanup(
                        &jobs,
                        history,
                        Err(Error::Invalid("archive seed worker panicked")),
                    );
                }
            })
            .map_err(|_| Arc::new(Error::Stopped))?
            .detach();
        receiver.await.map_err(|_| Arc::new(Error::Stopped))?
    }
}

fn record_cleanup(jobs: &Jobs, history: GroupHistory, result: Result<()>) {
    if let Err(error) = &result {
        tracing::warn!(%error, "archive seed cleanup retained its durable intent");
    }
    let mut state = jobs.state.lock().unwrap_or_else(|e| e.into_inner());
    if let Err(error) = result {
        state.cleanup_errors.insert(history, Arc::new(error));
    } else {
        state.cleanup_errors.remove(&history);
    }
}

type JournalOwner = (Arc<journal::Journal>, CellGroupArchiveRegistry);
type PrepareFailure = Box<(Error, Option<JournalOwner>)>;

fn failed(error: Error, journal: Option<JournalOwner>) -> PrepareFailure {
    Box::new((error, journal))
}

type Prepared = (
    CellGroupSeedAuthority,
    ::raft::prelude::Snapshot,
    Descriptor,
    Arc<journal::Journal>,
    CellGroupArchiveRegistry,
);

async fn prepare(
    config: &Config,
    runtime: &CellGroupRuntime,
    authority: CellGroupSeedAuthority,
    execution: &ControllerExecution,
) -> std::result::Result<Prepared, PrepareFailure> {
    let archives = config
        .archives
        .as_ref()
        .ok_or_else(|| failed(Error::Stopped, None))?;
    let history = authority.target().scope;
    let deadline = authority.deadline();
    let pool = archives.checkpoints.clone();
    // Capture before catalog I/O. The original exact-root selection and all
    // existing fresh-quorum/membership checks remain unchanged.
    let captured = execution
        .blocking(move || {
            let source = authority.capture(&pool)?;
            let snapshot = source.snapshot()?;
            let (authority, checkpoint) = source.into_archive_parts()?;
            Ok::<_, CellGroupCheckpointError>((authority, checkpoint, snapshot))
        })
        .map_err(|_| failed(Error::Stopped, None))?
        .await
        .map_err(|e| failed(Error::Worker(e), None))?
        .map_err(|e| failed(Error::Archive(e.into()), None))?;
    let (authority, checkpoint, snapshot) = captured;
    let profile = archives
        .namespaces
        .iter()
        .find(|p| {
            let n = p.directory.namespace_config();
            n.cluster == history.cluster && n.namespace == history.namespace
        })
        .ok_or_else(|| failed(Error::Invalid("archive seed namespace"), None))?;
    let birth = profile
        .directory
        .materialize_selected_history(history)
        .await
        .map_err(|e| failed(Error::Binding(e.into()), None))?;
    if birth.route() != authority.marker().route() {
        return Err(failed(Error::Invalid("archive seed birth"), None));
    }
    let registry = profile
        .directory
        .open_group_archive(execution, &archives.io, &birth)
        .await
        .map_err(|e| failed(Error::Binding(e), None))?;
    let filesystem = execution
        .run(async { crate::asyncrt::fs() })
        .await
        .map_err(|_| failed(Error::Stopped, None))?;
    let mut hash = sha2::Sha256::new();
    use sha2::Digest;
    for bytes in [
        history.cluster.bytes(),
        history.namespace.bytes(),
        history.group.bytes(),
        history.incarnation.bytes(),
    ] {
        hash.update(bytes);
    }
    let path = runtime
        .log()
        .limits()
        .directory
        .join("cell-group-seed-archives")
        .join(hex::encode(hash.finalize()));
    let creation = registry.creation().clone();
    let engine = runtime
        .log()
        .storage_instance()
        .map_err(|e| failed(Error::Runtime(e.into()), None))?
        .bytes();
    let journal = Arc::new(
        execution
            .blocking(move || journal::Journal::open_seed(filesystem, path, engine, creation))
            .map_err(|_| failed(Error::Stopped, None))?
            .await
            .map_err(|e| failed(Error::Worker(e), None))?
            .map_err(|e| failed(e, None))?,
    );
    let owner = (journal.clone(), registry.clone());
    let transfer = async {
        retire(&journal, &registry, execution).await?;
        authority.check().map_err(Error::Runtime)?;
        let intent = registry.prepare_upload(execution).await?;
        let saved = intent.clone();
        let j = journal.clone();
        execution
            .blocking(move || {
                j.store(&journal::Record {
                    pending: journal::Pending::Reserve(saved),
                    ..Default::default()
                })
            })?
            .await
            .map_err(Error::Worker)??;
        let ticket = registry.reserve(execution, &intent).await?;
        let j = journal.clone();
        execution
            .blocking(move || {
                j.store(&journal::Record {
                    pending: journal::Pending::Upload(ticket),
                    ..Default::default()
                })
            })?
            .await
            .map_err(Error::Worker)??;
        let upload = registry
            .open_upload(execution, &archives.owners, &ticket, &archives.tails)
            .await?;
        let reference = upload.upload_checkpoint(execution, checkpoint).await?;
        authority.check().map_err(Error::Runtime)?;
        upload.require_pending(execution).await?;
        Ok::<_, Error>(Descriptor {
            ticket,
            checkpoint: reference,
        })
    };
    let timer = runtime
        .log()
        .domain()
        .sleep_until(deadline)
        .map_err(|_| failed(Error::Stopped, Some(owner.clone())))?;
    let result = crate::asyncrt::select! {
        _ = timer => Err(Error::Runtime(RuntimeError::DeadlineExceeded)),
        result = transfer => result,
    };
    result
        .map(|descriptor| (authority, snapshot, descriptor, journal, registry))
        .map_err(|e| failed(e, Some(owner)))
}

async fn retire(
    journal: &Arc<journal::Journal>,
    registry: &CellGroupArchiveRegistry,
    execution: &ControllerExecution,
) -> Result<()> {
    let j = journal.clone();
    let record = execution
        .blocking(move || j.load())?
        .await
        .map_err(Error::Worker)??;
    if record.head.is_some() || record.retire.is_some() {
        return Err(Error::Invalid("archive seed journal purpose"));
    }
    let ticket = match record.pending {
        journal::Pending::None => return Ok(()),
        journal::Pending::Upload(ticket) => Some(ticket),
        journal::Pending::Reserve(intent) => match registry.reserve(execution, &intent).await {
            Ok(ticket) => Some(ticket),
            Err(CellGroupArchiveError::Conflict | CellGroupArchiveError::Expired) => None,
            Err(error) => return Err(error.into()),
        },
        journal::Pending::Publish { .. } => {
            return Err(Error::Invalid("archive seed publication intent"))
        }
    };
    if let Some(ticket) = ticket {
        registry.abort(execution, &ticket).await?;
    }
    let j = journal.clone();
    execution
        .blocking(move || j.store(&journal::Record::default()))?
        .await
        .map_err(Error::Worker)??;
    Ok(())
}
