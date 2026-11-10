// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Each remote mutation follows its exact durable intent. Restart either finds
//! that publication or fences its pending ticket before allocating again.
use super::*;
use journal::{Journal, Pending, Record};

#[derive(Clone)]
pub(super) struct Context {
    pub journal: Arc<Journal>,
    pub registry: CellGroupArchiveRegistry,
    pub uses: Arc<Mutex<Vec<crate::raft::cell_groups::archive::CoverageUse>>>,
    pub authority: Arc<Authority>,
}

pub(super) struct Authority {
    directory: RangeDirectory,
    birth: CellGroupBirth,
}

impl Authority {
    pub(super) async fn require_current(&self) -> Result<()> {
        self.directory
            .require_current_group_birth(&self.birth)
            .await
            .map_err(|error| Error::Binding(error.into()))
    }
}

pub(super) async fn open(
    registration: &CellGroupArchiveRegistration,
    runtime: &CellGroupRuntime,
    execution: &ControllerExecution,
    activation: Arc<crate::raft::cell_groups::archive_compaction::ArchiveActivation>,
) -> Result<Context> {
    runtime
        .log()
        .require_cell_group_archive(&registration.birth.route().history, activation)
        .await
        .map_err(|error| match error {
            crate::raft::storage::LogError::Overloaded => Error::Runtime(RuntimeError::Overloaded),
            error => Error::Runtime(error.into()),
        })?;
    let registry = registration
        .directory
        .open_group_archive(execution, &registration.io, &registration.birth)
        .await?;
    let filesystem = execution
        .run(async { crate::asyncrt::fs() })
        .await
        .map_err(|_| Error::Stopped)?;
    let history = registration.birth.route().history;
    let mut hash = sha2::Sha256::new();
    use sha2::Digest;
    hash.update(b"cellar-node-group-archive-journal-1");
    for bytes in [
        history.cluster.bytes(),
        history.namespace.bytes(),
        history.group.bytes(),
        history.incarnation.bytes(),
    ] {
        hash.update(bytes);
    }
    let directory = runtime
        .log()
        .limits()
        .directory
        .join("cell-group-archive-journals")
        .join(hex::encode(hash.finalize()));
    let engine = runtime
        .log()
        .storage_instance()
        .map_err(|error| Error::Runtime(RuntimeError::Storage(error)))?
        .bytes();
    let creation = registry.creation().clone();
    let journal = execution
        .blocking(move || Journal::open(filesystem, directory, engine, creation))
        .map_err(|_| Error::Stopped)?
        .await
        .map_err(Error::Worker)??;
    Ok(Context {
        journal: Arc::new(journal),
        registry,
        uses: Arc::new(Mutex::new(Vec::new())),
        authority: Arc::new(Authority {
            directory: registration.directory.clone(),
            birth: registration.birth.clone(),
        }),
    })
}

async fn load(context: &Context, execution: &ControllerExecution) -> Result<Record> {
    let journal = context.journal.clone();
    execution
        .blocking(move || journal.load())
        .map_err(|_| Error::Stopped)?
        .await
        .map_err(Error::Worker)?
}

async fn store(context: &Context, execution: &ControllerExecution, record: &Record) -> Result<()> {
    let journal = context.journal.clone();
    let record = record.clone();
    execution
        .blocking(move || journal.store(&record))
        .map_err(|_| Error::Stopped)?
        .await
        .map_err(Error::Worker)?
}

fn receiver(
    registration: &CellGroupArchiveRegistration,
) -> Result<CellGroupArchiveCheckpointReceiver> {
    Ok(registration
        .checkpoints
        .receive_archive(
            registration.store.clone(),
            registration.scratch.clone(),
            registration.scratch_directory.clone(),
        )
        .map_err(CellGroupArchiveError::from)?)
}

pub(super) struct Driven {
    pub coverage: CellGroupArchiveCoverage,
    pub retirement_pending: bool,
}

async fn select(
    context: &Context,
    execution: &ControllerExecution,
    record: &mut Record,
    coverage: CellGroupArchiveCoverage,
) -> Result<Driven> {
    let head = coverage.hold().clone();
    if record.retire.is_some() {
        return Err(Error::Invalid("unfinished archive hold retirement"));
    }
    record.retire = record.head.replace(head);
    record.pending = Pending::None;
    store(context, execution, record).await?;
    // The new head is already durable and independently verified. An earlier
    // output can retain the previous hold without preventing use of this head.
    // Its pending retirement still blocks allocation of another archive below.
    let allocation_ready = retire(context, execution, record).await?;
    Ok(Driven {
        coverage,
        retirement_pending: !allocation_ready,
    })
}

/// Release an unused previous hold. False leaves its journaled ownership intact;
/// it prevents another archive allocation, not verification of the current head.
async fn retire(
    context: &Context,
    execution: &ControllerExecution,
    record: &mut Record,
) -> Result<bool> {
    if let Some(previous) = &record.retire {
        let retained = {
            let mut uses = context
                .uses
                .lock()
                .map_err(|_| Error::Invalid("archive use lock"))?;
            uses.retain(|usage| usage.live());
            uses.iter().any(|usage| usage.retains(previous))
        };
        if retained {
            return Ok(false);
        }
        context.registry.release(execution, previous).await?;
        record.retire = None;
        store(context, execution, record).await?;
    }
    Ok(true)
}

pub(super) async fn drive(
    context: &Context,
    registration: &CellGroupArchiveRegistration,
    runtime: &CellGroupRuntime,
    execution: &ControllerExecution,
    prove_outputs: bool,
) -> Result<Driven> {
    let node = runtime.log().limits().node;
    let group = registration.birth.route().history.group;
    tracing::debug!(target: "cellar::raft::group_archive", ?node, ?group, stage = "drive");
    registration
        .directory
        .require_current_group_birth(&registration.birth)
        .await
        .map_err(CellGroupArchiveBindingError::from)?;
    let mut record = load(context, execution).await?;
    let allocation_ready = retire(context, execution, &mut record).await?;
    if !allocation_ready && (!prove_outputs || !matches!(record.pending, Pending::None)) {
        return Err(CellGroupArchiveError::Busy.into());
    }
    if let Pending::Publish { ticket, intent } = record.pending.clone() {
        match context
            .registry
            .reconcile_publication(
                execution,
                &intent,
                &registration.tails,
                receiver(registration)?,
            )
            .await
        {
            Ok(coverage) => return select(context, execution, &mut record, coverage).await,
            Err(CellGroupArchiveError::Expired) => (),
            Err(error) => return Err(error.into()),
        }
        // Absence alone does not disprove a delayed publishing CAS. Abort the
        // exact ticket to invalidate its old conditional version, then reconcile
        // once more in case publication won the race with that abort.
        context.registry.abort(execution, &ticket).await?;
        match context
            .registry
            .reconcile_publication(
                execution,
                &intent,
                &registration.tails,
                receiver(registration)?,
            )
            .await
        {
            Ok(coverage) => return select(context, execution, &mut record, coverage).await,
            Err(CellGroupArchiveError::Expired) => (),
            Err(error) => return Err(error.into()),
        }
        record.pending = Pending::None;
        store(context, execution, &record).await?;
    }
    if matches!(record.pending, Pending::None) {
        let current = runtime
            .log()
            .cell_group(&registration.birth.route().history)
            .and_then(|group| group.group_manifest())
            .map_err(|error| Error::Runtime(error.into()))?
            .ok_or(Error::Runtime(RuntimeError::NotReady))?;
        if let Some(head) = &record.head {
            // A queued output may already be covered even when later group
            // entries exist. The receipt verifier still checks its exact
            // position, cell version, witness and stored result against this
            // prefix; an uncovered output keeps waiting for allocation.
            if !allocation_ready || head.marker().position().index >= current.position().index {
                let coverage = context
                    .registry
                    .verify_hold(
                        execution,
                        head,
                        &registration.tails,
                        receiver(registration)?,
                    )
                    .await?;
                return Ok(Driven {
                    coverage,
                    retirement_pending: !allocation_ready,
                });
            }
        }
        // Retained output proofs may delay the previous hold's release. Reuse
        // above requires no new archive, but acquisition/upload must preserve
        // the existing current-plus-retiring bound.
        if !allocation_ready {
            return Err(CellGroupArchiveError::Busy.into());
        }
        // Another voter may have published this exact applied manifest while
        // our publication was in flight. Pin and independently verify its cut
        // before settling any receipts or granting local reclamation.
        record.pending = match context.registry.prepare_acquire(execution).await {
            Ok(intent) if intent.acquire_marker()? == &current => Pending::Acquire(intent),
            Ok(_) | Err(CellGroupArchiveError::Unconfirmed) => {
                Pending::Reserve(context.registry.prepare_upload(execution).await?)
            }
            Err(error) => return Err(error.into()),
        };
        store(context, execution, &record).await?;
    }
    if let Pending::Acquire(intent) = &record.pending {
        let hold = match context.registry.acquire(execution, intent).await {
            Ok(hold) => hold,
            Err(error @ (CellGroupArchiveError::Expired | CellGroupArchiveError::Conflict)) => {
                if matches!(
                    context.registry.acquire_outcome(execution, intent).await?,
                    CellGroupArchiveAcquireOutcome::Superseded
                ) {
                    record.pending = Pending::None;
                    store(context, execution, &record).await?;
                }
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        let coverage = context
            .registry
            .verify_hold(
                execution,
                &hold,
                &registration.tails,
                receiver(registration)?,
            )
            .await?;
        return select(context, execution, &mut record, coverage).await;
    }
    if let Pending::Reserve(intent) = &record.pending {
        match context.registry.reserve(execution, intent).await {
            Ok(ticket) => record.pending = Pending::Upload(ticket),
            Err(CellGroupArchiveError::Expired | CellGroupArchiveError::Conflict) => {
                record.pending = Pending::None;
                store(context, execution, &record).await?;
                return Err(CellGroupArchiveError::Expired.into());
            }
            Err(error) => return Err(error.into()),
        }
        store(context, execution, &record).await?;
    }
    let Pending::Upload(ticket) = record.pending else {
        return Err(Error::Invalid("archive upload stage"));
    };
    let upload = context
        .registry
        .open_upload(
            execution,
            &registration.owners,
            &ticket,
            &registration.tails,
        )
        .await?;
    let source = runtime
        .capture_checkpoint(
            registration.birth.route().history,
            &registration.checkpoints,
        )
        .await
        .map_err(CellGroupArchiveError::from)?;
    tracing::debug!(target: "cellar::raft::group_archive", ?node, ?group, stage = "captured",
        index = source.marker().position().index.get());
    let uploaded = upload
        .upload_cut(execution, &registration.tails, source)
        .await?;
    tracing::debug!(target: "cellar::raft::group_archive", ?node, ?group, stage = "uploaded");
    let publication = upload
        .prepare_publication(
            execution,
            &registration.tails,
            &uploaded,
            receiver(registration)?,
        )
        .await?;
    tracing::debug!(target: "cellar::raft::group_archive", ?node, ?group, stage = "verified");
    record.pending = Pending::Publish {
        ticket,
        intent: publication.intent().clone(),
    };
    store(context, execution, &record).await?;
    let coverage = publication.commit(execution).await?;
    tracing::debug!(target: "cellar::raft::group_archive", ?node, ?group, stage = "published");
    select(context, execution, &mut record, coverage).await
}
