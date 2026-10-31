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
}

pub(super) async fn open(
    registration: &CellGroupArchiveRegistration,
    runtime: &CellGroupRuntime,
    execution: &ControllerExecution,
) -> Result<Context> {
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

async fn select(
    context: &Context,
    execution: &ControllerExecution,
    record: &mut Record,
    coverage: CellGroupArchiveCoverage,
) -> Result<CellGroupArchiveCoverage> {
    let head = coverage.hold().clone();
    if record.retire.is_some() {
        return Err(Error::Invalid("unfinished archive hold retirement"));
    }
    record.retire = record.head.replace(head);
    record.pending = Pending::None;
    store(context, execution, record).await?;
    retire(context, execution, record).await?;
    Ok(coverage)
}

async fn retire(
    context: &Context,
    execution: &ControllerExecution,
    record: &mut Record,
) -> Result<()> {
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
            return Err(CellGroupArchiveError::Busy.into());
        }
        context.registry.release(execution, previous).await?;
        record.retire = None;
        store(context, execution, record).await?;
    }
    Ok(())
}

pub(super) async fn drive(
    context: &Context,
    registration: &CellGroupArchiveRegistration,
    runtime: &CellGroupRuntime,
    execution: &ControllerExecution,
) -> Result<CellGroupArchiveCoverage> {
    registration
        .directory
        .require_current_group_birth(&registration.birth)
        .await
        .map_err(CellGroupArchiveBindingError::from)?;
    let mut record = load(context, execution).await?;
    retire(context, execution, &mut record).await?;
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
            .status(registration.birth.route().history)
            .await
            .map_err(Error::Runtime)?;
        if let Some(head) = &record.head {
            if head.marker().position().index >= current.applied.index {
                let coverage = context
                    .registry
                    .verify_hold(
                        execution,
                        head,
                        &registration.tails,
                        receiver(registration)?,
                    )
                    .await?;
                return Ok(coverage);
            }
        }
        let intent = context.registry.prepare_upload(execution).await?;
        record.pending = Pending::Reserve(intent);
        store(context, execution, &record).await?;
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
    let uploaded = upload
        .upload_cut(execution, &registration.tails, source)
        .await?;
    let publication = upload
        .prepare_publication(
            execution,
            &registration.tails,
            &uploaded,
            receiver(registration)?,
        )
        .await?;
    record.pending = Pending::Publish {
        ticket,
        intent: publication.intent().clone(),
    };
    store(context, execution, &record).await?;
    let coverage = publication.commit(execution).await?;
    select(context, execution, &mut record, coverage).await
}
