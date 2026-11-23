// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Each remote mutation follows its exact durable intent. Restart either finds
//! that publication or fences its pending ticket before allocating again.
use super::reuse::{Admission, Eligibility};
use super::*;
use crate::raft::cell_groups::archive::CellGroupArchiveReservationRenewal;
use journal::{Journal, Pending, Record};

#[derive(Clone)]
pub(super) struct Context {
    pub journal: Arc<Journal>,
    pub registry: CellGroupArchiveRegistry,
    pub uses: super::reuse_index::Index,
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
        uses: super::reuse_index::Index::default(),
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
    pub published: GroupManifest,
    pub maintenance_pending: bool,
    pub eligibility: Option<Eligibility>,
}

/// Reuse only a physically live verification eligible for this admission.
/// A cleanup pin alone cannot extend a publication cohort or upgrade Quorum.
/// The weak index neither owns a workspace nor turns status into a proof.
pub(super) fn live(
    context: &Context,
    admission: Admission,
) -> Result<Option<CellGroupArchiveCoverage>> {
    context.uses.live(admission)
}

pub(super) struct Demand {
    pub outputs: bool,
    pub required_prefix: Option<LogIndex>,
    /// A prior completion's advisory marker in this owner. It supplies no
    /// physical custody and cannot authorize a response or compaction.
    pub observed_head: Option<GroupManifest>,
}

pub(super) enum Outcome {
    Verified(Driven),
    /// Retirement finished and the previously verified head still covers local
    /// apply. No new physical proof is created by this maintenance result.
    Maintenance(GroupManifest),
}

pub(super) async fn verify_retained(
    context: &Context,
    execution: &ControllerExecution,
    coverage: CellGroupArchiveCoverage,
) -> Result<Driven> {
    context.authority.require_current().await?;
    let record = load(context, execution).await?;
    if record.head.as_ref() != Some(coverage.hold())
        && record.retire.as_ref() != Some(coverage.hold())
    {
        return Err(Error::Invalid("retained archive hold journal"));
    }
    let published = record
        .head
        .as_ref()
        .ok_or(Error::Invalid("retained archive without head"))?
        .marker()
        .clone();
    Ok(Driven {
        coverage,
        published,
        maintenance_pending: record.retire.is_some() || !matches!(record.pending, Pending::None),
        eligibility: None,
    })
}

async fn select(
    context: &Context,
    execution: &ControllerExecution,
    record: &mut Record,
    coverage: CellGroupArchiveCoverage,
    eligibility: Eligibility,
) -> Result<Driven> {
    let head = coverage.hold().clone();
    if record.retire.is_some() {
        return Err(Error::Invalid("unfinished archive hold retirement"));
    }
    record.retire = record.head.replace(head);
    record.pending = Pending::None;
    store(context, execution, record).await?;
    // The new head and its predecessor's retirement debt are now durable.
    // Cleanup is not part of this publication's proof: a slow release must not
    // hold an already verified response on the archive lane. The next drive
    // retires the predecessor before allocating again, retaining the existing
    // current-plus-retiring bound across failures and restart.
    Ok(Driven {
        published: coverage.marker().clone(),
        coverage,
        maintenance_pending: record.retire.is_some(),
        eligibility: Some(eligibility),
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Retirement {
    Retained,
    Unchanged,
    Released,
}

impl Retirement {
    /// A quiet completion has no successor intent to carry the cleared debt.
    async fn flush(
        self,
        context: &Context,
        execution: &ControllerExecution,
        record: &Record,
    ) -> Result<()> {
        if self == Self::Released {
            store(context, execution, record).await?;
        }
        Ok(())
    }
}

/// Release an unused previous hold. A retained hold prevents allocation, not
/// verification of the current head. After release, the next required intent or
/// selection checkpoint also clears its debt. Failure before that checkpoint
/// leaves the old durable debt intact; release is idempotent on exact replay.
async fn retire(
    context: &Context,
    execution: &ControllerExecution,
    record: &mut Record,
) -> Result<Retirement> {
    if let Some(previous) = &record.retire {
        let retained = context.uses.retains(previous)?;
        if retained {
            return Ok(Retirement::Retained);
        }
        context.registry.release(execution, previous).await?;
        record.retire = None;
        return Ok(Retirement::Released);
    }
    Ok(Retirement::Unchanged)
}

/// Finish unretained predecessor debt after publication/proof work has joined.
/// Live output proofs keep their exact hold journaled for the next owner; this
/// never allocates an archive or drops a consumer's physical protection.
pub(super) async fn finish(context: &Context, execution: &ControllerExecution) -> Result<()> {
    let mut record = load(context, execution).await?;
    if record.retire.is_some() {
        context.authority.require_current().await?;
        retire(context, execution, &mut record)
            .await?
            .flush(context, execution, &record)
            .await?;
    }
    Ok(())
}

/// Initial acquisition and a renewed reservation use the same exact intent,
/// supersession handling and independent physical verification.
async fn acquire(
    context: &Context,
    registration: &CellGroupArchiveRegistration,
    execution: &ControllerExecution,
    record: &mut Record,
    intent: &CellGroupArchiveIntent,
    eligibility: Eligibility,
) -> Result<Driven> {
    let hold = match context.registry.acquire(execution, intent).await {
        Ok(hold) => hold,
        Err(error @ (CellGroupArchiveError::Expired | CellGroupArchiveError::Conflict)) => {
            if matches!(
                context.registry.acquire_outcome(execution, intent).await?,
                CellGroupArchiveAcquireOutcome::Superseded
            ) {
                record.pending = Pending::None;
                store(context, execution, record).await?;
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
    select(context, execution, record, coverage, eligibility).await
}

pub(super) async fn drive(
    context: &Context,
    registration: &CellGroupArchiveRegistration,
    runtime: &CellGroupRuntime,
    execution: &ControllerExecution,
    demand: Demand,
    retained_head: Option<CellGroupArchiveCoverage>,
    admitted_through: u64,
) -> Result<Outcome> {
    let node = runtime.log().limits().node;
    let group = registration.birth.route().history.group;
    tracing::debug!(target: "cellar::raft::group_archive", ?node, ?group, stage = "drive");
    registration
        .directory
        .require_current_group_birth(&registration.birth)
        .await
        .map_err(CellGroupArchiveBindingError::from)?;
    let mut record = load(context, execution).await?;
    let mut retirement = retire(context, execution, &mut record).await?;
    let allocation_ready = retirement != Retirement::Retained;
    if let Some(head) = record.head.as_ref().filter(|head| {
        demand.outputs
            && demand
                .required_prefix
                .is_some_and(|minimum| head.marker().position().index >= minimum)
    }) {
        // An unrelated newer cut may already own a durable reservation or an
        // uncertain publication. Confirm this exact old hold independently;
        // do not discard, replace or replay that pending mutation to serve a
        // request already covered here. The ordinary background owner retains
        // the entire pending operation and its original reconciliation debt.
        let coverage = context
            .registry
            .verify_hold(
                execution,
                head,
                &registration.tails,
                receiver(registration)?,
            )
            .await?;
        retirement.flush(context, execution, &record).await?;
        return Ok(Outcome::Verified(Driven {
            published: coverage.marker().clone(),
            coverage,
            maintenance_pending: !allocation_ready || !matches!(record.pending, Pending::None),
            eligibility: Some(Eligibility::existing(admitted_through)),
        }));
    }
    // Quiet maintenance can finish predecessor cleanup without discarding the
    // node's one physical proof. This is the exact journaled head, independently
    // recovered earlier and still admitted; the advisory marker cannot create it.
    if allocation_ready && !demand.outputs && matches!(record.pending, Pending::None) {
        if let Some(coverage) = retained_head
            .as_ref()
            .filter(|coverage| record.head.as_ref() == Some(coverage.hold()))
        {
            let current = runtime
                .log()
                .cell_group(&registration.birth.route().history)
                .and_then(|group| group.group_manifest())
                .map_err(|error| Error::Runtime(error.into()))?
                .ok_or(Error::Runtime(RuntimeError::NotReady))?;
            if coverage.marker().position().index >= current.position().index {
                retirement.flush(context, execution, &record).await?;
                return Ok(Outcome::Verified(Driven {
                    published: coverage.marker().clone(),
                    coverage: coverage.clone(),
                    maintenance_pending: false,
                    eligibility: None,
                }));
            }
        }
    }
    // Late covered requests can retain this exact admitted verification while
    // predecessor cleanup waits on remote I/O. Release our temporary owner
    // before any successor needs the original physical replay admission.
    drop(retained_head);
    if !allocation_ready && (!demand.outputs || !matches!(record.pending, Pending::None)) {
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
            Ok(coverage) => {
                return select(
                    context,
                    execution,
                    &mut record,
                    coverage,
                    Eligibility::existing(admitted_through),
                )
                .await
                .map(Outcome::Verified)
            }
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
            Ok(coverage) => {
                return select(
                    context,
                    execution,
                    &mut record,
                    coverage,
                    Eligibility::existing(admitted_through),
                )
                .await
                .map(Outcome::Verified)
            }
            Err(CellGroupArchiveError::Expired) => (),
            Err(error) => return Err(error.into()),
        }
        record.pending = Pending::None;
        store(context, execution, &record).await?;
        retirement = Retirement::Unchanged;
    }
    if matches!(record.pending, Pending::None) {
        let current = runtime
            .log()
            .cell_group(&registration.birth.route().history)
            .and_then(|group| group.group_manifest())
            .map_err(|error| Error::Runtime(error.into()))?
            .ok_or(Error::Runtime(RuntimeError::NotReady))?;
        if let Some(head) = &record.head {
            // Only this owner's prior physical verification enables the quiet
            // maintenance path. Restart/initial activation has no observation,
            // and waiting outputs always need their own actual proof below.
            if !demand.outputs
                && demand.observed_head.as_ref() == Some(head.marker())
                && head.marker().position().index >= current.position().index
            {
                tracing::debug!(target: "cellar::raft::group_archive", ?node, ?group,
                    stage = "maintenance-complete", index = head.marker().position().index.get());
                retirement.flush(context, execution, &record).await?;
                return Ok(Outcome::Maintenance(head.marker().clone()));
            }
            // A queued output may already be covered even when later group
            // entries exist. The receipt verifier still checks its exact
            // position, cell version, witness and stored result against this
            // prefix; an uncovered output keeps waiting for allocation.
            let may_cover = demand
                .required_prefix
                .is_none_or(|minimum| head.marker().position().index >= minimum);
            if may_cover
                && (!allocation_ready || head.marker().position().index >= current.position().index)
            {
                let coverage = context
                    .registry
                    .verify_hold(
                        execution,
                        head,
                        &registration.tails,
                        receiver(registration)?,
                    )
                    .await?;
                retirement.flush(context, execution, &record).await?;
                return Ok(Outcome::Verified(Driven {
                    published: coverage.marker().clone(),
                    coverage,
                    maintenance_pending: !allocation_ready,
                    eligibility: Some(Eligibility::existing(admitted_through)),
                }));
            }
        }
        // Retained output proofs may delay the previous hold's release. Reuse
        // above requires no new archive, but acquisition/upload must preserve
        // the existing current-plus-retiring bound.
        if !allocation_ready {
            return Err(CellGroupArchiveError::Busy.into());
        }
        // One group leader produces new cuts. Followers independently acquire
        // published prefixes instead of recapturing every applied update. This
        // role sample only schedules work: birth fencing, journal reconciliation
        // and physical verification remain the archive authority. In particular,
        // an already journaled upload still reconciles after role loss.
        let publishing = registration.publisher == CellGroupArchivePublisher::LocalReplica
            // Sealed sources still owe their final archive cut after serving
            // leadership is withdrawn. Registry fencing and verification,
            // rather than the scheduling role, authorize this publication.
            || current.source_seal().is_some()
            || runtime
                .observe(registration.birth.route().history)
                .await
                .map_err(Error::Runtime)?
                .role
                == crate::application::consensus::RaftReplicaRole::Leader;
        record.pending = match context.registry.prepare_acquire(execution).await {
            Ok(intent) => {
                let selected = intent.acquire_marker()?;
                let newer_prefix = !publishing
                    && selected.route() == current.route()
                    && selected.position().index < current.position().index
                    && record.head.as_ref().is_none_or(|head| {
                        selected.position().index > head.marker().position().index
                    });
                if selected == &current || newer_prefix {
                    Pending::Acquire(intent)
                } else if publishing {
                    Pending::Reserve(context.registry.prepare_upload(execution).await?)
                } else {
                    return Err(CellGroupArchiveError::Busy.into());
                }
            }
            Err(CellGroupArchiveError::Unconfirmed) => {
                // A late-bound archive may have no published prefix while its
                // only attached replica is a follower. Bootstrap through the
                // ordinary journaled reservation and verified publication; the
                // registry CAS, not this role sample, grants archive authority.
                let intent = context.registry.prepare_upload(execution).await?;
                if !publishing {
                    // Another publisher may have advanced between the two
                    // observations. A follower's bootstrap intent must still
                    // reserve against no predecessor, including at its CAS.
                    intent.require_initial_upload()?;
                }
                Pending::Reserve(intent)
            }
            Err(error) => return Err(error.into()),
        };
        store(context, execution, &record).await?;
    }
    if let Pending::Acquire(intent) = record.pending.clone() {
        return acquire(
            context,
            registration,
            execution,
            &mut record,
            &intent,
            Eligibility::existing(admitted_through),
        )
        .await
        .map(Outcome::Verified);
    }
    // One fresh slot per fair turn; further contention keeps its exact intent
    // and returns to the ordinary retry cadence instead of spinning.
    let mut renewed = false;
    while let Pending::Reserve(intent) = record.pending.clone() {
        match context.registry.reserve(execution, &intent).await {
            Ok(ticket) => record.pending = Pending::Upload(ticket),
            Err(error @ (CellGroupArchiveError::Expired | CellGroupArchiveError::Conflict))
                if !renewed =>
            {
                context.authority.require_current().await?;
                let current = runtime
                    .log()
                    .cell_group(&registration.birth.route().history)
                    .and_then(|group| group.group_manifest())
                    .map_err(|error| Error::Runtime(error.into()))?
                    .ok_or(Error::Runtime(RuntimeError::NotReady))?;
                let Some(successor) = context
                    .registry
                    .renew_superseded_reservation(execution, &intent, &current)
                    .await?
                else {
                    // Still pending or already allocated: preserve the exact
                    // original intent for reconciliation, including lost replies.
                    return Err(error.into());
                };
                record.pending = match successor {
                    CellGroupArchiveReservationRenewal::Acquire(intent) => Pending::Acquire(intent),
                    CellGroupArchiveReservationRenewal::Upload(intent) => Pending::Reserve(intent),
                };
                // Journal before the successor CAS. A crash can replay only
                // this exact allocation; the superseded slot stays consumed.
                store(context, execution, &record).await?;
                if let Pending::Acquire(intent) = record.pending.clone() {
                    return acquire(
                        context,
                        registration,
                        execution,
                        &mut record,
                        &intent,
                        Eligibility::existing(admitted_through),
                    )
                    .await
                    .map(Outcome::Verified);
                }
                renewed = true;
                continue;
            }
            Err(error) => return Err(error.into()),
        }
        // The already durable reservation intent reconciles this exact ticket,
        // even across unrelated registry revisions. Immutable uploads need no
        // second local checkpoint of that derived result. Failure/restart can
        // replay Reserve; publication still persists its ticket and intent
        // before CAS, then journals the selected head and predecessor debt.
        break;
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
    let uploaded = if demand.outputs {
        // A waiting S3 response needs this exact cut. Prefer a complete image
        // over reading and replaying an older checkpoint's tail on its original
        // request budget. Background publication may amortize image uploads.
        upload
            .upload_cut(execution, &registration.tails, source)
            .await?
    } else {
        upload
            .upload_next_cut(execution, &registration.tails, record.head.as_ref(), source)
            .await?
    };
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
    select(
        context,
        execution,
        &mut record,
        coverage,
        Eligibility::publication(),
    )
    .await
    .map(Outcome::Verified)
}
