// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One cell's private SQL/receipt producer under a physical group's authority.
//! The node's SQL pool must own this synchronous object and join its callbacks.

use super::*;
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits, GroupLtx, GroupMutation,
};
use crate::raft::cell_groups::{
    cell_image_history_digest, CellExecutionAuthority, CellReconciliationAuthority,
};
use crate::raft::completion::RuntimeError;
use cellar_logic::cell_group::{CellVersion, MutationSequence};

#[path = "cell_preparation.rs"]
mod preparation;
pub(crate) use preparation::{CellPrivatePreparation, CellPrivatePrepared, CellStageCapture};

#[path = "cell_checkpoint.rs"]
mod checkpoint;
pub(crate) use checkpoint::{CellCheckpointGrant, CellCheckpointPrepared};

#[path = "cell_rpc.rs"]
mod rpc;
pub(crate) use rpc::{
    CellCheckpointRetired, CellCheckpointRetirement, CellRpcDispatch, CellRpcEnvironment,
    CellRpcStart, CellRpcTurn,
};

/// A receipt proven against the exact committed cell image. This is not S3 coverage.
pub struct CellCommittedReceipt {
    pub receipt: StoredReceipt,
    pub version: CellVersion,
    pub position: LogPosition,
    /// Decision for the selected version, which may cover a prior receipt.
    pub witness: cellar_logic::cell_group::CommitWitness,
}

impl CellCommittedReceipt {
    pub fn observation(&self) -> crate::raft::version::CellVersionObservation {
        crate::raft::version::CellVersionObservation {
            version: self.version.clone(),
            witness: self.witness,
        }
    }
}

pub enum CellStageOutcome {
    Prepared(CellPreparedCommand),
    Duplicate(Box<CellCommittedReceipt>),
}

pub enum CellReconciliation {
    /// The original request's receipt, from the inspected committed image.
    /// Its position identifies that image, not necessarily the original entry.
    Committed(Box<CellCommittedReceipt>),
    /// A newer-term applied barrier excludes the old proposal and the original
    /// request has no receipt. A retry must still pass fresh command admission.
    NotCommitted,
    /// A replicated receipt floor or conflicting identity prevents a retry.
    /// Expiration does not establish whether the original command committed.
    Refused(CommandFailure),
}

struct PendingCell {
    prepared: CellPreparedCommand,
    request: CommandRequest,
    result: CellVersion,
    _preview: Preview,
    _prepared_image: std::sync::Arc<super::CellPreparedImage>,
    submitted: bool,
    rpc_step: Option<u64>,
    rpc_prior_steps: u64,
}

struct StagePreparation {
    sequence: MutationSequence,
    log_reservation: std::sync::Arc<crate::raft::cell_groups::log_retention::LogReservation>,
    maintenance: Option<crate::raft::receipt_maintenance::Plan>,
}

enum StageAdmission {
    Duplicate(Box<CellCommittedReceipt>),
    New(StagePreparation),
}

pub struct CellPrivateExecutor {
    options: Options,
    authority: std::sync::Arc<CellExecutionAuthority>,
    working: Option<WorkingImage>,
    pending: Option<PendingCell>,
    preparing: Option<PreparationOwner>,
    poisoned: bool,
    receipt_cursor: Option<ClientId>,
    _lock: PrivateLock,
}

/// Disposable physical custody before SQL opens or after SQLite and capture
/// close on their SQL owner. It contains no connection, continuation or callback.
pub(crate) struct CellPrivateCleanup {
    options: Options,
    lock: Option<PrivateLock>,
    closed: Result<(), ExecutionError>,
    discard: bool,
    retirement: Option<CellPrivateRetirement>,
}

/// Exact settlement has already been checked on SQL. Keep every preview and
/// log/image reservation until its existing physical cleanup owner joins.
pub(crate) struct CellPrivateRetirement {
    images: Images,
    pending: PendingCell,
}

impl CellPrivateRetirement {
    pub(crate) fn finish(self) -> Result<(), ExecutionError> {
        self.images
            .retire(&self.pending._preview)
            .map_err(storage)?;
        self.pending
            .prepared
            .release_log_reservation()
            .map_err(authority_failure)
    }
}

impl CellPrivateCleanup {
    /// A workspace rejected before a SQLite writer or lock was adopted.
    pub(crate) fn unopened_workspace(options: RootPrivateOptions) -> Self {
        Self {
            options: Options::rooted(options, None),
            lock: None,
            closed: Ok(()),
            discard: false,
            retirement: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn closed_workspace_for_test(
        options: RootPrivateOptions,
    ) -> Result<Self, ExecutionError> {
        // Custody tests start with a real disposable workspace and no SQLite
        // connection. Native integration separately covers SQL-owner closure.
        let options = Options::rooted(options, None);
        let lock = PrivateLock {
            _lock: options
                .host
                .filesystem()
                .lock_file(&options.directory.join("working.lock"))
                .map_err(storage)?,
            _workspace: options.images.workspace().cloned(),
        };
        Ok(Self {
            options,
            lock: Some(lock),
            closed: Ok(()),
            discard: false,
            retirement: None,
        })
    }

    pub(crate) fn finish(self) -> Result<(), ExecutionError> {
        let Self {
            options,
            lock,
            closed,
            discard,
            retirement,
        } = self;
        let store = options.images.root_store().cloned();
        // Inventory serialization can wait on another cell's physical I/O.
        // This work shares the joined cleanup task, never the SQL callback lane.
        let retired = retirement.map_or(Ok(()), CellPrivateRetirement::finish);
        let discarded = if discard {
            options.discard_cell_scratch().map_err(storage)
        } else {
            Ok(())
        };
        // The final rooted pin removes the workspace and syncs its parent
        // before refunding. Failure keeps the charge and quarantines admission.
        drop(options);
        drop(lock);
        // Preserve close_cell's cleanup-before-close error precedence.
        retired.and(discarded).and(closed)?;
        if let Some(store) = store {
            if store.usage().map_err(storage)?.quarantined {
                return Err(storage(cellar_ltx::internal::StoreError::Quarantined));
            }
        }
        Ok(())
    }
}

enum PreparationOwner {
    Local(std::sync::Arc<()>),
    Checkpoint(std::sync::Arc<()>),
}

impl CellPrivateExecutor {
    pub(crate) fn open_cursor_image(
        options: PrivateOptions,
        authority: CellExecutionAuthority,
    ) -> Result<Self, ExecutionError> {
        Self::open_image_kind(options.into(), authority, true)
    }

    pub fn open(
        options: PrivateOptions,
        authority: CellExecutionAuthority,
    ) -> Result<Self, ExecutionError> {
        Self::open_image(options.into(), authority)
    }

    pub fn open_rooted(
        options: RootPrivateOptions,
        authority: CellExecutionAuthority,
    ) -> Result<Self, ExecutionError> {
        Self::open_rooted_with_apply_limits(options, authority, None)
    }

    pub(crate) fn open_rooted_with_apply_limits(
        options: RootPrivateOptions,
        authority: CellExecutionAuthority,
        limits: Option<cellar_ltx::apply::ApplyLimits>,
    ) -> Result<Self, ExecutionError> {
        Self::open_image(Options::rooted(options, limits), authority)
    }

    fn open_image(
        options: Options,
        authority: CellExecutionAuthority,
    ) -> Result<Self, ExecutionError> {
        Self::open_image_kind(options, authority, false)
    }

    fn open_image_kind(
        mut options: Options,
        authority: CellExecutionAuthority,
        cursor_images: bool,
    ) -> Result<Self, ExecutionError> {
        options.limits.validate()?;
        options.images.validate_workspace(options.limits)?;
        authority.check().map_err(authority_failure)?;
        if authority.route().schema_digest != options.application_digest {
            return Err(ExecutionError::Invalid("cell execution schema"));
        }
        let filesystem = options.host.filesystem();
        let directory = options.directory.clone();
        let mut acquired = None;
        for slot in 0..if cursor_images { MAX_CURSOR_IMAGES } else { 1 } {
            if slot != 0 {
                let mut name = directory
                    .file_name()
                    .ok_or(ExecutionError::Invalid("private directory name"))?
                    .to_os_string();
                name.push(format!(".cursor-{slot}"));
                options.directory = directory.with_file_name(name);
            }
            options.prepare_cell_scratch()?;
            match filesystem.lock_file(&options.directory.join("working.lock")) {
                Ok(lock) => {
                    acquired = Some(lock);
                    break;
                }
                Err(error) if cursor_images && error.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(error) => return Err(storage(error)),
            }
        }
        let lock = PrivateLock {
            _lock: acquired.ok_or(ExecutionError::Refused(CommandFailure::Overloaded))?,
            _workspace: options.images.workspace().cloned(),
        };
        let working = match WorkingImage::open_cell(
            &options,
            authority.image(),
            cell_image_history_digest(&authority.cell().version.history),
        ) {
            Ok(working) => working,
            Err(error) => {
                options.discard_cell_scratch().map_err(storage)?;
                return Err(storage(error));
            }
        };
        if let Err(error) = authority.check() {
            working.close_cell(&options).map_err(storage)?;
            return Err(authority_failure(error));
        }
        Ok(Self {
            options,
            authority: std::sync::Arc::new(authority),
            working: Some(working),
            pending: None,
            preparing: None,
            poisoned: false,
            receipt_cursor: None,
            _lock: lock,
        })
    }

    /// Invoke a registered application operation with this cell's logical
    /// identity. Validation precedes receipt lookup, including duplicate retries.
    pub fn stage_registered(
        &mut self,
        attempt: CommandAttempt,
        class: &RaftClass,
        input: &[u8],
        allow_new: bool,
    ) -> Result<CellStageOutcome, ExecutionError> {
        let captured =
            self.capture_registered(attempt, class, input, allow_new, CommandAdmission::Public)?;
        self.complete_capture(captured)
    }

    pub(crate) fn capture_registered(
        &mut self,
        attempt: CommandAttempt,
        class: &RaftClass,
        input: &[u8],
        allow_new: bool,
        admission: CommandAdmission,
    ) -> Result<CellStageCapture, ExecutionError> {
        let history = self.authority.cell().version.history.clone();
        let contract = super::registered::validate_admission(
            self.options.limits,
            self.options.application_digest,
            &history,
            &attempt.request,
            class,
            input,
            admission,
        )?;
        let command = attempt.request.id;
        let durability = attempt.request.durability;
        let route_epoch = self.authority.route().epoch;
        let limits = self.options.limits;
        let system_request = admission.is_system().then(|| attempt.request.clone());
        let origin = crate::storage::raft_binding_recovery::Origin {
            group: self.authority.route().history,
            term: self.authority.term().get(),
        };
        self.capture(attempt, allow_new, |transaction| {
            if let Some(request) = system_request {
                class.invoke_system_command(&history, &request, transaction, input, limits, origin)
            } else {
                class.invoke_command(
                    &contract,
                    crate::application::consensus::CommandTarget {
                        cell: &history,
                        route_epoch: Some(route_epoch),
                    },
                    command,
                    durability,
                    transaction,
                    input,
                )
            }
        })
    }

    /// Stage on an owning blocking worker. Callback output is retained only in
    /// the private receipt transaction; it cannot escape as a committed result.
    /// The host serializes unresolved producers by CellHistory, independently
    /// from the group's consensus owner and other cells' SQL work.
    pub fn stage(
        &mut self,
        attempt: CommandAttempt,
        allow_new: bool,
        callback: impl FnOnce(&CommandTransaction<'_>) -> anyhow::Result<Vec<u8>>,
    ) -> Result<CellStageOutcome, ExecutionError> {
        let captured = self.capture(attempt, allow_new, callback)?;
        self.complete_capture(captured)
    }

    pub(crate) fn capture(
        &mut self,
        attempt: CommandAttempt,
        allow_new: bool,
        callback: impl FnOnce(&CommandTransaction<'_>) -> anyhow::Result<Vec<u8>>,
    ) -> Result<CellStageCapture, ExecutionError> {
        let mut preparation = match self.prepare_stage(&attempt, allow_new)? {
            StageAdmission::Duplicate(receipt) => return Ok(CellStageCapture::Duplicate(receipt)),
            StageAdmission::New(preparation) => preparation,
        };
        let working = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?;
        command::apply(
            working.database_mut(),
            self.options.limits,
            &attempt,
            preparation.maintenance.take(),
            callback,
        )?;
        self.capture_stage_input(attempt, preparation)
            .map(|input| CellStageCapture::Captured(Box::new(input)))
    }

    fn prepare_stage(
        &mut self,
        attempt: &CommandAttempt,
        allow_new: bool,
    ) -> Result<StageAdmission, ExecutionError> {
        if self.pending.is_some() || self.preparing.is_some() {
            return Err(ExecutionError::Busy);
        }
        if self.poisoned {
            return Err(ExecutionError::RebuildRequired);
        }
        self.authority.check().map_err(authority_failure)?;
        let working = self
            .working
            .as_ref()
            .ok_or(ExecutionError::RebuildRequired)?;
        let maintenance = match command::prepare(
            working.database().framework(),
            self.options.limits,
            &mut self.receipt_cursor,
            attempt,
            allow_new,
        )? {
            command::Admission::Duplicate => {
                return self
                    .read_receipt(&self.authority, &attempt.request)
                    .map(|receipt| StageAdmission::Duplicate(Box::new(receipt)))
            }
            command::Admission::New(maintenance) => maintenance,
        };
        let log_reservation = self
            .authority
            .reserve_log(self.options.limits.envelope)
            .map_err(authority_failure)?;
        let sequence = self
            .authority
            .cell()
            .version
            .sequence
            .checked_next()
            .map_err(|_| ExecutionError::Invalid("cell mutation sequence exhausted"))?;
        self.poisoned = true;
        let working = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?;
        working.begin_mutation();
        Ok(StageAdmission::New(StagePreparation {
            sequence,
            log_reservation,
            maintenance,
        }))
    }

    /// Set before awaiting proposal submission. A dropped response never frees
    /// this producer for another callback or establishes that its effect failed.
    pub fn mark_submitted(&mut self, proposal: ProposalId) -> Result<(), ExecutionError> {
        self.authority.check().map_err(authority_failure)?;
        let pending = self
            .pending
            .as_mut()
            .ok_or(ExecutionError::Invalid("no cell proposal"))?;
        if pending.submitted || pending.prepared.proposal() != proposal {
            return Err(ExecutionError::Invalid("cell proposal submission"));
        }
        pending.submitted = true;
        Ok(())
    }

    /// Settle only the exact committed entry and cell result, using a fresh
    /// current-term authority from the actual owner. Another cell's progress,
    /// no-op or selected image cannot settle this receipt. Compacted entries
    /// require later receipt reconciliation, not an inferred proposal proof.
    pub fn finish(
        &mut self,
        committed: CellExecutionAuthority,
    ) -> Result<CellCommittedReceipt, ExecutionError> {
        let receipt = self.committed_receipt(&committed)?;
        self.retire_reconciled()?;
        Ok(receipt)
    }

    /// The SQL pool keeps the response and original admission until the returned
    /// retirement and writer cleanup have completed on their physical owner.
    pub(crate) fn finish_for_cleanup(
        &mut self,
        committed: CellExecutionAuthority,
    ) -> Result<(CellCommittedReceipt, CellPrivateRetirement), ExecutionError> {
        let receipt = self.committed_receipt(&committed)?;
        Ok((receipt, self.defer_retirement()?))
    }

    fn committed_receipt(
        &self,
        committed: &CellExecutionAuthority,
    ) -> Result<CellCommittedReceipt, ExecutionError> {
        let pending = self
            .pending
            .as_ref()
            .ok_or(ExecutionError::Invalid("no cell proposal"))?;
        if !pending.submitted
            || committed.route() != self.authority.route()
            || committed.cell().version != pending.result
        {
            return Err(ExecutionError::Invalid("committed cell proposal/image"));
        }
        committed
            .check_committed_entry(pending.prepared.bytes())
            .map_err(authority_failure)?;
        committed.check().map_err(authority_failure)?;
        // No application work can run between capture and exact settlement.
        // This frozen writer produced the prepared image that the committed
        // entry proves above, just as for an intermediate RPC checkpoint.
        // Independent replay and reconciliation of other images still read
        // the selected immutable state.
        let receipt = {
            let connection = self
                .working
                .as_ref()
                .ok_or(ExecutionError::RebuildRequired)?
                .database()
                .framework();
            if !connection.is_autocommit() {
                return Err(ExecutionError::Invalid(
                    "final receipt writer is not frozen",
                ));
            }
            receipts::preflight(connection, &pending.request, &self.options.limits)?
                .ok_or(ExecutionError::Invalid("committed cell receipt missing"))?
        };
        committed.check().map_err(authority_failure)?;
        Ok(CellCommittedReceipt {
            receipt,
            version: committed.cell().version.clone(),
            position: committed.cell().last_mutation,
            witness: committed.cell().witness,
        })
    }

    /// Native state can survive only a receipt at this exact final image.
    /// Intermediate RPC checkpoints cannot return an instance to the idle cache.
    pub(crate) fn prepared_final_version(&self) -> Option<CellVersion> {
        self.pending
            .as_ref()
            .filter(|pending| pending.rpc_step.is_none())
            .map(|pending| pending.result.clone())
    }

    /// Resolve the original request without requiring the original log entry.
    /// Missing receipts cannot retire an ambiguous same-term proposal. The
    /// caller must close this disposable producer after successful resolution.
    /// A selected image that advances during inspection leaves the original
    /// producer uncertain; retry with a fresh reconciliation observation.
    pub fn reconcile(
        &mut self,
        proposal: ProposalId,
        authority: CellReconciliationAuthority,
    ) -> Result<CellReconciliation, ExecutionError> {
        let Some(resolution) = self.inspect_reconciliation(proposal, authority)? else {
            return Err(ExecutionError::Uncertain);
        };
        self.retire_reconciled()?;
        Ok(resolution)
    }

    pub(crate) fn reconcile_for_cleanup(
        &mut self,
        proposal: ProposalId,
        authority: CellReconciliationAuthority,
    ) -> Result<Option<(CellReconciliation, CellPrivateRetirement)>, ExecutionError> {
        let Some(resolution) = self.inspect_reconciliation(proposal, authority)? else {
            return Ok(None);
        };
        Ok(Some((resolution, self.defer_retirement()?)))
    }

    fn inspect_reconciliation(
        &self,
        proposal: ProposalId,
        authority: CellReconciliationAuthority,
    ) -> Result<Option<CellReconciliation>, ExecutionError> {
        let pending = self.pending.as_ref().ok_or(ExecutionError::Uncertain)?;
        if pending.prepared.proposal() != proposal
            || !self.authority.accepts_reconciliation(&authority)
        {
            return Err(ExecutionError::Invalid("cell reconciliation identity"));
        }
        let observe_uncertain = |phase: &'static str| {
            tracing::trace!(target: "cellar::raft::cell_reconciliation", phase,
                command = ?pending.request.id,
                base_sequence = self.authority.cell().version.sequence.get(),
                expected_sequence = pending.result.sequence.get(),
                observed_sequence = authority.cell().version.sequence.get(),
                base_term = self.authority.term().get(),
                observed_term = authority.term().get(),
                submitted = pending.submitted,
                prior_steps = pending.rpc_prior_steps,
                rpc_step = ?pending.rpc_step,
                "private receipt remains uncertain");
        };
        let current_error = |error| match error {
            RuntimeError::NotReady => {
                observe_uncertain("authority-not-ready");
                ExecutionError::Uncertain
            }
            error => authority_failure(error),
        };
        let check = || authority.check().map_err(current_error);
        check()?;
        let lookup = |connection: &rusqlite::Connection| {
            if let Some(step) = pending.rpc_step {
                receipts::lookup_rpc_identity(connection, &pending.request, &self.options.limits)
                    .map(|receipt| {
                        receipt
                            .filter(|receipt| receipt.rpc_step.is_none_or(|actual| actual >= step))
                    })
            } else {
                receipts::lookup_identity(connection, &pending.request, &self.options.limits)
            }
        };
        let receipt = if authority.cell().version == self.authority.cell().version {
            if pending.rpc_prior_steps == 0 {
                // Staging established absence in this exact immutable base;
                // an enrollment image need not yet have receipt tables.
                Ok(None)
            } else {
                // Continuing this RPC already validated its unfinished receipt
                // in this exact base. The final private receipt proves neither
                // completion nor absence until its own result image applies.
                observe_uncertain("unchanged-checkpoint");
                Err(ExecutionError::Uncertain)
            }
        } else if pending.submitted
            && authority.cell().version == pending.result
            && authority
                .retains_committed_entry(pending.prepared.bytes())
                .map_err(current_error)?
        {
            // The pending producer still owns the writer, with no application
            // work since capture. Exact version and committed-entry evidence
            // bind its frozen SQL state to the selected immutable image.
            let connection = self
                .working
                .as_ref()
                .ok_or(ExecutionError::RebuildRequired)?
                .database()
                .framework();
            if !connection.is_autocommit() {
                return Err(ExecutionError::Invalid("cell receipt writer is not frozen"));
            }
            lookup(connection)
        } else {
            let inspect = |connection: &rusqlite::Connection| {
                if let Some(step) = pending.rpc_step {
                    receipts::lookup_rpc_identity(
                        connection,
                        &pending.request,
                        &self.options.limits,
                    )
                    .map(|receipt| {
                        receipt
                            .filter(|receipt| receipt.rpc_step.is_none_or(|actual| actual >= step))
                    })
                } else {
                    receipts::lookup_identity(connection, &pending.request, &self.options.limits)
                }
            };
            // The producer is frozen while pending. Reuse its captured data
            // only when this independently checked applied view selects the
            // exact result. Later images and unavailable or busy connections
            // still use immutable inspection. Neither path supplies S3 proof.
            let captured = self
                .working
                .as_ref()
                .filter(|_| authority.cell().version == pending.result)
                .and_then(|working| working.captured_connection(pending.result.image));
            if let Some(connection) = captured {
                inspect(connection)
            } else {
                let database = self
                    .options
                    .images
                    .read_cell(&self.options, authority.image())
                    .map_err(storage)?;
                inspect(database.connection())
            }
        };
        let resolution = match receipt {
            Ok(Some(receipt)) => CellReconciliation::Committed(Box::new(CellCommittedReceipt {
                receipt,
                version: authority.cell().version.clone(),
                position: authority.cell().last_mutation,
                witness: authority.cell().witness,
            })),
            Ok(None) if authority.term() > self.authority.term() => {
                CellReconciliation::NotCommitted
            }
            Err(ExecutionError::Refused(
                failure @ (CommandFailure::CommandExpired | CommandFailure::CommandConflict),
            )) if authority.term() > self.authority.term() => CellReconciliation::Refused(failure),
            // A newer barrier can exclude this final proposal while earlier
            // root transactions remain committed under the same invocation.
            Err(ExecutionError::Uncertain)
                if authority.term() > self.authority.term() && pending.rpc_prior_steps != 0 =>
            {
                CellReconciliation::Refused(CommandFailure::Unavailable)
            }
            // This exact immutable image contains an intermediate receipt,
            // which cannot supply the final RPC response. A checked same-term
            // view stays pending until its term or cell version changes.
            Err(ExecutionError::Uncertain)
                if authority.term() == self.authority.term() && pending.rpc_prior_steps != 0 =>
            {
                check()?;
                return Ok(None);
            }
            Err(error) => return Err(error),
            Ok(None) => {
                check()?;
                return Ok(None);
            }
        };
        check()?;
        Ok(Some(resolution))
    }

    fn defer_retirement(&mut self) -> Result<CellPrivateRetirement, ExecutionError> {
        let pending = self.pending.take().ok_or(ExecutionError::Uncertain)?;
        self.poisoned = true;
        Ok(CellPrivateRetirement {
            images: self.options.images.clone(),
            pending,
        })
    }

    /// The caller has established the matching applied receipt or newer-term
    /// absence proof. Keep the preview and log reservation until cleanup joins.
    fn retire_reconciled(&mut self) -> Result<(), ExecutionError> {
        let pending = self.pending.as_ref().ok_or(ExecutionError::Uncertain)?;
        // Only exact committed/reconciliation authority retires a private
        // preview. Pending retains its live pin through physical settlement.
        self.options
            .images
            .retire(&pending._preview)
            .map_err(storage)?;
        pending
            .prepared
            .release_log_reservation()
            .map_err(authority_failure)?;
        self.pending = None;
        self.poisoned = true;
        Ok(())
    }

    fn read_receipt(
        &self,
        authority: &CellExecutionAuthority,
        request: &CommandRequest,
    ) -> Result<CellCommittedReceipt, ExecutionError> {
        authority.check().map_err(authority_failure)?;
        let database = self
            .options
            .images
            .read_cell(&self.options, authority.image())
            .map_err(storage)?;
        let receipt = receipts::preflight(database.connection(), request, &self.options.limits)?
            .ok_or(ExecutionError::Invalid("committed cell receipt missing"))?;
        authority.check().map_err(authority_failure)?;
        Ok(CellCommittedReceipt {
            receipt,
            version: authority.cell().version.clone(),
            position: authority.cell().last_mutation,
            witness: authority.cell().witness,
        })
    }

    /// Join and close the disposable writer. It never changes the outcome of
    /// an already submitted entry; recovery reads the committed cell image.
    pub fn close(mut self) -> Result<(), ExecutionError> {
        let store = self.options.images.root_store().cloned();
        if let Some(working) = self.working.take() {
            working.close_cell(&self.options).map_err(storage)?;
        }
        drop(self.options);
        drop(self._lock);
        if let Some(store) = store {
            if store.usage().map_err(storage)?.quarantined {
                return Err(storage(cellar_ltx::internal::StoreError::Quarantined));
            }
        }
        Ok(())
    }

    /// Split only a settled writer's physical cleanup from its SQL closure.
    /// The caller retains its reply, writer lease and bytes until finish joins.
    pub(crate) fn close_writer(
        mut self,
        retirement: CellPrivateRetirement,
    ) -> Result<CellPrivateCleanup, ExecutionError> {
        if self.pending.is_some() || self.preparing.is_some() {
            self.close()?;
            return Err(ExecutionError::Invalid("unsettled private cleanup"));
        }
        let discard = self.working.is_some();
        let closed = self.working.take().map_or(Ok(()), |working| {
            working.close_cell_writer().map_err(storage)
        });
        Ok(CellPrivateCleanup {
            options: self.options,
            lock: Some(self._lock),
            closed,
            discard,
            retirement: Some(retirement),
        })
    }
}

pub(crate) fn authority_failure(error: RuntimeError) -> ExecutionError {
    match error {
        RuntimeError::Overloaded => ExecutionError::Refused(CommandFailure::Overloaded),
        RuntimeError::NotReady | RuntimeError::NotLeader | RuntimeError::Closed => {
            ExecutionError::Refused(CommandFailure::Unavailable)
        }
        other => storage(other),
    }
}
