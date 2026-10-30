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
use cellar_logic::cell_group::CellVersion;

/// A receipt read from the exact committed cell image. This is not S3 coverage.
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
    submitted: bool,
}

pub struct CellPrivateExecutor {
    options: Options,
    authority: CellExecutionAuthority,
    working: Option<WorkingImage>,
    pending: Option<PendingCell>,
    poisoned: bool,
    receipt_cursor: Option<ClientId>,
    _lock: PrivateLock,
}

impl CellPrivateExecutor {
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
        Self::open_image(options.into(), authority)
    }

    fn open_image(
        options: Options,
        authority: CellExecutionAuthority,
    ) -> Result<Self, ExecutionError> {
        options.limits.validate()?;
        options.images.validate_workspace(options.limits)?;
        authority.check().map_err(authority_failure)?;
        if authority.route().schema_digest != options.application_digest {
            return Err(ExecutionError::Invalid("cell execution schema"));
        }
        let filesystem = options.host.filesystem();
        cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), &options.directory)
            .map_err(storage)?;
        let lock = PrivateLock {
            _lock: filesystem
                .lock_file(&options.directory.join("working.lock"))
                .map_err(storage)?,
            _workspace: options.images.workspace().cloned(),
        };
        let working = WorkingImage::open_cell(
            &options,
            authority.image(),
            cell_image_history_digest(&authority.cell().version.history),
        )
        .map_err(storage)?;
        authority.check().map_err(authority_failure)?;
        Ok(Self {
            options,
            authority,
            working: Some(working),
            pending: None,
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
        let history = self.authority.cell().version.history.clone();
        let contract = super::registered::validate(
            self.options.limits,
            self.options.application_digest,
            &history,
            &attempt.request,
            class,
            input,
            false,
        )?;
        let command = attempt.request.id;
        let durability = attempt.request.durability;
        self.stage(attempt, allow_new, |transaction| {
            class.invoke_command(&contract, &history, command, durability, transaction, input)
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
        if self.pending.is_some() {
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
            &attempt,
            allow_new,
        )? {
            command::Admission::Duplicate => {
                return self
                    .read_receipt(&self.authority, &attempt.request)
                    .map(|receipt| CellStageOutcome::Duplicate(Box::new(receipt)))
            }
            command::Admission::New(maintenance) => maintenance,
        };
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
        command::apply(
            working.database(),
            self.options.limits,
            &attempt,
            maintenance,
            callback,
        )?;
        let update = working
            .capture_cell(&self.options, self.authority.image())
            .map_err(private_failure)?;
        if cellar_logic::cell_group::CellImage::from(update.base)
            != self.authority.cell().version.image
        {
            return Err(ExecutionError::Invalid("captured cell base"));
        }
        let route = self.authority.route();
        let entry = CellGroupEntry {
            binding: GroupEntryBinding {
                group: route.history,
                route_epoch: route.epoch,
                birth_digest: route.birth_digest,
                schema_digest: route.schema_digest,
                proposal: attempt.proposal,
            },
            command: GroupCommand::Mutation(GroupMutation {
                cell: self.authority.cell().version.history.clone(),
                sequence,
                base: update.base,
                result: update.result,
                files: update
                    .files
                    .iter()
                    .map(|file| GroupLtx {
                        min_txid: file.min_txid,
                        max_txid: file.max_txid,
                        page_count: file.page_count,
                        decoded_bytes: file.decoded_bytes,
                        bytes: &file.bytes,
                    })
                    .collect(),
            }),
        };
        let bytes = entry
            .encode(
                route,
                GroupEntryLimits {
                    envelope: self.options.limits.envelope,
                    ..Default::default()
                },
            )
            .map_err(ExecutionError::Protocol)?;
        let preview = self
            .options
            .images
            .preview_cell(
                &self.options,
                self.authority.image(),
                attempt.preview,
                &update,
                cell_image_history_digest(&self.authority.cell().version.history),
            )
            .map_err(storage)?;
        self.authority.check().map_err(authority_failure)?;
        let prepared = CellPreparedCommand::new(
            bytes,
            attempt.proposal,
            self.authority.term(),
            route.history,
            self.authority.engine_owner().clone(),
        );
        self.pending = Some(PendingCell {
            prepared: prepared.clone(),
            request: attempt.request,
            result: CellVersion {
                history: self.authority.cell().version.history.clone(),
                sequence,
                image: update.result.into(),
            },
            _preview: preview,
            submitted: false,
        });
        self.poisoned = false;
        Ok(CellStageOutcome::Prepared(prepared))
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
        let receipt = self.read_receipt(&committed, &pending.request)?;
        self.pending = None;
        self.poisoned = true;
        Ok(receipt)
    }

    /// Resolve the original request without requiring the original log entry.
    /// Missing receipts cannot retire an ambiguous same-term proposal. The
    /// caller must close this disposable producer after successful resolution.
    pub fn reconcile(
        &mut self,
        proposal: ProposalId,
        authority: CellReconciliationAuthority,
    ) -> Result<CellReconciliation, ExecutionError> {
        let pending = self.pending.as_ref().ok_or(ExecutionError::Uncertain)?;
        if pending.prepared.proposal() != proposal
            || !self.authority.accepts_reconciliation(&authority)
        {
            return Err(ExecutionError::Invalid("cell reconciliation identity"));
        }
        authority.check().map_err(authority_failure)?;
        // Staging already established absence in this exact immutable base.
        // In particular an enrollment image need not yet have receipt tables.
        let receipt = if authority.cell().version == self.authority.cell().version {
            Ok(None)
        } else {
            let database = self
                .options
                .images
                .read_cell(&self.options, authority.image())
                .map_err(storage)?;
            receipts::lookup_identity(
                database.connection(),
                &pending.request,
                &self.options.limits,
            )
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
            Err(error) => return Err(error),
            Ok(None) => return Err(ExecutionError::Uncertain),
        };
        authority.check().map_err(authority_failure)?;
        self.pending = None;
        self.poisoned = true;
        Ok(resolution)
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
            working.close().map_err(storage)?;
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
}

pub(crate) fn authority_failure(error: RuntimeError) -> ExecutionError {
    match error {
        RuntimeError::NotReady | RuntimeError::NotLeader | RuntimeError::Closed => {
            ExecutionError::Refused(CommandFailure::Unavailable)
        }
        other => storage(other),
    }
}
