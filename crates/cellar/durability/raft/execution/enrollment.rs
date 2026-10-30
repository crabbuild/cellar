// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable SQLite initialization under a physical group's leader fence.

use super::cell::authority_failure;
use super::*;
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits, GroupLtx,
};
use crate::raft::cell_groups::{
    cell_image_history_digest, CellEnrollmentAuthority, CellEnrollmentState, EnrollmentRecord,
};

pub struct CellEnrollmentAttempt {
    pub creation: ProposalId,
    pub preview: ImageId,
    pub inputs: CommandInputs,
}

pub enum CellEnrollmentResolution {
    Committed(Box<EnrollmentRecord>),
    NotCommitted,
    Conflict,
}

struct Pending {
    record: EnrollmentRecord,
    _preview: Preview,
    submitted: bool,
}

pub struct CellPrivateEnrollment {
    options: Options,
    authority: CellEnrollmentAuthority,
    working: Option<WorkingImage>,
    pending: Option<Pending>,
    staged: bool,
    lock: PrivateLock,
}

impl CellPrivateEnrollment {
    pub fn open(
        options: PrivateOptions,
        authority: CellEnrollmentAuthority,
    ) -> Result<Self, ExecutionError> {
        Self::open_image(options.into(), authority)
    }

    pub fn open_rooted(
        options: RootPrivateOptions,
        authority: CellEnrollmentAuthority,
    ) -> Result<Self, ExecutionError> {
        Self::open_image(options.into(), authority)
    }

    fn open_image(
        options: Options,
        authority: CellEnrollmentAuthority,
    ) -> Result<Self, ExecutionError> {
        options.limits.validate()?;
        options.images.validate_workspace(options.limits)?;
        authority.check().map_err(authority_failure)?;
        if authority.state().record().is_some()
            || authority.state().route().schema_digest != options.application_digest
        {
            return Err(ExecutionError::Invalid("initialization binding"));
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
        let working = WorkingImage::open(
            &options,
            None,
            cell_image_history_digest(authority.state().cell()),
        )
        .map_err(storage)?;
        authority.check().map_err(authority_failure)?;
        Ok(Self {
            options,
            authority,
            working: Some(working),
            pending: None,
            staged: false,
            lock,
        })
    }

    /// Run once on the admitted blocking worker. Followers consume the encoded
    /// initial LTX image and never execute the initializer.
    pub fn prepare(
        &mut self,
        attempt: CellEnrollmentAttempt,
        initialize: impl FnOnce(&CommandTransaction<'_>) -> anyhow::Result<()>,
    ) -> Result<CellPreparedCommand, ExecutionError> {
        if self.staged {
            return Err(ExecutionError::Busy);
        }
        self.authority.check().map_err(authority_failure)?;
        self.staged = true;
        let working = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?;
        working.begin_mutation();
        working
            .database()
            .transaction(attempt.inputs, |_, tx| initialize(tx))
            .map_err(private_failure)?;
        let update = working
            .capture_initial(&self.options)
            .map_err(private_failure)?;
        let state = self.authority.state();
        let route = state.route();
        let entry = CellGroupEntry {
            binding: GroupEntryBinding {
                group: route.history,
                route_epoch: route.epoch,
                birth_digest: route.birth_digest,
                schema_digest: route.schema_digest,
                proposal: attempt.creation,
            },
            command: GroupCommand::Enroll {
                cell: state.cell().clone(),
                image: update.result,
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
            },
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
        let record = EnrollmentRecord::new(
            state.cell().clone(),
            attempt.creation,
            route.schema_digest,
            update.result.into(),
        )
        .map_err(storage)?;
        let preview = self
            .options
            .images
            .preview_initial(
                &self.options,
                attempt.preview,
                &update,
                cell_image_history_digest(state.cell()),
            )
            .map_err(storage)?;
        self.authority.check().map_err(authority_failure)?;
        self.pending = Some(Pending {
            record,
            _preview: preview,
            submitted: false,
        });
        Ok(CellPreparedCommand::new(
            bytes,
            attempt.creation,
            state.term(),
            state.route().history,
            state.engine_owner().clone(),
        ))
    }

    pub fn mark_submitted(&mut self, creation: ProposalId) -> Result<(), ExecutionError> {
        self.authority.check().map_err(authority_failure)?;
        let pending = self.pending.as_mut().ok_or(ExecutionError::Uncertain)?;
        if pending.record.creation() != creation || pending.submitted {
            return Err(ExecutionError::Invalid("enrollment submission"));
        }
        pending.submitted = true;
        Ok(())
    }

    /// The immutable creation record survives later cell mutations and original
    /// log compaction. Absence/conflict retires an old producer only behind a
    /// strictly newer applied term. No callback or initial-image replay occurs.
    pub fn resolve(
        &mut self,
        creation: ProposalId,
        state: CellEnrollmentState,
    ) -> Result<CellEnrollmentResolution, ExecutionError> {
        let pending = self.pending.as_ref().ok_or(ExecutionError::Uncertain)?;
        if pending.record.creation() != creation || !self.authority.state().same_binding(&state) {
            return Err(ExecutionError::Invalid("enrollment resolution binding"));
        }
        state.check().map_err(authority_failure)?;
        let outcome = match state.record() {
            Some(record) if record == &pending.record => {
                CellEnrollmentResolution::Committed(Box::new(record.clone()))
            }
            _ if state.term() <= self.authority.state().term() => {
                return Err(ExecutionError::Uncertain)
            }
            Some(_) => CellEnrollmentResolution::Conflict,
            None => CellEnrollmentResolution::NotCommitted,
        };
        self.pending = None;
        Ok(outcome)
    }

    pub fn close(mut self) -> Result<(), ExecutionError> {
        let store = self.options.images.root_store().cloned();
        if let Some(working) = self.working.take() {
            working.close().map_err(storage)?;
        }
        drop(self.options);
        drop(self.lock);
        if let Some(store) = store {
            if store.usage().map_err(storage)?.quarantined {
                return Err(storage(cellar_ltx::internal::StoreError::Quarantined));
            }
        }
        Ok(())
    }
}
