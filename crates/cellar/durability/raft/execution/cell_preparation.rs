// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Captured immutable input can be verified off the SQLite owner. Completing
//! physical preparation grants no proposal, serving or acknowledgement authority.

use super::*;
use std::sync::Arc;

pub(crate) struct CellPrivatePreparation {
    options: Options,
    base: crate::raft::cell_groups::CellImagePin,
    update: crate::raft::working::CapturedUpdate,
    preview_id: ImageId,
    history: [u8; 32],
    attempt: CommandAttempt,
    result: CellVersion,
    bytes: Vec<u8>,
    group: cellar_logic::cell_group::GroupHistory,
    term: Term,
    engine: Arc<()>,
    log_reservation: Arc<crate::raft::cell_groups::log_retention::LogReservation>,
    stamp: Arc<()>,
    rpc_step: Option<u64>,
    rpc_prior_steps: u64,
}

pub(crate) enum CellStageCapture {
    Captured(Box<CellPrivatePreparation>),
    Duplicate(Box<CellCommittedReceipt>),
}

pub(crate) struct CellPrivatePrepared {
    input: Option<Arc<CellPrivatePreparation>>,
    preview: Option<Preview>,
}

impl CellPrivatePrepared {
    /// A prepared image has not been submitted. Release its private retention
    /// on cancellation while keeping the pin through physical settlement.
    pub(crate) fn discard(mut self) -> Result<(), ExecutionError> {
        self.retire()
    }

    pub(crate) fn checked(self, check: Result<(), ExecutionError>) -> Result<Self, ExecutionError> {
        match check {
            Ok(()) => Ok(self),
            Err(error) => {
                self.discard()?;
                Err(error)
            }
        }
    }

    fn retire(&mut self) -> Result<(), ExecutionError> {
        if let Some(preview) = &self.preview {
            self.input
                .as_ref()
                .expect("owned private preparation")
                .options
                .images
                .retire(preview)
                .map_err(storage)?;
            self.preview = None;
        }
        Ok(())
    }
}

impl Drop for CellPrivatePrepared {
    fn drop(&mut self) {
        if let Err(error) = self.retire() {
            tracing::error!(%error, "failed to retire unadopted private image");
        }
    }
}

impl CellPrivatePreparation {
    /// No SQLite connection, mutable transaction or reusable Raft authority
    /// crosses this boundary. The original owner retains its writer and bytes.
    pub(crate) fn prepare(self: Arc<Self>) -> Result<CellPrivatePrepared, ExecutionError> {
        let preview = self
            .options
            .images
            .preview_cell(
                &self.options,
                &self.base,
                self.preview_id,
                &self.update,
                self.history,
            )
            .map_err(storage)?;
        Ok(CellPrivatePrepared {
            input: Some(self),
            preview: Some(preview),
        })
    }

    pub(super) fn checkpoint(mut self, step: u64) -> Self {
        self.rpc_step = Some(step);
        self
    }

    pub(super) fn completed_rpc(mut self, prior_steps: u64) -> Self {
        self.rpc_prior_steps = prior_steps;
        self
    }
}

impl CellPrivateExecutor {
    pub(super) fn complete_capture(
        &mut self,
        captured: CellStageCapture,
    ) -> Result<CellStageOutcome, ExecutionError> {
        match captured {
            CellStageCapture::Duplicate(receipt) => Ok(CellStageOutcome::Duplicate(receipt)),
            CellStageCapture::Captured(input) => {
                self.complete_preparation(Arc::new(*input).prepare()?)
            }
        }
    }

    pub(super) fn capture_stage_input(
        &mut self,
        attempt: CommandAttempt,
        preparation: StagePreparation,
    ) -> Result<CellPrivatePreparation, ExecutionError> {
        if self.preparing.is_some() || self.pending.is_some() {
            return Err(ExecutionError::Busy);
        }
        let StagePreparation {
            sequence,
            log_reservation,
            ..
        } = preparation;
        let working = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?;
        let update = working
            .capture_cell(&self.options, self.authority.image())
            .map_err(private_failure)?;
        if cellar_logic::cell_group::CellImage::from(update.base)
            != self.authority.cell().version.image
        {
            return Err(ExecutionError::Invalid("captured cell base"));
        }
        let route = self.authority.route();
        let bytes = CellGroupEntry {
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
        }
        .encode(
            route,
            GroupEntryLimits {
                envelope: self.options.limits.envelope,
                ..Default::default()
            },
        )
        .map_err(ExecutionError::Protocol)?;
        log_reservation
            .encoded(bytes.len())
            .map_err(|error| authority_failure(error.into()))?;
        let preview_id = super::super::images::cell_preview_id(
            &self.authority.cell().version.history,
            sequence.get(),
            &update,
            attempt.preview,
        )?;
        let stamp = Arc::new(());
        self.preparing = Some(stamp.clone());
        Ok(CellPrivatePreparation {
            options: self.options.clone(),
            base: self.authority.image().clone(),
            history: cell_image_history_digest(&self.authority.cell().version.history),
            result: CellVersion {
                history: self.authority.cell().version.history.clone(),
                sequence,
                image: update.result.into(),
            },
            update,
            preview_id,
            attempt,
            bytes,
            group: route.history,
            term: self.authority.term(),
            engine: self.authority.engine_owner().clone(),
            log_reservation,
            stamp,
            rpc_step: None,
            rpc_prior_steps: 0,
        })
    }

    /// Only the original live SQL producer may adopt the independently verified
    /// result. Deadline/response cancellation is checked by its owning worker.
    pub(crate) fn complete_preparation(
        &mut self,
        mut prepared: CellPrivatePrepared,
    ) -> Result<CellStageOutcome, ExecutionError> {
        let checked = (|| {
            let input = prepared.input.as_ref().expect("owned private preparation");
            if self.pending.is_some()
                || self
                    .preparing
                    .as_ref()
                    .is_none_or(|stamp| !Arc::ptr_eq(stamp, &input.stamp))
            {
                return Err(ExecutionError::Invalid("private preparation producer"));
            }
            self.authority.check().map_err(authority_failure)?;
            if input.group != self.authority.route().history
                || input.term != self.authority.term()
                || !Arc::ptr_eq(&input.engine, self.authority.engine_owner())
            {
                return Err(ExecutionError::Invalid("private preparation authority"));
            }
            if Arc::strong_count(input) != 1 {
                return Err(ExecutionError::Invalid("private preparation still running"));
            }
            Ok(())
        })();
        prepared = prepared.checked(checked)?;
        // The physical task and its retry owner have joined. Neither the
        // immutable input nor its only remaining Arc can escape this frame.
        let input = Arc::try_unwrap(prepared.input.take().expect("owned private preparation"))
            .unwrap_or_else(|_| unreachable!("joined private preparation"));
        let preview = prepared.preview.take().expect("owned private preview");
        let image = super::super::CellPreparedImage::new(
            input.engine.clone(),
            input.group,
            input.term,
            &input.bytes,
            &preview,
        );
        let command = CellPreparedCommand::new(
            input.bytes,
            input.attempt.proposal,
            input.term,
            input.group,
            input.engine,
        )
        .with_log_reservation(input.log_reservation)
        .with_prepared_image(&image);
        self.pending = Some(PendingCell {
            prepared: command.clone(),
            request: input.attempt.request,
            result: input.result,
            _preview: preview,
            _prepared_image: image,
            submitted: false,
            rpc_step: input.rpc_step,
            rpc_prior_steps: input.rpc_prior_steps,
        });
        self.preparing = None;
        self.poisoned = false;
        Ok(CellStageOutcome::Prepared(command))
    }
}
