// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Committed outputs retain proof admission, independently of retired SQL work.
use super::*;

pub(super) struct PendingOutput {
    runtime: CellGroupRuntime,
    route: GroupRoute,
    receipt: Box<CellSqlReceipt>,
    proof: Box<crate::raft::cell_groups::S3OutputProof>,
}

impl CellCommandInvocation {
    pub(super) async fn response(
        &mut self,
        receipt: Box<CellSqlReceipt>,
    ) -> Result<CellCommandResponse, CellDispatchError> {
        if receipt.receipt().receipt.command() != self.request.id
            || receipt.receipt().receipt.operation != self.request.contract.operation
            || receipt.receipt().receipt.contract_digest != self.request.contract.digest
            || receipt.receipt().receipt.request_digest
                != command_request_digest(&self.request.input)
            || receipt.receipt().version.history != self.cell
        {
            return Err(CellDispatchError::Invalid("command receipt binding"));
        }
        // SQL has resolved its producer. Archive failure and output fencing
        // cannot turn that committed result back into unresolved SQL ownership.
        let mut origin = self
            .origin
            .take()
            .ok_or(CellDispatchError::Invalid("resolved command origin"))?;
        let required = self
            .request
            .durability
            .max(receipt.receipt().receipt.minimum_durability());
        if required == Durability::S3 {
            let owner = self
                .dispatcher
                .s3
                .as_ref()
                .ok_or(CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::UnsupportedCapability,
                )))?;
            let limits = origin.options.limits();
            // Do not retain the preparation's SQL slot across archive waiting.
            origin.prepared = None;
            let settled = owner
                .command(
                    &origin.runtime,
                    origin.route.clone(),
                    receipt,
                    limits,
                    self.session.deadline_ms(),
                )
                .await?;
            self.output = Some(PendingOutput {
                runtime: origin.runtime,
                route: origin.route,
                receipt: settled.receipt,
                proof: settled.proof,
            });
            return self.finish_output().await;
        }
        let token = self.output_token(&receipt).await?;
        Ok(CellCommandResponse {
            output: CommandOutput::Local {
                receipt,
                token,
                s3: None,
            },
        })
    }

    pub(super) async fn finish_output(&mut self) -> Result<CellCommandResponse, CellDispatchError> {
        let output = self
            .output
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing committed output"))?;
        // The receipt already proves this command's contiguous durable apply.
        // A later exact mutation for another cell need not delay its S3 output;
        // every pending entry must carry the leader's independence certificate.
        // Controls, inherited or same-cell work still require ordinary apply.
        // Never reconstruct the result by reconciling its retired SQL producer.
        let authority = output
            .runtime
            .independent_cell_authority(output.route.history, self.cell.clone())
            .await
            .map_err(CellDispatchError::Runtime)?;
        if authority.route() != &output.route {
            return Err(CellDispatchError::Invalid("S3 response current route"));
        }
        if !authority
            .cell()
            .version
            .covers(&output.receipt.receipt().version)
            .map_err(|_| CellDispatchError::Invalid("S3 response receipt history"))?
        {
            return Err(CellDispatchError::Invalid(
                "S3 response current cell version",
            ));
        }
        output
            .proof
            .require_current(&self.dispatcher.domain, self.session.deadline_ms())
            .await?;
        authority.check().map_err(CellDispatchError::Runtime)?;
        let token = self.output_token(&output.receipt).await?;
        authority.check().map_err(CellDispatchError::Runtime)?;
        if self.dispatcher.domain.mono_ms().map_err(|_| closed())? >= self.session.deadline_ms() {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        let output = self
            .output
            .take()
            .ok_or(CellDispatchError::Invalid("consumed committed output"))?;
        if let Some(invocation) = self.archive_invocation.take() {
            invocation.completed(output.receipt.receipt().position.index);
        }
        Ok(CellCommandResponse {
            output: CommandOutput::Local {
                receipt: output.receipt,
                token,
                s3: Some(output.proof),
            },
        })
    }

    async fn output_token(
        &self,
        receipt: &CellSqlReceipt,
    ) -> Result<CellVersionToken, CellDispatchError> {
        let now = self
            .dispatcher
            .domain
            .run(async { crate::asyncrt::wall_ms().max(0) as u64 })
            .await
            .map_err(|_| closed())?;
        self.dispatcher
            .keys
            .issue_cell(
                &receipt.receipt().observation(),
                self.dispatcher.class.schema_digest(),
                now,
            )
            .map_err(CellDispatchError::Version)
    }
}
