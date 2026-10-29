// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reconcile physical cancellation before publishing the historical terminal fact.
use super::super::attempt::{Attempt, Stage};
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;

pub(in super::super) async fn reconcile(
    driver: &FleetRestartOperatorDriver,
    root: &FleetSnapshot,
    admission: FleetRestartOperatorAdmission,
    attempt: Option<Attempt>,
    command: Command,
    deadline: &Deadline<'_>,
) -> Result<FleetRestartOperatorProgress> {
    let catalog = &driver.catalog;
    let observer = &driver.observer;
    let record = admission.record.clone();
    let mut value = FleetRestartOperatorProgress {
        admission,
        phase: FleetRestartOperatorPhase::CancelRequested,
        checked_histories: 0,
        total_histories: root.head.history_count,
        inventory_revision: root.revision(),
        blocked: None,
        ready: None,
        cancelled: None,
        completed: None,
        waiting: false,
    };
    if let Some(attempt) = attempt
        .as_ref()
        .filter(|a| matches!(a.stage, Stage::Cancel { .. }))
    {
        let Stage::Cancel { gated } = attempt.stage else {
            return Err(FleetCatalogError::Conflict);
        };
        value.phase = FleetRestartOperatorPhase::Cancelling;
        let effect = if gated {
            Effect::Gate {
                request: attempt.request.clone(),
            }
        } else {
            Effect::Preparation {
                request: attempt.request.clone(),
            }
        };
        let outcome = if gated {
            catalog
                .cancel_restart_gate(
                    root,
                    &gate_request(&record, &command, &attempt.request)?,
                    Some(observer),
                    deadline.remaining()?,
                )
                .await
        } else {
            catalog
                .cancel_restart_preparation(
                    root,
                    &preparation_request(&record, &attempt.request)?,
                    deadline.remaining()?,
                )
                .await
        };
        match outcome {
            Ok(_) => {
                value.cancelled =
                    Some(complete(catalog, &record, &command, effect, deadline).await?)
            }
            Err(FleetCatalogError::Conflict)
                if root.revision() > attempt.request.expected_revision =>
            {
                catalog
                    .run(
                        deadline.remaining()?,
                        catalog.claim_restart_operator_attempt(
                            root,
                            &record,
                            Some(attempt.journal.id),
                            Stage::Cancel { gated },
                        ),
                    )
                    .await?;
            }
            Err(error) => return Err(error),
        }
    } else if let Some(operation) = command.execution {
        let original = catalog
            .run(deadline.remaining()?, async {
                let Some(bytes) = catalog
                    .inner
                    .lookup(
                        root.head.tables[FleetTable::Operations.slot()],
                        &hex::encode(operation),
                    )
                    .await?
                else {
                    return Ok(None);
                };
                let original: super::super::super::records::Record =
                    decode(super::super::super::records::KIND, &bytes, 4096)?;
                original.validate(catalog)?;
                if original.request.operation != operation
                    || original.node.boot_binding(original.catalog)? != record.boot
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                catalog.confirm_restart(root, &original).await?;
                let gated = catalog
                    .inner
                    .lookup(
                        root.head.tables[FleetTable::Operations.slot()],
                        &super::super::super::gate::Gate::key(operation),
                    )
                    .await?
                    .is_some();
                if gated {
                    catalog.checked_restart_gate(root, operation).await?;
                }
                Ok(Some(gated))
            })
            .await?;
        if let Some(gated) = original {
            // A winning birth/movement remains owned work. Finish that exact
            // choice before asking the low-level cancellation to release it.
            if gated {
                match catalog
                    .reconcile_restart_work(
                        ProposalId::new(operation)
                            .map_err(|_| FleetCatalogError::Corrupt("cancellation execution"))?,
                        record.birth_resolution,
                        &driver.namespaces,
                        Some(observer),
                        driver.evidence.as_deref(),
                        deadline.remaining()?,
                    )
                    .await?
                {
                    FleetRestartSettlementProgress::CatalogClear { .. } => (),
                    FleetRestartSettlementProgress::Blocked(blocker) => {
                        value.blocked = Some(blocker);
                        return Ok(value);
                    }
                    _ => return Ok(value),
                }
            }
            let previous = attempt
                .as_ref()
                .ok_or(FleetCatalogError::Corrupt("missing cancellation attempt"))?;
            catalog
                .run(
                    deadline.remaining()?,
                    catalog.claim_restart_operator_attempt(
                        root,
                        &record,
                        Some(previous.journal.id),
                        Stage::Cancel { gated },
                    ),
                )
                .await?;
        } else {
            value.cancelled =
                Some(complete(catalog, &record, &command, Effect::Unadmitted, deadline).await?);
        }
    } else {
        value.cancelled =
            Some(complete(catalog, &record, &command, Effect::Unadmitted, deadline).await?);
    }
    if value.cancelled.is_some() {
        value.phase = FleetRestartOperatorPhase::Cancelled;
    }
    value.admission.observed = catalog.read(deadline.remaining()?).await?.revision();
    Ok(value)
}

fn preparation_request(
    record: &Record,
    identity: &FleetRequestIdentity,
) -> Result<FleetRestartRequest> {
    Ok(FleetRestartRequest {
        identity: identity.clone(),
        node: NodeId::new(record.boot.node)
            .map_err(|_| FleetCatalogError::Corrupt("operator node"))?,
        expected_process: ProcessGeneration::new(record.boot.process)
            .map_err(|_| FleetCatalogError::Corrupt("operator process"))?,
        grace_seconds: record.grace_seconds,
    })
}

fn gate_request(
    record: &Record,
    command: &Command,
    identity: &FleetRequestIdentity,
) -> Result<FleetRestartGateCancelRequest> {
    let request = preparation_request(record, identity)?;
    Ok(FleetRestartGateCancelRequest {
        identity: request.identity,
        node: request.node,
        expected_process: request.expected_process,
        fence: ProposalId::new(command.fence)
            .map_err(|_| FleetCatalogError::Corrupt("operator fence"))?,
    })
}

impl FleetCatalog {
    pub(super) async fn check_operator_cancel_effect(
        &self,
        root: &FleetSnapshot,
        record: &Record,
        command: &Command,
        effect: &Effect,
    ) -> Result<()> {
        let committed = match effect {
            Effect::Unadmitted => {
                if let Some(operation) = command.execution {
                    if self
                        .inner
                        .lookup(
                            root.head.tables[FleetTable::Operations.slot()],
                            &hex::encode(operation),
                        )
                        .await?
                        .is_some()
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                return Ok(());
            }
            Effect::Preparation { request } => {
                self.restart_preparation_cancellation_applied(
                    root,
                    &preparation_request(record, request)?,
                )
                .await?
            }
            Effect::Gate { request } => {
                self.restart_gate_cancellation_applied(
                    root,
                    &gate_request(record, command, request)?,
                )
                .await?
            }
        };
        committed.ok_or(FleetCatalogError::Unconfirmed)?;
        Ok(())
    }
}

async fn complete(
    catalog: &FleetCatalog,
    record: &Record,
    command: &Command,
    effect: Effect,
    deadline: &Deadline<'_>,
) -> Result<FleetRestartOperatorCancelled> {
    let root = catalog.read(deadline.remaining()?).await?;
    catalog
        .run(deadline.remaining()?, async {
            let active = catalog
                .active_restart_operator_inner(&root)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            if active.record != *record
                || catalog
                    .operator_cancel_command(&root, record)
                    .await?
                    .as_ref()
                    != Some(command)
            {
                return Err(FleetCatalogError::Conflict);
            }
            catalog
                .check_operator_cancel_effect(&root, record, command, &effect)
                .await?;
            let terminal = Terminal {
                format: 1,
                request: super::super::attempt::identity(
                    root.revision(),
                    record.request.operation,
                )?,
                input_root: hash(&encode(HEAD_KIND, &root.head, ROOT_LIMIT)?),
                parent: command.request.id,
                command: hash(&encode(COMMAND_KIND, command, 4096)?),
                effect,
            };
            terminal.validate(command)?;
            let committed = catalog
                .commit_with_disruption(
                    &root,
                    &terminal.mutation()?,
                    None,
                    Authority::RestartOperator,
                    || {
                        deadline.remaining()?;
                        Ok(())
                    },
                )
                .await?;
            Ok(FleetRestartOperatorCancelled {
                record: terminal,
                applied: committed.applied_revision,
                observed: committed.snapshot.revision(),
            })
        })
        .await
}
