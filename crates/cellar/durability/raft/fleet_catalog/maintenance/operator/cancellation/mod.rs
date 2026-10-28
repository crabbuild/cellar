// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable cancellation intent precedes fencing; terminal release follows it.
mod records;
mod work;
use super::*;
pub(super) use records::Command;
use records::{Effect, Terminal, COMMAND_KIND, TERMINAL_KIND};

#[derive(Clone, Debug)]
pub struct FleetRestartOperatorCancelRequest {
    pub identity: FleetRequestIdentity,
    pub fence: ProposalId,
}

/// Human intent only; cancellation and operator-slot release remain pending.
pub struct FleetRestartOperatorCancelAdmission {
    record: Command,
    applied: u64,
    observed: u64,
}

impl FleetRestartOperatorCancelAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

/// Historical terminal cancellation. An admitted preparation returns to excluded
/// Ready; this fact grants no placement capacity or application-admission lease.
pub struct FleetRestartOperatorCancelled {
    record: Terminal,
    applied: u64,
    observed: u64,
}

impl FleetRestartOperatorCancelled {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn cancellation_request(&self) -> [u8; 16] {
        self.record.parent
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

impl FleetCatalog {
    /// Root the human command before any cancellation effect. Its CAS fences
    /// uncommitted old SDK CAS requests and prevents further preparation steps.
    pub fn cancel_restart_operator<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        request: &'a FleetRestartOperatorCancelRequest,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<FleetRestartOperatorCancelAdmission>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            validate_request(&request.identity)?;
            self.run(timeout, async {
                let current = self.inner.current().await?;
                let intent = self
                    .restart_operator_inner(
                        &current,
                        ProposalId::new(request.identity.operation)
                            .map_err(|_| FleetCatalogError::Invalid("operator operation"))?,
                    )
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if let Some(command) = self
                    .operator_cancel_candidate(request.identity.id, &intent.record)
                    .await?
                {
                    if command.request != request.identity || command.fence != request.fence.bytes()
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    if let Some(applied) = self
                        .operator_journal(&current, &command.mutation()?)
                        .await?
                    {
                        return Ok(FleetRestartOperatorCancelAdmission {
                            record: command,
                            applied,
                            observed: current.revision(),
                        });
                    }
                }
                if current.token != snapshot.token
                    || request.identity.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let active = self
                    .active_restart_operator_inner(snapshot)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if active.record != intent.record {
                    return Err(FleetCatalogError::Conflict);
                }
                if self
                    .operator_cancel_command(snapshot, &intent.record)
                    .await?
                    .is_some()
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let attempt = self
                    .restart_operator_attempt(snapshot, &intent.record)
                    .await?;
                let command = Command {
                    format: 1,
                    request: request.identity.clone(),
                    intent: hash(&intent.record.bytes()?),
                    input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                    fence: request.fence.bytes(),
                    execution: attempt.as_ref().map(|a| a.request.operation),
                };
                command.validate(&intent.record)?;
                let committed = self
                    .commit_with_disruption(
                        snapshot,
                        &command.mutation()?,
                        None,
                        Authority::RestartOperator,
                        || Ok(()),
                    )
                    .await?;
                Ok(FleetRestartOperatorCancelAdmission {
                    record: command,
                    applied: committed.applied_revision,
                    observed: committed.snapshot.revision(),
                })
            })
            .await
        })
    }

    pub fn restart_operator_cancelled<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<FleetRestartOperatorCancelled>>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            self.run(timeout, async {
                let Some(bytes) = self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Operations.slot()],
                        &Terminal::key(operation.bytes()),
                    )
                    .await?
                else {
                    return Ok(None);
                };
                let terminal: Terminal = decode(TERMINAL_KIND, &bytes, 4096)?;
                let intent = self
                    .restart_operator_inner(snapshot, operation)
                    .await?
                    .ok_or(FleetCatalogError::Corrupt("missing cancelled operator"))?;
                let command = self
                    .operator_cancel_command(snapshot, &intent.record)
                    .await?
                    .ok_or(FleetCatalogError::Corrupt("missing operator cancellation"))?;
                terminal.validate(&command)?;
                let applied = self
                    .operator_journal(snapshot, &terminal.mutation()?)
                    .await?
                    .ok_or(FleetCatalogError::Corrupt(
                        "unadmitted operator cancellation terminal",
                    ))?;
                self.check_operator_cancel_effect(
                    snapshot,
                    &intent.record,
                    &command,
                    &terminal.effect,
                )
                .await?;
                Ok(Some(FleetRestartOperatorCancelled {
                    record: terminal,
                    applied,
                    observed: snapshot.revision(),
                }))
            })
            .await
        })
    }

    pub(super) async fn operator_cancel_command(
        &self,
        snapshot: &FleetSnapshot,
        intent: &Record,
    ) -> Result<Option<Command>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Command::key(intent.request.operation),
            )
            .await?
        else {
            return Ok(None);
        };
        let command: Command = decode(COMMAND_KIND, &bytes, 4096)?;
        command.validate(intent)?;
        if self
            .operator_cancel_candidate(command.request.id, intent)
            .await?
            .as_ref()
            != Some(&command)
            || self
                .operator_journal(snapshot, &command.mutation()?)
                .await?
                .is_none()
        {
            return Err(FleetCatalogError::Corrupt(
                "operator cancellation projection",
            ));
        }
        Ok(Some(command))
    }

    async fn operator_cancel_candidate(
        &self,
        id: [u8; 16],
        intent: &Record,
    ) -> Result<Option<Command>> {
        let Some((mutation, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
        else {
            return Ok(None);
        };
        mutation.validate()?;
        let bytes = mutation
            .changes
            .iter()
            .find(|c| {
                c.table == FleetTable::Operations && c.key == Command::key(intent.request.operation)
            })
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let command: Command = decode(COMMAND_KIND, bytes, 4096)?;
        command.validate(intent)?;
        if command.request.id != id || command.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(command))
    }

    pub(in crate::raft::fleet_catalog::maintenance::operator) async fn operator_journal(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
    ) -> Result<Option<u64>> {
        let Some((stored, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(mutation.id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
        else {
            return Ok(None);
        };
        if stored != *mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let applied = self
            .inner
            .committed(
                &snapshot.head,
                &hex::encode(mutation.id),
                hash(&encode(REQUEST_KIND, mutation, REQUEST_LIMIT)?),
            )
            .await?;
        if applied.is_some_and(|r| mutation.expected_revision.checked_add(1) != Some(r)) {
            return Err(FleetCatalogError::Corrupt("operator journal revision"));
        }
        Ok(applied)
    }
}

pub(super) use work::reconcile;

pub(super) fn protected(key: &str) -> bool {
    key.starts_with(records::COMMAND_PREFIX) || key.starts_with(records::TERMINAL_PREFIX)
}
