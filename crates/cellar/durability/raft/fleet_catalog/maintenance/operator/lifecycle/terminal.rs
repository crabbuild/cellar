// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual signed offline proof refuses cancellation; actual M9 ends ownership.
use super::super::attempt::Attempt;
use super::super::cancellation::Command;
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const REFUSAL_KIND: u8 = 101;
const COMPLETE_KIND: u8 = 102;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Refusal {
    format: u8,
    request: FleetRequestIdentity,
    intent: [u8; 32],
    command: FleetRequestIdentity,
    execution: [u8; 16],
    offline: FleetRequestIdentity,
    closure: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Finished {
    format: u8,
    request: FleetRequestIdentity,
    intent: [u8; 32],
    attempt: [u8; 16],
    execution: [u8; 16],
    completion: FleetRequestIdentity,
    resume: FleetRequestIdentity,
}

pub struct FleetRestartOperatorCompleted {
    request: FleetRequestIdentity,
    applied: u64,
    observed: u64,
    completion: FleetRestartResumeCompletion,
}

impl FleetRestartOperatorCompleted {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.request
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }

    pub fn resume_completion(&self) -> &FleetRestartResumeCompletion {
        &self.completion
    }
}

fn identity(
    revision: u64,
    operation: [u8; 16],
    parent: [u8; 16],
    proof: [u8; 16],
    kind: u8,
) -> Result<FleetRequestIdentity> {
    let mut digest = Sha256::new();
    digest.update(b"cellar-operator-terminal-1");
    digest.update(operation);
    digest.update(parent);
    digest.update(proof);
    digest.update(revision.to_be_bytes());
    digest.update([kind]);
    let bytes: [u8; 32] = digest.finalize().into();
    let id = bytes[..16]
        .try_into()
        .map_err(|_| FleetCatalogError::Limit)?;
    ProposalId::new(id).map_err(|_| FleetCatalogError::Limit)?;
    Ok(FleetRequestIdentity {
        id,
        operation,
        principal: "fleet-maintenance-driver".into(),
        expected_revision: revision,
    })
}

fn mutation(
    request: &FleetRequestIdentity,
    key: String,
    value: Vec<u8>,
    release: bool,
) -> FleetMutation {
    let mut changes = vec![FleetChange {
        table: FleetTable::Operations,
        key,
        value: Some(value),
    }];
    if release {
        changes.push(FleetChange {
            table: FleetTable::Operations,
            key: super::super::records::ACTIVE.into(),
            value: None,
        });
    }
    FleetMutation {
        id: request.id,
        operation: request.operation,
        principal: request.principal.clone(),
        expected_revision: request.expected_revision,
        changes,
    }
}

impl FleetCatalog {
    pub(in super::super) async fn operator_joined_stop(
        &self,
        root: &FleetSnapshot,
        record: &Record,
        attempt: Option<&Attempt>,
    ) -> Result<bool> {
        let Some(stop) = attempt.and_then(|a| a.facts.stop.as_ref()) else {
            return Ok(false);
        };
        match self
            .restart_stop_closure_inner(root, &stop.request(record)?)
            .await
        {
            Ok(_) => Ok(true),
            Err(FleetCatalogError::Unconfirmed) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(in super::super) async fn operator_cancel_refused(
        &self,
        root: &FleetSnapshot,
        record: &Record,
        command: &Command,
    ) -> Result<bool> {
        Ok(self
            .operator_cancel_refusal_offline(root, record, command)
            .await?
            .is_some())
    }

    /// Return the offline predecessor already validated for this refusal and
    /// snapshot. A status read can check its matching offline fact directly.
    pub(in super::super) async fn operator_cancel_refusal_offline(
        &self,
        root: &FleetSnapshot,
        record: &Record,
        command: &Command,
    ) -> Result<Option<FleetRequestIdentity>> {
        let key = format!(
            "restart-controller-cancel-refused/{}",
            hex::encode(record.request.operation)
        );
        let Some(bytes) = self
            .inner
            .lookup(root.head.tables[FleetTable::Operations.slot()], &key)
            .await?
        else {
            return Ok(None);
        };
        let fact: Refusal = decode(REFUSAL_KIND, &bytes, 4096)?;
        validate_request(&fact.request)?;
        let (offline, _, gate) = self.checked_restart_offline(root, fact.execution).await?;
        let write = mutation(&fact.request, key, bytes, false);
        if fact.format != 1
            || gate.node.boot_binding(gate.preparation.catalog)? != record.boot
            || fact.intent != hash(&record.bytes()?)
            || fact.command != command.request
            || command.execution != Some(fact.execution)
            || fact.offline != offline.request
            || fact.closure != offline.closure
            || fact.request.operation != record.request.operation
            || self.operator_journal(root, &write).await?.is_none()
        {
            return Err(FleetCatalogError::Corrupt("operator cancellation refusal"));
        }
        Ok(Some(offline.request))
    }

    pub(in super::super) async fn refuse_operator_cancel(
        &self,
        root: &FleetSnapshot,
        record: &Record,
        attempt: &Attempt,
        command: &Command,
    ) -> Result<()> {
        if self.operator_cancel_refused(root, record, command).await? {
            return Ok(());
        }
        let (offline, stop, gate) = self
            .checked_restart_offline(root, attempt.request.operation)
            .await?;
        if gate.node.boot_binding(gate.preparation.catalog)? != record.boot
            || command.execution != Some(attempt.request.operation)
            || attempt
                .facts
                .stop
                .as_ref()
                .is_none_or(|s| s.identity != stop.request)
            || attempt.facts.offline.as_ref() != Some(&offline.request)
        {
            return Err(FleetCatalogError::Conflict);
        }
        let fact = Refusal {
            format: 1,
            request: identity(
                root.revision(),
                record.request.operation,
                command.request.id,
                offline.request.id,
                REFUSAL_KIND,
            )?,
            intent: hash(&record.bytes()?),
            command: command.request.clone(),
            execution: attempt.request.operation,
            offline: offline.request,
            closure: offline.closure,
        };
        let key = format!(
            "restart-controller-cancel-refused/{}",
            hex::encode(record.request.operation)
        );
        self.commit_with_disruption(
            root,
            &mutation(
                &fact.request,
                key,
                encode(REFUSAL_KIND, &fact, 4096)?,
                false,
            ),
            None,
            Authority::RestartOperator,
            || Ok(()),
        )
        .await?;
        Ok(())
    }

    /// Historical terminal fact. Reading it never releases a later operation.
    pub fn restart_operator_completed<'a>(
        &'a self,
        root: &'a FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<FleetRestartOperatorCompleted>>> {
        Box::pin(async move {
            self.check_snapshot(root)?;
            let deadline = Deadline::new(self, timeout)?;
            let fact = self
                .run(deadline.remaining()?, async {
                    let key = format!(
                        "restart-controller-finished/{}",
                        hex::encode(operation.bytes())
                    );
                    let Some(bytes) = self
                        .inner
                        .lookup(root.head.tables[FleetTable::Operations.slot()], &key)
                        .await?
                    else {
                        return Ok(None);
                    };
                    let fact: Finished = decode(COMPLETE_KIND, &bytes, 4096)?;
                    let record = self
                        .restart_operator_inner(root, operation)
                        .await?
                        .ok_or(FleetCatalogError::Corrupt("finished operator intent"))?
                        .record;
                    let attempt = self
                        .restart_operator_attempt(root, &record)
                        .await?
                        .ok_or(FleetCatalogError::Corrupt("finished operator attempt"))?;
                    let write = mutation(&fact.request, key, bytes, true);
                    validate_request(&fact.request)?;
                    if fact.format != 1
                        || fact.request.operation != operation.bytes()
                        || fact.request.expected_revision <= attempt.journal.expected_revision
                        || fact.completion.expected_revision >= fact.request.expected_revision
                        || fact.intent != hash(&record.bytes()?)
                        || attempt.journal.id != fact.attempt
                        || attempt.request.operation != fact.execution
                        || attempt.facts.completion.as_ref() != Some(&fact.completion)
                        || attempt.facts.resume.as_ref().map(|s| &s.identity) != Some(&fact.resume)
                    {
                        return Err(FleetCatalogError::Corrupt("finished operator scope"));
                    }
                    let applied = self
                        .operator_journal(root, &write)
                        .await?
                        .ok_or(FleetCatalogError::Corrupt("unadmitted operator completion"))?;
                    Ok(Some((fact, applied)))
                })
                .await?;
            let Some((fact, applied)) = fact else {
                return Ok(None);
            };
            let completion = self
                .restart_resume_completion(
                    root,
                    &FleetRestartResumeCompleteRequest {
                        identity: fact.completion,
                        resume: fact.resume,
                    },
                    deadline.remaining()?,
                )
                .await?;
            Ok(Some(FleetRestartOperatorCompleted {
                request: fact.request,
                applied,
                observed: root.revision(),
                completion,
            }))
        })
    }
}

pub(super) async fn finish(
    driver: &FleetRestartOperatorDriver,
    record: &Record,
    attempt: &Attempt,
    deadline: &Deadline<'_>,
) -> Result<FleetRestartOperatorCompleted> {
    let catalog = &driver.catalog;
    let root = catalog.read(deadline.remaining()?).await?;
    let completion = attempt
        .facts
        .completion
        .as_ref()
        .ok_or(FleetCatalogError::Conflict)?;
    let resume = attempt
        .facts
        .resume
        .as_ref()
        .ok_or(FleetCatalogError::Conflict)?;
    catalog
        .restart_resume_completion(
            &root,
            &FleetRestartResumeCompleteRequest {
                identity: completion.clone(),
                resume: resume.identity.clone(),
            },
            deadline.remaining()?,
        )
        .await?;
    catalog
        .run(deadline.remaining()?, async {
            let active = catalog
                .active_restart_operator_inner(&root)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let current = catalog
                .restart_operator_attempt(&root, record)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            if active.record != *record || current.journal.id != attempt.journal.id {
                return Err(FleetCatalogError::Conflict);
            }
            let fact = Finished {
                format: 1,
                request: identity(
                    root.revision(),
                    record.request.operation,
                    attempt.journal.id,
                    completion.id,
                    COMPLETE_KIND,
                )?,
                intent: hash(&record.bytes()?),
                attempt: attempt.journal.id,
                execution: attempt.request.operation,
                completion: completion.clone(),
                resume: resume.identity.clone(),
            };
            let key = format!(
                "restart-controller-finished/{}",
                hex::encode(record.request.operation)
            );
            catalog
                .commit_with_disruption(
                    &root,
                    &mutation(
                        &fact.request,
                        key,
                        encode(COMPLETE_KIND, &fact, 4096)?,
                        true,
                    ),
                    None,
                    Authority::RestartOperator,
                    || {
                        deadline.remaining()?;
                        Ok(())
                    },
                )
                .await?;
            Ok(())
        })
        .await?;
    let root = catalog.read(deadline.remaining()?).await?;
    catalog
        .restart_operator_completed(
            &root,
            ProposalId::new(record.request.operation)
                .map_err(|_| FleetCatalogError::Corrupt("operator operation"))?,
            deadline.remaining()?,
        )
        .await?
        .ok_or(FleetCatalogError::Unconfirmed)
}
