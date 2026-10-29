// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Separate authenticated grants are journaled before any SDK effect.
use super::super::attempt::Stage;
use super::*;
use serde::Deserialize;

const KIND: u8 = 100;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum FleetRestartOperatorAction {
    Stop {
        process: [u8; 16],
        stop: [u8; 16],
    },
    Return {
        process: [u8; 16],
    },
    Resume {
        attestation: Box<FleetCapacityAttestation>,
        resume: [u8; 16],
    },
}

#[derive(Clone, Debug)]
pub struct FleetRestartOperatorActionRequest {
    pub identity: FleetRequestIdentity,
    pub node: NodeId,
    pub action: FleetRestartOperatorAction,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in super::super) struct Grant {
    format: u8,
    pub request: FleetRequestIdentity,
    intent: [u8; 32],
    input_root: [u8; 32],
    pub action: FleetRestartOperatorAction,
}

pub struct FleetRestartOperatorActionAdmission {
    record: Grant,
    applied: u64,
    observed: u64,
}

impl FleetRestartOperatorActionAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn action(&self) -> &FleetRestartOperatorAction {
        &self.record.action
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

impl FleetRestartOperatorAction {
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::Stop { .. } => "stop",
            Self::Return { .. } => "return",
            Self::Resume { .. } => "resume",
        }
    }

    fn validate(&self, record: &Record) -> Result<()> {
        match self {
            Self::Stop { process, stop } => {
                if *process != record.boot.process {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                ProposalId::new(*stop)
                    .map_err(|_| FleetCatalogError::Invalid("operator stop child"))?;
            }
            Self::Return { process } => {
                ProcessGeneration::new(*process)
                    .map_err(|_| FleetCatalogError::Invalid("operator return process"))?;
                if *process == record.boot.process {
                    return Err(FleetCatalogError::PayloadConflict);
                }
            }
            Self::Resume {
                attestation,
                resume,
            } => {
                attestation.validate()?;
                ProposalId::new(*resume)
                    .map_err(|_| FleetCatalogError::Invalid("operator resume child"))?;
                if attestation.boot.catalog != record.boot.catalog
                    || attestation.boot.fleet != record.boot.fleet
                    || attestation.boot.node != record.boot.node
                    || attestation.boot.disk != record.boot.disk
                    || attestation.boot.engine != record.boot.engine
                    || attestation.boot.process == record.boot.process
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
            }
        }
        Ok(())
    }
}

impl Grant {
    pub fn key(operation: [u8; 16], name: &str) -> String {
        format!(
            "restart-controller-action/{}/{name}",
            hex::encode(operation)
        )
    }

    fn validate(&self, record: &Record) -> Result<()> {
        validate_request(&self.request)?;
        self.action.validate(record)?;
        if self.format != 1
            || self.intent != hash(&record.bytes()?)
            || self.input_root == [0; 32]
            || self.request.operation != record.request.operation
            || self.request.expected_revision <= record.request.expected_revision
        {
            return Err(FleetCatalogError::Corrupt("operator action grant"));
        }
        Ok(())
    }

    fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: Self::key(self.request.operation, self.action.name()),
                value: Some(encode(KIND, self, 4096)?),
            }],
        })
    }
}

impl FleetCatalog {
    /// Record one separate human grant. This does not stop, boot, resume or
    /// restore capacity; the owned controller consumes the guarded SDK proofs.
    pub fn admit_restart_operator_action<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        request: &'a FleetRestartOperatorActionRequest,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<FleetRestartOperatorActionAdmission>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            validate_request(&request.identity)?;
            self.run(timeout, async {
                let current = self.inner.current().await?;
                let record = self
                    .restart_operator_inner(
                        &current,
                        ProposalId::new(request.identity.operation)
                            .map_err(|_| FleetCatalogError::Invalid("operator operation"))?,
                    )
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?
                    .record;
                if record.boot.node != request.node.bytes() {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                request.action.validate(&record)?;
                if let Some(grant) = self
                    .operator_action_candidate(request.identity.id, &record)
                    .await?
                {
                    if grant.request != request.identity || grant.action != request.action {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    if let Some(applied) =
                        self.operator_journal(&current, &grant.mutation()?).await?
                    {
                        return Ok(FleetRestartOperatorActionAdmission {
                            record: grant,
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
                if self
                    .active_restart_operator_inner(snapshot)
                    .await?
                    .is_none_or(|a| a.record != record)
                    || request.action.name() != "resume"
                        && self
                            .operator_action(snapshot, &record, request.action.name())
                            .await?
                            .is_some()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let attempt = self
                    .restart_operator_attempt(snapshot, &record)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                match &request.action {
                    FleetRestartOperatorAction::Stop { .. }
                        if matches!(attempt.stage, Stage::AwaitStop { .. }) =>
                    {
                        if self
                            .operator_cancel_command(snapshot, &record)
                            .await?
                            .is_some()
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                    }
                    FleetRestartOperatorAction::Return { .. }
                        if matches!(attempt.stage, Stage::AwaitReturn)
                            || matches!(attempt.stage, Stage::Stopping)
                                && attempt.facts.offline.is_some() =>
                    {
                        // Stop --wait may observe M7 before the next controller
                        // checkpoint. Its canonical proof already admits return.
                        let (offline, _, _) = self
                            .checked_restart_offline(snapshot, attempt.request.operation)
                            .await?;
                        if attempt.facts.offline.as_ref() != Some(&offline.request) {
                            return Err(FleetCatalogError::Conflict);
                        }
                    }
                    FleetRestartOperatorAction::Resume { attestation, .. }
                        if matches!(attempt.stage, Stage::AwaitReturned | Stage::VerifyResume) =>
                    {
                        if self
                            .inner
                            .lookup(
                                snapshot.head.tables[FleetTable::Operations.slot()],
                                &format!(
                                    "restart-resume/{}",
                                    hex::encode(attempt.request.operation)
                                ),
                            )
                            .await?
                            .is_some()
                        {
                            return Err(FleetCatalogError::PayloadConflict);
                        }
                        let cut = self
                            .checked_returned_cut(snapshot, attempt.request.operation)
                            .await?;
                        if attestation.boot != cut.quiescence.boot {
                            return Err(FleetCatalogError::Conflict);
                        }
                    }
                    _ => return Err(FleetCatalogError::Conflict),
                }
                let grant = Grant {
                    format: 1,
                    request: request.identity.clone(),
                    intent: hash(&record.bytes()?),
                    input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                    action: request.action.clone(),
                };
                grant.validate(&record)?;
                let committed = self
                    .commit_with_disruption(
                        snapshot,
                        &grant.mutation()?,
                        None,
                        Authority::RestartOperator,
                        || Ok(()),
                    )
                    .await?;
                Ok(FleetRestartOperatorActionAdmission {
                    record: grant,
                    applied: committed.applied_revision,
                    observed: committed.snapshot.revision(),
                })
            })
            .await
        })
    }

    pub(in super::super) async fn operator_action(
        &self,
        root: &FleetSnapshot,
        record: &Record,
        name: &str,
    ) -> Result<Option<Grant>> {
        let Some(bytes) = self
            .inner
            .lookup(
                root.head.tables[FleetTable::Operations.slot()],
                &Grant::key(record.request.operation, name),
            )
            .await?
        else {
            return Ok(None);
        };
        let grant: Grant = decode(KIND, &bytes, 4096)?;
        grant.validate(record)?;
        if grant.action.name() != name
            || self
                .operator_action_candidate(grant.request.id, record)
                .await?
                .as_ref()
                != Some(&grant)
            || self
                .operator_journal(root, &grant.mutation()?)
                .await?
                .is_none()
        {
            return Err(FleetCatalogError::Corrupt("operator action projection"));
        }
        Ok(Some(grant))
    }

    async fn operator_action_candidate(
        &self,
        id: [u8; 16],
        record: &Record,
    ) -> Result<Option<Grant>> {
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
                c.table == FleetTable::Operations
                    && c.key.starts_with(&format!(
                        "restart-controller-action/{}/",
                        hex::encode(record.request.operation)
                    ))
            })
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let grant: Grant = decode(KIND, bytes, 4096)?;
        grant.validate(record)?;
        if grant.request.id != id || grant.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(grant))
    }
}
