// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A namespace decision can outlive its response. Its root claim remains until
//! the actual winner and root confirmation settle atomically.
use super::super::resources::Authority;
use super::gate::Gate;
use super::*;
use serde::Deserialize;

const KIND: u8 = 80;
const POINTER_KIND: u8 = 81;
const LIMIT: usize = 4096;
const PREFIX: &str = "restart-birth/";
const ACTIVE: &str = "restart-birth-active/";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Outcome {
    pub request: FleetRequestIdentity,
    pub state: FleetHistoryState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Action {
    pub format: u8,
    pub request: FleetRequestIdentity,
    pub input_root: [u8; 32],
    pub gate_parent: [u8; 16],
    pub gate_digest: [u8; 32],
    pub group: String,
    pub history_digest: [u8; 32],
    pub resolution: FleetRestartBirthResolution,
    #[serde(deserialize_with = "required_outcome")]
    pub outcome: Option<Outcome>,
}

fn required_outcome<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<Outcome>, D::Error> {
    Option::<Outcome>::deserialize(decoder)
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pointer {
    id: [u8; 16],
    digest: [u8; 32],
}

impl Action {
    fn key(id: [u8; 16]) -> String {
        format!("{PREFIX}{}", hex::encode(id))
    }

    fn active(operation: [u8; 16]) -> String {
        format!("{ACTIVE}{}", hex::encode(operation))
    }

    fn bytes(&self) -> Result<Vec<u8>> {
        encode(KIND, self, LIMIT)
    }

    pub fn check_history(&self, history: &FleetHistory) -> Result<()> {
        let mut original = history.clone();
        original.state = FleetHistoryState::Reserved;
        if original.binding().key().map_err(|_| corrupt())? != self.group
            || hash(&original.encode()?) != self.history_digest
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    fn validate(&self, gate: &Gate) -> Result<()> {
        validate_request(&self.request)?;
        super::super::records::validate_key(&self.group)?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.history_digest == [0; 32]
            || self.gate_parent != gate.request.id
            || self.gate_digest != gate.digest()?
            || self.request.operation != gate.request.operation
            || self.request.expected_revision <= gate.request.expected_revision
        {
            return Err(corrupt());
        }
        if let Some(outcome) = &self.outcome {
            validate_request(&outcome.request)?;
            if outcome.request.operation != self.request.operation
                || outcome.request.id == self.request.id
                || outcome.request.expected_revision <= self.request.expected_revision
                || matches!(outcome.state, FleetHistoryState::Reserved)
            {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    fn pending(&self) -> Self {
        let mut copy = self.clone();
        copy.outcome = None;
        copy
    }

    fn pointer(&self) -> Result<Pointer> {
        Ok(Pointer {
            id: self.request.id,
            digest: hash(&self.pending().bytes()?),
        })
    }

    pub fn admission(&self) -> Result<FleetMutation> {
        let mutation = FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![
                FleetChange {
                    table: FleetTable::Operations,
                    key: Self::key(self.request.id),
                    value: Some(self.pending().bytes()?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: Self::active(self.request.operation),
                    value: Some(encode(POINTER_KIND, &self.pointer()?, LIMIT)?),
                },
            ],
        };
        mutation.validate()?;
        Ok(mutation)
    }

    pub fn completion(&self, history: &FleetHistory) -> Result<FleetMutation> {
        self.check_history(history)?;
        let outcome = self.outcome.as_ref().ok_or_else(corrupt)?;
        if history.state() != &outcome.state {
            return Err(corrupt());
        }
        let mutation = FleetMutation {
            id: outcome.request.id,
            operation: outcome.request.operation,
            principal: outcome.request.principal.clone(),
            expected_revision: outcome.request.expected_revision,
            changes: vec![
                FleetChange {
                    table: FleetTable::Histories,
                    key: self.group.clone(),
                    value: Some(history.encode()?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: Self::key(self.request.id),
                    value: Some(self.bytes()?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: Self::active(self.request.operation),
                    value: None,
                },
            ],
        };
        mutation.validate()?;
        Ok(mutation)
    }
}

impl FleetCatalog {
    /// Read an exact historical birth-settlement receipt after a lost response.
    /// It cannot release a later action, disruption slot or readiness grant.
    pub async fn restart_birth_settlement(
        &self,
        snapshot: &FleetSnapshot,
        admission: ProposalId,
        timeout: Duration,
    ) -> Result<FleetRestartSettlementProgress> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let action = self
                .restart_birth_action(snapshot, admission.bytes())
                .await?;
            if action.outcome.is_none() {
                return Err(FleetCatalogError::SettlementInProgress {
                    admission: admission.bytes(),
                });
            }
            self.restart_birth_progress(snapshot, &action).await
        })
        .await
    }

    pub(in crate::raft::fleet_catalog) fn check_restart_settlement_mutation(
        &self,
        mutation: &FleetMutation,
        authority: Authority,
    ) -> Result<()> {
        if mutation.changes.iter().any(|c| {
            c.table == FleetTable::Operations
                && (c.key.starts_with(PREFIX) || c.key.starts_with(ACTIVE))
        }) && authority != Authority::RestartSettlement
        {
            return Err(FleetCatalogError::Invalid("restart settlement authority"));
        }
        Ok(())
    }

    pub(super) async fn restart_birth_action(
        &self,
        snapshot: &FleetSnapshot,
        id: [u8; 16],
    ) -> Result<Action> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Action::key(id),
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let action: Action = decode(KIND, &bytes, LIMIT)?;
        let gate = self
            .restart_gate_candidate_for_id(action.gate_parent, action.request.operation)
            .await?
            .ok_or_else(corrupt)?;
        gate.validate(self)?;
        action.validate(&gate)?;
        let gate_mutation = gate.mutation()?;
        if action.request.id != id
            || self
                .inner
                .committed(
                    &snapshot.head,
                    &hex::encode(gate_mutation.id),
                    hash(&encode(REQUEST_KIND, &gate_mutation, REQUEST_LIMIT)?),
                )
                .await?
                .is_none()
        {
            return Err(corrupt());
        }
        let expected = action.admission()?;
        let original = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
            .ok_or_else(corrupt)?
            .0;
        if original != expected {
            return Err(corrupt());
        }
        let applied = self
            .inner
            .committed(
                &snapshot.head,
                &hex::encode(id),
                hash(&encode(REQUEST_KIND, &expected, REQUEST_LIMIT)?),
            )
            .await?;
        if applied.is_none() || applied != action.request.expected_revision.checked_add(1) {
            return Err(corrupt());
        }
        if let Some(outcome) = &action.outcome {
            let mutation = self
                .inner
                .read::<FleetMutation>(
                    &format!("requests/{}", hex::encode(outcome.request.id)),
                    REQUEST_KIND,
                    REQUEST_LIMIT,
                )
                .await?
                .ok_or_else(corrupt)?
                .0;
            let bytes = mutation
                .changes
                .iter()
                .find(|c| c.table == FleetTable::Histories && c.key == action.group)
                .and_then(|c| c.value.as_deref())
                .ok_or_else(corrupt)?;
            if mutation != action.completion(&FleetHistory::decode(bytes)?)? {
                return Err(corrupt());
            }
            let applied = self
                .inner
                .committed(
                    &snapshot.head,
                    &hex::encode(mutation.id),
                    hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
                )
                .await?;
            if applied.is_none() || applied != outcome.request.expected_revision.checked_add(1) {
                return Err(corrupt());
            }
        }
        Ok(action)
    }

    pub(super) async fn active_restart_birth(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<Option<Action>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Action::active(operation),
            )
            .await?
        else {
            return Ok(None);
        };
        let pointer: Pointer = decode(POINTER_KIND, &bytes, LIMIT)?;
        let action = self.restart_birth_action(snapshot, pointer.id).await?;
        if action.request.operation != operation
            || action.outcome.is_some()
            || pointer != action.pointer()?
        {
            return Err(corrupt());
        }
        Ok(Some(action))
    }

    pub(super) async fn require_restart_settlement_idle(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<()> {
        if let Some(action) = self.active_restart_birth(snapshot, operation).await? {
            return Err(FleetCatalogError::SettlementInProgress {
                admission: action.request.id,
            });
        }
        Ok(())
    }

    pub(super) async fn admit_restart_birth(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        scope: &cellar_logic::consensus::GroupScope,
        resolution: FleetRestartBirthResolution,
        timeout: Duration,
    ) -> Result<(Action, FleetSnapshot)> {
        self.run(timeout, async {
            let gate = self
                .checked_restart_gate(snapshot, operation.bytes())
                .await?;
            self.require_restart_settlement_idle(snapshot, operation.bytes())
                .await?;
            let group = self.binding(scope)?.key().map_err(|_| corrupt())?;
            let history = self.history_inner(snapshot, &group).await?;
            if *history.state() != FleetHistoryState::Reserved
                || !history
                    .voters
                    .iter()
                    .chain(&history.learners)
                    .any(|m| m.node == gate.node.node)
            {
                return Err(FleetCatalogError::Conflict);
            }
            let action = Action {
                format: 1,
                request: super::settle::identity(snapshot, operation.bytes())?,
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                gate_parent: gate.request.id,
                gate_digest: gate.digest()?,
                group,
                history_digest: hash(&history.encode()?),
                resolution,
                outcome: None,
            };
            action.validate(&gate)?;
            let committed = self
                .commit_with_disruption(
                    snapshot,
                    &action.admission()?,
                    None,
                    Authority::RestartSettlement,
                    || Ok(()),
                )
                .await?;
            Ok((action, committed.snapshot))
        })
        .await
    }

    pub(super) async fn complete_restart_birth(
        &self,
        snapshot: &FleetSnapshot,
        parent: &Action,
        namespace: &NamespaceCatalog,
        winner: crate::raft::catalog::BirthDisposition,
        timeout: Duration,
    ) -> Result<FleetRestartSettlementProgress> {
        self.run(timeout, async {
            let mut action = self
                .restart_birth_action(snapshot, parent.request.id)
                .await?;
            if action.pending() != parent.pending() {
                return Err(FleetCatalogError::PayloadConflict);
            }
            if action.outcome.is_some() {
                return self.restart_birth_progress(snapshot, &action).await;
            }
            self.checked_restart_gate(snapshot, action.request.operation)
                .await?;
            if self
                .active_restart_birth(snapshot, action.request.operation)
                .await?
                .as_ref()
                != Some(&action)
            {
                return Err(FleetCatalogError::Conflict);
            }
            let mut history = self.history_inner(snapshot, &action.group).await?;
            action.check_history(&history)?;
            let scope = history.binding().scope().map_err(|_| corrupt())?;
            history.state = match winner {
                crate::raft::catalog::BirthDisposition::Live(birth)
                    if history.check_birth(namespace, &birth) =>
                {
                    FleetHistoryState::Live
                }
                crate::raft::catalog::BirthDisposition::Cancelled {
                    scope: actual,
                    operation,
                } if actual == scope => FleetHistoryState::Cancelled {
                    operation: operation.bytes(),
                },
                _ => return Err(FleetCatalogError::Conflict),
            };
            action.outcome = Some(Outcome {
                request: super::settle::identity(snapshot, action.request.operation)?,
                state: history.state.clone(),
            });
            let mutation = action.completion(&history)?;
            let commit = self
                .commit_with_disruption(
                    snapshot,
                    &mutation,
                    None,
                    Authority::RestartSettlement,
                    || Ok(()),
                )
                .await?;
            self.restart_birth_progress(&commit.snapshot, &action).await
        })
        .await
    }

    async fn restart_birth_progress(
        &self,
        snapshot: &FleetSnapshot,
        action: &Action,
    ) -> Result<FleetRestartSettlementProgress> {
        let outcome = action.outcome.as_ref().ok_or_else(corrupt)?;
        let history = self.history_inner(snapshot, &action.group).await?;
        action.check_history(&history)?;
        Ok(FleetRestartSettlementProgress::Birth {
            admission: action.request.id,
            resolution: action.resolution,
            group: history.binding().scope().map_err(|_| corrupt())?,
            state: outcome.state.clone(),
            applied_revision: outcome
                .request
                .expected_revision
                .checked_add(1)
                .ok_or_else(corrupt)?,
            observed_revision: snapshot.revision(),
        })
    }
}

fn corrupt() -> FleetCatalogError {
    FleetCatalogError::Corrupt("restart birth action")
}
