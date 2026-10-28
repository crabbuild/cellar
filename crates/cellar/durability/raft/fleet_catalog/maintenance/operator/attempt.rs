// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A rooted child attempt fences a delayed old CAS before a replacement is used.
use super::*;
use rand::RngCore;
use serde::Deserialize;

pub(super) const KIND: u8 = 97;
pub(super) const PREFIX: &str = "restart-controller-attempt/";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Stage {
    Prepare,
    Gate,
    Quiesce { gate: FleetRequestIdentity },
    Settle,
    Verify,
    AwaitStop { ready: FleetRequestIdentity },
    ConsentStop { ready: FleetRequestIdentity },
    Stopping,
    AwaitReturn,
    Returning,
    AwaitReturned,
    VerifyResume,
    Resuming,
    VerifyComplete,
    Finishing,
    Cancel { gated: bool },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Attempt {
    format: u8,
    pub journal: FleetRequestIdentity,
    pub request: FleetRequestIdentity,
    pub intent: [u8; 32],
    #[serde(deserialize_with = "required_parent")]
    predecessor: Option<[u8; 16]>,
    input_root: [u8; 32],
    pub stage: Stage,
    pub facts: lifecycle::Facts,
}

pub(super) fn identity(revision: u64, operation: [u8; 16]) -> Result<FleetRequestIdentity> {
    let mut id = [0; 16];
    crate::asyncrt::rng("fleet-maintenance-driver").fill_bytes(&mut id);
    ProposalId::new(id).map_err(|_| FleetCatalogError::Limit)?;
    Ok(FleetRequestIdentity {
        id,
        operation,
        expected_revision: revision,
        principal: "fleet-maintenance-driver".into(),
    })
}

impl Attempt {
    fn key(operation: [u8; 16]) -> String {
        format!("{PREFIX}{}", hex::encode(operation))
    }

    fn validate(&self, record: &Record) -> Result<()> {
        validate_request(&self.journal)?;
        validate_request(&self.request)?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.intent != hash(&record.bytes()?)
            || self.journal.operation != record.request.operation
            || self.request.operation == record.request.operation
            || self.journal.principal != "fleet-maintenance-driver"
            || self.request.principal != self.journal.principal
            || self.request.id == self.journal.id
            || self.request.id == record.request.id
            || self.journal.expected_revision.checked_add(1) != Some(self.request.expected_revision)
            || self.journal.expected_revision <= record.request.expected_revision
            || self.predecessor.is_none() && self.stage != Stage::Prepare
            || self
                .predecessor
                .is_some_and(|id| id == [0; 16] || id == self.journal.id)
        {
            return Err(FleetCatalogError::Corrupt("maintenance driver attempt"));
        }
        self.facts.validate(self.request.operation)?;
        if let Stage::ConsentStop { ready: parent }
        | Stage::AwaitStop { ready: parent }
        | Stage::Quiesce { gate: parent } = &self.stage
        {
            validate_request(parent)?;
            if parent.operation != self.request.operation
                || parent.expected_revision >= self.journal.expected_revision
            {
                return Err(FleetCatalogError::Corrupt("maintenance driver readiness"));
            }
        }
        let needs_stop = matches!(
            self.stage,
            Stage::Stopping
                | Stage::AwaitReturn
                | Stage::Returning
                | Stage::AwaitReturned
                | Stage::VerifyResume
                | Stage::Resuming
                | Stage::VerifyComplete
                | Stage::Finishing
        );
        let needs_offline = matches!(
            self.stage,
            Stage::AwaitReturn
                | Stage::Returning
                | Stage::AwaitReturned
                | Stage::VerifyResume
                | Stage::Resuming
                | Stage::VerifyComplete
                | Stage::Finishing
        );
        let needs_return = matches!(
            self.stage,
            Stage::AwaitReturned
                | Stage::VerifyResume
                | Stage::Resuming
                | Stage::VerifyComplete
                | Stage::Finishing
        );
        if needs_stop && self.facts.stop.is_none()
            || needs_offline && self.facts.offline.is_none()
            || needs_return && self.facts.returning.is_none()
            || matches!(
                self.stage,
                Stage::Resuming | Stage::VerifyComplete | Stage::Finishing
            ) && self.facts.resume.is_none()
            || matches!(self.stage, Stage::Finishing) && self.facts.completion.is_none()
        {
            return Err(FleetCatalogError::Corrupt("operator lifecycle stage"));
        }
        Ok(())
    }

    fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.journal.id,
            operation: self.journal.operation,
            principal: self.journal.principal.clone(),
            expected_revision: self.journal.expected_revision,
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: Self::key(self.journal.operation),
                value: Some(encode(KIND, self, 4096)?),
            }],
        })
    }
}

impl FleetCatalog {
    pub(super) async fn restart_operator_attempt(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
    ) -> Result<Option<Attempt>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Attempt::key(record.request.operation),
            )
            .await?
        else {
            return Ok(None);
        };
        let attempt: Attempt = decode(KIND, &bytes, 4096)?;
        attempt.validate(record)?;
        let mutation = attempt.mutation()?;
        let stored = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(mutation.id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt(
                "missing maintenance attempt journal",
            ))?
            .0;
        let applied = self
            .inner
            .committed(
                &snapshot.head,
                &hex::encode(mutation.id),
                hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("unadmitted maintenance attempt"))?;
        if stored != mutation || mutation.expected_revision.checked_add(1) != Some(applied) {
            return Err(FleetCatalogError::Corrupt("maintenance attempt journal"));
        }
        Ok(Some(attempt))
    }

    pub(super) async fn claim_restart_operator_attempt(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
        expected: Option<[u8; 16]>,
        stage: Stage,
    ) -> Result<()> {
        self.claim_restart_operator_step(snapshot, record, expected, stage, None)
            .await
    }

    pub(super) async fn claim_restart_operator_step(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
        expected: Option<[u8; 16]>,
        stage: Stage,
        facts: Option<lifecycle::Facts>,
    ) -> Result<()> {
        let active = self
            .active_restart_operator_inner(snapshot)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        if active.record != *record {
            return Err(FleetCatalogError::Conflict);
        }
        let previous = self.restart_operator_attempt(snapshot, record).await?;
        if previous.as_ref().map(|a| a.journal.id) != expected {
            return Err(FleetCatalogError::Conflict);
        }
        let cancellation = self.operator_cancel_command(snapshot, record).await?;
        match (&stage, cancellation) {
            (Stage::Cancel { .. }, Some(command))
                if command.execution == previous.as_ref().map(|a| a.request.operation) => {}
            (Stage::Stopping, Some(_))
                if self
                    .operator_joined_stop(snapshot, record, previous.as_ref())
                    .await? => {}
            (_, Some(command))
                if self
                    .operator_cancel_refused(snapshot, record, &command)
                    .await? => {}
            (Stage::Cancel { .. }, _) | (_, Some(_)) => return Err(FleetCatalogError::Conflict),
            _ => (),
        }
        if let Some(old) = previous
            .as_ref()
            .filter(|a| a.stage == Stage::Verify && stage == Stage::Verify)
        {
            let request = super::driver::ready_request(record, old.request.clone())?;
            if self.restart_readiness_inner(&request).await?.is_some() {
                return Err(FleetCatalogError::Conflict);
            }
        }
        // M1 fixes the entire SDK request under its operation key before its
        // Root CAS. An unadmitted retry therefore needs a new SDK operation,
        // while every step after M1 retains that admitted operation. The
        // coordination CAS fences a delayed older M1 before this one can run.
        let operation = if stage == Stage::Prepare {
            let mut operation = [0; 16];
            crate::asyncrt::rng("fleet-maintenance-driver").fill_bytes(&mut operation);
            ProposalId::new(operation).map_err(|_| FleetCatalogError::Limit)?;
            operation
        } else {
            previous
                .as_ref()
                .ok_or(FleetCatalogError::Conflict)?
                .request
                .operation
        };
        let attempt = Attempt {
            format: 1,
            journal: identity(snapshot.revision(), record.request.operation)?,
            request: identity(
                snapshot
                    .revision()
                    .checked_add(1)
                    .ok_or(FleetCatalogError::Limit)?,
                operation,
            )?,
            intent: hash(&record.bytes()?),
            predecessor: expected,
            input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
            stage,
            facts: facts.unwrap_or_else(|| {
                previous
                    .as_ref()
                    .map_or_else(lifecycle::Facts::default, |a| a.facts.clone())
            }),
        };
        attempt.validate(record)?;
        self.commit_with_disruption(
            snapshot,
            &attempt.mutation()?,
            None,
            Authority::RestartOperator,
            || Ok(()),
        )
        .await?;
        Ok(())
    }
}

fn required_parent<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<[u8; 16]>, D::Error> {
    Option::<[u8; 16]>::deserialize(decoder)
}
