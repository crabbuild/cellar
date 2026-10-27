// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Joined physical closure is monotonic; resume still needs independent proof.
use super::*;
use serde::Deserialize;

const KIND: u8 = 87;
const PREFIX: &str = "restart-offline/";

#[derive(Clone, Debug)]
pub struct FleetRestartOfflineRequest {
    pub identity: FleetRequestIdentity,
    pub stop: FleetRestartStopRequest,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft::fleet_catalog::maintenance) struct Offline {
    format: u8,
    pub request: FleetRequestIdentity,
    input_root: [u8; 32],
    pub stop_parent: [u8; 16],
    stop_digest: [u8; 32],
    pub closure: [u8; 32],
}

/// Journal-confirmed exact joined shutdown, retaining the disruption slot and
/// all resource charges. This is not authorization to boot, activate or resume.
pub struct FleetRestartOfflineAdmission {
    record: Offline,
    applied: u64,
    observed: u64,
}

impl FleetRestartOfflineAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }

    pub fn closure_digest(&self) -> [u8; 32] {
        self.record.closure
    }
}

impl Offline {
    pub fn key(operation: [u8; 16]) -> String {
        format!("{PREFIX}{}", hex::encode(operation))
    }

    fn validate(&self, stop: &Consent) -> Result<()> {
        validate_request(&self.request)?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.closure == [0; 32]
            || self.request.operation != stop.request.operation
            || self.stop_parent != stop.request.id
            || self.stop_digest != hash(&encode(records::KIND, stop, records::LIMIT)?)
            || self.request.expected_revision <= stop.request.expected_revision
        {
            return Err(FleetCatalogError::Corrupt("restart offline record"));
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
                key: Self::key(self.request.operation),
                value: Some(encode(KIND, self, records::LIMIT)?),
            }],
        })
    }
}

impl FleetCatalog {
    /// Confirm only the exact signed Node/domain/engine join. Losing the stop
    /// response or observing a closed endpoint cannot satisfy this boundary.
    pub async fn confirm_restart_offline(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartOfflineRequest,
        timeout: Duration,
    ) -> Result<FleetRestartOfflineAdmission> {
        self.check_snapshot(snapshot)?;
        validate_request(&request.identity)?;
        validate_request(&request.stop.identity)?;
        if request.identity.operation != request.stop.identity.operation
            || request.identity.expected_revision <= request.stop.identity.expected_revision
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let deadline = Deadline::new(self, timeout)?;
        self.run(deadline.remaining()?, async {
            let current = self.inner.current().await?;
            let prior = self.restart_offline_candidate(request.identity.id, request.identity.operation).await?;
            if let Some((record, stop, gate)) = &prior {
                if record.request != request.identity || !stop.matches(&request.stop, gate) { return Err(FleetCatalogError::PayloadConflict); }
                if let Some(applied) = self.restart_offline_applied(&current, record).await? {
                    return Ok(FleetRestartOfflineAdmission { record: record.clone(), applied, observed: current.revision() });
                }
            }
            if current.token != snapshot.token || request.identity.expected_revision != snapshot.revision() {
                return Err(FleetCatalogError::Conflict);
            }
            let (stop, gate) = self.restart_stop_candidate(request.stop.identity.id, request.identity.operation).await?
                .ok_or(FleetCatalogError::Conflict)?;
            if !stop.matches(&request.stop, &gate) { return Err(FleetCatalogError::PayloadConflict); }
            if self.restart_stop_applied(snapshot, &stop).await?.is_none() { return Err(FleetCatalogError::Conflict); }
            self.restart_stop_ready(&stop, &gate).await?;
            let active = self.active_disruption(snapshot).await?.ok_or(FleetCatalogError::Conflict)?;
            if !matches!(active, super::super::super::disruption::Disruption::Restart(ref preparation) if *preparation == gate.preparation)
                || snapshot.head.disruption != Some(gate.disruption) { return Err(FleetCatalogError::Conflict); }
            let selector = stop.selector(&gate)?;
            let closure = self.closure_sink(&selector)?.verify(&selector).await?;
            let record = Offline { format: 1, request: request.identity.clone(),
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?), stop_parent: stop.request.id,
                stop_digest: hash(&encode(records::KIND, &stop, records::LIMIT)?), closure: closure.digest() };
            record.validate(&stop)?;
            if prior.as_ref().is_some_and(|(old,_,_)| old != &record) { return Err(FleetCatalogError::PayloadConflict); }
            if self.inner.lookup(snapshot.head.tables[FleetTable::Operations.slot()], &Offline::key(request.identity.operation)).await?.is_some() {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let mutation = record.mutation()?;mutation.validate()?;
            let committed = self.commit_with_disruption(snapshot, &mutation, None, Authority::RestartStop, || {
                deadline.remaining()?;Ok(())
            }).await?;
            Ok(FleetRestartOfflineAdmission { record, applied: committed.applied_revision, observed: committed.snapshot.revision() })
        }).await
    }

    pub async fn restart_offline(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<FleetRestartOfflineAdmission> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let (record, _, _) = self
                .checked_restart_offline(snapshot, operation.bytes())
                .await?;
            let applied = self
                .restart_offline_applied(snapshot, &record)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            Ok(FleetRestartOfflineAdmission {
                record,
                applied,
                observed: snapshot.revision(),
            })
        })
        .await
    }

    pub(in crate::raft::fleet_catalog::maintenance) async fn checked_restart_offline(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<(Offline, Consent, Gate)> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Offline::key(operation),
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let record: Offline = decode(KIND, &bytes, records::LIMIT)?;
        let (canonical, stop, gate) = self
            .restart_offline_candidate(record.request.id, operation)
            .await?
            .ok_or(FleetCatalogError::Corrupt("restart offline request"))?;
        if canonical != record
            || self
                .restart_offline_applied(snapshot, &record)
                .await?
                .is_none()
            || self.restart_stop_applied(snapshot, &stop).await?.is_none()
        {
            return Err(FleetCatalogError::Corrupt("restart offline admission"));
        }
        let selector = stop.selector(&gate)?;
        if self
            .closure_sink(&selector)?
            .verify(&selector)
            .await?
            .digest()
            != record.closure
        {
            return Err(FleetCatalogError::Corrupt("restart offline closure"));
        }
        Ok((record, stop, gate))
    }

    async fn restart_offline_applied(
        &self,
        snapshot: &FleetSnapshot,
        record: &Offline,
    ) -> Result<Option<u64>> {
        let mutation = record.mutation()?;
        let applied = self
            .inner
            .committed(
                &snapshot.head,
                &hex::encode(record.request.id),
                hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
            )
            .await?;
        if applied.is_some_and(|revision| {
            Some(revision) != record.request.expected_revision.checked_add(1)
        }) {
            return Err(FleetCatalogError::Corrupt("restart offline revision"));
        }
        Ok(applied)
    }

    async fn restart_offline_candidate(
        &self,
        id: [u8; 16],
        operation: [u8; 16],
    ) -> Result<Option<(Offline, Consent, Gate)>> {
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
            .find(|change| {
                change.table == FleetTable::Operations && change.key == Offline::key(operation)
            })
            .and_then(|change| change.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Offline = decode(KIND, bytes, records::LIMIT)?;
        let (stop, gate) = self
            .restart_stop_candidate(record.stop_parent, operation)
            .await?
            .ok_or(FleetCatalogError::Corrupt("restart offline stop parent"))?;
        record.validate(&stop)?;
        if record.request.id != id
            || record.request.operation != operation
            || record.mutation()? != mutation
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some((record, stop, gate)))
    }
}
