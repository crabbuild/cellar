// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Separate human consent, receiver-owned stop proof and joined reconciliation.
mod offline;
mod records;
mod refresh;
use super::super::deadline::Deadline;
use super::super::resources::Authority;
use super::gate::Gate;
use super::*;
use crate::raft::fleet_stop::{
    FleetStopAction, FleetStopRequest, StopSelector, VerifiedFleetClosure,
};
pub use offline::{FleetRestartOfflineAdmission, FleetRestartOfflineRequest};
pub(in crate::raft::fleet_catalog::maintenance) use records::Consent;
pub use refresh::FleetRestartStopRefreshRequest;

#[derive(Clone, Debug)]
pub struct FleetRestartStopRequest {
    pub identity: FleetRequestIdentity,
    pub ready: FleetRequestIdentity,
    pub node: NodeId,
    pub expected_process: ProcessGeneration,
    pub stop: ProposalId,
}

/// Rooted human intent only. The node's receiver must renew runtime proof and
/// sync its exact local guard before this request can trigger owned shutdown.
pub struct FleetRestartStopAdmission {
    record: Consent,
    gate: Gate,
    applied: u64,
    observed: u64,
}

impl FleetRestartStopAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }

    pub fn stop_request(&self) -> Result<FleetStopRequest> {
        FleetStopRequest::new(self.record.selector(&self.gate)?, FleetStopAction::Stop)
            .map_err(|_| FleetCatalogError::Corrupt("restart stop request"))
    }
}

impl FleetCatalog {
    pub async fn admit_restart_stop(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartStopRequest,
        timeout: Duration,
    ) -> Result<FleetRestartStopAdmission> {
        self.check_snapshot(snapshot)?;
        validate_request(&request.identity)?;
        validate_request(&request.ready)?;
        if request.ready.operation != request.identity.operation
            || request.ready.expected_revision >= request.identity.expected_revision
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let deadline = Deadline::new(self, timeout)?;
        self.run(deadline.remaining()?, async {
            let current = self.inner.current().await?;
            let prior = self
                .restart_stop_candidate(request.identity.id, request.identity.operation)
                .await?;
            if let Some((record, gate)) = &prior {
                if !record.matches(request, gate) {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                if let Some(applied) = self.restart_stop_applied(&current, record).await? {
                    self.restart_stop_ready(record, gate).await?;
                    return Ok(FleetRestartStopAdmission {
                        record: record.clone(),
                        gate: gate.clone(),
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
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &Consent::key(request.identity.operation),
                )
                .await?
                .is_some()
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let gate = self
                .checked_restart_gate(snapshot, request.identity.operation)
                .await?;
            let record = Consent {
                format: 1,
                request: request.identity.clone(),
                ready: request.ready.clone(),
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                gate_parent: gate.request.id,
                gate_digest: gate.digest()?,
                stop: request.stop.bytes(),
                anchor: request.identity.id,
                previous: None,
            };
            record.validate(&gate)?;
            if !record.matches(request, &gate)
                || prior.as_ref().is_some_and(|(old, _)| old != &record)
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            self.restart_stop_ready(&record, &gate)
                .await?
                .check_current(self)
                .await?;
            let mutation = record.mutation()?;
            mutation.validate()?;
            let committed = self
                .commit_with_disruption(snapshot, &mutation, None, Authority::RestartStop, || {
                    deadline.remaining()?;
                    Ok(())
                })
                .await?;
            Ok(FleetRestartStopAdmission {
                record,
                gate,
                applied: committed.applied_revision,
                observed: committed.snapshot.revision(),
            })
        })
        .await
    }

    /// Own actual authenticated stop submission. Unknown responses retain the
    /// same request/child; only `restart_stop_closure` proves physical join.
    pub async fn stop_restart_maintenance(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartStopRequest,
        observer: &crate::application::consensus::RaftCells,
        timeout: Duration,
    ) -> Result<FleetRestartStopAdmission> {
        let deadline = Deadline::new(self, timeout)?;
        let admission = self
            .admit_restart_stop(snapshot, request, deadline.remaining()?)
            .await?;
        let accepted = observer
            .request_peer_fleet_stop(
                admission.stop_request()?,
                admission.observed_revision(),
                deadline.remaining()?,
            )
            .await?;
        if accepted.action() != FleetStopAction::Stop
            || accepted.operation() != request.identity.operation
            || accepted.request() != request.stop.bytes()
            || accepted.process() != request.expected_process.bytes()
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(admission)
    }

    /// Monotonic signed closure only, never readiness, slot release or resume.
    /// Missing/unknown physical outcome returns Unconfirmed; retry the exact
    /// request without inferring exit from transport failure or an absent port.
    pub async fn restart_stop_closure(
        &self,
        request: &FleetRestartStopRequest,
        timeout: Duration,
    ) -> Result<VerifiedFleetClosure> {
        validate_request(&request.identity)?;
        self.run(timeout, async {
            let current = self.inner.current().await?;
            self.restart_stop_closure_inner(&current, request).await
        })
        .await
    }

    pub(in crate::raft::fleet_catalog::maintenance) async fn restart_stop_closure_inner(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartStopRequest,
    ) -> Result<VerifiedFleetClosure> {
        let (record, gate) = self
            .restart_stop_candidate(request.identity.id, request.identity.operation)
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        if !record.matches(request, &gate) {
            return Err(FleetCatalogError::PayloadConflict);
        }
        if self
            .restart_stop_applied(snapshot, &record)
            .await?
            .is_none()
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        self.restart_stop_ready(&record, &gate).await?;
        let selector = record.selector(&gate)?;
        self.closure_sink(&selector)?.verify(&selector).await
    }

    pub(in crate::raft::fleet_catalog::maintenance) async fn checked_restart_stop(
        &self,
        snapshot: &FleetSnapshot,
        selector: &StopSelector,
    ) -> Result<(Consent, Gate, FleetRestartReadyAdmission)> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Consent::key(selector.operation),
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let record: Consent = decode(records::KIND, &bytes, records::LIMIT)?;
        let (canonical, gate) = self
            .restart_stop_candidate(record.request.id, selector.operation)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing restart stop request"))?;
        if canonical != record
            || self
                .restart_stop_applied(snapshot, &record)
                .await?
                .is_none()
            || record.selector(&gate)? != *selector
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let current_gate = self
            .checked_restart_gate(snapshot, selector.operation)
            .await?;
        if current_gate != gate {
            return Err(FleetCatalogError::Conflict);
        }
        let ready = self.restart_stop_ready(&record, &gate).await?;
        ready.check_current(self).await?;
        Ok((record, gate, ready))
    }

    pub(in crate::raft::fleet_catalog) async fn verify_restart_stop_request(
        &self,
        snapshot: &FleetSnapshot,
        selector: &StopSelector,
    ) -> Result<()> {
        self.checked_restart_stop(snapshot, selector)
            .await
            .map(|_| ())
    }

    pub(in crate::raft::fleet_catalog::maintenance) async fn restart_stop_ready(
        &self,
        record: &Consent,
        gate: &Gate,
    ) -> Result<FleetRestartReadyAdmission> {
        self.restart_readiness_inner(&record.ready_request(gate)?)
            .await?
            .ok_or(FleetCatalogError::Corrupt(
                "unconfirmed restart readiness parent",
            ))
    }

    async fn restart_stop_applied(
        &self,
        snapshot: &FleetSnapshot,
        record: &Consent,
    ) -> Result<Option<u64>> {
        let applied = self.restart_stop_journal_applied(snapshot, record).await?;
        if applied.is_some() && record.anchor != record.request.id {
            let (anchor, _) = self
                .restart_stop_candidate_raw(record.anchor, record.request.operation)
                .await?
                .ok_or(FleetCatalogError::Corrupt("missing stop anchor"))?;
            if self
                .restart_stop_journal_applied(snapshot, &anchor)
                .await?
                .is_none()
            {
                return Err(FleetCatalogError::Corrupt("unadmitted stop anchor"));
            }
        }
        Ok(applied)
    }

    async fn restart_stop_journal_applied(
        &self,
        snapshot: &FleetSnapshot,
        record: &Consent,
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
            return Err(FleetCatalogError::Corrupt("restart stop applied revision"));
        }
        Ok(applied)
    }

    fn restart_stop_candidate(
        &self,
        id: [u8; 16],
        operation: [u8; 16],
    ) -> futures_util::future::BoxFuture<'_, Result<Option<(Consent, Gate)>>> {
        Box::pin(async move {
            let Some((record, gate)) = self.restart_stop_candidate_raw(id, operation).await? else {
                return Ok(None);
            };
            if record.anchor != record.request.id {
                let (anchor, anchor_gate) = self
                    .restart_stop_candidate_raw(record.anchor, operation)
                    .await?
                    .ok_or(FleetCatalogError::Corrupt("missing original stop consent"))?;
                if anchor.anchor != anchor.request.id
                    || anchor.previous.is_some()
                    || anchor_gate != gate
                    || anchor.stop != record.stop
                    || anchor.request.expected_revision >= record.request.expected_revision
                {
                    return Err(FleetCatalogError::Corrupt("stop consent anchor"));
                }
            }
            Ok(Some((record, gate)))
        })
    }

    async fn restart_stop_candidate_raw(
        &self,
        id: [u8; 16],
        operation: [u8; 16],
    ) -> Result<Option<(Consent, Gate)>> {
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
                change.table == FleetTable::Operations && change.key == Consent::key(operation)
            })
            .and_then(|change| change.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Consent = decode(records::KIND, bytes, records::LIMIT)?;
        let gate = self
            .restart_gate_candidate_for_id(record.gate_parent, operation)
            .await?
            .ok_or(FleetCatalogError::Corrupt("restart stop gate"))?;
        record.validate(&gate)?;
        if record.request.id != id
            || record.request.operation != operation
            || record.mutation()? != mutation
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some((record, gate)))
    }

    pub(in crate::raft::fleet_catalog) fn check_restart_stop_mutation(
        &self,
        mutation: &FleetMutation,
        authority: Authority,
    ) -> Result<()> {
        if authority != Authority::RestartStop
            && mutation.changes.iter().any(|change| {
                change.table == FleetTable::Operations
                    && (change.key.starts_with(records::PREFIX)
                        || change.key.starts_with("restart-offline/"))
            })
        {
            return Err(FleetCatalogError::Invalid("restart stop authority"));
        }
        Ok(())
    }
}
