// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Renew consent without forking an accepted physical stop or its joined proof.
use super::*;
use futures_util::future::BoxFuture;

/// Separate human consent for a fresh Ready assessment in the exact previous
/// node/process/gate/child scope. Unknown outcomes retry this entire payload.
#[derive(Clone, Debug)]
pub struct FleetRestartStopRefreshRequest {
    pub previous: FleetRestartStopRequest,
    pub request: FleetRestartStopRequest,
}

impl FleetCatalog {
    pub fn refresh_restart_stop<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        request: &'a FleetRestartStopRefreshRequest,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<FleetRestartStopAdmission>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            let next = &request.request;
            validate_request(&next.identity)?;
            validate_request(&next.ready)?;
            validate_request(&request.previous.identity)?;
            if next.identity.operation != request.previous.identity.operation
                || next.identity.operation != next.ready.operation
                || next.identity.id == request.previous.identity.id
                || next.identity.expected_revision <= next.ready.expected_revision
                || next.ready.expected_revision <= request.previous.identity.expected_revision
                || next.node != request.previous.node
                || next.expected_process != request.previous.expected_process
                || next.stop != request.previous.stop
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let deadline = Deadline::new(self, timeout)?;
            self.run(deadline.remaining()?, async {
                let current = self.inner.current().await?;
                let (old, old_gate) = self
                    .restart_stop_candidate(request.previous.identity.id, next.identity.operation)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if !old.matches(&request.previous, &old_gate) {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let prior = self
                    .restart_stop_candidate(next.identity.id, next.identity.operation)
                    .await?;
                if let Some((record, gate)) = &prior {
                    if !record.matches(next, gate)
                        || record.previous != Some(request.previous.identity.id)
                        || record.anchor != old.anchor
                        || *gate != old_gate
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    // Exact confirmation precedes every current gate/proof check.
                    if let Some(applied) = self.restart_stop_applied(&current, record).await? {
                        return Ok(FleetRestartStopAdmission {
                            record: record.clone(),
                            gate: gate.clone(),
                            applied,
                            observed: current.revision(),
                        });
                    }
                }
                if current.token != snapshot.token
                    || next.identity.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let gate = old_gate;
                if self.restart_stop_applied(snapshot, &old).await?.is_none() {
                    return Err(FleetCatalogError::Conflict);
                }
                let projected = self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Operations.slot()],
                        &Consent::key(next.identity.operation),
                    )
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if projected != encode(records::KIND, &old, records::LIMIT)? {
                    return Err(FleetCatalogError::Conflict);
                }
                if self
                    .checked_restart_gate(snapshot, next.identity.operation)
                    .await?
                    != gate
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let record = Consent {
                    format: 1,
                    request: next.identity.clone(),
                    ready: next.ready.clone(),
                    input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                    gate_parent: gate.request.id,
                    gate_digest: gate.digest()?,
                    stop: old.stop,
                    anchor: old.anchor,
                    previous: Some(old.request.id),
                };
                record.validate(&gate)?;
                if prior
                    .as_ref()
                    .is_some_and(|(r, g)| *r != record || *g != gate)
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                self.restart_stop_ready(&record, &gate)
                    .await?
                    .check_current(self)
                    .await?;
                let committed = self
                    .commit_with_disruption(
                        snapshot,
                        &record.mutation()?,
                        None,
                        Authority::RestartStop,
                        || {
                            deadline.remaining()?;
                            Ok(())
                        },
                    )
                    .await?;
                Ok(FleetRestartStopAdmission {
                    record,
                    gate,
                    applied: committed.applied_revision,
                    observed: committed.snapshot.revision(),
                })
            })
            .await
        })
    }

    /// Historical intent only. This read performs no mutation, restores no live
    /// evidence and does not submit physical stop. The receiver renews proof.
    pub fn restart_stop_consent<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        request: &'a FleetRestartStopRequest,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<FleetRestartStopAdmission>>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            validate_request(&request.identity)?;
            self.run(timeout, async {
                let Some((record, gate)) = self
                    .restart_stop_candidate(request.identity.id, request.identity.operation)
                    .await?
                else {
                    return Ok(None);
                };
                if !record.matches(request, &gate) {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let Some(applied) = self.restart_stop_applied(snapshot, &record).await? else {
                    return Ok(None);
                };
                self.restart_stop_ready(&record, &gate).await?;
                Ok(Some(FleetRestartStopAdmission {
                    record,
                    gate,
                    applied,
                    observed: snapshot.revision(),
                }))
            })
            .await
        })
    }
}
