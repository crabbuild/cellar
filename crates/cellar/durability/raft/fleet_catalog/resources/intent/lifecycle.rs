// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cancel fences new work first; terminal transitions preserve admitted effects.
use super::*;

impl FleetCatalog {
    async fn intent_retry(
        &self,
        request: &FleetRequestIdentity,
        phase: FleetTopologyPhase,
        superseded: Option<[u8; 16]>,
    ) -> Result<Option<FleetTopologyOperation>> {
        let current = self.inner.current().await?;
        let Some((prior, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(request.id)),
                super::super::super::REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
        else {
            return Ok(None);
        };
        prior.validate()?;
        let projected = prior
            .changes
            .iter()
            .find(|c| c.table == FleetTable::Operations && c.key == row_key(request.operation))
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: IntentRecord = decode(ROW_KIND, projected, 4096)?;
        record.validate(self)?;
        if record.transition != *request
            || record.phase != phase
            || prior.id != request.id
            || superseded.is_some_and(|id| record.last_superseded_admission != Some(id))
            || prior.operation != request.operation
            || prior.expected_revision != request.expected_revision
            || prior.principal != request.principal
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        if self.resource_applied(&current, &prior).await?.is_some() {
            let actual = self.intent_record(&current, request.operation).await?;
            return self.intent_observation(&current, &actual).await.map(Some);
        }
        Ok(None)
    }

    /// Root a no-new-movement fence. Existing admitted work must still settle;
    /// cancellation does not roll back committed membership or physical state.
    pub async fn cancel_topology(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        timeout: Duration,
    ) -> Result<FleetTopologyOperation> {
        self.check_snapshot(snapshot)?;
        super::super::super::histories::validate_request(request)?;
        let deadline = super::super::super::deadline::Deadline::new(self, timeout)?;
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                if let Some(prior) = self
                    .intent_retry(request, FleetTopologyPhase::Cancelling, None)
                    .await?
                {
                    return Ok(prior);
                }
                self.require_intent_current(snapshot).await?;
                let mut record = self
                    .active_intent(snapshot)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if record.request.operation != request.operation
                    || record.phase != FleetTopologyPhase::Running
                    || request.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                record.phase = FleetTopologyPhase::Cancelling;
                record.transition = request.clone();
                record.validate(self)?;
                let change = mutation(request, vec![record.change()?]);
                let result = self
                    .commit_intent(snapshot, &change, || deadline.remaining().map(|_| ()))
                    .await?;
                let actual = self
                    .intent_record(&result.snapshot, request.operation)
                    .await?;
                self.intent_observation(&result.snapshot, &actual).await
            }),
        )
        .await
    }

    /// Finish cancellation only after every previously admitted debit is
    /// settled. Restore the prior allowed placement policy; keep partial moves.
    pub async fn complete_topology_cancellation(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        timeout: Duration,
    ) -> Result<FleetTopologyOperation> {
        self.check_snapshot(snapshot)?;
        super::super::super::histories::validate_request(request)?;
        let deadline = super::super::super::deadline::Deadline::new(self, timeout)?;
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                if let Some(prior) = self
                    .intent_retry(request, FleetTopologyPhase::Cancelled, None)
                    .await?
                {
                    return Ok(prior);
                }
                self.require_intent_current(snapshot).await?;
                let mut record = self
                    .active_intent(snapshot)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if record.request.operation != request.operation
                    || record.phase != FleetTopologyPhase::Cancelling
                    || request.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.require_intent_settled(snapshot).await?;
                let previous = match record.previous {
                    Some(digest) => Some(self.read_intent_policy(digest).await?.bytes()?),
                    None => None,
                };
                record.phase = FleetTopologyPhase::Cancelled;
                record.transition = request.clone();
                record.validate(self)?;
                let change = mutation(
                    request,
                    vec![
                        record.change()?,
                        active_change(None),
                        FleetChange {
                            table: FleetTable::Policy,
                            key: POLICY_KEY.into(),
                            value: previous,
                        },
                    ],
                );
                let result = self
                    .commit_intent(snapshot, &change, || deadline.remaining().map(|_| ()))
                    .await?;
                let actual = self
                    .intent_record(&result.snapshot, request.operation)
                    .await?;
                self.intent_observation(&result.snapshot, &actual).await
            }),
        )
        .await
    }

    /// Persist placement convergence from a complete current no-move preview.
    /// The keep-set stays enforced after completion; node closure is separate.
    pub async fn complete_topology(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        preview: &FleetTopologyPreview,
        timeout: Duration,
    ) -> Result<FleetTopologyOperation> {
        self.check_snapshot(snapshot)?;
        super::super::super::histories::validate_request(request)?;
        let deadline = super::super::super::deadline::Deadline::new(self, timeout)?;
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                if let Some(prior) = self
                    .intent_retry(request, FleetTopologyPhase::Complete, None)
                    .await?
                {
                    return Ok(prior);
                }
                self.require_intent_current(snapshot).await?;
                let mut record = self
                    .active_intent(snapshot)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if record.request.operation != request.operation
                    || record.phase != FleetTopologyPhase::Running
                    || request.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.require_intent_settled(snapshot).await?;
                self.require_intent_preview(snapshot, &record, preview)
                    .await?;
                if !preview.plan().moves.is_empty() {
                    return Err(FleetCatalogError::Invalid("topology moves remain"));
                }
                record.phase = FleetTopologyPhase::Complete;
                record.transition = request.clone();
                record.validate(self)?;
                let change = mutation(request, vec![record.change()?, active_change(None)]);
                let result = self
                    .commit_intent(snapshot, &change, || {
                        deadline.remaining()?;
                        self.require_resource_age(preview.oldest)
                    })
                    .await?;
                let actual = self
                    .intent_record(&result.snapshot, request.operation)
                    .await?;
                self.intent_observation(&result.snapshot, &actual).await
            }),
        )
        .await
    }

    /// Resolve an unconfirmed candidate without granting its bytes authority.
    /// This CAS and that candidate's old-root admission cannot both win. Work
    /// already admitted at a newer root must be discovered and settled instead.
    pub(in super::super) async fn supersede_topology_admission(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        candidate: &FleetRequestIdentity,
        timeout: Duration,
    ) -> Result<FleetTopologyOperation> {
        self.check_snapshot(snapshot)?;
        super::super::super::histories::validate_request(request)?;
        super::super::super::histories::validate_request(candidate)?;
        let deadline = super::super::super::deadline::Deadline::new(self, timeout)?;
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                if let Some(prior) = self
                    .intent_retry(request, FleetTopologyPhase::Running, Some(candidate.id))
                    .await?
                {
                    return Ok(prior);
                }
                self.require_intent_current(snapshot).await?;
                let mut record = self
                    .active_intent(snapshot)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if record.phase != FleetTopologyPhase::Running
                    || record.request.operation != request.operation
                    || candidate.operation != request.operation
                    || candidate.expected_revision != snapshot.revision()
                    || request.expected_revision != snapshot.revision()
                    || request.id == candidate.id
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.require_intent_settled(snapshot).await?;
                if !self.topology_candidate(snapshot, candidate).await? {
                    return Err(FleetCatalogError::Conflict);
                }
                record.transition = request.clone();
                record.last_superseded_admission = Some(candidate.id);
                record.validate(self)?;
                let change = mutation(request, vec![record.change()?]);
                let result = self
                    .commit_intent(snapshot, &change, || deadline.remaining().map(|_| ()))
                    .await?;
                let actual = self
                    .intent_record(&result.snapshot, request.operation)
                    .await?;
                self.intent_observation(&result.snapshot, &actual).await
            }),
        )
        .await
    }
}
