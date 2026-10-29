// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The physical receiver renews current proof; consent carries no health lease.
use super::*;

pub(crate) struct ResumeCheck {
    pub quiescence: StopSelector,
    pub deadline: u64,
}

impl FleetCatalog {
    pub(in crate::raft::fleet_catalog) fn verify_restart_resume_selector<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        selector: &'a StopSelector,
        action: FleetStopAction,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cut = self
                .checked_returned_cut(snapshot, selector.operation)
                .await?;
            match action {
                FleetStopAction::ObserveQuiescence if cut.quiescence == *selector => Ok(()),
                FleetStopAction::Resume | FleetStopAction::ObserveResume => {
                    let intent = self
                        .current_resume_intent(snapshot, selector.operation)
                        .await?;
                    intent.validate(&cut)?;
                    if intent.effect_selector()? != *selector {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    Ok(())
                }
                _ => Err(FleetCatalogError::PayloadConflict),
            }
        })
    }

    pub(crate) fn renew_restart_resume<'a>(
        &'a self,
        selector: &'a StopSelector,
        gateway: &'a RaftCells,
        local: LocalResume,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<ResumeCheck>> {
        Box::pin(async move {
            let deadline = Deadline::new(self, timeout)?;
            let owner_limit = gateway
                .handle
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?
                .checked_add(proof::MAX_SAMPLE_AGE_MS)
                .ok_or(FleetCatalogError::Limit)?;
            let (snapshot, request, cut, basis) = self
                .run(deadline.remaining()?, async {
                    let snapshot = self.inner.current().await?;
                    let cut = self
                        .checked_returned_cut(&snapshot, selector.operation)
                        .await?;
                    let record = self
                        .current_resume_intent(&snapshot, selector.operation)
                        .await?;
                    record.validate(&cut)?;
                    let basis = basis::Basis::capture(&snapshot, &cut);
                    if record.effect_selector()? != *selector || record.inputs != basis.digest()? {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let request = FleetRestartResumeRequest {
                        identity: record.request.identity.clone(),
                        returning: record.request.returning.clone(),
                        attestation: record.request.attestation.clone(),
                        resume: ProposalId::new(record.request.resume)
                            .map_err(|_| FleetCatalogError::Corrupt("resume child"))?,
                    };
                    self.check_restart_capacity_input(&snapshot, &cut.node, &request.attestation)
                        .await?;
                    Ok((snapshot, request, cut, basis))
                })
                .await?;
            local.check(&cut.quiescence, selector, ResumeMode::Paused)?;
            let quiescence = cut.quiescence.clone();
            let mut walk = FleetRestartResumeVerification::new(
                verification::VerificationInput {
                    catalog: self,
                    snapshot: &snapshot,
                    request: &request,
                    observer: gateway,
                    cut,
                    basis,
                    local: Some(local.clone()),
                },
                &deadline,
            )
            .await?;
            let expected = self
                .run(
                    deadline.remaining()?,
                    self.current_resume_intent(&snapshot, selector.operation),
                )
                .await?;
            if walk.book.physical != expected.physical {
                return Err(FleetCatalogError::Conflict);
            }
            // Root input pins retain only the complete logical inventory that
            // admitted this intent. All physical, quorum, matched and profile
            // evidence is reacquired; no saved samples or clocks are restored.
            let remaining = walk.effect_check(&deadline).await?;
            local.check(&quiescence, selector, ResumeMode::Paused)?;
            let expires = gateway
                .handle
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?
                .checked_add(remaining)
                .ok_or(FleetCatalogError::Limit)?
                .min(owner_limit);
            deadline.remaining()?;
            Ok(ResumeCheck {
                quiescence,
                deadline: expires,
            })
        })
    }
}
