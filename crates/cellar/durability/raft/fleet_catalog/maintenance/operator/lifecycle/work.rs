// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resume durable children; repeat every ephemeral physical/quorum proof.
use super::super::attempt::{Attempt, Stage};
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;

pub(in super::super) struct ResumeSession {
    pub attempt: [u8; 16],
    pub walk: FleetRestartResumeVerification,
}

pub(in super::super) fn step<'a>(
    driver: &'a FleetRestartOperatorDriver,
    record: &'a Record,
    attempt: &'a Attempt,
    stage: Stage,
    facts: Facts,
    deadline: &'a Deadline<'_>,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let root = driver.catalog.read(deadline.remaining()?).await?;
        driver
            .catalog
            .run(
                deadline.remaining()?,
                driver.catalog.claim_restart_operator_step(
                    &root,
                    record,
                    Some(attempt.journal.id),
                    stage,
                    Some(facts),
                ),
            )
            .await
    })
}

// A failed live observation with unchanged metadata has no immutable effect to
// replace. Back off instead of churning Root and starving a corrected grant.
pub(super) async fn retry_verification(
    driver: &FleetRestartOperatorDriver,
    record: &Record,
    attempt: &Attempt,
    facts: Facts,
    completing: bool,
    deadline: &Deadline<'_>,
) -> Result<()> {
    let catalog = &driver.catalog;
    let root = catalog.read(deadline.remaining()?).await?;
    let staged = catalog
        .run(
            deadline.remaining()?,
            catalog.inner.read::<FleetMutation>(
                &format!("requests/{}", hex::encode(attempt.request.id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            ),
        )
        .await?
        .is_some();
    if completing || staged || root.revision() != attempt.request.expected_revision {
        step(
            driver,
            record,
            attempt,
            Stage::VerifyResume,
            facts,
            deadline,
        )
        .await
    } else {
        Err(FleetCatalogError::Conflict)
    }
}

pub(super) async fn grant(
    driver: &FleetRestartOperatorDriver,
    root: &FleetSnapshot,
    record: &Record,
    name: &str,
    deadline: &Deadline<'_>,
) -> Result<Option<grants::Grant>> {
    driver
        .catalog
        .run(
            deadline.remaining()?,
            driver.catalog.operator_action(root, record, name),
        )
        .await
}

pub(in super::super) fn returning(
    record: &Record,
    attempt: &Attempt,
    grant: &grants::Grant,
) -> Result<FleetRestartReturnRequest> {
    let FleetRestartOperatorAction::Return { process } = grant.action else {
        return Err(FleetCatalogError::Corrupt("operator return grant"));
    };
    Ok(FleetRestartReturnRequest {
        identity: attempt.request.clone(),
        offline: attempt
            .facts
            .offline
            .clone()
            .ok_or(FleetCatalogError::Conflict)?,
        node: NodeId::new(record.boot.node)
            .map_err(|_| FleetCatalogError::Corrupt("operator node"))?,
        expected_process: ProcessGeneration::new(record.boot.process)
            .map_err(|_| FleetCatalogError::Corrupt("operator process"))?,
        next_process: ProcessGeneration::new(process)
            .map_err(|_| FleetCatalogError::Corrupt("operator return process"))?,
    })
}

pub(super) fn await_return<'a>(
    driver: &'a FleetRestartOperatorDriver,
    root: &'a FleetSnapshot,
    attempt: &'a Attempt,
    value: &'a mut FleetRestartOperatorProgress,
    deadline: &'a Deadline<'_>,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let record = value.admission.record.clone();
        let facts = attempt.facts.clone();
        if grant(driver, root, &record, "return", deadline)
            .await?
            .is_some()
        {
            step(driver, &record, attempt, Stage::Returning, facts, deadline).await?;
        } else {
            value.waiting = true;
        }
        Ok(())
    })
}

pub(super) fn admit_return<'a>(
    driver: &'a FleetRestartOperatorDriver,
    root: &'a FleetSnapshot,
    attempt: &'a Attempt,
    value: &'a mut FleetRestartOperatorProgress,
    deadline: &'a Deadline<'_>,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let catalog = &driver.catalog;
        let record = value.admission.record.clone();
        let mut facts = attempt.facts.clone();
        let grant = grant(driver, root, &record, "return", deadline)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing return grant"))?;
        let request = returning(&record, attempt, &grant)?;
        match catalog
            .admit_restart_return(root, &request, deadline.remaining()?)
            .await
        {
            Ok(_) => {
                facts.returning = Some(request.identity);
                step(
                    driver,
                    &record,
                    attempt,
                    Stage::AwaitReturned,
                    facts,
                    deadline,
                )
                .await?;
            }
            Err(FleetCatalogError::Conflict) => {
                step(driver, &record, attempt, Stage::Returning, facts, deadline).await?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    })
}

pub(super) fn await_returned<'a>(
    driver: &'a FleetRestartOperatorDriver,
    root: &'a FleetSnapshot,
    attempt: &'a Attempt,
    value: &'a mut FleetRestartOperatorProgress,
    deadline: &'a Deadline<'_>,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let record = value.admission.record.clone();
        let facts = attempt.facts.clone();
        if grant(driver, root, &record, "resume", deadline)
            .await?
            .is_some()
        {
            step(
                driver,
                &record,
                attempt,
                Stage::VerifyResume,
                facts,
                deadline,
            )
            .await?;
        } else {
            value.waiting = true;
        }
        Ok(())
    })
}

/// One boxed phase per turn keeps stop, return and verification frames separate.
pub(in super::super) fn reconcile<'a>(
    driver: &'a FleetRestartOperatorDriver,
    root: &'a FleetSnapshot,
    attempt: &'a Attempt,
    mut value: FleetRestartOperatorProgress,
    session: &'a mut Option<ResumeSession>,
    deadline: &'a Deadline<'_>,
) -> BoxFuture<'a, Result<FleetRestartOperatorProgress>> {
    Box::pin(async move {
        match &attempt.stage {
            Stage::ConsentStop { .. } => {
                super::stop::consent(driver, root, attempt, &mut value, deadline).await?
            }
            Stage::Stopping => {
                super::stop::stopping(driver, root, attempt, &mut value, deadline).await?
            }
            Stage::AwaitReturn => await_return(driver, root, attempt, &mut value, deadline).await?,
            Stage::Returning => admit_return(driver, root, attempt, &mut value, deadline).await?,
            Stage::AwaitReturned => {
                await_returned(driver, root, attempt, &mut value, deadline).await?
            }
            Stage::VerifyResume | Stage::VerifyComplete => {
                super::resume::verify(driver, root, attempt, &mut value, session, deadline).await?
            }
            Stage::Resuming => {
                super::resume::reopen(driver, root, attempt, &mut value, session, deadline).await?
            }
            Stage::Finishing => {
                value.completed = Some(
                    Box::pin(terminal::finish(
                        driver,
                        &value.admission.record,
                        attempt,
                        deadline,
                    ))
                    .await?,
                );
                value.phase = FleetRestartOperatorPhase::Complete;
            }
            _ => return Err(FleetCatalogError::Corrupt("operator lifecycle step")),
        }
        value.admission.observed = driver.catalog.read(deadline.remaining()?).await?.revision();
        Ok(value)
    })
}
