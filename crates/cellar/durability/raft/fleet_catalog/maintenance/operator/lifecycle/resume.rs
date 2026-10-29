// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Repeat returned-voter proof before consent and terminal completion.
use super::super::attempt::{Attempt, Stage};
use super::work::{grant, retry_verification, step, ResumeSession};
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;

pub(super) fn verify<'a>(
    driver: &'a FleetRestartOperatorDriver,
    root: &'a FleetSnapshot,
    attempt: &'a Attempt,
    value: &'a mut FleetRestartOperatorProgress,
    session: &'a mut Option<ResumeSession>,
    deadline: &'a Deadline<'_>,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let catalog = &driver.catalog;
        let record = value.admission.record.clone();
        let mut facts = attempt.facts.clone();
        let completing = matches!(attempt.stage, Stage::VerifyComplete);
        let grant = grant(driver, root, &record, "resume", deadline)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing resume grant"))?;
        let next = if completing {
            facts.resume.clone().ok_or(FleetCatalogError::Conflict)?
        } else {
            Resume {
                identity: attempt.request.clone(),
            }
        };
        let request = next.request(&facts, &grant.action)?;
        let complete = FleetRestartResumeCompleteRequest {
            identity: attempt.request.clone(),
            resume: request.identity.clone(),
        };
        let historical = if completing {
            catalog
                .restart_resume_completion(root, &complete, deadline.remaining()?)
                .await
                .map(|_| ())
        } else {
            catalog
                .restart_resume_admission(root, &request, deadline.remaining()?)
                .await
                .map(|_| ())
        };
        match historical {
            Ok(()) => {
                *session = None;
                if completing {
                    facts.completion = Some(complete.identity);
                    step(driver, &record, attempt, Stage::Finishing, facts, deadline).await?;
                } else {
                    facts.resume = Some(next);
                    step(driver, &record, attempt, Stage::Resuming, facts, deadline).await?;
                }
                return Ok(());
            }
            Err(FleetCatalogError::Unconfirmed) => (),
            Err(FleetCatalogError::PayloadConflict)
                if !completing && root.revision() != attempt.request.expected_revision =>
            {
                let admitted = catalog
                    .run(
                        deadline.remaining()?,
                        catalog.inner.lookup(
                            root.head.tables[FleetTable::Operations.slot()],
                            &format!("restart-resume/{}", hex::encode(attempt.request.operation)),
                        ),
                    )
                    .await?
                    .is_some();
                if admitted {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                *session = None;
                step(
                    driver,
                    &record,
                    attempt,
                    Stage::VerifyResume,
                    facts,
                    deadline,
                )
                .await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
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
        if staged || root.revision() != attempt.request.expected_revision {
            *session = None;
            step(
                driver,
                &record,
                attempt,
                attempt.stage.clone(),
                facts,
                deadline,
            )
            .await?;
            return Ok(());
        }
        if session
            .as_ref()
            .is_none_or(|s| s.attempt != attempt.request.id)
        {
            *session = None;
            match catalog
                .prepare_restart_resume(
                    root,
                    &request,
                    &driver.namespaces,
                    &driver.observer,
                    deadline.remaining()?,
                )
                .await
            {
                Ok(walk) => {
                    *session = Some(ResumeSession {
                        attempt: attempt.request.id,
                        walk,
                    })
                }
                Err(FleetCatalogError::Conflict) => {
                    retry_verification(driver, &record, attempt, facts, completing, deadline)
                        .await?;
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        let result = session
            .as_mut()
            .ok_or(FleetCatalogError::Closed)?
            .walk
            .reconcile_next(deadline.remaining()?)
            .await;
        match result {
            Ok(Some(_)) => {
                value.checked_histories =
                    session.as_ref().map_or(0, |s| s.walk.verified_histories());
            }
            Ok(None) => {
                let walk = session.take().ok_or(FleetCatalogError::Closed)?.walk;
                let published = if completing {
                    walk.complete_resume(&complete, deadline.remaining()?)
                        .await
                        .map(|_| ())
                } else {
                    walk.admit_resume(&request, deadline.remaining()?)
                        .await
                        .map(|_| ())
                };
                match published {
                    Ok(()) => {
                        if completing {
                            facts.completion = Some(complete.identity);
                            step(driver, &record, attempt, Stage::Finishing, facts, deadline)
                                .await?;
                        } else {
                            facts.resume = Some(next);
                            step(driver, &record, attempt, Stage::Resuming, facts, deadline)
                                .await?;
                        }
                    }
                    Err(FleetCatalogError::Conflict) => {
                        retry_verification(driver, &record, attempt, facts, completing, deadline)
                            .await?;
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(FleetCatalogError::Conflict) => {
                *session = None;
                retry_verification(driver, &record, attempt, facts, completing, deadline).await?;
            }
            Err(FleetCatalogError::Unconfirmed) => {
                *session = None;
                return Err(FleetCatalogError::Unconfirmed);
            }
            Err(error) => return Err(error),
        }
        Ok(())
    })
}

pub(super) fn reopen<'a>(
    driver: &'a FleetRestartOperatorDriver,
    root: &'a FleetSnapshot,
    attempt: &'a Attempt,
    value: &'a mut FleetRestartOperatorProgress,
    session: &'a mut Option<ResumeSession>,
    deadline: &'a Deadline<'_>,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let catalog = &driver.catalog;
        let record = value.admission.record.clone();
        let facts = attempt.facts.clone();
        let grant = grant(driver, root, &record, "resume", deadline)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing resume grant"))?;
        let request = facts
            .resume
            .as_ref()
            .ok_or(FleetCatalogError::Conflict)?
            .request(&facts, &grant.action)?;
        let admission = catalog
            .restart_resume_admission(root, &request, deadline.remaining()?)
            .await?;
        match catalog
            .resume_restart_maintenance(&admission, &driver.observer, deadline.remaining()?)
            .await
        {
            Ok(()) => {
                *session = None;
                step(
                    driver,
                    &record,
                    attempt,
                    Stage::VerifyComplete,
                    facts,
                    deadline,
                )
                .await?;
            }
            Err(FleetCatalogError::Conflict) => {
                *session = None;
                step(
                    driver,
                    &record,
                    attempt,
                    Stage::VerifyResume,
                    facts,
                    deadline,
                )
                .await?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    })
}
