// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently granted stop and signed joined offline confirmation.
use super::super::attempt::{Attempt, Stage};
use super::work::{grant, step};
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;

pub(super) fn consent<'a>(
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
        let Stage::ConsentStop { ready } = &attempt.stage else {
            return Err(FleetCatalogError::Corrupt("operator stop phase"));
        };
        let grant = grant(driver, root, &record, "stop", deadline)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing operator stop grant"))?;
        let FleetRestartOperatorAction::Stop { stop, .. } = grant.action else {
            return Err(FleetCatalogError::Corrupt("operator stop grant"));
        };
        let next = Stop {
            identity: attempt.request.clone(),
            ready: ready.clone(),
            child: stop,
        };
        let request = next.request(&record)?;
        let admitted = if let Some(previous) = &facts.stop {
            catalog
                .refresh_restart_stop(
                    root,
                    &FleetRestartStopRefreshRequest {
                        previous: previous.request(&record)?,
                        request,
                    },
                    deadline.remaining()?,
                )
                .await
        } else {
            catalog
                .admit_restart_stop(root, &request, deadline.remaining()?)
                .await
        };
        match admitted {
            Ok(_) => {
                facts.stop = Some(next);
                step(driver, &record, attempt, Stage::Stopping, facts, deadline).await?;
            }
            Err(FleetCatalogError::Conflict) => {
                step(driver, &record, attempt, Stage::Verify, facts, deadline).await?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    })
}

pub(super) fn stopping<'a>(
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
        let stop = facts
            .stop
            .as_ref()
            .ok_or(FleetCatalogError::Conflict)?
            .request(&record)?;
        if facts.offline.is_some() {
            step(
                driver,
                &record,
                attempt,
                Stage::AwaitReturn,
                facts,
                deadline,
            )
            .await?;
            value.phase = FleetRestartOperatorPhase::Offline;
        } else {
            match catalog
                .restart_stop_closure(&stop, deadline.remaining()?)
                .await
            {
                Ok(_) => {
                    let request = FleetRestartOfflineRequest {
                        identity: attempt.request.clone(),
                        stop,
                    };
                    match catalog
                        .confirm_restart_offline(root, &request, deadline.remaining()?)
                        .await
                    {
                        Ok(_) => {
                            facts.offline = Some(request.identity);
                            step(driver, &record, attempt, Stage::Stopping, facts, deadline)
                                .await?;
                            value.phase = FleetRestartOperatorPhase::Offline;
                        }
                        Err(FleetCatalogError::Conflict) => {
                            step(driver, &record, attempt, Stage::Stopping, facts, deadline)
                                .await?;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(FleetCatalogError::Unconfirmed) => {
                    match catalog
                        .stop_restart_maintenance(
                            root,
                            &stop,
                            &driver.observer,
                            deadline.remaining()?,
                        )
                        .await
                    {
                        Ok(_) => (),
                        Err(FleetCatalogError::Conflict) => {
                            step(driver, &record, attempt, Stage::Verify, facts, deadline).await?;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    })
}
