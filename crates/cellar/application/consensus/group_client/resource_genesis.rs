// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Lazy schema genesis for authenticated immutable resource inspection.
//! Callers validate the private capability and wire before entering this owner.

use super::super::CommandFailure;
use super::*;
use crate::application::cell_bindings::Route;
use crate::raft::completion::RuntimeError;
use cellar_logic::consensus::{IncarnationId, ProposalId};
use rand::RngCore as _;

impl RaftCells {
    pub(super) async fn resolve_resource_metadata(
        &self,
        route: &mut Route,
        deadline: u64,
        source_proven: bool,
    ) -> anyhow::Result<()> {
        if source_proven {
            super::super::require_framework_dispatch()?;
        }
        self.check_open()?;
        let remaining = deadline
            .checked_sub(self.handle.domain.mono_ms()?)
            .filter(|ms| *ms > 0)
            .ok_or(RuntimeError::DeadlineExceeded)?;
        if route.history_hint.is_none() {
            let observed = self
                .locate_resource_history(
                    route.cluster,
                    route.namespace,
                    route.cell.clone(),
                    QueryOptions {
                        timeout: Duration::from_millis(remaining),
                        ..Default::default()
                    },
                    source_proven,
                )
                .await?;
            // Only a verified fresh absence enters schema genesis. A read has
            // no target business operation, binding sequence or replay debt.
            // Enrollment remains independently idempotent and node-owned.
            route.history_hint = Some(match observed {
                Some(history) => history,
                None => {
                    self.enroll_resource_metadata(route, deadline, source_proven)
                        .await?
                }
            });
        }
        Ok(())
    }

    async fn enroll_resource_metadata(
        &self,
        route: &Route,
        deadline: u64,
        source_proven: bool,
    ) -> anyhow::Result<CellHistory> {
        let remaining = || -> anyhow::Result<Duration> {
            self.check_open()?;
            Ok(Duration::from_millis(
                deadline
                    .checked_sub(self.handle.domain.mono_ms()?)
                    .filter(|ms| *ms > 0)
                    .ok_or(RuntimeError::DeadlineExceeded)?,
            ))
        };
        let (_, _memory) = self.metadata_admission(remaining()?, 8192)?;
        let mut creation = [0; 16];
        let mut incarnation = [0; 16];
        let mut rng = crate::asyncrt::rng("resource-inspection-enrollment");
        rng.fill_bytes(&mut creation);
        rng.fill_bytes(&mut incarnation);
        creation[0] |= 1;
        incarnation[0] |= 1;
        let creation = ProposalId::new(creation)?;
        let candidate = CellHistory {
            cluster: route.cluster,
            namespace: route.namespace,
            cell: route.cell.clone(),
            incarnation: IncarnationId::new(incarnation)?,
        };
        match self
            .cell(candidate.clone())?
            .enroll_direct(
                CellEnrollmentRequest {
                    creation,
                    durability: route.durability,
                },
                remaining()?,
            )
            .await
        {
            Ok(proof) => {
                anyhow::ensure!(
                    proof.record().cell() == &candidate
                        && proof.record().creation() == creation
                        && proof.record().schema() == route.schema
                        && proof.durability() >= route.durability,
                    CommandFailure::CorruptState
                );
                // Release enrollment output and its physical admission before
                // the query selects and independently proves its current image.
                Ok(candidate)
            }
            Err(error) => loop {
                // An unknown creation never permits another candidate or a
                // command retry. A concurrent creator may own the logical name;
                // only fresh discovery can select it, then the query rechecks
                // current history/route/quorum and its own S3 minimum.
                match super::sql_ingress::ready_name_scoped(
                    self,
                    &candidate,
                    deadline,
                    source_proven,
                )
                .await
                {
                    Ok(Some(history)) => return Ok(history),
                    Ok(None) => (),
                    Err(_) => return Err(error.into()),
                }
                let delay = (remaining()?.as_millis() as u64)
                    .min(self.handle.core.controller.limits().retry_interval_ms);
                let now = self.handle.domain.mono_ms()?;
                self.handle
                    .domain
                    .sleep_until(now.saturating_add(delay).min(deadline))?
                    .await;
            },
        }
    }
}
