// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fence unadmitted partition work before resolving permanent birth decisions.
use super::{
    histories::{catalog_error, history_mutation, Origin},
    managed_partition_work::{finished_key, ManagedPartitionWork, ACTIVE_KEY},
    *,
};
use crate::raft::catalog::{CellGroupBirthDisposition, RangeDirectory};
use cellar_logic::{cell_group::GroupHistory, consensus::ProposalId};

fn cancellation_key(operation: [u8; 16]) -> String {
    format!("partition-work/cancellation/{}", hex::encode(operation))
}

fn group_key(group: &GroupHistory) -> String {
    format!(
        "partition-work/cancelled-group/{}",
        hex::encode(group.group.bytes())
    )
}

impl FleetCatalog {
    pub(super) async fn partition_cancellation_at(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<Option<ManagedPartitionWork>> {
        self.partition_work_value(snapshot, &cancellation_key(operation))
            .await
    }

    pub(crate) async fn partition_cancellation_requested(
        &self,
        operation: ProposalId,
        request: [u8; 32],
        timeout: Duration,
    ) -> Result<bool> {
        self.run(timeout, async {
            let current = self.inner.current().await?;
            match self
                .partition_cancellation_at(&current, operation.bytes())
                .await?
            {
                Some(work) if work.operation == operation.bytes() && work.request == request => {
                    Ok(true)
                }
                Some(_) => Err(FleetCatalogError::PayloadConflict),
                None => Ok(false),
            }
        })
        .await
    }

    /// Every fresh root mutation observes the cancellation fence. Historical
    /// committed receipts may still replay, but cannot grant a new root write.
    pub(super) async fn check_partition_cancellation(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        authority: resources::Authority,
    ) -> Result<()> {
        if authority == resources::Authority::PartitionWork {
            return Ok(());
        }
        if self
            .partition_cancellation_at(snapshot, mutation.operation)
            .await?
            .is_some()
        {
            return Err(FleetCatalogError::Conflict);
        }
        for change in mutation
            .changes
            .iter()
            .filter(|c| c.table == FleetTable::Histories)
        {
            let previous = self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Histories.slot()],
                    &change.key,
                )
                .await?;
            // The existing restart owner may reconcile the immutable namespace
            // cancellation. It validates the actual winner and keeps physical
            // occupancy charged. No other metadata or birth disposition changes.
            if authority == resources::Authority::RestartSettlement {
                if let (Some(before), Some(after)) = (previous.as_deref(), change.value.as_deref())
                {
                    let mut before = FleetHistory::decode(before)?;
                    let after = FleetHistory::decode(after)?;
                    if matches!(after.state, FleetHistoryState::Cancelled { .. }) {
                        before.state = after.state.clone();
                        if before == after {
                            continue;
                        }
                    }
                }
            }
            for bytes in [previous.as_deref(), change.value.as_deref()]
                .into_iter()
                .flatten()
            {
                let history = FleetHistory::decode(bytes)?;
                let scope = history
                    .binding()
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("cancelled partition group"))?;
                if self
                    .partition_work_value(snapshot, &group_key(&scope))
                    .await?
                    .is_some()
                {
                    return Err(FleetCatalogError::Conflict);
                }
            }
        }
        Ok(())
    }

    /// Cancellation applies only before managed transition admission. Its root
    /// CAS competes with every child reservation and transition-slot admission.
    /// Reserved histories and their capacity debt remain unchanged. A birth
    /// that already materialized blocks completion and retains the fenced cursor.
    pub(crate) async fn cancel_partition_work(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        request: [u8; 32],
        principal: &str,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let selected = self
            .run(
                deadline.remaining()?,
                Box::pin(async {
                    let current = self.inner.current().await?;
                    let fenced = self
                        .partition_cancellation_at(&current, operation.bytes())
                        .await?;
                    if let Some(finished) = self
                        .partition_work_value(&current, &finished_key(operation))
                        .await?
                    {
                        if finished.operation == operation.bytes()
                            && finished.request == request
                            && finished.principal == principal
                            && finished.namespace == directory.namespace().root_digest()
                            && fenced
                                .as_ref()
                                .is_some_and(|w| w.matches_request(&finished))
                        {
                            return Ok(None);
                        }
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    let work = self
                        .partition_work_at(&current)
                        .await?
                        .ok_or(FleetCatalogError::Conflict)?;
                    if work.operation != operation.bytes()
                        || work.request != request
                        || work.principal != principal
                        || work.namespace != directory.namespace().root_digest()
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    let plan = self
                        .transition_operator_plan_at(directory, operation, request)
                        .await?
                        .ok_or(FleetCatalogError::Conflict)?;
                    if plan.digest() != work.plan {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    let (_, children) =
                        directory.transition_routes(&plan).map_err(catalog_error)?;
                    if !(1..=2).contains(&children.len()) {
                        return Err(FleetCatalogError::Invalid(
                            "partition cancellation children",
                        ));
                    }
                    let mut owned = Vec::new();
                    for route in children {
                        let key = self.binding(&route.history)?.key().map_err(|_| {
                            FleetCatalogError::Invalid("partition cancellation binding")
                        })?;
                        if let Some(bytes) = self
                            .inner
                            .lookup(current.head.tables[FleetTable::Histories.slot()], &key)
                            .await?
                        {
                            let history = FleetHistory::decode(&bytes)?;
                            // A proposed child is not ownership. Do not fence or cancel
                            // an existing group belonging to another operation.
                            if history.operation != work.operation {
                                continue;
                            }
                            if history.origin != Origin::Managed
                                || !history.check_route(directory, &route)
                            {
                                return Err(FleetCatalogError::Conflict);
                            }
                            owned.push(route);
                        }
                    }
                    if let Some(fenced) = fenced {
                        if !fenced.matches_request(&work) {
                            return Err(FleetCatalogError::PayloadConflict);
                        }
                    } else {
                        if self
                            .observe_managed_transition_at(&current, directory, operation)
                            .await?
                            .is_some()
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                        let mut changes = vec![FleetChange {
                            table: FleetTable::Operations,
                            key: cancellation_key(operation.bytes()),
                            value: Some(work.locator_value()?),
                        }];
                        for route in &owned {
                            if self
                                .partition_work_value(&current, &group_key(&route.history))
                                .await?
                                .is_some()
                            {
                                return Err(FleetCatalogError::Conflict);
                            }
                            changes.push(FleetChange {
                                table: FleetTable::Operations,
                                key: group_key(&route.history),
                                value: Some(work.locator_value()?),
                            });
                        }
                        let mutation =
                            history_mutation(&work.identity(current.revision(), 2)?, changes);
                        mutation.validate()?;
                        self.commit_with_disruption(
                            &current,
                            &mutation,
                            None,
                            resources::Authority::PartitionWork,
                            || Ok(()),
                        )
                        .await?;
                    }
                    Ok(Some((work, owned)))
                }),
            )
            .await?;
        let Some((work, children)) = selected else {
            return Ok(());
        };
        for route in children {
            let snapshot = self.read(deadline.remaining()?).await?;
            // Selection recorded every owned child at the fenced root. Missing
            // canonical reservation state must refuse, never imply cleanup.
            let authorization = self
                .recover_birth_reservation(
                    &snapshot,
                    directory,
                    &route.history,
                    deadline.remaining()?,
                )
                .await?;
            if authorization.route() != &route {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let winner = self
                .inner
                .domain
                .run(Box::pin(crate::asyncrt::timeout(
                    deadline.remaining()?,
                    directory.cancel_fleet_group_birth(&authorization, operation),
                )))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(|_| FleetCatalogError::Unconfirmed)?
                .map_err(catalog_error)?;
            if !matches!(winner, CellGroupBirthDisposition::Cancelled { history, .. } if history == route.history)
            {
                return Err(FleetCatalogError::Conflict);
            }
        }
        self.run(deadline.remaining()?, async {
            let current = self.inner.current().await?;
            if let Some(finished) = self
                .partition_work_value(&current, &finished_key(operation))
                .await?
            {
                return if finished.matches_request(&work) {
                    Ok(())
                } else {
                    Err(FleetCatalogError::PayloadConflict)
                };
            }
            if !self
                .partition_work_at(&current)
                .await?
                .is_some_and(|w| w.matches_request(&work))
                || !self
                    .partition_cancellation_at(&current, operation.bytes())
                    .await?
                    .is_some_and(|w| w.matches_request(&work))
            {
                return Err(FleetCatalogError::Conflict);
            }
            let mutation = history_mutation(
                &work.identity(current.revision(), 3)?,
                vec![
                    FleetChange {
                        table: FleetTable::Operations,
                        key: ACTIVE_KEY.into(),
                        value: None,
                    },
                    FleetChange {
                        table: FleetTable::Operations,
                        key: finished_key(operation),
                        value: Some(work.locator_value()?),
                    },
                ],
            );
            mutation.validate()?;
            self.commit_with_disruption(
                &current,
                &mutation,
                None,
                resources::Authority::PartitionWork,
                || Ok(()),
            )
            .await?;
            Ok(())
        })
        .await
    }
}
