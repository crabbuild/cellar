// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One joined node service finishes the actual recovered groups' activation.
//! Pending genesis is never an application capability or a replacement quorum.
use super::*;
use crate::application::consensus::group_client::ControlServiceKind;
use crate::raft::{
    catalog::{CatalogError, DisasterFenceAuthority},
    completion::RuntimeError,
    storage::ReplicaLog,
};
use std::time::Duration;

pub(super) struct Registration {
    pub directory: RangeDirectory,
    pub request: Arc<CellGroupRecoveryRequest>,
    pub authority: Arc<dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>>,
}

pub(super) fn start(
    service: &RaftCells,
    log: ReplicaLog,
    runtime: CellGroupRuntime,
    mut groups: Vec<Registration>,
    bytes: usize,
    poll_ms: u64,
) -> anyhow::Result<()> {
    if groups.is_empty() {
        return Ok(());
    }
    let lease = service.register_control_service(ControlServiceKind::Recovery, bytes)?;
    let mut stop = lease.stop_receiver();
    log.domain()
        .spawn_cleanup(async move {
            let _lease = lease;
            while !groups.is_empty() && !*stop.borrow() {
                let mut ordinal = 0;
                while ordinal < groups.len() && !*stop.borrow() {
                    let pending = &groups[ordinal];
                    let history = pending.request.replacement().route().history;
                    match activate(pending, &log, &runtime).await {
                        Ok(true) => {
                            groups.remove(ordinal);
                        }
                        Ok(false) => {
                            ordinal += 1;
                        }
                        Err(error) if retryable(&error) => {
                            ordinal += 1;
                        }
                        Err(error) => {
                            tracing::warn!(%error, "recovered group activation refused");
                            // Refuse this entire group while retaining its original
                            // disk, seed and catalog intent for exact reconciliation.
                            let _ = runtime.quarantine(history).await;
                            groups.remove(ordinal);
                        }
                    }
                }
                if groups.is_empty() {
                    break;
                }
                crate::asyncrt::select! {
                    _ = stop.changed() => break,
                    _ = crate::asyncrt::sleep(Duration::from_millis(poll_ms)) => (),
                }
            }
        })?
        .detach();
    Ok(())
}

async fn activate(
    group: &Registration,
    log: &ReplicaLog,
    runtime: &CellGroupRuntime,
) -> Result<bool, CatalogError> {
    let permit = group
        .directory
        .prepare_group_recovery_activation(&group.request, log, group.authority.as_ref())
        .await?;
    let timer = log
        .domain()
        .sleep_until(permit.deadline())
        .map_err(|_| CatalogError::Unconfirmed)?;
    // Runtime queuing retains the permit and checks its original deadline.
    // Cancelling this waiter never creates a new recovery operation.
    let result = crate::asyncrt::select! {
        _ = timer => return Err(CatalogError::Unconfirmed),
        result = runtime.activate_recovery(permit) => result.map_err(CatalogError::Runtime)?,
    };
    match result {
        CellGroupRecoveryActivation::Accepted(_) => Ok(false),
        CellGroupRecoveryActivation::Applied(acknowledgement) => {
            if group
                .directory
                .group_recovery_pending(&group.request)
                .await?
            {
                group
                    .directory
                    .complete_group_recovery(
                        &group.request,
                        &acknowledgement,
                        group.authority.as_ref(),
                    )
                    .await?;
            }
            Ok(true)
        }
    }
}

fn retryable(error: &CatalogError) -> bool {
    matches!(
        error,
        CatalogError::Storage(_)
            | CatalogError::Unconfirmed
            | CatalogError::Runtime(
                RuntimeError::NotLeader
                    | RuntimeError::NotReady
                    | RuntimeError::Overloaded
                    | RuntimeError::DeadlineExceeded
                    | RuntimeError::StaleCompletion
            )
    )
}
