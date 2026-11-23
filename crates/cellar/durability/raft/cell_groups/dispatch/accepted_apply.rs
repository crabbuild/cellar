// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Suppress SQL probes that cannot yet observe a known accepted entry.
use super::*;
use tokio::sync::watch;

pub(super) async fn wait(
    runtime: &CellGroupRuntime,
    accepted: Option<&CellGroupProposal>,
    progress: &mut watch::Receiver<()>,
) -> Result<(), CellDispatchError> {
    let Some(accepted) = accepted else {
        // A lost acceptance has no scheduling hint. Retain normal reconciliation.
        return Ok(());
    };
    let Ok(group) = runtime.log().cell_group(&accepted.group) else {
        return Ok(());
    };
    let member = group.identity().member.get();
    loop {
        // Subscribe before sampling: apply or a term change can finish while the
        // status reply is in flight. Controller ticks also wake this owner.
        progress.borrow_and_update();
        let Ok(status) = runtime.status(accepted.group).await else {
            return Ok(());
        };
        if status.term != accepted.position.term
            || status.leader != Some(member)
            || status.applied.index >= accepted.position.index
        {
            return Ok(());
        }
        // This is only a scheduling hint. It neither acknowledges a command nor
        // retires its producer, including after its public response expires.
        // The caller still obtains fresh authority and verifies the exact SQL
        // receipt (or newer-term absence) after this wait.
        progress.changed().await.map_err(|_| closed())?;
    }
}
