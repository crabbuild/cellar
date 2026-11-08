// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Advisory physical-group metadata stays separate from logical cell versions.
use super::*;
use crate::raft::completion::RuntimeError;

impl RaftCells {
    pub(crate) fn partition_groups(
        &self,
        scopes: &[(
            cellar_logic::consensus::ClusterId,
            cellar_logic::consensus::NamespaceId,
        )],
        limit: usize,
    ) -> Result<Vec<cellar_logic::cell_group::GroupHistory>, RuntimeError> {
        if self.is_closed() || self.handle.admission.is_draining() {
            return Err(RuntimeError::Closed);
        }
        self.handle.core.controller.partition_groups(scopes, limit)
    }
}

impl RaftGroupHandle {
    /// Request the existing bounded publisher-discovery owner. A true result
    /// means a local publisher is installed, not that any cut is archived.
    /// Binding initialization and verified archive coverage remain separate.
    pub(crate) async fn ensure_existing_archive(
        &self,
        timeout: Duration,
    ) -> Result<bool, CellDispatchError> {
        let observed = self
            .observe_partition(timeout)
            .await
            .map_err(CellDispatchError::Runtime)?;
        match self
            .owner
            .handle
            .core
            .controller
            .s3_client(&self.owner.handle.domain)
            .map_err(CellDispatchError::Runtime)?
            .require_group(&observed.route)
        {
            Ok(()) => Ok(true),
            Err(CellDispatchError::Runtime(RuntimeError::NotReady)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Sample this local physical owner and its durable log cache. The deadline
    /// starts at invocation. This does not perform ReadIndex or open cell images.
    pub fn observe(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupObservation, RuntimeError>> + Send + 'static {
        let deadline = self.control_deadline(timeout);
        let group = self.clone();
        async move {
            let deadline = deadline?;
            group.control_live(deadline)?;
            let observation = super::drain::within(
                &group.owner.handle.domain,
                deadline,
                group.owner.handle.core.runtime.observe(group.history),
            )
            .await?;
            group.control_live(deadline)?;
            Ok(observation)
        }
    }

    /// Read bounded partition geometry from the selected in-memory manifest.
    /// No manifest walk, SQLite open, or ReadIndex is performed. Archive lag
    /// comes from the local archive owner's separate historical observation.
    pub fn observe_partition(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupPartitionObservation, RuntimeError>> + Send + 'static
    {
        let deadline = self.control_deadline(timeout);
        let group = self.clone();
        async move {
            let deadline = deadline?;
            group.control_live(deadline)?;
            let mut sample = super::drain::within(
                &group.owner.handle.domain,
                deadline,
                group
                    .owner
                    .handle
                    .core
                    .runtime
                    .observe_partition(group.history),
            )
            .await?;
            sample.archive_lag_entries = partition_archive_lag(
                &sample.route,
                sample.status.committed,
                group.archive_status()?,
            );
            group.control_live(deadline)?;
            Ok(sample)
        }
    }

    /// Last archive-controller publication, sampled independently of the Raft
    /// owner. None means no installed archive owner. Even a published cut grants
    /// no retained physical coverage or authority to compact the local log.
    pub fn archive_status(&self) -> Result<Option<CellGroupArchiveStatus>, RuntimeError> {
        if self.owner.is_closed() {
            return Err(RuntimeError::Closed);
        }
        self.owner
            .handle
            .core
            .controller
            .archive_status(self.history)
    }
}

// A retryable upload failure does not erase the owner's previous complete cut.
// These independently sampled counters are advisory; the transition preparer
// still needs a retained, verified archive closure before seal authorization.
fn partition_archive_lag(
    route: &cellar_logic::cell_group::GroupRoute,
    committed: cellar_logic::consensus::LogIndex,
    status: Option<CellGroupArchiveStatus>,
) -> Option<u64> {
    let status = status.filter(|s| !s.stopped)?;
    let archived = status.published.filter(|cut| cut.route() == route)?;
    // The archive owner can advance between the Raft sample and this read.
    Some(
        committed
            .get()
            .saturating_sub(archived.position().index.get()),
    )
}

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;
