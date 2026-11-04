// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Advisory physical-group metadata stays separate from logical cell versions.
use super::*;
use crate::raft::completion::RuntimeError;

impl RaftGroupHandle {
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
