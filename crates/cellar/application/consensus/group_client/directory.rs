// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Targeted router refresh uses the one joined directory worker and its cache.
use super::*;
use crate::raft::{completion::RuntimeError, group_host::RouterRefreshRequest};
use tokio::sync::mpsc;

impl RaftCells {
    pub(crate) async fn refresh_local_namespace_routes(
        &self,
        history: GroupHistory,
        deadline: u64,
    ) -> Result<(), RuntimeError> {
        self.metadata_live(deadline)
            .map_err(|_| RuntimeError::Closed)?;
        self.handle
            .domain
            .run(crate::asyncrt::timeout_at(
                deadline,
                self.handle.core.host.refresh_namespace_routes(history),
            ))
            .await
            .map_err(|_| RuntimeError::Closed)?
            .map_err(|_| RuntimeError::DeadlineExceeded)?
            .map_err(|_| RuntimeError::NotReady)?;
        self.metadata_live(deadline)
            .map_err(|_| RuntimeError::DeadlineExceeded)?;
        Ok(())
    }

    pub(crate) fn bind_router_refresh(
        &self,
    ) -> Result<mpsc::Receiver<RouterRefreshRequest>, RuntimeError> {
        self.handle
            .core
            .transport
            .bind_router_refresh(self.control_budget().clone())
    }

    pub(crate) fn refresh_managed_router(
        &self,
        history: GroupHistory,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send + 'static {
        let admitted = self
            .metadata_admission(timeout, 4096)
            .map_err(|error| match error {
                cellar_logic::consensus::CommandFailure::Overloaded => RuntimeError::Overloaded,
                _ => RuntimeError::Closed,
            });
        let owner = self.internal_observer();
        async move {
            let (deadline, _memory) = admitted?;
            owner
                .handle
                .core
                .transport
                .refresh_managed_router(history, deadline)
                .await?;
            owner
                .metadata_live(deadline)
                .map_err(|_| RuntimeError::DeadlineExceeded)?;
            Ok(())
        }
    }
}
