// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A retired source drains through its existing scheduler and persistence lane.
use super::*;

impl CellGroupRuntime {
    pub(in crate::raft::cell_groups) async fn quiesce_retired_source(
        &self,
        source: super::super::controller::RetiredSource,
    ) -> Result<(), RuntimeError> {
        let history = source.history();
        match self
            .call(
                Some(history),
                Command::QuiesceRetired(Box::new(source)),
                32 * 1024,
            )
            .await?
        {
            Response::Unit => Ok(()),
            _ => Err(RuntimeError::Protocol("source quiescence reply".into())),
        }
    }
}
