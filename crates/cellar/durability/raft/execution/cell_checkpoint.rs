// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A unique checkpoint proposal capability, without SQLite or continuation
//! authority. The original SQL owner keeps its writer until this work joins.

use super::*;
use std::sync::Arc;

pub(crate) struct CellCheckpointGrant {
    authority: Arc<CellExecutionAuthority>,
    stamp: Arc<()>,
}

pub(crate) struct CellCheckpointPrepared {
    authority: Arc<CellExecutionAuthority>,
    stamp: Arc<()>,
    pending: PendingCell,
}

impl CellPrivateExecutor {
    pub(super) fn checkpoint_grant(
        &mut self,
        input: &CellPrivatePreparation,
    ) -> Result<CellCheckpointGrant, ExecutionError> {
        match &self.preparing {
            Some(PreparationOwner::Local(stamp))
                if self.pending.is_none() && Arc::ptr_eq(stamp, input.stamp()) => {}
            _ => return Err(ExecutionError::Invalid("checkpoint preparation producer")),
        }
        let stamp = input.stamp().clone();
        self.preparing = Some(PreparationOwner::Checkpoint(stamp.clone()));
        Ok(CellCheckpointGrant {
            authority: self.authority.clone(),
            stamp,
        })
    }

    pub(crate) fn adopt_checkpoint(
        &mut self,
        prepared: CellCheckpointPrepared,
    ) -> Result<(), ExecutionError> {
        if self.pending.is_some()
            || !Arc::ptr_eq(&self.authority, &prepared.authority)
            || !matches!(&self.preparing, Some(PreparationOwner::Checkpoint(stamp))
                if Arc::ptr_eq(stamp, &prepared.stamp))
        {
            return Err(ExecutionError::Invalid("checkpoint return producer"));
        }
        // A submitted checkpoint can already have advanced the selected leaf
        // or lost its term. Adoption retains reconciliation custody only; it
        // grants no execution or acknowledgement authority.
        self.pending = Some(prepared.pending);
        self.preparing = None;
        self.poisoned = false;
        Ok(())
    }
}

impl CellCheckpointGrant {
    pub(crate) fn complete(
        self,
        image: CellPrivatePrepared,
    ) -> Result<CellCheckpointPrepared, ExecutionError> {
        let matches = image.matches_stamp(&self.stamp);
        let image = image.checked(if matches {
            Ok(())
        } else {
            Err(ExecutionError::Invalid("checkpoint preparation producer"))
        })?;
        let mut pending = image.into_pending(&self.authority)?;
        pending.submitted = true;
        Ok(CellCheckpointPrepared {
            authority: self.authority,
            stamp: self.stamp,
            pending,
        })
    }
}

impl CellCheckpointPrepared {
    pub(crate) fn command(&self) -> &CellPreparedCommand {
        &self.pending.prepared
    }

    pub(crate) fn expected(&self) -> &CellVersion {
        &self.pending.result
    }
}
