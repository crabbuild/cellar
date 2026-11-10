// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete bounded local inventory, assembled one page per scheduling turn.
use super::partition::observation_failure;
use super::*;
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{ClusterId, NamespaceId},
};
use std::sync::Arc;

#[derive(Default)]
pub(super) struct Inventory {
    revision: Option<[u8; 32]>,
    loading: Vec<GroupHistory>,
    after: Option<GroupHistory>,
    complete: Option<Arc<[GroupHistory]>>,
    pages: u64,
}

impl Inventory {
    pub(super) fn observe(
        &mut self,
        node: &RaftCells,
        scopes: &[(ClusterId, NamespaceId)],
        capacity: usize,
    ) -> Result<Option<Arc<[GroupHistory]>>> {
        let revision = node.partition_revision(scopes).map_err(failure)?;
        if self.revision != Some(revision) {
            self.revision = Some(revision);
            self.loading.clear();
            self.after = None;
            self.complete = None;
        }
        if let Some(complete) = &self.complete {
            return Ok(Some(complete.clone()));
        }
        let page = node
            .partition_page(
                scopes,
                Some(revision),
                self.after,
                capacity,
                capacity.min(16),
            )
            .map_err(failure)?;
        self.pages = self.pages.saturating_add(1);
        if page.revision != revision
            || self.loading.len().saturating_add(page.groups.len()) > capacity
        {
            return Err(FleetCatalogError::Limit.into());
        }
        self.loading.extend(page.groups);
        self.after = page.next;
        if self.after.is_some() {
            return Ok(None);
        }
        let complete: Arc<[GroupHistory]> = std::mem::take(&mut self.loading).into();
        self.complete = Some(complete.clone());
        Ok(Some(complete))
    }

    pub(super) fn pages(&self) -> u64 {
        self.pages
    }

    pub(super) fn groups(&self) -> usize {
        self.complete.as_ref().map_or(0, |groups| groups.len())
    }
}

fn failure(error: crate::raft::completion::RuntimeError) -> RaftFleetFailure {
    match error {
        crate::raft::completion::RuntimeError::Overloaded => FleetCatalogError::Limit.into(),
        other => observation_failure(other),
    }
}
