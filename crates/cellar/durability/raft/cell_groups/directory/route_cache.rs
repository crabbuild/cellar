// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded namespace range hints. Cached observations never grant authority.
use super::*;
use std::collections::VecDeque;

// Two bounded routing-key strings plus the route, deque slot and bookkeeping.
// Reserve the full capacity up front; idle/evicted slots cannot hide memory.
const ENTRY_BYTES: usize = 8192;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellRouteCacheUsage {
    pub entries: usize,
    pub capacity: usize,
    pub reserved_bytes: usize,
    pub revision: Option<u64>,
}

struct State {
    head: Option<(u64, [u8; 32])>,
    routes: VecDeque<GroupRoute>,
}

pub(crate) struct RouteCache {
    directory: RangeDirectory,
    capacity: usize,
    state: Mutex<State>,
}

impl RouteCache {
    pub(crate) fn new(
        directory: RangeDirectory,
        entries: usize,
        bytes: usize,
    ) -> Result<Self, CatalogError> {
        if !(1..=4096).contains(&entries) || !(ENTRY_BYTES..=64 * 1024 * 1024).contains(&bytes) {
            return Err(CatalogError::Limit);
        }
        let capacity = entries.min(bytes / ENTRY_BYTES);
        Ok(Self {
            directory,
            capacity,
            state: Mutex::new(State {
                head: None,
                routes: VecDeque::with_capacity(capacity),
            }),
        })
    }

    pub(crate) fn usage(&self) -> Result<CellRouteCacheUsage, CatalogError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CatalogError::Corrupt("routing cache"))?;
        Ok(CellRouteCacheUsage {
            entries: state.routes.len(),
            capacity: self.capacity,
            reserved_bytes: self.capacity * ENTRY_BYTES,
            revision: state.head.map(|h| h.0),
        })
    }

    pub(super) fn get(
        &self,
        cell: &CellHistory,
    ) -> Result<Option<(GroupRoute, u64)>, CatalogError> {
        let config = self.directory.namespace_config();
        if cell.cluster != config.cluster || cell.namespace != config.namespace {
            return Err(CatalogError::Invalid("routing cache namespace"));
        }
        let key = cell_routing_key(cell.namespace, &cell.cell);
        let mut state = self
            .state
            .lock()
            .map_err(|_| CatalogError::Corrupt("routing cache"))?;
        let Some((revision, _)) = state.head else {
            return Ok(None);
        };
        let Some(index) = state
            .routes
            .iter()
            .position(|route| route.range.contains(&key))
        else {
            return Ok(None);
        };
        let route = state
            .routes
            .remove(index)
            .ok_or(CatalogError::Corrupt("routing cache entry"))?;
        let result = route.clone();
        state.routes.push_back(route);
        Ok(Some((result, revision)))
    }

    pub(crate) fn observe(&self, view: &DirectoryView) -> Result<(), CatalogError> {
        self.directory.check_view(view)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| CatalogError::Corrupt("routing cache"))?;
        Self::advance(&mut state, view)
    }

    fn advance(state: &mut State, view: &DirectoryView) -> Result<(), CatalogError> {
        if let Some((revision, digest)) = state.head {
            if view.revision() < revision {
                return Err(CatalogError::Conflict("superseded routing observation"));
            }
            if view.revision() == revision {
                return if view.digest() == digest {
                    Ok(())
                } else {
                    Err(CatalogError::Corrupt("routing revision changed digest"))
                };
            }
        }
        state.routes.clear();
        state.head = Some((view.revision(), view.digest()));
        Ok(())
    }

    // Only the routing session calls this after lookup in this exact pinned view.
    // An older in-flight lookup cannot repopulate an observed successor root.
    pub(super) fn remember(
        &self,
        view: &DirectoryView,
        route: &GroupRoute,
    ) -> Result<(), CatalogError> {
        self.directory.check_view(view)?;
        wire::Route::encode(route, &self.directory)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| CatalogError::Corrupt("routing cache"))?;
        Self::advance(&mut state, view)?;
        if let Some(index) = state.routes.iter().position(|old| old == route) {
            state.routes.remove(index);
        }
        if state.routes.len() == self.capacity {
            state.routes.pop_front();
        }
        state.routes.push_back(route.clone());
        Ok(())
    }
}
