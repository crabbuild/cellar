// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded namespace range hints. Cached observations never grant authority.
use super::*;
use crate::raft::budget::{Budget, Permit};
use sha2::{Digest, Sha256};
use std::{collections::VecDeque, sync::Weak};

// Includes bounded routing-key strings, the original root/revision and slots.
const ENTRY_BYTES: usize = 8192;

// Encoded/decoded intent, two bounded Merkle frontiers and invalidation ranges.
// Borrowed from the existing invocation bank; exhaustion clears the cache.
const REPAIR_BYTES: usize = 512 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellRouteCacheUsage {
    pub entries: usize,
    pub capacity: usize,
    pub reserved_bytes: usize,
    /// Newest observed publication; retained rows may have older verified roots.
    pub revision: Option<u64>,
}

#[cfg(test)]
#[path = "route_cache_tests.rs"]
mod tests;

#[derive(Clone, Copy)]
struct Head {
    revision: u64,
    journal: [u8; 32],
    genesis: [u8; 32],
    root: [u8; 32],
    fence: Option<[u8; 32]>,
}

impl Head {
    fn observed(view: &DirectoryView) -> Result<Self, CatalogError> {
        let fence = if view.head.recovery.is_some() || view.head.import.is_some() {
            let mut hash = Sha256::new();
            hash.update(b"cellar-routing-cache-fence-1");
            // Stream the bounded fence into the digest without an uncharged
            // serialized copy. Every recovery phase/binding participates.
            serde_json::to_writer(&mut hash, &(&view.head.recovery, view.head.import))
                .map_err(|_| CatalogError::Corrupt("routing cache fence"))?;
            Some(hash.finalize().into())
        } else {
            None
        };
        Ok(Self {
            revision: view.revision(),
            journal: view.head.journal,
            genesis: view.head.genesis,
            root: view.digest(),
            fence,
        })
    }

    fn same_routing(self, next: Self) -> bool {
        self.revision == next.revision
            && self.journal == next.journal
            && self.genesis == next.genesis
            && self.fence == next.fence
    }

    fn check(&self, next: Self) -> Result<(), CatalogError> {
        if next.revision < self.revision {
            return Err(CatalogError::Conflict("superseded routing observation"));
        }
        if next.revision == self.revision && next.journal != self.journal {
            return Err(CatalogError::Corrupt("routing revision changed digest"));
        }
        Ok(())
    }
}

struct Entry {
    route: GroupRoute,
    revision: u64,
    root: [u8; 32],
}

struct State {
    head: Option<Head>,
    generation: Arc<()>,
    routes: VecDeque<Entry>,
    pending: bool,
}

// The verified newer head/generation is installed before proof I/O. Dropping
// that I/O must keep its negative observation and discard every unverified row.
struct RepairGuard<'a> {
    cache: &'a RouteCache,
    generation: Weak<()>,
    armed: bool,
}

impl Drop for RepairGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            if let Ok(mut state) = self.cache.state.lock() {
                if self.generation.ptr_eq(&Arc::downgrade(&state.generation)) {
                    if let Some(head) = state.head {
                        state.routes.retain(|entry| entry.revision >= head.revision);
                    }
                    state.pending = false;
                }
            }
        }
    }
}

pub(super) struct LookupGeneration(Weak<()>);

pub(crate) struct RouteCache {
    directory: RangeDirectory,
    capacity: usize,
    state: Mutex<State>,
    memory: Arc<Budget>,
    progress_bytes: usize,
    repair: tokio::sync::Mutex<()>,
}

impl RouteCache {
    pub(crate) fn new(
        directory: RangeDirectory,
        entries: usize,
        bytes: usize,
        memory: Arc<Budget>,
        progress_bytes: usize,
    ) -> Result<Self, CatalogError> {
        if !(1..=4096).contains(&entries)
            || !(ENTRY_BYTES..=64 * 1024 * 1024).contains(&bytes)
            || progress_bytes < 8192
            || progress_bytes > memory.capacity().0
            || memory.capacity().1 == 0
        {
            return Err(CatalogError::Limit);
        }
        let capacity = entries.min(bytes / ENTRY_BYTES);
        Ok(Self {
            directory,
            capacity,
            state: Mutex::new(State {
                head: None,
                generation: Arc::new(()),
                routes: VecDeque::with_capacity(capacity),
                pending: false,
            }),
            memory,
            progress_bytes,
            repair: tokio::sync::Mutex::new(()),
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
            revision: state.head.map(|h| h.revision),
        })
    }

    pub(super) fn generation(&self) -> Result<LookupGeneration, CatalogError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CatalogError::Corrupt("routing cache"))?;
        Ok(LookupGeneration(Arc::downgrade(&state.generation)))
    }

    pub(super) fn can_reuse(
        &self,
        route: &GroupRoute,
        revision: u64,
        root: [u8; 32],
    ) -> Result<bool, CatalogError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CatalogError::Corrupt("routing cache"))?;
        Ok(state.head.is_none_or(|head| {
            ((head.fence.is_none() || root == head.root) && revision >= head.revision)
                || state.routes.iter().any(|entry| {
                    (!state.pending || entry.revision >= head.revision)
                        && entry.route == *route
                        && entry.revision == revision
                        && entry.root == root
                })
        }))
    }

    pub(in crate::raft) fn get(
        &self,
        cell: &CellHistory,
        minimum_revision: u64,
    ) -> Result<Option<(GroupRoute, u64, [u8; 32])>, CatalogError> {
        let config = self.directory.namespace_config();
        if cell.cluster != config.cluster || cell.namespace != config.namespace {
            return Err(CatalogError::Invalid("routing cache namespace"));
        }
        let key = cell_routing_key(cell.namespace, &cell.cell);
        let mut state = self
            .state
            .lock()
            .map_err(|_| CatalogError::Corrupt("routing cache"))?;
        let Some(index) = state.routes.iter().position(|entry| {
            (!state.pending
                || state
                    .head
                    .is_some_and(|head| entry.revision >= head.revision))
                && entry.revision >= minimum_revision
                && entry.route.range.contains(&key)
        }) else {
            return Ok(None);
        };
        let entry = state
            .routes
            .remove(index)
            .ok_or(CatalogError::Corrupt("routing cache entry"))?;
        let result = (entry.route.clone(), entry.revision, entry.root);
        state.routes.push_back(entry);
        Ok(Some(result))
    }

    pub(crate) async fn refresh(&self) -> Result<u64, CatalogError> {
        self.directory
            .bounded(async {
                let view = self.directory.observe().await?;
                self.advance(&view, None, None).await?;
                Ok(view.revision())
            })
            .await
    }

    // A lookup captures its generation before its first await. The proof and
    // insertion share the original invocation's absolute deadline at the caller.
    pub(super) async fn observe_lookup(
        &self,
        view: &DirectoryView,
        generation: &LookupGeneration,
    ) -> Result<LookupGeneration, CatalogError> {
        Box::pin(self.advance(view, None, Some(generation))).await
    }

    pub(super) async fn remember(
        &self,
        view: &DirectoryView,
        route: &GroupRoute,
        generation: &LookupGeneration,
    ) -> Result<(), CatalogError> {
        wire::Route::encode(route, &self.directory)?;
        if let Some(recovery) = &view.head.recovery {
            if recovery.blocks_application_route(&self.directory, route)? {
                self.observe_lookup(view, generation).await?;
                return Err(CatalogError::Fenced);
            }
        }
        Box::pin(self.advance(view, Some(route), Some(generation)))
            .await
            .map(|_| ())
    }

    async fn advance(
        &self,
        view: &DirectoryView,
        route: Option<&GroupRoute>,
        generation: Option<&LookupGeneration>,
    ) -> Result<LookupGeneration, CatalogError> {
        self.directory.check_view(view)?;
        let next = Head::observed(view)?;
        let (previous, entered) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| CatalogError::Corrupt("routing cache"))?;
            if generation.is_some_and(|token| !token.0.ptr_eq(&Arc::downgrade(&state.generation))) {
                return Err(CatalogError::Conflict("superseded routing observation"));
            }
            if let Some(previous) = state.head {
                previous.check(next)?;
                if previous.same_routing(next) {
                    state.head = Some(next);
                    self.insert(&mut state, view, route);
                    return Ok(LookupGeneration(Arc::downgrade(&state.generation)));
                }
            }
            let previous = state.head;
            let repairable = previous.filter(|previous| {
                !state.pending
                    && !state.routes.is_empty()
                    && previous.fence.is_none()
                    && next.fence.is_none()
                    && previous.genesis == next.genesis
                    && previous.revision < next.revision
            });
            // Publish the negative boundary synchronously. No expired/cancelled
            // proof can forget it and let an older lookup refill its parent.
            if previous.is_some() || next.fence.is_some() {
                state.generation = Arc::new(());
            }
            state.head = Some(next);
            if let Some(previous) = repairable {
                state.pending = true;
                (previous, Arc::downgrade(&state.generation))
            } else {
                // A second publication during pending repair cannot omit the
                // first change by proving only the second edge of the chain.
                state.routes.clear();
                state.pending = false;
                self.insert(&mut state, view, route);
                return Ok(LookupGeneration(Arc::downgrade(&state.generation)));
            }
        };
        let mut guard = RepairGuard {
            cache: self,
            generation: entered.clone(),
            armed: true,
        };
        let mut reservation: Option<Permit> = None;
        let mut repair_owner = None;
        let proof = {
            if let Ok(owner) = self.repair.try_lock() {
                repair_owner = Some(owner);
                // Hold a normal invocation's maximum reservation while
                // admitting repair. Releasing it immediately leaves business
                // progress headroom; a one-slot/tight parent simply falls back.
                if let Some(progress) = self.memory.acquire(self.progress_bytes) {
                    reservation = self.memory.acquire(REPAIR_BYTES);
                    drop(progress);
                }
            }
            if reservation.is_some() {
                match self
                    .directory
                    .bounded(self.directory.routing_repair(
                        view,
                        previous.revision,
                        previous.journal,
                    ))
                    .await
                {
                    Ok(proof) => Some(proof),
                    Err(error) => {
                        tracing::debug!(
                            ?error,
                            "routing proof unavailable; invalidating all cached ranges"
                        );
                        None
                    }
                }
            } else {
                None
            }
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| CatalogError::Corrupt("routing cache"))?;
        // Any intervening publication/fence wins, even at the same catalog
        // revision. A late proof cannot restore its earlier root or routes.
        if !entered.ptr_eq(&Arc::downgrade(&state.generation)) {
            return Err(CatalogError::Conflict("superseded routing observation"));
        }
        if let Some(proof) = &proof {
            state.routes.retain(|entry| {
                entry.revision >= next.revision
                    || !proof
                        .intervals
                        .iter()
                        .any(|interval| routing_proof::overlaps(&entry.route.range, interval))
            });
        } else {
            state.routes.retain(|entry| entry.revision >= next.revision);
        }
        state.pending = false;
        self.insert(&mut state, view, route);
        let generation = LookupGeneration(Arc::downgrade(&state.generation));
        guard.armed = false;
        drop(state);
        drop(proof);
        drop(reservation);
        drop(repair_owner);
        Ok(generation)
    }

    fn insert(&self, state: &mut State, view: &DirectoryView, route: Option<&GroupRoute>) {
        let Some(route) = route else {
            return;
        };
        if let Some(index) = state.routes.iter().position(|entry| entry.route == *route) {
            state.routes.remove(index);
        }
        if state.routes.len() == self.capacity {
            state.routes.pop_front();
        }
        state.routes.push_back(Entry {
            route: route.clone(),
            revision: view.revision(),
            root: view.digest(),
        });
    }
}
