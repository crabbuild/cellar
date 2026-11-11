// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, coalesced requests into the existing joined directory/cache owner.
use crate::asyncrt::DomainToken;
use crate::raft::{
    budget::{Budget, Permit},
    completion::RuntimeError,
    performance::{Counter, Registry},
    transport::GroupRoute,
};
use cellar_logic::cell_group::GroupHistory;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{mpsc, watch};

const OWNER_BYTES: usize = 32 * 1024;
const WAITER_BYTES: usize = 4096;

/// Exact advisory generations selected before discovery. Weak references keep
/// allocation identities distinct without pinning routes, pools or engines.
#[derive(Clone)]
pub(crate) struct RouterRefreshKey {
    history: GroupHistory,
    route: Option<Weak<GroupRoute<GroupHistory>>>,
    configuration: Option<Weak<()>>,
}

impl RouterRefreshKey {
    pub(crate) fn new(
        history: GroupHistory,
        route: Option<&Arc<GroupRoute<GroupHistory>>>,
        configuration: Option<Weak<()>>,
    ) -> Self {
        Self {
            history,
            route: route.map(Arc::downgrade),
            configuration,
        }
    }

    fn matches(&self, other: &Self) -> bool {
        fn same<T>(a: &Option<Weak<T>>, b: &Option<Weak<T>>) -> bool {
            match (a, b) {
                (Some(a), Some(b)) => a.ptr_eq(b),
                (None, None) => true,
                _ => false,
            }
        }
        self.history == other.history
            && same(&self.route, &other.route)
            && same(&self.configuration, &other.configuration)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouterRefreshOutcome {
    Current,
    StalePlacement,
}

struct Pending {
    key: RouterRefreshKey,
    deadline: u64,
    result: watch::Sender<Option<Result<RouterRefreshOutcome, RuntimeError>>>,
    // Covers the physical owner, first waiter, result and bounded index slot.
    // Other waiters reserve separately in this same parent bank. The owner keeps
    // this charge even if every caller cancels; delivery also retains it.
    _memory: Permit,
}

#[derive(Default)]
struct Shared {
    pending: Mutex<Vec<Weak<Pending>>>,
}

pub(crate) struct RouterRefreshOwner {
    shared: Arc<Shared>,
    pending: Arc<Pending>,
    finished: bool,
}

impl RouterRefreshOwner {
    pub(super) fn history(&self) -> GroupHistory {
        self.pending.key.history
    }

    pub(super) fn deadline(&self) -> u64 {
        self.pending.deadline
    }

    pub(super) fn complete(mut self, result: Result<RouterRefreshOutcome, RuntimeError>) {
        self.finish(result);
    }

    fn finish(&mut self, result: Result<RouterRefreshOutcome, RuntimeError>) {
        if self.finished {
            return;
        }
        self.finished = true;
        let mut pending = self
            .shared
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        pending.retain(|entry| !entry.ptr_eq(&Arc::downgrade(&self.pending)));
        drop(pending);
        self.pending.result.send_replace(Some(result));
    }
}

impl Drop for RouterRefreshOwner {
    fn drop(&mut self) {
        // Queued work discarded at close must wake waiters too: their Arc keeps
        // the watch sender alive, so channel closure alone cannot signal this.
        self.finish(Err(RuntimeError::Closed));
    }
}

pub(crate) enum RouterRefreshRequest {
    Track {
        history: GroupHistory,
        _memory: Permit,
    },
    Observe {
        owner: RouterRefreshOwner,
    },
}

struct Admission {
    pending: Arc<Pending>,
    memory: Option<Permit>,
    owner: Option<RouterRefreshOwner>,
}

#[derive(Clone)]
pub(crate) struct RouterRefreshClient {
    sender: mpsc::Sender<RouterRefreshRequest>,
    memory: Arc<Budget>,
    domain: DomainToken,
    performance: Arc<Registry>,
    shared: Arc<Shared>,
}

impl RouterRefreshClient {
    /// Best-effort cache scheduling only. No metadata I/O, serving authority or
    /// request lifetime depends on this hint; a later use can enqueue it again.
    pub(crate) fn note(&self, history: GroupHistory) {
        let Some(memory) = self.memory.acquire(OWNER_BYTES) else {
            return;
        };
        let _ = self.sender.try_send(RouterRefreshRequest::Track {
            history,
            _memory: memory,
        });
    }

    pub(crate) fn channel(
        memory: Arc<Budget>,
        domain: DomainToken,
        performance: Arc<Registry>,
    ) -> Result<(Self, mpsc::Receiver<RouterRefreshRequest>), RuntimeError> {
        let (_, capacity) = memory.capacity();
        if capacity == 0 {
            return Err(RuntimeError::Invalid("router refresh capacity".into()));
        }
        let (sender, receiver) = mpsc::channel(capacity);
        Ok((
            Self {
                sender,
                memory,
                domain,
                performance,
                shared: Arc::new(Shared::default()),
            },
            receiver,
        ))
    }

    fn admit(&self, key: RouterRefreshKey, deadline: u64) -> Result<Admission, RuntimeError> {
        let mut index = self
            .shared
            .pending
            .lock()
            .map_err(|_| RuntimeError::Closed)?;
        index.retain(|entry| entry.strong_count() > 0);
        if let Some(pending) = index
            .iter()
            .filter_map(Weak::upgrade)
            .find(|pending| pending.key.matches(&key))
        {
            let memory = self
                .memory
                .acquire(WAITER_BYTES)
                .ok_or(RuntimeError::Overloaded)?;
            self.performance
                .increment(Counter::CellMembershipRefreshJoins, 1);
            return Ok(Admission {
                pending,
                memory: Some(memory),
                owner: None,
            });
        }
        let memory = self
            .memory
            .acquire(OWNER_BYTES)
            .ok_or(RuntimeError::Overloaded)?;
        let pending = Arc::new(Pending {
            key,
            deadline,
            result: watch::channel(None).0,
            _memory: memory,
        });
        index.push(Arc::downgrade(&pending));
        let owner = RouterRefreshOwner {
            shared: self.shared.clone(),
            pending: pending.clone(),
            finished: false,
        };
        Ok(Admission {
            pending,
            memory: None,
            owner: Some(owner),
        })
    }

    pub(crate) async fn request(
        &self,
        key: RouterRefreshKey,
        deadline: u64,
    ) -> Result<RouterRefreshOutcome, RuntimeError> {
        if self.domain.mono_ms().map_err(|_| RuntimeError::Closed)? >= deadline {
            return Err(RuntimeError::DeadlineExceeded);
        }
        let Admission {
            pending,
            memory,
            owner,
        } = self.admit(key, deadline)?;
        let mut response = pending.result.subscribe();
        if let Some(owner) = owner {
            if let Err(error) = self
                .sender
                .try_send(RouterRefreshRequest::Observe { owner })
            {
                let (request, error) = match error {
                    mpsc::error::TrySendError::Full(request) => (request, RuntimeError::Overloaded),
                    mpsc::error::TrySendError::Closed(request) => (request, RuntimeError::Closed),
                };
                if let RouterRefreshRequest::Observe { owner } = request {
                    owner.complete(Err(error.clone()));
                }
                return Err(error);
            }
            self.performance
                .increment(Counter::CellMembershipDiscoveryOwners, 1);
        }
        // Completion is advisory metadata only. Every caller still validates its
        // selected route/authority, with its original deadline and attempt bank.
        // A longer-lived joiner never extends the first owner's deadline.
        let result = self
            .domain
            .run(crate::asyncrt::timeout_at(deadline, async {
                loop {
                    let result = response.borrow_and_update().clone();
                    if let Some(result) = result {
                        return result;
                    }
                    response.changed().await.map_err(|_| RuntimeError::Closed)?;
                }
            }))
            .await
            .map_err(|_| RuntimeError::Closed)?
            .map_err(|_| RuntimeError::DeadlineExceeded)?;
        drop(memory);
        drop(pending);
        result
    }
}

#[cfg(test)]
#[path = "router_refresh_tests.rs"]
mod tests;
