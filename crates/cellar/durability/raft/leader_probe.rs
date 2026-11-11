// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded discovery owner per captured route and configuration. Discovery
//! selects a destination; every actual mutation still verifies its own authority.
use super::*;
use crate::raft::performance::Counter;
use crate::raft::version::VersionKeys;
use cellar_logic::cell_group::{CellHistory, GroupRoute as CellRoute};
use cellar_logic::consensus::{CommandFailure, MemberId};
use std::sync::Weak;

const OWNER_BYTES: usize = 32 * 1024;
const WAITER_BYTES: usize = 4096;

#[derive(Clone)]
struct Key {
    history: GroupHistory,
    route: Weak<GroupRoute<GroupHistory>>,
    configuration: Option<Weak<()>>,
    epoch: u64,
    birth_digest: [u8; 32],
}

impl Key {
    fn matches(&self, other: &Self) -> bool {
        self.history == other.history
            && self.route.ptr_eq(&other.route)
            && self.epoch == other.epoch
            && self.birth_digest == other.birth_digest
            && match (&self.configuration, &other.configuration) {
                (Some(a), Some(b)) => a.ptr_eq(b),
                (None, None) => true,
                _ => false,
            }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LeaderProbeOutcome {
    Leader(MemberId),
    Membership,
    Retry(CommandFailure),
}

type ResultValue = Result<LeaderProbeOutcome, TransportError>;

// Each reservation borrows the original control bank as well as its derived
// subquota. Together with the leader cache's quarter, at least half of either
// control dimension remains outside advisory routing metadata.
struct Memory {
    _parent: Permit,
    _limit: Permit,
}

struct Pending {
    key: Key,
    result: watch::Sender<Option<ResultValue>>,
    _memory: Memory,
}

#[derive(Default)]
struct Shared {
    // BTreeMap removes the allocation for a finished index slot immediately.
    // Weak identities never retain a completed route, engine or result.
    pending: Mutex<BTreeMap<usize, Weak<Pending>>>,
}

pub(super) struct Probes {
    shared: Arc<Shared>,
    limit: Arc<Budget>,
}

struct Owner {
    shared: Arc<Shared>,
    pending: Arc<Pending>,
    finished: bool,
}

impl Owner {
    fn complete(mut self, result: ResultValue) {
        self.finish(result);
    }

    fn finish(&mut self, result: ResultValue) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.shared
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&(Arc::as_ptr(&self.pending) as usize));
        self.pending.result.send_replace(Some(result));
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        // A queued owner may be discarded without ever being polled. Waiters
        // retain the sender, so channel closure alone cannot wake them.
        self.finish(Err(TransportError::Closed));
    }
}

struct Admission {
    pending: Arc<Pending>,
    _memory: Option<Memory>,
    owner: Option<(Owner, TaskGuard)>,
}

impl Admission {
    async fn wait(&self, state: &State, deadline: u64) -> ResultValue {
        let mut result = self.pending.result.subscribe();
        state
            .seed_until(deadline, async {
                loop {
                    if let Some(value) = result.borrow_and_update().clone() {
                        return value;
                    }
                    result.changed().await.map_err(|_| TransportError::Closed)?;
                }
            })
            .await
    }
}

impl Probes {
    pub(super) fn new(options: &TransportOptions) -> Self {
        Self {
            shared: Arc::new(Shared::default()),
            limit: Budget::new(
                options.node_control_bytes / 4,
                options.queue_items * options.max_peers / 4,
            ),
        }
    }

    fn reserve(&self, state: &State, bytes: usize) -> Result<Memory, TransportError> {
        let limit = self
            .limit
            .acquire(bytes)
            .ok_or(TransportError::Overloaded)?;
        let parent = state
            .outgoing
            .control
            .acquire(bytes)
            .ok_or(TransportError::Overloaded)?;
        Ok(Memory {
            _parent: parent,
            _limit: limit,
        })
    }

    fn admit(&self, state: &Arc<State>, key: Key) -> Result<Admission, TransportError> {
        // Capture/revalidate the route outside this lock. The only nested lock
        // order here is this index followed by the transport task registry.
        let mut index = self
            .shared
            .pending
            .lock()
            .map_err(|_| TransportError::Closed)?;
        if let Some(pending) = index
            .values()
            .filter_map(Weak::upgrade)
            .find(|pending| pending.key.matches(&key))
        {
            let memory = self.reserve(state, WAITER_BYTES)?;
            state
                .performance
                .increment(Counter::CellLeaderProbeJoins, 1);
            return Ok(Admission {
                pending,
                _memory: Some(memory),
                owner: None,
            });
        }
        let memory = self.reserve(state, OWNER_BYTES)?;
        // Reserve the original task quota before publishing an owner. A full
        // task bank cannot leave joiners waiting for work that never started.
        let guard = state.reserve_task()?;
        let pending = Arc::new(Pending {
            key,
            result: watch::channel(None).0,
            _memory: memory,
        });
        index.insert(Arc::as_ptr(&pending) as usize, Arc::downgrade(&pending));
        let owner = Owner {
            shared: self.shared.clone(),
            pending: pending.clone(),
            finished: false,
        };
        state
            .performance
            .increment(Counter::CellLeaderProbeOwners, 1);
        Ok(Admission {
            pending,
            _memory: None,
            owner: Some((owner, guard)),
        })
    }
}

impl RaftTransport {
    pub(crate) async fn discover_cell_command_leader(
        &self,
        route: &CellRoute,
        cell: &CellHistory,
        keys: VersionKeys,
        limit: usize,
        deadline: u64,
    ) -> ResultValue {
        let state = &self.handle.0;
        if !route.history.same_namespace(cell) {
            return Err(TransportError::Identity);
        }
        let key = state.probe_key(route)?;
        let mut admission = match state.leader_probes.admit(state, key.clone()) {
            Ok(admission) => admission,
            // Optional metadata must not reduce existing availability. Inline
            // discovery still consumes the caller's original bounded RPC plan.
            Err(TransportError::Overloaded) => {
                return state
                    .run_leader_probe(&key, route, cell, keys, limit, deadline)
                    .await;
            }
            Err(error) => return Err(error),
        };
        if let Some((owner, guard)) = admission.owner.take() {
            let worker = state.clone();
            let route = route.clone();
            let cell = cell.clone();
            let captured = key.clone();
            state.spawn_reserved(guard, async move {
                // Capture State, never Handle: discovery must not keep the last
                // public transport handle alive and prevent stop-on-drop.
                let result = worker
                    .seed_until(
                        deadline,
                        worker.run_leader_probe(&captured, &route, &cell, keys, limit, deadline),
                    )
                    .await;
                owner.complete(result);
            })?;
        }
        let result = admission.wait(state, deadline).await?;
        state.check_probe_key(&key, route)?;
        Ok(result)
    }
}

impl State {
    fn probe_key(&self, route: &CellRoute) -> Result<Key, TransportError> {
        let registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let view = registry
            .cell_query_groups
            .get(&route.history)
            .ok_or(TransportError::UnknownGroup)?;
        if view.schema_digest != route.schema_digest {
            return Err(TransportError::Identity);
        }
        Ok(Key {
            history: route.history,
            route: Arc::downgrade(view),
            configuration: registry.leaders.generation(view),
            epoch: route.epoch.get(),
            birth_digest: route.birth_digest,
        })
    }

    fn check_probe_key(&self, key: &Key, route: &CellRoute) -> Result<(), TransportError> {
        if key.matches(&self.probe_key(route)?) {
            Ok(())
        } else {
            Err(TransportError::StalePeer)
        }
    }

    fn probe_advice(&self, key: &Key, route: &CellRoute) -> Result<LeaderLookup, TransportError> {
        self.check_probe_key(key, route)?;
        let view = key.route.upgrade().ok_or(TransportError::StalePeer)?;
        let mut registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        self.lookup_cell_leader(&mut registry, &view)
    }

    async fn run_leader_probe(
        &self,
        key: &Key,
        route: &CellRoute,
        cell: &CellHistory,
        keys: VersionKeys,
        limit: usize,
        deadline: u64,
    ) -> ResultValue {
        self.check_probe_key(key, route)?;
        let view = key.route.upgrade().ok_or(TransportError::StalePeer)?;
        let candidates: Vec<_> = view
            .peers
            .iter()
            .take(limit)
            .map(|peer| peer.replica.member)
            .collect();
        let mut failures = ProbeFailures::default();
        for (index, member) in candidates.iter().enumerate() {
            self.check_probe_key(key, route)?;
            let now = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
            let remaining = deadline.saturating_sub(now);
            if remaining < 2 {
                return Err(TransportError::Timeout);
            }
            // The first owner's finite plan and absolute deadline never grow.
            // Preserve the existing share for the subsequent mutation itself.
            let probe_deadline = now + (remaining / (candidates.len() - index + 1) as u64).max(1);
            let response = self
                .probe_cell_command_leader(route, cell, *member, keys.clone(), probe_deadline)
                .await;
            self.check_probe_key(key, route)?;
            match response {
                Ok(true) => return Ok(LeaderProbeOutcome::Leader(*member)),
                Ok(false) => failures.no_leader = true,
                Err(error) => failures.observe(error)?,
            }
            match self.probe_advice(key, route)? {
                LeaderLookup::Known(member) => return Ok(LeaderProbeOutcome::Leader(member)),
                LeaderLookup::Membership | LeaderLookup::Unverified => failures.membership = true,
                LeaderLookup::Unknown => (),
            }
        }
        Ok(failures.repair())
    }
}

#[derive(Default)]
struct ProbeFailures {
    membership: bool,
    unreachable: bool,
    no_leader: bool,
    overloaded: bool,
}

impl ProbeFailures {
    fn observe(&mut self, error: TransportError) -> Result<(), TransportError> {
        match error {
            TransportError::StalePeer | TransportError::UnknownGroup => self.membership = true,
            TransportError::Io(_) | TransportError::Timeout => self.unreachable = true,
            TransportError::Overloaded => self.overloaded = true,
            error => return Err(error),
        }
        Ok(())
    }

    fn repair(&self) -> LeaderProbeOutcome {
        if self.membership {
            LeaderProbeOutcome::Membership
        } else if self.no_leader {
            LeaderProbeOutcome::Retry(CommandFailure::NotLeader { leader: None })
        } else if self.unreachable {
            LeaderProbeOutcome::Retry(CommandFailure::Unavailable)
        } else if self.overloaded {
            LeaderProbeOutcome::Retry(CommandFailure::Overloaded)
        } else {
            LeaderProbeOutcome::Membership
        }
    }
}

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "leader_probe_tests.rs"]
mod tests;
