// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded advisory routing, independent of serving and membership authority.
use super::*;
use crate::raft::leader_advice::LeaderAdvice;
use cellar_logic::consensus::MemberId;
use std::sync::Weak;

// Includes the map slot, fixed observation and at most 128 incoming voter IDs.
// Entries consume the ordinary control bank, capped at one quarter of both
// parent dimensions. The remaining capacity stays available to progress work.
pub(super) const ENTRY_BYTES: usize = 4096;

pub(super) struct Configuration {
    pub revision: u64,
    pub digest: [u8; 32],
    pub incoming: Vec<MemberId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaderLookup {
    Known(MemberId),
    Unknown,
    Unverified,
    Membership,
}

#[derive(Default)]
struct Observation {
    term: u64,
    leader: Option<MemberId>,
    conflicting: bool,
    usable: bool,
    started_at: u64,
}

impl Observation {
    fn remember(&mut self, advice: LeaderAdvice, started_at: u64, now: u64, ttl: u64) {
        if advice.term < self.term || started_at > now {
            return;
        }
        if advice.term > self.term {
            *self = Self {
                term: advice.term,
                leader: Some(advice.leader),
                started_at,
                ..Self::default()
            };
        } else if self.leader != Some(advice.leader) {
            self.conflicting = true;
            self.usable = false;
            return;
        }
        self.started_at = self.started_at.max(started_at);
        self.usable = !self.conflicting && now.saturating_sub(self.started_at) < ttl;
    }

    fn leader(&self, now: u64, ttl: u64) -> Option<MemberId> {
        (self.usable && now >= self.started_at && now - self.started_at < ttl)
            .then_some(self.leader)
            .flatten()
    }
}

struct Entry {
    route: Weak<GroupRoute<GroupHistory>>,
    generation: Arc<()>,
    configuration: Configuration,
    observation: Observation,
    membership_refresh: bool,
    touched: u64,
    _memory: Permit,
}

pub(super) struct Cache {
    capacity: usize,
    entries: BTreeMap<GroupHistory, Entry>,
}

impl Cache {
    pub(super) fn new(options: &TransportOptions) -> Self {
        Self {
            capacity: options
                .max_groups
                .min(options.node_control_bytes / 4 / ENTRY_BYTES)
                .min(options.queue_items * options.max_peers / 4),
            entries: BTreeMap::new(),
        }
    }

    pub(super) fn check_configuration(
        &self,
        route: &GroupRoute<GroupHistory>,
        configuration: &Configuration,
    ) -> Result<(), TransportError> {
        let incoming: BTreeSet<_> = configuration.incoming.iter().copied().collect();
        if configuration.revision == 0
            || configuration.digest == [0; 32]
            || incoming.is_empty()
            || incoming.len() > 128
            || incoming.len() != configuration.incoming.len()
            || incoming.iter().any(|member| {
                !route
                    .peers
                    .iter()
                    .any(|peer| peer.replica.member == *member)
            })
        {
            return Err(TransportError::Identity);
        }
        if let Some(entry) = self.entries.get(&route.scope) {
            if entry.configuration.revision > configuration.revision {
                return Err(TransportError::StalePeer);
            }
            if entry.configuration.revision == configuration.revision
                && (entry.configuration.digest != configuration.digest
                    || entry
                        .configuration
                        .incoming
                        .iter()
                        .copied()
                        .collect::<BTreeSet<_>>()
                        != incoming)
            {
                return Err(TransportError::Identity);
            }
        }
        Ok(())
    }

    pub(super) fn install(
        &mut self,
        route: &Arc<GroupRoute<GroupHistory>>,
        mut configuration: Configuration,
        memory: &Arc<Budget>,
        now: u64,
    ) -> Result<(), TransportError> {
        self.check_configuration(route, &configuration)?;
        configuration.incoming.sort_unstable();
        if let Some(entry) = self.entries.get_mut(&route.scope) {
            if entry.configuration.revision == configuration.revision
                && entry.configuration.digest == configuration.digest
                && entry.configuration.incoming == configuration.incoming
            {
                // A transport revision or endpoint change is not a new Raft
                // configuration. Preserve its term floor and conflict history;
                // neither reinstalling metadata nor rebinding a route renews age.
                if entry.membership_refresh || !entry.route.ptr_eq(&Arc::downgrade(route)) {
                    entry.membership_refresh = false;
                    entry.route = Arc::downgrade(route);
                    entry.observation.usable = false;
                    entry.generation = Arc::new(());
                }
                return Ok(());
            }
        }
        self.entries.remove(&route.scope);
        if self.capacity == 0 {
            return Ok(());
        }
        if self.entries.len() == self.capacity {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(history, entry)| (entry.touched, **history))
                .map(|(history, _)| *history)
            {
                self.entries.remove(&oldest);
            }
        }
        let Some(permit) = memory.acquire(ENTRY_BYTES) else {
            return Ok(());
        };
        self.entries.insert(
            route.scope,
            Entry {
                route: Arc::downgrade(route),
                generation: Arc::new(()),
                configuration,
                observation: Observation::default(),
                membership_refresh: false,
                touched: now,
                _memory: permit,
            },
        );
        Ok(())
    }

    pub(super) fn lookup(
        &mut self,
        route: &Arc<GroupRoute<GroupHistory>>,
        now: u64,
        ttl: u64,
    ) -> LeaderLookup {
        let Some(entry) = self
            .entries
            .get_mut(&route.scope)
            .filter(|entry| entry.route.ptr_eq(&Arc::downgrade(route)))
        else {
            return LeaderLookup::Unverified;
        };
        entry.touched = now;
        if entry.membership_refresh {
            return LeaderLookup::Membership;
        }
        entry
            .observation
            .leader(now, ttl)
            .map_or(LeaderLookup::Unknown, LeaderLookup::Known)
    }

    pub(super) fn generation(&self, route: &Arc<GroupRoute<GroupHistory>>) -> Option<Weak<()>> {
        self.entries
            .get(&route.scope)
            .filter(|entry| entry.route.ptr_eq(&Arc::downgrade(route)))
            .map(|entry| Arc::downgrade(&entry.generation))
    }

    pub(super) fn remember(
        &mut self,
        route: &Arc<GroupRoute<GroupHistory>>,
        generation: &Weak<()>,
        advice: LeaderAdvice,
        started_at: u64,
        now: u64,
        ttl: u64,
    ) {
        let Some(entry) = self.entries.get_mut(&route.scope).filter(|entry| {
            entry.route.ptr_eq(&Arc::downgrade(route))
                && generation.ptr_eq(&Arc::downgrade(&entry.generation))
        }) else {
            return;
        };
        if advice.history != route.scope || advice.term == 0 {
            return;
        }
        if advice.membership_revision < entry.configuration.revision {
            return;
        }
        if advice.membership_revision != entry.configuration.revision
            || advice.configuration_digest != entry.configuration.digest
            || !entry.configuration.incoming.contains(&advice.leader)
        {
            entry.membership_refresh = true;
            entry.observation.usable = false;
            return;
        }
        entry.observation.remember(advice, started_at, now, ttl);
        entry.touched = now;
    }

    pub(super) fn suspect(
        &mut self,
        route: &Arc<GroupRoute<GroupHistory>>,
        generation: &Weak<()>,
        member: MemberId,
    ) {
        if let Some(entry) = self.entries.get_mut(&route.scope).filter(|entry| {
            entry.route.ptr_eq(&Arc::downgrade(route))
                && generation.ptr_eq(&Arc::downgrade(&entry.generation))
        }) {
            if entry.observation.leader == Some(member) {
                entry.observation.usable = false;
            }
        }
    }

    pub(super) fn refresh_endpoint(
        &mut self,
        routes: &BTreeMap<GroupHistory, Arc<GroupRoute<GroupHistory>>>,
        node: NodeId,
    ) {
        for (history, entry) in &mut self.entries {
            if let Some(route) = routes.get(history).filter(|route| {
                !entry.route.ptr_eq(&Arc::downgrade(route))
                    && route.peers.iter().any(|peer| peer.replica.node == node)
            }) {
                entry.route = Arc::downgrade(route);
                entry.generation = Arc::new(());
                entry.observation.usable = false;
            }
        }
    }

    pub(super) fn prune(&mut self, routes: &BTreeMap<GroupHistory, Arc<GroupRoute<GroupHistory>>>) {
        self.entries.retain(|history, entry| {
            routes
                .get(history)
                .is_some_and(|route| entry.route.ptr_eq(&Arc::downgrade(route)))
        });
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(super) fn fence_peers(&mut self, allowed: impl Fn(&RaftPeerNode) -> bool) {
        for entry in self.entries.values_mut() {
            if entry.route.upgrade().is_some_and(|route| {
                route
                    .peers
                    .iter()
                    .any(|peer| !allowed(&RaftPeerNode::from(peer)))
            }) {
                entry.observation.usable = false;
                entry.generation = Arc::new(());
            }
        }
    }
}

impl RaftTransport {
    pub(crate) fn cell_command_leader(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
    ) -> Result<LeaderLookup, TransportError> {
        let state = &self.handle.0;
        let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        let Some(view) = registry.cell_query_groups.get(&route.history).cloned() else {
            return Ok(LeaderLookup::Unverified);
        };
        if view.schema_digest != route.schema_digest {
            return Err(TransportError::Identity);
        }
        let result = registry
            .leaders
            .lookup(&view, now, state.options.io_timeout_ms);
        if let LeaderLookup::Known(member) = result {
            let peer = view
                .peers
                .iter()
                .find(|peer| peer.replica.member == member)
                .ok_or(TransportError::Identity)?;
            registry.check_node_view(&RaftPeerNode::from(peer))?;
            if !registry
                .pools
                .get(&peer.replica.node)
                .is_some_and(|pool| pool.peer.same_host(peer))
            {
                return Err(TransportError::StalePeer);
            }
            state
                .performance
                .increment(super::super::performance::Counter::CellLeaderAdviceHits, 1);
        }
        Ok(result)
    }
}

impl State {
    pub(super) fn remember_cell_leader(
        &self,
        route: &Arc<GroupRoute<GroupHistory>>,
        generation: Option<&Weak<()>>,
        issuer: &RaftPeer<GroupHistory>,
        advice: Option<LeaderAdvice>,
        started_at: u64,
    ) {
        let Some(advice) = advice else {
            return;
        };
        let Some(generation) = generation else {
            return;
        };
        let Ok(now) = self.domain.mono_ms() else {
            return;
        };
        let Ok(mut registry) = self.registry.lock() else {
            return;
        };
        if self.admit(&registry).is_err()
            || !registry
                .cell_query_groups
                .get(&route.scope)
                .is_some_and(|current| Arc::ptr_eq(current, route))
            || !route.peers.contains(issuer)
            || registry
                .check_node_view(&RaftPeerNode::from(issuer))
                .is_err()
            || !registry
                .pools
                .get(&issuer.replica.node)
                .is_some_and(|pool| pool.peer.same_host(issuer))
        {
            return;
        }
        registry.leaders.remember(
            route,
            generation,
            advice,
            started_at,
            now,
            self.options.io_timeout_ms,
        );
    }

    pub(super) fn suspect_cell_leader(
        &self,
        route: &Arc<GroupRoute<GroupHistory>>,
        generation: Option<&Weak<()>>,
        member: MemberId,
    ) {
        let Some(generation) = generation else {
            return;
        };
        if let Ok(mut registry) = self.registry.lock() {
            registry.leaders.suspect(route, generation, member);
        }
    }
}

#[cfg(test)]
#[path = "leader_cache_tests.rs"]
mod tests;
