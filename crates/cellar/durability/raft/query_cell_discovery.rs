// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read-only physical discovery; no local RawNode, membership or write authority.
use super::*;
use crate::raft::storage::LogError;
use cellar_logic::consensus::{MemberId, NodeId};
use std::collections::BTreeMap;

impl RaftTransport {
    pub(crate) fn cell_query_route_limit(&self) -> usize {
        self.handle.0.options.max_groups
    }

    pub(crate) fn cell_query_router_has_member(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        member: MemberId,
    ) -> Result<bool, TransportError> {
        let state = &self.handle.0;
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        let view = registry
            .cell_query_groups
            .get(&route.history)
            .ok_or(TransportError::UnknownGroup)?;
        if view.schema_digest != route.schema_digest {
            return Err(TransportError::Identity);
        }
        Ok(view.peers.iter().any(|peer| peer.replica.member == member))
    }

    pub(crate) fn cell_query_router_members(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        limit: usize,
    ) -> Result<Vec<MemberId>, TransportError> {
        let state = &self.handle.0;
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        let view = registry
            .cell_query_groups
            .get(&route.history)
            .ok_or(TransportError::UnknownGroup)?;
        if view.schema_digest != route.schema_digest {
            return Err(TransportError::Identity);
        }
        Ok(view
            .peers
            .iter()
            .take(limit)
            .map(|p| p.replica.member)
            .collect())
    }

    /// Install trusted read destinations for a group without a local consensus
    /// owner, including retained removed-member disks. The caller supplies
    /// discovery; each destination independently checks
    /// namespace permission, live membership and the requested cell read proof.
    /// Shares node route/peer bounds with consensus and all other query routes.
    pub fn install_cell_query_group(
        &self,
        route: GroupRoute<GroupHistory>,
    ) -> Result<(), TransportError> {
        self.install_cell_query_group_inner(route, None)
    }

    pub(crate) fn install_observed_cell_query_group(
        &self,
        route: GroupRoute<GroupHistory>,
        observation: &crate::raft::host::FleetMembershipObservation,
    ) -> Result<(), TransportError> {
        if observation.replica().scope != route.scope
            || observation.schema_digest() != route.schema_digest
        {
            return Err(TransportError::Identity);
        }
        self.install_cell_query_group_inner(
            route,
            Some(leader_cache::Configuration {
                revision: observation.membership_revision().get(),
                digest: observation.configuration_digest(),
                incoming: observation.configuration().voters().to_vec(),
            }),
        )
    }

    /// Historical initial configuration metadata for an unmanaged static route.
    /// Advice must still match its exact digest/revision; this supplies neither
    /// current membership nor permission to serve, bootstrap or learn a peer.
    pub(crate) fn install_initial_cell_query_group(
        &self,
        route: GroupRoute<GroupHistory>,
        birth: &crate::raft::cell_groups::CellGroupBirth,
    ) -> Result<(), TransportError> {
        if birth.route().history != route.scope
            || birth.route().schema_digest != route.schema_digest
        {
            return Err(TransportError::Identity);
        }
        let state = &self.handle.0;
        // Serialization has the existing membership observation scratch bound.
        let Some(_scratch) = state.outgoing.control.acquire(64 * 1024) else {
            return self.install_cell_query_group(route);
        };
        let configuration = ::raft::prelude::ConfState {
            voters: birth
                .voters()
                .iter()
                .map(|voter| voter.member.get())
                .collect(),
            learners: birth
                .learners()
                .iter()
                .map(|learner| learner.member.get())
                .collect(),
            ..Default::default()
        };
        let membership = crate::raft::member_state::GroupMembership::initial(&configuration);
        let digest = crate::raft::host::group_membership_digest(&membership, &configuration)
            .map_err(|_| TransportError::Identity)?;
        self.install_cell_query_group_inner(
            route,
            Some(leader_cache::Configuration {
                revision: 1,
                digest,
                incoming: birth.voters().iter().map(|voter| voter.member).collect(),
            }),
        )
    }

    fn install_cell_query_group_inner(
        &self,
        route: GroupRoute<GroupHistory>,
        configuration: Option<leader_cache::Configuration>,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        let runtime = state.runtime.shared()?;
        match runtime.log().cell_group(&route.scope) {
            Ok(group) if group.consensus_claimed.load(Ordering::Acquire) => {
                return Err(TransportError::Identity)
            }
            Ok(_) | Err(LogError::NotFound) => (),
            Err(error) => return Err(TransportError::Runtime(error.into())),
        }
        if route.revision == 0
            || route.schema_digest == [0; 32]
            || route.peers.is_empty()
            || route.peers.len() > 128
        {
            return Err(TransportError::Identity);
        }
        let mut members = BTreeSet::new();
        let mut nodes = BTreeSet::new();
        let physical: Vec<_> = route.peers.iter().map(RaftPeerNode::from).collect();
        for (peer, endpoint) in route.peers.iter().zip(&physical) {
            endpoint.validate_endpoint()?;
            if peer.replica.scope != route.scope
                || peer.replica.node == runtime.log().limits().node
                || !members.insert(peer.replica.member)
                || !nodes.insert(peer.replica.node)
            {
                return Err(TransportError::Identity);
            }
        }
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        if let Some(configuration) = &configuration {
            registry
                .leaders
                .check_configuration(&route, configuration)?;
        }
        if registry.physical_groups.contains_key(&route.scope) {
            return Err(TransportError::Identity);
        }
        for peer in &physical {
            registry.check_node_view(peer)?;
        }
        if let Some(old) = registry.cell_query_groups.get(&route.scope) {
            if **old == route {
                let old = old.clone();
                if let Some(configuration) = configuration {
                    registry
                        .leaders
                        .install(&old, configuration, &state.outgoing.control, now)?;
                }
                return Ok(());
            }
            if old.revision >= route.revision || old.schema_digest != route.schema_digest {
                return Err(TransportError::StalePeer);
            }
            if route.peers.iter().any(|peer| {
                old.peers.iter().any(|previous| {
                    previous.replica.member == peer.replica.member
                        && (previous.replica.node != peer.replica.node
                            || previous.replica.disk != peer.replica.disk)
                })
            }) {
                return Err(TransportError::Identity);
            }
        } else if registry.route_count() >= state.options.max_groups {
            return Err(TransportError::Overloaded);
        }
        for peer in &physical {
            if registry
                .physical_groups
                .values()
                .chain(
                    registry
                        .cell_query_groups
                        .iter()
                        .filter(|(scope, _)| **scope != route.scope)
                        .map(|(_, view)| view),
                )
                .flat_map(|view| &view.peers)
                .any(|other| other.replica.node == peer.node && !peer.same_host(other))
                || registry
                    .groups
                    .values()
                    .chain(registry.query_groups.values())
                    .flat_map(|view| &view.peers)
                    .any(|other| other.replica.node == peer.node && !peer.same_host(other))
                || registry
                    .origins
                    .values()
                    .chain(registry.inspection_origins.values())
                    .filter_map(|grant| grant.peer.as_ref())
                    .any(|other| other.node == peer.node && other != peer)
            {
                return Err(TransportError::StalePeer);
            }
        }
        let starts = state.prepare_pools(&mut registry, &physical)?;
        let route = Arc::new(route);
        registry
            .cell_query_groups
            .insert(route.scope, route.clone());
        if let Some(configuration) = configuration {
            registry
                .leaders
                .install(&route, configuration, &state.outgoing.control, now)?;
        }
        registry.prune_pools();
        drop(registry);
        state.start_pools(starts)
    }

    /// An advisory scheduling sample, not an admission or revocation proof.
    /// Unchanged idle hints must not wake parked groups. Changed or unsettled
    /// rooted placement still needs a quorum observation before route refresh.
    /// Entered requests pin the route through response validation.
    pub(crate) fn cell_query_group_needs_refresh(
        &self,
        history: &GroupHistory,
        voters: Option<&BTreeMap<NodeId, MemberId>>,
    ) -> Result<bool, TransportError> {
        let state = &self.handle.0;
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        Ok(registry.cell_query_groups.get(history).is_none_or(|route| {
            Arc::strong_count(route) > 1
                || voters.is_none_or(|voters| {
                    voters.len() != route.peers.len()
                        || route.peers.iter().any(|peer| {
                            voters.get(&peer.replica.node) != Some(&peer.replica.member)
                        })
                })
        }))
    }

    /// Cache eviction cannot invalidate an entered read or write. Admission
    /// clones the route under this same registry lock; its Arc pins the entry
    /// through queueing, I/O and response validation. Explicit removal below
    /// still invalidates route identity immediately for entered attempts.
    pub(crate) fn evict_idle_cell_query_group(
        &self,
        history: &GroupHistory,
    ) -> Result<bool, TransportError> {
        let state = &self.handle.0;
        state.runtime.shared()?;
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if registry
            .cell_query_groups
            .get(history)
            .is_some_and(|route| Arc::strong_count(route) > 1)
        {
            return Ok(false);
        }
        registry.cell_query_groups.remove(history);
        registry.prune_pools();
        Ok(true)
    }

    /// Remove read-only discovery, invalidating queued/in-flight route identity
    /// and retiring peer pools that no other route or grant retains.
    pub fn remove_cell_query_group(&self, history: &GroupHistory) -> Result<(), TransportError> {
        let state = &self.handle.0;
        state.runtime.shared()?;
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        registry.cell_query_groups.remove(history);
        registry.prune_pools();
        Ok(())
    }
}

impl State {
    pub(super) fn current_cell_query_route(
        &self,
        route: &Arc<GroupRoute<GroupHistory>>,
        peer: &RaftPeer<GroupHistory>,
    ) -> bool {
        let Ok(runtime) = self.runtime.shared() else {
            return false;
        };
        match runtime.log().cell_group(&route.scope) {
            Ok(group) if group.consensus_claimed.load(Ordering::Acquire) => {
                self.current_physical(route, peer)
            }
            Ok(_) | Err(LogError::NotFound) => self.registry.lock().is_ok_and(|registry| {
                self.admit(&registry).is_ok()
                    && registry
                        .cell_query_groups
                        .get(&route.scope)
                        .is_some_and(|current| Arc::ptr_eq(current, route))
                    && registry
                        .pools
                        .get(&peer.replica.node)
                        .is_some_and(|pool| pool.peer.same_host(peer))
            }),
            Err(_) => false,
        }
    }
}
