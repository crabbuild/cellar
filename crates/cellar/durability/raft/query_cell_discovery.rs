// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read-only physical discovery; no local RawNode, membership or write authority.
use super::*;
use crate::raft::storage::LogError;

impl RaftTransport {
    /// Install trusted read destinations for a physical group absent from this
    /// node. The caller supplies discovery; each destination independently checks
    /// namespace permission, live membership and the requested cell read proof.
    /// Shares node route/peer bounds with consensus and all other query routes.
    pub fn install_cell_query_group(
        &self,
        route: GroupRoute<GroupHistory>,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        let runtime = state.runtime.shared()?;
        if !matches!(
            runtime.log().cell_group(&route.scope),
            Err(LogError::NotFound)
        ) {
            return Err(TransportError::Identity);
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
        if registry.physical_groups.contains_key(&route.scope) {
            return Err(TransportError::Identity);
        }
        for peer in &physical {
            registry.check_node_view(peer)?;
        }
        if let Some(old) = registry.cell_query_groups.get(&route.scope) {
            if **old == route {
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
        registry
            .cell_query_groups
            .insert(route.scope, Arc::new(route));
        registry.prune_pools();
        drop(registry);
        state.start_pools(starts)
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
            Ok(_) => self.current_physical(route, peer),
            Err(LogError::NotFound) => self.registry.lock().is_ok_and(|registry| {
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
