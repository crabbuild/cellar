// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit write grants remain independent from query and voting permission.
use super::*;
use crate::raft::transport_options::NamespaceCommandOrigin;

impl RaftTransport {
    /// Install trusted namespace command permission. It cannot create a group or
    /// authorize a consensus message. Revisions include revocation tombstones.
    pub fn grant_command_origin(
        &self,
        grant: NamespaceCommandOrigin,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        grant.peer.validate_endpoint()?;
        if grant.revision == 0 || grant.peer.node == state.runtime.log().limits().node {
            return Err(TransportError::Identity);
        }
        let key = (grant.cluster, grant.namespace, grant.peer.node);
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        registry.check_node_view(&grant.peer)?;
        registry.check_physical_endpoint(&grant.peer)?;
        if let Some(old) = registry.command_origins.get(&key) {
            if old.revision == grant.revision && old.peer.as_ref() == Some(&grant.peer) {
                return Ok(());
            }
            if old.revision >= grant.revision {
                return Err(TransportError::StalePeer);
            }
        } else if registry.command_origins.len() >= state.options.max_command_origins {
            return Err(TransportError::Overloaded);
        }
        if registry
            .groups
            .values()
            .chain(registry.query_groups.values())
            .flat_map(|route| &route.peers)
            .any(|peer| peer.replica.node == grant.peer.node && !grant.peer.same_host(peer))
            || registry
                .command_origins
                .iter()
                .filter(|(other, _)| **other != key)
                .filter_map(|(_, old)| old.peer.as_ref())
                .any(|peer| peer.node == grant.peer.node && *peer != grant.peer)
        {
            return Err(TransportError::StalePeer);
        }
        if registry
            .origins
            .values()
            .filter_map(|grant| grant.peer.as_ref())
            .any(|old| old.node == grant.peer.node && old != &grant.peer)
        {
            return Err(TransportError::StalePeer);
        }
        if registry
            .inspection_origins
            .values()
            .filter_map(|origin| origin.peer.as_ref())
            .any(|peer| peer.node == grant.peer.node && peer != &grant.peer)
        {
            return Err(TransportError::StalePeer);
        }
        let starts = state.prepare_pools(&mut registry, std::slice::from_ref(&grant.peer))?;
        let (stop, _) = watch::channel(false);
        if let Some(old) = registry.command_origins.insert(
            key,
            Arc::new(queries::OriginGrant {
                revision: grant.revision,
                peer: Some(grant.peer),
                stop,
            }),
        ) {
            old.stop.send_replace(true);
        }
        registry.prune_pools();
        drop(registry);
        state.start_pools(starts)
    }

    pub fn revoke_command_origin(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
        node: NodeId,
        revision: u64,
    ) -> Result<(), TransportError> {
        if revision == 0 {
            return Err(TransportError::Identity);
        }
        let state = &self.handle.0;
        let key = (cluster, namespace, node);
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if let Some(old) = registry.command_origins.get(&key) {
            if old.revision == revision && old.peer.is_none() {
                return Ok(());
            }
            if old.revision >= revision {
                return Err(TransportError::StalePeer);
            }
        } else if registry.command_origins.len() >= state.options.max_command_origins {
            return Err(TransportError::Overloaded);
        }
        let (stop, _) = watch::channel(true);
        if let Some(old) = registry.command_origins.insert(
            key,
            Arc::new(queries::OriginGrant {
                revision,
                peer: None,
                stop,
            }),
        ) {
            old.stop.send_replace(true);
        }
        registry.prune_pools();
        Ok(())
    }
}
