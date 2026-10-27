//! A rooted fleet cut fences every physical endpoint before advisory discovery.
//! It never changes committed membership or treats placement exclusion as revoke.
use super::*;
use crate::raft::fleet_catalog::{
    FleetNodeState, FleetPermitAuthorization, FleetReplicaSeparation, FleetTrustSnapshot,
};
use crate::raft::member_state::JoiningMember;

impl Registry {
    pub(super) fn check_fleet_peer(&self, peer: &RaftPeerNode) -> Result<(), TransportError> {
        if self.fleet_trust.as_ref().is_some_and(|trust| {
            trust
                .node(peer.node)
                .is_none_or(|node| !node.matches_peer(peer))
        }) {
            return Err(TransportError::Refused);
        }
        Ok(())
    }
}

impl RaftTransport {
    /// Routing may omit a permanently quarantined member. Its Raft vote and
    /// physical placement still belong to the durable configuration until a
    /// quorum commits removal. Missing discovery alone is never sufficient.
    pub(crate) fn node_isolated(&self, node: NodeId) -> Result<bool, TransportError> {
        let registry = self
            .handle
            .0
            .registry
            .lock()
            .map_err(|_| TransportError::Closed)?;
        Ok(registry
            .fleet_trust
            .as_ref()
            .and_then(|trust| trust.node(node))
            .is_some_and(|node| node.is_isolated()))
    }

    /// Check a rooted admission against current trust and actual durable
    /// configuration. Exclusion may continue the exact reserved destination
    /// boot; it cannot substitute another process or admit fresh placement.
    pub(crate) fn check_fleet_placement(
        &self,
        scope: &GroupScope,
        members: &[NodeId],
        target: &JoiningMember,
        authorization: &FleetPermitAuthorization,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        let trust = registry
            .fleet_trust
            .as_ref()
            .ok_or(TransportError::Refused)?;
        if trust.revision() < authorization.minimum_trust_revision() {
            return Err(TransportError::StalePeer);
        }
        if scope.cluster != trust.cluster() {
            return Err(TransportError::Refused);
        }
        let destination = trust.node(target.node).ok_or(TransportError::Refused)?;
        let original_boot = authorization.target_boot().ok_or(TransportError::Refused)?;
        if authorization
            .permit()
            .replacement()
            .map_err(|_| TransportError::Identity)?
            != *target
        {
            return Err(TransportError::Identity);
        }
        let continuing = matches!(
            destination.state(),
            FleetNodeState::Draining | FleetNodeState::RestartPending
        );
        if !destination.permits_transport()
            || (!destination.eligibility().receive_replicas && !continuing)
            || destination.boot_binding().ok().as_ref() != Some(original_boot)
            || !authorization.matches_scope(scope)
            || destination.storage_instance().is_none()
        {
            return Err(TransportError::StalePeer);
        }
        if destination.disk() != target.disk {
            return Err(TransportError::Identity);
        }
        let mut nodes = BTreeSet::new();
        let mut hosts = BTreeSet::new();
        for node in members.iter().copied().chain(std::iter::once(target.node)) {
            let enrolled = trust.node(node).ok_or(TransportError::Refused)?;
            if !nodes.insert(node)
                || trust.replica_separation() == FleetReplicaSeparation::PhysicalHosts
                    && !hosts.insert(enrolled.physical_host())
            {
                return Err(TransportError::Identity);
            }
        }
        Ok(())
    }

    /// Install one complete current-root cut atomically. Revoked task owners
    /// retain physical reservations until they exit through their stop signals.
    pub(crate) fn install_fleet_trust(
        &self,
        trust: FleetTrustSnapshot,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        let local = state.runtime.log().limits();
        let own = trust.node(local.node).ok_or(TransportError::Refused)?;
        if !own.permits_transport()
            || own.disk() != local.disk
            || own.label() != state.auth.source()
            || own.storage_instance()
                != Some(
                    state
                        .runtime
                        .log()
                        .storage_instance()
                        .map_err(|_| TransportError::Closed)?,
                )
        {
            return Err(TransportError::StalePeer);
        }
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if let Some(previous) = &registry.fleet_trust {
            trust
                .check_successor(previous)
                .map_err(|_| TransportError::StalePeer)?;
            if trust.revision() == previous.revision() {
                return Ok(());
            }
        }
        let allowed = |peer: &RaftPeerNode| {
            trust
                .node(peer.node)
                .is_some_and(|node| node.matches_peer(peer))
        };
        for view in registry.node_views.values() {
            if allowed(view.peer())
                && trust.node(view.peer().node).is_some_and(|node| {
                    node.cluster() != view.cluster()
                        || node.storage_instance() != Some(view.storage_instance())
                })
            {
                return Err(TransportError::Identity);
            }
        }
        registry.pools.retain(|_, pool| {
            if allowed(&pool.peer) {
                true
            } else {
                pool.stop.send_replace(true);
                false
            }
        });
        for peer in registry.seed_peers.values() {
            if !allowed(&peer.peer) {
                peer.stop.send_replace(true);
            }
        }
        let Registry {
            origins,
            inspection_origins,
            ..
        } = &mut *registry;
        for grant in origins.values_mut().chain(inspection_origins.values_mut()) {
            if grant.peer.as_ref().is_some_and(|peer| !allowed(peer)) {
                grant.stop.send_replace(true);
                // A boot supersession cancels this physical owner, but the
                // logical grant still belongs to the same enrolled node. Keep
                // its descriptor so a verified newer view can refresh it.
                // Removed or changed registrations retain the explicit revoke.
                if grant.peer.as_ref().is_some_and(|peer| {
                    trust
                        .node(peer.node)
                        .is_some_and(|node| node.matches_registration(peer))
                }) {
                    continue;
                }
                let (stop, _) = watch::channel(false);
                *grant = Arc::new(queries::OriginGrant {
                    revision: grant.revision,
                    peer: None,
                    stop,
                });
            }
        }
        registry.fleet_trust = Some(Arc::new(trust));
        if !state.inspection_enabled.swap(true, Ordering::AcqRel) {
            state.control_connections.add_permits(inspections::SLOTS);
        }
        drop(registry);
        state.invalidate_snapshot_routes();
        Ok(())
    }
}
