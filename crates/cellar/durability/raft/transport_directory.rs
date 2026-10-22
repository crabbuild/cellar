//! Atomic physical process refresh, separate from committed group revisions.
use super::*;
use crate::raft::catalog::NodeView;

fn same_registration(left: &RaftPeerNode, right: &RaftPeerNode) -> bool {
    left.node == right.node
        && left.disk == right.disk
        && left.label == right.label
        && left.server_name == right.server_name
}

impl Registry {
    /// Once observed, a process view also fences later route/grant installs.
    /// Membership changes cannot reintroduce an older physical endpoint.
    pub(super) fn check_node_view(&self, peer: &RaftPeerNode) -> Result<(), TransportError> {
        if self
            .node_views
            .get(&peer.node)
            .is_some_and(|view| view.peer() != peer)
        {
            return Err(TransportError::StalePeer);
        }
        Ok(())
    }
}

impl RaftTransport {
    /// Refresh one explicitly configured physical node across all routes and
    /// permissions. This changes no group/member identity or configuration
    /// revision. A superseded local process is refused; its host must close and
    /// join the owning service before any further operation.
    pub fn accept_node_view(
        &self,
        view: NodeView,
        trusted: &RaftPeerNode,
    ) -> Result<(), TransportError> {
        let peer = view.peer();
        peer.validate_endpoint()?;
        trusted.validate_endpoint()?;
        if !same_registration(peer, trusted) {
            return Err(TransportError::Identity);
        }
        let state = &self.handle.0;
        let local = state.runtime.log().limits();
        if peer.node == local.node
            && (peer.disk != local.disk
                || peer.process != local.process
                || peer.label != state.auth.source()
                || view.storage_instance()
                    != state
                        .runtime
                        .log()
                        .storage_instance()
                        .map_err(|_| TransportError::Closed)?)
        {
            return Err(TransportError::StalePeer);
        }
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if let Some(old) = registry.node_views.get(&peer.node) {
            if old.storage_instance() != view.storage_instance()
                || !same_registration(old.peer(), peer)
            {
                return Err(TransportError::Identity);
            }
            if old.revision() > view.revision()
                || (old.revision() == view.revision() && old != &view)
            {
                return Err(TransportError::StalePeer);
            }
            if old.peer().process == peer.process && old.peer() != peer {
                return Err(TransportError::Identity);
            }
            if old.revision() == view.revision() {
                return Ok(());
            }
        } else if registry.node_views.len() > state.options.max_peers {
            return Err(TransportError::Overloaded);
        }
        // Validate every scope before replacing any pool, route or permission.
        // The physical registration is global even across different clusters.
        for old in registry
            .groups
            .values()
            .chain(registry.query_groups.values())
            .flat_map(|route| &route.peers)
            .filter(|old| old.replica.node == peer.node)
        {
            if !same_registration(&RaftPeerNode::from(old), peer) {
                return Err(TransportError::Identity);
            }
        }
        for old in registry
            .origins
            .values()
            .chain(registry.inspection_origins.values())
            .filter_map(|grant| grant.peer.as_ref())
            .filter(|old| old.node == peer.node)
        {
            if !same_registration(old, peer) {
                return Err(TransportError::Identity);
            }
        }
        if registry
            .seed_peers
            .get(&peer.node)
            .is_some_and(|old| !same_registration(&old.peer, peer))
        {
            return Err(TransportError::Identity);
        }
        let physical = if registry.pools.contains_key(&peer.node) {
            std::slice::from_ref(peer)
        } else {
            &[]
        };
        // Reservations include old workers until physical completion. A full
        // budget refuses the refresh without partially changing routing.
        let starts = state.prepare_pools(&mut registry, physical)?;
        refresh_routes(&mut registry.groups, peer);
        refresh_routes(&mut registry.query_groups, peer);
        let Registry {
            origins,
            inspection_origins,
            ..
        } = &mut *registry;
        for grant in origins
            .values_mut()
            .chain(inspection_origins.values_mut())
            .filter(|grant| {
                grant
                    .peer
                    .as_ref()
                    .is_some_and(|old| old.node == peer.node && old != peer)
            })
        {
            let (stop, _) = watch::channel(false);
            let changed = queries::OriginGrant {
                revision: grant.revision,
                peer: Some(peer.clone()),
                stop,
            };
            grant.stop.send_replace(true);
            *grant = Arc::new(changed);
        }
        if let Some(old) = registry.seed_peers.get_mut(&peer.node) {
            if old.peer != *peer {
                let (stop, _) = watch::channel(false);
                old.stop.send_replace(true);
                *old = SeedPeer {
                    peer: peer.clone(),
                    stop,
                };
            }
        }
        registry.node_views.insert(peer.node, view.clone());
        drop(registry);
        // A retained source is host-owned. Mark an old route finished so its
        // owner can release it; an active socket still retains its reservation
        // until its cancelled physical I/O exits.
        state.invalidate_snapshot_routes();
        state.start_pools(starts)
    }
}

fn refresh_routes(routes: &mut BTreeMap<GroupScope, Arc<GroupRoute>>, peer: &RaftPeerNode) {
    for route in routes.values_mut() {
        if !route
            .peers
            .iter()
            .any(|old| old.replica.node == peer.node && !peer.same_host(old))
        {
            continue;
        }
        let mut changed = (**route).clone();
        for old in changed
            .peers
            .iter_mut()
            .filter(|old| old.replica.node == peer.node)
        {
            old.replica.process = peer.process;
            old.label = peer.label.clone();
            old.server_name = peer.server_name.clone();
            old.control_address = peer.control_address;
            old.data_address = peer.data_address;
            old.query_address = peer.query_address;
        }
        *route = Arc::new(changed);
    }
}

/// Physical peer generation captured by grant/metadata work before it starts.
/// It confers no group authority. The registry retains bounded tombstones for
/// the process lifetime so a stale source hint cannot silently revive an owner.
pub(super) struct SeedPeer {
    peer: RaftPeerNode,
    stop: watch::Sender<bool>,
}

impl State {
    pub(super) fn seed_peer_epoch(
        &self,
        peer: &RaftPeerNode,
    ) -> Result<watch::Receiver<bool>, TransportError> {
        peer.validate_endpoint()?;
        let mut registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        registry.check_node_view(peer)?;
        if let Some(old) = registry.seed_peers.get(&peer.node) {
            return if old.peer == *peer {
                Ok(old.stop.subscribe())
            } else {
                Err(TransportError::StalePeer)
            };
        }
        if registry.seed_peers.len() >= self.options.max_peers {
            return Err(TransportError::Overloaded);
        }
        if registry
            .pools
            .get(&peer.node)
            .is_some_and(|pool| pool.peer != *peer)
        {
            return Err(TransportError::StalePeer);
        }
        let (stop, wait) = watch::channel(false);
        registry.seed_peers.insert(
            peer.node,
            SeedPeer {
                peer: peer.clone(),
                stop,
            },
        );
        Ok(wait)
    }

    pub(super) fn seed_process_until<'a, T: 'a>(
        &'a self,
        epoch: &watch::Receiver<bool>,
        deadline: u64,
        future: impl Future<Output = Result<T, TransportError>> + 'a,
    ) -> impl Future<Output = Result<T, TransportError>> + 'a {
        let mut epoch = epoch.clone();
        // TLS and snapshot futures already contain bounded transport state.
        // Box before constructing the guard so nested select/await frames do
        // not multiply that state on the caller's stack during node startup.
        let future = Box::pin(future);
        async move {
            if *epoch.borrow_and_update() {
                return Err(TransportError::StalePeer);
            }
            crate::asyncrt::select_biased! { "physical process revocation precedes a ready seed response";
                _=epoch.changed()=>Err(TransportError::StalePeer),
                result=self.seed_until(deadline,future)=>{
                    let value=result?;
                    if *epoch.borrow() {Err(TransportError::StalePeer)} else {Ok(value)}
                },
            }
        }
    }
}

impl State {
    pub(super) fn seed_process_io<'a, T: 'a>(
        &'a self,
        epoch: &watch::Receiver<bool>,
        deadline: u64,
        future: impl Future<Output = Result<T, TransportError>> + 'a,
    ) -> impl Future<Output = Result<T, TransportError>> + 'a {
        self.seed_process_until(epoch, deadline, self.seed_io(deadline, future))
    }
}
