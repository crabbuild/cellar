// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical group routing on the existing node transport's queues and sockets.
use super::*;
use crate::raft::cache::GroupLog;
use crate::raft::cell_groups::CellGroupRuntime;

type PhysicalRoute = GroupRoute<GroupHistory>;
type PhysicalPeer = RaftPeer<GroupHistory>;

pub(super) enum SendRoute {
    Legacy {
        scope: GroupScope,
        route: Arc<GroupRoute>,
        peer: RaftPeer,
    },
    Physical {
        route: Arc<PhysicalRoute>,
        peer: PhysicalPeer,
    },
}

pub(super) fn permitted(
    group: &GroupLog<GroupHistory>,
    peers: &[&ReplicaIdentity<GroupHistory>],
) -> Result<(), TransportError> {
    let cache = group
        .read()
        .map_err(|e| TransportError::Runtime(e.into()))?;
    let conf = &cache.conf_state;
    for peer in peers {
        if !cache.membership.permits_group(peer)
            || !conf
                .voters
                .iter()
                .chain(&conf.voters_outgoing)
                .chain(&conf.learners)
                .chain(&conf.learners_next)
                .any(|member| *member == peer.member.get())
        {
            return Err(TransportError::StalePeer);
        }
    }
    Ok(())
}

impl RaftTransport {
    pub(crate) fn uses_shared_runtime(&self, runtime: &CellGroupRuntime) -> bool {
        self.handle
            .0
            .runtime
            .shared()
            .is_ok_and(|owner| owner.same_owner(runtime))
    }

    /// Install already-authorized physical membership. This creates no group,
    /// grants no serving authority and changes no committed configuration.
    pub fn install_physical_group(&self, route: PhysicalRoute) -> Result<(), TransportError> {
        let state = &self.handle.0;
        let runtime = state.runtime.shared()?;
        let group = runtime
            .log()
            .cell_group(&route.scope)
            .map_err(|_| TransportError::UnknownGroup)?;
        if route.revision == 0 || route.peers.is_empty() || route.peers.len() > 128 {
            return Err(TransportError::Invalid("physical group route".into()));
        }
        let cache = group
            .read()
            .map_err(|e| TransportError::Runtime(e.into()))?;
        if cache
            .group_manifest
            .as_ref()
            .is_none_or(|root| root.route().schema_digest != route.schema_digest)
        {
            return Err(TransportError::Identity);
        }
        drop(cache);
        let local = group.identity();
        let mut members = BTreeSet::new();
        let mut nodes = BTreeSet::new();
        for peer in &route.peers {
            if peer.replica.scope != route.scope
                || !members.insert(peer.replica.member)
                || !nodes.insert(peer.replica.node)
            {
                return Err(TransportError::Identity);
            }
            RaftPeerNode::from(peer).validate_endpoint()?;
            permitted(&group, &[&peer.replica])?;
        }
        let own = route
            .peers
            .iter()
            .find(|p| p.replica == *local)
            .ok_or(TransportError::Identity)?;
        if own.label != state.auth.source()
            || own.control_address != state.local_control
            || own.data_address != state.local_data
            || own.query_address != state.local_query
        {
            return Err(TransportError::Identity);
        }
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        for peer in &route.peers {
            registry.check_node_view(&RaftPeerNode::from(peer))?;
        }
        if registry.cell_query_groups.contains_key(&route.scope) {
            return Err(TransportError::Identity);
        }
        if let Some(old) = registry.physical_groups.get(&route.scope) {
            if **old == route {
                return Ok(());
            }
            if old.revision >= route.revision || old.schema_digest != route.schema_digest {
                return Err(TransportError::StalePeer);
            }
            for peer in &route.peers {
                if old.peers.iter().any(|p| {
                    p.replica.member == peer.replica.member
                        && (p.replica.node != peer.replica.node
                            || p.replica.disk != peer.replica.disk)
                }) {
                    return Err(TransportError::Identity);
                }
            }
        } else if registry.route_count() >= state.options.max_groups {
            return Err(TransportError::Overloaded);
        }
        let physical: Vec<_> = route
            .peers
            .iter()
            .filter(|p| p.replica.node != local.node)
            .map(RaftPeerNode::from)
            .collect();
        for peer in &physical {
            if registry
                .physical_groups
                .values()
                .chain(registry.cell_query_groups.values())
                .filter(|view| view.scope != route.scope)
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
                    .filter_map(|g| g.peer.as_ref())
                    .any(|other| other.node == peer.node && other != peer)
            {
                return Err(TransportError::StalePeer);
            }
        }
        let starts = state.prepare_pools(&mut registry, &physical)?;
        registry
            .physical_groups
            .insert(route.scope, Arc::new(route));
        registry.prune_pools();
        drop(registry);
        state.start_pools(starts)
    }

    pub fn remove_physical_group(&self, history: &GroupHistory) -> Result<(), TransportError> {
        let state = &self.handle.0;
        state.runtime.shared()?;
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        registry.physical_groups.remove(history);
        registry.prune_pools();
        Ok(())
    }

    /// Queue a physical-group frame on existing bounded per-node lanes. Delivery
    /// acknowledges receiver admission only. Complete group snapshots require CG-05.
    pub fn submit_physical(
        &self,
        history: &GroupHistory,
        message: Message,
    ) -> Result<Delivery, TransportError> {
        let state = &self.handle.0;
        let runtime = state.runtime.shared()?;
        if message.get_msg_type() == MessageType::MsgSnapshot {
            return Err(TransportError::Refused);
        }
        let lane = frame::classify(&message)?;
        let size = message.compute_size() as usize;
        if size > frame::payload_limit(lane, &state.options) {
            return Err(TransportError::Limit);
        }
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        let route = registry
            .physical_groups
            .get(history)
            .ok_or(TransportError::UnknownGroup)?
            .clone();
        let group = runtime
            .log()
            .cell_group(history)
            .map_err(|_| TransportError::UnknownGroup)?;
        let local = group.identity().clone();
        if message.from != local.member.get() {
            return Err(TransportError::Identity);
        }
        let peer = route
            .peers
            .iter()
            .find(|p| p.replica.member.get() == message.to)
            .ok_or(TransportError::Identity)?
            .clone();
        if peer.replica.node == local.node {
            return Err(TransportError::Identity);
        }
        permitted(&group, &[&local, &peer.replica])?;
        let pool = registry
            .pools
            .get(&peer.replica.node)
            .ok_or(TransportError::StalePeer)?
            .clone();
        drop(registry);
        let charge = size + state.options.max_header_bytes + frame::PREFIX + frame::TAG;
        let node = state
            .outgoing
            .lane(lane)
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        let peer_permit = pool
            .outgoing
            .lane(lane)
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        let body = message
            .write_to_bytes()
            .map_err(|_| TransportError::Protocol)?;
        frame::scan_message(&body, state.options.max_entries)?;
        let wire = Route::new_group(
            &local,
            state.auth.source(),
            &peer.replica,
            &peer.label,
            route.schema_digest,
        )?;
        let mut header = RequestHeader::new(wire, 1, lane, &body);
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if !registry
            .physical_groups
            .get(history)
            .is_some_and(|r| Arc::ptr_eq(r, &route))
            || !registry
                .pools
                .get(&peer.replica.node)
                .is_some_and(|p| Arc::ptr_eq(p, &pool))
        {
            return Err(TransportError::StalePeer);
        }
        let sequence = state
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| TransportError::Closed)?
            + 1;
        header.set_sequence(sequence);
        let (result, wait) = oneshot::channel();
        let request = SendRequest {
            member: peer.replica.member,
            route: SendRoute::Physical { route, peer },
            header,
            body,
            snapshot: false,
            result: Some(result),
            queued: Some(state.performance.timer(match lane {
                Lane::Control => Operation::ControlQueue,
                Lane::Data => Operation::DataQueue,
            })),
            _node: node,
            _peer: peer_permit,
        };
        pool.tx[lane_index(lane)].try_send(request).map_err(|e| {
            let error = match &e {
                mpsc::error::TrySendError::Full(_) => TransportError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
            };
            if let Some(timer) = e.into_inner().queued.take() {
                timer.finish(false);
            }
            error
        })?;
        Ok(Delivery { result: wait })
    }
}

impl State {
    pub(super) fn current_physical(&self, route: &Arc<PhysicalRoute>, peer: &PhysicalPeer) -> bool {
        self.registry.lock().is_ok_and(|r| {
            self.admit(&r).is_ok()
                && r.physical_groups
                    .get(&route.scope)
                    .is_some_and(|current| Arc::ptr_eq(current, route))
                && r.pools
                    .get(&peer.replica.node)
                    .is_some_and(|pool| pool.peer.same_host(peer))
                && self.runtime.shared().is_ok_and(|runtime| {
                    runtime.log().cell_group(&route.scope).is_ok_and(|group| {
                        permitted(&group, &[group.identity(), &peer.replica]).is_ok()
                    })
                })
        })
    }

    pub(super) fn authorize_physical(&self, wire: &Route) -> Result<Arc<Pool>, TransportError> {
        let runtime = self.runtime.shared()?;
        let (source, target) = wire.decode_group()?;
        let registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let route = registry
            .physical_groups
            .get(&target.scope)
            .ok_or(TransportError::UnknownGroup)?;
        let group = runtime
            .log()
            .cell_group(&target.scope)
            .map_err(|_| TransportError::UnknownGroup)?;
        if *group.identity() != target
            || wire.target_label() != self.auth.source()
            || wire.schema() != route.schema_digest
        {
            return Err(TransportError::StalePeer);
        }
        let peer = route
            .peers
            .iter()
            .find(|p| p.replica == source && p.label == wire.source_label())
            .ok_or(TransportError::StalePeer)?;
        permitted(&group, &[&source, &target])?;
        registry
            .pools
            .get(&source.node)
            .filter(|pool| pool.peer.same_host(peer))
            .cloned()
            .ok_or(TransportError::StalePeer)
    }
}

impl Registry {
    pub(super) fn check_physical_endpoint(
        &self,
        peer: &RaftPeerNode,
    ) -> Result<(), TransportError> {
        if self
            .command_origins
            .values()
            .filter_map(|grant| grant.peer.as_ref())
            .any(|old| old.node == peer.node && old != peer)
        {
            return Err(TransportError::StalePeer);
        }
        if self
            .physical_groups
            .values()
            .chain(self.cell_query_groups.values())
            .flat_map(|route| &route.peers)
            .any(|other| other.replica.node == peer.node && !peer.same_host(other))
        {
            return Err(TransportError::StalePeer);
        }
        Ok(())
    }
}
