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

    pub(crate) fn bind_router_refresh(
        &self,
        memory: Arc<Budget>,
    ) -> Result<mpsc::Receiver<crate::raft::group_host::RouterRefreshRequest>, RuntimeError> {
        let state = &self.handle.0;
        let mut registry = state.registry.lock().map_err(|_| RuntimeError::Closed)?;
        state.admit(&registry).map_err(|_| RuntimeError::Closed)?;
        if registry.router_refresh.is_some() {
            return Err(RuntimeError::AlreadyAttached);
        }
        let (client, receiver) =
            crate::raft::group_host::RouterRefreshClient::channel(memory, state.domain.clone())?;
        registry.router_refresh = Some(client);
        Ok(receiver)
    }

    pub(crate) async fn refresh_managed_router(
        &self,
        history: GroupHistory,
        deadline: u64,
    ) -> Result<(), RuntimeError> {
        let client = {
            let state = &self.handle.0;
            let registry = state.registry.lock().map_err(|_| RuntimeError::Closed)?;
            state.admit(&registry).map_err(|_| RuntimeError::Closed)?;
            registry
                .router_refresh
                .clone()
                .ok_or(RuntimeError::NotFound)?
        };
        client.request(history, deadline).await
    }

    /// Remove dispatch only after the exact retired source/member loses its consensus claim.
    /// Old pools/streams retain their physical admission until their own exit.
    pub(crate) fn retire_physical_group(
        &self,
        retired: &crate::raft::cell_groups::RetiredSource,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        let log = state.runtime.shared()?.log();
        retired.check(log).map_err(|_| TransportError::Refused)?;
        let history = retired.history();
        let group = log
            .cell_group(&history)
            .map_err(|_| TransportError::UnknownGroup)?;
        if group.consensus_claimed.load(Ordering::Acquire) {
            return Err(TransportError::Refused);
        }
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        group_snapshots::revoke(&mut registry, &history);
        registry.physical_groups.remove(&history);
        registry.physical_owners.remove(&history);
        registry.prune_pools();
        Ok(())
    }

    /// Install already-authorized physical membership. This creates no group,
    /// grants no serving authority and changes no committed configuration.
    pub fn install_physical_group(&self, route: PhysicalRoute) -> Result<(), TransportError> {
        self.install_physical(route, false, None).map(|_| ())
    }

    pub(crate) fn register_physical_group(
        &self,
        route: PhysicalRoute,
    ) -> Result<PhysicalRegistration, TransportError> {
        let (route, owner) = self.install_physical(route, true, None)?;
        Ok(PhysicalRegistration {
            state: Arc::downgrade(&self.handle.0),
            route,
            owner,
            checkpoint: None,
        })
    }

    pub(super) fn install_physical(
        &self,
        route: PhysicalRoute,
        exclusive: bool,
        refresh: Option<Arc<()>>,
    ) -> Result<(Arc<PhysicalRoute>, Arc<()>), TransportError> {
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
        if let Some(expected) = &refresh {
            if !registry
                .physical_owners
                .get(&route.scope)
                .is_some_and(|owner| Arc::ptr_eq(owner, expected))
            {
                return Err(TransportError::StalePeer);
            }
        }
        for peer in &route.peers {
            registry.check_node_view(&RaftPeerNode::from(peer))?;
        }
        let replaces_discovery = registry.cell_query_groups.contains_key(&route.scope);
        if let Some(old) = registry.physical_groups.get(&route.scope) {
            if exclusive {
                return Err(TransportError::Identity);
            }
            if **old == route {
                let owner = registry
                    .physical_owners
                    .get(&route.scope)
                    .ok_or(TransportError::Closed)?;
                return Ok((old.clone(), owner.clone()));
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
        } else if !replaces_discovery && registry.route_count() >= state.options.max_groups {
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
        let route = Arc::new(route);
        if refresh.is_none() {
            group_snapshots::revoke(&mut registry, &route.scope);
        }
        let owner = refresh.unwrap_or_else(|| Arc::new(()));
        // Only this fully validated local engine/membership can replace an
        // advisory route. Revoke its queued route identities under the same
        // registry lock; discovery itself never creates a physical owner.
        registry.cell_query_groups.remove(&route.scope);
        registry.physical_owners.insert(route.scope, owner.clone());
        registry.physical_groups.insert(route.scope, route.clone());
        registry.prune_pools();
        drop(registry);
        if let Err(error) = state.start_pools(starts) {
            if exclusive {
                drop(PhysicalRegistration {
                    state: Arc::downgrade(state),
                    route,
                    owner,
                    checkpoint: None,
                });
            }
            return Err(error);
        }
        Ok((route, owner))
    }

    /// Refresh only advisory managed peer routes. Committed membership supplies
    /// member/disk bindings; a current rooted node view supplies the endpoint.
    /// Missing discovery keeps known voters usable and never removes Raft debt.
    pub(crate) fn reconcile_managed_physical(
        &self,
        history: &GroupHistory,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        let runtime = state.runtime.shared()?;
        let (old, owner, has_isolated_peer) = {
            let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
            state.admit(&registry)?;
            let old = registry
                .physical_groups
                .get(history)
                .cloned()
                .ok_or(TransportError::UnknownGroup)?;
            let owner = registry
                .physical_owners
                .get(history)
                .cloned()
                .ok_or(TransportError::UnknownGroup)?;
            let has_isolated_peer = registry.fleet_trust.as_ref().is_some_and(|trust| {
                old.peers.iter().any(|peer| {
                    trust
                        .node(peer.replica.node)
                        .is_some_and(|node| node.is_isolated())
                })
            });
            (old, owner, has_isolated_peer)
        };
        let group = runtime
            .log()
            .cell_group(history)
            .map_err(|_| TransportError::UnknownGroup)?;
        // Never hold a group read guard while taking the registry lock. A queued
        // writer can otherwise deadlock against registry->group authorization.
        let (binding, members) = {
            let cache = group
                .read()
                .map_err(|e| TransportError::Runtime(e.into()))?;
            let Some(fleet) = cache.membership.fleet_status() else {
                return Ok(());
            };
            let current = crate::raft::member_state::members(&cache.conf_state);
            if !has_isolated_peer
                && current.len() == old.peers.len()
                && old.peers.iter().all(|peer| {
                    current.contains(&peer.replica.member.get())
                        && cache.membership.permits_group(&peer.replica)
                })
            {
                return Ok(());
            }
            if !current.contains(&group.identity().member.get()) {
                return Err(TransportError::StalePeer);
            }
            let members = current
                .into_iter()
                .map(|id| {
                    let member = cellar_logic::consensus::MemberId::new(id)
                        .map_err(|_| TransportError::Identity)?;
                    let joined = cache
                        .membership
                        .joined(member)
                        .map_err(|e| TransportError::Runtime(e.into()))?;
                    Ok((member, joined.map(|(binding, _)| binding)))
                })
                .collect::<Result<Vec<_>, TransportError>>()?;
            (fleet.binding, members)
        };
        let peers = {
            let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
            state.admit(&registry)?;
            if !registry
                .physical_owners
                .get(history)
                .is_some_and(|current| Arc::ptr_eq(current, &owner))
            {
                return Err(TransportError::StalePeer);
            }
            // An advisory view by itself cannot expand a managed route. Until
            // trust arrives, leave existing routing permissions unchanged.
            let Some(trust) = &registry.fleet_trust else {
                return Ok(());
            };
            if trust.fleet() != binding.fleet()
                || trust.catalog() != binding.catalog()
                || trust.cluster() != history.cluster
            {
                return Err(TransportError::Refused);
            }
            let mut peers = Vec::with_capacity(members.len());
            for (member, joined) in members {
                let prior = old.peers.iter().find(|peer| peer.replica.member == member);
                let peer = if let Some(joined) = joined {
                    let Some(view) = registry.node_views.get(&joined.node) else {
                        continue;
                    };
                    let node = view.peer();
                    if node.disk != joined.disk || registry.check_node_view(node).is_err() {
                        continue;
                    }
                    RaftPeer {
                        replica: ReplicaIdentity {
                            scope: *history,
                            member,
                            node: node.node,
                            disk: node.disk,
                            process: node.process,
                        },
                        label: node.label.clone(),
                        server_name: node.server_name.clone(),
                        control_address: node.control_address,
                        data_address: node.data_address,
                        query_address: node.query_address,
                    }
                } else {
                    // Initial member identities come from the authorized birth
                    // route, never from guessing an ID from node discovery.
                    let Some(prior) = prior else {
                        continue;
                    };
                    prior.clone()
                };
                // Only rooted permanent quarantine can omit an initial voter
                // whose retained endpoint would otherwise reject the whole
                // refreshed route, including a newly admitted learner. Keep
                // its durable membership and seed-owner/resource debt intact.
                if peer.replica.node != group.identity().node
                    && trust
                        .node(peer.replica.node)
                        .is_some_and(|node| node.is_isolated())
                {
                    continue;
                }
                peers.push(peer);
            }
            peers
        };
        if peers == old.peers {
            return Ok(());
        }
        let revision = old
            .revision
            .checked_add(1)
            .ok_or(TransportError::StalePeer)?;
        self.install_physical(
            PhysicalRoute {
                scope: *history,
                schema_digest: old.schema_digest,
                revision,
                peers,
            },
            false,
            Some(owner),
        )?;
        Ok(())
    }

    pub fn remove_physical_group(&self, history: &GroupHistory) -> Result<(), TransportError> {
        let state = &self.handle.0;
        state.runtime.shared()?;
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        group_snapshots::revoke(&mut registry, history);
        registry.physical_groups.remove(history);
        registry.physical_owners.remove(history);
        registry.prune_pools();
        Ok(())
    }

    /// Queue a physical-group frame on existing bounded per-node lanes. Delivery
    /// acknowledges receiver admission only. Snapshots use the bounded checkpoint stream.
    pub fn submit_physical(
        &self,
        history: &GroupHistory,
        message: Message,
    ) -> Result<Delivery, TransportError> {
        self.submit_physical_inner(history, message, true, None)
    }

    /// Retain the runtime's exact prior-protocol send admission through physical
    /// transport completion, independently of the Delivery response waiter.
    pub fn submit_cell_group_event(
        &self,
        event: &crate::raft::cell_groups::CellGroupRuntimeEvent,
    ) -> Result<Delivery, TransportError> {
        let crate::raft::cell_groups::CellGroupEffect::Message(message) = event
            .effect()
            .map_err(|e| TransportError::Runtime(e.clone()))?
        else {
            return Err(TransportError::Protocol);
        };
        self.submit_physical_inner(
            &event.group(),
            message.clone(),
            true,
            Some(event.delivery_admission()),
        )
    }

    fn submit_physical_inner(
        &self,
        history: &GroupHistory,
        message: Message,
        discover: bool,
        source: Option<Arc<Permit>>,
    ) -> Result<Delivery, TransportError> {
        let state = &self.handle.0;
        let runtime = state.runtime.shared()?;
        if message.get_msg_type() == MessageType::MsgSnapshot {
            self.reconcile_managed_physical(history)?;
            return self.submit_group_checkpoint(history, message, source);
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
        let peer = match route
            .peers
            .iter()
            .find(|p| p.replica.member.get() == message.to)
        {
            Some(peer) => peer.clone(),
            None if discover => {
                // Only a missing route needs discovery on ordinary traffic.
                // Retry once, before allocating/charging a serialized frame.
                drop(registry);
                self.reconcile_managed_physical(history)?;
                return self.submit_physical_inner(history, message, false, source);
            }
            None => return Err(TransportError::StalePeer),
        };
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
            _source: source,
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

/// Removing an old registration must never remove a replacement route.
pub(crate) struct PhysicalRegistration {
    state: std::sync::Weak<State>,
    route: Arc<PhysicalRoute>,
    owner: Arc<()>,
    pub(crate) checkpoint: Option<CellGroupCheckpointRegistration>,
}

impl Drop for PhysicalRegistration {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        if let Ok(mut registry) = state.registry.lock() {
            if registry
                .physical_owners
                .get(&self.route.scope)
                .is_some_and(|owner| Arc::ptr_eq(owner, &self.owner))
            {
                group_snapshots::revoke(&mut registry, &self.route.scope);
                registry.physical_groups.remove(&self.route.scope);
                registry.physical_owners.remove(&self.route.scope);
                registry.prune_pools();
            }
        };
    }
}
