// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed router membership discovery uses the existing joined directory owner.
use super::*;
use crate::raft::{
    fleet_catalog::{FleetGroupAuthorization, FleetTrustReader, FleetTrustSnapshot},
    storage::ReplicaLog,
    transport::{GroupRoute, RaftPeer, RaftTransport},
};
use cellar_logic::consensus::ReplicaIdentity;
use std::{collections::VecDeque, time::Duration};

pub(super) struct TargetRefresh<'a> {
    pub(super) history: GroupHistory,
    pub(super) deadline: u64,
    pub(super) registrations: &'a [crate::raft::catalog::NodeRegistration],
}

struct RouterObservation<'a> {
    route: &'a cellar_logic::cell_group::GroupRoute,
    authority: &'a FleetGroupAuthorization,
    observed: &'a crate::raft::host::FleetMembershipObservation,
    revision: u64,
    deadline: u64,
}

struct NamespaceWork {
    directory: RangeDirectory,
    next_node: usize,
}

pub(super) struct RouterDirectory {
    namespaces: Vec<NamespaceWork>,
    next_history: usize,
    installed: VecDeque<GroupHistory>,
    max_installed: usize,
}

impl RouterDirectory {
    pub(super) fn new(
        directories: impl Iterator<Item = RangeDirectory>,
        max_installed: usize,
    ) -> Option<Self> {
        let namespaces = directories
            .map(|directory| NamespaceWork {
                directory,
                next_node: 0,
            })
            .collect::<Vec<_>>();
        (!namespaces.is_empty()).then_some(Self {
            namespaces,
            next_history: 0,
            installed: VecDeque::new(),
            max_installed,
        })
    }

    pub(super) async fn refresh(
        &mut self,
        log: &ReplicaLog,
        transport: &RaftTransport,
        observer: &RaftCells,
        reader: &FleetTrustReader,
        trust: &FleetTrustSnapshot,
        deadline: u64,
    ) -> anyhow::Result<()> {
        // Foreground discovery records the histories this node actually uses.
        // Refresh one such entry per turn; an idle namespace declaration must
        // not broadcast inspections of every group or churn the bounded cache.
        if self.installed.is_empty() {
            return Ok(());
        }
        self.next_history %= self.installed.len();
        let group = self.installed[self.next_history];
        self.next_history = (self.next_history + 1) % self.installed.len();
        match log.cell_group(&group) {
            Ok(local)
                if local
                    .consensus_claimed
                    .load(std::sync::atomic::Ordering::Acquire) =>
            {
                return Ok(())
            }
            Ok(_) | Err(crate::raft::storage::LogError::NotFound) => (),
            Err(error) => return Err(error.into()),
        }
        // Cached destinations are hints. Polling their quorum while no
        // request uses them would immediately undo all-voter hibernation.
        // Foreground misses/failures still use refresh_target, and the node
        // directory independently refreshes trust, endpoints and revocation.
        if !transport.cell_query_group_in_use(&group)? {
            return Ok(());
        }
        let remaining = || super::directory::remaining(log, deadline);
        let namespace = self
            .namespaces
            .iter_mut()
            .find(|namespace| {
                let config = namespace.directory.namespace_config();
                config.cluster == group.cluster && config.namespace == group.namespace
            })
            .ok_or_else(|| anyhow::anyhow!("cached router namespace capability"))?;
        let selected = namespace
            .directory
            .materialize_selected_history(group)
            .await?;
        let route = selected.route().clone();
        let ordinal = namespace.next_node;
        if !trust.nodes().is_empty() {
            namespace.next_node = (ordinal + 1) % trust.nodes().len();
        }
        let Some(discovery) = reader
            .authorize_router_group(trust, &group, remaining()?)
            .await?
        else {
            return Ok(());
        };
        let local = log.limits().node;
        let source = if let Some(nodes) = &discovery.nodes {
            (0..nodes.len()).find_map(|offset| {
                let node = nodes[(ordinal + offset) % nodes.len()];
                (node != local
                    && trust
                        .node(node)
                        .is_some_and(|host| host.permits_transport()))
                .then_some(node)
            })
        } else {
            // An unsettled/untracked configuration still has the original
            // bounded fleet fallback; a hint never invents a current owner.
            let nodes = trust.nodes();
            (0..nodes.len()).find_map(|offset| {
                let host = &nodes[(ordinal + offset) % nodes.len()];
                (host.node() != local && host.permits_transport()).then_some(host.node())
            })
        };
        let Some(source) = source else {
            return Ok(());
        };
        let authority = discovery.authorization;
        let revision = trust.revision();
        let proof = observer
            .observe_peer_fleet_membership(
                source,
                authority.clone(),
                revision,
                remaining()?.min(Duration::from_millis(500)),
            )
            .await?;
        self.install_observed(
            log,
            transport,
            RouterObservation {
                route: &route,
                authority: &authority,
                observed: proof.membership(),
                revision,
                deadline,
            },
        )
        .map(|_| ())
    }

    pub(super) async fn refresh_target(
        &mut self,
        log: &ReplicaLog,
        transport: &RaftTransport,
        observer: &RaftCells,
        reader: &FleetTrustReader,
        trust: &FleetTrustSnapshot,
        target: TargetRefresh<'_>,
    ) -> anyhow::Result<()> {
        let TargetRefresh {
            history,
            deadline,
            registrations,
        } = target;
        let remaining = || super::directory::remaining(log, deadline);
        let namespace = self
            .namespaces
            .iter()
            .find(|namespace| {
                let config = namespace.directory.namespace_config();
                config.cluster == history.cluster && config.namespace == history.namespace
            })
            .ok_or_else(|| anyhow::anyhow!("router namespace capability"))?;
        let birth = namespace
            .directory
            .materialize_selected_history(history)
            .await?;
        let route = birth.route().clone();
        if let Some(local) = log.cell_group(&history).ok().filter(|local| {
            local
                .consensus_claimed
                .load(std::sync::atomic::Ordering::Acquire)
        }) {
            anyhow::ensure!(
                local
                    .group_manifest()?
                    .as_ref()
                    .is_some_and(|marker| marker.route() == &route
                        && marker.is_active()
                        && marker.source_seal().is_none()),
                "selected local router target is not attached"
            );
            return Ok(());
        }
        let discovery = reader
            .authorize_router_group(trust, &history, remaining()?)
            .await?
            .ok_or_else(|| anyhow::anyhow!("router target is not live at this Fleet root"))?;
        let revision = trust.revision();
        let authority = discovery.authorization;
        let nodes = discovery
            .nodes
            .unwrap_or_else(|| birth.voters().iter().map(|v| v.node).collect());
        for node in nodes {
            let Some(host) = trust.node(node).filter(|host| host.permits_transport()) else {
                continue;
            };
            // Refresh this exact enrolled endpoint; discovery grants no namespace
            // origin permission. Never accept a caller-provided address.
            for registration in registrations
                .iter()
                .filter(|registration| registration.cluster() == history.cluster)
            {
                if let Some(view) = registration.observe_fleet_node(host, remaining()?).await? {
                    let peer = view.peer().clone();
                    transport.accept_node_view(view, &peer)?;
                }
            }
            let proof = observer
                .observe_peer_fleet_membership(
                    node,
                    authority.clone(),
                    revision,
                    remaining()?.min(Duration::from_millis(500)),
                )
                .await;
            let proof = match proof {
                Ok(proof) => proof,
                Err(error) => {
                    tracing::debug!(target: "cellar::raft::group_host::directory", ?history, ?node, revision, ?error, "targeted router voter observation unavailable");
                    continue;
                }
            };
            if self.install_observed(
                log,
                transport,
                RouterObservation {
                    route: &route,
                    authority: &authority,
                    observed: proof.membership(),
                    revision,
                    deadline,
                },
            )? {
                return Ok(());
            }
        }
        anyhow::bail!("no ready router target quorum observed")
    }

    fn install_observed(
        &mut self,
        log: &ReplicaLog,
        transport: &RaftTransport,
        sample: RouterObservation<'_>,
    ) -> anyhow::Result<bool> {
        let RouterObservation {
            route,
            authority,
            observed,
            revision,
            deadline,
        } = sample;
        let group = route.history;
        let local = log.limits().node;
        let mut peers = Vec::new();
        for voter in authority.router_voters(observed)? {
            let peer = transport.managed_group_peer(&group, voter.node, revision)?;
            anyhow::ensure!(
                voter.joined_disk.is_none_or(|disk| peer.disk == disk),
                "router member disk changed"
            );
            peers.push(RaftPeer {
                replica: ReplicaIdentity {
                    scope: group,
                    member: voter.member,
                    node: voter.node,
                    disk: peer.disk,
                    process: peer.process,
                },
                label: peer.label,
                server_name: peer.server_name,
                control_address: peer.control_address,
                data_address: peer.data_address,
                query_address: peer.query_address,
            });
        }
        // A selected local destination must attach its own engine separately.
        if peers.iter().any(|peer| peer.replica.node == local) {
            return Ok(false);
        }
        anyhow::ensure!(
            observed.schema_digest() == route.schema_digest,
            "router schema changed"
        );
        super::directory::remaining(log, deadline)?;
        let route = GroupRoute {
            scope: group,
            revision: observed.membership_revision().get(),
            schema_digest: observed.schema_digest(),
            peers,
        };
        // Share the transport's explicit route cap. A cache turn cannot revoke
        // an admitted read/write attempt; defer when every candidate is held.
        if !self.installed.contains(&group) && self.installed.len() >= self.max_installed {
            self.evict_idle(transport)?;
        }
        if let Err(error) = transport.install_cell_query_group(route.clone()) {
            if error != crate::raft::transport::TransportError::Overloaded {
                return Err(error.into());
            }
            self.evict_idle(transport)?;
            transport.install_cell_query_group(route)?;
        }
        self.installed.retain(|history| *history != group);
        self.installed.push_back(group);
        Ok(true)
    }

    fn evict_idle(&mut self, transport: &RaftTransport) -> anyhow::Result<()> {
        for _ in 0..self.installed.len() {
            if let Some(oldest) = self.installed.pop_front() {
                if transport.evict_idle_cell_query_group(&oldest)? {
                    return Ok(());
                }
                self.installed.push_back(oldest);
            }
        }
        Err(crate::raft::transport::TransportError::Overloaded.into())
    }
}
