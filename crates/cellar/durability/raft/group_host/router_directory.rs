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
    digest: Option<[u8; 32]>,
    cursor: Option<CellGroupDirectoryCursor>,
    next_node: usize,
}

pub(super) struct RouterDirectory {
    namespaces: Vec<NamespaceWork>,
    next_namespace: usize,
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
                digest: None,
                cursor: None,
                next_node: 0,
            })
            .collect::<Vec<_>>();
        (!namespaces.is_empty()).then_some(Self {
            namespaces,
            next_namespace: 0,
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
        let remaining = || super::directory::remaining(log, deadline);
        let index = self.next_namespace;
        self.next_namespace = (index + 1) % self.namespaces.len();
        let namespace = &mut self.namespaces[index];
        // One immutable page per namespace turn; topology changes restart its
        // cursor. Declaring a namespace permits discovery, never application I/O.
        let view = namespace.directory.observe().await?;
        if namespace.digest != Some(view.digest()) {
            namespace.cursor = None;
        }
        let page = namespace
            .directory
            .group_page(&view, namespace.cursor.as_ref(), 1)
            .await?;
        namespace.digest = Some(view.digest());
        namespace.cursor = page.next;
        let Some(route) = page.routes.into_iter().next() else {
            return Ok(());
        };
        let group = route.history;
        let nodes = trust.nodes();
        let local = log.limits().node;
        // Use the same rotating candidate throughout one namespace sweep.
        // Advancing per group can alias group and voter counts and never sample
        // an owner of a particular group.
        let source = (0..nodes.len()).find_map(|offset| {
            let node = &nodes[(namespace.next_node + offset) % nodes.len()];
            (node.node() != local && node.permits_transport()).then_some(node.node())
        });
        if namespace.cursor.is_none() && !nodes.is_empty() {
            namespace.next_node = (namespace.next_node + 1) % nodes.len();
        }
        match log.cell_group(&group) {
            Ok(_) => return Ok(()),
            Err(crate::raft::storage::LogError::NotFound) => (),
            Err(error) => return Err(error.into()),
        }
        let Some(source) = source else {
            return Ok(());
        };
        // Sample one physical node and group per turn. The rotating fleet scan
        // can recover even if this router was offline through all replacements.
        let Some(authority) = reader
            .authorize_router_group(trust, &group, remaining()?)
            .await?
        else {
            return Ok(());
        };
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
        if let Ok(local) = log.cell_group(&history) {
            anyhow::ensure!(
                local
                    .consensus_claimed
                    .load(std::sync::atomic::Ordering::Acquire)
                    && local
                        .group_manifest()?
                        .as_ref()
                        .is_some_and(|marker| marker.route() == &route
                            && marker.is_active()
                            && marker.source_seal().is_none()),
                "selected local router target is not attached"
            );
            return Ok(());
        }
        let authority = reader
            .authorize_router_group(trust, &history, remaining()?)
            .await?
            .ok_or_else(|| anyhow::anyhow!("router target is not live at this Fleet root"))?;
        let revision = trust.revision();
        for voter in birth.voters() {
            let Some(host) = trust
                .node(voter.node)
                .filter(|host| host.permits_transport())
            else {
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
                    voter.node,
                    authority.clone(),
                    revision,
                    remaining()?.min(Duration::from_millis(500)),
                )
                .await;
            let Ok(proof) = proof else {
                continue;
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
        // Share the transport's explicit route cap. Evict only this worker's
        // advisory entries; admitted physical attempts keep their own owners.
        if let Err(error) = transport.install_cell_query_group(route.clone()) {
            if error != crate::raft::transport::TransportError::Overloaded {
                return Err(error.into());
            }
            let Some(oldest) = self.installed.pop_front() else {
                return Err(error.into());
            };
            transport.remove_cell_query_group(&oldest)?;
            transport.install_cell_query_group(route)?;
        }
        self.installed.retain(|history| *history != group);
        self.installed.push_back(group);
        while self.installed.len() > self.max_installed {
            if let Some(oldest) = self.installed.pop_front() {
                transport.remove_cell_query_group(&oldest)?;
            }
        }
        Ok(true)
    }
}
