// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed router membership discovery uses the existing joined directory owner.
use super::*;
use crate::raft::{
    fleet_catalog::{FleetTrustReader, FleetTrustSnapshot},
    storage::ReplicaLog,
    transport::{GroupRoute, RaftPeer, RaftTransport},
};
use cellar_logic::consensus::ReplicaIdentity;
use std::{collections::VecDeque, time::Duration};

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
        let observed = proof.membership();
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
            return Ok(());
        }
        anyhow::ensure!(
            observed.schema_digest() == route.schema_digest,
            "router schema changed"
        );
        remaining()?;
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
        Ok(())
    }
}
