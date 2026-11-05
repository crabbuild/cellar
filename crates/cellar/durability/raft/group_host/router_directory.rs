// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed router membership discovery uses the existing joined directory owner.
use super::*;
use crate::raft::{
    fleet_catalog::{FleetTrustReader, FleetTrustSnapshot},
    storage::ReplicaLog,
    transport::{GroupRoute, RaftPeer, RaftTransport},
};
use cellar_logic::consensus::ReplicaIdentity;
use std::time::Duration;

struct Work {
    group: GroupHistory,
    next_node: usize,
}

pub(super) struct RouterDirectory {
    work: Vec<Work>,
    next_group: usize,
}

impl RouterDirectory {
    pub(super) fn new(groups: impl Iterator<Item = GroupHistory>) -> Option<Self> {
        let work = groups
            .map(|group| Work {
                group,
                next_node: 0,
            })
            .collect::<Vec<_>>();
        (!work.is_empty()).then_some(Self {
            work,
            next_group: 0,
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
        let nodes = trust.nodes();
        let local = log.limits().node;
        let index = self.next_group;
        self.next_group = (index + 1) % self.work.len();
        let work = &mut self.work[index];
        let source = (0..nodes.len()).find_map(|_| {
            let node = &nodes[work.next_node % nodes.len()];
            work.next_node = (work.next_node + 1) % nodes.len();
            (node.node() != local && node.permits_transport()).then_some(node.node())
        });
        let Some(source) = source else {
            return Ok(());
        };
        // Sample one physical node and group per turn. The rotating fleet scan
        // can recover even if this router was offline through all replacements.
        let Some(authority) = reader
            .authorize_router_group(trust, &work.group, remaining()?)
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
            let peer = transport.managed_group_peer(&work.group, voter.node, revision)?;
            anyhow::ensure!(
                voter.joined_disk.is_none_or(|disk| peer.disk == disk),
                "router member disk changed"
            );
            peers.push(RaftPeer {
                replica: ReplicaIdentity {
                    scope: work.group,
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
        remaining()?;
        transport.install_cell_query_group(GroupRoute {
            scope: work.group,
            revision: observed.membership_revision().get(),
            schema_digest: observed.schema_digest(),
            peers,
        })?;
        Ok(())
    }
}
