//! Bounded observations. Dropping a probe waiter cancels its independently
//! owned transport task; decoded values move under the controller reservation.
use super::*;
use crate::raft::completion::ReplicationStatus;
use crate::raft::inspection::ReplicaOccupancy;

pub(super) struct Probe {
    pub peer: RaftPeerNode,
    pub started: u64,
    pub result: Result<Box<InspectionSnapshot>, InspectionError>,
}

pub(in crate::raft::host) struct Observed {
    pub(super) started: u64,
    pub(super) local: Box<InspectionSnapshot>,
    pub(super) peers: Vec<Probe>,
    pub(super) progress: Option<Result<ReplicationStatus, RuntimeError>>,
    _memory: Permit,
}

pub(super) struct Work {
    pub local: RaftCellHandle,
    pub transport: RaftTransport,
    pub runtime: RaftRuntime,
    pub domain: crate::asyncrt::DomainToken,
    pub options: RaftControllerOptions,
    pub targets: Vec<RaftPeerNode>,
    pub memory: Permit,
}

/// Current bindings are mandatory. A bounded rotating candidate set avoids an
/// all-node fan-out. A dormant source pulls only one configured peer at a time.
pub(super) fn targets(
    scope: &GroupScope,
    cells: &BTreeMap<GroupScope, Cell>,
    peers: &BTreeMap<NodeId, RaftPeerNode>,
    local: NodeId,
    cursor: Option<NodeId>,
    limit: usize,
) -> Result<(Vec<RaftPeerNode>, Option<NodeId>), CommandFailure> {
    let mut result = BTreeMap::new();
    if let Some(cell) = cells.get(scope) {
        return bound_nodes(cell, peers, local, cursor, limit);
    }
    append_candidates(peers, local, cursor, 1, &mut result);
    let next = result.keys().next_back().copied().or(cursor);
    Ok((result.into_values().collect(), next))
}

fn bound_nodes(
    cell: &Cell,
    peers: &BTreeMap<NodeId, RaftPeerNode>,
    local: NodeId,
    cursor: Option<NodeId>,
    limit: usize,
) -> Result<(Vec<RaftPeerNode>, Option<NodeId>), CommandFailure> {
    let (ids, membership) = {
        let cache = cell
            .group
            .read()
            .map_err(|_| CommandFailure::CorruptState)?;
        (
            super::super::super::member_state::members(&cache.conf_state),
            cache.membership.clone(),
        )
    };
    let mut result = BTreeMap::new();
    for id in ids {
        let member = MemberId::new(id).map_err(|_| CommandFailure::CorruptState)?;
        let joined = membership
            .joined(member)
            .map_err(|_| CommandFailure::CorruptState)?;
        let node = if let Some((joined, _)) = joined.as_ref() {
            joined.node
        } else {
            cell.birth
                .voters()
                .iter()
                .chain(cell.birth.learners())
                .find(|initial| initial.member == member)
                .ok_or(CommandFailure::CorruptState)?
                .node
        };
        if node == local {
            continue;
        }
        let peer = peers.get(&node).ok_or(CommandFailure::Unavailable)?;
        if joined.is_some_and(|(binding, _)| binding.disk != peer.disk) {
            return Err(CommandFailure::CorruptState);
        }
        result.insert(node, peer.clone());
    }
    if result.len() >= limit {
        return Err(CommandFailure::Overloaded);
    }
    let next = append_candidates(peers, local, cursor, limit - 1, &mut result).or(cursor);
    Ok((result.into_values().collect(), next))
}

fn append_candidates(
    peers: &BTreeMap<NodeId, RaftPeerNode>,
    local: NodeId,
    cursor: Option<NodeId>,
    limit: usize,
    result: &mut BTreeMap<NodeId, RaftPeerNode>,
) -> Option<NodeId> {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let mut next = None;
    let after = cursor.map_or(Unbounded, Excluded);
    let through = cursor.map_or(Unbounded, Included);
    for (node, peer) in peers
        .range((after, Unbounded))
        .chain(peers.range((Unbounded, through)))
    {
        if result.len() >= limit {
            break;
        }
        next = Some(*node);
        if *node != local {
            result.entry(*node).or_insert_with(|| peer.clone());
        }
    }
    next
}

impl Work {
    pub(super) async fn run(self) -> Result<Observed, CommandFailure> {
        let started = self
            .domain
            .mono_ms()
            .map_err(|_| CommandFailure::Unavailable)?;
        let deadline = started
            .checked_add(self.options.operation_timeout_ms)
            .ok_or(CommandFailure::Unavailable)?;
        let timer = self
            .domain
            .sleep_until(deadline)
            .map_err(|_| CommandFailure::Unavailable)?;
        let work = Box::pin(self.observe(started, deadline));
        crate::asyncrt::select_biased! { "the original controller observation deadline precedes a late sample";
            _ = timer => Err(CommandFailure::Unavailable),
            result = work => result,
        }
    }

    async fn observe(self, started: u64, deadline: u64) -> Result<Observed, CommandFailure> {
        let timeout = std::time::Duration::from_millis(self.options.operation_timeout_ms);
        let local = self.local.inspect_snapshot(timeout).await?;
        let leader = local
            .view
            .member
            .as_ref()
            .is_some_and(|member| member.is_leader);
        let pull = matches!(
            local.view.occupancy,
            ReplicaOccupancy::Standby | ReplicaOccupancy::Occupied(_)
        );
        let targets = if leader || pull { self.targets } else { vec![] };
        let scope = local.view.scope.clone();
        let schema = local.view.schema_digest;
        let birth = local.view.birth_digest;
        let peers = futures_util::stream::iter(targets)
            .map(|peer| {
                let domain = self.domain.clone();
                let transport = self.transport.clone();
                let scope = scope.clone();
                let timeout = self.options.probe_timeout_ms;
                async move {
                    let started = domain.mono_ms().map_err(|_| CommandFailure::Unavailable)?;
                    let until = deadline.min(started.saturating_add(timeout));
                    let result =
                        match transport.inspect_peer(scope, schema, birth, peer.clone(), until) {
                            Ok(delivery) => {
                                delivery.receive().await.map(|received| received.snapshot)
                            }
                            Err(error) => Err(error),
                        };
                    Ok::<_, CommandFailure>(Probe {
                        peer,
                        started,
                        result,
                    })
                }
            })
            .buffer_unordered(self.options.probe_concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let progress = if leader {
            Some(self.runtime.replication_status(&scope).await)
        } else {
            None
        };
        Ok(Observed {
            started,
            local,
            peers,
            progress,
            _memory: self.memory,
        })
    }
}
