// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One required replacement after the adapter proves exact-boot isolation.
//! Selection grants no membership, health, reservation or release authority.
use super::{ResourceLedger, Resources, TopologyError, TopologyNode, TopologyPolicy};
use crate::consensus::{GroupScope, LogIndex, MemberId, NodeId};
use crate::raft_placement::{PlacedMember, RepairPlacement};
use std::collections::BTreeSet;

pub struct IsolatedRepairView {
    pub scope: GroupScope,
    pub revision: LogIndex,
    pub high_water: u64,
    pub voters: [PlacedMember; 3],
    pub failed: NodeId,
    pub failed_host: String,
    /// Complete occupied bindings, including retired copies. The isolated
    /// node's binding remains occupied even though it cannot be inspected.
    pub occupied: Vec<NodeId>,
    pub peak: Resources,
}

fn label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

fn pressure(total: Resources, capacity: Resources) -> (u128, u128) {
    [
        (total.engine_slots, capacity.engine_slots),
        (total.durable_bytes, capacity.durable_bytes),
        (total.memory_bytes, capacity.memory_bytes),
    ]
    .into_iter()
    .map(|(n, d)| (u128::from(n), u128::from(d)))
    .fold(
        (0, 1),
        |(a, b), (c, d)| if c * b > a * d { (c, d) } else { (a, b) },
    )
}

/// Keep both surviving voters and choose a fresh eligible binding under all
/// three resource bounds. The adapter supplies authenticated complete inputs
/// and a permanent isolation proof; missing/stale samples never create failure.
/// The current history must have no admitted work. Unrelated reservations stay
/// charged, including old-boot debt; the adapter also enforces the global action
/// bound. Equal pressure uses a stable per-history tie break.
pub fn select_isolated_repair(
    view: &IsolatedRepairView,
    nodes: &[TopologyNode],
    policy: TopologyPolicy,
    now_ms: u64,
) -> Result<RepairPlacement, TopologyError> {
    if policy.revision == 0 || policy.max_work == 0 || policy.max_work > 50_000_000 {
        return Err(TopologyError::InvalidPolicy);
    }
    if nodes.len() < 3 || nodes.len() > 64 || view.occupied.len() > 64 {
        return Err(TopologyError::Incomplete);
    }
    if policy.max_work < nodes.len() as u64 * 2 {
        return Err(TopologyError::SearchExhausted);
    }
    if view.scope.validate().is_err()
        || view.revision.get() == 0
        || view.revision.get() >= u64::MAX - 2
        || !label(&view.failed_host)
        || view.peak.engine_slots != 1
        || view.peak.memory_bytes == 0
        || view.peak.durable_bytes == 0
    {
        return Err(TopologyError::InvalidInput);
    }
    let occupied = view.occupied.iter().copied().collect::<BTreeSet<_>>();
    let members = view
        .voters
        .iter()
        .map(|v| v.member)
        .collect::<BTreeSet<_>>();
    let voters = view.voters.iter().map(|v| v.node).collect::<BTreeSet<_>>();
    if occupied.len() != view.occupied.len()
        || members.len() != 3
        || voters.len() != 3
        || view
            .voters
            .iter()
            .map(|v| v.disk)
            .collect::<BTreeSet<_>>()
            .len()
            != 3
        || !voters.contains(&view.failed)
        || !voters.is_subset(&occupied)
        || view.voters.iter().any(|v| v.member.get() > view.high_water)
    {
        return Err(TopologyError::InvalidInput);
    }
    let next = view
        .high_water
        .checked_add(1)
        .filter(|n| *n < u64::MAX - 2)
        .ok_or(TopologyError::Limit)?;
    let mut ids = BTreeSet::new();
    let mut disks = BTreeSet::new();
    for node in nodes {
        let scope = node.ledger.scope;
        if scope.cluster != view.scope.cluster
            || scope.catalog != nodes[0].ledger.scope.catalog
            || scope.incarnation != nodes[0].ledger.scope.incarnation
            || scope.node.node == view.failed
            || view
                .voters
                .iter()
                .find(|v| v.node == view.failed)
                .is_some_and(|v| v.disk == scope.node.disk)
            || !ids.insert(scope.node.node)
            || !disks.insert(scope.node.disk)
            || !label(&node.host)
            || !label(&node.region)
            || !label(&node.zone)
        {
            return Err(TopologyError::InvalidInput);
        }
        if node
            .ledger
            .reservations
            .iter()
            .any(|reservation| reservation.group == view.scope)
        {
            return Err(TopologyError::UnsettledMovement);
        }
        ResourceLedger::restore(node.ledger.clone()).map_err(TopologyError::Ledger)?;
    }
    let survivors = view
        .voters
        .iter()
        .filter(|v| v.node != view.failed)
        .map(|v| {
            nodes
                .iter()
                .find(|n| n.ledger.scope.node.node == v.node && n.ledger.scope.node.disk == v.disk)
                .ok_or(TopologyError::Incomplete)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for survivor in &survivors {
        ResourceLedger::restore(survivor.ledger.clone())
            .map_err(TopologyError::Ledger)?
            .charge(now_ms)
            .map_err(TopologyError::Ledger)?;
    }
    let domains = |candidate: &TopologyNode| {
        let set = survivors
            .iter()
            .copied()
            .chain(std::iter::once(candidate))
            .collect::<Vec<_>>();
        (!policy.distinct_hosts
            || (candidate.host != view.failed_host
                && set.iter().map(|n| &n.host).collect::<BTreeSet<_>>().len() == 3))
            && (!policy.distinct_zones
                || set
                    .iter()
                    .map(|n| (&n.region, &n.zone))
                    .collect::<BTreeSet<_>>()
                    .len()
                    == 3)
            && (!policy.distinct_regions
                || set.iter().map(|n| &n.region).collect::<BTreeSet<_>>().len() == 3)
    };
    let mut choices = Vec::new();
    for node in nodes {
        if !node.keep || !node.receives_replicas || occupied.contains(&node.ledger.scope.node.node)
        {
            continue;
        }
        let ledger = ResourceLedger::restore(node.ledger.clone()).map_err(TopologyError::Ledger)?;
        let charge = match ledger.charge(now_ms) {
            Ok(charge) => charge,
            Err(super::LedgerError::StaleObservation) => continue,
            Err(error) => return Err(TopologyError::Ledger(error)),
        };
        if charge.active_moves >= node.ledger.policy.max_active_moves {
            continue;
        }
        let total = charge
            .total
            .checked_add(view.peak)
            .map_err(TopologyError::Ledger)?;
        if node.ledger.policy.capacity.covers(total) && domains(node) {
            choices.push((node, pressure(total, node.ledger.policy.capacity)));
        }
    }
    choices.sort_by(|(a, (an, ad)), (b, (bn, bd))| {
        (an * bd)
            .cmp(&(bn * ad))
            .then_with(|| {
                crate::raft_placement::rank(&view.scope, b.ledger.scope.node.node).cmp(
                    &crate::raft_placement::rank(&view.scope, a.ledger.scope.node.node),
                )
            })
            .then_with(|| a.ledger.scope.node.node.cmp(&b.ledger.scope.node.node))
    });
    let target = choices
        .first()
        .map(|(node, _)| *node)
        .ok_or(TopologyError::RecoveryUnproven(view.failed))?;
    let obsolete = view
        .voters
        .iter()
        .find(|v| v.node == view.failed)
        .ok_or(TopologyError::InvalidInput)?;
    Ok(RepairPlacement {
        scope: view.scope.clone(),
        policy_revision: policy.revision,
        expected_revision: view.revision,
        obsolete: obsolete.member,
        replacement: PlacedMember {
            member: MemberId::new(next).map_err(|_| TopologyError::Limit)?,
            node: target.ledger.scope.node.node,
            disk: target.ledger.scope.node.disk,
        },
    })
}
