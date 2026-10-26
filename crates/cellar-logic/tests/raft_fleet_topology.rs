// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Executable topology witnesses. No catalog, deployed fleet or performance
//! qualification is implied by these pure decision-core cases.

use cellar_logic::{consensus::*, raft_fleet::*};
use std::collections::BTreeMap;

fn node(n: usize) -> NodeId {
    NodeId::new([n as u8 + 1; 16]).unwrap()
}

fn resources(slots: u64, bytes: u64, memory: u64) -> Resources {
    Resources {
        engine_slots: slots,
        durable_bytes: bytes,
        memory_bytes: memory,
    }
}

fn policy() -> TopologyPolicy {
    TopologyPolicy {
        revision: 1,
        distinct_hosts: true,
        distinct_zones: true,
        distinct_regions: false,
        max_work: 50_000_000,
    }
}

fn fixture(count: usize, triples: &[[usize; 3]]) -> (TopologyView, Vec<TopologyTarget>) {
    let cluster = ClusterId::new([1; 16]).unwrap();
    let nodes = (0..count)
        .map(|n| {
            let scope = CapacityScope {
                cluster,
                incarnation: IncarnationId::new([2; 16]).unwrap(),
                catalog: [3; 32],
                node: NodeIdentity {
                    node: node(n),
                    disk: DiskLineage::new([n as u8 + 65; 16]).unwrap(),
                    process: ProcessGeneration::new([7; 16]).unwrap(),
                },
                engine: EngineInstanceId::new([n as u8 + 65; 16]).unwrap(),
                boot: [8; 16],
                profile_revision: 1,
            };
            let copies = triples.iter().filter(|t| t.contains(&n)).count() as u64;
            TopologyNode {
                ledger: LedgerSnapshot {
                    scope,
                    policy: LedgerPolicy {
                        capacity: resources(1000, 100_000, 100_000),
                        reserve: resources(1, 1, 1),
                        max_active_moves: 2,
                        max_sample_age_ms: 1000,
                    },
                    sample: PhysicalSample {
                        scope,
                        sequence: 1,
                        observed_ms: 100,
                        physical: resources(copies, copies * 10, copies * 10),
                    },
                    reservations: vec![],
                    complete: true,
                },
                host: format!("host-{n}"),
                region: "region".into(),
                zone: format!("zone-{n}"),
                keep: true,
                receives_replicas: true,
            }
        })
        .collect();
    let groups: Vec<_> = triples
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let mut id = [0; 16];
            id[..8].copy_from_slice(&(i as u64 + 1).to_be_bytes());
            TopologyGroup {
                scope: GroupScope {
                    cluster,
                    namespace: NamespaceId::new([2; 16]).unwrap(),
                    cell: format!("Counter:{i:05}"),
                    group: GroupId::new(id).unwrap(),
                    incarnation: IncarnationId::new([4; 16]).unwrap(),
                },
                revision: LogIndex::new(10),
                voters: t.map(node),
                occupied: t.map(node).to_vec(),
                resident: resources(1, 10, 10),
                peak: resources(1, 20, 20),
            }
        })
        .collect();
    let targets = groups
        .iter()
        .map(|g| TopologyTarget {
            group: g.scope.clone(),
            voters: g.voters,
        })
        .collect();
    (
        TopologyView {
            revision: [9; 32],
            complete: true,
            nodes,
            groups,
        },
        targets,
    )
}

fn symmetric() -> Vec<[usize; 3]> {
    let mut triples = vec![];
    for a in 0..5 {
        for b in a + 1..5 {
            for c in b + 1..5 {
                triples.extend(vec![[a, b, c]; 50]);
            }
        }
    }
    triples
}

fn replay(view: &TopologyView, targets: &[TopologyTarget], moves: &[ReplicaMove]) {
    let mut voters: BTreeMap<_, _> = view
        .groups
        .iter()
        .map(|g| (g.scope.clone(), g.voters))
        .collect();
    for (i, m) in moves.iter().enumerate() {
        assert_eq!(m.after, i.checked_sub(1));
        let set = voters.get_mut(&m.group).unwrap();
        assert!(set.contains(&m.source.node.node));
        assert!(!set.contains(&m.target.node.node));
        *set.iter_mut().find(|n| **n == m.source.node.node).unwrap() = m.target.node.node;
        assert_ne!(set[0], set[1]);
        assert_ne!(set[1], set[2]);
        assert_ne!(set[0], set[2]);
    }
    for target in targets {
        let mut actual = voters[&target.group];
        actual.sort();
        let mut desired = target.voters;
        desired.sort();
        assert_eq!(actual, desired);
    }
}

#[test]
fn five_to_six_verifies_250_replacements_and_all_loss_scenarios() {
    let (view, mut targets) = fixture(6, &symmetric());
    // The ten triples in lexicographic order, each with fifty histories.
    let departing = [2, 1, 0, 0, 4, 3, 3, 1, 4, 2];
    for (i, target) in targets.iter_mut().enumerate() {
        if i % 50 < 25 {
            *target
                .voters
                .iter_mut()
                .find(|n| **n == node(departing[i / 50]))
                .unwrap() = node(5);
        }
    }
    let result = preview_topology(&view, policy(), &targets, 100).unwrap();
    assert_eq!(result.moves.len(), 250);
    assert_eq!(result.recovery.len(), 6);
    let mut counts = BTreeMap::new();
    for t in &result.assignments {
        for n in t.voters {
            *counts.entry(n).or_insert(0) += 1;
        }
    }
    assert!(counts.values().all(|n| *n == 250));
    replay(&view, &targets, &result.moves);
    for scenario in &result.recovery {
        assert_eq!(scenario.moves.len(), 250);
        assert!(scenario.moves.iter().all(
            |m| m.source == scenario.failed && m.target.node.node != scenario.failed.node.node
        ));
    }
    // All five sources keep their 300 occupied slots plus the reserve.
    for peak in &result.peaks {
        assert_eq!(
            peak.resources.engine_slots,
            if peak.scope.node.node == node(5) {
                251
            } else {
                301
            }
        );
    }
}

#[test]
fn five_to_four_requires_retained_and_recovery_headroom() {
    let (mut view, mut targets) = fixture(5, &symmetric());
    view.nodes[4].keep = false;
    for (i, t) in targets.iter_mut().enumerate() {
        if t.voters.contains(&node(4)) {
            let missing: Vec<_> = (0..4).map(node).filter(|n| !t.voters.contains(n)).collect();
            *t.voters.iter_mut().find(|n| **n == node(4)).unwrap() =
                missing[usize::from(i % 50 >= 25)];
        }
    }
    for n in &mut view.nodes {
        n.ledger.policy.capacity.engine_slots = 501;
    }
    let result = preview_topology(&view, policy(), &targets, 100).unwrap();
    assert_eq!(result.moves.len(), 300);
    assert_eq!(result.recovery.len(), 4);
    assert!(result.moves.iter().all(|m| m.target.node.node != node(4)));
    for scenario in &result.recovery {
        assert_eq!(scenario.moves.len(), 375);
        assert!(scenario
            .peaks
            .iter()
            .filter(
                |p| p.scope.node.node != scenario.failed.node.node && p.scope.node.node != node(4)
            )
            .all(|p| p.resources.engine_slots == 501));
    }
    replay(&view, &targets, &result.moves);
    for n in &mut view.nodes {
        n.ledger.policy.capacity.engine_slots = 500;
    }
    assert!(matches!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::RecoveryUnproven(_))
    ));
}

fn staging() -> (TopologyView, Vec<TopologyTarget>) {
    let (mut v, mut t) = fixture(5, &[[0, 1, 2], [0, 1, 2]]);
    v.nodes[2].keep = false;
    for t in &mut t {
        t.voters = [node(0), node(1), node(3)];
    }
    v.nodes[3].ledger.policy.capacity = resources(10, 11, 11);
    v.groups[0].resident = resources(1, 6, 1);
    v.groups[0].peak = resources(1, 8, 2);
    v.groups[1].resident = resources(1, 1, 6);
    v.groups[1].peak = resources(1, 2, 10);
    (v, t)
}

#[test]
fn staged_search_backtracks_when_first_move_blocks_the_second() {
    let (view, targets) = staging();
    let plan = preview_topology(&view, policy(), &targets, 100).unwrap();
    assert_eq!(plan.moves[0].group, view.groups[1].scope);
    assert_eq!(plan.moves[1].group, view.groups[0].scope);
    replay(&view, &targets, &plan.moves);
}

#[test]
fn final_fit_does_not_prove_a_staged_path() {
    let (mut view, targets) = staging();
    view.groups[0].peak.durable_bytes = 10;
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::NoStagedPath)
    );
}

#[test]
fn recovery_assignment_backtracks_across_independent_resources() {
    let (mut view, targets) = fixture(5, &[[0, 1, 2], [0, 1, 2]]);
    view.groups[0].resident = resources(1, 6, 1);
    view.groups[0].peak = view.groups[0].resident;
    view.groups[1].resident = resources(1, 1, 6);
    view.groups[1].peak = view.groups[1].resident;
    view.nodes[3].ledger.policy.capacity = resources(10, 7, 7);
    view.nodes[4].ledger.policy.capacity = resources(10, 7, 2);
    let plan = preview_topology(&view, policy(), &targets, 100).unwrap();
    let loss = plan
        .recovery
        .iter()
        .find(|s| s.failed.node.node == node(0))
        .unwrap();
    assert_eq!(
        loss.moves
            .iter()
            .find(|m| m.group == view.groups[0].scope)
            .unwrap()
            .target
            .node
            .node,
        node(4)
    );
    assert_eq!(
        loss.moves
            .iter()
            .find(|m| m.group == view.groups[1].scope)
            .unwrap()
            .target
            .node
            .node,
        node(3)
    );
}

#[test]
fn search_exhaustion_is_not_infeasibility_or_a_partial_preview() {
    let (view, targets) = staging();
    let mut p = policy();
    p.max_work = 1;
    assert_eq!(
        preview_topology(&view, p, &targets, 100),
        Err(TopologyError::SearchExhausted)
    );
}

#[test]
fn retained_binding_and_excluded_destinations_are_refused() {
    let (mut view, mut targets) = fixture(5, &[[0, 1, 2]]);
    targets[0].voters[2] = node(3);
    view.groups[0].occupied.push(node(3));
    view.nodes[3].ledger.sample.physical.engine_slots = 1;
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::OccupiedTarget)
    );
    view.groups[0].occupied.pop();
    view.nodes[3].keep = false;
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::ExcludedTarget)
    );
}

#[test]
fn domain_policy_applies_to_stable_triples_not_four_way_union() {
    let (mut view, mut targets) = fixture(5, &[[0, 1, 2]]);
    targets[0].voters[2] = node(3);
    view.nodes[3].zone = view.nodes[2].zone.clone();
    let result = preview_topology(&view, policy(), &targets, 100).unwrap();
    assert_eq!(result.moves.len(), 1);
    view.nodes[3].zone = view.nodes[1].zone.clone();
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::FailureDomains)
    );
}

#[test]
fn incomplete_stale_and_unsettled_inputs_cannot_authorize_preview() {
    let (mut view, targets) = fixture(5, &[[0, 1, 2]]);
    view.complete = false;
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::Incomplete)
    );
    view.complete = true;
    assert_eq!(
        preview_topology(&view, policy(), &targets, 1101),
        Err(TopologyError::Ledger(LedgerError::StaleObservation))
    );
    let scope = view.nodes[3].ledger.scope;
    view.nodes[3].ledger.reservations.push(Reservation {
        id: ProposalId::new([5; 16]).unwrap(),
        group: view.groups[0].scope.clone(),
        scope,
        peak: resources(1, 10, 10),
        materialized: None,
    });
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::UnsettledMovement)
    );
}

#[test]
fn occupied_inventory_must_cover_all_current_voters_and_physical_slots() {
    let (mut view, targets) = fixture(5, &[[0, 1, 2]]);
    view.groups[0].occupied.pop();
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::Incomplete)
    );
    view.groups[0].occupied.push(node(2));
    view.nodes[2].ledger.sample.physical.engine_slots = 0;
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::Incomplete)
    );
}

#[test]
fn exact_boot_and_input_revisions_survive_in_the_preview() {
    let (view, mut targets) = fixture(5, &[[0, 1, 2]]);
    targets[0].voters[2] = node(3);
    let result = preview_topology(&view, policy(), &targets, 100).unwrap();
    assert_eq!(result.input_revision, view.revision);
    assert_eq!(result.policy_revision, 1);
    assert_eq!(result.moves[0].source, view.nodes[2].ledger.scope);
    assert_eq!(result.moves[0].target, view.nodes[3].ledger.scope);
}

#[test]
fn equivalent_input_order_produces_identical_witnesses() {
    let (mut view, mut targets) = fixture(5, &[[0, 1, 2], [0, 1, 2]]);
    targets[0].voters[2] = node(3);
    let result = preview_topology(&view, policy(), &targets, 100).unwrap();
    view.nodes.reverse();
    view.groups.reverse();
    targets.reverse();
    for group in &mut view.groups {
        group.voters.reverse();
        group.occupied.reverse();
    }
    for target in &mut targets {
        target.voters.reverse();
    }
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100).unwrap(),
        result
    );
}

#[test]
fn all_small_triple_changes_preserve_rf3_and_have_recovery_witnesses() {
    let mut triples = vec![];
    for a in 0..6 {
        for b in a + 1..6 {
            for c in b + 1..6 {
                triples.push([a, b, c]);
            }
        }
    }
    // The seventh node remains a fresh destination even for disjoint triples.
    for old in &triples {
        for new in &triples {
            let (view, mut targets) = fixture(7, &[*old]);
            targets[0].voters = new.map(node);
            let result = preview_topology(&view, policy(), &targets, 100).unwrap();
            assert_eq!(
                result.moves.len(),
                new.iter().filter(|n| !old.contains(n)).count()
            );
            replay(&view, &targets, &result.moves);
            for scenario in &result.recovery {
                let mut recovered = targets.clone();
                for m in &scenario.moves {
                    let t = recovered.iter_mut().find(|t| t.group == m.group).unwrap();
                    *t.voters
                        .iter_mut()
                        .find(|n| **n == m.source.node.node)
                        .unwrap() = m.target.node.node;
                }
                assert!(recovered
                    .iter()
                    .all(|t| !t.voters.contains(&scenario.failed.node.node)));
                let mut final_view = view.clone();
                final_view.groups[0].voters = targets[0].voters;
                replay(&final_view, &recovered, &scenario.moves);
            }
        }
    }
}

#[test]
fn overflow_cannot_be_reported_as_spare_capacity() {
    let (mut view, mut targets) = fixture(5, &[[0, 1, 2]]);
    targets[0].voters[2] = node(3);
    view.groups[0].peak.durable_bytes = u64::MAX;
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::Ledger(LedgerError::ArithmeticOverflow))
    );
}

#[test]
fn kept_nodes_need_separate_current_receiving_eligibility() {
    let (mut view, mut targets) = fixture(4, &[[0, 1, 2]]);
    view.nodes[3].receives_replicas = false;
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::RecoveryUnproven(node(0)))
    );
    targets[0].voters[2] = node(3);
    assert_eq!(
        preview_topology(&view, policy(), &targets, 100),
        Err(TopologyError::ExcludedTarget)
    );
}
