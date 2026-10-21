//! Placement contracts; native controller repair is qualified separately.
#[path = "support/raft_cluster.rs"]
#[allow(dead_code)]
mod cluster;
#[path = "support/raft_repair_native.rs"]
mod native_repairs;
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;

use cellar_logic::{consensus::*, raft_placement::*};

fn fixture() -> (PlacementView, Vec<NodeCapacity>, PlacementPolicy) {
    let nodes: Vec<_> = (1..=6)
        .map(|id| NodeCapacity {
            node: NodeId::new([id; 16]).unwrap(),
            disk: DiskLineage::new([id + 10; 16]).unwrap(),
            pool: "primary".into(),
            region: "west".into(),
            zone: format!("z{id}"),
            sampled_ms: 1000,
            health: if id == 3 {
                NodeHealth::Unavailable
            } else {
                NodeHealth::Healthy
            },
            compatible: true,
            draining: false,
            groups: if id == 4 { 50 } else { 10 },
            group_capacity: 100,
            bytes: 1024,
            byte_capacity: 1024 * 1024,
        })
        .collect();
    let view = PlacementView {
        scope: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: "Counter:repair".into(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        revision: LogIndex::new(7),
        high_water: 9,
        voters: nodes[..3]
            .iter()
            .enumerate()
            .map(|(i, n)| PlacedMember {
                member: MemberId::new(i as u64 + 1).unwrap(),
                node: n.node,
                disk: n.disk,
            })
            .collect(),
        learners: vec![],
        joint: false,
        repairing: false,
    };
    let policy = PlacementPolicy {
        revision: 2,
        pool: "primary".into(),
        distinct_zones: true,
        distinct_regions: false,
        max_sample_age_ms: 100,
        required_bytes: 4096,
    };
    (view, nodes, policy)
}

#[test]
fn insufficient_domain_capacity() {
    let (view, mut nodes, policy) = fixture();
    for n in &mut nodes[3..] {
        n.zone = "z1".into();
    }
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::InsufficientDomains)
    );
    assert_eq!(view.voters[2].member.get(), 3);
}

#[test]
fn placement_is_deterministic_and_preserves_member_watermark() {
    let (mut view, mut nodes, policy) = fixture();
    let chosen = select_repair(&view, &nodes, &policy, 1000)
        .unwrap()
        .unwrap();
    assert_eq!(chosen.obsolete.get(), 3);
    assert_eq!(chosen.replacement.member.get(), 10);
    assert_ne!(chosen.replacement.node, nodes[3].node);
    assert_eq!(chosen.expected_revision, view.revision);
    assert_eq!(chosen.policy_revision, 2);
    nodes.reverse();
    view.voters.reverse();
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000).unwrap(),
        Some(chosen)
    );
    assert_eq!(view.high_water, 9);
}

#[test]
fn placement_refuses_stale_capacity_and_unconfirmed_failure() {
    let (view, mut nodes, policy) = fixture();
    for n in &mut nodes[3..] {
        n.sampled_ms = 0;
    }
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::InsufficientCapacity)
    );
    nodes[2].sampled_ms = 0;
    assert_eq!(select_repair(&view, &nodes, &policy, 1000), Ok(None));
}

#[test]
fn placement_keeps_joint_and_pending_plans_serialized() {
    let (mut view, nodes, policy) = fixture();
    view.repairing = true;
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::ConfigurationInTransition)
    );
    view.repairing = false;
    view.joint = true;
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::ConfigurationInTransition)
    );
}

#[test]
fn placement_refuses_incompatible_draining_full_and_overflowed_targets() {
    let (view, mut nodes, policy) = fixture();
    nodes[3].compatible = false;
    nodes[4].draining = true;
    nodes[5].groups = nodes[5].group_capacity;
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::InsufficientCapacity)
    );
    nodes[5].groups = 0;
    nodes[5].bytes = u64::MAX;
    nodes[5].byte_capacity = u64::MAX;
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::InsufficientCapacity)
    );
}

#[test]
fn placement_refuses_duplicate_physical_nodes_and_invalid_policy() {
    let (view, mut nodes, mut policy) = fixture();
    nodes[5].node = nodes[4].node;
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::InvalidObservation)
    );
    policy.revision = 0;
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::InvalidPolicy)
    );
}

#[test]
fn placement_treats_zone_names_within_their_regions() {
    let (view, mut nodes, policy) = fixture();
    for n in &mut nodes[3..] {
        n.zone = "z1".into();
        n.region = "east".into();
    }
    assert!(select_repair(&view, &nodes, &policy, 1000)
        .unwrap()
        .is_some());
    let strict = PlacementPolicy {
        distinct_regions: true,
        ..policy
    };
    assert_eq!(
        select_repair(&view, &nodes, &strict, 1000),
        Err(PlacementError::InsufficientDomains)
    );
}

#[test]
fn placement_refuses_invalid_view_and_exhausted_member_ids() {
    let (mut view, nodes, policy) = fixture();
    view.high_water = u64::MAX - 3;
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::ExhaustedMemberIds)
    );
    view.high_water = u64::MAX - 2;
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::InvalidConfiguration)
    );
    view.high_water = 9;
    view.revision = LogIndex::new(u64::MAX - 2);
    assert_eq!(
        select_repair(&view, &nodes, &policy, 1000),
        Err(PlacementError::InvalidConfiguration)
    );
}

#[test]
fn placement_balances_the_limiting_capacity() {
    let (view, mut nodes, policy) = fixture();
    nodes[3].groups = 0;
    nodes[3].bytes = nodes[3].byte_capacity - policy.required_bytes;
    let chosen = select_repair(&view, &nodes, &policy, 1000)
        .unwrap()
        .unwrap();
    assert_ne!(chosen.replacement.node, nodes[3].node);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_before_promotion() -> anyhow::Result<()> {
    native_repairs::learner_before_promotion().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_plan_idempotence() -> anyhow::Result<()> {
    native_repairs::stale_plan_idempotence().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_cannot_reconfigure() -> anyhow::Result<()> {
    native_repairs::minority_cannot_reconfigure().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joint_configuration_restart() -> anyhow::Result<()> {
    native_repairs::joint_configuration_restart().await
}
