// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::{ClusterId, GroupId, NamespaceId};

#[test]
fn whole_group_history_retains_range_and_original_creation() {
    let history = fixture();
    let restored = FleetHistory::decode(&history.encode().unwrap()).unwrap();
    let placement = restored.initial_placement().unwrap();
    assert_eq!(placement.history, history.binding.scope().unwrap());
    assert_eq!(placement.creation.bytes(), history.creation);
    assert_eq!(placement.epoch.get(), 7);
    assert_eq!(placement.voters.len(), 3);
    assert_eq!(
        bound_key(placement.range.lower()).as_deref(),
        history.lower.as_deref()
    );
    assert_eq!(
        bound_key(placement.range.upper()).as_deref(),
        history.upper.as_deref()
    );
    let mut changed = restored;
    changed.upper = Some("Counter:zz".into());
    assert_ne!(history.name_key().unwrap(), changed.name_key().unwrap());
    assert_eq!(history.group_key().unwrap(), changed.group_key().unwrap());
}

#[test]
fn pending_groups_with_the_same_range_keep_distinct_inventory_keys() {
    let first = fixture();
    let mut second = first.clone();
    let mut history = second.binding.scope().unwrap();
    history.group = GroupId::new([13; 16]).unwrap();
    history.incarnation = IncarnationId::new([14; 16]).unwrap();
    second.binding = FleetGroupBinding::new(
        IncarnationId::new(first.binding.fleet()).unwrap(),
        &history,
        first.binding.catalog(),
    )
    .unwrap();
    assert_eq!(first.lower, second.lower);
    assert_eq!(first.upper, second.upper);
    assert_eq!(first.epoch, second.epoch);
    assert_ne!(first.name_key().unwrap(), second.name_key().unwrap());
    assert_ne!(first.group_key().unwrap(), second.group_key().unwrap());
}

#[test]
fn history_refuses_missing_bounds_and_invalid_ranges() {
    let history = fixture();
    for field in ["lower", "upper", "epoch", "creation"] {
        let mut value = serde_json::to_value(&history).unwrap();
        value.as_object_mut().unwrap().remove(field);
        let bytes = encode(HISTORY_KIND, &value, 4096).unwrap();
        assert!(
            FleetHistory::decode(&bytes).is_err(),
            "accepted missing {field}"
        );
    }
    let mut inverted = history.clone();
    inverted.lower = history.upper;
    inverted.upper = history.lower;
    assert!(inverted.encode().is_err());
}

fn fixture() -> FleetHistory {
    let namespace = NamespaceId::new([2; 16]).unwrap();
    let mut names = ["Counter:a", "Counter:z"];
    names.sort_by_key(|name| {
        crate::application::consensus::cell_routing_key(namespace, &CellId::new(name).unwrap())
    });
    FleetHistory {
        binding: FleetGroupBinding::new(
            IncarnationId::new([5; 16]).unwrap(),
            &GroupHistory {
                cluster: ClusterId::new([1; 16]).unwrap(),
                namespace,
                group: GroupId::new([3; 16]).unwrap(),
                incarnation: IncarnationId::new([4; 16]).unwrap(),
            },
            [6; 32],
        )
        .unwrap(),
        schema_digest: [7; 32],
        namespace_root: [8; 32],
        birth_digest: [9; 32],
        lower: Some(names[0].into()),
        upper: Some(names[1].into()),
        epoch: 7,
        creation: [10; 16],
        admission: [11; 16],
        operation: [12; 16],
        voters: (1..=3)
            .map(|member| Member {
                member,
                node: [member as u8; 16],
            })
            .collect(),
        learners: vec![],
        state: FleetHistoryState::Reserved,
        origin: Origin::Managed,
    }
}
