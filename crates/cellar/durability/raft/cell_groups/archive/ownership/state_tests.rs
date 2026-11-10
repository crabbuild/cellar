// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registry metadata tests grant no physical archive coverage.
use super::*;
use cellar_logic::cell_group::{CellRange, GroupRoute, RouteEpoch};
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

fn selected() -> (State, CellGroupArchiveIntent) {
    let route = GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [5; 32],
        schema_digest: [6; 32],
    };
    let identity = CellGroupArchiveCreation {
        domain: IDENTITY_DOMAIN.into(),
        id: [7; 32],
        route: route.clone(),
    };
    let root = Descriptor {
        object: Object {
            registry: identity.id,
            generation: 2,
            digest: [8; 32],
            length: 128,
        },
        marker: GroupManifest::genesis(route).unwrap(),
    };
    let mut state = State::empty(identity.clone());
    state.revision = 3;
    state.selected = Some(root.clone());
    state.check(&identity).unwrap();
    let intent = CellGroupArchiveIntent {
        domain: INTENT_DOMAIN.into(),
        registry: identity.id,
        revision: 4,
        nonce: [9; 32],
        action: Action::Acquire { root },
    };
    (state, intent)
}

#[test]
fn acquire_outcome_reconciles_a_lost_response_after_unrelated_allocations() {
    let (mut state, intent) = selected();
    assert!(matches!(
        state.acquire_outcome(&intent).unwrap(),
        CellGroupArchiveAcquireOutcome::Pending
    ));
    let Allocation::Hold(hold) = state.allocate(&intent).unwrap() else {
        panic!("acquire did not allocate a hold");
    };
    let unrelated = CellGroupArchiveIntent {
        revision: 5,
        nonce: [10; 32],
        action: Action::Reserve {
            parent: state.selected.clone(),
        },
        ..intent.clone()
    };
    state.allocate(&unrelated).unwrap();
    state.check(&state.identity).unwrap();
    let CellGroupArchiveAcquireOutcome::Held(observed) = state.acquire_outcome(&intent).unwrap()
    else {
        panic!("lost response did not reconcile");
    };
    assert_eq!(*observed, *hold);
    assert_eq!(state.holds.len(), 1);
}

#[test]
fn acquire_outcome_requires_a_consumed_revision_before_reallocation() {
    let (mut state, intent) = selected();
    let competing = CellGroupArchiveIntent {
        nonce: [11; 32],
        action: Action::Reserve {
            parent: state.selected.clone(),
        },
        ..intent.clone()
    };
    assert!(matches!(
        state.acquire_outcome(&intent).unwrap(),
        CellGroupArchiveAcquireOutcome::Pending
    ));
    state.allocate(&competing).unwrap();
    assert!(matches!(
        state.acquire_outcome(&intent).unwrap(),
        CellGroupArchiveAcquireOutcome::Superseded
    ));
    assert!(matches!(
        state.allocate(&intent),
        Err(CellGroupArchiveError::Expired)
    ));
    assert!(state.holds.is_empty());
}

#[test]
fn acquire_outcome_refuses_foreign_registry_and_other_allocation_purposes() {
    let (state, mut intent) = selected();
    intent.registry = [12; 32];
    assert!(matches!(
        state.acquire_outcome(&intent),
        Err(CellGroupArchiveError::Conflict)
    ));
    intent.registry = state.identity.id;
    intent.action = Action::Reserve {
        parent: state.selected.clone(),
    };
    assert!(state.acquire_outcome(&intent).is_err());
}

#[test]
fn cancelling_an_acquire_fences_a_delayed_allocation() {
    let (mut state, intent) = selected();
    assert!(state.cancel_acquire(&intent).unwrap());
    assert_eq!(state.revision, intent.revision);
    assert!(matches!(
        state.allocate(&intent),
        Err(CellGroupArchiveError::Expired)
    ));
    assert!(!state.cancel_acquire(&intent).unwrap());
    assert!(state.holds.is_empty());
    state.check(&state.identity).unwrap();
}

#[test]
fn cancelling_a_committed_acquire_releases_only_its_hold() {
    let (mut state, intent) = selected();
    state.allocate(&intent).unwrap();
    let unrelated = CellGroupArchiveIntent {
        revision: intent.revision + 1,
        nonce: [10; 32],
        ..intent.clone()
    };
    let Allocation::Hold(other) = state.allocate(&unrelated).unwrap() else {
        panic!("unrelated acquire did not allocate a hold");
    };
    assert!(state.cancel_acquire(&intent).unwrap());
    assert_eq!(state.holds, vec![*other]);
    assert!(!state.cancel_acquire(&intent).unwrap());
    assert!(matches!(
        state.allocate(&intent),
        Err(CellGroupArchiveError::Expired)
    ));
    state.check(&state.identity).unwrap();
}

#[test]
fn cancelling_a_superseded_acquire_preserves_the_same_revision_winner() {
    let (mut state, intent) = selected();
    let unrelated = CellGroupArchiveIntent {
        nonce: [10; 32],
        ..intent.clone()
    };
    let Allocation::Hold(other) = state.allocate(&unrelated).unwrap() else {
        panic!("unrelated acquire did not allocate a hold");
    };
    assert!(!state.cancel_acquire(&intent).unwrap());
    assert_eq!(state.holds, vec![*other]);
    state.check(&state.identity).unwrap();
}
