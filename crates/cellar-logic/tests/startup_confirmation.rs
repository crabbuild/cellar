// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Startup inventory is node work; caller cancellation and teardown must not
//! leave its confirmation markers attached to retired owner reads.
use cellar_logic::{
    on_event, Config, Effect, Event, LeaseCasOutcome, LocalCell, NodeLeaseSpec, OwnerRecord,
    OwnershipOnEvict, Phase, State,
};

fn started() -> State {
    let mut state = State::new(
        "owner",
        Config {
            max_resident: 2,
            max_cached_routes: 16,
            max_activations: 1,
            max_evictions: 1,
            max_releases: 1,
            max_outbound_websockets: 8,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: true,
            peer_protocol: 1,
            operation_deadline_ms: Some(30_000),
            owner_log_recovery_backoff_ms: 1_000,
            owner_log_recovery_attempts: 2,
            alarm_resident_ms: 1_000,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
    );
    let effects = on_event(
        &mut state,
        Event::StartNodeLease {
            now_ms: 0,
            now_mono_ms: 0,
            spec: NodeLeaseSpec {
                lineage: None,
                addr: "owner:8081".into(),
                peer_protocol: 1,
                generation: "generation".into(),
                resume_generation: None,
                ttl_ms: 100_000,
            },
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadSelfNodeLease { op } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        &mut state,
        Event::SelfNodeLeaseRead {
            op,
            now_ms: 0,
            now_mono_ms: 0,
            result: Ok(None),
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CasNodeLease { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        &mut state,
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: 0,
            result: Ok(LeaseCasOutcome::Applied {
                etag: "lease".into(),
            }),
            stamped_log_state: None,
        },
    );
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::ReadLocalCells)));
    assert!(state.node_authoritative());
    assert!(!state.ownership_confirmed());
    state
}

fn inventory() -> Event {
    Event::LocalCellsRead {
        result: Ok(["Counter:a", "Counter:b"]
            .into_iter()
            .map(|id| LocalCell {
                id: id.into(),
                epoch: 7,
                live: false,
            })
            .collect()),
    }
}

fn read_op(state: &State, id: &str) -> u64 {
    match state.phase(id) {
        Some(Phase::ReadingOwner { op, .. }) => *op,
        phase => panic!("expected owner read for {id}, got {phase:?}"),
    }
}

fn owner_read(op: u64) -> Event {
    Event::OwnerRead {
        op,
        now_ms: 0,
        result: Ok(Some(OwnerRecord {
            node: Some("owner".into()),
            lineage: None,
            epoch: 7,
            etag: "owner".into(),
        })),
    }
}

#[test]
fn preserve_retires_inflight_and_queued_confirmation_without_claiming_completion() {
    let mut state = started();
    on_event(&mut state, inventory());
    let old = read_op(&state, "Counter:a");
    assert_eq!(state.phase("Counter:b"), Some(&Phase::WaitingActivation));
    for (request, cell) in [(1, "Counter:a"), (2, "Counter:b")] {
        on_event(
            &mut state,
            Event::Request {
                request,
                cell: cell.into(),
            },
        );
    }
    let effects = on_event(&mut state, Event::BeginPreserve);
    let refused: Vec<_> = effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Complete {
                request,
                result: Err(cellar_logic::RequestError::NodeFenced),
            } => Some(*request),
            _ => None,
        })
        .collect();
    assert_eq!(refused, vec![1, 2]);
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        Effect::CasOwner { .. } | Effect::Restore { .. } | Effect::StartRuntime { .. }
    )));
    state.validate().unwrap();
    assert_eq!(state.activating(), 0);
    assert!(state.activation_waiting().is_empty());
    assert!(
        !state.ownership_confirmed(),
        "abandoned inventory is not a confirmed count"
    );
    for id in ["Counter:a", "Counter:b"] {
        assert_eq!(state.phase(id), Some(&Phase::Inactive));
    }
    assert!(on_event(&mut state, owner_read(old)).is_empty());
    assert_eq!(
        state.owned_cells(),
        0,
        "late completion revived retired ownership"
    );
}

#[test]
fn cancelled_caller_does_not_cancel_queued_startup_confirmation() {
    let mut state = started();
    on_event(&mut state, inventory());
    let first = read_op(&state, "Counter:a");
    on_event(
        &mut state,
        Event::Request {
            request: 1,
            cell: "Counter:b".into(),
        },
    );
    on_event(&mut state, Event::Cancel { request: 1 });
    assert_eq!(state.phase("Counter:b"), Some(&Phase::WaitingActivation));
    assert!(!state.ownership_confirmed());
    on_event(&mut state, owner_read(first));
    let second = read_op(&state, "Counter:b");
    on_event(&mut state, owner_read(second));
    state.validate().unwrap();
    assert!(state.ownership_confirmed());
    assert_eq!(state.owned_cells(), 2);
    assert_eq!(state.occupied(), 0, "confirmation must not start a runtime");
    for id in ["Counter:a", "Counter:b"] {
        assert_eq!(state.phase(id), Some(&Phase::Dormant { epoch: 7 }));
    }
}

#[test]
fn fence_retires_confirmation_and_ignores_late_owner_read() {
    let mut state = started();
    on_event(&mut state, inventory());
    let old = read_op(&state, "Counter:a");
    on_event(&mut state, Event::NodeFenced);
    state.validate().unwrap();
    assert!(state.is_fenced());
    assert!(!state.ownership_confirmed());
    assert!(on_event(&mut state, owner_read(old)).is_empty());
    assert_eq!(state.activating(), 0);
    for id in ["Counter:a", "Counter:b"] {
        assert_eq!(state.phase(id), Some(&Phase::Fenced));
    }
}

#[test]
fn late_inventory_cannot_restart_confirmation_after_teardown() {
    for teardown in [Event::BeginPreserve, Event::NodeFenced] {
        let mut state = started();
        on_event(&mut state, teardown);
        assert!(on_event(&mut state, inventory()).is_empty());
        assert!(!state.ownership_confirmed());
        assert_eq!(state.activating(), 0);
        assert!(state.phase("Counter:a").is_none());
        state.validate().unwrap();
    }
}
