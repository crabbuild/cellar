//! Settled routing history must not retain every business identity ever seen.

use cellar_logic::{
    on_event, CasOutcome, Config, Effect, Event, Failure, NodeLeaseRecord, OwnerRecord,
    OwnershipOnEvict, Phase, RestoreOutcome, Route, State, Timer,
};
use std::collections::VecDeque;

fn config() -> Config {
    Config {
        max_resident: 8,
        max_cached_routes: 8,
        max_activations: 8,
        max_evictions: 4,
        max_releases: 4,
        max_outbound_websockets: 8,
        ownership_on_evict: OwnershipOnEvict::Release,
        require_node_lease: false,
        peer_protocol: 1,
        operation_deadline_ms: Some(30_000),
        owner_log_recovery_backoff_ms: 1_000,
        owner_log_recovery_attempts: 2,
        alarm_resident_ms: 1_000,
        idle_evict_ms: None,
        pressure: Default::default(),
    }
}

fn request(state: &mut State, request: u64, cell: &str) -> u64 {
    on_event(
        state,
        Event::Request {
            request,
            cell: cell.into(),
        },
    )
    .into_iter()
    .find_map(|effect| match effect {
        Effect::ReadOwner { op, .. } => Some(op),
        _ => None,
    })
    .expect("a new cell must read ownership")
}

fn fail(state: &mut State, request: u64, op: u64) -> Vec<Effect> {
    let effects = on_event(
        state,
        Event::OwnerRead {
            op,
            now_ms: request * 60_000,
            result: Err(Failure::Definite),
        },
    );
    assert!(effects.iter().any(|effect| matches!(effect,
        Effect::Complete { request: completed, result: Err(_), .. } if *completed == request)));
    state.validate().unwrap();
    effects
}

fn remote(state: &mut State, id: u64, cell: &str) {
    let op = request(state, id, cell);
    let mut effects = on_event(
        state,
        Event::OwnerRead {
            op,
            now_ms: 0,
            result: Ok(Some(OwnerRecord {
                node: Some("peer".into()),
                epoch: 7,
                etag: "owner".into(),
            })),
        },
    );
    if let Some(op) = effects.iter().find_map(|effect| match effect {
        Effect::ReadNodeLease { op, .. } => Some(*op),
        _ => None,
    }) {
        effects = on_event(
            state,
            Event::NodeLeaseRead {
                op,
                now_ms: 0,
                result: Ok(Some(NodeLeaseRecord {
                    node: "peer".into(),
                    addr: "peer:8081".into(),
                    expires_ms: 1_000_000,
                    peer_protocol: 1,
                    generation: "generation".into(),
                    log_state: None,
                    etag: "lease".into(),
                })),
            },
        );
    }
    assert!(effects.iter().any(|effect| matches!(effect,
        Effect::Complete { request, result: Ok(Route::Remote { epoch: 7, .. }) } if *request == id)));
    state.validate().unwrap();
}

#[test]
fn live_remote_routes_reuse_authority_and_evict_the_least_recently_used_record() {
    let mut limits = config();
    limits.max_cached_routes = 2;
    let mut state = State::new("routes", limits);
    remote(&mut state, 1, "Account:a");
    remote(&mut state, 2, "Account:b");
    let reuse = on_event(
        &mut state,
        Event::Request {
            request: 3,
            cell: "Account:a".into(),
        },
    );
    assert!(reuse.iter().any(|effect| matches!(
        effect,
        Effect::Complete {
            request: 3,
            result: Ok(Route::Remote { epoch: 7, .. })
        }
    )));
    assert!(!reuse
        .iter()
        .any(|effect| matches!(effect, Effect::ReadOwner { .. })));
    remote(&mut state, 4, "Account:c");
    assert_eq!(state.cached_routes(), 2);
    assert!(matches!(
        state.phase("Account:a"),
        Some(Phase::Remote { .. })
    ));
    assert!(matches!(
        state.phase("Account:c"),
        Some(Phase::Remote { .. })
    ));
    assert!(state.phase("Account:b").is_none());
    remote(&mut state, 5, "Account:b");
    assert!(state.phase("Account:a").is_none());
    assert_eq!(state.cached_routes(), 2);
}

#[test]
fn disabling_routing_history_preserves_inflight_and_queued_requests() {
    let mut limits = config();
    limits.max_cached_routes = 0;
    let mut state = State::new("pending", limits);
    let mut reads = VecDeque::new();
    for id in 1..=24 {
        let effects = on_event(
            &mut state,
            Event::Request {
                request: id,
                cell: format!("Account:{id}"),
            },
        );
        for effect in effects {
            if let Effect::ReadOwner { op, cell } = effect {
                reads.push_back((op, cell));
            }
        }
    }
    assert_eq!(state.cached_routes(), 0);
    assert_eq!(
        state
            .phase_census()
            .into_iter()
            .map(|(_, n)| n)
            .sum::<usize>(),
        24
    );
    assert_eq!(reads.len(), limits.max_activations);
    let mut completed = 0;
    while let Some((op, cell)) = reads.pop_front() {
        let id = cell.strip_prefix("Account:").unwrap().parse().unwrap();
        let effects = fail(&mut state, id, op);
        completed += 1;
        for effect in effects {
            if let Effect::ReadOwner { op, cell } = effect {
                reads.push_back((op, cell));
            }
        }
        assert!(state.phase(&cell).is_none());
    }
    assert_eq!(completed, 24);
    assert_eq!(
        state
            .phase_census()
            .into_iter()
            .map(|(_, n)| n)
            .sum::<usize>(),
        0
    );
}

#[test]
fn failed_route_history_is_bounded_and_forgotten_operations_stay_stale() {
    let limits = config();
    let mut state = State::new("history", limits);
    let mut first_op = None;
    for id in 1..=2_000 {
        let cell = format!("Account:{id:06}");
        let op = request(&mut state, id, &cell);
        first_op.get_or_insert(op);
        fail(&mut state, id, op);
        assert!(!state.is_active(&cell));
    }
    let retained: usize = state
        .phase_census()
        .into_iter()
        .map(|(_, count)| count)
        .sum();
    assert_eq!(state.occupied(), 0);
    assert_eq!(state.owned_cells(), 0);
    assert!(
        retained <= limits.max_cached_routes,
        "settled failed routes retained {retained} idle cell records"
    );

    let cell = "Account:000001";
    let current = request(&mut state, 2_001, cell);
    assert_ne!(current, first_op.unwrap());
    assert!(on_event(
        &mut state,
        Event::OwnerRead {
            op: first_op.unwrap(),
            now_ms: 0,
            result: Err(Failure::Definite),
        }
    )
    .is_empty());
    assert_eq!(
        state.phase(cell),
        Some(&Phase::ReadingOwner { op: current })
    );
    fail(&mut state, 2_001, current);
}

#[test]
fn expired_queue_history_is_reclaimed_and_old_timers_cannot_expire_new_demand() {
    let mut limits = config();
    limits.max_cached_routes = 0;
    limits.max_activations = 1;
    let mut state = State::new("queue", limits);
    let held = request(&mut state, 1, "Account:held");
    let mut timers = Vec::new();
    for id in 2..=12 {
        for effect in on_event(
            &mut state,
            Event::Request {
                request: id,
                cell: format!("Account:{id}"),
            },
        ) {
            if let Effect::ScheduleTimer {
                timer: timer @ Timer::QueuedActivation { .. },
                ..
            } = effect
            {
                timers.push(timer);
            }
        }
    }
    assert_eq!(timers.len(), 11);
    let old = timers[0].clone();
    for timer in timers {
        on_event(
            &mut state,
            Event::TimerFired {
                timer,
                now_ms: 30_000,
                now_mono_ms: 30_000,
            },
        );
    }
    assert_eq!(
        state
            .phase_census()
            .into_iter()
            .map(|(_, n)| n)
            .sum::<usize>(),
        1,
        "expired waiters must not remain as idle history"
    );
    assert_eq!(
        state.phase("Account:held"),
        Some(&Phase::ReadingOwner { op: held })
    );
    let effects = on_event(
        &mut state,
        Event::Request {
            request: 13,
            cell: "Account:2".into(),
        },
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::ScheduleTimer {
            timer: Timer::QueuedActivation { .. },
            ..
        }
    )));
    let stale = on_event(
        &mut state,
        Event::TimerFired {
            timer: old,
            now_ms: 30_000,
            now_mono_ms: 30_000,
        },
    );
    assert!(!stale
        .iter()
        .any(|effect| matches!(effect, Effect::Complete { request: 13, .. })));
    assert_eq!(state.phase("Account:2"), Some(&Phase::WaitingActivation));
}

#[test]
fn cancelled_queued_requests_do_not_leave_idle_routing_history() {
    let mut limits = config();
    limits.max_cached_routes = 0;
    limits.max_activations = 1;
    let mut state = State::new("cancel", limits);
    let held = request(&mut state, 1, "Account:held");
    for id in 2..=12 {
        let cell = format!("Account:{id}");
        on_event(
            &mut state,
            Event::Request {
                request: id,
                cell: cell.clone(),
            },
        );
        assert_eq!(state.phase(&cell), Some(&Phase::WaitingActivation));
        on_event(&mut state, Event::Cancel { request: id });
        assert!(
            state.phase(&cell).is_none(),
            "cancelled waiter {cell} left an idle routing record"
        );
    }
    assert_eq!(state.cached_routes(), 0);
    assert_eq!(
        state.phase("Account:held"),
        Some(&Phase::ReadingOwner { op: held })
    );
}

#[test]
fn a_zero_route_cache_retains_authority_for_resident_and_dormant_cells() {
    let mut limits = config();
    limits.max_cached_routes = 0;
    limits.ownership_on_evict = OwnershipOnEvict::Sticky;
    let mut state = State::new("owner", limits);
    let cell = "Account:owned";
    let op = request(&mut state, 1, cell);
    on_event(
        &mut state,
        Event::OwnerRead {
            op,
            now_ms: 0,
            result: Ok(Some(OwnerRecord {
                node: Some("owner".into()),
                epoch: 7,
                etag: "owner".into(),
            })),
        },
    );
    for _ in 0..4 {
        let event = match state.phase(cell).cloned().unwrap() {
            Phase::Acquiring { op, .. } => Event::OwnerCasCompleted {
                op,
                result: Ok(CasOutcome::Applied),
            },
            Phase::Restoring { op, .. } => Event::RestoreCompleted {
                op,
                result: Ok(RestoreOutcome {
                    restored: true,
                    alarm: None,
                }),
            },
            Phase::Starting { op, .. } => Event::RuntimeStarted {
                op,
                isolate: Some(cellar_logic::isolate::HeapId::new(1)),
                generation: 1,
                result: Ok(()),
            },
            Phase::Publishing { op, .. } => Event::Published { op, result: Ok(()) },
            phase => panic!("unexpected activation phase {phase:?}"),
        };
        on_event(&mut state, event);
        assert_eq!(state.cached_routes(), 0);
    }
    assert_eq!(state.phase(cell), Some(&Phase::Resident { epoch: 8 }));
    assert_eq!(state.owned_cells(), 1);
    on_event(&mut state, Event::ActivityFinished { request: 1 });
    on_event(&mut state, Event::Evict { cell: cell.into() });
    let Phase::EnsuringDurability { op, .. } = state.phase(cell).cloned().unwrap() else {
        panic!("eviction must prove durability");
    };
    on_event(&mut state, Event::DurabilityChecked { op, result: Ok(()) });
    let Phase::Cleaning { op, .. } = state.phase(cell).cloned().unwrap() else {
        panic!("eviction must stop the runtime");
    };
    on_event(&mut state, Event::RuntimeStopped { op });
    assert_eq!(state.phase(cell), Some(&Phase::Dormant { epoch: 8 }));
    for id in 2..=202 {
        let op = request(&mut state, id, &format!("Account:failed-{id}"));
        fail(&mut state, id, op);
    }
    assert_eq!(state.cached_routes(), 0);
    assert_eq!(
        state
            .phase_census()
            .into_iter()
            .map(|(_, n)| n)
            .sum::<usize>(),
        1
    );
    assert_eq!(state.owned_cells(), 1);
    assert_eq!(state.phase(cell), Some(&Phase::Dormant { epoch: 8 }));
}
