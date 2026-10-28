// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Settled routing history must not retain every business identity ever seen.

use cellar_logic::{
    on_event, CasOutcome, Config, Effect, Event, Failure, NodeLeaseRecord, OwnerRecord,
    OwnershipOnEvict, Phase, RestoreOutcome, Route, State, Timer,
};
use std::collections::VecDeque;

fn capacity_fixture(cache_limit: usize) -> State {
    capacity_fixture_slots(cache_limit, 1)
}

fn capacity_fixture_slots(cache_limit: usize, max_resident: usize) -> State {
    use cellar_logic::{LeaseCasOutcome, NodeLeaseSpec};
    let mut limits = config();
    limits.max_cached_routes = cache_limit;
    limits.max_resident = max_resident;
    limits.require_node_lease = true;
    let mut state = State::new("origin", limits);
    let effects = on_event(
        &mut state,
        Event::StartNodeLease {
            now_ms: 0,
            now_mono_ms: 0,
            spec: NodeLeaseSpec {
                lineage: None,
                addr: "origin:8081".into(),
                peer_protocol: 1,
                generation: "origin-generation".into(),
                resume_generation: None,
                ttl_ms: 100_000,
            },
        },
    );
    let op = effects
        .iter()
        .find_map(|e| match e {
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
        .find_map(|e| match e {
            Effect::CasNodeLease { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    on_event(
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
    assert!(state.node_authoritative());
    // An unresolved local activation holds the sole residency slot. New
    // unowned cells must consult fleet capacity without evicting that work.
    let op = request(&mut state, 1, "Account:held");
    on_event(
        &mut state,
        Event::OwnerRead {
            op,
            now_ms: 0,
            result: Ok(None),
        },
    );
    assert!(matches!(
        state.phase("Account:held"),
        Some(Phase::Acquiring { .. })
    ));
    state
}

fn capacity_route(state: &mut State, request_id: u64, cell: &str, sample: u64) -> Vec<Effect> {
    capacity_route_with_peers(state, request_id, cell, vec![capacity_peer(sample)])
}

fn capacity_peer(sample: u64) -> cellar_logic::CapacityPeer {
    cellar_logic::CapacityPeer {
        native_container_slots: None,
        node: "peer".into(),
        addr: "peer:8081".into(),
        expires_ms: 100_000,
        peer_protocol: 1,
        sampled_ms: sample,
        owned_cells: Some(1),
        placement_weight: Some(1),
        bucket_format: Some(1),
        resident_cells: 1,
        host_websockets: 0,
        rss_bytes: 1,
        in_use_bytes: Some(1),
        pressured: false,
        memory_headroom: Some(true),
        restoring: 0,
        paced_handoff: true,
        rebalance_paused: false,
        draining: false,
    }
}

fn capacity_route_with_peers(
    state: &mut State,
    request_id: u64,
    cell: &str,
    peers: Vec<cellar_logic::CapacityPeer>,
) -> Vec<Effect> {
    let op = request(state, request_id, cell);
    let effects = on_event(
        state,
        Event::OwnerRead {
            op,
            now_ms: 0,
            result: Ok(None),
        },
    );
    let op = effects
        .iter()
        .find_map(|e| match e {
            Effect::ReadCapacityPeers { op, .. } => Some(*op),
            _ => None,
        })
        .expect("a full origin must consult fleet capacity");
    let effects = on_event(
        state,
        Event::CapacityPeersRead {
            op,
            now_ms: 300,
            result: Ok(peers),
        },
    );
    state.validate().unwrap();
    effects
}

#[test]
fn an_old_refusal_does_not_erase_reservations_against_a_newer_sample() {
    let mut state = capacity_fixture(0);
    capacity_route(&mut state, 2, "Account:old", 100);
    capacity_route(&mut state, 3, "Account:reserved", 200);
    on_event(
        &mut state,
        Event::InvalidateRemote {
            cell: "Account:old".into(),
            node: "peer".into(),
            epoch: 0,
            capacity_sample_ms: Some(100),
        },
    );
    let mut alternative = capacity_peer(200);
    alternative.node = "peer-two".into();
    alternative.addr = "peer-two:8081".into();
    let effects = capacity_route_with_peers(
        &mut state,
        4,
        "Account:next",
        vec![capacity_peer(200), alternative],
    );
    assert!(
        effects.iter().any(|e| matches!(e, Effect::Complete {
        result: Ok(Route::Remote { node, .. }), ..
    } if node == "peer-two")),
        "a delayed refusal erased newer admission reservations"
    );
}

#[test]
fn capacity_refusal_survives_disabling_or_evicting_its_route_cache_entry() {
    for cache_limit in [0, 1] {
        let mut state = capacity_fixture(cache_limit);
        let effects = capacity_route(&mut state, 2, "Account:refused", 100);
        let sample = effects
            .iter()
            .find_map(|e| match e {
                Effect::Complete {
                    result:
                        Ok(Route::Remote {
                            epoch: 0,
                            capacity_sample_ms,
                            ..
                        }),
                    ..
                } => *capacity_sample_ms,
                _ => None,
            })
            .expect("the dispatch must retain its capacity sample");
        if cache_limit == 1 {
            capacity_route(&mut state, 3, "Account:other", 100);
        }
        assert!(state.phase("Account:refused").is_none());
        on_event(
            &mut state,
            Event::InvalidateRemote {
                cell: "Account:refused".into(),
                node: "peer".into(),
                epoch: 0,
                capacity_sample_ms: Some(sample),
            },
        );
        let retry = capacity_route(&mut state, 4, "Account:retry", 100);
        assert!(
            !retry.iter().any(|e| matches!(
                e,
                Effect::Complete {
                    result: Ok(Route::Remote { .. }),
                    ..
                }
            )),
            "reused a refused capacity sample with cache limit {cache_limit}"
        );
        assert_eq!(state.phase("Account:retry"), Some(&Phase::WaitingCapacity));
        assert!(state.cached_routes() <= cache_limit);
    }
}

#[test]
fn delayed_capacity_refusal_preserves_a_newer_sample_and_route() {
    let mut state = capacity_fixture(8);
    capacity_route(&mut state, 2, "Account:old", 100);
    capacity_route(&mut state, 3, "Account:new", 200);
    on_event(
        &mut state,
        Event::InvalidateRemote {
            cell: "Account:new".into(),
            node: "peer".into(),
            epoch: 0,
            capacity_sample_ms: Some(100),
        },
    );
    assert!(matches!(
        state.phase("Account:new"),
        Some(Phase::Remote { .. })
    ));
    let reused = on_event(
        &mut state,
        Event::Request {
            request: 4,
            cell: "Account:new".into(),
        },
    );
    assert!(reused.iter().any(|e| matches!(
        e,
        Effect::Complete {
            result: Ok(Route::Remote {
                capacity_sample_ms: Some(200),
                ..
            }),
            ..
        }
    )));
    // Once the newer observation is refused, an older late callback cannot
    // lower that rejection watermark and make the rejected sample usable.
    for sample in [200, 100] {
        on_event(
            &mut state,
            Event::InvalidateRemote {
                cell: "Account:new".into(),
                node: "peer".into(),
                epoch: 0,
                capacity_sample_ms: Some(sample),
            },
        );
    }
    let retry = capacity_route(&mut state, 5, "Account:blocked", 200);
    assert!(!retry.iter().any(|e| matches!(
        e,
        Effect::Complete {
            result: Ok(Route::Remote { .. }),
            ..
        }
    )));
    let fresh = capacity_route(&mut state, 6, "Account:fresh", 300);
    assert!(fresh.iter().any(|e| matches!(
        e,
        Effect::Complete {
            result: Ok(Route::Remote {
                capacity_sample_ms: Some(300),
                ..
            }),
            ..
        }
    )));
    state.validate().unwrap();
}

#[test]
fn a_capacity_hint_cannot_invalidate_owner_authority_or_an_unobserved_sample() {
    let mut state = capacity_fixture(8);
    remote(&mut state, 2, "Account:owned");
    for (epoch, capacity_sample_ms) in [(0, Some(100)), (7, Some(100)), (6, None)] {
        on_event(
            &mut state,
            Event::InvalidateRemote {
                cell: "Account:owned".into(),
                node: "peer".into(),
                epoch,
                capacity_sample_ms,
            },
        );
        assert!(matches!(
            state.phase("Account:owned"),
            Some(Phase::Remote { epoch: 7, .. })
        ));
    }
    on_event(
        &mut state,
        Event::InvalidateRemote {
            cell: "Account:owned".into(),
            node: "peer".into(),
            epoch: 7,
            capacity_sample_ms: None,
        },
    );
    assert_eq!(state.phase("Account:owned"), Some(&Phase::Inactive));
    capacity_route(&mut state, 3, "Account:capacity", 100);
    for sample in [None, Some(0), Some(200)] {
        on_event(
            &mut state,
            Event::InvalidateRemote {
                cell: "Account:capacity".into(),
                node: "peer".into(),
                epoch: 0,
                capacity_sample_ms: sample,
            },
        );
        assert!(matches!(
            state.phase("Account:capacity"),
            Some(Phase::Remote { epoch: 0, .. })
        ));
    }
    state.validate().unwrap();
}

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
                lineage: None,
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
                    lineage: None,
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
        Some(&Phase::ReadingOwner {
            op: current,
            reconciles: 0
        })
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
        Some(&Phase::ReadingOwner {
            op: held,
            reconciles: 0
        })
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
        Some(&Phase::ReadingOwner {
            op: held,
            reconciles: 0
        })
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
                lineage: None,
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
    let observed = state.owned_cell_page(None, 1);
    assert_eq!(observed.items.len(), 1);
    assert_eq!(observed.items[0].scope, cell);
    assert_eq!(observed.items[0].epoch, 8);
    assert_eq!(observed.items[0].phase, "resident");
    assert_eq!(state.owned_cell_page(Some(cell), 100).items.len(), 0);
    assert_eq!(state.owned_cells(), 1);
    on_event(&mut state, Event::ActivityFinished { request: 1 });
    let rejected = on_event(
        &mut state,
        Event::EvictRequestedAtEpoch {
            request: 900,
            cell: cell.into(),
            epoch: 7,
            now_mono_ms: 1,
        },
    );
    assert!(rejected.contains(&Effect::EvictionAdmission {
        request: 900,
        result: Err(cellar_logic::EvictError::Refused(
            cellar_logic::EvictRefusal::EpochChanged
        )),
    }));
    assert_eq!(state.phase(cell), Some(&Phase::Resident { epoch: 8 }));
    let admitted = on_event(
        &mut state,
        Event::EvictRequestedAtEpoch {
            request: 901,
            cell: cell.into(),
            epoch: 8,
            now_mono_ms: 2,
        },
    );
    assert!(admitted.contains(&Effect::EvictionAdmission {
        request: 901,
        result: Ok(cellar_logic::EvictionAdmission::Pending),
    }));
    let Phase::EnsuringDurability { op, .. } = state.phase(cell).cloned().unwrap() else {
        panic!("eviction must prove durability");
    };
    on_event(&mut state, Event::DurabilityChecked { op, result: Ok(()) });
    let Phase::Cleaning { op, .. } = state.phase(cell).cloned().unwrap() else {
        panic!("eviction must stop the runtime");
    };
    let finished = on_event(&mut state, Event::RuntimeStopped { op });
    assert!(finished.contains(&Effect::EvictionFinished {
        request: 901,
        result: Ok(cellar_logic::EvictSuccess::Evicted),
    }));
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

#[test]
fn terminal_adoption_failures_advance_drain_progress_without_counting_stale_replies() {
    let mut limits = config();
    limits.max_releases = 1;
    limits.ownership_on_evict = OwnershipOnEvict::Sticky;
    let mut state = State::new("owner", limits);
    for request_id in 1..=3 {
        let cell = format!("Account:drain-{request_id}");
        let op = request(&mut state, request_id, &cell);
        on_event(
            &mut state,
            Event::OwnerRead {
                op,
                now_ms: 0,
                result: Ok(Some(OwnerRecord {
                    lineage: None,
                    node: Some("owner".into()),
                    epoch: 7,
                    etag: "owner".into(),
                })),
            },
        );
        for _ in 0..4 {
            let event = match state.phase(&cell).cloned().unwrap() {
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
        }
        on_event(
            &mut state,
            Event::ActivityFinished {
                request: request_id,
            },
        );
    }
    let mut effects = on_event(&mut state, Event::ReleaseAll);
    for expected in 1..=3 {
        assert_eq!(state.settled_handoffs(), expected - 1);
        assert!(state.handoffs_in_flight() <= 1);
        loop {
            let next = effects.iter().find_map(|effect| match effect {
                Effect::EnsureDurable { op, .. } => Some(Event::DurabilityChecked {
                    op: *op,
                    result: Ok(()),
                }),
                Effect::StopRuntime { op, .. } => Some(Event::RuntimeStopped { op: *op }),
                Effect::ReleaseOwner { op, .. } => Some(Event::OwnerReleased {
                    op: *op,
                    result: Ok(CasOutcome::Applied),
                }),
                _ => None,
            });
            let Some(next) = next else {
                break;
            };
            effects = on_event(&mut state, next);
            assert_eq!(
                state.settled_handoffs(),
                expected - 1,
                "nonterminal release work was counted as settled"
            );
        }
        let op = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::AdoptReleased { op, .. } => Some(*op),
                _ => None,
            })
            .expect("released cell must await successor adoption");
        assert_eq!(state.adopting(), 1);
        effects = on_event(
            &mut state,
            Event::SuccessorAdopted {
                op,
                result: Err(Failure::Definite),
            },
        );
        assert_eq!(state.settled_handoffs(), expected);
        assert_eq!(
            state.handed_off(),
            0,
            "a failed acknowledgement became a successful adoption"
        );
        on_event(
            &mut state,
            Event::SuccessorAdopted {
                op,
                result: Err(Failure::Definite),
            },
        );
        assert_eq!(
            state.settled_handoffs(),
            expected,
            "stale adoption reply counted twice"
        );
        state.validate().unwrap();
    }
    assert_eq!(state.owned_cells(), 0);
    assert_eq!(state.handoffs_in_flight(), 0);
}

#[test]
fn missing_owner_lease_requires_recovery_before_claiming() {
    let mut limits = config();
    limits.owner_log_recovery_attempts = 1;
    let mut state = State::new("replacement", limits);
    let op = request(&mut state, 1, "Account:missing-proof");
    let effects = on_event(
        &mut state,
        Event::OwnerRead {
            op,
            now_ms: 0,
            result: Ok(Some(OwnerRecord {
                lineage: None,
                node: Some("missing-leader".into()),
                epoch: 7,
                etag: "original-owner".into(),
            })),
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadNodeLease { op, .. } => Some(*op),
            _ => None,
        })
        .expect("a foreign owner must read its lease");
    let effects = on_event(
        &mut state,
        Event::NodeLeaseRead {
            op,
            now_ms: 0,
            result: Ok(None),
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::RecoverNodeLog { op, owner, .. } if owner == "missing-leader" => Some(*op),
            _ => None,
        })
        .expect("missing metadata must not bypass the recovery interlock");
    assert!(matches!(
        state.phase("Account:missing-proof"),
        Some(Phase::RecoveringOwnerLog { .. })
    ));
    state.validate().unwrap();
    let effects = on_event(
        &mut state,
        Event::NodeLogRecovered {
            op,
            result: Err(Failure::Ambiguous),
        },
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::Complete {
            request: 1,
            result: Err(_),
            ..
        }
    )));
    assert!(matches!(
        state.phase("Account:missing-proof"),
        Some(Phase::Inactive)
    ));
    state.validate().unwrap();
}

#[test]
fn ambiguous_takeover_preserves_the_acquire_reconciliation_bound() {
    for (log_state, transient_recovery_failure) in [
        (None, false),
        (Some(cellar_logic::log_tier::LogState::Sealed), false),
        (Some(cellar_logic::log_tier::LogState::Open), false),
        (Some(cellar_logic::log_tier::LogState::Open), true),
    ] {
        let mut state = State::new("replacement", config());
        let cell = "Account:ambiguous-takeover";
        let op = request(&mut state, 1, cell);
        let owner = OwnerRecord {
            lineage: None,
            node: Some("predecessor".into()),
            epoch: 7,
            etag: "unchanged-owner".into(),
        };
        let mut events = VecDeque::from([Event::OwnerRead {
            op,
            now_ms: 1,
            result: Ok(Some(owner.clone())),
        }]);
        let mut attempts = 0;
        let mut failed_recovery_attempt = None;
        let mut now = 1;
        let mut completed = false;
        while let Some(event) = events.pop_front() {
            for effect in on_event(&mut state, event) {
                match effect {
                    Effect::ReadOwner { op, .. } => events.push_back(Event::OwnerRead {
                        op,
                        now_ms: now,
                        result: Ok(Some(owner.clone())),
                    }),
                    Effect::ReadNodeLease { op, .. } => events.push_back(Event::NodeLeaseRead {
                        op,
                        now_ms: now,
                        result: Ok(Some(NodeLeaseRecord {
                            lineage: None,
                            node: "predecessor".into(),
                            addr: "predecessor:8081".into(),
                            expires_ms: 0,
                            peer_protocol: 1,
                            generation: "prior-generation".into(),
                            log_state,
                            etag: "expired-lease".into(),
                        })),
                    }),
                    Effect::CasOwner { op, .. } => {
                        attempts += 1;
                        assert!(
                            attempts <= cellar_logic::MAX_ACQUIRE_RECONCILES + 1,
                            "unchanged foreign ownership reset the reconciliation budget"
                        );
                        events.push_back(Event::OwnerCasCompleted {
                            op,
                            result: Err(Failure::Ambiguous),
                        });
                    }
                    Effect::RecoverNodeLog { op, .. } => {
                        let result = if transient_recovery_failure
                            && attempts > 0
                            && failed_recovery_attempt != Some(attempts)
                        {
                            failed_recovery_attempt = Some(attempts);
                            Err(Failure::Definite)
                        } else {
                            Ok(())
                        };
                        events.push_back(Event::NodeLogRecovered { op, result });
                    }
                    Effect::ScheduleTimer {
                        timer: timer @ Timer::OwnerLogRecoveryRetry { .. },
                        at_mono_ms,
                    } => {
                        now = at_mono_ms + 1;
                        events.push_back(Event::TimerFired {
                            timer,
                            now_ms: now,
                            now_mono_ms: at_mono_ms,
                        });
                    }
                    Effect::Complete {
                        request: 1, result, ..
                    } => {
                        assert_eq!(result, Err(cellar_logic::RequestError::AcquireFailed));
                        completed = true;
                    }
                    Effect::Restore { .. } => panic!("unproven ownership admitted a writer"),
                    _ => {}
                }
            }
            state.validate().unwrap();
        }
        assert!(completed, "unconfirmed takeover did not settle its request");
        assert_eq!(attempts, cellar_logic::MAX_ACQUIRE_RECONCILES + 1);
        assert_eq!(state.phase(cell), Some(&Phase::Inactive));
    }
}

#[test]
fn an_ambiguous_takeover_readback_retains_the_predecessor_restore() {
    let mut state = State::new("replacement", config());
    let cell = "Account:applied-takeover";
    let op = request(&mut state, 1, cell);
    let effects = on_event(
        &mut state,
        Event::OwnerRead {
            op,
            now_ms: 1,
            result: Ok(Some(OwnerRecord {
                lineage: None,
                node: Some("predecessor".into()),
                epoch: 7,
                etag: "prior-owner".into(),
            })),
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadNodeLease { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        &mut state,
        Event::NodeLeaseRead {
            op,
            now_ms: 1,
            result: Ok(Some(NodeLeaseRecord {
                lineage: None,
                node: "predecessor".into(),
                addr: "predecessor:8081".into(),
                expires_ms: 0,
                peer_protocol: 1,
                generation: "prior-generation".into(),
                log_state: Some(cellar_logic::log_tier::LogState::Sealed),
                etag: "expired-lease".into(),
            })),
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CasOwner { op, epoch: 8, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        &mut state,
        Event::OwnerCasCompleted {
            op,
            result: Err(Failure::Ambiguous),
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadOwner { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        &mut state,
        Event::OwnerRead {
            op,
            now_ms: 1,
            result: Ok(Some(OwnerRecord {
                lineage: None,
                node: Some("replacement".into()),
                epoch: 8,
                etag: "applied-owner".into(),
            })),
        },
    );
    assert!(effects.iter().any(|effect|matches!(effect, Effect::Restore { spec, .. }
        if spec.epoch == 8 && !spec.fresh && spec.took_over && spec.prior.as_deref() == Some("predecessor"))));
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::CasOwner { .. } | Effect::Complete { .. })));
    state.validate().unwrap();
}

#[test]
fn a_reused_node_name_cannot_route_or_claim_another_lineage() {
    for expires_ms in [0, 100_000] {
        let mut state = State::new("replacement", config());
        let op = request(&mut state, 1, "Account:old-lineage");
        let effects = on_event(
            &mut state,
            Event::OwnerRead {
                op,
                now_ms: 1,
                result: Ok(Some(OwnerRecord {
                    node: Some("replacement".into()),
                    lineage: Some("old".into()),
                    epoch: 7,
                    etag: "owner".into(),
                })),
            },
        );
        let op = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReadNodeLease { op, .. } => Some(*op),
                _ => None,
            })
            .expect("a reused name must check durable lineage before claiming");
        let effects = on_event(
            &mut state,
            Event::NodeLeaseRead {
                op,
                now_ms: 1,
                result: Ok(Some(NodeLeaseRecord {
                    node: "replacement".into(),
                    lineage: Some("fresh".into()),
                    addr: "replacement:8080".into(),
                    expires_ms,
                    peer_protocol: 1,
                    generation: "new-process".into(),
                    log_state: None,
                    etag: "lease".into(),
                })),
            },
        );
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete {
                request: 1,
                result: Err(_),
                ..
            }
        )));
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::CasOwner { .. } | Effect::Restore { .. } | Effect::RecoverNodeLog { .. }
        )));
        state.validate().unwrap();
        let op = request(&mut state, 2, "Account:old-lineage");
        let effects = on_event(
            &mut state,
            Event::OwnerRead {
                op,
                now_ms: 2,
                result: Ok(Some(OwnerRecord {
                    node: Some("replacement".into()),
                    lineage: Some("old".into()),
                    epoch: 8,
                    etag: "restored-owner".into(),
                })),
            },
        );
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::ReadNodeLease { .. })),
            "a mismatched lineage stayed cached instead of rereading restored metadata"
        );
        state.validate().unwrap();
    }
}

#[test]
fn a_preserved_lineage_keeps_same_name_activation_available() {
    let mut state = State::new_with_lineage("replacement", config(), Some("retained".into()));
    let op = request(&mut state, 1, "Account:retained-lineage");
    let effects = on_event(
        &mut state,
        Event::OwnerRead {
            op,
            now_ms: 0,
            result: Ok(Some(OwnerRecord {
                node: Some("replacement".into()),
                lineage: Some("retained".into()),
                epoch: 7,
                etag: "owner".into(),
            })),
        },
    );
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::CasOwner { epoch: 8, .. })));
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        Effect::ReadNodeLease { .. } | Effect::RecoverNodeLog { .. }
    )));
    state.validate().unwrap();
}

fn native_lookup(state: &mut State, request: u64, cell: &str, handoff: bool) -> Vec<Effect> {
    let effects = on_event(
        state,
        Event::NativeSandboxRequestAt {
            request,
            cell: cell.into(),
            capacity_handoff: handoff,
            available: 0,
            now_mono_ms: 300,
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadOwner { op, .. } => Some(*op),
            _ => None,
        })
        .expect("read ownership before placement");
    on_event(
        state,
        Event::OwnerRead {
            op,
            now_ms: 300,
            result: Ok(None),
        },
    )
}

#[test]
fn native_placement_uses_class_slots_and_reserves_each_advisory_sample() {
    let mut state = capacity_fixture_slots(8, 4);
    let mut room = capacity_peer(200);
    room.node = "room".into();
    room.native_container_slots = Some([("Sandbox".into(), 2)].into());
    let mut full = capacity_peer(200);
    full.node = "full".into();
    full.native_container_slots = Some([("Sandbox".into(), 0)].into());
    full.resident_cells = 0;
    let mut wrong_class = capacity_peer(200);
    wrong_class.native_container_slots = Some([("Other".into(), 10)].into());
    let mut draining = room.clone();
    draining.node = "draining".into();
    draining.draining = true;
    for index in 0..3 {
        let effects = native_lookup(&mut state, 10 + index, &format!("Sandbox:{index}"), false);
        let op = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReadCapacityPeers { op, .. } => Some(*op),
                _ => None,
            })
            .expect("container exhaustion consults peers even with free cell slots");
        let effects = on_event(
            &mut state,
            Event::CapacityPeersRead {
                op,
                now_ms: 300,
                result: Ok(vec![
                    capacity_peer(200),
                    full.clone(),
                    wrong_class.clone(),
                    draining.clone(),
                    room.clone(),
                ]),
            },
        );
        if index < 2 {
            assert!(effects.iter().any(|effect|matches!(effect,Effect::Complete {result:Ok(Route::Remote {node,..}),..} if node=="room")));
        } else {
            assert!(effects.iter().any(|effect| matches!(
                effect,
                Effect::Complete {
                    result: Err(cellar_logic::RequestError::CapacityExhausted),
                    ..
                }
            )));
            assert!(!effects
                .iter()
                .any(|effect| matches!(effect, Effect::CasOwner { .. })));
        }
        state.validate().unwrap();
    }
    // Cleanup is ordinary demand, so it can acquire an execution identity on a
    // full Engine and prove resource absence without needing a creation slot.
    let op = request(&mut state, 20, "Sandbox:cleanup");
    let effects = on_event(
        &mut state,
        Event::OwnerRead {
            op,
            now_ms: 300,
            result: Ok(None),
        },
    );
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::CasOwner { .. })));
}

#[test]
fn native_capacity_handoff_refuses_before_claim_and_cleanup_can_join_a_start() {
    let mut state = capacity_fixture_slots(8, 4);
    let effects = native_lookup(&mut state, 10, "Sandbox:full", true);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::Complete {
            request: 10,
            result: Err(cellar_logic::RequestError::CapacityExhausted)
        }
    )));
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        Effect::CasOwner { .. } | Effect::ReadCapacityPeers { .. }
    )));
    let effects = on_event(
        &mut state,
        Event::NativeSandboxRequestAt {
            request: 11,
            cell: "Sandbox:joined".into(),
            capacity_handoff: false,
            available: 0,
            now_mono_ms: 300,
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadOwner { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    on_event(
        &mut state,
        Event::RequestAt {
            request: 12,
            cell: "Sandbox:joined".into(),
            now_mono_ms: 300,
        },
    );
    let effects = on_event(
        &mut state,
        Event::OwnerRead {
            op,
            now_ms: 300,
            result: Ok(None),
        },
    );
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::CasOwner { .. })),
        "creation capacity must not strand cleanup"
    );
    state.validate().unwrap();
}
