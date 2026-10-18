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
