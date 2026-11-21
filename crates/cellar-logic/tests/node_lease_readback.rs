// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A lost response retains its original candidate; readback cannot revive a gap.
use cellar_logic::{
    log_tier::LogState, on_event, Config, Effect, Event, Failure, HaltReason, LeaseCasOutcome,
    NodeLeaseRecord, NodeLeaseSpec, OwnershipOnEvict, State, Timer,
};

fn read_op(effects: &[Effect]) -> u64 {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadSelfNodeLease { op } => Some(*op),
            _ => None,
        })
        .expect("lease read")
}

fn write(effects: &[Effect]) -> (u64, NodeLeaseRecord) {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CasNodeLease { op, record, .. } => Some((*op, record.clone())),
            _ => None,
        })
        .expect("lease write")
}

fn renew(effects: &[Effect]) -> Timer {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ScheduleTimer {
                timer: Timer::NodeLeaseRenew { generation },
                ..
            } => Some(Timer::NodeLeaseRenew {
                generation: *generation,
            }),
            _ => None,
        })
        .expect("renewal timer")
}

fn after_lost_write_and_rejected_retry(stamp: Option<LogState>) -> (State, u64, NodeLeaseRecord) {
    let mut state = State::new(
        "owner",
        Config {
            max_resident: 16,
            max_cached_routes: 16,
            max_activations: 8,
            max_evictions: 4,
            max_releases: 4,
            max_outbound_websockets: 8,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: true,
            peer_protocol: 1,
            operation_deadline_ms: Some(10_000),
            owner_log_recovery_backoff_ms: 50,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 100,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
    );
    let effects = on_event(
        &mut state,
        Event::StartNodeLease {
            now_ms: 10_000,
            now_mono_ms: 10_000,
            spec: NodeLeaseSpec {
                lineage: Some("fixture-lineage".into()),
                addr: "owner:8081".into(),
                peer_protocol: 1,
                generation: "generation".into(),
                resume_generation: None,
                ttl_ms: 1_000,
            },
        },
    );
    let op = read_op(&effects);
    let effects = on_event(
        &mut state,
        Event::SelfNodeLeaseRead {
            op,
            now_ms: 10_000,
            now_mono_ms: 10_000,
            result: Ok(None),
        },
    );
    let (op, mut prior) = write(&effects);
    prior.etag = "prior".into();
    let effects = on_event(
        &mut state,
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: 10_000,
            result: Ok(LeaseCasOutcome::Applied {
                etag: prior.etag.clone(),
            }),
            stamped_log_state: None,
        },
    );
    let timer = renew(&effects);
    on_event(&mut state, Event::LocalCellsRead { result: Ok(vec![]) });
    let effects = on_event(
        &mut state,
        Event::TimerFired {
            timer,
            now_ms: 10_333,
            now_mono_ms: 10_333,
        },
    );
    let (op, mut late) = write(&effects);
    assert_eq!(late.expires_ms, 11_333);
    late.etag = "late-original-write".into();
    let effects = on_event(
        &mut state,
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: 10_667,
            result: Err(Failure::Ambiguous),
            stamped_log_state: None,
        },
    );
    // The physical write is still running: this old read is not proof that
    // it cannot commit after readback and before the next conditional write.
    let op = read_op(&effects);
    on_event(
        &mut state,
        Event::SelfNodeLeaseRead {
            op,
            now_ms: 10_668,
            now_mono_ms: 10_668,
            result: Ok(Some(prior)),
        },
    );
    let effects = on_event(
        &mut state,
        Event::NudgeNodeLease {
            now_ms: 10_700,
            now_mono_ms: 10_700,
        },
    );
    let (op, next) = write(&effects);
    assert_eq!(next.expires_ms, 11_700);
    // The original write won the ETag. A new folded-log state was sampled
    // for this rejected attempt; it does not change the original candidate.
    let effects = on_event(
        &mut state,
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: 10_701,
            result: Ok(LeaseCasOutcome::Rejected),
            stamped_log_state: stamp,
        },
    );
    state.validate().unwrap();
    (state, read_op(&effects), late)
}

fn observed(
    state: &mut State,
    op: u64,
    record: NodeLeaseRecord,
    wall: u64,
    mono: u64,
) -> Vec<Effect> {
    on_event(
        state,
        Event::SelfNodeLeaseRead {
            op,
            now_ms: wall,
            now_mono_ms: mono,
            result: Ok(Some(record)),
        },
    )
}

fn assert_fenced(state: &State, effects: &[Effect], expected: HaltReason) {
    assert!(state.is_fenced());
    assert!(!state.node_authoritative());
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::Halt { reason, .. } if *reason == expected)));
    state.validate().unwrap();
}

#[test]
fn original_ambiguous_candidate_survives_a_changed_retry_stamp() {
    let (mut state, op, late) = after_lost_write_and_rejected_retry(Some(LogState::Open));
    let effects = observed(&mut state, op, late, 10_702, 10_702);
    assert!(
        !state.is_fenced(),
        "forgot the exact original ambiguous candidate"
    );
    assert!(state.node_authoritative());
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::Halt { .. })));
    let deadline = effects.iter().find_map(|effect| match effect {
        Effect::ScheduleTimer {
            timer: Timer::NodeLeaseFence { .. },
            at_mono_ms,
        } => Some(*at_mono_ms),
        _ => None,
    });
    assert_eq!(deadline, Some(11_333));
    state.validate().unwrap();
}

#[test]
fn same_stamp_late_candidate_preserves_existing_adoption() {
    let (mut state, op, late) = after_lost_write_and_rejected_retry(None);
    let effects = observed(&mut state, op, late, 10_702, 10_702);
    assert!(!state.is_fenced());
    assert!(state.node_authoritative());
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::Halt { .. })));
    state.validate().unwrap();
}

#[test]
fn late_candidate_cannot_revive_expired_monotonic_authority() {
    let (mut state, op, late) = after_lost_write_and_rejected_retry(None);
    on_event(
        &mut state,
        Event::RequestAt {
            request: 1,
            cell: "Counter:refused-after-expiry".into(),
            now_mono_ms: 11_001,
        },
    );
    assert!(!state.node_authoritative());
    let effects = observed(&mut state, op, late, 11_001, 11_001);
    assert_fenced(&state, &effects, HaltReason::NodeLeaseExpired);
}

#[test]
fn late_candidate_cannot_cross_the_prior_published_wall_deadline() {
    let (mut state, op, late) = after_lost_write_and_rejected_retry(None);
    let effects = observed(&mut state, op, late, 11_001, 10_702);
    assert_fenced(&state, &effects, HaltReason::NodeLeaseExpired);
}

#[test]
fn recovery_stamps_and_foreign_identity_still_refuse() {
    for change in 0..7 {
        let (mut state, op, mut late) = after_lost_write_and_rejected_retry(None);
        match change {
            0 => late.log_state = Some(LogState::Recovering),
            1 => late.log_state = Some(LogState::Sealed),
            2 => late.generation = "foreign-generation".into(),
            3 => late.lineage = Some("foreign-lineage".into()),
            4 => late.node = "foreign-node".into(),
            5 => late.addr = "foreign:8081".into(),
            6 => late.peer_protocol = 2,
            _ => unreachable!(),
        }
        let effects = observed(&mut state, op, late, 10_702, 10_702);
        assert_fenced(&state, &effects, HaltReason::NodeLeaseMismatch);
    }
}

fn original_prior(late: &NodeLeaseRecord) -> NodeLeaseRecord {
    let mut prior = late.clone();
    prior.expires_ms = 11_000;
    prior.etag = "prior".into();
    prior.log_state = None;
    prior
}

#[test]
fn rejected_attempt_does_not_authorize_an_unwritten_stamp_or_expiry() {
    for change in 0..3 {
        let (mut state, op, mut late) = after_lost_write_and_rejected_retry(Some(LogState::Open));
        match change {
            0 => late.log_state = Some(LogState::Open),
            1 => late.expires_ms = 11_500,
            2 => late.expires_ms = 11_332,
            _ => unreachable!(),
        }
        let effects = observed(&mut state, op, late, 10_702, 10_702);
        assert_fenced(&state, &effects, HaltReason::NodeLeaseMismatch);
    }
}

#[test]
fn multiple_ambiguous_attempts_keep_both_ordinary_stamps_and_the_earliest_bound() {
    for stamp in [None, Some(LogState::Open)] {
        for choose_original in [false, true] {
            let (mut state, op, original) =
                after_lost_write_and_rejected_retry(Some(LogState::Open));
            let prior = original_prior(&original);
            observed(&mut state, op, prior.clone(), 10_702, 10_702);
            let effects = on_event(
                &mut state,
                Event::NudgeNodeLease {
                    now_ms: 10_750,
                    now_mono_ms: 10_750,
                },
            );
            let (op, mut later) = write(&effects);
            later.log_state = stamp;
            later.etag = "another-possible-winner".into();
            let effects = on_event(
                &mut state,
                Event::NodeLeaseCasCompleted {
                    op,
                    now_mono_ms: 10_751,
                    result: Err(Failure::Ambiguous),
                    stamped_log_state: stamp,
                },
            );
            observed(&mut state, read_op(&effects), prior, 10_752, 10_752);
            let effects = on_event(
                &mut state,
                Event::NudgeNodeLease {
                    now_ms: 10_800,
                    now_mono_ms: 10_800,
                },
            );
            let (op, _) = write(&effects);
            let effects = on_event(
                &mut state,
                Event::NodeLeaseCasCompleted {
                    op,
                    now_mono_ms: 10_801,
                    result: Ok(LeaseCasOutcome::Rejected),
                    stamped_log_state: Some(LogState::Open),
                },
            );
            let candidate = if choose_original { original } else { later };
            let effects = observed(&mut state, read_op(&effects), candidate, 10_802, 10_802);
            assert!(!state.is_fenced());
            assert!(state.node_authoritative());
            let deadline = if choose_original || stamp.is_none() {
                11_333
            } else {
                11_750
            };
            assert!(effects.iter().any(|effect| matches!(effect,
                Effect::ScheduleTimer { timer: Timer::NodeLeaseFence { .. }, at_mono_ms }
                if *at_mono_ms == deadline)));
            state.validate().unwrap();
        }
    }
}

#[test]
fn changed_etag_invalidates_unresolved_old_guards() {
    let (mut state, op, late) = after_lost_write_and_rejected_retry(Some(LogState::Open));
    let mut prior = original_prior(&late);
    prior.etag = "new-guard-with-same-body".into();
    observed(&mut state, op, prior, 10_702, 10_702);
    let effects = on_event(
        &mut state,
        Event::NudgeNodeLease {
            now_ms: 10_750,
            now_mono_ms: 10_750,
        },
    );
    let (op, _) = write(&effects);
    let effects = on_event(
        &mut state,
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: 10_751,
            result: Ok(LeaseCasOutcome::Rejected),
            stamped_log_state: Some(LogState::Open),
        },
    );
    let effects = observed(&mut state, read_op(&effects), late, 10_752, 10_752);
    assert_fenced(&state, &effects, HaltReason::NodeLeaseMismatch);
}

#[test]
fn a_stale_read_sample_cannot_reverse_observed_expiry() {
    let (mut state, op, late) = after_lost_write_and_rejected_retry(None);
    on_event(
        &mut state,
        Event::RequestAt {
            request: 1,
            cell: "Counter:refused-after-expiry".into(),
            now_mono_ms: 11_001,
        },
    );
    let effects = observed(&mut state, op, late, 10_702, 10_702);
    assert_fenced(&state, &effects, HaltReason::NodeLeaseExpired);
}

#[test]
fn matching_current_body_cannot_replace_an_older_candidates_sample() {
    let (mut state, op, late) = after_lost_write_and_rejected_retry(Some(LogState::Open));
    let prior = original_prior(&late);
    observed(&mut state, op, prior, 10_702, 10_702);
    // A backward wall step makes the retry serialize the original expiry.
    let effects = on_event(
        &mut state,
        Event::NudgeNodeLease {
            now_ms: 10_333,
            now_mono_ms: 10_800,
        },
    );
    let (op, current) = write(&effects);
    assert_eq!(current.expires_ms, late.expires_ms);
    let effects = on_event(
        &mut state,
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: 10_801,
            result: Ok(LeaseCasOutcome::Rejected),
            stamped_log_state: None,
        },
    );
    let effects = observed(&mut state, read_op(&effects), late, 10_702, 10_802);
    assert!(state.node_authoritative());
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::ScheduleTimer {
            timer: Timer::NodeLeaseFence { .. },
            at_mono_ms: 11_333
        }
    )));
    state.validate().unwrap();
}

#[test]
fn applied_result_cannot_cross_a_wall_deadline_already_observed_elsewhere() {
    let (mut state, op, late) = after_lost_write_and_rejected_retry(None);
    let prior = original_prior(&late);
    observed(&mut state, op, prior, 10_702, 10_702);
    let effects = on_event(
        &mut state,
        Event::NudgeNodeLease {
            now_ms: 10_750,
            now_mono_ms: 10_750,
        },
    );
    let (op, _) = write(&effects);
    on_event(
        &mut state,
        Event::TimerFired {
            timer: Timer::NodeLeaseFence {
                generation: u64::MAX,
            },
            now_ms: 11_000,
            now_mono_ms: 10_751,
        },
    );
    assert!(!state.node_authoritative());
    let effects = on_event(
        &mut state,
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: 10_752,
            result: Ok(LeaseCasOutcome::Applied {
                etag: "late-applied".into(),
            }),
            stamped_log_state: None,
        },
    );
    assert_fenced(&state, &effects, HaltReason::NodeLeaseExpired);
}
