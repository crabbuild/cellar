//! Output-proof failure may overlap a still-running alarm on the same cell.
use cellar_logic::{
    on_event, wake::AlarmSnapshot, CasOutcome, Channel, Config, Effect, Event, Failure,
    OwnerRecord, OwnershipOnEvict, Phase, ProofSource, RequestError, RestoreOutcome, State,
    StopCause, Timer,
};

const CELL: &str = "__Queue:reset";

fn resident() -> State {
    let mut state = State::new(
        "owner",
        Config {
            max_resident: 8,
            max_cached_routes: 8,
            max_activations: 8,
            max_evictions: 4,
            max_releases: 4,
            max_outbound_websockets: 8,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: false,
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
        Event::Request {
            request: 1,
            cell: CELL.into(),
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
        let event = match state.phase(CELL).cloned().unwrap() {
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
                isolate: None,
                generation: 1,
                result: Ok(()),
            },
            Phase::Publishing { op, .. } => Event::Published { op, result: Ok(()) },
            phase => panic!("unexpected activation {phase:?}"),
        };
        on_event(&mut state, event);
    }
    assert_eq!(state.phase(CELL), Some(&Phase::Resident { epoch: 8 }));
    state
}

#[test]
fn failed_output_proof_retires_running_and_gated_alarms_before_reset() {
    for alarm_gated in [false, true] {
        let mut state = resident();
        let armed = on_event(
            &mut state,
            Event::AlarmObserved {
                cell: CELL.into(),
                alarm: AlarmSnapshot::without_wake(Some(1_000)),
                covered: true,
                now_ms: 1_000,
                now_mono_ms: 1_000,
            },
        );
        let timer = armed
            .into_iter()
            .find_map(|effect| match effect {
                Effect::ScheduleTimer {
                    timer: timer @ Timer::CellAlarm { .. },
                    ..
                } => Some(timer),
                _ => None,
            })
            .unwrap();
        let fired = on_event(
            &mut state,
            Event::TimerFired {
                timer,
                now_ms: 1_000,
                now_mono_ms: 1_000,
            },
        );
        let alarm = fired
            .iter()
            .find_map(|effect| match effect {
                Effect::FireAlarm { op, .. } => Some(*op),
                _ => None,
            })
            .unwrap();
        if alarm_gated {
            let gated = on_event(
                &mut state,
                Event::AlarmFinished {
                    op: alarm,
                    cell: CELL.into(),
                    epoch: 8,
                    now_ms: 1_000,
                    now_mono_ms: 1_000,
                    result: Ok((AlarmSnapshot::without_wake(None), false, Some(1))),
                },
            );
            assert!(gated
                .iter()
                .any(|effect| matches!(effect, Effect::AwaitDurable { .. })));
        }
        let output = on_event(
            &mut state,
            Event::Output {
                request: 1,
                channel: Channel::CellRpc,
                position: Some(2),
                observed: None,
                epoch: Some(8),
            },
        );
        let proof = output
            .iter()
            .find_map(|effect| match effect {
                Effect::AwaitDurable { op, .. } => Some(*op),
                _ => None,
            })
            .unwrap();
        let failed = on_event(
            &mut state,
            Event::DurableReached {
                op: proof,
                result: Err(Failure::Ambiguous),
                source: ProofSource::Bucket,
            },
        );
        assert!(failed.iter().any(|effect| matches!(
            effect,
            Effect::Release {
                request: 1,
                result: Err(RequestError::DurabilityUnproven),
                ..
            }
        )));
        let stop = failed
            .iter()
            .find_map(|effect| match effect {
                Effect::StopRuntime {
                    op,
                    cause: StopCause::Reset,
                    bounded: false,
                    ..
                } => Some(*op),
                _ => None,
            })
            .expect("failed proof must discard the runtime");
        assert!(
            !failed
                .iter()
                .any(|effect| matches!(effect, Effect::ReconcileWakeEntry { .. })),
            "unproven alarm consumption must not retire its discoverable wake: {failed:?}"
        );
        state.validate().unwrap();
        assert!(!state.owned_cell_page(None, 100).items[0].alarm_firing);
        assert!(matches!(
            state.phase(CELL),
            Some(Phase::Cleaning {
                cause: StopCause::Reset,
                ..
            })
        ));
        // No proof of the consuming alarm write exists. A late completion must
        // neither acknowledge it nor publish a new durability barrier.
        let late = on_event(
            &mut state,
            Event::AlarmFinished {
                op: alarm,
                cell: CELL.into(),
                epoch: 8,
                now_ms: 1_001,
                now_mono_ms: 1_001,
                result: Ok((AlarmSnapshot::without_wake(None), false, Some(9))),
            },
        );
        assert!(
            late.is_empty(),
            "retired alarm completion changed reset state: {late:?}"
        );
        on_event(&mut state, Event::RuntimeStopped { op: stop });
        state.validate().unwrap();
        assert!(!state
            .owned_cell_page(None, 100)
            .items
            .iter()
            .any(|cell| cell.alarm_firing));
    }
}
