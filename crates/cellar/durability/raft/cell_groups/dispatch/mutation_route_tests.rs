// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Repair scope follows actual observations; capacity is not placement drift.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::asyncrt::ExecutionDomain;
use futures_util::FutureExt;

#[test]
fn busy_or_no_leader_retries_the_installed_placement() {
    assert_eq!(
        ProbeFailures {
            overloaded: true,
            ..Default::default()
        }
        .repair(),
        ProbeRepair::Retry(CommandFailure::Overloaded)
    );
    assert_eq!(
        ProbeFailures {
            no_leader: true,
            ..Default::default()
        }
        .repair(),
        ProbeRepair::Retry(CommandFailure::NotLeader { leader: None })
    );
    assert_eq!(
        ProbeFailures {
            no_leader: true,
            overloaded: true,
            ..Default::default()
        }
        .repair(),
        ProbeRepair::Retry(CommandFailure::NotLeader { leader: None })
    );
}

#[test]
fn unreachable_probes_preserve_placement_and_do_not_infer_membership_change() {
    for error in [
        TransportError::Timeout,
        TransportError::Io("connection reset".into()),
    ] {
        let mut failures = ProbeFailures::default();
        failures.observe(error).unwrap();
        assert_eq!(
            failures.repair(),
            ProbeRepair::Retry(CommandFailure::Unavailable)
        );
    }
}

#[test]
fn missing_or_stale_members_need_membership_discovery() {
    assert_eq!(ProbeFailures::default().repair(), ProbeRepair::Membership);
    for (no_leader, overloaded) in [(false, false), (true, false), (false, true), (true, true)] {
        assert_eq!(
            ProbeFailures {
                membership: true,
                unreachable: true,
                no_leader,
                overloaded
            }
            .repair(),
            ProbeRepair::Membership
        );
    }
    for error in [TransportError::UnknownGroup, TransportError::StalePeer] {
        let mut failures = ProbeFailures::default();
        failures.observe(error).unwrap();
        assert_eq!(failures.repair(), ProbeRepair::Membership);
    }
}

#[test]
fn probe_authority_failures_preserve_the_terminal_cause() {
    for error in [
        TransportError::Identity,
        TransportError::Authentication,
        TransportError::Refused,
    ] {
        let mut failures = ProbeFailures::default();
        assert!(matches!(
            failures.observe(error.clone()),
            Err(MutationSelectionError::Terminal(CellDispatchError::Transport(actual))) if actual == error
        ));
    }
}

#[tokio::test]
async fn backoff_retains_original_deadline_and_owner() -> anyhow::Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let token = domain
        .run(async { crate::asyncrt::services().domain_token().unwrap() })
        .await?;
    let deadline = token.mono_ms()? + 20;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        backoff(&token, deadline, u64::MAX, 3),
    )
    .await?;
    assert!(matches!(
        result,
        Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded))
    ));
    assert!(token.mono_ms()? >= deadline);
    assert!(matches!(
        backoff(&token, deadline, 1, 3).now_or_never(),
        Some(Err(CellDispatchError::Runtime(
            RuntimeError::DeadlineExceeded
        )))
    ));
    domain.shutdown().await;
    assert!(matches!(
        backoff(&token, u64::MAX, 1, 3).now_or_never(),
        Some(Err(CellDispatchError::Runtime(RuntimeError::Closed)))
    ));
    Ok(())
}

#[test]
fn remaining_tickets_leave_time_for_capacity_progress_and_final_execution() {
    let deadline = 5000;
    let mut now = 0;
    for remaining in [3, 2, 1] {
        let next = retry_at(now, deadline, 5, remaining);
        assert!(next > now && next < deadline);
        now = next;
    }
    assert!(now >= 1000 && deadline - now >= 2000);
    assert_eq!(retry_at(4999, deadline, 5, 3), deadline);
    assert_eq!(
        retry_at(u64::MAX - 1, u64::MAX, u64::MAX, usize::MAX),
        u64::MAX
    );
}
