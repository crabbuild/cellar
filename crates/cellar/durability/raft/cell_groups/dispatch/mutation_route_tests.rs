// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Repair scope follows actual observations; capacity is not placement drift.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::asyncrt::ExecutionDomain;
use futures_util::FutureExt;

#[test]
fn only_busy_or_no_leader_retries_the_installed_placement() {
    assert_eq!(
        ProbeFailures {
            overloaded: true,
            ..Default::default()
        }
        .leader_retry(),
        Some(CommandFailure::Overloaded)
    );
    assert_eq!(
        ProbeFailures {
            no_leader: true,
            ..Default::default()
        }
        .leader_retry(),
        Some(CommandFailure::NotLeader { leader: None })
    );
    assert_eq!(
        ProbeFailures {
            no_leader: true,
            overloaded: true,
            discovery: false
        }
        .leader_retry(),
        Some(CommandFailure::NotLeader { leader: None })
    );
}

#[test]
fn unreachable_or_unverified_members_keep_the_discovery_fallback() {
    assert_eq!(ProbeFailures::default().leader_retry(), None);
    for (no_leader, overloaded) in [(false, false), (true, false), (false, true), (true, true)] {
        assert_eq!(
            ProbeFailures {
                discovery: true,
                no_leader,
                overloaded
            }
            .leader_retry(),
            None
        );
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
