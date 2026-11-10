// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Arrival boundaries, correlation and cancellation ownership for shared reads.
use super::*;
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

fn identity(group: u8, term: u64, configuration: u64) -> Identity {
    Identity {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([group; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        term: Term::new(term),
        configuration,
    }
}

#[test]
fn only_pre_initiation_reads_share_the_correlated_barrier() {
    let scope = identity(3, 5, 7);
    let mut cohorts = Cohorts::new(4);
    let (first, joined) = cohorts.join(scope, vec![1], 0).unwrap();
    assert!(!joined);
    let (second, joined) = cohorts.join(scope, vec![2], 0).unwrap();
    assert!(joined);
    assert_eq!(first, second);
    let mut issued = 0;
    cohorts.progress(&first, 0, |expected, term, barrier, _| {
        assert_eq!(expected, scope);
        assert!(barrier.is_none());
        *term = Some(scope.term);
        issued += 1;
    });
    // Even while that barrier is pending, this arrival must initiate a later one.
    let (late, joined) = cohorts.join(scope, vec![3], 1).unwrap();
    assert!(!joined);
    assert_ne!(late, first);
    // The first caller can cancel while the second retains the physical result.
    cohorts.leave(&first);
    assert!(cohorts.observe(scope.history, scope.term, LogIndex::new(11), &first));
    cohorts.progress(&second, 2, |_, term, barrier, retry| {
        assert_eq!(*term, Some(scope.term));
        assert_eq!(*barrier, Some(LogIndex::new(11)));
        assert!(!retry);
    });
    cohorts.progress(&late, 2, |_, term, barrier, _| {
        assert!(term.is_none() && barrier.is_none());
        *term = Some(scope.term);
        issued += 1;
    });
    assert_eq!(issued, 2);
    cohorts.leave(&second);
    assert!(!cohorts.observe(scope.history, scope.term, LogIndex::new(12), &first));
    cohorts.leave(&late);
    assert!(cohorts.entries.is_empty() && cohorts.open.is_empty());
    assert_eq!(cohorts.members, 0);
}

#[test]
fn term_configuration_and_group_changes_cannot_join_an_open_cohort() {
    let mut cohorts = Cohorts::new(4);
    let scopes = [
        identity(3, 5, 7),
        identity(3, 6, 7),
        identity(3, 6, 8),
        identity(4, 6, 8),
    ];
    let mut contexts = Vec::new();
    for (sequence, scope) in scopes.into_iter().enumerate() {
        let (context, joined) = cohorts.join(scope, vec![sequence as u8], 0).unwrap();
        assert!(!joined && !contexts.contains(&context));
        contexts.push(context);
    }
    for (scope, context) in scopes.into_iter().zip(&contexts) {
        cohorts.progress(context, 0, |expected, term, _, _| {
            assert_eq!(expected, scope);
            *term = Some(scope.term);
        });
    }
    let scope = scopes[0];
    assert!(cohorts.observe(
        scopes[3].history,
        scope.term,
        LogIndex::new(99),
        &contexts[0]
    ));
    assert!(cohorts.observe(scope.history, Term::new(6), LogIndex::new(99), &contexts[0]));
    cohorts.progress(&contexts[0], 1, |_, _, barrier, _| {
        assert!(barrier.is_none())
    });
    for context in contexts {
        cohorts.leave(&context);
    }
}

#[test]
fn original_admission_and_deadlines_bound_shared_members_and_retransmission() {
    let scope = identity(3, 5, 7);
    let mut cohorts = Cohorts::new(2);
    let (context, _) = cohorts.join(scope, vec![1], 10).unwrap();
    let (shared, _) = cohorts.join(scope, vec![2], 10).unwrap();
    assert!(matches!(
        cohorts.join(scope, vec![3], 10),
        Err(RuntimeError::Overloaded)
    ));
    assert_eq!(cohorts.deadline(&context, 20), 20);
    let retry_at = 10 + super::super::reads::FORWARDED_RETRY_MS;
    cohorts.progress(&context, 10, |_, term, _, retry| {
        assert!(!retry);
        *term = Some(scope.term);
    });
    cohorts.progress(&context, retry_at, |_, _, _, retry| assert!(retry));
    // One retransmission opportunity belongs to the cohort, not each waiter.
    cohorts.progress(&shared, retry_at, |_, _, _, retry| assert!(!retry));
    cohorts.leave(&context);
    cohorts.leave(&shared);
    let (fresh, joined) = cohorts.join(scope, vec![3], retry_at).unwrap();
    assert!(!joined && fresh != context);
    assert!(!cohorts.observe(scope.history, scope.term, LogIndex::new(10), &context));
}
