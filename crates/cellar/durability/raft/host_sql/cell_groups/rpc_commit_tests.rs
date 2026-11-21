// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::sync::atomic::AtomicUsize;

#[tokio::test]
async fn expired_waiter_keeps_resubmitting_the_owned_uncertain_preparation() {
    for overloaded in [true, false] {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let token = domain.services().domain_token().unwrap();
        let attempts = AtomicUsize::new(0);
        let result = submit_checkpoint_attempts(
            &token,
            None,
            || {
                if attempts.load(Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err(crate::raft::execution::authority_failure(
                        RuntimeError::DeadlineExceeded,
                    ))
                }
            },
            // A previous submission may have entered. This waiter cannot seal it.
            || Ok(false),
            || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                std::future::ready(match attempt {
                    0 => Err(if overloaded {
                        RuntimeError::Overloaded
                    } else {
                        RuntimeError::NotReady
                    }),
                    1 => Ok(true),
                    _ => panic!("settled checkpoint resubmitted"),
                })
            },
        )
        .await;
        domain.shutdown().await;
        assert!(result.unwrap());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "waiter expiry skipped the owned submission after an uncertain refusal"
        );
    }
}

#[tokio::test]
async fn closed_waiter_with_unsubmitted_seal_never_submits() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let token = domain.services().domain_token().unwrap();
    let attempts = AtomicUsize::new(0);
    let result = submit_checkpoint_attempts(
        &token,
        None,
        || Err(unavailable()),
        || Ok(true),
        || {
            attempts.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(true))
        },
    )
    .await;
    domain.shutdown().await;
    assert!(matches!(
        result,
        Err(ExecutionError::Refused(CommandFailure::Unavailable))
    ));
    assert_eq!(attempts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn closed_waiter_with_unknown_submission_observes_runtime_term_loss() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let token = domain.services().domain_token().unwrap();
    let attempts = AtomicUsize::new(0);
    let result = submit_checkpoint_attempts(
        &token,
        None,
        || Err(unavailable()),
        || Ok(false),
        || {
            attempts.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Err(RuntimeError::NotLeader))
        },
    )
    .await;
    domain.shutdown().await;
    assert!(
        result.unwrap(),
        "unknown submission still requires applied reconciliation"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "waiter closure substituted for the runtime's authoritative disposition"
    );
}

#[tokio::test]
async fn elapsed_submission_deadline_requires_a_positive_seal() {
    for sealed in [true, false] {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let token = domain.services().domain_token().unwrap();
        let attempts = AtomicUsize::new(0);
        let result = submit_checkpoint_attempts(
            &token,
            Some(0),
            || Ok(()),
            || Ok(sealed),
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(true))
            },
        )
        .await;
        domain.shutdown().await;
        assert_eq!(result.unwrap(), !sealed);
        assert_eq!(attempts.load(Ordering::SeqCst), usize::from(!sealed));
    }
}
