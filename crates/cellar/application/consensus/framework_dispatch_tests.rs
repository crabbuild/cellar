// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::cell::Cell;
use std::future::{poll_fn, Future};
use std::task::{Context, Poll, Waker};

#[test]
fn ready_sdk_reply_cannot_escape_revocation_during_its_poll() {
    let _caller = CallbackGuard::enter().unwrap();
    let live = Cell::new(true);
    let mut reply = Box::pin(poll_fn(|_| {
        assert!(require_framework_dispatch().is_ok());
        assert!(!callback_active());
        live.set(false);
        Poll::Ready(vec![7_u8; 8])
    }));
    let mut cx = Context::from_waker(Waker::noop());
    let outcome = with_framework_dispatch(
        || {
            anyhow::ensure!(live.get(), super::super::CommandFailure::Unavailable);
            Ok(())
        },
        || reply.as_mut().poll(&mut cx),
    );
    assert!(outcome.is_err(), "a revoked SDK reply reached its caller");
    assert!(matches!(
        outcome
            .unwrap_err()
            .downcast_ref::<super::super::CommandFailure>(),
        Some(super::super::CommandFailure::Unavailable)
    ));
    assert!(callback_active());
    assert!(require_framework_dispatch().is_err());
}

#[test]
fn pending_sdk_poll_cannot_keep_permission_after_original_deadline() {
    let _caller = CallbackGuard::enter().unwrap();
    let live = Cell::new(true);
    let outcome = with_framework_dispatch(
        || {
            anyhow::ensure!(
                live.get(),
                crate::raft::completion::RuntimeError::DeadlineExceeded
            );
            Ok(())
        },
        || {
            assert!(require_framework_dispatch().is_ok());
            live.set(false);
            Poll::<()>::Pending
        },
    );
    assert!(outcome.is_err(), "an expired poll remained admitted");
    assert!(matches!(
        outcome
            .unwrap_err()
            .downcast_ref::<crate::raft::completion::RuntimeError>(),
        Some(crate::raft::completion::RuntimeError::DeadlineExceeded)
    ));
    assert!(callback_active());
    assert!(require_framework_dispatch().is_err());
}

#[test]
fn refused_sdk_admission_never_polls_or_changes_its_callers_scope() {
    let _caller = CallbackGuard::enter().unwrap();
    let polled = Cell::new(false);
    let outcome = with_framework_dispatch(
        || Err(super::super::CommandFailure::Unavailable.into()),
        || polled.set(true),
    );
    assert!(outcome.is_err());
    assert!(!polled.get());
    assert!(callback_active());
    assert!(require_framework_dispatch().is_err());
}
