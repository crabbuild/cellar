// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::cell::Cell;
use std::io::{Error, ErrorKind};
use std::task::{Context, Poll, Waker};

#[test]
fn queued_reply_cannot_outlive_its_source_permit() {
    let live = Cell::new(true);
    let (send, response) = tokio::sync::oneshot::channel::<Result<Vec<u8>>>();
    let mut waiting = Box::pin(receive(async { response.await? }, || {
        anyhow::ensure!(
            live.get(),
            Error::new(ErrorKind::PermissionDenied, "source revoked")
        );
        Ok(())
    }));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    // The target has completed while the source is still valid. Delivery waits
    // for another application turn, during which that permission is revoked.
    send.send(Ok(vec![1, 2, 3])).unwrap();
    live.set(false);
    let Poll::Ready(result) = waiting.as_mut().poll(&mut cx) else {
        panic!("queued reply did not settle")
    };
    let error = result.expect_err("a queued reply escaped its revoked source");
    assert_eq!(
        error.downcast_ref::<Error>().unwrap().kind(),
        ErrorKind::PermissionDenied
    );
}

#[test]
fn expired_source_is_checked_even_when_target_returns_an_error() {
    let (send, response) = tokio::sync::oneshot::channel::<Result<()>>();
    let mut waiting = Box::pin(receive(async { response.await? }, || {
        Err(Error::new(ErrorKind::TimedOut, "original source deadline").into())
    }));
    send.send(Err(Error::other("target failure").into()))
        .unwrap();
    let Poll::Ready(result) = waiting
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("queued target failure did not settle")
    };
    assert_eq!(
        result.unwrap_err().downcast_ref::<Error>().unwrap().kind(),
        ErrorKind::TimedOut
    );
}

#[test]
fn current_source_preserves_target_success_and_typed_failure() {
    for success in [false, true] {
        let response = async {
            if success {
                Ok(vec![1, 2, 3])
            } else {
                Err(Error::new(ErrorKind::NotFound, "target absent").into())
            }
        };
        let mut waiting = Box::pin(receive(response, || Ok(())));
        let Poll::Ready(result) = waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("ready target result did not settle")
        };
        if success {
            assert_eq!(result.unwrap(), vec![1, 2, 3]);
        } else {
            assert_eq!(
                result.unwrap_err().downcast_ref::<Error>().unwrap().kind(),
                ErrorKind::NotFound
            );
        }
    }
}
