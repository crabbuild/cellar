// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical callback ownership outlives a Workflow's logical timeout result.

use crate::application::{EventState, HandlerFuture, Output, Storage};
use std::{sync::Arc, task::Poll};

pub(crate) async fn join_turn(
    mut handler: HandlerFuture<Output>,
    state: Arc<EventState>,
    storage: Storage,
) -> anyhow::Result<Output> {
    let mut result = None;
    let mut background = Vec::<HandlerFuture<()>>::new();
    let mut failure = None;
    let mut checkpoint: Option<HandlerFuture<()>> = None;
    let mut proven = false;
    std::future::poll_fn(|cx| {
        if result.is_none() {
            if let Poll::Ready(output) = handler.as_mut().poll(cx) {
                result = Some(output);
            }
        }
        background.append(&mut state.background.lock().unwrap());
        // Publish the logical timeout/terminal state while continuing to own
        // the old physical callback. Another command may inspect/control it,
        // but the callback slot is not released until all continuations join.
        if result.as_ref().is_some_and(Result::is_ok) && !proven && !background.is_empty() {
            let proof = checkpoint.get_or_insert_with(|| storage.sync());
            match proof.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(output) => {
                    proven = true;
                    if let Err(error) = output {
                        result = Some(Err(error));
                    }
                }
            }
        }
        background.retain_mut(|future| match future.as_mut().poll(cx) {
            Poll::Pending => true,
            Poll::Ready(output) => {
                if let Err(error) = output {
                    failure.get_or_insert(error);
                }
                false
            }
        });
        // Polling a background future can install another owned continuation.
        if !state.background.lock().unwrap().is_empty() {
            cx.waker().wake_by_ref();
        }
        if result.is_some() && background.is_empty() && state.background.lock().unwrap().is_empty()
        {
            Poll::Ready(match failure.take() {
                Some(error) => Err(error),
                None => result.take().unwrap(),
            })
        } else {
            Poll::Pending
        }
    })
    .await
}
