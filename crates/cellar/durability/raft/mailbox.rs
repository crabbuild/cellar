// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A bounded queue whose synchronous publications join receiver teardown.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, Weak,
};
use tokio::sync::mpsc;

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "mailbox_tests.rs"]
mod tests;

struct State<T> {
    sender: Mutex<Option<mpsc::Sender<T>>>,
    closed: AtomicBool,
}

pub(crate) struct Sender<T> {
    state: Arc<State<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Sender<T> {
    pub(crate) fn try_send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        if self.state.closed.load(Ordering::Acquire) {
            return Err(mpsc::error::TrySendError::Closed(value));
        }
        // Keep the synchronous reserve/publish operation indivisible with
        // receiver teardown. Exposing a raw sender or reservation would break
        // this contract and may retain late replies after Receiver::drop.
        let Ok(sender) = self.state.sender.lock() else {
            return Err(mpsc::error::TrySendError::Closed(value));
        };
        match sender.as_ref() {
            Some(sender) => sender.try_send(value),
            None => Err(mpsc::error::TrySendError::Closed(value)),
        }
    }
}

pub(crate) struct Receiver<T> {
    inner: mpsc::Receiver<T>,
    // A receiver must not keep its own producer alive after all clients drop.
    state: Weak<State<T>>,
}

impl<T> Receiver<T> {
    pub(crate) async fn recv(&mut self) -> Option<T> {
        self.inner.recv().await
    }

    pub(crate) fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        self.inner.try_recv()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state.closed.store(true, Ordering::Release);
            state
                .sender
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
        }
        // All sends have completed, and no producer can reserve another slot.
        // Dropping the inner receiver now releases every buffered value.
    }
}

pub(crate) fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (sender, inner) = mpsc::channel(capacity);
    let state = Arc::new(State {
        sender: Mutex::new(Some(sender)),
        closed: AtomicBool::new(false),
    });
    let receiver = Receiver {
        inner,
        state: Arc::downgrade(&state),
    };
    (Sender { state }, receiver)
}
