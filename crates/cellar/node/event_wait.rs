// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One immutable event RPC deadline, with conservative submission certainty.
use crate::application::event_stream::{DeliveryError, ErrorCode};
use anyhow::Result;
use std::{
    future::Future,
    sync::atomic::{AtomicU8, Ordering},
};
use tokio::sync::oneshot;

pub(super) struct Budget {
    deadline_ms: u64,
    certainty: AtomicU8,
}

impl Budget {
    pub(super) fn new(duration_ms: u64) -> Self {
        Self::until(crate::asyncrt::mono_ms().saturating_add(duration_ms))
    }

    fn until(deadline_ms: u64) -> Self {
        Self {
            deadline_ms,
            certainty: AtomicU8::new(0),
        }
    }

    pub(super) fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }

    pub(super) fn submitted(&self) {
        self.certainty.store(1, Ordering::SeqCst);
    }

    pub(super) fn refused(&self, attempt: cellar_logic::routing::Attempt) {
        let certainty = match attempt {
            cellar_logic::routing::Attempt::NeverConnected => 0,
            cellar_logic::routing::Attempt::Ambiguous => 1,
            cellar_logic::routing::Attempt::NotOwner => 2,
        };
        self.certainty.store(certainty, Ordering::SeqCst);
    }

    fn failure(&self, message: &'static str) -> anyhow::Error {
        let code = match self.certainty.load(Ordering::SeqCst) {
            0 => ErrorCode::Unavailable,
            2 => ErrorCode::OwnershipLost,
            _ => ErrorCode::Ambiguous,
        };
        DeliveryError::new(code, message).into()
    }
}

pub(super) async fn receive<T>(
    operation: impl Future<Output = Result<T>>,
    cancel: Option<oneshot::Receiver<()>>,
    budget: Option<&Budget>,
) -> Result<T> {
    let waiting = async {
        if let Some(budget) = budget {
            let remaining = budget.deadline_ms.saturating_sub(crate::asyncrt::mono_ms());
            if remaining == 0 {
                return Err(budget.failure("event RPC deadline elapsed"));
            }
            crate::asyncrt::select_biased! {
                "the original event RPC deadline wins an undelivered result";
                _=crate::asyncrt::sleep(std::time::Duration::from_millis(remaining))=>Err(budget.failure("event RPC deadline elapsed")),
                result=operation=>result,
            }
        } else {
            operation.await
        }
    };
    match cancel {
        Some(mut cancel) => crate::asyncrt::select_biased! {
            "caller cancellation wins an undelivered event RPC result or deadline";
            _=&mut cancel=>Err(match budget {
                Some(budget)=>budget.failure("event RPC cancelled"),
                None=>anyhow::anyhow!("Durable Object RPC cancelled"),
            }),
            result=waiting=>result,
        },
        None => waiting.await,
    }
}

#[cfg(test)]
#[path = "event_wait_tests.rs"]
mod tests;
