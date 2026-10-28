// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Opt-in phase evidence for development-store writes. No payloads or paths.
//! Native completion can follow caller cancellation, so retain the original
//! dispatch and clock until the blocking operation has actually returned.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Default)]
pub(super) struct Timing(Option<Arc<Observation>>);

struct Observation {
    id: u64,
    started: Instant,
    dispatch: tracing::Dispatch,
    span: tracing::Span,
}

impl Timing {
    fn enabled() -> Self {
        if !tracing::enabled!(target: "store", tracing::Level::DEBUG) {
            return Self::default();
        }

        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(Some(Arc::new(Observation {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            started: crate::asyncrt::instant(),
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            span: tracing::Span::current(),
        })))
    }

    pub(super) fn mark(&self, phase: &'static str) {
        let Some(observation) = &self.0 else {
            return;
        };
        let elapsed_us = crate::asyncrt::instant()
            .saturating_duration_since(observation.started)
            .as_micros()
            .min(u64::MAX as u128) as u64;
        tracing::dispatcher::with_default(&observation.dispatch, || {
            let _entered = observation.span.enter();
            tracing::debug!(target: "store", event = "local_store_put_phase",
                operation = observation.id, phase, elapsed_us,
                "development store write phase");
        });
    }
}

pub(super) struct Caller {
    pub(super) timing: Timing,
    delivered: bool,
}

impl Caller {
    pub(super) fn new() -> Self {
        let timing = Timing::enabled();
        timing.mark("queued");
        Self {
            timing,
            delivered: false,
        }
    }

    pub(super) fn delivered(&mut self) {
        self.delivered = true;
        self.timing.mark("delivered");
    }
}

impl Drop for Caller {
    fn drop(&mut self) {
        if !self.delivered {
            self.timing.mark("caller_dropped");
        }
    }
}
