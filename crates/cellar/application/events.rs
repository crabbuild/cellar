// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Event values and execution-domain abort timers for Rust applications.
use super::{Context, HandlerFuture, Request};
use anyhow::Result;
pub use cellar_native::web::{
    AbortController, AbortReason, AbortSignal, AbortWait, CustomEvent, DomException, Event,
    EventDispatchError, EventInit, EventListener, EventTarget, Listener, ListenerOptions,
    SharedEventValue, WeakAbortSignal,
};

impl Context {
    /// The signal for this incoming HTTP event. Its controller belongs to the
    /// original invocation and fires before disconnect cleanup drops handlers.
    pub fn request_signal(&self) -> Option<AbortSignal> {
        self.state
            .incoming_abort
            .lock()
            .unwrap()
            .as_ref()
            .and_then(WeakAbortSignal::upgrade)
    }

    /// Timer work uses the injected clock and the original event. A timer alone
    /// cannot extend residency; finishing or retiring drops unused timers.
    pub fn abort_timeout(&self, ms: f64) -> Result<AbortSignal> {
        self.check_turn()?;
        AbortSignal::timeout(ms, |delay, controller| {
            self.state
                .unrefed
                .lock()
                .unwrap()
                .push(Box::pin(async move {
                    crate::asyncrt::sleep(delay).await;
                    controller.abort(Some(AbortReason::timeout()))?;
                    Ok(())
                }));
            Ok(())
        })
    }

    /// Abort a routed fetch with the exact shared reason while the existing
    /// dispatch guard sends cancellation to the target and releases its body.
    pub fn fetch_cell_with_signal(
        &self,
        scope: String,
        name: Option<String>,
        request: Request,
        signal: AbortSignal,
    ) -> HandlerFuture<crate::engine_api::HttpResponse> {
        if let Err(reason) = signal.throw_if_aborted() {
            return Box::pin(async move { Err(anyhow::Error::new(reason)) });
        }
        let dispatch = crate::native_worker::fetch_cell(self, scope, name, request, true);
        Box::pin(async move {
            crate::asyncrt::select_biased! {
                "a latched caller abort wins over an undelivered routed reply";
                reason=signal.cancelled()=>Err(anyhow::Error::new(reason)),
                result=dispatch=>result,
            }
        })
    }
}
