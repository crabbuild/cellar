// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The native reserved cron cell. Schedule and retry policy remain in
//! cellar_logic::cron; storage, wake discovery and ownership remain the host's.

use super::{Application, Cell, Context, EventState, HandlerFuture, Invocation, Output, Storage};
use crate::engine_api::HttpResponse;
use anyhow::{anyhow, Result};
use cellar_logic::cron as policy;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};

#[derive(Clone, Debug)]
pub struct Scheduled {
    pub scheduled_ms: i64,
    pub cron: String,
    no_retry: Arc<AtomicBool>,
}
impl Scheduled {
    /// Decline retry if this handler fails. Late calls only change this
    /// controller's flag; they cannot settle or change another occurrence.
    pub fn no_retry(&self) {
        self.no_retry.store(true, Ordering::Release);
    }
}

struct Owed {
    occurrence: i64,
    indices: Vec<usize>,
    armed_for: i64,
}
#[derive(Default)]
struct State {
    retry: i64,
    owed: Option<Owed>,
}
struct Schedule {
    storage: Storage,
    application: Arc<dyn Application>,
    expressions: Vec<String>,
    parsed: Vec<policy::Cron>,
    positions: Vec<usize>,
    state: Mutex<State>,
}

pub(crate) fn create_cell(
    context: &Context,
    application: Arc<dyn Application>,
    expressions: Vec<String>,
) -> Result<Arc<dyn Cell>> {
    let mut parsed = Vec::new();
    let mut positions = Vec::new();
    // Match the existing op: an invalid control-plane entry must neither
    // silence the valid expressions nor shift their reported original text.
    for (index, expression) in expressions.iter().enumerate() {
        if let Ok(cron) = policy::parse(expression) {
            parsed.push(cron);
            positions.push(index);
        }
    }
    Ok(Arc::new(Schedule {
        storage: context.storage()?,
        application,
        expressions,
        parsed,
        positions,
        state: Mutex::new(State::default()),
    }))
}
fn now() -> i64 {
    crate::ownership_store::now_ms() as i64
}
impl Schedule {
    fn arm(&self, occurrence: Option<i64>, failed: Vec<usize>, retried: bool) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.retry = if failed.is_empty() {
            0
        } else if retried {
            state.retry + 1
        } else {
            1
        };
        let plan = policy::plan(&self.parsed, None, now(), state.retry, !failed.is_empty());
        state.owed = if plan.arm_is_retry {
            Some(Owed {
                occurrence: occurrence.expect("a retry follows an occurrence"),
                indices: failed,
                armed_for: plan.arm_at.expect("retry has an alarm"),
            })
        } else {
            None
        };
        match plan.arm_at {
            Some(at) => self.storage.set_alarm(at),
            None => self.storage.delete_alarm(),
        }
    }
    fn fetch(&self) -> Result<Output> {
        if self.state.lock().unwrap().owed.is_none() {
            let armed = self.storage.get_alarm()?;
            // Preserve a due occurrence after downtime. A pending retry
            // preserves both its deadline and its activation-local owed set.
            if armed.is_none_or(|at| at > now()) {
                self.arm(None, Vec::new(), false)?;
            }
        }
        Ok(Output::Http(HttpResponse {
            status: 204,
            body: Vec::new(),
            stream: None,
            headers: Vec::new(),
            websocket: None,
            write_position: None,
            observed_position: None,
        }))
    }
    async fn alarm(&self, context: &Context, fired: i64) -> Result<()> {
        let (occurrence, due, retried) = {
            let state = self.state.lock().unwrap();
            match state.owed.as_ref().filter(|owed| owed.armed_for == fired) {
                Some(owed) => (owed.occurrence, owed.indices.clone(), true),
                None => {
                    let plan = policy::plan(&self.parsed, Some(fired), now(), 0, false);
                    (
                        fired,
                        plan.matching
                            .into_iter()
                            .map(|index| self.positions[index])
                            .collect(),
                        false,
                    )
                }
            }
        };
        let mut failed = Vec::new();
        for index in due {
            let controller = Scheduled {
                scheduled_ms: occurrence,
                cron: self.expressions[index].clone(),
                no_retry: Arc::new(AtomicBool::new(false)),
            };
            if ScheduledRun::new(context, self.application.clone(), controller).await {
                failed.push(index);
            }
        }
        self.arm(Some(occurrence), failed, retried)
    }
}
impl Cell for Schedule {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            match invocation {
                Invocation::Fetch(_) => self.fetch(),
                Invocation::Alarm { scheduled_ms } => {
                    self.alarm(&context, scheduled_ms).await?;
                    Ok(Output::Done)
                }
                _ => Err(anyhow!("the cron schedule received an unsupported event")),
            }
        })
    }
}

/// A scheduled handler is a worker event inside the alarm's host turn.
/// It has its own registration lifetime while retaining the exact parent
/// I/O authority and output gate. Background work is polled concurrently
/// with its handler and drained before the next expression starts.
struct ScheduledRun {
    context: Context,
    controller: Scheduled,
    handler: Option<HandlerFuture<Output>>,
    background: Vec<HandlerFuture<()>>,
    retry: bool,
}
impl ScheduledRun {
    fn new(parent: &Context, application: Arc<dyn Application>, controller: Scheduled) -> Self {
        let mut context = parent.clone();
        context.identity = None;
        context.state = Arc::new(EventState::default());
        context
            .state
            .blocks
            .initializing
            .store(false, Ordering::Release);
        let handler = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            application.invoke(context.clone(), Invocation::Scheduled(controller.clone()))
        }))
        .unwrap_or_else(|panic| {
            let error = crate::native_worker::panic_error(panic);
            Box::pin(async move { Err(error) })
        });
        Self {
            context,
            controller,
            handler: Some(handler),
            background: Vec::new(),
            retry: false,
        }
    }
}
impl Future for ScheduledRun {
    type Output = bool;
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<bool> {
        let this = self.get_mut();
        if let Some(handler) = &mut this.handler {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler.as_mut().poll(cx)
            }))
            .unwrap_or_else(|panic| Poll::Ready(Err(crate::native_worker::panic_error(panic))));
            if let Poll::Ready(result) = result {
                this.handler = None;
                if let Err(error) = result {
                    tracing::error!(cron=%this.controller.cron,%error,"scheduled handler failed");
                    // The existing cell records this before draining waitUntil.
                    this.retry = !this.controller.no_retry.load(Ordering::Acquire);
                }
            }
        }
        this.background
            .append(&mut this.context.state.background.lock().unwrap());
        this.background.retain_mut(|future| {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx)))
                    .unwrap_or_else(|panic| {
                        Poll::Ready(Err(crate::native_worker::panic_error(panic)))
                    });
            match result {
                Poll::Pending => true,
                Poll::Ready(result) => {
                    if let Err(error) = result {
                        tracing::error!(%error,"scheduled background work failed");
                    }
                    false
                }
            }
        });
        let queued = !this.context.state.background.lock().unwrap().is_empty();
        if queued {
            cx.waker().wake_by_ref();
        }
        if this.handler.is_none() && this.background.is_empty() && !queued {
            Poll::Ready(this.retry)
        } else {
            Poll::Pending
        }
    }
}
impl Drop for ScheduledRun {
    fn drop(&mut self) {
        self.context.state.retired.store(true, Ordering::Release);
        self.context.state.background.lock().unwrap().clear();
    }
}
