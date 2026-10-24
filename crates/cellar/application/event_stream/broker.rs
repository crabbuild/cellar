// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{
    error::{error, ErrorCode},
    store, subscription_store,
};
use crate::application::{Cell, Context, HandlerFuture, Invocation, Output, Result};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) fn create_cell(context: &Context) -> Result<Arc<dyn Cell>> {
    super::schema::open(&context.storage()?)?;
    let (changed, _) = tokio::sync::watch::channel(0u64);
    Ok(Arc::new(Broker {
        changed,
        waiters: AtomicUsize::new(0),
    }))
}

struct Broker {
    changed: tokio::sync::watch::Sender<u64>,
    waiters: AtomicUsize,
}

struct Waiter<'a>(&'a AtomicUsize);

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Broker {
    fn notify(&self) {
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    async fn next(&self, context: &Context, body: &Value) -> Result<Value> {
        let wait = subscription_store::integer(body, "wait_ms")?;
        let limits = crate::application::limits::current().events;
        if wait < 0 || wait > limits.max_poll_ms {
            return Err(error(
                ErrorCode::InvalidRequest,
                "poll deadline exceeds policy",
            ));
        }
        let deadline = crate::asyncrt::mono_ms()
            .checked_add(u64::try_from(wait)?)
            .ok_or_else(|| error(ErrorCode::InvalidRequest, "poll deadline overflow"))?;
        // Subscribe before observing durable state. A successful mutation in
        // between the observation and await leaves a changed revision pending.
        let mut signal = self.changed.subscribe();
        let mut waiter = None;
        loop {
            context.check_turn()?;
            signal.borrow_and_update();
            let fetch = subscription_store::next(context, body)?;
            if let Some(value) = fetch.value {
                return Ok(value);
            }
            if crate::asyncrt::mono_ms() >= deadline {
                return Ok(Value::Null);
            }
            if waiter.is_none() {
                let host = context
                    .io
                    .host_services()
                    .events()
                    .waiter(limits.max_host_waiters)
                    .ok_or_else(|| {
                        error(
                            ErrorCode::TooManyWaiters,
                            "host stream waiter capacity exhausted",
                        )
                    })?;
                self.waiters
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                        if count < limits.max_waiters {
                            count.checked_add(1)
                        } else {
                            None
                        }
                    })
                    .map_err(|_| {
                        error(
                            ErrorCode::TooManyWaiters,
                            "stream waiter capacity exhausted",
                        )
                    })?;
                waiter = Some((Waiter(&self.waiters), host));
            }
            let wake = if let Some(retry) = fetch.retry_at_ms {
                // The transaction persisted logical time; compare against it
                // as well as the injected wall clock when scheduling a wait.
                let (_, row) = store::load(&context.storage()?)?;
                let now = crate::asyncrt::wall_ms().max(store::number(&row, "logical_ms")?);
                deadline.min(
                    crate::asyncrt::mono_ms()
                        .saturating_add(retry.saturating_sub(now).max(1) as u64),
                )
            } else {
                deadline
            };
            // Neither a SQLite transaction nor a native input hold crosses
            // this wait. Activation retirement cancels its owning turn.
            crate::asyncrt::select! {
                changed=signal.changed()=>{ changed.map_err(|_|error(ErrorCode::DestinationDeleted,"stream activation closed"))?; },
                _=crate::asyncrt::sleep_until(wake)=>{},
            }
        }
    }
}

pub(crate) fn body(args: &crate::engine_api::RpcData) -> Result<Value> {
    let value = crate::application::decode_rpc(args)?.to_json()?;
    let values = value.as_array().ok_or_else(|| {
        error(
            ErrorCode::InvalidRequest,
            "event RPC arguments must be a single object",
        )
    })?;
    if values.len() != 1 || !values[0].is_object() {
        return Err(error(
            ErrorCode::InvalidRequest,
            "event RPC arguments must be a single object",
        ));
    }
    Ok(values[0].clone())
}

pub(crate) fn reply(result: Result<Value>) -> Result<Output> {
    let value = match result {
        Ok(value) => json!({"ok":value}),
        Err(failure) => {
            let failure = failure
                .downcast_ref::<super::DeliveryError>()
                .cloned()
                .unwrap_or_else(|| {
                    super::DeliveryError::new(ErrorCode::InvalidRequest, failure.to_string())
                });
            json!({"error":failure})
        }
    };
    Ok(Output::Rpc(crate::application::encode_rpc(
        &cellar_native::clone::StructuredValue::from_json(value)?,
    )?))
}

impl Cell for Broker {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = invocation else {
                return Err(error(
                    ErrorCode::Forbidden,
                    "stream accepts only authorized event RPC",
                ));
            };
            let result = async {
                let body = body(&args)?;
                match method.as_str() {
                    "__streamConfigure" => {
                        let result = store::initialize(&context, &body);
                        if result.is_ok() {
                            self.notify();
                        }
                        result
                    }
                    "__streamAppend" => {
                        let result = store::append(&context, &body);
                        if result.is_ok() {
                            self.notify();
                        }
                        result
                    }
                    "__streamSeal" => super::producer::accept_seal(&context, &body),
                    "__streamRetirementCheck" => super::retirement_store::check(&context, &body),
                    "__streamRetire" => {
                        let result = super::retirement_store::close(&context, &body);
                        if result.is_ok() {
                            self.notify();
                        }
                        result
                    }
                    "__streamRead" => store::read(&context, &body),
                    "__streamResolve" => store::resolve(&context, &body),
                    "__streamNext" => self.next(&context, &body).await,
                    "__streamAck" => {
                        let services = context.io.host_services();
                        let timer = services.events().metrics.timer(
                            crate::event_metrics::Histogram::Ack,
                            services.domain_token(),
                        );
                        let result = subscription_store::ack(&context, &body);
                        timer.finish(result.is_ok());
                        if let Ok(value) = &result {
                            if value["status"] == "already_acknowledged" {
                                services
                                    .events()
                                    .metrics
                                    .increment(crate::event_metrics::Counter::AckReplayed, 1);
                            }
                            self.notify();
                        }
                        result
                    }
                    "__streamRetry" => {
                        let result = subscription_store::retry(&context, &body);
                        if result.is_ok() {
                            self.notify();
                        }
                        result
                    }
                    "__streamExtend" => subscription_store::extend(&context, &body),
                    "__streamSubscriptionStatus" => super::lifecycle_store::status(&context, &body),
                    "__streamSubscriptions" => super::inventory::list(&context, &body),
                    "__streamSources" => super::source_inventory::list(&context, &body),
                    "__streamSubscriptionChange" => {
                        let result = super::lifecycle_store::change(&context, &body);
                        if result.is_ok() {
                            self.notify();
                        }
                        result
                    }
                    "__streamAudit" => super::lifecycle_store::audit(&context, &body),
                    "__streamOperationStatus" => {
                        super::lifecycle_store::operation_status(&context, &body)
                    }
                    "__streamDescribe" => super::statistics::describe(&context),
                    _ => Err(error(
                        ErrorCode::UnsupportedProtocol,
                        "unknown stream operation",
                    )),
                }
            }
            .await;
            if result
                .as_ref()
                .err()
                .and_then(|failure| failure.downcast_ref::<super::DeliveryError>())
                .is_some_and(|failure| failure.code == ErrorCode::CursorExpired)
            {
                context
                    .io
                    .host_services()
                    .events()
                    .metrics
                    .increment(crate::event_metrics::Counter::CursorExpired, 1);
            }
            reply(result)
        })
    }
}

pub(crate) async fn maintain(
    context: Context,
    installation: cellar_logic::work::Installation,
) -> Result<Output> {
    let db = context.storage()?;
    if db.work_installation(cellar_logic::work::Reason::StreamMaintenance)? != Some(installation) {
        return Ok(Output::Done);
    }
    let (definition, row) = store::load_state(&db)?;
    if store::text(&row, "lifecycle")? == "deleted" {
        let now = crate::asyncrt::wall_ms().max(store::number(&row, "logical_ms")?);
        super::retirement_store::cleanup(&db, now)?;
        return Ok(Output::Done);
    }
    let now = store::clock(&db)?;
    db.tx(|tx| {
        tx.system_sql(
            "DELETE FROM __sys_stream_leases WHERE expires_ms<=?",
            &[now.into()],
        )?;
        store::sweep(tx, &definition, now, 0, 0)?;
        store::schedule(tx, &definition)
    })?;
    Ok(Output::Done)
}
