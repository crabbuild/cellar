// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded inline foreign callbacks inside native SQLite transaction_sync.
use super::{Callbacks, GuestFailure, Registration};
use crate::runtime::{Event, Life, Reply, Runtime};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{Context, InvocationAuthority};
use cellar_guest::{failure, operations};
use futures_util::FutureExt;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, mpsc, Arc};
use tokio::sync::{mpsc as channel, OwnedSemaphorePermit};

struct Call {
    id: u64,
    name: String,
    payload: String,
    callback_scope: Option<u64>,
    blocking: bool,
    synchronous: Option<mpsc::SyncSender<Reply>>,
    _bytes: OwnedSemaphorePermit,
}

impl Call {
    fn decode(event: Event) -> Result<Self> {
        let Event::Operation {
            id,
            name,
            payload,
            callback_scope,
            blocking,
            synchronous,
            _bytes,
        } = event
        else {
            anyhow::bail!("guest completed during a synchronous native callback");
        };
        Ok(Self {
            id,
            name,
            payload,
            callback_scope,
            blocking,
            synchronous,
            _bytes,
        })
    }

    fn reply(&self, pump: &Pump<'_>, result: Result<Value>) -> Result<()> {
        pump.runtime.respond(
            pump.invocation,
            self.id,
            failure::result(&self.name, result, pump.runtime.options.max_value_bytes)?,
            self.synchronous.clone(),
        )
    }
}

pub(crate) struct Pump<'a> {
    pub runtime: &'a Arc<Runtime>,
    pub context: &'a Context,
    pub state: &'a operations::State,
    pub callbacks: &'a Callbacks,
    pub receive: &'a mut channel::Receiver<Event>,
    pub active: &'a Life,
    pub owner: &'a Life,
    pub invocation: u64,
}

impl Pump<'_> {
    pub fn serve(&mut self, event: Event) -> Result<()> {
        let request = Call::decode(event)?;
        let deadline = cellar::asyncrt::mono_ms()
            .saturating_add(u64::try_from(self.runtime.options.turn_timeout.as_millis())?.max(1));
        let scope = request
            .callback_scope
            .map(|id| self.callbacks.scope(self.context, id))
            .transpose();
        match scope {
            Ok(Some(scope)) => {
                scope.run_sync(self.context, || self.transaction(request, deadline, 0))
            }
            Ok(None) => self.transaction(request, deadline, 0),
            Err(error) => request.reply(self, Err(error)),
        }
    }

    fn live(&self, authority: &InvocationAuthority, deadline: u64) -> Result<()> {
        ensure!(
            !self.runtime.closed.load(Ordering::Acquire)
                && self.active.load(Ordering::Acquire)
                && self.owner.load(Ordering::Acquire),
            "synchronous callback authority ended"
        );
        authority.check(self.context)?;
        ensure!(
            cellar::asyncrt::mono_ms() < deadline,
            "synchronous callback exceeded its execution budget"
        );
        Ok(())
    }

    fn transaction(&mut self, opening: Call, deadline: u64, depth: usize) -> Result<()> {
        let prepared = (|| -> Result<_> {
            ensure!(
                opening.blocking && opening.name == "workers.callback.start_sync",
                failure::Validation
            );
            let input: Value =
                serde_json::from_str(&opening.payload).context(failure::Validation)?;
            ensure!(
                input.as_object().is_some_and(|value| value.is_empty()),
                failure::Validation
            );
            ensure!(
                depth < 64
                    && self.callbacks.active.load(Ordering::Relaxed)
                        < self.runtime.options.max_batch_operations,
                failure::Validation
            );
            Ok((
                self.context.invocation_authority()?,
                self.context.storage()?,
            ))
        })();
        let (authority, storage) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return opening.reply(self, Err(error)),
        };
        self.callbacks.active.fetch_add(1, Ordering::Relaxed);
        let _registration = Registration(self.callbacks.active.clone());
        let mut acknowledged = false;
        let mut finished: Option<(Call, bool)> = None;
        let result = storage.transaction_sync(|_| {
            self.live(&authority, deadline)?;
            opening.reply(self, Ok(json!({"id":opening.id})))?;
            acknowledged = true;
            loop {
                self.live(&authority, deadline)?;
                let event = match self.receive.try_recv() {
                    Ok(event) => event,
                    Err(channel::error::TryRecvError::Disconnected) => {
                        anyhow::bail!("synchronous callback bridge ended")
                    }
                    Err(channel::error::TryRecvError::Empty) => {
                        // The native synchronous callback cannot yield SQLite's
                        // stack frame. Park this original host thread briefly;
                        // the separate engine/I/O threads own message delivery.
                        std::thread::park_timeout(std::time::Duration::from_millis(1));
                        continue;
                    }
                };
                let call = Call::decode(event)?;
                if call.name == "workers.callback.finish_sync" {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Finish {
                        id: u64,
                        success: bool,
                    }
                    let finish = serde_json::from_str::<Finish>(&call.payload)
                        .context(failure::Validation)
                        .and_then(|finish| {
                            ensure!(
                                call.blocking && finish.id == opening.id,
                                failure::Validation
                            );
                            Ok(finish)
                        });
                    match finish {
                        Ok(finish) => {
                            let success = finish.success;
                            finished = Some((call, success));
                            ensure!(success, GuestFailure);
                            return Ok(());
                        }
                        Err(error) => {
                            call.reply(self, Err(error))?;
                            continue;
                        }
                    }
                }
                if !call.blocking || call.callback_scope != Some(opening.id) {
                    call.reply(self, Err(anyhow::anyhow!(failure::Validation)))?;
                    continue;
                }
                if call.name == "workers.callback.start_sync" {
                    self.transaction(call, deadline, depth + 1)?;
                    continue;
                }
                let result = (|| -> Result<Value> {
                    ensure!(
                        crate::runtime::supports_synchronous(&call.name)
                            && !call.name.starts_with("workers.callback."),
                        failure::Validation
                    );
                    let input = serde_json::from_str(&call.payload).context(failure::Validation)?;
                    operations::execute(
                        self.context,
                        self.state,
                        &call.name,
                        input,
                        &cellar_guest::Limits {
                            max_body_bytes: self.runtime.options.max_body_bytes,
                            max_batch_operations: self.runtime.options.max_batch_operations,
                        },
                    )
                    .now_or_never()
                    .context("a synchronous SDK operation unexpectedly yielded")?
                })();
                call.reply(self, result)?;
            }
        });
        // Rollback failures poison the activation. Never acknowledge an ordinary
        // callback throw as safely rolled back after that native failure.
        let result = result.and_then(|_| self.context.invocation_authority().map(|_| ()));
        let result = match result {
            Err(error) if error.is::<GuestFailure>() => {
                self.context.invocation_authority().map(|_| ())
            }
            other => other,
        };
        if let Some((finish, _)) = finished {
            finish.reply(self, result.map(|_| Value::Null))
        } else if !acknowledged {
            opening.reply(self, result.map(|_| Value::Null))
        } else {
            result?;
            anyhow::bail!("synchronous callback ended without completion")
        }
    }
}
