// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native callback boundaries retained by the original native turn driver.
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{CallbackScope, Context};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::oneshot;

#[derive(Debug)]
struct GuestFailure;

impl std::fmt::Display for GuestFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("guest callback failed")
    }
}

impl std::error::Error for GuestFailure {}

#[derive(Default)]
pub(crate) struct Callbacks {
    entries: Arc<Mutex<HashMap<u64, Entry>>>,
    active: Arc<AtomicUsize>,
}

struct Registration(Arc<AtomicUsize>);

impl Drop for Registration {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Entry {
    scope: Option<CallbackScope>,
    finish: oneshot::Sender<bool>,
    done: oneshot::Receiver<Result<()>>,
}

impl Callbacks {
    pub fn scope(&self, context: &Context, id: u64) -> Result<CallbackScope> {
        let scope = self
            .entries
            .lock()
            .unwrap()
            .get(&id)
            .and_then(|entry| entry.scope.clone())
            .context("native callback token is closed")
            .context(cellar_guest::failure::Validation)?;
        scope.check(context)?;
        Ok(scope)
    }

    pub async fn execute(
        &self,
        context: &Context,
        name: &str,
        value: Value,
        id: u64,
        maximum: usize,
    ) -> Result<Value> {
        if name == "workers.callback.rollback" {
            ensure!(
                value.as_object().is_some_and(|value| value.is_empty()),
                cellar_guest::failure::Validation
            );
            context.callback_scope()?.rollback(context)?;
            return Ok(Value::Null);
        }
        if name == "workers.callback.finish" {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Finish {
                id: u64,
                success: bool,
            }
            let input: Finish =
                serde_json::from_value(value).context(cellar_guest::failure::Validation)?;
            let entry = self
                .entries
                .lock()
                .unwrap()
                .remove(&input.id)
                .context("native callback token is closed")
                .context(cellar_guest::failure::Validation)?;
            context.invocation_authority()?;
            ensure!(entry.scope.is_some(), "native callback is not ready");
            let _ = entry.finish.send(input.success);
            let result = entry
                .done
                .await
                .context("native callback completion was abandoned")?;
            if let Err(error) = result {
                if input.success || !error.is::<GuestFailure>() {
                    return Err(error);
                }
            }
            return Ok(Value::Null);
        }
        ensure!(
            name == "workers.callback.begin",
            cellar_guest::failure::Validation
        );

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Begin {
            kind: Kind,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Kind {
            Block,
            Transaction,
        }
        let input: Begin =
            serde_json::from_value(value).context(cellar_guest::failure::Validation)?;
        let storage = context.storage()?;
        let (finish, receive_finish) = oneshot::channel();
        let (done, receive_done) = oneshot::channel();
        let (ready, receive_ready) = oneshot::channel();
        {
            let mut entries = self.entries.lock().unwrap();
            ensure!(
                entries.len() < maximum && self.active.load(Ordering::Relaxed) < maximum,
                cellar_guest::failure::Validation
            );
            ensure!(
                !entries.contains_key(&id),
                "native callback identity reused"
            );
            entries.insert(
                id,
                Entry {
                    scope: None,
                    finish,
                    done: receive_done,
                },
            );
            self.active.fetch_add(1, Ordering::Relaxed);
        }
        let registration = Registration(self.active.clone());
        // No strong registry reference is held by native background work. If
        // the owning bridge is dropped, senders close and boundaries unwind.
        let entries = Arc::downgrade(&self.entries);
        let callback_context = context.clone();
        let callback = move || async move {
            let scope = callback_context.callback_scope()?;
            let entries = entries.upgrade().context("native callback bridge ended")?;
            entries
                .lock()
                .unwrap()
                .get_mut(&id)
                .context("native callback was cancelled")?
                .scope = Some(scope);
            drop(entries);
            let _ = ready.send(());
            ensure!(
                receive_finish
                    .await
                    .context("native callback bridge ended")?,
                GuestFailure
            );
            Ok(())
        };
        let work = match input.kind {
            Kind::Block => context.block_concurrency_while(move |_| callback()),
            Kind::Transaction => storage.transaction(context, move |_| callback()),
        };
        let entries = Arc::downgrade(&self.entries);
        context.wait_until(async move {
            let _registration = registration;
            let result = work.await;
            // A failed admission/timeout also revokes an entry whose caller
            // never submitted finish. Complete replies retain native failures.
            if let Some(entries) = entries.upgrade() {
                entries.lock().unwrap().remove(&id);
            }
            let _ = done.send(result);
            Ok(())
        })?;
        receive_ready
            .await
            .context("native callback admission failed")?;
        Ok(json!({"id":id}))
    }
}
