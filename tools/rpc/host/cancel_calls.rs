//! Bounded native caller controls for the real proof-wait qualification.
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, web, Context};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

use super::generated::{CounterClient, GetCountRequest, IncrementRequest};

#[derive(Default)]
pub struct Calls(Mutex<BTreeMap<String, Arc<State>>>);

#[derive(Default)]
struct State {
    controller: web::AbortController,
    finish: Notify,
    settled: Mutex<Option<Value>>,
}

struct Registration {
    calls: Arc<Calls>,
    name: String,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.calls.0.lock().unwrap().remove(&self.name);
    }
}

impl Calls {
    fn state(&self, name: &str) -> Result<Arc<State>> {
        self.0
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .context("native proof call is not pending")
    }

    pub fn snapshot(&self, name: &str) -> Result<Value> {
        Ok(self
            .state(name)?
            .settled
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| json!({"settled":false})))
    }

    pub fn release(&self, name: &str, finish: bool) -> Result<Value> {
        let state = self.state(name)?;
        if finish {
            // notify_one retains a permit if the original caller has not yet
            // reached its final wait. There is exactly one registered waiter.
            state.finish.notify_one();
        } else {
            state.controller.abort(None)?;
        }
        Ok(json!({"released":true}))
    }

    pub async fn increment(
        self: &Arc<Self>,
        context: &Context,
        cell: (&str, &str),
        id: &str,
        delta: i64,
        deadline: bool,
    ) -> Result<Value> {
        let state = Arc::new(State::default());
        {
            let mut calls = self.0.lock().unwrap();
            ensure!(
                calls.is_empty() && !id.is_empty() && id.len() <= 128,
                "native proof control capacity exceeded"
            );
            calls.insert(id.into(), state.clone());
        }
        let _registration = Registration {
            calls: self.clone(),
            name: id.into(),
        };
        let counter = CounterClient::bind(context, cell.0, cell.1)?;
        let failure = counter
            .increment_with_options(
                IncrementRequest {
                    delta: Some(delta),
                    operation_id: id.into(),
                },
                rpc::CallOptions {
                    timeout: Some(Duration::from_millis(if deadline { 1000 } else { 5000 })),
                    signal: Some(state.controller.signal()),
                },
            )
            .await
            .err()
            .context("held proof unexpectedly delivered a successful mutation")?;
        let result = json!({"code":failure.code(),"outcome":failure.outcome()});
        let fresh = CounterClient::bind(context, "RUST", "cancel-followup")?
            .get_count(GetCountRequest {})
            .await?;
        let writes = fresh.count.context("missing cleanup probe count")?.value;
        *state.settled.lock().unwrap() =
            Some(json!({"settled":true,"followup_writes":writes,"result":result}));
        // Keep the originating invocation live while its retired target's
        // original SDK facades are tested. Parent retirement cannot stand in
        // for call-local cancellation or deadline propagation.
        state.finish.notified().await;
        let final_fresh = CounterClient::bind(context, "RUST", "cancel-followup")?
            .get_count(GetCountRequest {})
            .await?;
        let final_writes = final_fresh
            .count
            .context("missing final caller authority check")?
            .value;
        Ok(
            json!({"node":context.var("DOMAIN_MARKER"),"result":result,"followup_writes":writes,"final_followup_writes":final_writes}),
        )
    }
}
