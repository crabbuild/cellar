//! Retain native authority while its actual handler is suspended in sync().
use super::super::generated::{CounterClient, IncrementRequest};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, Context};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

struct State {
    view: Value,
    context: Option<Context>,
    client: Option<CounterClient>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            view: json!({"ready":false}),
            context: None,
            client: None,
        }
    }
}

#[derive(Default)]
pub struct Live(Mutex<State>);

pub struct Guard {
    live: Arc<Live>,
    id: String,
    completed: bool,
}

impl Guard {
    pub fn finish(&mut self, success: bool) {
        self.completed = success;
        if !success {
            self.cancelled();
        }
    }

    fn cancelled(&self) {
        let mut state = self.live.0.lock().unwrap();
        if state.view["id"] == self.id {
            state.view["cancelled"] = json!(true);
            state.view["waiting"] = json!(false);
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.completed {
            self.cancelled();
        }
    }
}

impl Live {
    pub fn begin(self: &Arc<Self>, context: &Context, id: &str) -> Result<Option<Guard>> {
        if !id.starts_with("proof-fence-sync-")
            || context.var("DOMAIN_MARKER") != Some("proof-owner")
        {
            return Ok(None);
        }
        let identity = context
            .identity()
            .context("live native probe requires a cell")?;
        let client = CounterClient::bind(context, "RUST", "alice")?;
        ensure!(
            context.storage().is_ok(),
            "original native context had no storage authority"
        );
        let mut state = self.0.lock().unwrap();
        ensure!(
            state.context.is_none(),
            "previous native probe was not cleared"
        );
        state.view = json!({"ready":true,"id":id,
            "node":context.var("DOMAIN_MARKER").context("missing live domain marker")?,
            "class":identity.class,"scope":identity.scope,"epoch":identity.epoch.to_string(),
            "active_before":true,"committed":false,"waiting":false,"cancelled":false,
            "late_done":false,"storage_refused":false,"read_refused":false,"call_refused":false});
        state.context = Some(context.clone());
        state.client = Some(client);
        Ok(Some(Guard {
            live: self.clone(),
            id: id.into(),
            completed: false,
        }))
    }

    pub fn committed(&self, context: &Context, id: &str) -> Result<()> {
        if !id.starts_with("proof-fence-sync-")
            || context.var("DOMAIN_MARKER") != Some("proof-owner")
        {
            return Ok(());
        }
        let identity = context
            .identity()
            .context("live probe requires an identity")?;
        let sample = cellar::storage::sync_sample(&identity.scope)?
            .context("live probe has no SQLite connection")?;
        ensure!(
            !sample.in_transaction && sample.epoch == identity.epoch,
            "live sync probe is not after its admitted transaction"
        );
        let storage = context.storage()?;
        let count = storage
            .get_json("count")?
            .context("live probe has no count")?;
        let receipt = storage
            .get_json(&format!("receipt:{id}"))?
            .context("live probe has no receipt")?;
        let mut state = self.0.lock().unwrap();
        ensure!(
            state.view["id"] == id,
            "live native probe changed command identity"
        );
        state.view["count"] = count;
        state.view["receipt"] = receipt;
        state.view["committed"] = json!(true);
        state.view["waiting"] = json!(true);
        Ok(())
    }

    pub fn snapshot(&self) -> Value {
        self.0.lock().unwrap().view.clone()
    }

    pub async fn release(&self) -> Result<Value> {
        let (context, client) = {
            let state = self.0.lock().unwrap();
            ensure!(
                state.view["cancelled"] == true,
                "native probe was released before its handler retired"
            );
            (
                state
                    .context
                    .clone()
                    .context("native probe context missing")?,
                state
                    .client
                    .clone()
                    .context("native probe client missing")?,
            )
        };
        let storage_refused = context
            .storage()
            .and_then(|storage| storage.put_json("late-fenced", &json!(true)))
            .is_err();
        let read_refused = context
            .storage()
            .and_then(|storage| storage.get_json("count"))
            .is_err();
        let failure = client
            .increment(IncrementRequest {
                delta: Some(1),
                operation_id: "fenced-late".into(),
            })
            .await
            .err()
            .context("stale native RPC client admitted a mutation")?;
        let call_refused = failure.code() == rpc::Code::ContextExpired
            && failure.outcome() == rpc::EffectOutcome::NotCommitted;
        let mut state = self.0.lock().unwrap();
        state.view["storage_refused"] = json!(storage_refused);
        state.view["read_refused"] = json!(read_refused);
        state.view["call_refused"] = json!(call_refused);
        state.view["late_done"] = json!(true);
        Ok(state.view.clone())
    }

    pub fn clear(&self) -> Result<Value> {
        let mut state = self.0.lock().unwrap();
        ensure!(
            state.view["late_done"] == true,
            "native probe was cleared before late checks completed"
        );
        *state = State::default();
        Ok(json!({"cleared":true}))
    }
}
