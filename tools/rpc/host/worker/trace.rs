// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, Context};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Role {
    Provider,
    Child,
    Trigger,
}

struct Entry {
    source: rpc::BudgetSource,
    budget: rpc::CallBudget,
    view: Value,
    checkpoint: Option<Value>,
    retired: bool,
    entered: Instant,
    retired_after_ms: Option<u128>,
}

#[derive(Default)]
pub(super) struct Trace {
    pub armed: AtomicBool,
    pub ready: Notify,
    pub release: Notify,
    pub terminating: AtomicBool,
    pub termination_ready: Notify,
    entries: Mutex<BTreeMap<Role, Entry>>,
    caller_node: Mutex<Option<String>>,
    observation_error: Mutex<Option<String>>,
    late: Mutex<Option<Value>>,
    services: Mutex<BTreeMap<String, std::sync::Weak<cellar::host_services::HostServices>>>,
}

pub(super) struct Frame {
    trace: Arc<Trace>,
    role: Role,
}

impl Trace {
    pub fn observe_domain(&self, context: &Context) -> Result<()> {
        let node = context
            .var("DOMAIN_MARKER")
            .context("worker observation lacks a node")?;
        let current = cellar::asyncrt::services();
        let mut services = self.services.lock().unwrap();
        if let Some(original) = services.get(node).and_then(std::sync::Weak::upgrade) {
            ensure!(
                Arc::ptr_eq(&original, &current),
                "worker node changed execution domain"
            );
        } else {
            ensure!(
                services.len() < 4,
                "worker service observations exceed their bounded fixture"
            );
            services.insert(node.into(), Arc::downgrade(&current));
        }
        Ok(())
    }

    pub fn observed_services(&self) -> Vec<Arc<cellar::host_services::HostServices>> {
        self.services
            .lock()
            .unwrap()
            .values()
            .filter_map(std::sync::Weak::upgrade)
            .collect()
    }

    pub fn quotas(&self) -> Value {
        self.services.lock().unwrap().iter().map(|(node, weak)| {
            let value = match weak.upgrade() {
                Some(services) => json!({"source":services.outbound_rpc_usage(),"receiver":services.inbound_rpc_usage()}),
                None => json!({"retired":true}),
            };
            (node.clone(), value)
        }).collect::<serde_json::Map<_,_>>().into()
    }

    pub fn observe_late(&self, context: &Context) -> Result<()> {
        // A fresh native read proves stale Python work did not change storage.
        // This runs only in that read's installed turn, never in a coordinator.
        let identity = context
            .identity()
            .context("late probe has no cell identity")?;
        let storage = context.storage()?;
        let count = storage.get_json("count")?;
        let late = storage.get_json("python-late")?;
        let mut slot = self.late.lock().unwrap();
        let prior = match slot.as_ref() {
            None => 0,
            Some(value) => value["ordinal"]
                .as_u64()
                .context("late probe ordinal is missing")?,
        };
        let ordinal = prior
            .checked_add(1)
            .context("late probe ordinal exhausted")?;
        *slot = Some(
            json!({"ordinal":ordinal,"node":context.var("DOMAIN_MARKER"),
            "class":identity.class,"scope":identity.scope,"epoch":identity.epoch,
            "count":count,"late":late}),
        );
        Ok(())
    }

    pub fn late(&self) -> Result<Value> {
        self.check_observation()?;
        self.late
            .lock()
            .unwrap()
            .clone()
            .context("fresh native late-write probe was not observed")
    }

    pub fn caller(&self, context: &Context) -> Result<()> {
        let node = context
            .var("DOMAIN_MARKER")
            .context("worker caller has no domain")?
            .to_string();
        let mut slot = self.caller_node.lock().unwrap();
        ensure!(
            slot.is_none(),
            "worker fixture observed more than one original caller"
        );
        *slot = Some(node);
        Ok(())
    }

    pub fn enter(self: &Arc<Self>, role: Role, context: &Context) -> Result<Frame> {
        let identity = context.identity().context("worker frame has no identity")?;
        let source = rpc::BudgetSource::capture(context)?;
        let budget = source.start()?;
        let remaining = budget.remaining_ms()?;
        let entry = Entry {
            source,
            budget,
            view: json!({"node":context.var("DOMAIN_MARKER"),"class":identity.class,
                "scope":identity.scope,"epoch":identity.epoch,"initial_remaining_ms":remaining}),
            checkpoint: None,
            retired: false,
            entered: Instant::now(),
            retired_after_ms: None,
        };
        ensure!(
            self.entries.lock().unwrap().insert(role, entry).is_none(),
            "duplicate worker fixture frame"
        );
        Ok(Frame {
            trace: self.clone(),
            role,
        })
    }

    pub async fn hold(self: &Arc<Self>, context: &Context) -> Result<()> {
        let _frame = self.enter(Role::Child, context)?;
        self.ready.notify_one();
        // Graceful drain releases this after admission closes. Failure cases
        // must observe native retirement while the business future stays held;
        // their final cleanup releases the gate only after auditing that proof.
        self.release.notified().await;
        Ok(())
    }

    pub fn observe_provider(&self, context: &Context) -> Result<()> {
        // Called only while polling the provider under its installed native
        // turn. The coordinator never installs or uses a foreign Context.
        let storage = context.storage()?;
        let scope = &context
            .identity()
            .context("worker provider has no identity")?
            .scope;
        let sample = cellar::storage::sync_sample(scope)?
            .context("worker provider has no native storage sample")?;
        if sample.in_transaction {
            return Ok(());
        }
        let checkpoint = json!({"count":storage.get_json("count")?,
            "before_receipt":storage.get_json("receipt:worker-before")?,
            "after_receipt":storage.get_json("receipt:worker-after")?,
            "transaction_open":sample.in_transaction,"write_position":sample.position});
        let mut entries = self.entries.lock().unwrap();
        entries
            .get_mut(&Role::Provider)
            .context("worker provider was not observed")?
            .checkpoint = Some(checkpoint);
        Ok(())
    }

    pub fn observation_failed(&self, error: anyhow::Error) {
        self.observation_error
            .lock()
            .unwrap()
            .get_or_insert_with(|| error.to_string());
    }

    pub fn check_observation(&self) -> Result<()> {
        let error = self.observation_error.lock().unwrap();
        ensure!(
            error.is_none(),
            "worker native observation failed: {error:?}"
        );
        Ok(())
    }

    pub fn checkpoint_ready(&self, after: bool, delta: i64) -> bool {
        let entries = self.entries.lock().unwrap();
        let Some(snapshot) = entries
            .get(&Role::Provider)
            .and_then(|entry| entry.checkpoint.as_ref())
        else {
            return false;
        };
        checkpoint_matches(snapshot, after, delta)
    }

    pub fn live(&self, owner: &str, caller: &str, provider_class: &str) -> Result<Value> {
        self.check_observation()?;
        let entries = self.entries.lock().unwrap();
        ensure!(
            entries.len() == 2,
            "worker case lacks provider/child execution"
        );
        let parent = &entries[&Role::Provider];
        let child = &entries[&Role::Child];
        ensure!(
            !parent.retired && !child.retired,
            "worker frame retired before failure injection"
        );
        ensure!(
            parent.view["node"] == owner && child.view["node"] == owner,
            "worker frames ran outside the owner"
        );
        ensure!(
            parent.view["class"] == provider_class && child.view["class"] == "RustCounter",
            "worker frame classes differ"
        );
        ensure!(
            parent.view["scope"] != child.view["scope"] && !parent.source.owns(&child.budget),
            "worker child reused parent authority"
        );
        ensure!(
            parent.view["epoch"].as_u64().is_some_and(|epoch| epoch > 0)
                && child.view["epoch"].as_u64().is_some_and(|epoch| epoch > 0),
            "worker frame epochs are missing"
        );
        ensure!(
            self.caller_node.lock().unwrap().as_deref() == Some(caller),
            "worker call was not admitted at the expected source"
        );
        // Sources were captured inside their actual SDK turns. Their weak
        // clock/authority probes may run here without installing a foreign
        // native context or keeping one alive through the test.
        parent.source.start()?.remaining_ms()?;
        child.source.start()?.remaining_ms()?;
        Ok(
            json!({"provider":parent.view,"child":child.view,"caller_node":caller,
            "checkpoint":parent.checkpoint,"distinct_authority":true,"live_before_failure":true}),
        )
    }

    pub fn termination(&self, owner: &str) -> Result<Value> {
        let entries = self.entries.lock().unwrap();
        let trigger = entries
            .get(&Role::Trigger)
            .context("V8 trigger never reached native code")?;
        ensure!(
            entries.len() == 3
                && trigger.view["node"] == owner
                && trigger.view["class"] == "RustCounter"
                && trigger.view["epoch"]
                    .as_u64()
                    .is_some_and(|epoch| epoch > 0),
            "V8 trigger has no original native authority"
        );
        for role in [Role::Provider, Role::Child] {
            let original = entries
                .get(&role)
                .context("V8 trigger has no held provider/child pair")?;
            ensure!(
                original.view["scope"] != trigger.view["scope"]
                    && !original.source.owns(&trigger.budget),
                "V8 trigger reused held authority"
            );
        }
        Ok(trigger.view.clone())
    }

    pub fn retired(&self, frames: usize) -> bool {
        let entries = self.entries.lock().unwrap();
        entries.len() == frames && entries.values().all(|entry| entry.retired)
    }

    pub fn minimum_remaining_ms(&self) -> Result<u64> {
        let entries = self.entries.lock().unwrap();
        ensure!(
            entries.len() == 2 && entries.values().all(|entry| !entry.retired),
            "shutdown probe no longer has two live native frames"
        );
        entries.values().try_fold(u64::MAX, |minimum, entry| {
            Ok(minimum.min(entry.budget.remaining_ms()?))
        })
    }

    /// Read only the original weak budgets and physical frame retirement.
    pub fn snapshot(&self) -> Value {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .map(|(role, entry)| {
                let remaining = match entry.budget.remaining_ms() {
                    Ok(ms) => json!({"remaining_ms":ms}),
                    Err(error) => json!({"refused":format!("{:?}", error.code())}),
                };
                (
                    format!("{role:?}"),
                    json!({"initial_remaining_ms":entry.view["initial_remaining_ms"],
                "age_ms":entry.entered.elapsed().as_millis(),"budget":remaining,
                "retired":entry.retired,"retired_after_ms":entry.retired_after_ms}),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }

    pub fn assert_retired_before_deadline(&self, frames: usize) -> Result<()> {
        let entries = self.entries.lock().unwrap();
        ensure!(
            entries.len() == frames,
            "forced shutdown lost a native frame"
        );
        for entry in entries.values() {
            let retired = entry
                .retired_after_ms
                .context("forced shutdown left a native frame live")?;
            let original = entry.view["initial_remaining_ms"]
                .as_u64()
                .context("forced shutdown has no original native deadline")?;
            ensure!(retired < u128::from(original),
                "native frame reached its RPC deadline before shutdown retired it: {retired}ms >= {original}ms");
        }
        Ok(())
    }

    pub fn assert_revoked(&self, frames: usize) -> Result<()> {
        let entries = self.entries.lock().unwrap();
        ensure!(
            entries.len() == frames && entries.values().all(|entry| entry.retired),
            "worker native frames are still live"
        );
        for entry in entries.values() {
            let failure = entry
                .source
                .start()
                .err()
                .context("retired worker frame retained budget authority")?;
            ensure!(
                failure.code() == rpc::Code::ContextExpired,
                "worker budget probe did not expire with its original scope"
            );
        }
        Ok(())
    }

    pub fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }

    pub fn clear_probes(&self) {
        // Probes are weak and deliberately retain no native context.
        self.entries.lock().unwrap().clear();
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        if let Some(entry) = self.trace.entries.lock().unwrap().get_mut(&self.role) {
            entry.retired = true;
            entry.retired_after_ms = Some(entry.entered.elapsed().as_millis());
        }
    }
}

fn checkpoint_matches(snapshot: &Value, after: bool, delta: i64) -> bool {
    let count = if after {
        json!(delta.to_string())
    } else {
        Value::Null
    };
    let receipt = if after {
        json!({"delta":delta.to_string(),"value":delta.to_string()})
    } else {
        Value::Null
    };
    snapshot.as_object().is_some_and(|object| {
        object.len() == 5
            && ["count", "before_receipt", "after_receipt"]
                .iter()
                .all(|key| object.contains_key(*key))
    }) && snapshot["count"] == count
        && snapshot["before_receipt"].is_null()
        && snapshot["after_receipt"] == receipt
        && snapshot["transaction_open"] == false
        && snapshot["write_position"]
            .as_u64()
            .is_some_and(|position| !after || position > 0)
}

#[cfg(test)]
#[path = "trace_tests.rs"]
mod tests;
