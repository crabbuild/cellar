//! Native observations cannot be substituted by guest-supplied status fields.
use anyhow::{ensure, Result};
use cellar::sdk::{rpc, Context, Invocation};
use rpc::prost::Message;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use super::super::generated::{CounterClient, GetCountRequest, IncrementRequest};

struct Entry {
    view: Value,
    budget: rpc::CallBudget,
    retired: bool,
}

#[derive(Default)]
struct State {
    armed: bool,
    entries: BTreeMap<String, Entry>,
}

#[derive(Default)]
pub struct Tracker(Mutex<State>);

pub struct Frame {
    tracker: Arc<Tracker>,
    role: String,
    path: Vec<String>,
    pub native: bool,
    pub gate: bool,
}

impl Tracker {
    pub fn disarm_after_node_drain(&self) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        ensure!(
            state.entries.values().all(|entry| entry.retired),
            "cycle shutdown retained native frames"
        );
        state.armed = false;
        Ok(())
    }

    /// The coordinator calls this only after the previous native nodes joined.
    pub fn begin_case(&self) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        ensure!(
            state.entries.values().all(|entry| entry.retired),
            "previous cycle still has native frames"
        );
        *state = State {
            armed: true,
            entries: BTreeMap::new(),
        };
        Ok(())
    }

    pub fn enter(self: &Arc<Self>, context: &Context, event: &Invocation) -> Result<Option<Frame>> {
        let Some(identity) = context.identity() else {
            return Ok(None);
        };
        let Invocation::CellRpc { method, args } = event else {
            return Ok(None);
        };
        let gate = identity.name.as_deref() == Some("cycle-gate") && method.ends_with("/GetCount");
        let path = if method.ends_with("/Increment") {
            let (_, body) = rpc::decode_envelope(args, rpc::Limits::default().max_message_bytes)?;
            let request = IncrementRequest::decode(body.as_slice())?;
            if !request.operation_id.starts_with("t15|") {
                return Ok(None);
            }
            request
                .operation_id
                .split('|')
                .map(String::from)
                .collect::<Vec<_>>()
        } else if gate {
            vec![]
        } else {
            return Ok(None);
        };
        if !gate {
            ensure!(
                path.len() == 5
                    && ["self", "aba"].contains(&path[3].as_str())
                    && ["outer", "middle", "inner"].contains(&path[4].as_str()),
                "invalid native cycle path"
            );
        }
        let role = if gate { "gate" } else { &path[4] }.to_owned();
        let source = rpc::BudgetSource::capture(context)?;
        let budget = source.start()?;
        let remaining = budget.remaining_ms()?;
        let mut state = self.0.lock().unwrap();
        if !state.armed {
            return Ok(None);
        }
        ensure!(
            state.entries.len() < 4 && !state.entries.contains_key(&role),
            "native cycle frame bound/identity violated"
        );
        ensure!(
            state
                .entries
                .values()
                .all(|entry| !source.owns(&entry.budget)),
            "reentrant cycle inherited another invocation's authority"
        );
        state.entries.insert(
            role.clone(),
            Entry {
                budget,
                retired: false,
                view: json!({"role":role,"node":context.var("DOMAIN_MARKER"),"class":identity.class,
                "name":identity.name,"scope":identity.scope,"epoch":identity.epoch,
                "distinct_authority":true,"initial_remaining_ms":remaining}),
            },
        );
        Ok(Some(Frame {
            tracker: self.clone(),
            role,
            path,
            native: identity.class == "RustCounter",
            gate,
        }))
    }

    pub fn snapshot(&self) -> Value {
        let state = self.0.lock().unwrap();
        Value::Object(
            state
                .entries
                .iter()
                .map(|(role, entry)| {
                    let mut view = entry.view.clone();
                    // This captured clock/cancellation check does not consult the
                    // controller's executing SDK turn. Foreign-turn refusal is not
                    // evidence that a suspended native frame actually lost authority.
                    view["revoked"] = json!(entry
                        .budget
                        .remaining_ms()
                        .is_err_and(|error| error.code() == rpc::Code::ContextExpired));
                    view["retired"] = json!(entry.retired);
                    (role.clone(), view)
                })
                .collect(),
        )
    }
}

impl Frame {
    /// Called inside the real Rust provider, after its atomic receipt synced.
    pub async fn wait_native(&self, context: &Context) -> Result<()> {
        let options = rpc::CallOptions {
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        if self.role == "inner" {
            CounterClient::bind(context, "RUST", "cycle-gate")?
                .get_count_with_options(GetCountRequest {}, options)
                .await?;
        } else {
            let next = if self.role == "outer" && self.path[3] == "aba" {
                "middle"
            } else {
                "inner"
            };
            let mut path = self.path.clone();
            path[4] = next.into();
            let binding = if next == "middle" { &path[2] } else { &path[1] };
            CounterClient::bind(
                context,
                binding,
                if next == "middle" {
                    "cycle-b"
                } else {
                    "cycle-a"
                },
            )?
            .increment_with_options(
                IncrementRequest {
                    delta: Some(1),
                    operation_id: path.join("|"),
                },
                options,
            )
            .await?;
        }
        anyhow::bail!("held native cycle unexpectedly succeeded")
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        if let Some(entry) = self.tracker.0.lock().unwrap().entries.get_mut(&self.role) {
            entry.retired = true;
        }
    }
}
