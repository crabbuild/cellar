use cellar::sdk::{rpc, Context};
use serde_json::{json, Value};
use std::{sync::Mutex, time::Duration};

use super::super::generated::{CounterClient, GetCountRequest};

#[derive(Default)]
pub struct Native(Mutex<Option<Value>>);

impl Native {
    pub fn control(&self, clear: bool) -> Value {
        let mut state = self.0.lock().unwrap();
        if clear {
            *state = None;
        }
        state.clone().unwrap_or_else(|| json!({"ready":false}))
    }

    pub async fn after_commit(&self, context: &Context, id: &str) -> Result<(), rpc::Failure> {
        let mode = id.split(':').nth(1).unwrap_or_default();
        let internal = || rpc::Failure::new(rpc::Code::Internal);
        let failure = match mode {
            "child" => {
                let result = CounterClient::bind(context, "RUST", "t06-unconstructed")?
                    .get_count_with_options(
                        GetCountRequest {},
                        rpc::CallOptions {
                            timeout: Some(Duration::ZERO),
                            ..Default::default()
                        },
                    )
                    .await;
                let failure = result.err().ok_or_else(internal)?;
                if failure.code() != rpc::Code::DeadlineExceeded
                    || failure.outcome() != rpc::EffectOutcome::NotCommitted
                {
                    return Err(internal());
                }
                failure
            }
            "forge" => {
                serde_json::from_value(json!({"code":"MethodNotFound","outcome":"not_committed"}))
                    .map_err(|_| internal())?
            }
            "invalid" => {
                if serde_json::from_value::<rpc::Failure>(
                    json!({"code":"T06-private-code-sentinel","outcome":"not_committed"}),
                )
                .is_ok()
                {
                    return Err(internal());
                }
                internal()
            }
            _ => return Err(internal()),
        };
        let storage = context.storage().map_err(|_| internal())?;
        let count = storage
            .get_json("count")
            .map_err(|_| internal())?
            .ok_or_else(internal)?;
        let receipt = storage
            .get_json(&format!("receipt:{id}"))
            .map_err(|_| internal())?
            .ok_or_else(internal)?;
        let mut state = self.0.lock().unwrap();
        if state.is_some() {
            return Err(internal());
        }
        *state = Some(
            json!({"ready":true,"id":id,"mode":mode,"parent_active":true,"count":count,"receipt":receipt,
            "child":if mode == "child" {json!({"code":failure.code(),"outcome":failure.outcome()})} else {Value::Null}}),
        );
        Err(failure)
    }
}
