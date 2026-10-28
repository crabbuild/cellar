// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A deliberately incorrect native cell that bypasses generated reply finishing.
use anyhow::ensure;
use cellar::sdk::{rpc, Cell, Context, HandlerFuture, Invocation, Output};
use serde_json::{json, Value};
use std::sync::Arc;

use super::super::generated::IncrementRequest;

pub struct Raw {
    inner: Arc<dyn Cell>,
    contract: Arc<rpc::Contract>,
    observation: Arc<super::Native>,
}

impl Raw {
    pub fn new(
        inner: Arc<dyn Cell>,
        contract: Arc<rpc::Contract>,
        observation: Arc<super::Native>,
    ) -> Self {
        Self {
            inner,
            contract,
            observation,
        }
    }
}

impl Cell for Raw {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let probe = if let Invocation::CellRpc { method, args } = &invocation {
                if method.ends_with("/Increment") {
                    let (_, bytes) =
                        rpc::decode_envelope(args, self.contract.limits().max_message_bytes)?;
                    let request =
                        <IncrementRequest as rpc::prost::Message>::decode(bytes.as_slice())?;
                    let fresh = context
                        .storage()?
                        .get_json(&format!("receipt:{}", request.operation_id))?
                        .is_none();
                    fresh.then_some(request.operation_id)
                } else {
                    None
                }
            } else {
                None
            };
            let output = self
                .inner
                .clone()
                .invoke(context.clone(), invocation)
                .await?;
            let Some(id) = probe else {
                return Ok(output);
            };
            let mode = id
                .strip_prefix("t06-raw:")
                .and_then(|value| value.split(':').next())
                .ok_or_else(|| anyhow::anyhow!("invalid raw provenance operation"))?;
            // The ordinary generated handler has committed/synced a receipt
            // before this custom wrapper fabricates or forwards its envelope.
            let Output::Rpc(reply) = output else {
                anyhow::bail!("raw probe has no RPC result");
            };
            rpc::decode_reply(self.contract.digest(), &reply, self.contract.limits())?;
            let (reply, child) = match mode {
                "forge" => {
                    let failure = serde_json::from_value(
                        json!({"code":"MethodNotFound","outcome":"not_committed"}),
                    )?;
                    (
                        rpc::Prepared::refusal(
                            self.contract.digest(),
                            failure,
                            self.contract.limits(),
                        )?,
                        Value::Null,
                    )
                }
                "forward" => {
                    let reply = context
                        .namespace("RUST")?
                        .get_by_name("t06-raw-unconstructed")
                        .rpc(
                            &context,
                            "example.counter.v1.Counter/PrivateHelper",
                            rpc::encode_envelope(
                                self.contract.digest(),
                                vec![],
                                self.contract.limits().max_message_bytes,
                            )?,
                        )
                        .await?;
                    let failure =
                        rpc::decode_reply(self.contract.digest(), &reply, self.contract.limits())
                            .unwrap_err();
                    ensure!(
                        failure.code() == rpc::Code::MethodNotFound
                            && failure.outcome() == rpc::EffectOutcome::NotCommitted,
                        "child lost its actual host pre-factory refusal"
                    );
                    (
                        reply,
                        json!({"code":failure.code(),"outcome":failure.outcome()}),
                    )
                }
                _ => anyhow::bail!("unknown raw provenance mode"),
            };
            self.observation.record(&context, &id, mode, child)?;
            Ok(Output::Rpc(reply))
        })
    }
}
