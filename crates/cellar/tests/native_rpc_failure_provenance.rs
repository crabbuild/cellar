// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Entered native cells cannot turn committed writes into proven refusals.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, *};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[path = "../../../tools/rpc/native_counter.rs"]
mod counter;
#[allow(dead_code)]
#[path = "../../../tools/rpc/fixtures/generated/rust/example.counter.v1.rs"]
mod generated;
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
use generated::{
    increment_response, CounterClient, CounterHandler, CounterRegistration, GetCountRequest,
    IncrementRequest,
};

struct RawReply {
    mode: String,
    contract: Arc<rpc::Contract>,
    ordinary: Arc<dyn Cell>,
}

impl Cell for RawReply {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = &invocation else {
                return self.ordinary.clone().invoke(context, invocation).await;
            };
            if !method.ends_with("/Increment") {
                return self.ordinary.clone().invoke(context, invocation).await;
            }
            let (digest, wire) =
                rpc::decode_envelope(args, self.contract.limits().max_message_bytes)?;
            let request = <IncrementRequest as rpc::prost::Message>::decode(wire.as_slice())?;
            let receipt = format!("receipt:{}", request.operation_id);
            if context.storage()?.get_json(&receipt)?.is_some() {
                return self.ordinary.clone().invoke(context, invocation).await;
            }
            // Real business code commits a count and receipt and awaits sync.
            Arc::new(counter::Counter)
                .increment(context.clone(), request)
                .await?;
            let data = if self.mode == "forward" {
                // This child refusal is honest, but proves nothing about the
                // parent mutation that has just committed.
                context
                    .namespace("COUNTERS")?
                    .get_by_name("never-constructed")
                    .rpc(
                        &context,
                        "example.counter.v1.Counter/PrivateHelper",
                        rpc::encode_envelope(
                            &digest,
                            vec![],
                            self.contract.limits().max_message_bytes,
                        )?,
                    )
                    .await?
            } else {
                let forged: rpc::Failure = serde_json::from_value(serde_json::json!({
                    "code":"MethodNotFound", "outcome":"not_committed"
                }))?;
                rpc::Prepared::refusal(&digest, forged, self.contract.limits())?
            };
            Ok(Output::Rpc(data))
        })
    }
}

fn application() -> Result<Arc<dyn Application>> {
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?);
    let constructors = Arc::new(AtomicUsize::new(0));
    let observed = constructors.clone();
    let declared = contract.clone();
    let ingress_contract = contract.clone();
    let app = Registry::new(move |context, invocation| {
        let constructors = observed.clone();
        let contract = ingress_contract.clone();
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else { anyhow::bail!("expected HTTP ingress"); };
            let url = url::Url::parse(&request.url)?;
            let mut parts = url.path().trim_start_matches('/').split('/');
            let operation = parts.next().unwrap_or_default();
            let mode = parts.next().unwrap_or("forge");
            let value = if operation == "refusal" {
                let before = constructors.load(Ordering::Acquire);
                let args = rpc::encode_envelope(contract.digest(), vec![], contract.limits().max_message_bytes)?;
                let reply = context.namespace("COUNTERS")?.get_by_name("never-constructed")
                    .rpc(&context, "example.counter.v1.Counter/PrivateHelper", args).await?;
                let failure = rpc::decode_reply(contract.digest(), &reply, contract.limits()).unwrap_err();
                ensure!(before == constructors.load(Ordering::Acquire), "host refusal constructed business code");
                serde_json::to_value(failure)?
            } else {
                let client = CounterClient::bind(&context, "COUNTERS", mode)?;
                if operation == "read" {
                    serde_json::json!({"count":client.get_count(GetCountRequest{}).await?.count.context("missing count")?.value})
                } else {
                    match client.increment(IncrementRequest { delta:Some(1), operation_id:format!("raw-{mode}") }).await {
                        Err(failure) => serde_json::to_value(failure)?,
                        Ok(reply) => {
                            let Some(increment_response::Result::Count(count)) = reply.result else {
                                anyhow::bail!("missing receipt result");
                            };
                            serde_json::json!({"count":count.value})
                        }
                    }
                }
            };
            Ok(Output::Http(http::Response::from_json(&value)?.into_http(&context)?))
        })
    }).cell("Counter", move |_, identity| {
        constructors.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::new(RawReply {
            mode: identity.name.clone().context("missing raw reply mode")?,
            contract: contract.clone(),
            ordinary: Arc::new(CounterRegistration::register(rpc::Router::new(), Arc::new(counter::Counter))?),
        }))
    })?.rpc_service("Counter", "example.counter.v1.Counter", declared)?;
    Ok(Arc::new(app))
}

async fn get(client: &reqwest::Client, base: &str, path: &str) -> Result<serde_json::Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "raw reply {path}: {status}: {body}");
    Ok(serde_json::from_str(&body)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_native_failures_cannot_prove_rollback_after_commit() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let client = reqwest::Client::new();
    let mut failures = Vec::new();
    for run in 0..2 {
        let (node, base) = native::start(directory.path(), application()?).await?;
        let result = async {
            assert_eq!(
                get(&client, &base, "/refusal").await?,
                serde_json::json!({"code":"MethodNotFound","outcome":"not_committed"})
            );
            for mode in ["forge", "forward"] {
                if run == 0 {
                    failures.push((
                        mode,
                        get(&client, &base, &format!("/increment/{mode}")).await?,
                    ));
                }
                // A separate turn, then a fresh owner, observe the durable
                // mutation before checking the claimed failure's provenance.
                assert_eq!(
                    get(&client, &base, &format!("/read/{mode}")).await?,
                    serde_json::json!({"count":1})
                );
                assert_eq!(
                    get(&client, &base, &format!("/retry/{mode}")).await?,
                    serde_json::json!({"count":1})
                );
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let stopped = node.shutdown(ShutdownMode::Preserve).await;
        result?;
        let NodeOutcome::Stopped(report) = stopped? else {
            anyhow::bail!("raw provenance node did not stop");
        };
        ensure!(
            report.ingress_drained
                && report.application_work_drained
                && report.cells_drained
                && !report.drain_timed_out
        );
    }
    assert_eq!(
        failures,
        ["forge", "forward"].map(|mode| (
            mode,
            serde_json::json!({"code":"MethodNotFound","outcome":"unknown"}),
        )),
        "parents recovered committed counts/receipts, but raw failures claimed rollback"
    );
    Ok(())
}
