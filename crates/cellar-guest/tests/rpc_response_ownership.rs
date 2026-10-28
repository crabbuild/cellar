// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native response ownership overlaps the language bridge's byte admission.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::{rpc, *};
use rpc::prost::Message;
use std::{sync::Arc, time::Duration};

#[allow(dead_code)]
#[path = "../../../tools/rpc/fixtures/generated/rust/example.counter.v1.rs"]
mod generated;
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;

struct Counter;

#[derive(Default)]
struct Signals {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl generated::CounterHandler for Counter {
    fn increment(
        self: Arc<Self>,
        _: Context,
        _: generated::IncrementRequest,
    ) -> rpc::HandlerFuture<generated::IncrementResponse> {
        Box::pin(async { Ok(generated::IncrementResponse::default()) })
    }

    fn get_count(
        self: Arc<Self>,
        _: Context,
        _: generated::GetCountRequest,
    ) -> rpc::HandlerFuture<generated::GetCountResponse> {
        Box::pin(async {
            Ok(generated::GetCountResponse {
                count: Some(generated::Count { value: 42 }),
            })
        })
    }
}

fn application(signals: Arc<Signals>) -> Result<Arc<dyn Application>> {
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?);
    let ingress = contract.clone();
    let application = Registry::new(move |context, invocation| {
        let contract = ingress.clone();
        let signals = signals.clone();
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                anyhow::bail!("expected HTTP")
            };
            let budget = if request.url.ends_with("/budget") {
                Some(rpc::BudgetSource::capture(&context)?.start()?)
            } else {
                None
            };
            let bytes = cellar_guest::operations::call_response_bytes_in_budget(
                &context,
                contract.clone(),
                serde_json::json!({
                    "binding":"COUNTERS", "name":"handoff",
                    "method":"example.counter.v1.Counter/GetCount",
                    "contract_digest":contract.digest(),
                }),
                vec![],
                None,
                budget,
            )
            .await?;
            if request.url.ends_with("/hold") {
                signals.entered.notify_one();
                signals.release.notified().await;
            }
            // The result can wait for the adapter's next poll after native
            // delivery retires. Its byte owner must survive that scheduling gap.
            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            let services = cellar::asyncrt::services();
            let before_bridge = services.rpc_response_usage().reserved_response_bytes > 0;
            let response = generated::GetCountResponse::decode(bytes.as_ref())?;
            let (bytes, owner) = bytes.into_parts();
            let capacity = if request.url.ends_with("/refuse") {
                0
            } else {
                4096
            };
            let bridge = Arc::new(tokio::sync::Semaphore::new(capacity));
            let admitted = bridge
                .clone()
                .try_acquire_many_owned(u32::try_from(bytes.len() + 128)?);
            let with_bridge = services.rpc_response_usage().reserved_response_bytes > 0;
            let accepted = admitted.is_ok();
            // This is the same transfer order as the adapters: acquire a
            // bridge permit, then relinquish native response ownership.
            let queued = admitted.ok().map(|permit| (bytes, permit));
            drop(owner);
            let retained_bridge = queued.is_some() && bridge.available_permits() < capacity;
            let value = serde_json::json!({
                "value":response.count.unwrap().value,
                "before_bridge":before_bridge, "with_bridge":with_bridge,
                "accepted":accepted, "retained_bridge":retained_bridge,
            });
            drop(queued);
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    })
    .cell("Counter", |_, _| {
        Ok(Arc::new(generated::CounterRegistration::register(
            rpc::Router::new(),
            Arc::new(Counter),
        )?) as Arc<dyn Cell>)
    })?
    .rpc_service("Counter", "example.counter.v1.Counter", contract)?;
    Ok(Arc::new(application))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guest_response_admission_survives_the_bridge_handoff_and_refusal() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let (node, base) = native::start(directory.path(), application(Arc::default())?).await?;
    let services = node.services();
    let operation = async {
        for path in ["plain", "budget", "refuse"] {
            let response = client.get(format!("{base}/{path}")).send().await?;
            let status = response.status();
            let body = response.text().await?;
            ensure!(status.is_success(), "{path}: {status}: {body}");
            let reply: serde_json::Value = serde_json::from_str(&body)?;
            ensure!(
                reply["value"] == 42,
                "probe did not receive its generated reply: {reply}"
            );
            ensure!(
                reply["before_bridge"] == true && reply["with_bridge"] == true,
                "native response retired before {path} bridge admission: {reply}"
            );
            ensure!(
                reply["accepted"] == (path != "refuse")
                    && reply["retained_bridge"] == (path != "refuse"),
                "bridge admission/refusal did not execute: {reply}"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let outcome = if operation.is_err() {
        node.cancel().await
    } else {
        node.shutdown(ShutdownMode::Preserve).await
    };
    ensure!(
        services.rpc_response_usage() == Default::default(),
        "native response survived host join"
    );
    ensure!(
        services.outbound_rpc_usage() == Default::default(),
        "native input survived host join"
    );
    operation?;
    ensure!(
        matches!(outcome?, NodeOutcome::Stopped(_)),
        "handoff node did not stop"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_cancel_releases_a_response_waiting_for_bridge_admission() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let signals = Arc::new(Signals::default());
    let (node, base) = native::start(directory.path(), application(signals.clone())?).await?;
    let services = node.services();
    let mut requesting =
        tokio::spawn(async move { client.get(format!("{base}/hold")).send().await });
    let entered = tokio::time::timeout(Duration::from_secs(2), signals.entered.notified()).await;
    let held = services.rpc_response_usage();
    // Cancel and join even when the barrier or reservation observation failed.
    let outcome = node.cancel().await;
    let joined = tokio::time::timeout(Duration::from_secs(2), &mut requesting).await;
    let request_joined = joined.is_ok();
    if let Ok(completed) = joined {
        drop(completed?);
    }
    if !request_joined {
        requesting.abort();
        let _ = requesting.await;
    }
    entered?;
    ensure!(
        held.active_buffers > 0 && held.reserved_response_bytes > 0,
        "response lost admission while waiting for the bridge: {held:?}"
    );
    let outcome = outcome?;
    ensure!(
        outcome == NodeOutcome::Cancelled,
        "response-wait cancellation returned {outcome:?}"
    );
    ensure!(
        request_joined,
        "client request survived joined host cancellation"
    );
    ensure!(
        services.rpc_response_usage() == Default::default(),
        "response survived host cancellation"
    );
    ensure!(
        services.outbound_rpc_usage() == Default::default(),
        "request survived host cancellation"
    );
    Ok(())
}
