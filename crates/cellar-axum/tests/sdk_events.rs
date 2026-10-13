//! Exercise the builder's SDK contracts through real broker and workflow cells.
#![allow(clippy::disallowed_methods)] // Only the test coordinator uses a host timeout.
mod support;
use anyhow::{ensure, Result};
use axum::{routing::get, Json, Router};
use cellar::engine_api::RpcData;
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output};
use cellar_axum::{ApplicationBuilder, ApplicationDefinition, AxumCell, CellContext};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

struct Rpc;

impl Cell for Rpc {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = invocation else {
                anyhow::bail!("expected RPC");
            };
            ensure!(method == "echo");
            context
                .storage()?
                .put_json("invoked", &serde_json::json!(true))?;
            Ok(Output::Rpc(args))
        })
    }
}

async fn exercise(
    CellContext(context): CellContext,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let result = async {
        let response = context
            .namespace("COUNTERS")?
            .get_by_name("alpha")
            .rpc(&context, "echo", RpcData::Json("7".into()))
            .await?;
        ensure!(matches!(response,RpcData::Json(ref value) if value == "7"));
        context
            .queue("Q")?
            .send(
                &context,
                cellar::sdk::queue::Body::Text("delivered".into()),
                Default::default(),
            )
            .await?;
        context
            .workflow("WF")?
            .create(
                &context,
                cellar::sdk::workflow::CreateOptions {
                    id: Some("order-1".into()),
                    params: Some(cellar_native::clone::StructuredValue::from_json(
                        serde_json::json!(7),
                    )?),
                    ..Default::default()
                },
            )
            .await?;
        Ok(Json(serde_json::json!({"rpc":7})))
    }
    .await;
    result.map_err(|error: anyhow::Error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
    })
}

fn definition(queue: Arc<Notify>, workflow: Arc<Notify>) -> Result<ApplicationDefinition> {
    ApplicationBuilder::new(Router::new().route("/exercise", get(exercise)))
        .cell("Counter", |_, _| {
            Ok(Arc::new(AxumCell::with_cell(Router::new(), Arc::new(Rpc))))
        })?
        .events(move |_, invocation| {
            let signal = queue.clone();
            Box::pin(async move {
                let Invocation::Queue(batch) = invocation else {
                    anyhow::bail!("expected Queue");
                };
                cellar::sdk::queue::dispatch(batch, move |event| async move {
                    ensure!(event.queue() == "jobs" && event.messages().len() == 1);
                    event.ack_all()?;
                    signal.notify_one();
                    Ok(())
                })
                .await
            })
        })
        .workflow("Orders", move |_, event, steps| {
            let signal = workflow.clone();
            Box::pin(async move {
                ensure!(event.workflow_name == "orders");
                let output = steps
                    .do_step("first", move |_| async move { Ok(event.payload) })
                    .await?;
                signal.notify_one();
                Ok(output)
            })
        })?
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpc_queue_and_workflow_delegate_through_the_existing_sdk_on_both_hosts() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let queue = Arc::new(Notify::new());
        let workflow = Arc::new(Notify::new());
        let q = queue.clone();
        let w = workflow.clone();
        let host = support::start_configured(
            directory.path(),
            external,
            "",
            "sdk-events",
            None,
            move || definition(q.clone(), w.clone()),
            |graph| {
                let options = &mut graph.primary.options;
                options.do_classes.extend([
                    cellar::deploy::QUEUE_CLASS.into(),
                    cellar::deploy::workflow_class("adapter"),
                ]);
                options
                    .queue_bindings
                    .push(cellar::engine_api::QueueBinding {
                        environment: "Q".into(),
                        queue: "jobs".into(),
                        delivery_delay: 0,
                    });
                options
                    .queue_consumers
                    .push(cellar::protocol::QueueConsumerConfig {
                        queue: "jobs".into(),
                        max_batch_size: 1,
                        max_batch_timeout: 0,
                        max_retries: 1,
                        dead_letter_queue: None,
                        max_concurrency: Some(1),
                        retry_delay: None,
                    });
                options
                    .workflow_bindings
                    .push(cellar::engine_api::WorkflowBinding {
                        environment: "WF".into(),
                        workflow: "orders".into(),
                        class: "Orders".into(),
                    });
            },
        )
        .await?;
        let response = support::client()?
            .get(format!("{}/exercise", host.url))
            .send()
            .await?;
        ensure!(
            response.status() == 200,
            "SDK exercise: {}",
            response.text().await?
        );
        ensure!(response.json::<serde_json::Value>().await?["rpc"] == 7);
        tokio::time::timeout(Duration::from_secs(5), queue.notified()).await?;
        tokio::time::timeout(Duration::from_secs(5), workflow.notified()).await?;
        host.stop().await?;
    }
    Ok(())
}
