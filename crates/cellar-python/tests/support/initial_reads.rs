// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Capture on a real native cell turn, with competing writes and bucket recovery.
use super::*;

struct WithWriter(Arc<PythonApplication>);

impl Application for WithWriter {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.0.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(Writer(self.0.create_cell(context, identity)?)))
    }
}

struct Writer(Arc<dyn Cell>);

impl Cell for Writer {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if request.url.contains("/native/")) {
            return Box::pin(async move {
                let Invocation::Fetch(request) = event else {
                    unreachable!()
                };
                ensure!(request.method == "POST");
                let value = if request.url.ends_with("/seed") {
                    json!(1)
                } else if request.url.ends_with("/write") {
                    json!(41)
                } else if request.url.ends_with("/large") {
                    json!("x".repeat(4096))
                } else if request.url.ends_with("/oversize") {
                    json!("PRIVATE-STATE-SENTINEL".repeat(1024))
                } else {
                    anyhow::bail!("unknown native fixture path")
                };
                context.storage()?.transaction_sync(|tx| {
                    tx.put_json("value", &value)?;
                    tx.put_json("nullable", &Value::Null)?;
                    tx.delete("absent")?;
                    Ok(())
                })?;
                Ok(Output::Http(
                    http::Response::from_json(&json!(true))?.into_http(&context)?,
                ))
            });
        }
        self.0.clone().invoke(context, event)
    }
}

async fn get(client: &reqwest::Client, url: &str, path: &str) -> Result<Value> {
    Ok(client
        .get(format!("{url}{path}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn post(client: &reqwest::Client, url: &str, path: &str) -> Result<()> {
    ensure!(
        client
            .post(format!("{url}{path}"))
            .send()
            .await?
            .error_for_status()?
            .json::<bool>()
            .await?
    );
    Ok(())
}

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options {
            // Allow the JSON response wrapper around a 4 KiB value. The
            // combined invocation limit below still rejects state + 4 KiB input.
            max_body_bytes: 8192,
            max_value_bytes: 8192,
            max_batch_operations: 3,
            ..Options::default()
        },
        Source::new(include_str!("initial_reads.py"), "initial_reads.py")
            .events([EventKind::Fetch])
            .cell("Counter")
            .cell_initial_reads("Counter", EventKind::Fetch, ["value", "nullable", "absent"]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
                let wrapped = Arc::new(WithWriter(application.clone()));
                let (node, url) = start_with(&directory.path().join("old"), wrapped.clone(), "initial-old", Some(bucket.clone())).await?;
                let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
                post(&client, &url, "/native/seed").await?;
                let before = application.bridge_report().operations.admitted;
                let view = get(&client, &url, "/view").await.context("initial captured view")?;
                ensure!(view == json!({"value":{"present":true,"value":1},"nullable":{"present":true,"value":null},"absent":{"present":false,"value":null}}));
                ensure!(application.bridge_report().operations.admitted - before == 1, "initial reads made extra SDK calls");

                post(&client, &url, "/native/large").await?;
                ensure!(get(&client, &url, "/view").await.context("large captured view")?["value"]["value"].as_str().unwrap().len() == 4096);
                client.post(format!("{url}/view")).body(vec![0_u8; 1024]).send().await?.error_for_status().context("large captured view with body")?;
                let entered = get(&client, &url, "/entered").await?;
                let refused = client.post(format!("{url}/view")).body(vec![0_u8; 4096]).send().await?;
                ensure!(refused.status().is_server_error(), "body and initial state escaped combined budget");
                ensure!(get(&client, &url, "/entered").await? == entered);
                post(&client, &url, "/native/oversize").await?;
                let refused = client.get(format!("{url}/view")).send().await?;
                ensure!(refused.status().is_server_error());
                ensure!(!refused.text().await?.contains("PRIVATE-STATE-SENTINEL"));
                ensure!(get(&client, &url, "/entered").await? == entered);
                post(&client, &url, "/native/seed").await?;

                let held = get(&client, &url, "/hold");
                let writer = async {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while get(&client, &url, "/ready").await? != json!(true) {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        post(&client, &url, "/native/write").await?;
                        ensure!(get(&client, &url, "/release").await? == json!(true));
                        Ok::<_, anyhow::Error>(())
                    }).await??;
                    Ok::<_, anyhow::Error>(())
                };
                let (held, ()) = tokio::try_join!(held, writer)?;
                ensure!(held == json!({"initial":1,"current":41}));
                ensure!(get(&client, &url, "/own-write").await? == json!({"initial":41,"current":42}));
                stop(node, &monitor).await?;
                tokio::time::sleep(Duration::from_millis(1100)).await;
                let (node, url) = start_with(&directory.path().join("new"), wrapped, "initial-new", Some(bucket)).await?;
                ensure!(get(&client, &url, "/view").await.context("recovered captured view")?["value"] == json!({"present":true,"value":42}));
                stop(node, &monitor).await
            })
        },
    )?;
    ensure!(
        report.complete() && report.bridge.operations.settled(),
        "initial reads leaked work: {report:?}"
    );
    ensure!(report.objects_created == 2 && report.objects_retired == 2);
    Ok(())
}
