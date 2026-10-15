//! Retained storage and commit are attempted from a poll spanning takeover.

use super::*;
use serde_json::{json, Value};
use std::sync::{Condvar, Mutex};
use tokio::sync::{mpsc, Semaphore};

struct Controls {
    before_commit: bool,
    entered: Semaphore,
    release: (Mutex<bool>, Condvar),
    attempted: mpsc::UnboundedSender<Result<(), String>>,
}

impl Controls {
    fn wait(&self) -> Result<()> {
        self.entered.add_permits(1);
        let (released, _) = self
            .release
            .1
            .wait_timeout_while(
                self.release.0.lock().unwrap(),
                Duration::from_secs(15),
                |released| !*released,
            )
            .unwrap();
        ensure!(*released, "test did not release the running callback");
        Ok(())
    }

    fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}

struct ReleaseOnDrop(Arc<Controls>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct RunningCounter {
    label: &'static str,
    controls: Arc<Controls>,
}

impl Cell for RunningCounter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        if !matches!(&invocation, Invocation::Fetch(request) if request.url.ends_with("/running")) {
            return Arc::new(Counter(self.label)).invoke(context, invocation);
        }
        Box::pin(async move {
            let db = context.storage()?;
            let events = context.stream::<Changed>("EVENTS")?;
            let result = if self.controls.before_commit {
                db.tx(|tx| {
                    tx.put_json("count", &json!(999))?;
                    tx.emit(&events, Changed { value: 999 })?;
                    self.controls.wait()?;
                    Ok(())
                })
            } else {
                self.controls.wait()?;
                db.tx(|tx| {
                    tx.put_json("count", &json!(999))?;
                    tx.emit(&events, Changed { value: 999 })
                })
            };
            self.controls
                .attempted
                .send(result.map_err(|error| format!("{error:#}")))?;
            // Even an application that swallows the local refusal must not
            // turn a fenced invocation into a visible successful response.
            Ok(super::super::response("stale-success"))
        })
    }
}

fn applications_with_controls(controls: Arc<Controls>) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let controls = controls.clone();
        Ok(Arc::new(
            Registry::new(|context, invocation| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = invocation else {
                        return Ok(Output::Done);
                    };
                    let account = context.namespace("ACCOUNTS")?.get_by_name("events");
                    Ok(Output::Http(account.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(RunningCounter {
                    label: "old",
                    controls: controls.clone(),
                }))
            })?,
        ))
    })
}

async fn pending(client: &reqwest::Client, node: &Node, scope: &str) -> Result<Value> {
    Ok(operator(
        client,
        node,
        scope,
        &json!({"op":"inspect"}),
        "running-owner",
    )
    .await?["result"]
        .clone())
}

fn assert_source(actual: &Value, original: &Value) -> Result<()> {
    for field in ["incarnation", "pending_rows", "pending_bytes"] {
        ensure!(actual[field] == original[field], "source {field} changed");
    }
    ensure!(actual["lanes"]
        .as_array()
        .is_some_and(|lanes| lanes.len() == 1));
    ensure!(
        actual["lanes"][0]["revision"] == original["lanes"][0]["revision"],
        "producer identity or progress changed"
    );
    Ok(())
}

async fn history(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(stream_operator(
        client,
        node,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "running-owner",
    )
    .await?["result"]["records"]
        .clone())
}

async fn owner(
    observer: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
    scope: &str,
) -> Result<Value> {
    observer
        .run(async {
            let (bytes, _) = bucket
                .get(&format!("cells/{scope}/own.json"))
                .await?
                .context("source owner missing")?;
            Ok::<_, anyhow::Error>(serde_json::from_slice(&bytes)?)
        })
        .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_source_poll_cannot_write_commit_or_reply_after_fenced_takeover() -> Result<()> {
    for before_commit in [false, true] {
        let directory = tempfile::tempdir()?;
        let old_core = Executor::new("running-source-old-core")?;
        let new_core = Executor::new("running-source-new-core")?;
        let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
        let (isolated, fault) = event_owner_outage::Outage::wrap(&bucket)?;
        let (send, mut attempted) = mpsc::unbounded_channel();
        let controls = Arc::new(Controls {
            before_commit,
            entered: Semaphore::new(0),
            release: (Mutex::new(false), Condvar::new()),
            attempted: send,
        });
        let release_on_drop = ReleaseOnDrop(controls.clone());
        let (config, mut services) = inputs(
            "running-old",
            &directory.path().join("old"),
            &old_core,
            Some(isolated),
            0,
        )
        .await?;
        services.fleet.as_mut().unwrap().fleet_durability = false;
        let old = Node::start(
            config,
            services,
            graph(1, false),
            applications_with_controls(controls.clone())?,
        )
        .await?;
        super::super::ready(&old).await?;
        let client = client()?;
        ensure!(call(&client, &old, reqwest::Method::POST, "/write").await? == "old:1");
        let scope = event_owner_outage::source_scope();
        let original = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let page = pending(&client, &old, &scope).await?;
                if page["pending_rows"] == "0" {
                    break Ok::<_, anyhow::Error>(page);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        let original_history = history(&client, &old).await?;
        ensure!(original_history
            .as_array()
            .is_some_and(|records| records.len() == 1));
        ensure!(
            original_history[0]["event"]["payload"] == json!({"value":1})
                && original_history[0]["offset"] == "1"
        );
        ensure!(
            original["lanes"][0]["revision"]["next_sequence"] == "2"
                && original["lanes"][0]["revision"]["settled_through"] == "1"
        );
        let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
        let old_owner = owner(&observer, &bucket, &scope).await?;
        let old_epoch = old_owner["epoch"]
            .as_u64()
            .context("original owner epoch missing")?;
        let request = client
            .post(format!("http://{}/running", old.info().public_address))
            .send();
        let request = tokio::spawn(request);
        tokio::time::timeout(Duration::from_secs(5), controls.entered.acquire())
            .await??
            .forget();
        fault.isolate();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !old.is_closed() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("old domain did not close while callback remained executing")?;
        ensure!(fault.refused() > 0 && attempted.try_recv().is_err());
        let (config, mut services) = inputs(
            "running-new",
            &directory.path().join("new"),
            &new_core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        services.fleet.as_mut().unwrap().fleet_durability = false;
        let mut declaration = graph(1, false);
        declaration.primary.event_streams.clear();
        let successor = Node::start(config, services, declaration, applications("new")?).await?;
        super::super::ready(&successor).await?;
        // Activate only by reading the recovered source; never replay /write.
        ensure!(call(&client, &successor, reqwest::Method::GET, "/read").await? == "new:1");
        let new_owner = owner(&observer, &bucket, &scope)
            .await?
            .as_object()
            .cloned()
            .context("invalid owner")?;
        ensure!(
            new_owner["node"] == "running-new"
                && new_owner["epoch"]
                    .as_u64()
                    .context("successor owner epoch missing")?
                    > old_epoch
        );
        assert_source(&pending(&client, &successor, &scope).await?, &original)?;
        ensure!(history(&client, &successor).await? == original_history);
        ensure!(
            attempted.try_recv().is_err(),
            "old callback ended before successor audit"
        );
        controls.release();
        let refusal = tokio::time::timeout(Duration::from_secs(5), attempted.recv())
            .await?
            .context("old callback never attempted storage")?
            .expect_err("stale storage operation committed");
        ensure!(
            refusal.contains("execution domain is closed"),
            "unexpected stale refusal: {refusal}"
        );
        let result = tokio::time::timeout(Duration::from_secs(5), request).await??;
        ensure!(
            result.is_err() || !result.as_ref().unwrap().status().is_success(),
            "stale callback returned success"
        );
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            old.run_until(std::future::pending()),
        )
        .await??;
        ensure!(
            outcome
                == NodeOutcome::Fenced {
                    code: 3,
                    reason: FenceReason::NodeLeaseExpired
                }
        );
        ensure!(call(&client, &successor, reqwest::Method::GET, "/read").await? == "new:1");
        assert_source(&pending(&client, &successor, &scope).await?, &original)?;
        ensure!(history(&client, &successor).await? == original_history);
        ensure!(
            call(
                &client,
                &successor,
                reqwest::Method::POST,
                "/process-ack?source=projection"
            )
            .await?
                == "1"
        );
        stopped(successor).await?;
        ensure!(old_core.handle().spawn(async { 7 }).await? == 7);
        observer.shutdown().await;
        drop(release_on_drop);
    }
    Ok(())
}
