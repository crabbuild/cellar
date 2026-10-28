// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Automatic bounded reclamation under quota pressure alongside other cells.
use super::{call, client, graph, inputs, ready, response, stopped, Executor};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{filesystem::*, *};
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

const CHUNK: usize = 64 * 1024;
const PINNED_CHUNKS: usize = 12;
const ALLOCATION_CHUNKS: usize = 16;
const LOGICAL_CHUNKS: usize = 32;
const HOT_ROUNDS: u64 = 32;
const COLD_CELLS: usize = 4;
const COLD_ROUNDS: u64 = 24;

#[derive(Default)]
struct Signals {
    pinned: AtomicBool,
    cold_under_pressure: AtomicUsize,
    started: Notify,
    release: Notify,
    producing: Notify,
    cold_finished: Notify,
}

struct Workspace(Arc<Signals>);

fn counter(storage: &Storage) -> Result<u64> {
    Ok(storage
        .get_json("round")?
        .and_then(|v| v.as_u64())
        .unwrap_or(0))
}

fn sample(fs: &Filesystem, storage: &Storage) -> Result<Value> {
    let usage = fs.statvfs()?;
    ensure!(usage.allocated_bytes <= (ALLOCATION_CHUNKS * CHUNK) as u64);
    ensure!(usage.logical_bytes <= (LOGICAL_CHUNKS * CHUNK) as u64);
    Ok(
        json!({"round":counter(storage)?,"logical":usage.logical_bytes,
        "allocated":usage.allocated_bytes,"inodes":usage.inodes}),
    )
}

impl Cell for Workspace {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let url = url::Url::parse(&request.url)?;
            let parts: Vec<_> = url.path().split('/').filter(|p| !p.is_empty()).collect();
            ensure!(parts.len() >= 2, "invalid fairness fixture route");
            let storage = context.storage()?;
            let fs = context.fs()?;
            match parts[1] {
                "prepare" => {
                    ensure!(parts[0] == "hot" && counter(&storage)? == 0);
                    fs.write_file("/pinned", &vec![7; PINNED_CHUNKS * CHUNK])?;
                    let pinned = fs.open("/pinned", OpenOptions::read_only())?;
                    fs.unlink("/pinned")?;
                    storage.tx(|tx| {
                        let fs = tx.fs()?;
                        for index in 0..ALLOCATION_CHUNKS - PINNED_CHUNKS {
                            let path = format!("/seed-{index}");
                            fs.write_file(&path, &vec![9; CHUNK])?;
                            fs.unlink(&path)?;
                        }
                        let full = fs.statvfs()?;
                        ensure!(full.allocated_bytes == (ALLOCATION_CHUNKS * CHUNK) as u64);
                        let error = fs.write_file("/refused", &vec![1; CHUNK]).unwrap_err();
                        ensure!(
                            error.errno == Errno::EDQUOT && error.outcome == Outcome::NotApplied
                        );
                        ensure!(fs.stat("/refused").unwrap_err().errno == Errno::ENOENT);
                        ensure!(fs.statvfs()? == full);
                        tx.put_json("quota_refusal_checked", &json!(true))
                    })?;
                    self.0.pinned.store(true, Ordering::SeqCst);
                    let signals = self.0.clone();
                    context.wait_until(async move {
                        signals.started.notify_one();
                        signals.release.notified().await;
                        let mut bytes = [0; 8];
                        ensure!(pinned.pread(&mut bytes, (PINNED_CHUNKS * CHUNK - 8) as u64)? == 8);
                        ensure!(bytes == [7; 8], "reclamation changed the pinned orphan");
                        pinned.close()?;
                        signals.pinned.store(false, Ordering::SeqCst);
                        fs.sync().await?;
                        Ok(())
                    })?;
                    Ok(response(json!({"prepared":true})))
                }
                "round" => {
                    ensure!(parts[0] == "hot", "only the hot cell produces garbage");
                    let round: u64 = parts.get(2).context("missing hot round")?.parse()?;
                    let previous = counter(&storage)?;
                    ensure!(
                        round == previous + 1,
                        "hot mutation was reordered or repeated"
                    );
                    let result: Result<()> = storage.tx(|tx| {
                        tx.put_json("round", &json!(round))?;
                        let fs = tx.fs()?;
                        fs.write_file("/scratch", &vec![round as u8; CHUNK])?;
                        fs.unlink("/scratch")?;
                        Ok(())
                    });
                    let status = match result {
                        Ok(()) => "ok",
                        Err(error) => {
                            let refusal = error
                                .downcast_ref::<Error>()
                                .context("unexpected hot failure")?;
                            ensure!(
                                refusal.errno == Errno::EDQUOT
                                    && refusal.outcome == Outcome::NotApplied
                            );
                            ensure!(
                                counter(&storage)? == previous,
                                "quota failure committed business state"
                            );
                            "quota"
                        }
                    };
                    ensure!(fs.stat("/scratch").unwrap_err().errno == Errno::ENOENT);
                    let mut result = sample(&fs, &storage)?;
                    result["status"] = status.into();
                    Ok(response(result))
                }
                "tick" => {
                    ensure!(parts[0] != "hot", "hot cell is not a cold writer");
                    let round: u64 = parts.get(2).context("missing cold round")?.parse()?;
                    ensure!(round == counter(&storage)? + 1);
                    storage.tx(|tx| {
                        tx.fs()?.write_file("/counter", &round.to_be_bytes())?;
                        tx.put_json("round", &json!(round))
                    })?;
                    ensure!(fs.read_file("/counter")? == round.to_be_bytes());
                    if self.0.pinned.load(Ordering::SeqCst) {
                        self.0.cold_under_pressure.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(response(sample(&fs, &storage)?))
                }
                "state" => {
                    if parts[0] != "hot" {
                        ensure!(fs.read_file("/counter")? == counter(&storage)?.to_be_bytes());
                    } else {
                        ensure!(storage.get_json("quota_refusal_checked")? == Some(json!(true)));
                    }
                    Ok(response(sample(&fs, &storage)?))
                }
                _ => anyhow::bail!("unknown fairness fixture operation"),
            }
        })
    }
}

fn applications(signals: Arc<Signals>) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let factory = signals.clone();
        Ok(Arc::new(
            Registry::new(|context, invocation| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = invocation else {
                        return Ok(Output::Done);
                    };
                    let url = url::Url::parse(&request.url)?;
                    let name = url.path().split('/').nth(1).context("missing cell name")?;
                    let cell = context.namespace("ACCOUNTS")?.get_by_name(name);
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(Workspace(factory.clone())))
            })?
            .filesystem("Account", Policy::default())?,
        ))
    })
}

async fn start(
    name: &str,
    path: &Path,
    core: &Executor,
    bucket: cellar::bucket::Bucket,
    signals: Arc<Signals>,
) -> Result<Node> {
    let (mut config, services) = inputs(name, path, core, Some(bucket), 0).await?;
    config.cell_limits.filesystem.gc_batch_chunks = 1;
    config.cell_limits.filesystem.max_allocated_bytes = (ALLOCATION_CHUNKS * CHUNK) as u64;
    config.cell_limits.filesystem.max_logical_bytes = (LOGICAL_CHUNKS * CHUNK) as u64;
    config.cell_limits.filesystem.max_host_handles = 64;
    Node::start(config, services, graph(), applications(signals)?).await
}

async fn query(client: &reqwest::Client, node: &Node, path: &str) -> Result<Value> {
    let text = call(client, node, reqwest::Method::GET, path).await?;
    Ok(serde_json::from_str(&text)?)
}

async fn workload(node: &Node, signals: &Signals) -> Result<()> {
    let client = client()?;
    ensure!(query(&client, node, "/hot/prepare").await?["prepared"] == true);
    tokio::time::timeout(Duration::from_secs(5), signals.started.notified()).await?;
    let producer = async {
        for round in 1..=HOT_ROUNDS {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let value = query(&client, node, &format!("/hot/round/{round}")).await?;
                    if value["status"] == "ok" {
                        ensure!(value["round"] == round);
                        break Ok::<_, anyhow::Error>(());
                    }
                    ensure!(value["status"] == "quota" && value["round"] == round - 1);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .with_context(|| format!("automatic reclamation stalled at hot round {round}"))??;
            if round == 2 {
                signals.producing.notify_one();
            }
            if round == HOT_ROUNDS / 2 {
                // At least twelve newer chunks must already have been
                // reclaimed while the older twelve-chunk orphan stays pinned.
                signals.cold_finished.notified().await;
                ensure!(signals.pinned.load(Ordering::SeqCst));
                signals.release.notify_one();
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    let independent = async {
        signals.producing.notified().await;
        futures_util::future::try_join_all((0..COLD_CELLS).map(|cell| {
            let client = &client;
            async move {
                for round in 1..=COLD_ROUNDS {
                    let state = query(client, node, &format!("/cold-{cell}/tick/{round}")).await?;
                    ensure!(state["round"] == round && state["logical"] == 8);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok::<_, anyhow::Error>(())
            }
        }))
        .await?;
        signals.cold_finished.notify_one();
        Ok::<_, anyhow::Error>(())
    };
    futures_util::future::try_join(producer, independent).await?;
    ensure!(
        signals.cold_under_pressure.load(Ordering::SeqCst) == COLD_CELLS * COLD_ROUNDS as usize
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = query(&client, node, "/hot/state").await?;
            ensure!(state["round"] == HOT_ROUNDS, "hot receipt changed: {state}");
            // Unlinked inodes keep their logical charge until their final
            // bounded collection batch; allocated bytes alone do not prove
            // that all reclamation obligations have completed.
            if state["allocated"] == 0 && state["logical"] == 0 && state["inodes"] == 1 {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("automatic reclamation did not return the hot cell to its empty root")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filesystem_reclamation_under_pressure_preserves_other_cells_and_recovers() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = management
        .run(async { cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite")) })
        .await??;
    let core = Executor::new("filesystem-fairness-core")?;
    let signals = Arc::new(Signals::default());
    let old_path = directory.path().join("old");
    let node = start(
        "fairness-owner",
        &old_path,
        &core,
        bucket.clone(),
        signals.clone(),
    )
    .await?;
    ready(&node).await?;
    let result = tokio::time::timeout(Duration::from_secs(45), workload(&node, &signals)).await;
    signals.release.notify_one();
    let shutdown = stopped(node).await;
    if let Err(error) = result {
        management.shutdown().await;
        shutdown?;
        return Err(error.into());
    }
    let result = result.unwrap();
    if let Err(error) = result {
        management.shutdown().await;
        shutdown?;
        return Err(error);
    }
    shutdown?;
    ensure!(!signals.pinned.load(Ordering::SeqCst));
    std::fs::remove_dir_all(&old_path)?;
    let successor = start(
        "fairness-successor",
        &directory.path().join("fresh"),
        &core,
        bucket,
        signals,
    )
    .await?;
    ready(&successor).await?;
    let result = async {
        let client = client()?;
        let state = query(&client, &successor, "/hot/state").await?;
        ensure!(
            state["round"] == HOT_ROUNDS
                && state["logical"] == 0
                && state["allocated"] == 0
                && state["inodes"] == 1
        );
        for cell in 0..COLD_CELLS {
            let state = query(&client, &successor, &format!("/cold-{cell}/state")).await?;
            ensure!(state["round"] == COLD_ROUNDS && state["logical"] == 8 && state["inodes"] == 2);
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let shutdown = stopped(successor).await;
    management.shutdown().await;
    shutdown?;
    result
}
