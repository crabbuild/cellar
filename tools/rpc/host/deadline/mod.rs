//! Deadline qualification is native-only; it does not claim guest cancellation.
use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::ShutdownMode};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use super::{application::Composite, fleet};
mod application;
pub mod cancellation;
use application::{App, Probe};

const OWNER: &str = "rpc-deadline-owner";
const SOURCE: &str = "rpc-deadline-source";

async fn read(
    client: &reqwest::Client,
    base: &str,
    name: &str,
    timeout: Option<u64>,
) -> Result<Value> {
    let mut url = reqwest::Url::parse(base)?;
    url.query_pairs_mut().append_pair("name", name);
    if let Some(timeout) = timeout {
        url.query_pairs_mut()
            .append_pair("timeout", &timeout.to_string());
    }
    let response = client.get(url).send().await?;
    ensure!(
        response.status().is_success(),
        "deadline ingress failed: {}",
        response.status()
    );
    Ok(response.json().await?)
}

pub async fn run(inner: Arc<Composite>) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let probes = BTreeMap::from([
        ("deadline-short", Probe::new(800, None)),
        ("deadline-nested", Probe::new(0, Some("deadline-child"))),
        ("deadline-child", Probe::new(800, None)),
        ("deadline-capped", Probe::new(2_200, None)),
    ]);
    let app = Arc::new(App { inner, probes });
    let (owner, owner_url) = fleet::start_configured(
        &directory.path().join("owner"),
        app.clone(),
        OWNER,
        bucket.clone(),
        None,
        |config, _| {
            config.actor.core.operation_deadline_ms = Some(1_500);
            Ok(())
        },
    )
    .await?;
    let mut source = None;
    let result = async {
        for name in app.probes.keys() {
            ensure!(
                read(&client, &owner_url, name, None).await?["result"] == json!({"writes":0}),
                "warm deadline state is not empty"
            );
        }
        // Different domain ages make copying an absolute timestamp observably
        // wrong. Both nodes still use their real execution-domain clocks.
        tokio::time::sleep(Duration::from_millis(800)).await;
        let (node, source_url) = fleet::start(
            &directory.path().join("source"),
            app.clone(),
            SOURCE,
            bucket,
        )
        .await?;
        source = Some(node);
        let mut cases = Vec::new();
        for (name, timeout, observed) in [
            ("deadline-short", 400, "deadline-short"),
            ("deadline-nested", 400, "deadline-child"),
            ("deadline-capped", 5_000, "deadline-capped"),
        ] {
            let root = &app.probes[name];
            let target = &app.probes[observed];
            root.armed.store(true, Ordering::Release);
            target.armed.store(true, Ordering::Release);
            let reply = read(&client, &source_url, name, Some(timeout)).await?;
            ensure!(reply["node"] == SOURCE, "deadline caller crossed domains");
            ensure!(
                reply["result"] == json!({"code":"DeadlineExceeded","outcome":"unknown"}),
                "deadline result is not conservative: {reply}"
            );
            let refusal = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(observed) = target.refusal.lock().unwrap().clone() {
                        break observed;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("deadline handler never entered/settled")?;
            ensure!(
                refusal.child_binding
                    && refusal.new_storage
                    && refusal.cached_write
                    && refusal.commit,
                "expired peer target retained SDK authority: {refusal:?}"
            );
            root.armed.store(false, Ordering::Release);
            target.armed.store(false, Ordering::Release);
            for cell in [name, observed] {
                ensure!(
                    read(&client, &source_url, cell, None).await?["result"] == json!({"writes":0}),
                    "late write persisted or transaction cleanup failed"
                );
                let entries = app.probes[cell].entries.lock().unwrap();
                let first = entries.first().context("deadline cell never entered")?;
                ensure!(
                    entries.len() >= 3
                        && entries.iter().all(|entry| entry.node == OWNER
                            && entry.scope == first.scope
                            && entry.epoch == first.epoch),
                    "deadline test did not preserve its remote owner/scope/epoch: {entries:?}"
                );
            }
            cases.push(
                json!({"name":name,"requested_timeout_ms":timeout,"observed_cell":observed,
                "refused":{"child_binding":refusal.child_binding,"new_storage":refusal.new_storage,
                    "cached_write":refusal.cached_write,"commit":refusal.commit},
                "late_writes":0,"remote_owner_and_epoch_verified":true}),
            );
        }
        Ok::<_, anyhow::Error>(json!({"format":1,"suite":"deadline","backend":"rust",
            "case_count":cases.len(),"cases":cases,"receiver_host_cap_ms":1_500,
            "separate_domain_clock_origins":true,"remote_fleet_qualified":false}))
    }
    .await;
    // Always drain both nodes, including after a deliberately failing audit.
    let source_stop = match source {
        Some(node) => fleet::stop(node, ShutdownMode::Preserve).await,
        None => Ok(()),
    };
    let owner_stop = fleet::stop(owner, ShutdownMode::Preserve).await;
    let evidence = result?;
    source_stop?;
    owner_stop?;
    Ok(evidence)
}
