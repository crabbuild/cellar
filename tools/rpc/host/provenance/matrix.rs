// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::ShutdownMode};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use super::super::{application::Composite, fleet};

const OWNER: &str = "rpc-provenance-owner";
const SOURCE: &str = "rpc-provenance-source";
const DELTA: i64 = 9_007_199_254_740_993;

pub(super) async fn request(
    client: &reqwest::Client,
    base: &str,
    caller: &str,
    action: &str,
    query: &[(&str, &str)],
) -> Result<Value> {
    let mut url = reqwest::Url::parse(&format!("{base}/{caller}/{action}"))?;
    url.query_pairs_mut().extend_pairs(query.iter().copied());
    let response = client.get(url).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status.is_success(),
        "provenance {caller}/{action} failed: {status}: {body}"
    );
    ensure!(
        !body.contains("T06-private") && !body.contains("T06-outcome-accessed"),
        "private exception data escaped the bridge"
    );
    Ok(serde_json::from_str(&body)?)
}

pub async fn run(app: Arc<Composite>) -> Result<Value> {
    app.audit.reset_after_node_drain();
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let (owner, owner_url) = fleet::start(
        &directory.path().join("owner"),
        app.clone(),
        OWNER,
        bucket.clone(),
    )
    .await?;
    let mut source = None;
    let result = async {
        let mut owners = BTreeMap::new();
        for (target, _, class) in fleet::CLASSES {
            let cursor = app.audit.cursor();
            ensure!(request(&client, &owner_url, "rust", "read", &[("target",target)]).await? == json!({"value":"0"}));
            let visit = app.audit.since(cursor).into_iter().find(|visit| visit.class.as_deref() == Some(class)).context("parent warmup was not observed")?;
            ensure!(visit.node == OWNER, "parent was not placed on owner");
            owners.insert(target, visit);
        }
        let factories = app.audit.factories().len();
        ensure!(factories == 4, "unexpected provenance warmup factory count");
        // Pin both raw providers before the source joins. Caller location
        // alone does not establish the durable object's ownership placement.
        let raw_owners = super::raw_matrix::warm(&app, &client, &owner_url).await?;
        let factories = app.audit.factories().len();
        ensure!(factories == 6, "raw provenance warmup did not construct exactly two objects");
        let (node, source_url) = fleet::start(&directory.path().join("source"), app.clone(), SOURCE, bucket).await?;
        source = Some(node);
        let mut counts = BTreeMap::new();
        let mut rows = Vec::new();
        for (route, base, ingress) in [("local", owner_url.as_str(), OWNER), ("peer", source_url.as_str(), SOURCE)] {
            for (caller, _, _) in fleet::CLASSES {
                for (provider, _, _) in fleet::CLASSES {
                    for mode in ["child", "forge", "invalid"] {
                        let cursor = app.audit.cursor();
                        let id = format!("t06:{mode}:{route}:{caller}:{provider}");
                        let code = match mode {"child"=>"DeadlineExceeded","forge"=>"MethodNotFound",_=>"Internal"};
                        let expected_failure = json!({"code":code,"outcome":"unknown"});
                        let delta = DELTA.to_string();
                        let args = [("target",provider),("id",id.as_str()),("delta",delta.as_str())];
                        let failure = request(&client, base, caller, "increment", &args).await?;
                        ensure!(failure == expected_failure, "{route} {caller}->{provider}/{mode}: parent borrowed child provenance or lost its fixed category: {failure}");
                        let current = counts.entry(provider).or_insert(0_i64);
                        *current = current.checked_add(DELTA).context("provenance count overflow")?;
                        let expected_count = current.to_string();
                        let observed = request(&client, base, provider, "provenance-control", &[]).await?;
                        ensure!(observed["ready"] == true && observed["id"] == id && observed["mode"] == mode && observed["parent_active"] == true
                            && observed["count"].as_str() == Some(expected_count.as_str())
                            && observed["receipt"] == json!({"delta":delta,"value":expected_count}), "parent did not retain its actual committed receipt/authority: {observed}");
                        let child = if mode == "child" {json!({"code":"DeadlineExceeded","outcome":"not_committed"})} else {Value::Null};
                        ensure!(observed["child"] == child, "child refusal lacked call-local evidence: {observed}");
                        request(&client, base, provider, "provenance-control", &[("action","clear")]).await?;
                        for _ in 0..2 {
                            let replay = request(&client, base, caller, "increment", &args).await?;
                            ensure!(replay == json!({"value":expected_count}), "parent receipt retry duplicated or lost a committed mutation: {replay}");
                        }
                        ensure!(request(&client, base, caller, "read", &[("target",provider)]).await? == json!({"value":expected_count}), "parent state changed after explicit retries");
                        let visits = app.audit.since(cursor);
                        let expected = &owners[provider];
                        let cells: Vec<_> = visits.iter().filter(|visit| visit.class.is_some()).collect();
                        ensure!(cells.len() == 4 && cells.iter().all(|visit| visit.node == expected.node && visit.class == expected.class && visit.scope == expected.scope && visit.epoch == expected.epoch),
                            "refused child entered a cell or parent moved owner/scope/epoch: {cells:?}");
                        ensure!(cells.iter().filter(|visit| visit.method.ends_with("/Increment")).count() == 3, "an admitted parent was automatically replayed");
                        ensure!(app.audit.factories().len() == factories, "pre-submission child refusal constructed business code");
                        let caller_visits: Vec<_> = visits.iter().filter(|visit| visit.class.is_none() && visit.method == format!("ingress:{caller}")).collect();
                        ensure!(caller_visits.len() >= 4 && caller_visits.iter().all(|visit| visit.node == ingress), "caller ran on an unexpected execution domain or was not observed");
                        rows.push(json!({"route":route,"caller":caller,"provider":provider,"mode":mode,"failure":failure,"child":child,
                            "parent_active_after_refusal":true,"count":expected_count,"receipt_retries":2,"factory_count":factories,
                            "owner":expected.node,"scope":expected.scope,"epoch":expected.epoch,"automatic_retries":0}));
                    }
                }
            }
        }
        ensure!(rows.len() == 96, "provenance matrix incomplete");
        rows.extend(super::raw_matrix::run(&app, &client, &owner_url, &source_url, &raw_owners).await?);
        ensure!(rows.len() == 112, "raw native provenance matrix incomplete");
        Ok(json!({"format":1,"suite":"provenance","case_count":rows.len(),"cases":rows,"remote_fleet_qualified":false}))
    }.await;
    let source_stop = if let Some(node) = source {
        fleet::stop(node, ShutdownMode::Preserve).await
    } else {
        Ok(())
    };
    let owner_stop = fleet::stop(owner, ShutdownMode::Preserve).await;
    let evidence = result?;
    source_stop?;
    owner_stop?;
    Ok(evidence)
}
