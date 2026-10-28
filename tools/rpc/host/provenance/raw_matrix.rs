// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context as _, Result};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};

use super::{
    super::{
        application::{Composite, Visit},
        fleet,
    },
    matrix::request,
};

const DELTA: &str = "9007199254740993";
const OWNER: &str = "rpc-provenance-owner";
const SOURCE: &str = "rpc-provenance-source";

pub(super) async fn warm(
    app: &Arc<Composite>,
    client: &reqwest::Client,
    owner_url: &str,
) -> Result<BTreeMap<&'static str, Visit>> {
    let mut owners = BTreeMap::new();
    for mode in ["forge", "forward"] {
        let cell = format!("t06-raw-{mode}");
        let cursor = app.audit.cursor();
        ensure!(
            request(
                client,
                owner_url,
                "rust",
                "read",
                &[("target", "rust"), ("cell", cell.as_str())]
            )
            .await?
                == json!({"value":"0"})
        );
        let owner = app
            .audit
            .since(cursor)
            .into_iter()
            .find(|visit| visit.class.as_deref() == Some("RustCounter"))
            .context("raw native warmup was not observed")?;
        ensure!(
            owner.node == OWNER,
            "raw native parent was not placed on owner"
        );
        owners.insert(mode, owner);
    }
    ensure!(
        owners["forge"].scope != owners["forward"].scope,
        "raw provenance modes share an object"
    );
    Ok(owners)
}

pub(super) async fn run(
    app: &Arc<Composite>,
    client: &reqwest::Client,
    owner_url: &str,
    source_url: &str,
    owners: &BTreeMap<&str, Visit>,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut counts = BTreeMap::new();
    for (route, base, ingress) in [("local", owner_url, OWNER), ("peer", source_url, SOURCE)] {
        for (caller, _, _) in fleet::CLASSES {
            for mode in ["forge", "forward"] {
                let cell = format!("t06-raw-{mode}");
                let target = [("target", "rust"), ("cell", cell.as_str())];
                let owner = &owners[mode];
                let factories = app.audit.factories().len();
                let cursor = app.audit.cursor();
                let id = format!("t06-raw:{mode}:{route}:{caller}");
                let args = [
                    ("target", "rust"),
                    ("cell", cell.as_str()),
                    ("id", id.as_str()),
                    ("delta", DELTA),
                ];
                let failure = request(client, base, caller, "increment", &args).await?;
                ensure!(
                    failure == json!({"code":"MethodNotFound","outcome":"unknown"}),
                    "{route} {caller}->rust/raw-{mode} reported false rollback: {failure}"
                );
                let count = counts.entry(mode).or_insert(0_i64);
                *count = count
                    .checked_add(DELTA.parse()?)
                    .context("raw count overflow")?;
                let expected_count = count.to_string();
                let observed = request(client, base, "rust", "provenance-control", &[]).await?;
                let child = if mode == "forward" {
                    json!({"code":"MethodNotFound","outcome":"not_committed"})
                } else {
                    Value::Null
                };
                ensure!(
                    observed
                        == json!({"ready":true,"id":id,"mode":mode,"parent_active":true,
                    "count":expected_count,"receipt":{"delta":DELTA,"value":expected_count},"child":child}),
                    "raw parent did not commit/retain its count, receipt and authority: {observed}"
                );
                request(
                    client,
                    base,
                    "rust",
                    "provenance-control",
                    &[("action", "clear")],
                )
                .await?;
                for _ in 0..2 {
                    ensure!(
                        request(client, base, caller, "increment", &args).await?
                            == json!({"value":expected_count}),
                        "raw parent receipt retry duplicated or lost a commit"
                    );
                }
                ensure!(
                    request(client, base, caller, "read", &target).await?
                        == json!({"value":expected_count}),
                    "raw parent state changed after explicit retries"
                );
                let visits = app.audit.since(cursor);
                let parents: Vec<_> = visits
                    .iter()
                    .filter(|visit| visit.class.is_some())
                    .collect();
                ensure!(
                    parents.len() == 4
                        && parents.iter().all(|visit| visit.node == owner.node
                            && visit.class == owner.class
                            && visit.scope == owner.scope
                            && visit.epoch == owner.epoch),
                    "raw child entered business code or parent authority moved"
                );
                ensure!(
                    parents
                        .iter()
                        .filter(|visit| visit.method.ends_with("/Increment"))
                        .count()
                        == 3,
                    "raw native mutation was automatically replayed"
                );
                ensure!(
                    app.audit.factories().len() == factories,
                    "raw child refusal constructed business code"
                );
                let callers: Vec<_> = visits
                    .iter()
                    .filter(|visit| {
                        visit.class.is_none() && visit.method == format!("ingress:{caller}")
                    })
                    .collect();
                ensure!(
                    callers.len() >= 4 && callers.iter().all(|visit| visit.node == ingress),
                    "raw caller ran in an unexpected domain"
                );
                rows.push(json!({"route":route,"caller":caller,"provider":"rust","mode":format!("raw-{mode}"),
                    "failure":failure,"child":child,"parent_active_after_refusal":true,"count":expected_count,"receipt_retries":2,
                    "factory_count":factories,"factories_after":app.audit.factories().len(),"owner":owner.node,
                    "scope":owner.scope,"epoch":owner.epoch,"automatic_retries":0}));
            }
        }
    }
    ensure!(rows.len() == 16, "raw native provenance pairs incomplete");
    Ok(rows)
}
