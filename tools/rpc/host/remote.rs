//! Authenticate real peer RPCs and audit the execution node, scopes and epochs.
use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::ShutdownMode};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use super::{
    application::{Composite, Visit},
    fleet,
};

type Owners = BTreeMap<String, (String, u64)>;

fn execution(visits: &[Visit], source: &str, owner: &str) -> Result<(BTreeSet<String>, Owners)> {
    ensure!(source != owner, "remote audit cannot use a local caller");
    let mut caller = None;
    let mut pairs = BTreeSet::new();
    let mut owners = BTreeMap::new();
    for visit in visits {
        if let Some(language) = visit.method.strip_prefix("ingress:") {
            ensure!(
                visit.node == source && visit.class.is_none(),
                "request ingress moved domains: {visit:?}"
            );
            caller = Some(language);
            continue;
        }
        let (language, _, class) = fleet::CLASSES
            .iter()
            .find(|(_, _, class)| Some(*class) == visit.class.as_deref())
            .context("unknown observed RPC class")?;
        ensure!(
            visit.node == owner,
            "RPC did not execute on the remote owner: {visit:?}"
        );
        ensure!(
            matches!(
                visit.method.as_str(),
                "example.counter.v1.Counter/Increment" | "example.counter.v1.Counter/GetCount"
            ),
            "unregistered method entered a cell"
        );
        let scope = visit.scope.clone().context("cell scope is missing")?;
        let epoch = visit.epoch.context("cell epoch is missing")?;
        if let Some(previous) = owners.insert((*class).into(), (scope.clone(), epoch)) {
            ensure!(
                previous == (scope, epoch),
                "owner changed inside a stable matrix phase"
            );
        }
        if visit.method.ends_with("/Increment") {
            let caller = caller.context("RPC executed without its initiating ingress")?;
            ensure!(
                fleet::CLASSES
                    .iter()
                    .any(|(language, _, _)| *language == caller),
                "unknown caller language"
            );
            pairs.insert(format!("{caller}->{language}"));
        }
    }
    ensure!(
        pairs.len() == 16 && owners.len() == 4,
        "remote execution audit is incomplete"
    );
    Ok((pairs, owners))
}

async fn read(client: &reqwest::Client, base: &str, target: &str) -> Result<Value> {
    let response = client
        .get(format!("{base}/rust/read?target={target}"))
        .send()
        .await?;
    ensure!(
        response.status().is_success(),
        "remote read failed: {}",
        response.status()
    );
    Ok(response.json().await?)
}

async fn increment(
    client: &reqwest::Client,
    base: &str,
    caller: &str,
    target: &str,
    id: &str,
    delta: i64,
) -> Result<Value> {
    let response = client
        .get(format!(
            "{base}/{caller}/increment?target={target}&delta={delta}&id={id}"
        ))
        .send()
        .await?;
    ensure!(
        response.status().is_success(),
        "concurrent increment failed: {}",
        response.status()
    );
    Ok(response.json().await?)
}

async fn concurrency(
    app: &Composite,
    client: &reqwest::Client,
    base: &str,
    owners: &Owners,
) -> Result<Value> {
    let large = 9_007_199_254_740_993_i64;
    for (target, _, class) in fleet::CLASSES {
        let before = large * 4 + 2 + i64::from(target != "rust");
        let cursor = app.audit.cursor();
        let mut pending = tokio::task::JoinSet::new();
        for index in 0..8 {
            let client = client.clone();
            let base = base.to_string();
            pending.spawn(async move {
                let caller = fleet::CLASSES[index % 4].0;
                let id = format!("distinct-{target}-{index}");
                let result =
                    increment(&client, &base, caller, target, &id, index as i64 + 1).await?;
                Ok::<_, anyhow::Error>((caller, id, index as i64 + 1, result))
            });
        }
        let mut saved = Vec::new();
        while let Some(result) = pending.join_next().await {
            let (caller, id, delta, reply) = result??;
            let value: i64 = reply["value"]
                .as_str()
                .context("distinct concurrent call failed")?
                .parse()?;
            ensure!(
                value > before && value <= before + 36,
                "concurrent reply crossed counter bounds"
            );
            saved.push((caller, id, delta, reply));
        }
        ensure!(saved.len() == 8, "concurrent distinct command was lost");
        for (caller, id, delta, reply) in saved {
            ensure!(
                increment(client, base, caller, target, &id, delta).await? == reply,
                "concurrent receipt was mixed with another call"
            );
        }
        ensure!(
            read(client, base, target).await? == json!({"value":(before + 36).to_string()}),
            "concurrent distinct total is not exact"
        );
        let mut pending = tokio::task::JoinSet::new();
        for index in 0..8 {
            let client = client.clone();
            let base = base.to_string();
            pending.spawn(async move {
                increment(
                    &client,
                    &base,
                    fleet::CLASSES[index % 4].0,
                    target,
                    &format!("same-{target}"),
                    13,
                )
                .await
            });
        }
        let mut replies = 0;
        while let Some(result) = pending.join_next().await {
            ensure!(
                result?? == json!({"value":(before + 49).to_string()}),
                "same-ID race duplicated a mutation or changed its receipt"
            );
            replies += 1;
        }
        ensure!(replies == 8, "same-ID concurrent command was lost");
        ensure!(
            increment(client, base, "rust", target, &format!("same-{target}"), 14).await?
                == json!({"error":"operation_conflict"}),
            "same-ID race lost its semantic conflict check"
        );
        ensure!(
            read(client, base, target).await? == json!({"value":(before + 49).to_string()}),
            "concurrent duplicate changed total"
        );
        for visit in app.audit.since(cursor) {
            if visit.class.is_none() {
                ensure!(
                    visit.node == "rpc-ingress",
                    "concurrent ingress crossed nodes"
                );
            } else {
                let identity = owners.get(class).context("missing concurrent owner")?;
                ensure!(
                    visit.node == "rpc-next"
                        && visit.class.as_deref() == Some(class)
                        && visit.scope.as_deref() == Some(identity.0.as_str())
                        && visit.epoch == Some(identity.1),
                    "concurrent call crossed owner, scope or epoch: {visit:?}"
                );
            }
        }
    }
    Ok(
        json!({"distinct_commands_per_provider":8,"same_id_racers_per_provider":8,
        "concurrent_receipts_replayed":true,"exact_totals":true,"semantic_conflicts_refused":true}),
    )
}

pub async fn run(app: Arc<Composite>) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let (owner, owner_url) = fleet::start(
        &directory.path().join("owner"),
        app.clone(),
        "rpc-owner",
        bucket.clone(),
    )
    .await?;
    // Place every named entity while this is the only live node. The audit
    // below verifies actual execution; placement is never inferred from a URL.
    let seeded = app.audit.cursor();
    for (target, _, _) in fleet::CLASSES {
        ensure!(
            read(&client, &owner_url, target).await? == json!({"value":"0"}),
            "fleet was not empty"
        );
    }
    let visits = app.audit.since(seeded);
    ensure!(
        visits.iter().filter(|visit| visit.class.is_some()).count() == 4
            && visits.iter().all(|visit| visit.node == "rpc-owner"),
        "seed did not place all four providers on the owner"
    );
    let (next_owner, next_url) = fleet::start(
        &directory.path().join("next"),
        app.clone(),
        "rpc-next",
        bucket.clone(),
    )
    .await?;
    let cursor = app.audit.cursor();
    let first = super::qualify::matrix(&client, &next_url, 0).await?;
    let (observed, previous_owners) = execution(&app.audit.since(cursor), "rpc-next", "rpc-owner")?;
    ensure!(
        first == observed,
        "declared and observed remote pairs differ"
    );

    fleet::stop(owner, ShutdownMode::Handoff).await?;
    // A third startup leaves two live nodes. The new ingress must call the new
    // owner through authenticated peer RPC, including replaying saved receipts.
    let (ingress, ingress_url) = fleet::start(
        &directory.path().join("ingress"),
        app.clone(),
        "rpc-ingress",
        bucket,
    )
    .await?;
    let cursor = app.audit.cursor();
    let second = super::qualify::matrix(&client, &ingress_url, 1).await?;
    let (observed, current_owners) =
        execution(&app.audit.since(cursor), "rpc-ingress", "rpc-next")?;
    ensure!(
        second == observed && first == second,
        "handoff lost a remote language pair"
    );
    for (class, (scope, epoch)) in &current_owners {
        let previous = previous_owners
            .get(class)
            .context("missing previous owner")?;
        ensure!(
            scope == &previous.0 && *epoch > previous.1,
            "handoff changed identity or did not advance epoch for {class}"
        );
    }
    let concurrency = concurrency(&app, &client, &ingress_url, &current_owners).await?;
    fleet::stop(ingress, ShutdownMode::Preserve).await?;
    fleet::stop(next_owner, ShutdownMode::Preserve).await?;
    Ok(json!({
        "format":1, "contract_digest":app.contract.digest(),
        "topology":"authenticated-two-node-fleet", "simultaneous_nodes":2, "native_node_starts":3,
        "pairs":first, "pair_count":first.len(), "remote_matrix_verified":true,
        "remote_matrix_after_handoff_verified":true, "acknowledged_state_and_receipts_recovered":true,
        "scope_identity_preserved":true, "owner_epochs_advanced":true,
        "fleet_durability":true, "guest_runtime_restarts":0,
        "concurrency":concurrency,
        "remote_fleet_qualified":false, "abrupt_crash_qualified":false,
    }))
}

#[cfg(test)]
#[path = "remote_tests.rs"]
mod tests;
