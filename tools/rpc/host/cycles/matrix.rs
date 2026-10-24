//! Real same-cell reentry and split-owner cycles, using no mock transport.
use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::ShutdownMode};
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

use super::super::{
    application::{Composite, Visit},
    fleet,
};

const LEFT: &str = "rpc-cycle-left";
const RIGHT: &str = "rpc-cycle-right";

#[derive(Clone, Copy, Debug)]
struct Case {
    a: &'static str,
    b: &'static str,
    shape: &'static str,
    peer: bool,
    deadline: bool,
}

fn binding(language: &str) -> &'static str {
    fleet::CLASSES
        .iter()
        .find(|(name, _, _)| *name == language)
        .unwrap()
        .1
}

fn class(language: &str) -> &'static str {
    fleet::CLASSES
        .iter()
        .find(|(name, _, _)| *name == language)
        .unwrap()
        .2
}

async fn request(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    query: &[(&str, &str)],
) -> Result<Value> {
    let mut url = reqwest::Url::parse(&format!("{base}{path}"))?;
    url.query_pairs_mut().extend_pairs(query.iter().copied());
    let response = client.get(url).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "cycle {path} failed: {status}: {body}");
    Ok(serde_json::from_str(&body)?)
}

async fn read(client: &reqwest::Client, base: &str, target: &str, name: &str) -> Result<Value> {
    request(
        client,
        base,
        "/rust/read",
        &[("target", target), ("cell", name)],
    )
    .await
}

async fn wait_native(app: &Composite, count: usize, retired: bool) -> Result<Value> {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            last = app.cycles.snapshot();
            let entries = last
                .as_object()
                .context("cycle native evidence is not an object")?;
            if entries.len() == count
                && entries
                    .values()
                    .all(|entry| entry["retired"] == retired && entry["revoked"] == retired)
            {
                break Ok::<_, anyhow::Error>(last.clone());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("cycle native frames did not reach retired={retired}: {last}"))?
}

async fn guest_control(
    client: &reqwest::Client,
    base: &str,
    language: &str,
    action: &str,
) -> Result<Value> {
    request(
        client,
        base,
        &format!("/{language}/cycle-control"),
        &[("action", action)],
    )
    .await
}

fn same(visit: &Visit, entry: &Value) -> bool {
    entry["node"] == visit.node
        && entry["class"].as_str() == visit.class.as_deref()
        && entry["scope"].as_str() == visit.scope.as_deref()
        && entry["epoch"].as_u64() == visit.epoch
}

async fn case(app: Arc<Composite>, client: &reqwest::Client, case: Case) -> Result<Value> {
    app.audit.reset_after_node_drain();
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (left, left_url) = fleet::start(
        &directory.path().join("left"),
        app.clone(),
        LEFT,
        bucket.clone(),
    )
    .await?;
    let mut right = None;
    let mut pending = None;
    let id = format!(
        "t15|{}|{}|{}|outer",
        binding(case.a),
        binding(case.b),
        case.shape
    );
    let mut source_url = left_url.clone();
    let result = async {
        ensure!(read(client, &left_url, case.a, "cycle-a").await? == json!({"value":"0"}), "A warmup failed");
        ensure!(read(client, &left_url, "rust", "cycle-gate").await? == json!({"value":"0"}), "gate warmup failed");
        if case.peer {
            let (node, url) = fleet::start(&directory.path().join("right"), app.clone(), RIGHT, bucket).await?;
            right = Some(node);
            source_url = url;
        }
        if case.shape == "aba" {
            ensure!(read(client, &source_url, case.b, "cycle-b").await? == json!({"value":"0"}), "B warmup failed");
        }
        ensure!(read(client, &source_url, "rust", "cancel-followup").await? == json!({"value":"0"}), "source followup warmup failed");
        let warm = app.audit.since(0);
        let followup = warm.iter().rev().find(|visit| visit.class.as_deref() == Some("RustCounter") && visit.method.ends_with("/GetCount"))
            .context("followup cell was not observed on warmup")?;
        app.cycles.begin_case()?;
        let cursor = app.audit.cursor();
        let task_client = client.clone();
        let task_url = source_url.clone();
        let task_id = id.clone();
        pending = Some(tokio::spawn(async move {
            request(&task_client, &task_url, "/rust/proof-cancel", &[("target", case.a), ("cell", "cycle-a"),
                ("id", &task_id), ("delta", "1"), ("trigger", if case.deadline {"deadline"} else {"signal"})]).await
        }));
        let count = if case.shape == "self" {3} else {4};
        let live = wait_native(&app, count, false).await?;
        let cap = if case.deadline {1000} else {5000};
        ensure!(live.as_object().unwrap().values().all(|entry| entry["distinct_authority"] == true
            && entry["initial_remaining_ms"].as_u64().is_some_and(|remaining| remaining > 0 && remaining <= cap)),
            "cycle extended its source budget or reused invocation authority: {live}");
        ensure!(live["outer"]["class"] == class(case.a) && live["inner"]["class"] == class(case.a)
            && live["outer"]["scope"] == live["inner"]["scope"] && live["outer"]["epoch"] == live["inner"]["epoch"]
            && live["outer"]["node"] == LEFT && live["inner"]["node"] == LEFT
            && live["gate"]["node"] == LEFT && live["gate"]["class"] == "RustCounter",
            "cycle did not actually reenter A under the original owner/scope/epoch: {live}");
        if case.shape == "aba" {
            ensure!(live["middle"]["class"] == class(case.b) && live["middle"]["node"] == if case.peer {RIGHT} else {LEFT}
                && live["middle"]["scope"] != live["outer"]["scope"], "cycle B was not on the intended owner: {live}");
        }
        ensure!(read(client, &source_url, case.a, "cycle-a").await? == json!({"value":"2"}), "A did not sync both entered frames");
        if case.shape == "aba" {ensure!(read(client, &source_url, case.b, "cycle-b").await? == json!({"value":"1"}), "B did not sync its entered frame");}
        if !case.deadline {
            request(client, &source_url, "/rust/cancel-release", &[("name", &id)]).await?;
        }
        let settled = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let view = request(client, &source_url, "/rust/cancel-state", &[("name", &id)]).await?;
                if view["settled"] == true {break Ok::<_, anyhow::Error>(view);}
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.context("cycle caller did not join its native operation")??;
        let expected = json!({"code":if case.deadline {"DeadlineExceeded"} else {"Cancelled"}, "outcome":"unknown"});
        ensure!(settled["result"] == expected && settled["followup_writes"] == 0, "cycle lost failure provenance or admission: {settled}");
        let retired = wait_native(&app, count, true).await?;
        let mut guests = serde_json::Map::new();
        for language in ["python", "v8", "node"] {
            let roles: BTreeSet<_> = [("outer", case.a), ("inner", case.a), ("middle", case.b)].into_iter()
                .filter(|(role, provider)| *provider == language && (*role != "middle" || case.shape == "aba"))
                .map(|(role, _)| role).collect();
            guest_control(client, &source_url, language, "release").await?;
            let value = tokio::time::timeout(Duration::from_secs(4), async {
                loop {
                    let value = guest_control(client, &source_url, language, "status").await?;
                    let rows = value.as_array().context("guest cycle evidence is not an array")?;
                    let actual: BTreeSet<_> = rows.iter().filter_map(|row| row["role"].as_str()).collect();
                    ensure!(actual == roles && rows.len() == roles.len(), "guest cycle frame inventory differs: {value}");
                    if rows.iter().all(|row| row["late_done"] == true) {break Ok::<_, anyhow::Error>(value);}
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }).await.with_context(|| format!("{language} cycle continuation did not settle"))??;
            ensure!(value.as_array().unwrap().iter().all(|row| ["storage_refused", "read_refused", "call_refused"].iter().all(|key| row[*key] == true)),
                "{language} cycle retained SDK authority: {value}");
            guests.insert(language.into(), value);
            guest_control(client, &source_url, language, "clear").await?;
        }
        // Same named cells must still admit new reads and a mutation, while
        // the original source invocation remains alive at its finish barrier.
        ensure!(read(client, &source_url, case.a, "cycle-a").await? == json!({"value":"2"}), "A acquired a late write");
        if case.shape == "aba" {ensure!(read(client, &source_url, case.b, "cycle-b").await? == json!({"value":"1"}), "B acquired a late write");}
        let mutation = request(client, &source_url, "/rust/increment", &[("target", case.a), ("cell", "cycle-a"), ("id", "cycle-fresh"), ("delta", "1")]).await?;
        ensure!(mutation == json!({"value":"3"}), "cycle leaked mutation admission or committed late work");
        if case.shape == "aba" {
            let mutation = request(client, &source_url, "/rust/increment", &[("target", case.b), ("cell", "cycle-b"), ("id", "cycle-fresh"), ("delta", "1")]).await?;
            ensure!(mutation == json!({"value":"2"}), "B leaked mutation admission or committed late work");
        }
        let visits = app.audit.since(cursor);
        for visit in visits.iter().filter(|visit| visit.class.is_some()) {
            ensure!(retired.as_object().unwrap().values().any(|entry| same(visit, entry))
                || visit.node == followup.node && visit.class == followup.class && visit.scope == followup.scope && visit.epoch == followup.epoch,
                "cycle moved owner/scope/epoch: {visit:?}");
        }
        let increments = visits.iter().filter(|visit| visit.method.ends_with("/Increment")).count();
        let expected_increments = count + usize::from(case.shape == "aba");
        ensure!(increments == expected_increments, "cycle automatically retried a mutation: {increments}, expected {expected_increments}");
        request(client, &source_url, "/rust/cancel-release", &[("name", &id), ("phase", "finish")]).await?;
        let reply = tokio::time::timeout(Duration::from_secs(4), pending.take().context("cycle source task missing")?).await???;
        ensure!(reply["result"] == expected && reply["followup_writes"] == 0 && reply["final_followup_writes"] == 0,
            "cycle source lost authority during cleanup: {reply}");
        Ok(json!({"a":case.a,"b":case.b,"shape":case.shape,"route":if case.peer {"peer"} else {"local"},
            "trigger":if case.deadline {"deadline"} else {"signal"}, "live":live,"retired":retired,"guests":guests,
            "failure":expected,"fresh_a_count":"3","fresh_b_count":if case.shape == "aba" {json!("2")} else {Value::Null},
            "source_authority_preserved":true,"automatic_mutation_retries":0}))
    }.await;
    // Failed assertions still release retained guest frames and the original
    // caller, then join native nodes. A failed run never publishes a report.
    if result.is_err() {
        let _ = request(
            client,
            &source_url,
            "/rust/cancel-release",
            &[("name", &id)],
        )
        .await;
        for language in ["python", "v8", "node"] {
            let _ = guest_control(client, &source_url, language, "release").await;
        }
        let _ = request(
            client,
            &source_url,
            "/rust/cancel-release",
            &[("name", &id), ("phase", "finish")],
        )
        .await;
    }
    if let Some(task) = pending {
        task.abort();
        let _ = task.await;
    }
    let right_stop = if let Some(node) = right {
        fleet::stop(node, ShutdownMode::Preserve).await
    } else {
        Ok(())
    };
    let left_stop = fleet::stop(left, ShutdownMode::Preserve).await;
    let disarmed = app.cycles.disarm_after_node_drain();
    let value = result?;
    right_stop?;
    left_stop?;
    disarmed?;
    Ok(value)
}

pub async fn run(app: Arc<Composite>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut rows = Vec::new();
    for shape in ["self", "aba"] {
        for (a, _, _) in fleet::CLASSES {
            for (b, _, _) in fleet::CLASSES {
                if shape == "self" && a != b {
                    continue;
                }
                for peer in [false, true] {
                    for deadline in [false, true] {
                        eprintln!(
                            "cycle case {}: {shape} {a}->{b}, peer={peer}, deadline={deadline}",
                            rows.len() + 1
                        );
                        let scenario = Case {
                            a,
                            b,
                            shape,
                            peer,
                            deadline,
                        };
                        rows.push(
                            case(app.clone(), &client, scenario)
                                .await
                                .with_context(|| format!("cycle scenario {scenario:?}"))?,
                        );
                    }
                }
            }
        }
    }
    ensure!(rows.len() == 80, "cycle matrix incomplete");
    Ok(
        json!({"format":1,"suite":"cycles","case_count":rows.len(),"cases":rows,"remote_fleet_qualified":false}),
    )
}
