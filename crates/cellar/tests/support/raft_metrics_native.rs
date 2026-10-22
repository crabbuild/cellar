//! Actual generation-bound HTTP sampling across real initialized Raft groups.
use super::{faults, public_node, registration, wait_status};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, Node, ShutdownMode};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

async fn fetch(node: &Node, query: &str, key: u8) -> Result<(reqwest::StatusCode, String)> {
    let path = format!("/peer/raft/metrics?{query}");
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?;
    let auth = cellar::peer_auth::PeerAuth::new([key; 32], "metrics-client")?;
    let response = auth
        .sign(
            http.get(format!("http://{}{path}", node.info().internal_address)),
            "GET",
            &path,
            &[],
            "raft-host",
        )?
        .send()
        .await?;
    cellar::peer_auth::validate_response(response.headers())?;
    let status = response.status();
    let body = response.text().await?;
    let limit = if query.contains("view=performance") {
        64
    } else {
        16
    };
    ensure!(body.len() < limit * 1024, "unbounded metrics response");
    Ok((status, body))
}

pub(super) async fn unavailable(node: &Node) -> Result<()> {
    let (status, body) = fetch(
        node,
        &format!("generation={}", node.info().lease_generation),
        11,
    )
    .await?;
    ensure!(status == reqwest::StatusCode::SERVICE_UNAVAILABLE && !body.contains("cellar_raft_"));
    let (status, _) = fetch(
        node,
        &format!(
            "generation={}&view=performance",
            node.info().lease_generation
        ),
        11,
    )
    .await?;
    ensure!(status == reqwest::StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

pub(super) async fn pin_count(node: &Node, expected: usize) -> Result<()> {
    let query = format!("generation={}&format=json", node.info().lease_generation);
    let (status, body) = fetch(node, &query, 11).await?;
    ensure!(status == reqwest::StatusCode::OK);
    let value: serde_json::Value = serde_json::from_str(&body)?;
    ensure!(value["query_pins"]["in_use"].as_u64() == Some(expected as u64));
    if expected > 0 {
        ensure!(value["sql"]["running_workers"]
            .as_u64()
            .is_some_and(|count| count > 0));
        ensure!(value["sql"]["work_reservations"]
            .as_u64()
            .is_some_and(|count| count > 0));
    }
    Ok(())
}

fn samples(text: &str) -> Result<BTreeSet<String>> {
    let mut identities = BTreeSet::<String>::new();
    let mut declared = BTreeSet::new();
    let mut sampled = BTreeSet::<String>::new();
    let mut closed = BTreeSet::new();
    let mut current = None;
    for line in text.lines() {
        if let Some(typed) = line.strip_prefix("# TYPE ") {
            let mut fields = typed.split_whitespace();
            let name = fields.next().context("missing metric type name")?;
            ensure!(matches!(fields.next(), Some("counter" | "gauge")) && fields.next().is_none());
            ensure!(
                declared.insert(name) && !sampled.contains(name),
                "repeated or late metric TYPE: {name}"
            );
        } else if !line.starts_with('#') && !line.is_empty() {
            let name = line.split(['{', ' ']).next().context("metric family")?;
            ensure!(declared.contains(name), "sample without TYPE: {name}");
            if current != Some(name) {
                if let Some(previous) = current {
                    closed.insert(previous);
                }
                ensure!(!closed.contains(name), "metric family is split: {name}");
                current = Some(name);
            }
            sampled.insert(name.into());
        }
    }
    for line in text
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
    {
        let mut fields = line.split_whitespace();
        let identity = fields.next().context("missing sample identity")?;
        fields
            .next()
            .context("missing sample value")?
            .parse::<usize>()?;
        ensure!(
            fields.next().is_none() && identities.insert(identity.into()),
            "duplicate or malformed sample"
        );
    }
    ensure!(identities.len() == 110);
    for identity in &identities {
        ensure!(
            identity.starts_with("cellar_raft_")
                && !identity.contains("Counter:")
                && !identity.contains("group=")
                && !identity.contains("member=")
                && !identity.contains("node=")
                && !identity.contains("cell=")
        );
    }
    Ok(identities)
}

pub async fn group_cardinality() -> Result<()> {
    let small = cluster(1).await?;
    let larger = cluster(32).await?;
    ensure!(
        small == larger,
        "series identities grew with group population"
    );
    Ok(())
}

async fn cluster(groups: usize) -> Result<BTreeSet<String>> {
    let (gate, _) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate)?;
    let registrations = (0..groups)
        .map(|index| {
            let mut registration = registration(definition.schema)?;
            registration.namespace.cluster = ClusterId::new([groups as u8; 16])?;
            registration.birth.scope.cluster = registration.namespace.cluster;
            registration.birth.scope.cell = format!("Counter:metrics-{index}");
            registration.birth.scope.group = GroupId::new([index as u8 + 20; 16])?;
            Ok(registration)
        })
        .collect::<Result<Vec<_>>>()?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let result = async {
        let generation = node.info().lease_generation.clone();
        let query = format!("generation={generation}");
        let (status, body) = fetch(&node, &query, 11).await?;
        ensure!(status == reqwest::StatusCode::NOT_FOUND && !body.contains("cellar_raft_"));
        let defaults = RaftHostOptions::default();
        let host = RaftHostOptions {
            max_groups: 64,
            node_profile: Some(RaftNodeProfile {
                pool: "metrics".into(),
                region: "local".into(),
                zone: "local".into(),
                group_capacity: 64,
                leader_capacity: 64,
                byte_capacity: defaults.images.max_bytes * 64,
                bytes_per_group: defaults.images.max_bytes,
                draining: false,
            }),
            ..defaults
        };
        let service = public_node::cells_many(
            &node,
            definition.application.clone(),
            Arc::new(faults::Store::default()),
            registrations.clone(),
            false,
            host,
            TransportOptions::default(),
        )
        .await?;
        let performance_before = service.performance();
        let mut receipts = Vec::new();
        for registration in &registrations {
            let handle = service.cell(
                registration.namespace.namespace,
                &registration.birth.scope.cell,
            )?;
            public_node::ready(&handle).await?;
            receipts.push(
                handle
                    .command(
                        definition.increment.clone(),
                        1i64.to_be_bytes().to_vec(),
                        public_node::options(1)?,
                    )
                    .await?,
            );
            wait_status(&handle, |status| {
                status.command_ready && status.queries.historical
            })
            .await?;
        }
        ensure!(calls.load(Ordering::SeqCst) == groups);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if service.resource_usage().groups.is_some_and(|sample| {
                    sample.attached_groups == groups
                        && sample.coherent_images == groups
                        && sample.historical_queries == groups
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let (status, text) = fetch(&node, &query, 11).await?;
        ensure!(status == reqwest::StatusCode::OK);
        let mut identities = samples(&text)?;
        let perf_query = format!("{query}&view=performance");
        let (status, text) = fetch(&node, &perf_query, 11).await?;
        ensure!(status == reqwest::StatusCode::OK);
        let observed = service.performance();
        ensure!(observed.available && !observed.saturated && observed.source.len() == 32);
        ensure!(observed.operations.len() == 11 && observed.counters.len() == 11);
        let mut perf_ids = BTreeSet::new();
        for line in text.lines().filter(|line| !line.starts_with('#') && !line.is_empty()) {
            let mut fields = line.split_whitespace();
            let name = fields.next().context("performance metric identity")?;
            let value = fields.next().context("performance metric value")?.parse::<f64>()?;
            ensure!(value.is_finite() && value >= 0.0 && fields.next().is_none());
            ensure!(perf_ids.insert(name.to_owned()));
        }
        ensure!(perf_ids.len() == 266);
        let expected = observed.prometheus().lines().filter(|line| !line.starts_with('#') && !line.is_empty()).map(|line| line.split_whitespace().next().unwrap().to_owned()).collect::<BTreeSet<_>>();
        ensure!(perf_ids == expected && perf_ids.is_disjoint(&identities));
        identities.extend(perf_ids);
        let (status, json) = fetch(&node, &format!("{perf_query}&format=json"), 11).await?;
        ensure!(status == reqwest::StatusCode::OK && !json.contains("Counter:"));
        let json: serde_json::Value = serde_json::from_str(&json)?;
        ensure!(json["source"] == observed.source && json["available"] == true && json["saturated"] == false);
        for name in ["engine_file_write_completed_bytes", "sql_prepared_ltx_bytes", "sql_prepared_proposal_bytes"] {
            let value = json["counters"].as_array().context("counters")?.iter().find(|counter| counter["name"] == name).context("measured counter")?;
            ensure!(value["value"].as_str().context("decimal counter")?.parse::<u64>()? > 0);
        }
        for operation in &observed.operations {
            ensure!(operation.buckets.len() == 16 && operation.buckets.last().unwrap().count == operation.count);
            ensure!(operation.count + operation.timing_unavailable == operation.succeeded + operation.failed + operation.abandoned);
            ensure!(operation.started >= operation.succeeded + operation.failed + operation.abandoned);
        }
        for (invalid, key, expected) in [
            (perf_query.clone(), 12, reqwest::StatusCode::UNAUTHORIZED),
            (format!("generation={}&view=performance", "0".repeat(64)), 11, reqwest::StatusCode::CONFLICT),
            (format!("{perf_query}&view=performance"), 11, reqwest::StatusCode::BAD_REQUEST),
            (format!("{query}&view=other"), 11, reqwest::StatusCode::BAD_REQUEST),
        ] {
            let (status, _) = fetch(&node, &invalid, key).await?;
            ensure!(status == expected);
        }
        let (status, json) = fetch(&node, &format!("{query}&format=json"), 11).await?;
        ensure!(status == reqwest::StatusCode::OK && !json.contains("Counter:"));
        let json: serde_json::Value = serde_json::from_str(&json)?;
        ensure!(json.as_object().context("resource object")?.len() == 9);
        let actual = serde_json::to_value(service.resource_usage())?;
        for budget in ["business", "control", "archive_capture"] {
            for capacity in ["max_bytes", "max_items"] {
                ensure!(json[budget][capacity] == actual[budget][capacity]);
            }
            ensure!(
                json[budget]["bytes"].as_u64().context("bytes")?
                    <= json[budget]["max_bytes"]
                        .as_u64()
                        .context("byte capacity")?
            );
            ensure!(
                json[budget]["items"].as_u64().context("items")?
                    <= json[budget]["max_items"]
                        .as_u64()
                        .context("item capacity")?
            );
        }
        ensure!(json["query_pins"]["in_use"] == 0 && json["query_pins"]["limit"] == 16);
        ensure!(json["sql"]["worker_capacity"] == 2 && json["sql"]["work_capacity"] == 64);
        ensure!(json["transport"]["member_routes"].as_u64() == Some(groups as u64));
        ensure!(json["transport"]["router_routes"] == 0);
        for direction in ["incoming", "outgoing"] {
            for lane in ["control", "data", "query"] {
                for capacity in ["max_bytes", "max_items"] {
                    ensure!(
                        json["transport"][direction][lane][capacity]
                            == actual["transport"][direction][lane][capacity]
                    );
                }
            }
        }
        for (invalid, key, expected) in [
            (
                format!("generation={}", "0".repeat(64)),
                11,
                reqwest::StatusCode::CONFLICT,
            ),
            (query.clone(), 12, reqwest::StatusCode::UNAUTHORIZED),
            (
                format!("{query}&{query}"),
                11,
                reqwest::StatusCode::BAD_REQUEST,
            ),
            (
                format!("{query}&format=other"),
                11,
                reqwest::StatusCode::BAD_REQUEST,
            ),
        ] {
            let (status, body) = fetch(&node, &invalid, key).await?;
            ensure!(status == expected && !body.contains("cellar_raft_"));
        }
        // Detailed exact-history inspection remains a separate opted-in SDK
        // operation; none of its identities becomes a metrics label.
        let index = groups - 1;
        let selected = &registrations[index];
        let handle = service.cell(selected.namespace.namespace, &selected.birth.scope.cell)?;
        let view = handle.inspect().await?;
        ensure!(view.scope == selected.birth.scope && view.node.groups as usize == groups);
        ensure!(view
            .member
            .context("member inspection")?
            .published
            .is_some_and(|position| position.index >= receipts[index].version.position.index));
        ensure!(
            calls.load(Ordering::SeqCst) == groups,
            "sampling invoked business code"
        );
        cellar::note!("raft_performance_observations {}", serde_json::json!({"groups":groups, "before":performance_before, "after":service.performance()}));
        Ok::<_, anyhow::Error>(identities)
    }
    .await;
    let joined = node.shutdown(ShutdownMode::Preserve).await;
    let result = result?;
    let outcome = joined?;
    let cellar::sdk::NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop: {outcome:?}")
    };
    if let Some(raft) = report.raft {
        raft.result?;
    }
    Ok(result)
}
