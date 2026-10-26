// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context, Result};
use cellar::{bucket::Bucket, sdk::ShutdownMode};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use super::relay::{Fault, Relay};
use crate::{
    application::{Composite, Visit},
    fleet,
};

const OWNER: &str = "peer-fault-owner";
const SOURCE: &str = "peer-fault-source";
const INCREMENT: &str = "example.counter.v1.Counter/Increment";

async fn request(client: &reqwest::Client, base: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(
        status.is_success(),
        "generated ingress {path}: {status}: {text}"
    );
    serde_json::from_str(&text).context("generated ingress returned invalid JSON")
}

fn domain(visits: &[Visit], class: &str, identity: &(String, u64)) -> Result<usize> {
    let mut increments = 0;
    for visit in visits {
        if visit.class.is_none() {
            ensure!(
                visit.node == SOURCE && visit.method.starts_with("ingress:"),
                "unexpected ingress domain: {visit:?}"
            );
        } else {
            ensure!(
                visit.node == OWNER
                    && visit.class.as_deref() == Some(class)
                    && visit.scope.as_deref() == Some(identity.0.as_str())
                    && visit.epoch == Some(identity.1),
                "peer fault changed owner, scope or epoch: {visit:?}"
            );
            ensure!(
                visit.method == INCREMENT || visit.method == "example.counter.v1.Counter/GetCount",
                "unregistered method reached business code"
            );
            increments += usize::from(visit.method == INCREMENT);
        }
    }
    Ok(increments)
}

async fn cases(
    app: &Composite,
    client: &reqwest::Client,
    base: &str,
    relay: &Relay,
    owners: &BTreeMap<String, (String, u64)>,
) -> Result<Value> {
    let mut cases = Vec::new();
    let mut counts = BTreeMap::<&str, i64>::new();
    for fault in [
        Fault::Reconnect,
        Fault::ReplyLoss,
        Fault::MethodRefusal,
        Fault::ElementRefusal,
        Fault::ByteRefusal,
    ] {
        for (target, _, class) in fleet::CLASSES {
            let identity = owners.get(class).context("provider identity missing")?;
            for (caller, _, _) in fleet::CLASSES {
                let before = *counts.get(target).unwrap_or(&0);
                let expected = json!({"value":(before + 1).to_string()});
                let id = format!("{}-{caller}-{target}", fault.name());
                let path = format!("/{caller}/increment?target={target}&delta=1&id={id}");
                let cursor = app.audit.cursor();
                let returned = app.audit.returned_cursor();
                let factories = app.audit.factories().len();
                let handshakes = relay.handshakes();
                if fault == Fault::Reconnect {
                    relay.disconnect_idle().await?;
                }
                relay.arm(fault, class, &id, before + 1)?;
                let first = request(client, base, &path)
                    .await
                    .with_context(|| format!("{} {caller}->{target}", fault.name()))?;
                let first_handlers = domain(&app.audit.since(cursor), class, identity)?;
                let proof = relay.observed().with_context(|| {
                    format!(
                        "{} {caller}->{target} returned {first}; visits {:?}",
                        fault.name(),
                        app.audit.since(cursor)
                    )
                })?;
                ensure!(
                    proof["scope"] == identity.0,
                    "relay and native owner scopes differ"
                );
                ensure!(
                    app.audit.factories().len() == factories,
                    "peer refusal or reconnect ran a new factory"
                );
                if let Some(code) = fault.refusal_code() {
                    ensure!(
                        first == json!({"code":code,"outcome":"not_committed"}),
                        "call-specific refusal changed: {first}"
                    );
                    ensure!(
                        first_handlers == 0 && app.audit.returned_since(returned).is_empty(),
                        "refused call entered business code"
                    );
                } else {
                    ensure!(
                        first_handlers == 1,
                        "uncertain submission was replayed or never reached its owner"
                    );
                    let results = app.audit.returned_since(returned);
                    ensure!(
                        results.len() == 1
                            && results[0].count_matching_storage.as_deref()
                                == Some((before + 1).to_string().as_str())
                            && results[0].transaction_open == Some(false)
                            && results[0].write_position.is_some(),
                        "owner did not close its exact mutation transaction"
                    );
                    if fault == Fault::ReplyLoss {
                        ensure!(
                            first == json!({"code":"Unavailable","outcome":"unknown"}),
                            "lost committed reply claimed a known outcome: {first}"
                        );
                    } else {
                        ensure!(
                            first == expected && relay.handshakes() > handshakes,
                            "forced reconnect did not use a fresh signed tunnel: {first}"
                        );
                    }
                }
                let current = if fault.refusal_code().is_some() {
                    before
                } else {
                    before + 1
                };
                ensure!(
                    request(client, base, &format!("/{caller}/read?target={target}")).await?
                        == json!({"value":current.to_string()}),
                    "fault changed committed state"
                );
                ensure!(
                    domain(&app.audit.since(cursor), class, identity)? == first_handlers,
                    "SDK detached an automatic mutation retry"
                );

                // A receipt retry is an explicit application decision. Neither
                // reconnection nor uncertain submission may make this decision.
                for _ in 0..2 {
                    ensure!(
                        request(client, base, &path).await? == expected,
                        "explicit same-ID retry lost its stored result"
                    );
                }
                ensure!(
                    request(
                        client,
                        base,
                        &format!("/{caller}/increment?target={target}&delta=2&id={id}")
                    )
                    .await?
                        == json!({"error":"operation_conflict"}),
                    "same-ID changed-command retry was accepted"
                );
                ensure!(
                    request(client, base, &format!("/{caller}/read?target={target}")).await?
                        == expected,
                    "receipt retry duplicated a mutation"
                );
                let total_handlers = domain(&app.audit.since(cursor), class, identity)?;
                ensure!(
                    total_handlers == first_handlers + 3,
                    "handler count shows an implicit retry or missing explicit call"
                );
                counts.insert(target, before + 1);
                cases.push(json!({"scenario":fault.name(),"caller":caller,"provider":target,
                    "owner":OWNER,"source":SOURCE,"class":class,"scope":identity.0,"epoch":identity.1,
                    "first_reply":first,"first_handler_count":first_handlers,"total_handler_count":total_handlers,
                    "factory_count_change":0,"counter_before":before.to_string(),"counter_after":(before+1).to_string(),
                    "explicit_receipt_retries":2,"changed_command_refused":true,"automatic_replay":false,
                    "peer":proof,"new_signed_handshakes":relay.handshakes()-handshakes}));
            }
        }
    }
    ensure!(cases.len() == 80, "peer fault matrix is incomplete");
    Ok(
        json!({"format":1,"contract_digest":app.contract.digest(),"topology":"signed-peer-fault-relay",
        "case_count":cases.len(),"cases":cases,"fleet_durability":true,
        "remote_fleet_qualified":false,"abrupt_crash_qualified":false}),
    )
}

pub async fn run(app: Arc<Composite>) -> Result<Value> {
    app.audit.reset_after_node_drain();
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let relay = Relay::start(app.contract.clone()).await?;
    let receiver = Arc::new(super::receiver::Receiver::new(app.clone())?);
    let mut owner = None;
    let mut source = None;
    let result = async {
        let (node, owner_url) = fleet::start_configured(
            &directory.path().join("owner"),
            receiver,
            OWNER,
            bucket.clone(),
            None,
            |config, services| {
                relay.destination(services.internal_listener.local_addr()?);
                config.identity.advertise = relay.address.to_string();
                Ok(())
            },
        )
        .await?;
        owner = Some(node);
        let seeded = app.audit.cursor();
        for (target, _, _) in fleet::CLASSES {
            ensure!(
                request(&client, &owner_url, &format!("/rust/read?target={target}")).await?
                    == json!({"value":"0"}),
                "peer fault fleet was not empty"
            );
        }
        let mut owners = BTreeMap::new();
        for visit in app
            .audit
            .since(seeded)
            .into_iter()
            .filter(|visit| visit.class.is_some())
        {
            ensure!(visit.node == OWNER, "seed executed on another node");
            ensure!(
                owners
                    .insert(
                        visit.class.context("seed class missing")?,
                        (
                            visit.scope.context("seed scope missing")?,
                            visit.epoch.context("seed epoch missing")?
                        )
                    )
                    .is_none(),
                "duplicate seed execution"
            );
        }
        ensure!(
            owners.len() == 4,
            "all provider cells must be seeded before source joins"
        );
        let (node, base) = fleet::start(
            &directory.path().join("source"),
            app.clone(),
            SOURCE,
            bucket,
        )
        .await?;
        source = Some(node);
        for (target, _, _) in fleet::CLASSES {
            ensure!(
                request(&client, &base, &format!("/rust/read?target={target}")).await?
                    == json!({"value":"0"}),
                "warm remote read changed state"
            );
        }
        cases(&app, &client, &base, &relay, &owners).await
    }
    .await;
    // Cleanup runs after a failed assertion as well as a successful matrix.
    // Join both native nodes before closing their shared relay and its sockets.
    let mut cleanup = Vec::new();
    for node in [source, owner].into_iter().flatten() {
        if let Err(error) = fleet::stop(node, ShutdownMode::Preserve).await {
            cleanup.push(format!("{error:#}"));
        }
    }
    if let Err(error) = relay.stop().await {
        cleanup.push(format!("{error:#}"));
    }
    if !cleanup.is_empty() {
        let detail = format!("peer fault cleanup failed: {}", cleanup.join("; "));
        return match result {
            Err(error) => Err(error.context(detail)),
            Ok(_) => Err(anyhow::anyhow!(detail)),
        };
    }
    let mut report = result?;
    report["native_nodes_drained"] = json!(true);
    report["relay_joined"] = json!(true);
    report["relay_live_rpc_connections"] = json!(0);
    Ok(report)
}
