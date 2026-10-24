// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! T14: cancellation and deadlines while real durability proof is withheld.
use super::*;

#[derive(Clone, Copy, Debug)]
struct Case {
    caller: &'static str,
    provider: &'static str,
    class: &'static str,
    local: bool,
    sync: bool,
    deadline: bool,
}

impl Case {
    fn phase(self) -> &'static str {
        if self.sync {
            "sync"
        } else {
            "output"
        }
    }

    fn topology(self) -> &'static str {
        if self.local {
            "local"
        } else {
            "remote"
        }
    }

    fn trigger(self) -> &'static str {
        if self.deadline {
            "deadline"
        } else {
            "signal"
        }
    }

    fn result(self) -> Value {
        if self.caller == "python" && !self.deadline {
            json!({"cancelled":true})
        } else {
            let code = if self.deadline {
                "DeadlineExceeded"
            } else {
                "Cancelled"
            };
            json!({"code":code,"outcome":"unknown"})
        }
    }
}

async fn caller_settled(
    client: &reqwest::Client,
    base: &str,
    case: Case,
    id: &str,
) -> Result<Value> {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            last = request(
                client,
                base,
                &format!("/{}/cancel-state?name={id}", case.caller),
            )
            .await?;
            if last["settled"] == true {
                return Ok::<_, anyhow::Error>(last.clone());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("caller did not settle while proof was held; last={last}"))?
}

async fn case(app: Arc<Composite>, client: &reqwest::Client, case: Case) -> Result<Value> {
    app.audit.reset_after_node_drain();
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let controls = Controls::default();
    let (owner, owner_url) = fleet::start_controlled(
        &directory.path().join("owner"),
        app.clone(),
        "proof-owner",
        bucket.clone(),
        Some(controls.clone()),
    )
    .await?;
    ensure!(
        request(
            client,
            &owner_url,
            &format!("/rust/read?target={}", case.provider)
        )
        .await?
            == json!({"value":"0"}),
        "proof wait owner not empty"
    );
    let (ingress, ingress_url) = fleet::start_controlled(
        &directory.path().join("ingress"),
        app.clone(),
        "proof-ingress",
        bucket.clone(),
        Some(controls.clone()),
    )
    .await?;
    let mut baseline = 0;
    warm(
        client,
        &owner_url,
        case.provider,
        &controls,
        &mut baseline,
        1,
    )
    .await?;
    let initial_members = controls.peers.members.lock().unwrap().clone();
    ensure!(
        initial_members == BTreeSet::from(["proof-ingress".into()]),
        "proof wait warmup did not establish its first replica"
    );
    let (replica, _) = fleet::start_controlled(
        &directory.path().join("replica"),
        app.clone(),
        "proof-replica",
        bucket,
        Some(controls.clone()),
    )
    .await?;
    warm(
        client,
        &owner_url,
        case.provider,
        &controls,
        &mut baseline,
        2,
    )
    .await?;
    let members = controls.peers.members.lock().unwrap().clone();
    ensure!(
        members == BTreeSet::from(["proof-ingress".into(), "proof-replica".into()]),
        "proof wait has no real peer ensemble"
    );
    let (base, source) = if case.local {
        (&owner_url, "proof-owner")
    } else {
        (&ingress_url, "proof-ingress")
    };
    // Use a different, already warm cell on the caller node to audit admission.
    // Its read must not depend on the target's deliberately withheld proof.
    ensure!(
        request(client, base, "/rust/proof-followup").await? == json!({"value":"0"}),
        "cleanup probe warmup failed"
    );
    let delta = 9_007_199_254_740_993_i64;
    let expected = baseline
        .checked_add(delta)
        .context("proof wait count overflow")?;
    let id = format!(
        "{}{}-{}-{}-{}",
        if case.sync {
            "proof-cancel-sync-"
        } else {
            "proof-no-sync-cancel-"
        },
        case.topology(),
        case.trigger(),
        case.caller,
        case.provider
    );
    let returned_cursor = app.audit.returned_cursor();
    let invoked_cursor = app.audit.cursor();
    let peer_before = controls.peers.entered.load(Ordering::Acquire);
    let data_before = controls.data.entered.load(Ordering::Acquire);
    let _release = Release(controls.peers.clone(), controls.data.clone());
    controls.peers.hold();
    controls.data.hold_and_drain().await;
    let (pending_client, pending_url, pending_id) = (client.clone(), base.clone(), id.clone());
    let mut pending = Pending(tokio::spawn(async move {
        request(
            &pending_client,
            &pending_url,
            &format!(
                "/{}/proof-cancel?target={}&delta={delta}&id={pending_id}&trigger={}",
                case.caller,
                case.provider,
                case.trigger()
            ),
        )
        .await
    }));
    let watch = live::Watch {
        app: &app,
        client,
        url: base,
        provider: case.provider,
        class: case.class,
        id: &id,
        expected,
        delta,
        invoked_cursor,
        returned_cursor,
    };
    let (observed, live_before) = if case.sync {
        watch.waiting(&controls, peer_before, data_before).await?
    } else {
        (
            returned(&app, returned_cursor, &controls, peer_before, data_before).await?,
            Value::Null,
        )
    };
    let peer_held = controls.peers.entered.load(Ordering::Acquire) - peer_before;
    let data_held = controls.data.entered.load(Ordering::Acquire) - data_before;
    ensure!(
        observed.visit.node == "proof-owner"
            && observed.visit.class.as_deref() == Some(case.class)
            && observed.count_matching_storage.as_deref() == Some(expected.to_string().as_str()),
        "proof wait did not observe the actual local commit"
    );
    ensure!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending.0)
            .await
            .is_err(),
        "caller returned before cancellation/deadline while proof was held"
    );
    if case.caller != "rust" {
        ensure!(
            request(
                client,
                base,
                &format!("/{}/cancel-attack?name={id}", case.caller)
            )
            .await?["scope_refused"]
                == true,
            "foreign invocation acquired caller SDK authority"
        );
    }
    if !case.deadline {
        ensure!(
            request(
                client,
                base,
                &format!("/{}/cancel-release?name={id}", case.caller)
            )
            .await?["released"]
                == true,
            "signal was not delivered"
        );
    }
    let settled = caller_settled(client, base, case, &id).await?;
    ensure!(
        settled["result"] == case.result() && settled["followup_writes"] == 0,
        "proof wait cancellation/deadline did not settle and release admission: {settled}"
    );
    ensure!(
        controls.peers.is_held() && controls.data.is_held(),
        "proof was released before the cancellation cleanup audit"
    );
    let live_after = if case.sync {
        watch.resume().await?
    } else {
        Value::Null
    };
    ensure!(
        controls.peers.is_held() && controls.data.is_held(),
        "late authority audit released proof"
    );
    ensure!(
        app.audit
            .since(invoked_cursor)
            .iter()
            .filter(|visit| visit.method.ends_with("/Increment"))
            .count()
            == 1,
        "proof wait mutation was automatically retried"
    );
    ensure!(
        !pending.0.is_finished(),
        "parent did not remain live through cleanup and late SDK checks"
    );
    ensure!(
        request(
            client,
            base,
            &format!("/{}/cancel-release?name={id}&phase=finish", case.caller)
        )
        .await?["released"]
            == true,
        "parent completion control failed"
    );
    let result = tokio::time::timeout(Duration::from_secs(3), &mut pending.0)
        .await
        .context("caller did not join after completion control")???;
    ensure!(
        result["node"] == source
            && result["result"] == case.result()
            && result["followup_writes"] == 0
            && result["final_followup_writes"] == 0,
        "caller origin/outcome changed after cleanup: {result}"
    );
    controls.peers.open();
    controls.data.open();
    let count = read(client, base, case.provider).await?;
    let expected_text = expected.to_string();
    ensure!(
        count["value"].as_str() == Some(expected_text.as_str()),
        "cancellation lost a locally committed write: {count}"
    );
    for _ in 0..2 {
        ensure!(
            request(
                client,
                base,
                &format!(
                    "/{}/increment?target={}&delta={delta}&id={id}",
                    case.caller, case.provider
                )
            )
            .await?
                == count,
            "explicit proof wait receipt retry duplicated a mutation"
        );
    }
    ensure!(
        read(client, base, case.provider).await? == count,
        "receipt retries changed state"
    );
    let visits = app.audit.since(invoked_cursor);
    let writes: Vec<_> = visits
        .iter()
        .filter(|visit| visit.method.ends_with("/Increment"))
        .collect();
    ensure!(
        writes.len() == 3
            && writes.iter().all(|visit| visit.node == "proof-owner"
                && visit.class == observed.visit.class
                && visit.scope == observed.visit.scope
                && visit.epoch == observed.visit.epoch),
        "proof wait changed owner/scope/epoch or replayed a mutation"
    );
    let ingress_visits: Vec<_> = visits
        .iter()
        .filter(|visit| visit.method == format!("ingress:{}", case.caller))
        .collect();
    ensure!(
        !ingress_visits.is_empty() && ingress_visits.iter().all(|visit| visit.node == source),
        "caller crossed execution domains"
    );
    fleet::stop(owner, ShutdownMode::Preserve).await?;
    fleet::stop(ingress, ShutdownMode::Preserve).await?;
    fleet::stop(replica, ShutdownMode::Preserve).await?;
    Ok(
        json!({"caller":case.caller,"provider":case.provider,"class":case.class,"phase":case.phase(),
        "trigger":case.trigger(),"topology":case.topology(),"ingress":source,"owner":observed.visit.node,
        "scope":observed.visit.scope,"epoch":observed.visit.epoch,"result":result["result"],"settled_while_held":settled,
        "final_followup_writes":result["final_followup_writes"],
        "initial_replica_members":initial_members,"replica_members":members,
        "peer_proof_held":peer_held,"bucket_proof_held":data_held,
        "baseline_count":baseline.to_string(),"local_count":observed.count_matching_storage,"expected_count":expected_text,"final_count":count["value"],
        "transaction_open":observed.transaction_open,"write_position":observed.write_position,
        "handler_returned":!case.sync,"live_before":live_before,"live_after":live_after,
        "parent_live_during_cleanup":true,"proof_held_through_cleanup":true,"receipt_replayed":true,
        "automatic_replays":0,"owner_scope_epoch_verified":true,"remote_fleet_qualified":false}),
    )
}

pub async fn run(app: Arc<Composite>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut cases = Vec::new();
    for deadline in [false, true] {
        for local in [true, false] {
            for sync in [false, true] {
                for (caller, _, _) in fleet::CLASSES {
                    for (provider, _, class) in fleet::CLASSES {
                        let spec = Case {
                            caller,
                            provider,
                            class,
                            local,
                            sync,
                            deadline,
                        };
                        eprintln!("proof wait qualification: {spec:?}");
                        cases.push(
                            case(app.clone(), &client, spec)
                                .await
                                .with_context(|| format!("proof wait {spec:?}"))?,
                        );
                    }
                }
            }
        }
    }
    super::wait_evidence::validate(&cases)?;
    Ok(
        json!({"format":1,"suite":"proof-wait","case_count":cases.len(),"cases":cases,
        "pair_count":16,"trigger_count":2,"phase_count":2,"topology_count":2,
        "js_max_pending_operations":1,"runtimes_drained_before_publication":true,"remote_fleet_qualified":false}),
    )
}

/// Fast feedback for authority assertions. This deliberately cannot publish a
/// complete matrix or claim any full qualification flag.
pub async fn smoke(app: Arc<Composite>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut cases = Vec::new();
    for (caller, local, deadline) in [
        ("rust", true, false),
        ("rust", true, true),
        ("python", true, false),
        ("v8", false, false),
        ("node", false, true),
    ] {
        let spec = Case {
            caller,
            provider: "rust",
            class: "RustCounter",
            local,
            sync: true,
            deadline,
        };
        cases.push(
            case(app.clone(), &client, spec)
                .await
                .with_context(|| format!("proof wait smoke {spec:?}"))?,
        );
    }
    Ok(
        json!({"format":1,"diagnostic":"proof-wait-smoke","qualified":false,"case_count":cases.len(),"cases":cases}),
    )
}
