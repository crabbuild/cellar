// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Guest budgets cross actual codecs, signed peers and nested providers.
use super::*;
use crate::application::Visit;

const GUESTS: [(&str, &str, &str, &str); 3] = [
    (
        "python",
        "guest-python-direct",
        "guest-python-capped",
        "guest-python-child",
    ),
    ("v8", "guest-v8-direct", "guest-v8-capped", "guest-v8-child"),
    (
        "node",
        "guest-node-direct",
        "guest-node-capped",
        "guest-node-child",
    ),
];

async fn request(
    client: &reqwest::Client,
    base: &str,
    language: &str,
    path: &str,
    query: &[(&str, &str)],
) -> Result<Value> {
    let mut url = reqwest::Url::parse(base)?;
    url.set_path(&format!("/{language}/{path}"));
    url.query_pairs_mut().extend_pairs(query.iter().copied());
    let response = client.get(url.clone()).send().await?;
    ensure!(
        response.status().is_success(),
        "guest deadline ingress failed at {url}: {}",
        response.status()
    );
    Ok(response.json().await?)
}

fn same_owner(visit: &Visit, expected: &Visit) -> bool {
    visit.node == OWNER
        && visit.class == expected.class
        && visit.scope == expected.scope
        && visit.epoch == expected.epoch
}

async fn refusal(probe: &Probe) -> Result<application::Refusal> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(value) = probe.refusal.lock().unwrap().clone() {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("guest deadline target did not attempt late SDK work")
}

pub(super) async fn run(inner: Arc<Composite>) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    // Previous suites have drained every native node before this audit starts.
    inner.audit.reset_after_node_drain();
    let mut probes = BTreeMap::new();
    for (_, direct, capped, child) in GUESTS {
        probes.insert(direct, Probe::new(800, None));
        probes.insert(capped, Probe::new(2_200, None));
        probes.insert(child, Probe::new(800, None));
    }
    probes.insert("guest-followup", Probe::new(0, None));
    let app = Arc::new(App { inner, probes });
    let configure = |config: &mut cellar::sdk::NodeConfig, _: &mut cellar::sdk::NodeServices| {
        // Retain this fixed set of fault probes without unrelated transfers.
        config.actor.core.max_resident = 64;
        config.actor.core.max_cached_routes = 64;
        Ok(())
    };
    let (owner, owner_url) = fleet::start_configured(
        &directory.path().join("owner"),
        app.clone(),
        OWNER,
        bucket.clone(),
        None,
        |config, services| {
            configure(config, services)?;
            config.actor.core.operation_deadline_ms = Some(1_500);
            Ok(())
        },
    )
    .await?;
    let mut source = None;
    let result = async {
        // Establish ownership before starting the ingress node. Never infer
        // placement from the HTTP URL: every later entry is audited below.
        for name in app.probes.keys() {
            ensure!(read(&client, &owner_url, name, None).await?["result"] == json!({"writes":0}), "guest deadline warmup failed");
        }
        let mut parents = BTreeMap::new();
        for (guest, _, _, _) in GUESTS {
            let cursor = app.inner.audit.cursor();
            let reply = request(&client, &owner_url, guest, "deadline", &[("target", guest), ("name", "deadline-nested")]).await?;
            ensure!(reply["result"] == json!({"writes":0}), "guest parent warmup failed");
            let class = fleet::CLASSES.iter().find(|(language, _, _)| *language == guest).unwrap().2;
            let visit = app.inner.audit.since(cursor).into_iter().find(|visit| visit.class.as_deref() == Some(class)).context("guest parent never entered")?;
            ensure!(visit.node == OWNER, "guest parent not owned by receiver");
            parents.insert(guest, visit);
        }
        let mut codec_owners = BTreeMap::new();
        for (target, _, class) in fleet::CLASSES {
            for phase in ["verify", "encode", "decode", "control"] {
                let name = format!("codec-budget-{phase}");
                let cursor = app.inner.audit.cursor();
                let reply = request(&client, &owner_url, "python", "deadline", &[("target", target), ("name", &name)]).await?;
                ensure!(reply["result"] == json!({"writes":0}), "remote codec warmup failed");
                let visit = app.inner.audit.since(cursor).into_iter().find(|visit| visit.class.as_deref() == Some(class)).context("codec target never entered")?;
                ensure!(visit.node == OWNER, "codec target not owned by receiver");
                codec_owners.insert((target, phase), visit);
            }
        }
        // Distinct domain ages expose accidental copying of absolute times.
        tokio::time::sleep(Duration::from_millis(800)).await;
        let (node, source_url) = fleet::start_configured(&directory.path().join("source"), app.clone(), SOURCE, bucket, None, configure).await?;
        source = Some(node);
        let mut cases = Vec::new();
        for (guest, direct, capped, child) in GUESTS {
            for (mode, base, ingress, name, target, observed, timeout, cap) in [
                ("local", owner_url.as_str(), OWNER, direct, "rust", direct, "400", 400),
                ("remote", source_url.as_str(), SOURCE, direct, "rust", direct, "400", 400),
                ("receiver-cap", source_url.as_str(), SOURCE, capped, "rust", capped, "5000", 1500),
                ("nested", source_url.as_str(), SOURCE, "deadline-nested", guest, child, "400", 400),
            ] {
                eprintln!("guest deadline: {guest}/{mode}");
                let probe = &app.probes[observed];
                probe.refusal.lock().unwrap().take();
                let before = probe.entries.lock().unwrap().len();
                probe.armed.store(true, Ordering::Release);
                if mode == "nested" {
                    request(&client, base, guest, "deadline-control", &[("action", "arm"), ("child", child)]).await?;
                }
                let cursor = app.inner.audit.cursor();
                let reply = request(&client, base, guest, "deadline", &[("target", target), ("name", name), ("timeout", timeout)]).await?;
                ensure!(reply["node"] == ingress && reply["result"] == json!({"code":"DeadlineExceeded","outcome":"unknown"}) && reply["followup_writes"] == 0,
                    "{guest}/{mode} lost deadline/outcome/cleanup: {reply}");
                let refused = refusal(probe).await.with_context(|| format!(
                    "{guest}/{mode}: entries before={before}, after={:?}, transaction_entered={}",
                    probe.entries.lock().unwrap(), probe.checkpoint.load(Ordering::Acquire)))?;
                ensure!(refused.child_binding && refused.new_storage && refused.cached_write && refused.commit, "{guest}/{mode} retained late native SDK authority: {refused:?}");
                let entry = probe.entries.lock().unwrap().get(before).cloned().context("expired child never entered")?;
                ensure!(entry.remaining_ms > 0 && entry.remaining_ms <= cap, "{guest}/{mode} extended the native receiver cap: {entry:?}");
                let mut parent = Value::Null;
                if mode == "nested" {
                    parent = tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let value = request(&client, base, guest, "deadline-control", &[("action", "status")]).await?;
                            if value["late_done"] == true { break Ok::<_, anyhow::Error>(value); }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }).await.context("expired guest parent did not settle")??;
                    let parent_cap = parent["remaining_ms"].as_u64().context("guest parent cap missing")?;
                    ensure!(parent_cap <= 400 && parent_cap >= entry.remaining_ms + 80 && parent["verify_entered"] == true,
                        "{guest} child budget reset or did not charge codecs: {parent}, child={entry:?}");
                    ensure!(["storage_refused", "cached_call_refused", "new_call_refused"].iter().all(|key| parent[*key] == true), "{guest} expired parent retained SDK authority: {parent}");
                    request(&client, base, guest, "deadline-control", &[("action", "disarm")]).await?;
                    let fresh = request(&client, base, guest, "deadline", &[("target", target), ("name", name)]).await?;
                    ensure!(fresh["result"] == json!({"writes":0}), "expired parent wrote or retained admission");
                    let visits: Vec<_> = app.inner.audit.since(cursor).into_iter().filter(|visit| visit.class.is_some()).collect();
                    ensure!(visits.len() == 2 && visits.iter().all(|visit| same_owner(visit, &parents[guest])), "{guest} parent moved owner/scope/epoch: {visits:?}");
                }
                probe.armed.store(false, Ordering::Release);
                ensure!(read(&client, &source_url, observed, None).await?["result"] == json!({"writes":0}), "late native write persisted");
                let entries = probe.entries.lock().unwrap();
                let first = &entries[0];
                ensure!(entries.iter().all(|entry| entry.node == OWNER && entry.scope == first.scope && entry.epoch == first.epoch), "guest deadline changed child owner/scope/epoch");
                cases.push(json!({"caller":guest,"mode":mode,"ingress":ingress,"owner":OWNER,"native_remaining_ms":entry.remaining_ms,
                    "parent":parent,"late_writes":0,"followup_admitted":true,"late_native_sdk_refused":true,"owner_scope_epoch_verified":true}));
            }
        }
        let mut codec_cases = Vec::new();
        for (index, (guest, _, _, _)) in GUESTS.into_iter().enumerate() {
            for (target, _, _) in fleet::CLASSES {
                for phase in ["verify", "encode", "decode", "control"] {
                    if phase == "control" && guest != "node" { continue; }
                    let cursor = app.inner.audit.cursor();
                    let id = format!("remote-codec-{guest}");
                    let reply = request(&client, &source_url, guest, "codec-budget", &[("target", target), ("phase", phase), ("id", &id), ("remote", "true")]).await?;
                    let count = if phase == "decode" { index + 1 } else { 0 };
                    let expected_count = count.to_string();
                    let expected_outcome = if phase == "decode" { "unknown" } else { "not_committed" };
                    ensure!(reply["code"] == "DeadlineExceeded" && reply["outcome"] == expected_outcome
                        && reply["codec_entered"] == phase && reply["count"].as_str() == Some(expected_count.as_str()), "remote {guest}->{target}/{phase} invalid deadline or state: {reply}");
                    if phase == "decode" { ensure!(reply["replay"].as_str() == Some(expected_count.as_str()), "remote decode expiry lost its receipt"); }
                    let visits = app.inner.audit.since(cursor);
                    let ingress: Vec<_> = visits.iter().filter(|visit| visit.class.is_none()).collect();
                    let ingress_method = format!("ingress:{guest}");
                    ensure!(ingress.len() == 1 && ingress[0].node == SOURCE && ingress[0].method == ingress_method,
                        "remote codec ingress crossed execution domains: {ingress:?}");
                    let cells: Vec<_> = visits.iter().filter(|visit| visit.class.is_some()).collect();
                    let expected = &codec_owners[&(target, phase)];
                    ensure!(!cells.is_empty() && cells.iter().all(|visit| same_owner(visit, expected)), "remote codec target moved owner/scope/epoch: {cells:?}");
                    let writes = cells.iter().filter(|visit| visit.method.ends_with("/Increment")).count();
                    let expected_writes = if phase == "decode" { 2 } else { 0 };
                    ensure!(writes == expected_writes, "expired codec admitted an unexpected mutation");
                    codec_cases.push(json!({"caller":guest,"provider":target,"phase":phase,"outcome":reply["outcome"],
                        "ingress":SOURCE,"owner":OWNER,"count":reply["count"],"receipt_replayed":phase == "decode","owner_scope_epoch_verified":true}));
                }
            }
        }
        Ok::<_, anyhow::Error>(json!({"case_count":cases.len(),"cases":cases,"codec_case_count":codec_cases.len(),"codec_cases":codec_cases,
            "separate_domain_clock_origins":true,"remote_fleet_qualified":false}))
    }.await;
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
