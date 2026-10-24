//! Explicit cancellation must revoke native SDK authority before another poll.
use super::*;

pub async fn run(inner: Arc<Composite>) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let app = Arc::new(App {
        inner,
        probes: BTreeMap::from([
            ("cancel-local", Probe::new(800, None)),
            ("cancel-remote", Probe::new(800, None)),
            ("cancel-drop", Probe::new(800, None)),
            ("cancel-nested", Probe::new(0, Some("cancel-child"))),
            ("cancel-child", Probe::new(800, None)),
            ("cancel-committed", Probe::committed(800)),
            ("cancel-python-local", Probe::new(800, None)),
            ("cancel-python-remote", Probe::new(800, None)),
            (
                "cancel-python-nested",
                Probe::new(0, Some("cancel-python-child")),
            ),
            ("cancel-python-child", Probe::new(800, None)),
            ("cancel-python-committed", Probe::committed(800)),
            ("cancel-v8-local", Probe::new(800, None)),
            ("cancel-v8-remote", Probe::new(800, None)),
            ("cancel-v8-nested", Probe::new(0, Some("cancel-v8-child"))),
            ("cancel-v8-child", Probe::new(800, None)),
            ("cancel-v8-committed", Probe::committed(800)),
            ("cancel-node-local", Probe::new(800, None)),
            ("cancel-node-remote", Probe::new(800, None)),
            (
                "cancel-node-nested",
                Probe::new(0, Some("cancel-node-child")),
            ),
            ("cancel-node-child", Probe::new(800, None)),
            ("cancel-node-committed", Probe::committed(800)),
            ("cancel-followup", Probe::new(0, None)),
        ]),
    });
    let (owner, owner_url) = fleet::start_configured(
        &directory.path().join("owner"),
        app.clone(),
        OWNER,
        bucket.clone(),
        None,
        |config, _| {
            // Keep the fixed probe set resident so this suite measures
            // cancellation without introducing unrelated owner transfers.
            config.actor.core.max_resident = 32;
            config.actor.core.max_cached_routes = 32;
            Ok(())
        },
    )
    .await?;
    let mut source = None;
    let result = async {
        for name in app.probes.keys() {
            ensure!(read(&client, &owner_url, name, None).await?["result"] == json!({"writes":0}), "cancellation probe warmup failed");
        }
        let (node, source_url) = fleet::start_configured(&directory.path().join("source"), app.clone(), SOURCE, bucket, None,
            |config, _| { config.actor.core.max_resident = 32; config.actor.core.max_cached_routes = 32; Ok(()) }).await?;
        source = Some(node);
        let mut cases = Vec::new();
        for (name, mode, observed, base, ingress) in [
            ("cancel-local", "signal", "cancel-local", owner_url.as_str(), OWNER),
            ("cancel-remote", "signal", "cancel-remote", source_url.as_str(), SOURCE),
            ("cancel-drop", "drop", "cancel-drop", source_url.as_str(), SOURCE),
            ("cancel-nested", "signal", "cancel-child", source_url.as_str(), SOURCE),
            ("cancel-committed", "signal", "cancel-committed", source_url.as_str(), SOURCE),
            ("cancel-python-local", "python-task", "cancel-python-local", owner_url.as_str(), OWNER),
            ("cancel-python-remote", "python-task", "cancel-python-remote", source_url.as_str(), SOURCE),
            ("cancel-python-nested", "python-task", "cancel-python-child", source_url.as_str(), SOURCE),
            ("cancel-python-committed", "python-task", "cancel-python-committed", source_url.as_str(), SOURCE),
            ("cancel-v8-local", "v8-signal", "cancel-v8-local", owner_url.as_str(), OWNER),
            ("cancel-v8-remote", "v8-signal", "cancel-v8-remote", source_url.as_str(), SOURCE),
            ("cancel-v8-nested", "v8-signal", "cancel-v8-child", source_url.as_str(), SOURCE),
            ("cancel-v8-committed", "v8-signal", "cancel-v8-committed", source_url.as_str(), SOURCE),
            ("cancel-node-local", "node-signal", "cancel-node-local", owner_url.as_str(), OWNER),
            ("cancel-node-remote", "node-signal", "cancel-node-remote", source_url.as_str(), SOURCE),
            ("cancel-node-nested", "node-signal", "cancel-node-child", source_url.as_str(), SOURCE),
            ("cancel-node-committed", "node-signal", "cancel-node-committed", source_url.as_str(), SOURCE),
        ] {
            let root = &app.probes[name];
            let target = &app.probes[observed];
            root.armed.store(true, Ordering::Release);
            target.armed.store(true, Ordering::Release);
            let mut url = reqwest::Url::parse(base)?;
            url.query_pairs_mut().append_pair("name", name).append_pair("timeout", "5000").append_pair("cancel", mode);
            let guest = match mode { "python-task" => Some("python"), "v8-signal" => Some("v8"), "node-signal" => Some("node"), _ => None };
            let response = if let Some(guest) = guest {
                url.set_path(&format!("/{guest}/cancel"));
                let control = async {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while !target.checkpoint.load(Ordering::Acquire) {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }).await.context("Guest RPC never reached target checkpoint")?;
                    let response = client.get(format!("{base}/{guest}/cancel-attack?name={name}")).send().await?;
                    ensure!(response.status().is_success() && response.json::<Value>().await?["scope_refused"] == true,
                        "another guest invocation acquired original SDK authority");
                    let response = client.get(format!("{base}/{guest}/cancel-release?name={name}")).send().await?;
                    ensure!(response.status().is_success(), "Guest cancellation control failed");
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while target.refusal.lock().unwrap().is_none() {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }).await.context("Guest cancelled target did not settle")?;
                    let response = client.get(format!("{base}/{guest}/cancel-release?name={name}&phase=finish")).send().await?;
                    ensure!(response.status().is_success(), "Guest cancellation completion control failed");
                    Ok::<_, anyhow::Error>(())
                };
                let (response, control) = tokio::join!(client.get(url).send(), control);
                control?;
                response?
            } else {
                client.get(url).send().await?
            };
            ensure!(response.status().is_success(), "cancellation ingress failed: {}", response.status());
            let reply: Value = response.json().await?;
            ensure!(reply["node"] == ingress, "cancellation ingress crossed domains");
            let expected = match mode {
                "drop" => json!({"dropped":true}),
                "python-task" => json!({"cancelled":true}),
                _ => json!({"code":"Cancelled","outcome":"unknown"}),
            };
            ensure!(reply["result"] == expected, "cancellation returned an incorrect outcome: {reply}");
            let refusal = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(value) = target.refusal.lock().unwrap().clone() { break value; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.context("cancelled target did not settle")?;
            ensure!(refusal.child_binding && refusal.new_storage && refusal.cached_write && refusal.commit,
                "cancelled target retained SDK authority: {refusal:?}");
            if guest.is_some() {
                ensure!(reply["followup_writes"] == 0, "Guest mutation permit was not released: {reply}");
            }
            root.armed.store(false, Ordering::Release);
            target.armed.store(false, Ordering::Release);
            for cell in [name, observed] {
                let committed = i64::from(app.probes[cell].commit_before_cancel);
                ensure!(read(&client, base, cell, None).await?["result"] == json!({"writes":committed}), "cancellation lost a synced write or retained an uncommitted/late write");
                let entries = app.probes[cell].entries.lock().unwrap();
                let first = entries.first().context("cancellation probe never entered")?;
                ensure!(entries.len() >= 3 && entries.iter().all(|entry| entry.node == OWNER && entry.scope == first.scope && entry.epoch == first.epoch), "cancellation moved owner/scope/epoch: {entries:?}");
            }
            cases.push(json!({"name":name,"mode":mode,"ingress":ingress,"owner":OWNER,
                "caller":guest.unwrap_or("rust"),
                "foreign_sdk_refused":guest.map(|_| true),
                "followup_admitted":guest.map(|_| true),
                "child_binding_refused":refusal.child_binding,"storage_access_refused":refusal.new_storage,
                "cached_write_refused":refusal.cached_write,"commit_refused":refusal.commit,"late_writes":0,
                "synced_write_preserved":target.commit_before_cancel}));
        }
        Ok::<_, anyhow::Error>(json!({"format":1,"suite":"cancellation","backends":["rust","python","v8","node"],
            "case_count":cases.len(),"cases":cases,"remote_fleet_qualified":false}))
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
