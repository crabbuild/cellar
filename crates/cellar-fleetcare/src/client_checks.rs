//! Untimed invariant and SDK-capability gates for the measuring client.
use super::*;

fn spec(id: &str, action: &str, payload: Value, expected: Option<u64>) -> Command {
    Command {
        command_id: id.into(),
        action: action.into(),
        payload,
        expected_version: expected,
    }
}

pub(super) async fn command(
    client: &Client,
    config: &Config,
    writer: &Mutex<BufWriter<tokio::fs::File>>,
    path: &str,
    command: Command,
) -> Result<Receipt> {
    let value = fetch(
        client,
        config,
        0,
        path,
        Some(&serde_json::to_value(&command)?),
    )
    .await
    .with_context(|| format!("command {} at {path}", command.command_id))?;
    let receipt: Receipt = serde_json::from_value(value["receipt"].clone())?;
    let scope = value["scope"]
        .as_str()
        .context("probe receipt lacks scope")?;
    ensure!(
        receipt.digest == command.digest(scope)?,
        "probe receipt digest mismatch"
    );
    append(
        writer,
        &Attempt {
            phase: config.phase.clone(),
            index: 0,
            operation: "probe".into(),
            path: path.into(),
            ingress: 0,
            scheduled_us: 0,
            sent_us: Some(0),
            finished_us: 0,
            status: "ok".into(),
            bytes: 0,
            command: Some(command),
            receipt: Some(receipt.clone()),
            scope: Some(scope.into()),
            owner: value["owner"].as_str().map(str::to_owned),
        },
    )
    .await?;
    writer.lock().await.flush().await?;
    Ok(receipt)
}

pub(super) async fn probes(client: &Client, config: &Config) -> Result<Value> {
    let writer = Mutex::new(BufWriter::new(
        tokio::fs::File::create(&config.ledger).await?,
    ));
    let mut cases = Vec::new();
    for path in [
        "/api/0/devices/0/diagnostics/storage",
        "/api/0/devices/0/diagnostics/facet",
        "/api/0/devices/0/diagnostics/container",
        "/diagnostics/bindings",
        "/diagnostics/service",
        "/diagnostics/assets",
        "/diagnostics/blob",
        "/diagnostics/loader",
        "/diagnostics/web",
        "/diagnostics/tcp",
        "/diagnostics/outbound",
        "/diagnostics/tls",
        "/diagnostics/queue",
    ] {
        let started = Instant::now();
        let value = fetch(client, config, 0, path, None)
            .await
            .with_context(|| format!("diagnostic {path}"))?;
        ensure!(
            value["passed"].as_array().is_some_and(|p| !p.is_empty()),
            "diagnostic returned no assertions"
        );
        cases.push(json!({"path":path,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0,"evidence":value}));
        tokio::fs::write(
            &config.report,
            serde_json::to_vec_pretty(&json!({"action":"probes","completed":false,"cases":cases}))?,
        )
        .await?;
    }
    let path = "/api/0/devices/0";
    let first = command(
        client,
        config,
        &writer,
        path,
        spec(
            "probe-telemetry",
            "telemetry",
            json!({"seq":100,"temperature":95,"offline_after_ms":1000}),
            Some(1),
        ),
    )
    .await?;
    ensure!(
        first.accepted && first.result["health"] == "alerting",
        "telemetry transition failed"
    );
    let repeat = command(
        client,
        config,
        &writer,
        path,
        spec(
            "probe-telemetry",
            "telemetry",
            json!({"seq":100,"temperature":95,"offline_after_ms":1000}),
            Some(1),
        ),
    )
    .await?;
    ensure!(repeat == first, "replay changed receipt");
    let rejected = command(
        client,
        config,
        &writer,
        path,
        spec(
            "probe-conflict",
            "telemetry",
            json!({"seq":101}),
            Some(99999),
        ),
    )
    .await?;
    ensure!(
        !rejected.accepted && rejected.version == first.version,
        "version rejection mutated state"
    );
    ensure!(
        fetch(
            client,
            config,
            0,
            path,
            Some(
                &json!({"command_id":"probe-telemetry","action":"telemetry","payload":{"seq":999}})
            )
        )
        .await
        .is_err(),
        "changed ID was accepted"
    );
    let inventory = "/api/0/inventory/0";
    ensure!(
        command(
            client,
            config,
            &writer,
            inventory,
            spec(
                "probe-reserve",
                "reserve",
                json!({"reservation_id":"probe-expire","qty":2,"ttl_ms":1000}),
                None
            )
        )
        .await?
        .accepted,
        "reservation rejected"
    );
    let started = Instant::now();
    loop {
        let device = fetch(client, config, 0, path, None).await?;
        let reservation = fetch(
            client,
            config,
            0,
            &format!("{inventory}/reservations/probe-expire"),
            None,
        )
        .await?;
        if device["state"]["health"] == "offline"
            && reservation["reservation"]["status"] == "expired"
        {
            break;
        }
        ensure!(
            started.elapsed() < Duration::from_secs(30),
            "durable deadline failed to fire"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cases.push(json!({"case":"device_offline_reservation_expiry", "visible_after_ms":started.elapsed().as_secs_f64()*1000.0}));
    let book = "/api/0/books/0";
    ensure!(
        command(
            client,
            config,
            &writer,
            book,
            spec(
                "probe-saga",
                "create",
                json!({"order_id":"probe-saga","inventory_id":0,"saga":true}),
                None
            )
        )
        .await?
        .accepted,
        "saga rejected"
    );
    let started = Instant::now();
    loop {
        let order = fetch(
            client,
            config,
            0,
            &format!("{book}/orders/probe-saga"),
            None,
        )
        .await?;
        if order["order"]["status"] == "completed" {
            break;
        }
        ensure!(
            started.elapsed() < Duration::from_secs(60),
            "saga did not complete: {order}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cases.push(json!({"case":"workflow_participant_saga", "visible_after_ms":started.elapsed().as_secs_f64()*1000.0}));
    for (order, status) in [
        ("probe-ambiguous", "completed"),
        ("probe-reject", "cancelled"),
    ] {
        ensure!(
            command(
                client,
                config,
                &writer,
                book,
                spec(
                    order,
                    "create",
                    json!({"order_id":order,"inventory_id":0,"saga":true}),
                    None
                )
            )
            .await?
            .accepted,
            "failure saga rejected"
        );
        let started = Instant::now();
        loop {
            let value = fetch(client, config, 0, &format!("{book}/orders/{order}"), None).await?;
            if value["order"]["status"] == status {
                break;
            }
            ensure!(
                started.elapsed() < Duration::from_secs(60),
                "failure saga did not settle: {value}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let state: Value = client
            .get("http://technician:8080/state")
            .send()
            .await?
            .json()
            .await?;
        let id = format!(
            "notify-{}",
            cellar_fleetcare::contracts::assignment_id(0, 0, order)
        );
        ensure!(
            state["attempts"][&id].as_u64().is_some_and(|n| n >= 2),
            "workflow did not retry technician failure"
        );
        ensure!(
            (status == "completed") == state["effects"].get(&id).is_some(),
            "notification side effect mismatch"
        );
        cases.push(json!({"case":"workflow_egress_recovery","order":order,"status":status,"visible_after_ms":started.elapsed().as_secs_f64()*1000.0}));
    }
    // Public order names are local to each book. Shared inventory and the
    // external technician must still distinguish their participant identities.
    let order = "probe-scoped-name";
    for (tenant, book_id) in [(0, 0), (0, 1), (1, 0)] {
        let path = format!("/api/{tenant}/books/{book_id}");
        ensure!(
            command(
                client,
                config,
                &writer,
                &path,
                spec(
                    order,
                    "create",
                    json!({"order_id":order,"inventory_id":0,"saga":true}),
                    None
                )
            )
            .await?
            .accepted,
            "scoped order rejected"
        );
        let started = Instant::now();
        let reservation = cellar_fleetcare::contracts::reservation_id(tenant, book_id, order);
        loop {
            let value = fetch(client, config, 0, &format!("{path}/orders/{order}"), None).await?;
            ensure!(
                value["order"]["reservation_id"] == reservation,
                "order participant identity mismatch"
            );
            if value["order"]["status"] == "completed" {
                break;
            }
            ensure!(
                started.elapsed() < Duration::from_secs(60),
                "scoped saga did not complete: {value}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let value = fetch(
            client,
            config,
            0,
            &format!("/api/{tenant}/inventory/0/reservations/{reservation}"),
            None,
        )
        .await?;
        ensure!(
            value["reservation"]["status"] == "consumed",
            "scoped reservation was not consumed"
        );
        let external: Value = client
            .get("http://technician:8080/state")
            .send()
            .await?
            .json()
            .await?;
        let identity = format!(
            "notify-{}",
            cellar_fleetcare::contracts::assignment_id(tenant, book_id, order)
        );
        ensure!(
            external["effects"][&identity]["tenant"] == tenant
                && external["effects"][&identity]["book"] == book_id,
            "external effect crossed authority"
        );
    }
    cases.push(json!({"case":"order_identity_across_books_and_tenants","passed":true}));
    for action in ["pause", "resume", "restart", "terminate", "delete", "seed"] {
        fetch(
            client,
            config,
            0,
            "/system/workflow/0/1",
            Some(&json!({"action":action})),
        )
        .await
        .with_context(|| format!("workflow control {action}"))?;
    }
    let started = Instant::now();
    loop {
        let mut poison = false;
        for partition in 0..3 {
            let rows = fetch(
                client,
                config,
                0,
                &format!("/system/sql/0/{partition}?quarantine=1"),
                None,
            )
            .await?;
            poison |= rows["rows"]
                .as_array()
                .context("quarantine rows absent")?
                .iter()
                .any(|r| r["effect_id"] == "diagnostic-poison");
        }
        if poison {
            break;
        }
        ensure!(
            started.elapsed() < Duration::from_secs(30),
            "poison did not reach configured DLQ"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cases.push(json!({"case":"workflow_controls_and_queue_poison_dlq","passed":true}));
    fetch(
        client,
        config,
        0,
        "/system/dlq/0/0",
        Some(&json!({
            "effect_id":"diagnostic-dlq-projection", "kind":"projection", "version":1, "state":{"version":1},
        })),
    )
    .await?;
    let started = Instant::now();
    loop {
        let mut recovered = false;
        for partition in 0..3 {
            let rows = fetch(
                client,
                config,
                0,
                &format!("/system/sql/0/{partition}?after=seed-dlq-"),
                None,
            )
            .await?;
            for row in rows["rows"].as_array().context("projection rows absent")? {
                if row["source_scope"] == "seed-dlq-0" {
                    let snapshot: Value = serde_json::from_str(
                        row["snapshot_json"].as_str().context("snapshot absent")?,
                    )?;
                    recovered |= snapshot["effect_id"] == "diagnostic-dlq-projection"
                        && snapshot["state"]["version"] == 1;
                }
            }
        }
        if recovered {
            break;
        }
        ensure!(
            started.elapsed() < Duration::from_secs(30),
            "valid DLQ projection was not repaired"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cases.push(json!({"case":"dead_letter_projection_recovery","passed":true}));
    cases.extend(super::subscriptions::verify(client, config, &writer).await?);
    cases.push(super::attachments::verify(client, config, &writer).await?);
    writer.lock().await.flush().await?;
    Ok(json!({"action":"probes","cases":cases}))
}

pub(super) async fn projections(
    client: &Client,
    config: &Config,
    manifest: &Manifest,
) -> Result<Value> {
    let expected = Mutex::new(BTreeMap::<String, u64>::new());
    stream::iter(
        manifest
            .cells
            .iter()
            .filter(|e| is_business(&e.kind))
            .enumerate(),
    )
    .map(Ok::<_, anyhow::Error>)
    .try_for_each_concurrent(config.concurrency.min(32), |(i, e)| {
        let expected = &expected;
        async move {
            let value = fetch(client, config, i % config.urls.len(), &e.path, None).await?;
            expected.lock().await.insert(
                e.scope.clone(),
                value["state"]["version"]
                    .as_u64()
                    .context("authoritative version absent")?,
            );
            Ok(())
        }
    })
    .await?;
    let expected = expected.into_inner();
    let started = Instant::now();
    loop {
        let mut actual = BTreeMap::new();
        for e in manifest.cells.iter().filter(|e| e.kind == "sql") {
            let mut after = String::new();
            loop {
                let mut url = url::Url::parse(&format!("http://placeholder{}", e.path))?;
                url.query_pairs_mut().append_pair("after", &after);
                let value = fetch(
                    client,
                    config,
                    0,
                    &format!("{}?{}", url.path(), url.query().unwrap_or("")),
                    None,
                )
                .await?;
                let rows = value["rows"].as_array().context("projection rows absent")?;
                for row in rows {
                    let scope = row["source_scope"]
                        .as_str()
                        .context("projection scope absent")?;
                    let version = row["version"]
                        .as_u64()
                        .context("projection version absent")?;
                    let snapshot: Value = serde_json::from_str(
                        row["snapshot_json"]
                            .as_str()
                            .context("projection snapshot absent")?,
                    )?;
                    if expected.contains_key(scope) {
                        ensure!(
                            snapshot["state"]["version"] == version,
                            "projection snapshot and watermark differ"
                        );
                    }
                    actual.insert(scope.to_string(), version);
                    after = scope.to_string();
                }
                if rows.len() < 100 {
                    break;
                }
            }
        }
        let pending = expected
            .iter()
            .filter(|(scope, v)| actual.get(*scope).is_none_or(|a| a < *v))
            .count();
        if pending == 0 {
            return Ok(
                json!({"action":"projections","sources":expected.len(),"visibility_check_elapsed_s":started.elapsed().as_secs_f64()}),
            );
        }
        if started.elapsed() >= Duration::from_secs(120) {
            let missing: Vec<_> = expected
                .iter()
                .filter(|(scope, v)| actual.get(*scope).is_none_or(|a| a < *v))
                .take(10)
                .map(|(scope, version)| {
                    json!({"scope":scope,
                    "expected_version":version,"actual_version":actual.get(scope)})
                })
                .collect();
            anyhow::bail!(
                "{pending} projections failed their captured authoritative watermarks: {}",
                serde_json::to_string(&missing)?
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
