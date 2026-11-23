// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Signed read admission, archive coverage and fresh quorum refusal.

use super::*;

async fn request(
    cluster: &ManagedCluster,
    name: &str,
    body: serde_json::Value,
    signed: bool,
    tampered: bool,
) -> Result<reqwest::StatusCode> {
    let info = cluster.nodes[1].info();
    let path = format!(
        "/runtime/{}?name={}",
        cellar::engine_api::queue_cell_scope(DEAD),
        url::form_urlencoded::byte_serialize(name.as_bytes()).collect::<String>()
    );
    let payload = serde_json::to_vec(&body)?;
    let request = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!("http://{}{path}", info.internal_address))
        .body(payload.clone());
    let request = if signed {
        cellar::peer_auth::PeerAuth::new([11; 32], "queue-operator-fixture")?
            .sign(request, "POST", &path, &payload, &info.node)?
    } else {
        request
    };
    let request = if tampered {
        request.body(serde_json::to_vec(&json!({"op":"resume"}))?)
    } else {
        request
    };
    let response = request.send().await?;
    let status = response.status();
    let _ = response.bytes().await?;
    Ok(status)
}

pub async fn qualify() -> Result<()> {
    for rooted in [false, true] {
        let s = Scenario::start(
            rooted,
            Arc::new(catalog_fault::Store::default()),
            false,
            5000,
            None,
            8,
            Durability::S3.into(),
        )
        .await?;
        let domain = s.cluster.fixtures[0].domain.clone();
        let checked = domain.run(async {
            ready(&s.cluster, &[s.dead]).await?;
            let leader = managed_fixture::leader(&s.cluster.hosts, s.dead).await?;
            let handle = s.cluster.hosts[leader].group(s.dead)?;
            let before = handle.status().await?.committed;
            ensure!(admin(&s.cluster, DEAD, json!({"op":"info"})).await?["stored"] == 0.0);
            let peek = admin(&s.cluster, DEAD, json!({"op":"peek","console":true})).await?;
            ensure!(peek["messages"].as_array().is_some_and(Vec::is_empty));
            ensure!(peek["truncated"] == false && peek["byte_budget"] == 512.0 * 1024.0);

            for (name, signed, tampered) in [(DEAD, false, false), (DEAD, true, true), (QUEUE, true, false)] {
                let status = request(&s.cluster, name, json!({"op":"info"}), signed, tampered).await?;
                ensure!(status.is_client_error(), "Queue read authentication/scope refusal: {status}");
            }
            let maximum = s.cluster.fixtures[0].cell_limits.queue.max_admin_batch_size;
            for body in [
                json!({"op":"peek","limit":0}),
                json!({"op":"peek","limit":-1}),
                json!({"op":"peek","limit":1.5}),
                json!({"op":"peek","limit":maximum.checked_add(1).context("admin limit overflow")?}),
                json!({"op":"peek","console":true,"limit":101}),
            ] {
                ensure!(admin(&s.cluster, DEAD, body).await.is_err(), "invalid Queue read limit succeeded");
            }
            ensure!(handle.status().await?.committed == before, "Queue reads/refusals appended mutations");

            let owner = &s.cluster.hosts[1];
            let history = owner.resolve_cell_history(
                s.dead.cluster,
                s.dead.namespace,
                CellId::new(cellar::engine_api::queue_cell_scope(DEAD))?,
                QueryOptions::default(),
            ).await?.context("initialized Queue history")?;
            let cell = owner.cell(history)?;
            let stale = cell.query(
                ProposalId::new([201; 16])?,
                &cell.contract("__cellar_queue_operator_read")?,
                vec![],
                QueryOptions { consistency: QueryConsistency::Stale, ..Default::default() },
            ).await;
            ensure!(stale.is_err_and(|error| error.to_string().contains("Queue operator read consistency")),
                "Queue operator query did not refuse stale consistency before decoding");

            let store = s.cluster.fixtures[0].store.clone();
            let denied = store.denied_group_archive_writes.load(Ordering::Acquire);
            store.deny_group_archive_writes.store(true, Ordering::Release);
            let release = BlockArchive(store.clone());
            // The response is ambiguous, so do not retry this mutation. Prove
            // that it committed while its required archive remained unavailable.
            ensure!(admin(&s.cluster, DEAD, json!({"op":"pause"})).await.is_err(),
                "S3 pause escaped the real archive write fault");
            ready(&s.cluster, &[s.dead]).await?;
            let committed = handle.status().await?.committed;
            ensure!(committed > before && store.denied_group_archive_writes.load(Ordering::Acquire) > denied,
                "archive fault did not cover a committed Queue mutation");
            // The shared SQL worker and signed read ingress remain healthy at
            // Quorum while the S3 Queue must wait for the selected image's proof.
            ensure!(admin(&s.cluster, QUEUE, json!({"op":"info"})).await?["backlogCount"] == 0.0);
            for op in ["info", "peek"] {
                let status = request(&s.cluster, DEAD, json!({"op":op}), true, false).await?;
                ensure!(status == reqwest::StatusCode::SERVICE_UNAVAILABLE,
                    "S3 Queue {op} escaped missing archive coverage: {status}");
            }
            ensure!(handle.status().await?.committed == committed,
                "failed archive-covered Queue reads appended mutations");
            drop(release);
            archive_ready(&s.cluster, &[s.dead]).await?;
            ensure!(admin(&s.cluster, DEAD, json!({"op":"info"})).await?["paused"] == true);
            ensure!(admin(&s.cluster, DEAD, json!({"op":"peek"})).await?["messages"].as_array().is_some_and(Vec::is_empty));
            ensure!(handle.status().await?.committed == committed,
                "recovered S3 Queue reads appended mutations");

            let survivor = s.cluster.hosts[1].group(s.dead)?;
            for index in [0, 2] {
                s.cluster.hosts[index].close().await?;
            }
            let before_loss = survivor.status().await?.committed;
            let status = request(&s.cluster, DEAD, json!({"op":"info"}), true, false).await?;
            ensure!(status == reqwest::StatusCode::SERVICE_UNAVAILABLE,
                "Queue info escaped fresh quorum loss: {status}");
            ensure!(survivor.status().await?.committed == before_loss,
                "Queue quorum-loss read appended a mutation");
            eprintln!("Queue operator read boundaries: rooted={rooted} archive_and_quorum_refusals=true");
            Ok::<_, anyhow::Error>(())
        }).await;
        let retain = !matches!(&checked, Ok(Ok(())));
        let cleanup = s.close(retain).await;
        domain.shutdown().await;
        checked??;
        cleanup?;
    }
    Ok(())
}
