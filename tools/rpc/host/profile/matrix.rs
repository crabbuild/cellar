// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Every vector traverses real generated clients, native cells and peer routing.
use super::*;
use cellar::bucket::Bucket;
use serde_json::{json, Value};

async fn matrix(
    app: &application::Composite,
    vectors: &[Vector],
    client: &reqwest::Client,
    base: &str,
    source: &str,
    remote: bool,
) -> Result<Vec<Value>> {
    let mut cases = Vec::new();
    for (caller, _, _) in crate::fleet::CLASSES {
        for (provider, _, class) in crate::fleet::CLASSES {
            for vector in vectors {
                let cursor = app.audit.cursor();
                let response = client
                    .get(format!(
                        "{base}/{caller}/profile?target={provider}&vector={}",
                        vector.id
                    ))
                    .send()
                    .await
                    .with_context(|| {
                        format!("profile {source} {caller}->{provider}/{} send", vector.id)
                    })?;
                let status = response.status();
                let body = response.text().await.with_context(|| {
                    format!("profile {source} {caller}->{provider}/{} body", vector.id)
                })?;
                ensure!(
                    status.is_success(),
                    "profile {caller}->{provider}/{}: {status} {body}",
                    vector.id
                );
                ensure!(
                    serde_json::from_str::<Value>(&body)?
                        == json!({"id":vector.id,"method":vector.method,"verified":true}),
                    "profile caller did not validate its generated result"
                );
                let visits = app.audit.since(cursor);
                ensure!(
                    visits.len() == 2,
                    "profile bypassed or duplicated native dispatch"
                );
                let ingress = &visits[0];
                let owner = &visits[1];
                ensure!(
                    ingress.node == source
                        && ingress.class.is_none()
                        && ingress.method == format!("ingress:{caller}")
                        && ingress.vector == vector.id,
                    "profile ingress changed authority"
                );
                ensure!(
                    owner.node == "profile-owner"
                        && owner.class.as_deref() == Some(class)
                        && owner.scope.as_ref().is_some_and(|scope| !scope.is_empty())
                        && owner.epoch.is_some_and(|epoch| epoch > 0)
                        && owner.vector == vector.id
                        && owner.method == format!("{SERVICE}/{}", vector.method),
                    "profile business call used the wrong native owner"
                );
                ensure!(
                    !remote || ingress.node != owner.node,
                    "profile peer case stayed local"
                );
                cases.push(json!({"route":if remote {"signed-peer"} else {"local"},"caller":caller,"provider":provider,"id":vector.id,"method":vector.method,"type":vector.r#type,"source":source,"owner":owner.node,"class":owner.class,"scope":owner.scope,"epoch":owner.epoch,"request_semantics_verified":true,"reply_semantics_verified":true,"caller_verified":true}));
            }
        }
    }
    ensure!(
        cases.len() == 16 * vectors.len(),
        "incomplete live profile matrix"
    );
    Ok(cases)
}

pub(super) async fn run(
    app: Arc<application::Composite>,
    vectors: Arc<Vec<Vector>>,
) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut nodes = Vec::new();
    let result = async {
        // The full profile creates every vector's durable cell and shares its
        // SQLite object store between two nodes. Preserve authority across
        // cold I/O; fault/lease qualification retains its separate short policy.
        let (owner, owner_url) = crate::fleet::start_configured(
            &directory.path().join("owner"),
            app.clone(),
            "profile-owner",
            bucket.clone(),
            None,
            |config, _| {
                config.actor.lease_ttl_ms = 5_000;
                Ok(())
            },
        )
        .await?;
        nodes.push(owner);
        // Local qualification also places every vector's cell while the owner
        // is the only node. The peer phase must reach those actual owners.
        let mut cases = matrix(&app, &vectors, &client, &owner_url, "profile-owner", false).await?;
        let (source, source_url) = crate::fleet::start_configured(
            &directory.path().join("source"),
            app.clone(),
            "profile-source",
            bucket,
            None,
            |config, _| {
                config.actor.lease_ttl_ms = 5_000;
                Ok(())
            },
        )
        .await?;
        nodes.push(source);
        cases.extend(matrix(&app, &vectors, &client, &source_url, "profile-source", true).await?);
        Ok::<_, anyhow::Error>(cases)
    }
    .await;
    // Capture fixed native health before shutdown changes it. This separates
    // a failed request from a later cleanup failure without exposing guest
    // exception text or relaxing the profile's call/lease/handler budgets.
    let mut before_cleanup = Vec::new();
    if result.is_err() {
        for node in &nodes {
            let closed = node.is_closed();
            let readiness =
                match tokio::time::timeout(Duration::from_millis(100), node.is_ready()).await {
                    Ok(Ok(true)) => "ready",
                    Ok(Ok(false)) => "not-ready",
                    Ok(Err(_)) => "unavailable",
                    Err(_) => "observation-timeout",
                };
            before_cleanup.push(format!(
                "{}:closed={closed},readiness={readiness}",
                node.info().node
            ));
        }
    }
    let mut errors = Vec::new();
    for node in nodes.into_iter().rev() {
        if let Err(error) = crate::fleet::stop(node, ShutdownMode::Preserve).await {
            errors.push(format!("{error:#}"));
        }
    }
    let drained = app.audit.drained();
    let cases = result
        .with_context(|| format!("profile before_cleanup={before_cleanup:?}, node cleanup={errors:?}, reservations={drained:?}"))?;
    ensure!(errors.is_empty(), "profile node cleanup failed: {errors:?}");
    drained?;
    Ok(
        json!({"format":1,"contract_digest":app.contract_digest(),"vector_count":vectors.len(),"case_count":cases.len(),"cases":cases,"native_nodes_drained":true,"native_reservations_after_join":0,"all_memory_qualified":false,"python_modes_qualified":false,"release_qualified":false}),
    )
}
