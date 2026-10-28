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
        let (owner, owner_url) = crate::fleet::start(
            &directory.path().join("owner"),
            app.clone(),
            "profile-owner",
            bucket.clone(),
        )
        .await?;
        nodes.push(owner);
        // Local qualification also places every vector's cell while the owner
        // is the only node. The peer phase must reach those actual owners.
        let mut cases = matrix(&app, &vectors, &client, &owner_url, "profile-owner", false).await?;
        let (source, source_url) = crate::fleet::start(
            &directory.path().join("source"),
            app.clone(),
            "profile-source",
            bucket,
        )
        .await?;
        nodes.push(source);
        cases.extend(matrix(&app, &vectors, &client, &source_url, "profile-source", true).await?);
        Ok::<_, anyhow::Error>(cases)
    }
    .await;
    let mut errors = Vec::new();
    for node in nodes.into_iter().rev() {
        if let Err(error) = crate::fleet::stop(node, ShutdownMode::Preserve).await {
            errors.push(format!("{error:#}"));
        }
    }
    let drained = app.audit.drained();
    let cases = result
        .with_context(|| format!("profile node cleanup={errors:?}, reservations={drained:?}"))?;
    ensure!(errors.is_empty(), "profile node cleanup failed: {errors:?}");
    drained?;
    Ok(
        json!({"format":1,"contract_digest":app.contract_digest(),"vector_count":vectors.len(),"case_count":cases.len(),"cases":cases,"native_nodes_drained":true,"native_reservations_after_join":0,"all_memory_qualified":false,"python_modes_qualified":false,"release_qualified":false}),
    )
}
