// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real managed enrollment and a bounded fake Engine validate the public
//! provisioning boundary. This does not claim container shutdown evidence.
use super::*;
use anyhow::Context;
use cellar::docker::{ContainerId, Docker};
use object_store::ObjectStore;
use serde_json::json;
use sha2::Digest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_docker_binding_records_ready_boot_and_refuses_evacuation() -> Result<()> {
    Box::pin(binding()).await
}

async fn binding() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_origin_root, origin_node, origin) = f.empty_host().await?;
    let (_target_root, target_node, target) = f.peer_host(5).await?;
    let evidence = capacity_tests::attestation(&f, &origin, &target).await?;
    let boot = evidence.boot.clone();
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("engine.sock");
    let listener = UnixListener::bind(&socket)?;
    let id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let catalog = Fixture::open_catalog(&f.domain, f.store.clone())?;
    let adapter = FleetDockerIsolation::new(
        catalog.clone(),
        Docker::new(&socket),
        "native-binding",
        vec![capacity_tests::namespace(&f).await?],
    )?;
    let node = NodeId::new(boot.node)?;
    let container = ContainerId::new(id)?;
    ensure!(matches!(
        adapter.bind_node(node, &container, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    let cut = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .admit_node_capacity(
            &cut,
            &FleetCapacityRequest {
                identity: Fixture::request(240, cut.revision()),
                attestation: evidence.clone(),
                action: FleetCapacityAction::Qualify,
            },
            &origin,
            &[capacity_tests::namespace(&f).await?],
            TIMEOUT,
        )
        .await?;
    let payload = json!({"Id":id,"RestartCount":0,"Config":{"Labels":{"io.cellar.fleet.owner":"native-binding","io.cellar.fleet.catalog":hex::encode(boot.catalog),"io.cellar.fleet.node":hex::encode(boot.node),"io.cellar.fleet.boot-mode":"single-use","io.cellar.fleet.workers":"contained"}},"HostConfig":{"AutoRemove":false,"Privileged":false,"PidMode":"","RestartPolicy":{"Name":"no","MaximumRetryCount":0}},"State":{"Running":true,"Restarting":false,"Paused":false,"Dead":false,"StartedAt":"2026-10-28T00:00:00.000000001Z"}});
    let peer = f.domain.spawn(async move {
        for turn in 0..4 {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") {
                ensure!(bytes.len() < 8192);
                bytes.push(stream.read_u8().await?);
            }
            let head = String::from_utf8(bytes)?;
            let expected = if turn % 2 == 0 {
                "GET /info HTTP/1.1".into()
            } else {
                format!("GET /containers/{id}/json HTTP/1.1")
            };
            ensure!(head.starts_with(&expected));
            let body = serde_json::to_vec(&if turn % 2 == 0 {
                json!({"ID":"native-engine","OSType":"linux"})
            } else {
                payload.clone()
            })?;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
            stream.write_all(&body).await?;
        }
        Ok::<_, anyhow::Error>(())
    })?;
    let before = catalog.read(TIMEOUT).await?.revision();
    adapter
        .bind_node(node, &container, TIMEOUT)
        .await
        .context("bind Ready node")?;
    let reopened = FleetDockerIsolation::new(
        Fixture::open_catalog(&f.domain, f.store.clone())?,
        Docker::new(&socket),
        "native-binding",
        vec![capacity_tests::namespace(&f).await?],
    )?;
    reopened
        .bind_node(node, &container, TIMEOUT)
        .await
        .context("reconcile Ready binding")?;
    peer.await??;
    ensure!(catalog.read(TIMEOUT).await?.revision() == before);
    let digest = sha2::Sha256::digest(serde_json::to_vec(&boot)?);
    let key = object_store::path::Path::from(format!(
        "tenant/hosts/raft/{}/fleet/platform/docker-isolation-1/{}/boots/{}",
        hex::encode([1; 16]),
        hex::encode(boot.fleet),
        hex::encode(digest)
    ));
    let saved = f.store.get(&key).await?.bytes().await?;
    let stored: serde_json::Value = serde_json::from_slice(&saved)?;
    ensure!(stored["boot"] == serde_json::to_value(&boot)? && stored["container"] == id);
    // Binding is allowed before placement, but departure requires the normal
    // Active capacity boundary. Preserve that guard in this fixture too.
    let cut = catalog.read(TIMEOUT).await?;
    f.remote(&origin, node, cut.revision()).await?;
    catalog
        .admit_node_capacity(
            &cut,
            &FleetCapacityRequest {
                identity: Fixture::request(241, cut.revision()),
                attestation: evidence,
                action: FleetCapacityAction::Activate,
            },
            &origin,
            &[capacity_tests::namespace(&f).await?],
            TIMEOUT,
        )
        .await
        .context("activate bound node")?;
    let cut = catalog.read(TIMEOUT).await?;
    catalog
        .begin_evacuation(
            &cut,
            &Fixture::request(242, cut.revision()),
            node,
            &[capacity_tests::namespace(&f).await?],
            TIMEOUT,
        )
        .await
        .context("evacuate Active bound node")?;
    ensure!(matches!(
        reopened.bind_node(node, &container, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    capacity_tests::joined(target_node).await?;
    capacity_tests::joined(origin_node).await?;
    f.domain.shutdown().await;
    Ok(())
}
