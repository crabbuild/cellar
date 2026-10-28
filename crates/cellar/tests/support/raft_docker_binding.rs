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
    Box::pin(binding(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_docker_resources_observe_local_repair_candidate() -> Result<()> {
    Box::pin(binding(true)).await
}

async fn binding(local_observer: bool) -> Result<()> {
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
    let payload = json!({"Id":id,"RestartCount":0,"Config":{"Labels":{"io.cellar.fleet.owner":"native-binding","io.cellar.fleet.catalog":hex::encode(boot.catalog),"io.cellar.fleet.node":hex::encode(boot.node),"io.cellar.fleet.boot-mode":"single-use","io.cellar.fleet.workers":"contained"}},"HostConfig":{"AutoRemove":false,"Privileged":false,"PidMode":"","RestartPolicy":{"Name":"no","MaximumRetryCount":0},"NanoCpus":1_000_000_000u64,"Memory":3u64*1024*1024*1024,"MemorySwap":3u64*1024*1024*1024},"State":{"Running":true,"Restarting":false,"Paused":false,"Dead":false,"StartedAt":"2026-10-28T00:00:00.000000001Z"}});
    let peer = f.domain.spawn(async move {
        for turn in 0..16 {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") {
                ensure!(bytes.len() < 8192);
                bytes.push(stream.read_u8().await?);
            }
            let head = String::from_utf8(bytes)?;
            let info = turn < 4 && turn % 2 == 0 || turn >= 4 && turn % 4 == 0;
            let stats = turn >= 4 && turn % 4 == 2;
            let expected = if info {
                "GET /info HTTP/1.1".into()
            } else if stats {
                format!("GET /containers/{id}/stats?stream=false&one-shot=true HTTP/1.1")
            } else {
                format!("GET /containers/{id}/json HTTP/1.1")
            };
            ensure!(head.starts_with(&expected));
            let body = serde_json::to_vec(&if info {
                json!({"ID":"native-engine","OSType":"linux"})
            } else if stats {
                json!({"id":id,"read":"2026-10-28T00:00:01.000000001Z","memory_stats":{"usage":128u64*1024*1024,"limit":3u64*1024*1024*1024}})
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
    let view = target.observe_node(TIMEOUT).await?;
    f.remote(&origin, node, before).await?;
    let resources = FleetDockerResources::new(
        reopened.clone(),
        if local_observer {
            target.clone()
        } else {
            origin.clone()
        },
        FleetDockerResourcePolicy {
            durable_overhead_bytes: 2 * 1024 * 1024,
            resident: FleetResourceBounds {
                engine_slots: 1,
                durable_bytes: view.node.profile.bytes_per_group,
                memory_bytes: 8 * 1024 * 1024,
            },
            peak: FleetResourceBounds {
                engine_slots: 1,
                durable_bytes: view.node.profile.bytes_per_group * 2,
                memory_bytes: 16 * 1024 * 1024,
            },
            reserve: FleetResourceBounds::default(),
        },
    )?;
    let sample = resources.observe(node, TIMEOUT).await?;
    ensure!(
        sample.boot == boot
            && sample.used.memory_bytes >= 128 * 1024 * 1024
            && sample.used.durable_bytes >= 2 * 1024 * 1024
    );
    let record_key = object_store::path::Path::from(format!(
        "tenant/hosts/raft/{}/fleet/platform/docker-isolation-1/{}/resources/{}",
        hex::encode([1; 16]),
        hex::encode(boot.fleet),
        hex::encode(sample.evidence_digest)
    ));
    let bytes = f.store.get(&record_key).await?.bytes().await?;
    ensure!(sha2::Sha256::digest(&bytes).as_slice() == sample.evidence_digest);
    // Exercise the service-provider interfaces. They supply inputs, while the
    // service independently authorizes the actual reservation and membership.
    let group = GroupScope {
        cluster: f.declaration.namespace.cluster,
        namespace: f.declaration.namespace.namespace,
        cell: "Counter:resource-input".into(),
        group: GroupId::new([231; 16])?,
        incarnation: IncarnationId::new([232; 16])?,
    };
    let movement = FleetPendingMovement {
        reservation: [233; 16],
        group: group.clone(),
        plan: RepairPlan {
            id: ProposalId::new([234; 16])?,
            policy_revision: 1,
            expected_revision: cellar_logic::consensus::LogIndex::new(1),
            obsolete: MemberId::new(1)?,
            replacement: JoiningMember {
                node,
                disk: DiskLineage::new(boot.disk)?,
                member: MemberId::new(4)?,
            },
        },
    };
    let input = FleetMovementEvidence::refresh(&resources, &movement, TIMEOUT).await?;
    ensure!(
        input.reservation == movement.reservation
            && input.attestation.boot == boot
            && input.from_s3
    );
    let context = FleetIsolatedRepairContext {
        operation: ProposalId::new([235; 16])?,
        failed_boot: origin
            .observe_node(TIMEOUT)
            .await?
            .fleet_boot
            .context("origin boot")?,
        group: group.clone(),
    };
    let input = FleetIsolatedRepairEvidence::refresh(&resources, &context, TIMEOUT).await?;
    ensure!(
        input.history.group == group
            && input.nodes.len() == 1
            && input.namespaces.len() == 1
            && input.nodes[0].attestation.boot == boot
    );
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
    ensure!(matches!(
        resources.observe(node, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    capacity_tests::joined(target_node).await?;
    capacity_tests::joined(origin_node).await?;
    f.domain.shutdown().await;
    Ok(())
}
