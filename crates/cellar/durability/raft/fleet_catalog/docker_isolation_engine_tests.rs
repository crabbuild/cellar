// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! External harness must name a local Engine socket and pre-pulled image.
//! This verifies the Docker boundary, not a deployed Cellar/RustFS fleet.
use super::*;
use rand::RngCore;

#[tokio::test]
#[ignore = "requires explicit owned Docker Engine harness"]
async fn docker_isolation_real_engine_stops_paused_container_and_retains_data() -> anyhow::Result<()>
{
    let socket =
        std::env::var("CELLAR_FLEET_DOCKER_TEST_SOCKET").context("explicit test Engine socket")?;
    let image = std::env::var("CELLAR_FLEET_DOCKER_TEST_IMAGE")
        .context("explicit pre-pulled test image")?;
    ensure!(
        image.starts_with("busybox@sha256:") && image.len() == 79,
        "pin the test image digest"
    );
    let f = Fixture::new()?;
    let docker = Docker::new(socket);
    let name = f
        .domain
        .run(async {
            format!(
                "cellar-fleet-isolation-{:016x}",
                crate::asyncrt::rng("fleet-isolation-test").next_u64()
            )
        })
        .await?;
    let marker = "cellar-owned-isolation-retained-data";
    let created=f.domain.run(crate::asyncrt::timeout(WAIT,docker.expect("POST",&format!("/containers/create?name={name}"),Some(json!({
        "Image":image,"Cmd":["sh","-c",format!("echo {marker} > /data/marker; exec sleep 3600")],
        "Volumes":{"/data":{}},
        "Labels":{"io.cellar.fleet.owner":"fixture","io.cellar.fleet.catalog":hex::encode(f.boot.catalog),"io.cellar.fleet.node":hex::encode(f.boot.node),"io.cellar.fleet.boot-mode":"single-use","io.cellar.fleet.workers":"contained"},
        "HostConfig":{"NetworkMode":"none","AutoRemove":false,"Privileged":false,"RestartPolicy":{"Name":"no","MaximumRetryCount":0},"NanoCpus":1_000_000_000u64,"Memory":3u64*1024*1024*1024,"MemorySwap":3u64*1024*1024*1024}
    })),"create owned isolation test container"))).await???;
    let id = ContainerId::new(
        created.json()?["Id"]
            .as_str()
            .context("created immutable ID")?,
    )?;
    let outcome = f
        .domain
        .run(crate::asyncrt::timeout(Duration::from_secs(20), async {
            docker
                .expect(
                    "POST",
                    &format!("/containers/{id}/start"),
                    None,
                    "start owned target",
                )
                .await?;
            // Require the marker before injecting the paused-container failure.
            crate::asyncrt::timeout(WAIT, async {
                loop {
                    let (reply, _) = docker
                        .archive(
                            "GET",
                            &format!("/containers/{id}/archive?path=/data/marker"),
                            bytes::Bytes::new(),
                            64 * 1024,
                        )
                        .await?;
                    if reply.status.is_success()
                        && reply
                            .body
                            .windows(marker.len())
                            .any(|v| v == marker.as_bytes())
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                    crate::asyncrt::sleep(Duration::from_millis(20)).await;
                }
            })
            .await??;
            let adapter =
                FleetDockerIsolation::new(f.catalog.clone(), docker.clone(), "fixture", vec![])?;
            let binding = adapter.inner.engine.bind(&f.boot, &id).await?;
            adapter.inner.persist_binding(&binding).await?;
            docker
                .expect(
                    "POST",
                    &format!("/containers/{id}/pause"),
                    None,
                    "pause owned target",
                )
                .await?;
            let proof = adapter
                .run(
                    Duration::from_secs(15),
                    adapter.isolate_boot(ProposalId::new([9; 16])?, &f.boot),
                )
                .await?;
            ensure!(proof.boot == f.boot);
            let retained = docker
                .expect(
                    "GET",
                    &format!("/containers/{id}/json"),
                    None,
                    "inspect retained target",
                )
                .await?
                .json()?;
            ensure!(
                retained["Id"] == id.as_str()
                    && retained["State"]["Status"] == "exited"
                    && retained["State"]["Pid"] == 0
            );
            ensure!(retained["HostConfig"]["RestartPolicy"]["Name"] == "no");
            ensure!(
                retained["HostConfig"]["NanoCpus"] == 1_000_000_000u64
                    && retained["HostConfig"]["Memory"] == 3u64 * 1024 * 1024 * 1024
            );
            ensure!(retained["Mounts"]
                .as_array()
                .context("retained mounts")?
                .iter()
                .any(|m| m["Destination"] == "/data" && m["Type"] == "volume"));
            let (archive, _) = docker
                .archive(
                    "GET",
                    &format!("/containers/{id}/archive?path=/data/marker"),
                    bytes::Bytes::new(),
                    64 * 1024,
                )
                .await?;
            ensure!(
                archive.status.is_success()
                    && archive
                        .body
                        .windows(marker.len())
                        .any(|v| v == marker.as_bytes())
            );
            let successor =
                FleetDockerIsolation::new(f.catalog.clone(), docker.clone(), "fixture", vec![])?;
            ensure!(
                successor
                    .run(
                        WAIT,
                        successor.isolate_boot(ProposalId::new([9; 16])?, &f.boot)
                    )
                    .await?
                    == proof
            );
            Ok::<_, anyhow::Error>(())
        }))
        .await;
    // Test-only cleanup of the exact ID created above, after retention checks.
    // The production adapter has no DELETE path or volume reclamation authority.
    let cleanup = f
        .domain
        .run(crate::asyncrt::timeout(
            WAIT,
            docker.call(
                "DELETE",
                &format!("/containers/{id}?force=true&v=true"),
                None,
            ),
        ))
        .await;
    f.domain.shutdown().await;
    outcome???;
    ensure!(
        cleanup???.status.as_u16() == 204,
        "owned test cleanup failed"
    );
    Ok(())
}
