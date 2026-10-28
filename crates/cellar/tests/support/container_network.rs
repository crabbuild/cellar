// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual production CellContainer/helper lifetime, not a protocol substitute.

use super::*;
use cellar::{
    asyncrt::ExecutionDomain,
    container::{self, ContainerSpec, EngineOptions, NetworkHelperOptions, Release, StartParams},
};

#[derive(Clone, Copy, Debug)]
pub(super) enum Scenario {
    Reconnect,
    InstallerFailure,
    HelperFailure,
}

pub(super) async fn exercise(campaign: &mut Campaign, scenario: Scenario) -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir_in("/tmp")?;
    let image = campaign
        .docker
        .expect(
            "GET",
            &format!("/images/{}/json", campaign.helper_image),
            None,
            "resolve qualification helper",
        )
        .await?
        .json()?["Id"]
        .as_str()
        .context("helper image identity")?
        .to_owned();
    let mut options = NetworkHelperOptions::new(
        image.clone(),
        "/opt/cellar-egress/bin/cellar-egress".into(),
        "127.0.0.11:53".parse()?,
    );
    if matches!(scenario, Scenario::InstallerFailure) {
        options.nft_path = "/missing-production-installer".into();
    }
    domain.services().configure_containers(EngineOptions {
        capacity: None,
        socket: campaign.docker.socket().into(),
        namespace: Some(format!("network-{}-{scenario:?}", campaign.owner)),
        runtime: Some("runc".into()),
        dns: Vec::new(),
        network_helper: Some(options),
    })?;
    let node = format!("network-{}", campaign.owner);
    let mut namespace = None;
    let result = domain
        .run(async {
            container::configure(node.clone(), None, directory.path().into())?;
            container::install_specs(
                vec![ContainerSpec {
                    class_name: "Workspace".into(),
                    image: campaign.app_image.clone(),
                    instance_type: Some("dev".into()),
                    max_instances: None,
                    runtime: None,
                    exposed_ports: vec![8080],
                }],
                Some(image.clone()),
                Vec::new(),
            );
            let engine = container::engine().await?;
            namespace = Some(engine.namespace().to_owned());
            // Publish recovery ownership before the first network/fence/start
            // effect, so the outer runner can clean up a killed test process.
            std::io::Write::write_all(
                &mut std::io::stdout(),
                format!(
                    "\nCELLAR_EGRESS_PRODUCTION_OWNER={}\n",
                    json!({"node":node, "host":engine.namespace()})
                )
                .as_bytes(),
            )?;
            let cell = engine.attach("Workspace:network", "Workspace").await?;
            let run = cell.begin_run();
            let monitor = engine.monitor(&cell, run);
            let started = engine
                .start(
                    &cell,
                    run,
                    StartParams {
                        entrypoint: Some(vec![
                            "node".into(),
                            "--input-type=module".into(),
                            "-e".into(),
                            include_str!("../../../../tools/sandbox/egress/service.mjs").into(),
                        ]),
                        env: Vec::new(),
                        enable_internet: false,
                        labels: Vec::new(),
                    },
                )
                .await;
            if matches!(scenario, Scenario::InstallerFailure) {
                ensure!(started.is_err(), "missing installer started an application");
                ensure!(
                    monitor.await.is_err(),
                    "failed startup reported a process exit"
                );
                ensure!(
                    owned(&campaign.docker, engine.namespace())
                        .await?
                        .is_empty(),
                    "failed startup leaked owned resources"
                );
                return Ok::<_, anyhow::Error>(());
            }
            started?;
            let resources = owned(&campaign.docker, engine.namespace()).await?;
            ensure!(
                resources.len() == 2,
                "production run did not own exactly its application and helper"
            );
            let helper = role(&resources, "helper")?;
            let app = role(&resources, "application")?;
            let info = campaign
                .docker
                .expect(
                    "GET",
                    &format!("/containers/{app}/json"),
                    None,
                    "inspect production namespace",
                )
                .await?
                .json()?;
            ensure!(
                info.pointer("/HostConfig/NetworkMode")
                    .and_then(Value::as_str)
                    == Some(format!("container:{helper}").as_str()),
                "application joined the wrong namespace"
            );
            ensure!(
                engine.reserved_memory_bytes() == 512 * 1024 * 1024,
                "helper reservation missing from host capacity"
            );
            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(1))
                .build()?;
            asyncrt_ready(&client, &cell).await?;
            if matches!(scenario, Scenario::HelperFailure) {
                campaign
                    .docker
                    .expect(
                        "POST",
                        &format!("/containers/{helper}/kill?signal=SIGKILL"),
                        None,
                        "fault owned helper",
                    )
                    .await?;
                ensure!(
                    tokio::time::timeout(Duration::from_secs(5), monitor)
                        .await?
                        .is_err(),
                    "helper failure was hidden by application cleanup"
                );
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        if owned(&campaign.docker, engine.namespace())
                            .await?
                            .is_empty()
                        {
                            break Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await??;
                ensure!(
                    !cell.running() && cell.address(8080).is_err(),
                    "failed helper retained usable application routing"
                );
            } else {
                cell.set_inactivity(Duration::from_secs(1));
                engine.release("Workspace:network", Release::Keep).await?;
                let adopted = engine.attach("Workspace:network", "Workspace").await?;
                ensure!(
                    Arc::ptr_eq(&adopted, &cell),
                    "warm reconnect replaced the run"
                );
                tokio::time::sleep(Duration::from_millis(1100)).await;
                asyncrt_ready(&client, &cell).await?;
                let current = owned(&campaign.docker, engine.namespace()).await?;
                ensure!(
                    role(&current, "helper")? == helper && role(&current, "application")? == app,
                    "reconnect changed immutable resources"
                );
                engine
                    .release("Workspace:network", Release::Destroy)
                    .await?;
                ensure!(
                    monitor.await.map_err(anyhow::Error::msg)? == 137,
                    "owned removal lost the original process exit"
                );
            }
            ensure!(
                owned(&campaign.docker, engine.namespace())
                    .await?
                    .is_empty(),
                "production cleanup left owned resources"
            );
            ensure!(
                engine.reserved_memory_bytes() == 0,
                "retired helper retained a capacity reservation"
            );
            Ok(())
        })
        .await;
    domain.shutdown().await;
    let cleanup = match namespace {
        Some(namespace) => cleanup(campaign, &image, &namespace).await,
        None => Ok(()),
    };
    result.with_context(|| format!("production network domain closed; cleanup={cleanup:?}"))??;
    cleanup
}

async fn asyncrt_ready(client: &reqwest::Client, cell: &container::CellContainer) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let address = cell.address(8080).map_err(anyhow::Error::msg)?;
            if let Ok(mut reply) = client.get(format!("http://{address}/ready")).send().await {
                use sha2::Digest;
                let status = reply.status();
                let remote = reply.remote_addr();
                let identified = reply.headers().get("x-cellar-network-fixture").is_some_and(|value| value=="1");
                let mut body=Vec::new();
                while let Some(chunk)=reply.chunk().await? {
                    ensure!(body.len()+chunk.len()<=4096,"oversized production fixture response");
                    body.extend_from_slice(&chunk);
                }
                ensure!(
                    status.is_success() && identified && body == b"owned-network",
                    "wrong production application response: address={address}, remote={remote:?}, status={status}, identified={identified}, bytes={}, digest={:x}",body.len(),sha2::Sha256::digest(&body)
                );
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

async fn owned(docker: &Docker, namespace: &str) -> Result<Vec<Value>> {
    let filters = json!({"label":[format!("cellar.host={namespace}")]}).to_string();
    let value = docker
        .expect(
            "GET",
            &format!(
                "/containers/json?all=true&filters={}",
                percent_encoding::utf8_percent_encode(&filters, percent_encoding::NON_ALPHANUMERIC)
            ),
            None,
            "inspect owned production resources",
        )
        .await?
        .json()?;
    let rows = value
        .as_array()
        .context("owned production list is not an array")?
        .clone();
    ensure!(
        rows.iter().all(
            |row| row.pointer("/Labels/cellar.host").and_then(Value::as_str) == Some(namespace)
        ),
        "production inventory included a foreign resource"
    );
    Ok(rows)
}

fn role(resources: &[Value], role: &str) -> Result<String> {
    let found = resources
        .iter()
        .filter(|row| {
            row.pointer("/Labels/cellar.network.role")
                .and_then(Value::as_str)
                == Some(role)
        })
        .collect::<Vec<_>>();
    ensure!(
        found.len() == 1,
        "production role did not have exactly one resource"
    );
    let id = found[0]["Id"].as_str().context("production resource ID")?;
    cellar::docker::ContainerId::new(id)?;
    Ok(id.into())
}

async fn cleanup(campaign: &mut Campaign, image: &str, namespace: &str) -> Result<()> {
    for resource in owned(&campaign.docker, namespace).await? {
        let id = resource["Id"].as_str().context("cleanup ID")?;
        cellar::docker::ContainerId::new(id)?;
        campaign
            .docker
            .expect(
                "DELETE",
                &format!("/containers/{id}?force=true"),
                None,
                "clean owned production resource",
            )
            .await?;
    }
    let filters = json!({"label":[format!("cellar.host={namespace}")]}).to_string();
    let networks = campaign
        .docker
        .expect(
            "GET",
            &format!(
                "/networks?filters={}",
                percent_encoding::utf8_percent_encode(&filters, percent_encoding::NON_ALPHANUMERIC)
            ),
            None,
            "list owned production networks",
        )
        .await?
        .json()?;
    for network in networks.as_array().context("network list")? {
        ensure!(
            network
                .pointer("/Labels/cellar.host")
                .and_then(Value::as_str)
                == Some(namespace),
            "foreign network in cleanup"
        );
        let id = network["Id"].as_str().context("network ID")?;
        cellar::docker::ContainerId::new(id)?;
        campaign
            .docker
            .expect(
                "DELETE",
                &format!("/networks/{id}"),
                None,
                "remove owned production network",
            )
            .await?;
    }
    let table = format!("cellar_{}", &namespace[..24]);
    let id=campaign.create(json!({"Image":image,"Entrypoint":["/usr/sbin/nft"],"Cmd":["delete","table","inet",table],
        "HostConfig":{"NetworkMode":"host","Runtime":"runc","CapDrop":["ALL"],"CapAdd":["NET_ADMIN"]}})).await?;
    campaign
        .docker
        .expect(
            "POST",
            &format!("/containers/{id}/start"),
            None,
            "remove owned production fence",
        )
        .await?;
    let result = campaign
        .docker
        .expect(
            "POST",
            &format!("/containers/{id}/wait"),
            None,
            "join fence cleanup",
        )
        .await?
        .json()?;
    // A profile rejected before fence installation has no table to remove.
    ensure!(
        result["StatusCode"] == 0,
        "owned production fence cleanup failed"
    );
    Ok(())
}
