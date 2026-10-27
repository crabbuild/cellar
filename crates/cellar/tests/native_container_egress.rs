// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual Engine attach and helper-first network namespace qualification.
//! Run through tools/sandbox/egress/qualify.py; no implicit daemon discovery.
#![allow(clippy::disallowed_methods)] // Finite external campaign owns and joins all work.

use anyhow::{ensure, Context, Result};
use cellar::docker::Docker;
use cellar_egress::{
    control,
    docker_io::DockerIo,
    transport::{self, Stream},
    wire::{self, Open},
    Destination, Host, HttpTarget,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SESSION: [u8; 32] = [83; 32];
const REQUEST: &[u8] =
    b"GET /gateway HTTP/1.1\r\nHost: backup.internal\r\nConnection: close\r\n\r\n";
const REPLY: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nengine-native";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Success,
    InstallerFailure,
    RetireDuringRequest,
}

struct Retirement<T>(control::Routes<T>);

impl<T> Drop for Retirement<T> {
    fn drop(&mut self) {
        self.0.retire();
    }
}

struct Campaign {
    docker: Docker,
    helper_image: String,
    app_image: String,
    network: String,
    runtime: String,
    owner: String,
    created: Vec<String>,
}

impl Campaign {
    fn configured() -> Result<Self> {
        let configured = |name| std::env::var(name).with_context(|| format!("missing {name}"));
        let owner = configured("CELLAR_EGRESS_ENGINE_OWNER")?;
        let runtime = configured("CELLAR_EGRESS_ENGINE_RUNTIME")?;
        ensure!(
            runtime == "runc",
            "this qualification profile requires runc"
        );
        ensure!(owner.len() == 12 && owner.bytes().all(|byte| byte.is_ascii_hexdigit()));
        Ok(Self {
            docker: Docker::new(configured("CELLAR_EGRESS_ENGINE_SOCKET")?),
            helper_image: configured("CELLAR_EGRESS_ENGINE_IMAGE")?,
            app_image: configured("CELLAR_EGRESS_APP_IMAGE")?,
            network: configured("CELLAR_EGRESS_ENGINE_NETWORK")?,
            runtime,
            owner,
            created: Vec::new(),
        })
    }

    async fn create(&mut self, mut body: Value) -> Result<String> {
        body["Labels"] = json!({"cellar.egress.qualifier": self.owner});
        let reply = self
            .docker
            .expect(
                "POST",
                "/containers/create",
                Some(body),
                "create qualification container",
            )
            .await?
            .json()?;
        let id = reply["Id"]
            .as_str()
            .context("Engine omitted container ID")?
            .to_owned();
        ensure!(id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        self.created.push(id.clone());
        Ok(id)
    }

    async fn remove_all(&self) -> Result<()> {
        let mut errors = Vec::new();
        for id in self.created.iter().rev() {
            match tokio::time::timeout(
                Duration::from_secs(5),
                self.docker
                    .call("DELETE", &format!("/containers/{id}?force=true"), None),
            )
            .await
            {
                Ok(Ok(reply)) if reply.status.is_success() || reply.status.as_u16() == 404 => {}
                Ok(Ok(reply)) => errors.push(format!("remove {id}: {}", reply.status)),
                Ok(Err(error)) => errors.push(format!("remove {id}: {error}")),
                Err(_) => errors.push(format!("remove {id}: timed out")),
            }
        }
        ensure!(
            errors.is_empty(),
            "owned resource cleanup failed: {errors:?}"
        );
        Ok(())
    }

    async fn run(&mut self, mode: Mode) -> Result<()> {
        let fail_install = mode == Mode::InstallerFailure;
        let session: String = SESSION.iter().map(|byte| format!("{byte:02x}")).collect();
        let mut command = vec![
            "--session".to_owned(),
            session,
            "--internet".into(),
            "false".into(),
            "--dns".into(),
            "127.0.0.11:53".into(),
        ];
        if fail_install {
            command.extend(["--nft-path".into(), "/missing-qualification-nft".into()]);
        }
        let helper = self.create(json!({
            "Image": self.helper_image,
            "Entrypoint": ["/opt/cellar-egress/bin/cellar-egress"],
            "Cmd": command,
            "OpenStdin": true, "StdinOnce": true, "AttachStdin": true,
            "AttachStdout": true, "AttachStderr": true, "Tty": false,
            "HostConfig": {
                "NetworkMode": self.network, "Runtime": self.runtime,
                "CapDrop": ["ALL"], "CapAdd": ["NET_ADMIN"],
                "SecurityOpt": ["no-new-privileges"], "PidsLimit": 128,
                "Memory": 268435456, "MemorySwap": 268435456,
                "Sysctls": {"net.ipv4.ip_unprivileged_port_start": "0", "net.ipv6.conf.all.disable_ipv6": "0", "net.ipv6.conf.lo.disable_ipv6": "0"}
            }
        })).await?;
        // Attach before starting: readiness cannot disappear into daemon logs,
        // and closing this exact connection retires only this helper run.
        let io = self.docker.attach_io(&helper).await?;
        let info = self
            .docker
            .expect(
                "GET",
                &format!("/containers/{helper}/json"),
                None,
                "inspect unstarted helper",
            )
            .await?
            .json()?;
        ensure!(info.pointer("/State/Running").and_then(Value::as_bool) == Some(false));
        let (link, mut incoming, driver) =
            transport::session(DockerIo::new(io), transport::Role::Host);
        let (installer, routes, controller) = control::channel(link, SESSION);
        let native_routes = routes.clone();
        let native = async {
            let _retire = Retirement(native_routes.clone());
            let Some(control) = incoming.next().await else {
                ensure!(fail_install, "helper omitted readiness");
                return Ok::<_, anyhow::Error>(());
            };
            ensure!(!fail_install, "failed installation announced readiness");
            let data = async {
                let stream = incoming
                    .next()
                    .await
                    .context("helper omitted intercepted stream")?;
                gateway(stream, &native_routes, mode).await?;
                ensure!(
                    incoming.next().await.is_none(),
                    "unexpected extra intercepted connection"
                );
                Ok::<_, anyhow::Error>(())
            };
            let (control, data) = tokio::join!(controller.run(control), data);
            control?;
            data?;
            Ok(())
        };
        let application = async {
            let _retire = Retirement(routes.clone());
            self.docker
                .expect(
                    "POST",
                    &format!("/containers/{helper}/start"),
                    None,
                    "start helper",
                )
                .await?;
            let ready = installer.ready().await;
            if fail_install {
                ensure!(ready.is_err(), "failed helper was ready");
                ensure!(
                    self.created.len() == 1,
                    "application allocated before readiness"
                );
            } else {
                ready?;
                installer
                    .install(
                        HttpTarget::parse("backup.internal")?,
                        "native-handler".to_owned(),
                    )
                    .await?;
                // The actual second container shares this exact helper's
                // namespace. DNS and application TCP cross real nft rules.
                let script = include_str!("../../../tools/sandbox/egress/application.mjs");
                let app = self.create(json!({
                    "Image": self.app_image, "Entrypoint": ["node"], "Cmd": ["--input-type=module", "-e", script],
                    "Env": [format!("CELLAR_EGRESS_EXPECT_CLOSED={}", u8::from(mode == Mode::RetireDuringRequest))],
                    "HostConfig": {"NetworkMode": format!("container:{helper}"), "Runtime": self.runtime, "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges"], "PidsLimit": 64, "Memory": 67108864, "MemorySwap": 67108864}
                })).await?;
                self.docker
                    .expect(
                        "POST",
                        &format!("/containers/{app}/start"),
                        None,
                        "start application",
                    )
                    .await?;
                let exit = self
                    .docker
                    .expect(
                        "POST",
                        &format!("/containers/{app}/wait"),
                        None,
                        "join application",
                    )
                    .await?
                    .json()?;
                ensure!(
                    exit["StatusCode"] == 0,
                    "application DNS/HTTP round-trip failed: {exit}"
                );
            }
            routes.retire();
            let exit = self
                .docker
                .expect(
                    "POST",
                    &format!("/containers/{helper}/wait"),
                    None,
                    "join helper",
                )
                .await?
                .json()?;
            if fail_install {
                ensure!(exit["StatusCode"] == 1, "invalid helper startup succeeded");
                let logs = self
                    .docker
                    .expect(
                        "GET",
                        &format!("/containers/{helper}/logs?stdout=true&stderr=true"),
                        None,
                        "inspect known startup refusal",
                    )
                    .await?;
                ensure!(
                    String::from_utf8_lossy(&logs.body)
                        .contains("start namespace firewall installer"),
                    "helper exited for an unrelated reason"
                );
            } else {
                ensure!(
                    exit["StatusCode"] == 0,
                    "helper retirement did not join successfully: {exit}"
                );
            }
            Ok::<_, anyhow::Error>(())
        };
        let (driver, native, application) = tokio::join!(driver.run(), native, application);
        application?;
        native?;
        if let Err(error) = driver {
            ensure!(
                fail_install
                    && format!("{error:#}").contains("container network helper wrote stderr"),
                "unexpected private transport failure: {error:#}"
            );
        }
        Ok(())
    }
}

async fn gateway(mut stream: Stream, routes: &control::Routes<String>, mode: Mode) -> Result<()> {
    let Open::Intercept { destination } = wire::read_open(&mut stream).await? else {
        anyhow::bail!("unexpected helper stream");
    };
    ensure!(destination.port() == 80);
    let mut request = vec![0; REQUEST.len()];
    stream.read_exact(&mut request).await?;
    ensure!(request == REQUEST, "application request changed");
    let host = Host::parse("backup.internal")?;
    let handler: Arc<String> = routes
        .resolve(&Destination {
            socket: destination,
            host: Some(&host),
        })
        .context("uncommitted helper route")?;
    ensure!(handler.as_str() == "native-handler");
    if mode == Mode::RetireDuringRequest {
        routes.retire();
        ensure!(
            stream.write_all(REPLY).await.is_err(),
            "retired private stream accepted response bytes"
        );
        return Ok(());
    }
    stream.write_all(REPLY).await?;
    stream.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit owned Engine image/network configuration"]
async fn exact_engine_attach_gates_application_start_and_joins_helper() -> Result<()> {
    for mode in [
        Mode::Success,
        Mode::InstallerFailure,
        Mode::RetireDuringRequest,
    ] {
        let mut campaign = Campaign::configured()?;
        let result = tokio::time::timeout(Duration::from_secs(30), campaign.run(mode)).await;
        let cleanup = campaign.remove_all().await;
        result.with_context(|| {
            format!("Engine helper campaign exceeded deadline ({mode:?}), cleanup={cleanup:?}")
        })??;
        cleanup?;
    }
    std::io::Write::write_all(
        &mut std::io::stdout(),
        b"\nCELLAR_EGRESS_ENGINE_RESULT={\"exact_id_attach\":true,\"helper_first\":true,\"real_application_namespace\":true,\"startup_failure_no_application\":true,\"inflight_retirement\":true,\"joined_helper\":true}\n",
    )?;
    Ok(())
}
