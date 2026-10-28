// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// External fault qualification owns its process namespace, filesystem and clock.
#![allow(clippy::disallowed_methods)]

#[cfg(target_os = "linux")]
mod qualification {
    use anyhow::{ensure, Context, Result};
    use cellar_egress::{control, transport, Destination, Host, HttpTarget};
    use std::{
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        process::Stdio,
        time::Duration,
    };
    use tokio::process::Command;

    fn fixture(startup: bool) -> Result<(PathBuf, PathBuf)> {
        let directory = PathBuf::from(if startup {
            "/tmp/cellar-egress-startup-fault"
        } else {
            "/tmp/cellar-egress-retirement-fault"
        });
        std::fs::create_dir(&directory)?;
        let first = directory.join("first");
        let pid = directory.join("pid");
        let executable = directory.join("installer");
        let initial = if startup {
            String::new()
        } else {
            format!(
                "if [ ! -f '{}' ]; then\n  : > '{}'\n  exec /usr/sbin/nft \"$@\"\nfi\n",
                first.display(),
                first.display()
            )
        };
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\n{initial}echo $$ > '{}'\nexec /bin/sleep 60\n",
                pid.display()
            ),
        )?;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))?;
        Ok((executable, pid))
    }

    async fn started(path: &Path) -> Result<u32> {
        loop {
            match std::fs::read_to_string(path) {
                Ok(pid) if !pid.trim().is_empty() => return Ok(pid.trim().parse()?),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn run(startup: bool) -> Result<()> {
        let (installer_path, pid_path) = fixture(startup)?;
        let executable = std::env::current_exe()?
            .parent()
            .and_then(|path| path.parent())
            .context("missing helper binary directory")?
            .join("cellar-egress");
        let session = "52".repeat(32);
        let mut child = Command::new(executable)
            .args([
                "--session",
                &session,
                "--dns",
                "127.0.0.3:53",
                "--nft-path",
                installer_path.to_str().context("invalid fixture path")?,
                "--install-timeout-ms",
                "500",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let io = tokio::io::join(
            child.stdout.take().context("missing helper stdout")?,
            child.stdin.take().context("missing helper stdin")?,
        );
        let (link, mut incoming, driver) = transport::session(io, transport::Role::Host);
        let (installer, routes, controller) = control::channel::<String>(link.clone(), [0x52; 32]);
        let helper = child.wait_with_output();
        let native = async {
            let Some(stream) = incoming.next().await else {
                ensure!(startup, "helper omitted an expected control stream");
                drop(controller);
                return Ok(());
            };
            ensure!(
                !startup,
                "startup admitted work before firewall installation"
            );
            controller.run(stream).await
        };
        let application = async {
            if startup {
                ensure!(
                    installer.ready().await.is_err(),
                    "uninstalled firewall reported ready"
                );
            } else {
                installer.ready().await?;
                let mut install = Box::pin(installer.install(
                    HttpTarget::parse("never.internal")?,
                    "uncommitted".to_owned(),
                ));
                ensure!(futures_util::poll!(install.as_mut()).is_pending());
                started(&pid_path).await?;
                let host = Host::parse("never.internal")?;
                let destination = Destination {
                    socket: "127.0.0.2:80".parse()?,
                    host: Some(&host),
                };
                ensure!(
                    routes.resolve(&destination).is_none(),
                    "unacknowledged handler became visible"
                );
                routes.retire();
                ensure!(
                    install.await.is_err(),
                    "retired installation reported success"
                );
                ensure!(
                    routes.resolve(&destination).is_none(),
                    "retirement revived pending handler"
                );
            }
            Ok::<_, anyhow::Error>(())
        };
        let (helper, driver, native, application) =
            tokio::join!(helper, driver.run(), native, application);
        application?;
        native?;
        driver?;
        let helper = helper?;
        let pid = started(&pid_path).await?;
        ensure!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "firewall installer was killed but not reaped"
        );
        if startup {
            ensure!(!helper.status.success());
            ensure!(
                String::from_utf8_lossy(&helper.stderr).contains("firewall installation timed out"),
                "unexpected startup failure"
            );
        } else {
            ensure!(
                helper.status.success(),
                "retirement did not join successfully: {}",
                String::from_utf8_lossy(&helper.stderr)
            );
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    use std::io::Write;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            qualification::run(true).await?;
            qualification::run(false).await
        })
        .await?
    })?;
    std::io::stdout().write_all(b"{\"startup_pre_admission\":true,\"entered_installer_retirement\":true,\"unacknowledged_route_refusal\":true,\"installer_reaped\":true}\n")?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("helper retirement qualification requires Linux NET_ADMIN");
}
