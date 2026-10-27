// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::Config;
use crate::{dns, firewall::Program, HttpTarget, Rules};
use anyhow::{ensure, Context, Result};
use std::{
    process::Stdio,
    sync::{Arc, Mutex},
};
use tokio::{io::AsyncWriteExt, process::Command};

#[derive(Clone)]
pub(super) struct Policy {
    pub config: Config,
    rules: Arc<Mutex<Rules<()>>>,
}

impl Policy {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            rules: Arc::new(Mutex::new(Rules::default())),
        }
    }

    pub fn resolve(&self, packet: &[u8]) -> Result<dns::Disposition> {
        dns::resolve(packet, &self.rules.lock().unwrap(), self.config.internet)
    }

    pub async fn install(&self, targets: Vec<HttpTarget>) -> Result<()> {
        let program = Program::new(
            &targets,
            self.config.http_port,
            self.config.dns_port,
            self.config.internet,
        )?;
        let mut rules = Rules::default();
        for target in targets {
            let prepared = rules.prepare(target, ())?;
            rules.commit(prepared)?;
        }
        apply(&program, self.config.install_timeout, &self.config.nft_path).await?;
        *self.rules.lock().unwrap() = rules;
        Ok(())
    }
}

async fn apply(
    program: &Program,
    timeout: std::time::Duration,
    executable: &std::path::Path,
) -> Result<()> {
    let mut child = Command::new(executable)
        .env_clear()
        .args(["-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start namespace firewall installer")?;
    let mut input = child.stdin.take().context("firewall stdin missing")?;
    let result = tokio::time::timeout(timeout, async {
        input.write_all(program.as_str().as_bytes()).await?;
        input.shutdown().await?;
        drop(input);
        child.wait().await
    })
    .await;
    match result {
        Ok(Ok(status)) => {
            ensure!(status.success(), "namespace firewall installation failed");
            Ok(())
        }
        Ok(Err(error)) => {
            kill_and_join(&mut child).await?;
            Err(error.into())
        }
        Err(_) => {
            kill_and_join(&mut child).await?;
            anyhow::bail!("namespace firewall installation timed out")
        }
    }
}

async fn kill_and_join(child: &mut tokio::process::Child) -> Result<()> {
    // Joining is part of normal error/timeout shutdown. Kill-on-drop is only
    // a process-cancellation fallback, never proof of completed cleanup.
    let _ = child.start_kill();
    child
        .wait()
        .await
        .context("join namespace firewall installer")?;
    Ok(())
}
