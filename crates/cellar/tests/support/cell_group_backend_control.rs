// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An explicit test-only handshake with the runner that owns the RustFS container.
use super::*;
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    path::PathBuf,
};

#[derive(Serialize)]
struct Request {
    format: u8,
    sequence: u8,
    mode: &'static str,
    action: &'static str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    format: u8,
    sequence: u8,
    mode: String,
    action: String,
    running: bool,
}

pub(super) struct Control {
    store: Arc<archive_controller::archive_store::Store>,
    directory: Option<PathBuf>,
    rooted: bool,
}

impl Control {
    pub(super) fn new(
        store: Arc<archive_controller::archive_store::Store>,
        directory: Option<PathBuf>,
        rooted: bool,
    ) -> Self {
        Self {
            store,
            directory,
            rooted,
        }
    }

    pub(super) async fn set_running(&self, domain: &ExecutionDomain, running: bool) -> Result<()> {
        let Some(directory) = self.directory.clone() else {
            self.store.outage.store(!running, Ordering::Release);
            return Ok(());
        };
        let sequence = u8::from(self.rooted) * 2 + u8::from(running);
        let mode = if self.rooted {
            "extents"
        } else {
            "materialized"
        };
        let action = if running { "start" } else { "stop" };
        let encoded = serde_json::to_vec(&Request {
            format: 1,
            sequence,
            mode,
            action,
        })?;
        let response = directory.join(format!("{sequence:02}-response.json"));
        domain
            .blocking(move || -> Result<()> {
                anyhow::ensure!(
                    directory.is_absolute() && directory.is_dir(),
                    "explicit control directory"
                );
                let pending = directory.join(format!("{sequence:02}-request.pending"));
                let request = directory.join(format!("{sequence:02}-request.json"));
                let mut file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&pending)?;
                file.write_all(&encoded)?;
                file.sync_all()?;
                // Atomic create, never replacement of another operation's request.
                std::fs::hard_link(&pending, request)?;
                std::fs::remove_file(pending)?;
                std::fs::File::open(directory)?.sync_all()?;
                Ok(())
            })?
            .await??;
        let deadline = domain
            .run(async { cellar::asyncrt::mono_ms().saturating_add(30_000) })
            .await?;
        loop {
            let path = response.clone();
            let reply = domain
                .blocking(move || -> Result<Option<Response>> {
                    let file = match std::fs::File::open(path) {
                        Ok(file) => file,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Ok(None)
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let mut bytes = Vec::new();
                    file.take(4097).read_to_end(&mut bytes)?;
                    anyhow::ensure!(bytes.len() <= 4096, "bounded control reply");
                    Ok(Some(serde_json::from_slice(&bytes)?))
                })?
                .await??;
            if let Some(reply) = reply {
                anyhow::ensure!(
                    reply.format == 1
                        && reply.sequence == sequence
                        && reply.mode == mode
                        && reply.action == action
                        && reply.running == running,
                    "exact backend transition reply"
                );
                return Ok(());
            }
            domain
                .run(async {
                    anyhow::ensure!(
                        cellar::asyncrt::mono_ms() < deadline,
                        "backend control deadline"
                    );
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                    Ok::<_, anyhow::Error>(())
                })
                .await??;
        }
    }
}
