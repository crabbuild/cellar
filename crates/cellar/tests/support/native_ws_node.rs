use anyhow::{bail, Context as _, Result};
use reqwest::Client;
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

pub struct Node {
    pub child: Child,
    pub url: String,
    pub internal: String,
    pub log: std::path::PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Node {
    pub async fn start(
        root: &Path,
        name: &str,
        program: &str,
        peer: &str,
        source: Option<&Path>,
    ) -> Result<Self> {
        Self::launch(root, name, program, peer, source, None).await
    }

    #[allow(dead_code)] // Shared by the separate fleet binding harness.
    pub async fn start_fleet(
        root: &Path,
        name: &str,
        program: &str,
        bucket: &Path,
    ) -> Result<Self> {
        Self::launch(root, name, program, "ws://127.0.0.1:1", None, Some(bucket)).await
    }

    async fn launch(
        root: &Path,
        name: &str,
        program: &str,
        peer: &str,
        source: Option<&Path>,
        bucket: Option<&Path>,
    ) -> Result<Self> {
        let log = root.join(format!("{name}.log"));
        let stdout = std::fs::File::create(&log)?;
        let mut command = Command::new(program);
        command
            .env_clear()
            .args([
                "--listen",
                "127.0.0.1:0",
                "--internal-listen",
                "127.0.0.1:0",
            ])
            .env("CELLAR_WEBSOCKET_PEER", peer)
            .env("CELLAR_NODE", name)
            .env("CELLAR_TEST_DATA_DIR", root.join(format!("{name}-data")))
            .env("CELLAR_TOKIO_THREADS", "2")
            .env("CELLAR_SHUTDOWN_TOTAL_MS", "5000")
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(stdout.try_clone()?))
            .stderr(Stdio::from(stdout));
        if let Some(bucket) = bucket {
            command
                .env("CELLAR_BUCKET", "cellar-dev")
                .env("CELLAR_INTERNAL_DEV_STORE", bucket)
                .env("CELLAR_DURABILITY", "bucket")
                .env("CELLAR_PLACEMENT_WEIGHT", "1")
                .env("CELLAR_REBALANCE_INTERVAL_MS", "0");
        }
        if let Some(source) = source {
            command.env("CELLAR_TEST_SCRIPT_PATH", source);
        }
        let mut node = Self {
            child: command.spawn()?,
            url: String::new(),
            internal: String::new(),
            log,
        };
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let text = std::fs::read_to_string(&node.log)?;
                let address = |prefix: &str| {
                    text.lines()
                        .find_map(|line| {
                            line.strip_prefix(prefix)
                                .and_then(|rest| rest.split_whitespace().next())
                        })
                        .map(|addr| format!("http://{addr}"))
                };
                if let (Some(url), Some(internal)) = (
                    address("cellar listening on "),
                    address("cellar internal listening on "),
                ) {
                    node.url = url;
                    node.internal = internal;
                    return Ok::<_, anyhow::Error>(());
                }
                if let Some(status) = node.child.try_wait()? {
                    bail!("{name} exited {status}: {text}");
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("daemon boot timeout")??;
        Ok(node)
    }

    pub async fn stop(&mut self, client: &Client) -> Result<()> {
        client
            .post(format!("{}/shutdown", self.internal))
            .send()
            .await?
            .error_for_status()?;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(status) = self.child.try_wait()? {
                    anyhow::ensure!(
                        status.success(),
                        "daemon failed: {}",
                        std::fs::read_to_string(&self.log)?
                    );
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await??;
        Ok(())
    }
}
