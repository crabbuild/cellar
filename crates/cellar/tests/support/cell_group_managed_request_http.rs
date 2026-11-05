// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual native and Axum ingress for the shared counter application.
use super::*;

const SECRET: &str = "managed-counter-fixture-bearer";

pub(super) fn secret() -> String {
    SECRET.into()
}

pub(super) struct Reply {
    pub status: u16,
    body: String,
    label: String,
}

impl Reply {
    pub fn refused(self, status: u16, body: &str) -> Result<()> {
        ensure!(
            self.status == status && self.body == body,
            "{}: expected HTTP {status} ({body}), got {} ({})",
            self.label,
            self.status,
            self.body
        );
        Ok(())
    }

    pub fn success(self) -> Result<serde_json::Value> {
        ensure!(
            self.status == 200,
            "{}: HTTP {}: {}",
            self.label,
            self.status,
            self.body
        );
        Ok(serde_json::from_str(&self.body)?)
    }
}

pub(super) struct Web {
    native: Vec<String>,
    axum: Vec<String>,
    stop: tokio::sync::watch::Sender<bool>,
    tasks: Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl Web {
    pub async fn start(nodes: &[Option<Node>], apis: &[Arc<counter::CounterApi>]) -> Result<Self> {
        let (stop, _) = tokio::sync::watch::channel(false);
        let mut web = Self {
            native: vec![],
            axum: vec![],
            stop,
            tasks: vec![],
        };
        let started = async {
            for (node, api) in nodes.iter().zip(apis) {
                web.native.push(format!(
                    "http://{}",
                    node.as_ref()
                        .context("live HTTP node")?
                        .info()
                        .public_address
                ));
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                web.axum.push(format!("http://{}", listener.local_addr()?));
                let mut stopping = web.stop.subscribe();
                let router = counter::router(api.clone());
                web.tasks.push(tokio::spawn(async move {
                    axum::serve(listener, router)
                        .with_graceful_shutdown(async move {
                            let _ = stopping.changed().await;
                        })
                        .await
                }));
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = started {
            let joined = web.close().await;
            return Err(match joined {
                Ok(()) => error,
                Err(join) => error.context(format!("partial HTTP startup join: {join:?}")),
            });
        }
        Ok(web)
    }

    fn url(&self, node: usize, axum: bool, name: &str) -> String {
        let base = if axum {
            &self.axum[node]
        } else {
            &self.native[node]
        };
        format!("{base}/counters/{name}")
    }

    async fn send(request: reqwest::RequestBuilder, label: String) -> Result<Reply> {
        let response = request.bearer_auth(SECRET).send().await?;
        let status = response.status().as_u16();
        let body = response.text().await?;
        Ok(Reply {
            status,
            body,
            label,
        })
    }

    pub async fn command(
        &self,
        client: &reqwest::Client,
        (node, axum): (usize, bool),
        name: &str,
        delta: i64,
        sequence: u64,
        retry: u64,
    ) -> Result<Reply> {
        Self::send(
            client
                .post(format!("{}?delta={delta}", self.url(node, axum, name)))
                .header("x-command-id", format!("{}:{sequence}", "c7".repeat(16)))
                .header("x-retry-until-ms", retry),
            format!("command node={node} axum={axum} cell={name} sequence={sequence}"),
        )
        .await
    }

    pub async fn query(
        &self,
        client: &reqwest::Client,
        (node, axum): (usize, bool),
        name: &str,
        mode: &str,
        receipt: &serde_json::Value,
        remote: Option<usize>,
    ) -> Result<Reply> {
        let mut request = client
            .get(self.url(node, axum, name))
            .header("x-read-consistency", mode);
        if mode == "at-least" {
            request = request.header(
                "x-min-version",
                receipt["version_token"]
                    .as_str()
                    .context("opaque receipt token")?,
            );
        }
        if let Some(remote) = remote {
            request = request
                .header("x-query-members", remote as u64 + 1)
                .header("x-query-attempt-ms", "4000");
        }
        Self::send(
            request,
            format!("query node={node} axum={axum} cell={name} mode={mode} remote={remote:?}"),
        )
        .await
    }

    pub async fn close(self) -> Result<()> {
        let _ = self.stop.send(true);
        let results = futures_util::future::join_all(self.tasks).await;
        for result in results {
            result??;
        }
        Ok(())
    }
}

pub(super) fn value(reply: &serde_json::Value, name: &str, expected: i64) -> Result<()> {
    ensure!(reply["cell"].as_str() == Some(format!("Counter:{name}").as_str()));
    ensure!(
        reply["value"].as_i64() == Some(expected),
        "wrong business result: {reply}"
    );
    Ok(())
}

pub(super) fn same_receipt(
    original: &serde_json::Value,
    duplicate: &serde_json::Value,
) -> Result<()> {
    for field in ["value", "cell", "incarnation"] {
        ensure!(
            original[field] == duplicate[field],
            "duplicate changed {field}"
        );
    }
    // The selected image may cover a historical receipt. Its version is an
    // observation floor, not the client's immutable command sequence.
    ensure!(
        duplicate["sequence"]
            .as_u64()
            .context("duplicate image version")?
            >= original["sequence"]
                .as_u64()
                .context("original image version")?
    );
    Ok(())
}
