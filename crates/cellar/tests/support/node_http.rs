// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Two real embedded nodes with application-owned egress configuration.

use super::{client, graph, inputs, ready, response, stopped, Account, Executor};
use anyhow::{ensure, Result};
use cellar::sdk::*;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Semaphore};

struct Server(tokio::task::JoinHandle<()>);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn applications(origin: String) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let origin = origin.clone();
        Ok(Arc::new(
            Registry::new(move |context, event| {
                let origin = origin.clone();
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        return Ok(Output::Done);
                    };
                    let path = request.url.rsplit('/').next().unwrap();
                    let result = async {
                        let reply = context
                            .fetch(http::OutboundRequest::get(format!("{origin}/{path}")))
                            .await?;
                        http::Response::from_http(reply).text(&context).await
                    }
                    .await;
                    let message = result.unwrap_or_else(|error| format!("failed:{error}"));
                    Ok(response(message))
                })
            })
            .cell("Account", |_, _| {
                Ok(Arc::new(Account {
                    label: "unused",
                    signals: None,
                }))
            })?,
        ))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_hosts_capture_http_clients_and_separate_deadlines() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let origin = format!("http://sdk-http.test:{}", address.port());
    let (entered, mut observed) = mpsc::unbounded_channel();
    let long_release = Arc::new(Semaphore::new(0));
    let release = long_release.clone();
    let _server = Server(tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        while let Ok((mut socket, _)) = listener.accept().await {
            let entered = entered.clone();
            let release = release.clone();
            connections.spawn(async move {
                let mut bytes = Vec::new();
                let mut byte = [0; 1];
                while !bytes.ends_with(b"\r\n\r\n") {
                    if socket.read(&mut byte).await.unwrap_or(0) == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&byte);
                }
                let headers = String::from_utf8(bytes).unwrap();
                let label = if headers.contains("x-cellar-host: short") {
                    "short"
                } else if headers.contains("x-cellar-host: long") {
                    "long"
                } else {
                    panic!("request bypassed the caller's HTTP factory: {headers}");
                };
                let response_head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    label.len()
                );
                let body_only = headers.starts_with("GET /body ");
                if body_only {
                    socket.write_all(response_head.as_bytes()).await.unwrap();
                }
                if body_only || headers.starts_with("GET /held ") {
                    entered.send(label).unwrap();
                    if label == "short" {
                        let _ = socket.read(&mut byte).await;
                        return;
                    }
                    release.acquire().await.unwrap().forget();
                }
                if !body_only {
                    let _ = socket.write_all(response_head.as_bytes()).await;
                }
                let _ = socket.write_all(label.as_bytes()).await;
            });
            while let Some(result) = connections.try_join_next() {
                result.expect("HTTP fixture connection panicked");
            }
        }
    }));
    let directory = tempfile::tempdir()?;
    let core = Executor::new("caller-http-core")?;
    let mut nodes = Vec::new();
    for (label, timeout) in [
        ("short", Duration::from_millis(250)),
        ("long", Duration::from_secs(10)),
    ] {
        let (mut config, mut services) =
            inputs(label, &directory.path().join(label), &core, None, 0).await?;
        config.http.request_timeout = timeout;
        services.http_client = Some(HttpClientFactory::new(move || {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("x-cellar-host", label.parse().unwrap());
            reqwest::Client::builder()
                .no_proxy()
                .resolve("sdk-http.test", address)
                .default_headers(headers)
        }));
        let node = Node::start(config, services, graph(), applications(origin.clone())?).await?;
        ready(&node).await?;
        ensure!(
            node.services().http_policy().request_timeout == timeout,
            "node did not capture its HTTP deadline"
        );
        nodes.push(node);
    }
    let client = client()?;
    // Hold the response head, then hold only its body. Both phases must use
    // the originating host's deadline through the actual SDK stream adapter.
    for path in ["held", "body"] {
        let mut pending = Vec::new();
        for node in &nodes {
            let client = client.clone();
            let url = format!("http://{}/{path}", node.info().public_address);
            pending.push(tokio::spawn(async move {
                client
                    .get(url)
                    .send()
                    .await?
                    .error_for_status()?
                    .text()
                    .await
            }));
        }
        let mut arrivals = Vec::new();
        for _ in 0..2 {
            arrivals.push(
                tokio::time::timeout(Duration::from_secs(3), observed.recv())
                    .await?
                    .unwrap(),
            );
        }
        arrivals.sort();
        ensure!(
            arrivals == ["long", "short"],
            "both hosts must reach the supplied DNS target with their own headers"
        );
        let long = pending.pop().unwrap();
        let short =
            tokio::time::timeout(Duration::from_secs(3), pending.pop().unwrap()).await???;
        ensure!(
            short.starts_with(if path == "held" {
                "failed:fetch:"
            } else {
                "failed:response stream:"
            }),
            "short request did not reach its HTTP deadline at {path}: {short}"
        );
        ensure!(
            !long.is_finished(),
            "short deadline affected another host's pending request"
        );
        let answer = client
            .get(format!("http://{}/ok", nodes[0].info().public_address))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        ensure!(
            answer == "short",
            "a timed-out request disabled subsequent egress"
        );
        long_release.add_permits(1);
        ensure!(
            tokio::time::timeout(Duration::from_secs(3), long).await??? == "long",
            "long request did not complete with its own pool"
        );
    }
    for node in nodes {
        stopped(node).await?;
    }
    ensure!(
        core.handle().spawn(async { true }).await?,
        "HTTP node shutdown stopped the caller's core executor"
    );
    ensure!(
        tokio::spawn(async { true }).await?,
        "HTTP node shutdown stopped the caller's host executor"
    );
    Ok(())
}
