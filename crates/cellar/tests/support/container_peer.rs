// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Engine API fixture with real HTTP, TCP and RFC6455 peers.
#![allow(dead_code)]
#[path = "container_engine.rs"]
mod engine;
use anyhow::Result;
use engine::Engine;
use fastwebsockets::{Frame, OpCode};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::{
    body::{Bytes, Incoming},
    Request, Response,
};
use hyper_util::rt::TokioIo;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::{JoinHandle, JoinSet},
};

pub struct Peer {
    pub engine: Arc<Engine>,
    pub socket: PathBuf,
    pub websockets: Arc<AtomicUsize>,
    pub pending_http: Arc<AtomicUsize>,
    tasks: Vec<JoinHandle<()>>,
    _directory: tempfile::TempDir,
}

impl Drop for Peer {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct Connected(Arc<AtomicUsize>);

impl Drop for Connected {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Peer {
    pub async fn start() -> Result<Self> {
        Self::start_on_port(None).await
    }

    /// Published applications keep their declared container port. Linux reaches
    /// that port on the inspected IP; other hosts use Docker's port mapping.
    pub async fn start_with_http_port(port: u16) -> Result<Self> {
        anyhow::ensure!(port != 0, "container fixture port must be nonzero");
        Self::start_on_port(Some(port)).await
    }

    async fn start_on_port(port: Option<u16>) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("cellar-container-peer-")
            .tempdir_in("/tmp")?;
        let socket = directory.path().join("engine.sock");
        let engine_listener = tokio::net::UnixListener::bind(&socket)?;
        let http = http_listener(port).await?;
        let tcp = tokio::net::TcpListener::bind((http.local_addr()?.ip(), 0)).await?;
        let engine = Arc::new(Engine {
            network_ip: Some(http.local_addr()?.ip().to_string()),
            http_port: http.local_addr()?.port(),
            tcp_port: tcp.local_addr()?.port(),
            ..Default::default()
        });
        engine.app_image_loaded.store(true, Ordering::SeqCst);
        let engine_copy = engine.clone();
        let engines = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((stream, _)) = engine_listener.accept().await {
                let engine = engine_copy.clone();
                connections.spawn(async move {
                    let service =
                        hyper::service::service_fn(move |request| engine.clone().answer(request));
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .with_upgrades()
                        .await;
                });
            }
        });
        let websockets = Arc::new(AtomicUsize::new(0));
        let pending_http = Arc::new(AtomicUsize::new(0));
        let pending_requests = pending_http.clone();
        let active = websockets.clone();
        let http = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((stream, _)) = http.accept().await {
                let active = active.clone();
                let pending_requests = pending_requests.clone();
                connections.spawn(async move {
                    let service =
                        hyper::service::service_fn(move |mut request: Request<Incoming>| {
                            let active = active.clone();
                            let pending_requests = pending_requests.clone();
                            async move {
                                if request.uri().path() == "/pending-head" {
                                    pending_requests.fetch_add(1, Ordering::SeqCst);
                                    let _request = Connected(pending_requests);
                                    return std::future::pending().await;
                                }
                                if request.uri().path() == "/pending-body" {
                                    pending_requests.fetch_add(1, Ordering::SeqCst);
                                    let connected = Connected(pending_requests);
                                    let body =
                                        StreamBody::new(futures_util::stream::once(async move {
                                            let _connected = connected;
                                            std::future::pending::<
                                                Result<hyper::body::Frame<Bytes>, std::io::Error>,
                                            >()
                                            .await
                                        }))
                                        .boxed();
                                    return Ok(Response::builder().status(200).body(body)?);
                                }
                                if request.uri().path() == "/stream-body" {
                                    use futures_util::StreamExt;
                                    pending_requests.fetch_add(1, Ordering::SeqCst);
                                    let connected = Connected(pending_requests);
                                    let first =
                                        futures_util::stream::iter([Ok::<_, std::io::Error>(
                                            hyper::body::Frame::data(Bytes::from_static(&[
                                                0, 255, 42,
                                            ])),
                                        )]);
                                    let rest = futures_util::stream::once(async move {
                                        let _connected = connected;
                                        std::future::pending::<
                                            Result<hyper::body::Frame<Bytes>, std::io::Error>,
                                        >()
                                        .await
                                    });
                                    return Ok(Response::builder()
                                        .status(200)
                                        .header("x-stream", "head-before-eof")
                                        .body(BodyExt::boxed(StreamBody::new(
                                            first.chain(rest),
                                        )))?);
                                }
                                if request.uri().path() == "/ws" {
                                    let (response, upgrade) =
                                        fastwebsockets::upgrade::upgrade(&mut request)?;
                                    tokio::spawn(async move {
                                        let mut ws = upgrade.await.unwrap();
                                        active.fetch_add(1, Ordering::SeqCst);
                                        let _connected = Connected(active);
                                        while let Ok(frame) = ws.read_frame().await {
                                            let frame = match frame.opcode {
                                                OpCode::Text => {
                                                    Frame::text(frame.payload.to_vec().into())
                                                }
                                                OpCode::Binary => {
                                                    Frame::binary(frame.payload.to_vec().into())
                                                }
                                                OpCode::Close => break,
                                                _ => continue,
                                            };
                                            if ws.write_frame(frame).await.is_err() {
                                                break;
                                            }
                                        }
                                    });
                                    return Ok::<_, anyhow::Error>(response.map(|_| {
                                        Full::new(Bytes::new())
                                            .map_err(|never| match never {})
                                            .boxed()
                                    }));
                                }
                                Ok(Response::builder()
                                    .status(302)
                                    .header("location", "https://unrelated.invalid")
                                    .body(
                                        Full::new(Bytes::from_static(b"container response"))
                                            .map_err(|never| match never {})
                                            .boxed(),
                                    )?)
                            }
                        });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .with_upgrades()
                        .await;
                });
            }
        });
        let tcp = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((mut stream, _)) = tcp.accept().await {
                connections.spawn(async move {
                    let mut bytes = Vec::new();
                    if stream.read_to_end(&mut bytes).await.is_ok() {
                        let _ = stream.write_all(&bytes).await;
                        let _ = stream.shutdown().await;
                    }
                });
            }
        });
        Ok(Self {
            engine,
            socket,
            websockets,
            pending_http,
            tasks: vec![engines, http, tcp],
            _directory: directory,
        })
    }

    pub fn check_options(&self) {
        let calls = self.engine.calls.lock().unwrap();
        let create = calls
            .iter()
            .find(|(_, path, _)| path.starts_with("/containers/create?name="))
            .unwrap();
        assert_eq!(create.2["Labels"]["cellar.user.app"], "adapter");
        assert!(create.2["Env"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("HELLO=world")));
        let exec = calls
            .iter()
            .find(|(_, path, body)| path.contains("/exec?") && body["WorkingDir"] == "/tmp")
            .unwrap();
        assert_eq!(exec.2["User"], "1000");
        assert_eq!(exec.2["Env"], serde_json::json!(["EXEC=yes"]));
        let terminal = calls
            .iter()
            .find(|(_, path, body)| path.contains("/exec?") && body["Tty"] == true)
            .unwrap();
        assert_eq!(terminal.2["ConsoleSize"], serde_json::json!([24, 80]));
        assert!(calls
            .iter()
            .any(|(method, path, _)| method == "POST" && path.ends_with("/resize?h=40&w=132")));
    }

    pub fn configure(
        &self,
        config: &mut cellar::sdk::NodeConfig,
        services: &mut cellar::sdk::NodeServices,
        graph: &mut cellar::generation::DeploymentGraph,
    ) {
        config.runtime.handler_budget = std::time::Duration::from_secs(10);
        services.containers = Some(cellar::container::EngineOptions {
            socket: self.socket.clone(),
            namespace: None,
            runtime: None,
            dns: Vec::new(),
            network_helper: None,
        });
        graph.primary.containers = vec![cellar::container::ContainerSpec {
            class_name: "Counter".into(),
            image: cellar::container::image_reference(&"a".repeat(64)),
            instance_type: Some("basic".into()),
            max_instances: None,
            runtime: None,
            exposed_ports: Vec::new(),
        }];
        graph.primary.fence_image = Some(cellar::container::image_reference(&"b".repeat(64)));
        graph.primary.options.vars.extend([
            ("HTTP_PORT".into(), self.engine.http_port.to_string()),
            ("TCP_PORT".into(), self.engine.tcp_port.to_string()),
        ]);
    }
}

async fn http_listener(port: Option<u16>) -> Result<tokio::net::TcpListener> {
    #[cfg(target_os = "linux")]
    if let Some(port) = port {
        // Give concurrent fixtures distinct container IPs without reserving a
        // shared host port. Linux routes the full 127/8 range to loopback.
        for _ in 0..16 {
            let seed = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let unique = seed.local_addr()?.port();
            let ip = std::net::Ipv4Addr::new(127, 1, (unique >> 8) as u8, unique as u8);
            match tokio::net::TcpListener::bind((ip, port)).await {
                Ok(listener) => return Ok(listener),
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => continue,
                Err(error) => return Err(error.into()),
            }
        }
        anyhow::bail!("could not reserve an isolated container fixture address");
    }
    #[cfg(not(target_os = "linux"))]
    let _ = port;
    Ok(tokio::net::TcpListener::bind("127.0.0.1:0").await?)
}

/// A third-party provider delegates to native while auditing actual dispatch.
/// This verifies provider selection through all three guest runtimes.
#[derive(Default)]
pub struct AuditedProvider {
    pub calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub sessions: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub pending_reads: Arc<AtomicUsize>,
}

impl cellar_guest::containers::Provider for AuditedProvider {
    fn exec_pty(&self) -> bool {
        cellar_guest::containers::Native.exec_pty()
    }

    fn queued_lifecycle(&self) -> bool {
        true
    }

    fn synchronous_running(&self) -> bool {
        true
    }

    fn environment(&self) -> cellar_guest::containers::Environment {
        cellar_guest::containers::Native.environment()
    }

    fn max_resources(&self) -> Option<usize> {
        cellar_guest::containers::Native.max_resources()
    }

    fn name(&self) -> &str {
        "audited-native"
    }

    fn operations(&self) -> &[cellar_guest::containers::Operation] {
        cellar_guest::containers::Native.operations()
    }

    fn open(&self) -> Box<dyn cellar_guest::containers::ProviderSession> {
        self.sessions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::new(AuditedSession {
            inner: cellar_guest::containers::Native.open(),
            calls: self.calls.clone(),
            pending_reads: self.pending_reads.clone(),
        })
    }
}

struct AuditedSession {
    inner: Box<dyn cellar_guest::containers::ProviderSession>,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pending_reads: Arc<AtomicUsize>,
}

impl cellar_guest::containers::ProviderSession for AuditedSession {
    fn preflight(
        &self,
        context: &cellar::sdk::Context,
        operation: cellar_guest::containers::Operation,
        input: &serde_json::Value,
    ) -> Result<()> {
        self.inner.preflight(context, operation, input)
    }

    fn running_snapshot(&self, context: &cellar::sdk::Context) -> Result<bool> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.running_snapshot(context)
    }

    fn execute<'a>(
        &'a self,
        context: &'a cellar::sdk::Context,
        operation: cellar_guest::containers::Operation,
        input: serde_json::Value,
        limits: &'a cellar_guest::containers::Limits,
    ) -> cellar_guest::containers::ProviderFuture<'a> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            let _pending = if operation == cellar_guest::containers::Operation::ReadResponse {
                self.pending_reads.fetch_add(1, Ordering::SeqCst);
                Some(Connected(self.pending_reads.clone()))
            } else {
                None
            };
            self.inner.execute(context, operation, input, limits).await
        })
    }
}
