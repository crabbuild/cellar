// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared Engine protocol with joined serving tasks; no Docker VM is substituted.
#![allow(dead_code, clippy::disallowed_methods)]
#[path = "../../../cellar/tests/support/container_engine.rs"]
mod engine;
use anyhow::Result;
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
};

pub struct Peer {
    pub engine: Arc<engine::Engine>,
    socket: PathBuf,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
    _directory: tempfile::TempDir,
}

impl Peer {
    pub async fn start() -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("cellar-child-engine-")
            .tempdir_in("/tmp")?;
        let socket = directory.path().join("engine.sock");
        let listener = tokio::net::UnixListener::bind(&socket)?;
        let engine = Arc::new(engine::Engine::default());
        engine.app_image_loaded.store(true, Ordering::SeqCst);
        let backend = engine.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let accepted = cellar::asyncrt::select_biased! { "stop accepting before joining the Engine fixture connections";
                    _ = &mut stopped => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((stream, _)) = accepted else {
                    break;
                };
                let backend = backend.clone();
                connections.spawn(async move {
                    let service =
                        hyper::service::service_fn(move |request| backend.clone().answer(request));
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .with_upgrades()
                        .await;
                });
            }
            connections.shutdown().await;
        });
        Ok(Self {
            engine,
            socket,
            stop,
            task,
            _directory: directory,
        })
    }

    pub fn configure(
        &self,
        config: &mut cellar::sdk::NodeConfig,
        services: &mut cellar::sdk::NodeServices,
        graph: &mut cellar::generation::DeploymentGraph,
    ) {
        config.runtime.handler_budget = std::time::Duration::from_secs(10);
        services.containers = Some(cellar::container::EngineOptions {
            capacity: None,
            socket: self.socket.clone(),
            namespace: None,
            runtime: None,
            dns: Vec::new(),
            network_helper: None,
        });
        graph.primary.containers = vec![cellar::container::ContainerSpec {
            class_name: "Sandbox".into(),
            image: cellar::container::image_reference(&"a".repeat(64)),
            instance_type: Some("basic".into()),
            max_instances: None,
            runtime: None,
            exposed_ports: Vec::new(),
        }];
        graph.primary.fence_image = Some(cellar::container::image_reference(&"b".repeat(64)));
    }

    pub async fn stop(self) -> Result<()> {
        let _ = self.stop.send(());
        self.task.await?;
        self.engine.join_processes().await
    }
}
