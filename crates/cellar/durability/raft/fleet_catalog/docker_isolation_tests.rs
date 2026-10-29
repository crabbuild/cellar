// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)] // Finite fake Engine tasks are joined.

use super::*;
use crate::bucket::StorageBackend;
use anyhow::{ensure, Context};
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use serde_json::{json, Value};
use std::sync::Mutex as SyncMutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

const ID: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[path = "docker_isolation_engine_tests.rs"]
mod real_engine;

#[path = "docker_resources_tests.rs"]
mod resources_tests;

const START: &str = "2026-10-28T00:00:00.000000001Z";
const FINISH: &str = "2026-10-28T00:01:00.000000001Z";
const WAIT: Duration = Duration::from_secs(3);

#[derive(Debug)]
struct NoListing;

#[async_trait::async_trait]
impl PaginatedListStore for NoListing {
    async fn list_paginated(
        &self,
        _: Option<&str>,
        _: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        Err(object_store::Error::NotSupported {
            source: "isolation does not list storage".into(),
        })
    }
}

struct Fixture {
    domain: ExecutionDomain,
    catalog: FleetCatalog,
    boot: BootBinding,
}

impl Fixture {
    fn new() -> anyhow::Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let store = Arc::new(object_store::memory::InMemory::new());
        let bucket = Bucket::from_clients(
            store.clone(),
            store,
            Arc::new(NoListing),
            StorageBackend::S3,
            "test".into(),
            "".into(),
        )?;
        let catalog = FleetCatalog::new(
            &domain,
            bucket,
            ClusterId::new([1; 16])?,
            IncarnationId::new([2; 16])?,
            "isolation/",
        )?;
        let boot = BootBinding {
            catalog: catalog.catalog_id()?,
            fleet: [2; 16],
            node: [3; 16],
            disk: [4; 16],
            engine: [5; 16],
            boot: [6; 16],
            process: [7; 16],
            epoch: 2,
            join_key: ed25519_dalek::SigningKey::from_bytes(&[8; 32])
                .verifying_key()
                .to_bytes(),
        };
        Ok(Self {
            domain,
            catalog,
            boot,
        })
    }

    fn adapter(&self, server: &Server) -> anyhow::Result<FleetDockerIsolation> {
        Ok(FleetDockerIsolation::new(
            self.catalog.clone(),
            Docker::new(&server.path),
            "fixture",
            vec![],
        )?)
    }

    async fn bind(&self, adapter: &FleetDockerIsolation) -> anyhow::Result<Binding> {
        Ok(adapter
            .run(WAIT, async {
                let binding = adapter
                    .inner
                    .engine
                    .bind(
                        &self.boot,
                        &ContainerId::new(ID).map_err(|_| FleetCatalogError::Invalid("test ID"))?,
                    )
                    .await?;
                adapter.inner.persist_binding(&binding).await?;
                Ok(binding)
            })
            .await?)
    }

    async fn isolate(&self, adapter: &FleetDockerIsolation) -> Result<FleetIsolationAttestation> {
        adapter
            .run(
                WAIT,
                adapter.isolate_boot(ProposalId::new([9; 16]).unwrap(), &self.boot),
            )
            .await
    }
}

struct State {
    inspect: Value,
    stats: Value,
    restart_during_stats: bool,
    engine: String,
    missing: bool,
    lose_update: bool,
    lose_kill: bool,
    hang_update: bool,
    oversized: bool,
    requests: Vec<String>,
}

struct Server {
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
    state: Arc<SyncMutex<State>>,
    stop: tokio::sync::oneshot::Sender<()>,
    peer: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Server {
    fn new(boot: &BootBinding) -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("engine.sock");
        let listener = UnixListener::bind(&path)?;
        let state = Arc::new(SyncMutex::new(State {
            inspect: json!({"Id":ID,"RestartCount":0,
                "Config":{"Labels":{"io.cellar.fleet.owner":"fixture","io.cellar.fleet.catalog":hex::encode(boot.catalog),"io.cellar.fleet.node":hex::encode(boot.node),"io.cellar.fleet.boot-mode":"single-use","io.cellar.fleet.workers":"contained"},"Env":["SECRET=must-not-escape"]},
                "HostConfig":{"AutoRemove":false,"Privileged":false,"PidMode":"","RestartPolicy":{"Name":"no","MaximumRetryCount":0},"NanoCpus":1_000_000_000u64,"Memory":3u64*1024*1024*1024,"MemorySwap":3u64*1024*1024*1024},
                "State":{"Running":true,"Restarting":false,"Paused":false,"Dead":false,"Status":"running","Pid":123,"StartedAt":START,"FinishedAt":"0001-01-01T00:00:00Z","ExitCode":0}}),
            engine: "engine-original".into(),
            stats: json!({"id":ID,"read":FINISH,"memory_stats":{"usage":128u64*1024*1024,"limit":3u64*1024*1024*1024,"stats":{"cache":64u64*1024*1024}}}),
            restart_during_stats: false,
            missing: false,
            lose_update: false,
            lose_kill: false,
            hang_update: false,
            oversized: false,
            requests: vec![],
        }));
        let shared = state.clone();
        let (stop, mut stopping) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            loop {
                let accepted = crate::asyncrt::select! { _=&mut stopping => break, accepted=listener.accept()=>accepted? };
                let (mut socket, _) = accepted;
                let (head, body) = request(&mut socket).await?;
                let (status, value, lose, hang, oversized) = {
                    let mut state = shared.lock().unwrap();
                    state.requests.push(head.clone());
                    ensure!(state.requests.len() < 100, "unbounded request retry");
                    if head == "GET /info" {
                        (
                            200,
                            json!({"ID":state.engine,"OSType":"linux"}),
                            false,
                            false,
                            false,
                        )
                    } else if head == format!("GET /containers/{ID}/json") {
                        (
                            if state.missing { 404 } else { 200 },
                            state.inspect.clone(),
                            false,
                            false,
                            state.oversized,
                        )
                    } else if head
                        == format!("GET /containers/{ID}/stats?stream=false&one-shot=true")
                    {
                        if state.restart_during_stats {
                            state.inspect["RestartCount"] = json!(1);
                        }
                        (200, state.stats.clone(), false, false, false)
                    } else if head == format!("POST /containers/{ID}/update") {
                        ensure!(
                            body == json!({"RestartPolicy":{"Name":"no","MaximumRetryCount":0}})
                        );
                        let lose = std::mem::take(&mut state.lose_update);
                        (200, json!({"Warnings":[]}), lose, state.hang_update, false)
                    } else if head == format!("POST /containers/{ID}/kill?signal=SIGKILL") {
                        let s = &mut state.inspect["State"];
                        s["Running"] = json!(false);
                        s["Paused"] = json!(false);
                        s["Status"] = json!("exited");
                        s["Pid"] = json!(0);
                        s["FinishedAt"] = json!(FINISH);
                        s["ExitCode"] = json!(137);
                        let lose = std::mem::take(&mut state.lose_kill);
                        (204, Value::Null, lose, false, false)
                    } else {
                        anyhow::bail!("unexpected Engine operation: {head}");
                    }
                };
                if lose {
                    continue;
                }
                if hang {
                    let mut bytes = Vec::new();
                    socket.read_to_end(&mut bytes).await?;
                    ensure!(bytes.is_empty());
                    continue;
                }
                let bytes = if status == 204 {
                    vec![]
                } else {
                    serde_json::to_vec(&value)?
                };
                let length = if oversized {
                    256 * 1024 + 1
                } else {
                    bytes.len()
                };
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n").as_bytes()).await?;
                if !oversized {
                    socket.write_all(&bytes).await?;
                }
            }
            Ok(())
        });
        Ok(Self {
            _directory: directory,
            path,
            state,
            stop,
            peer,
        })
    }

    async fn close(self) -> anyhow::Result<()> {
        let _ = self.stop.send(());
        tokio::time::timeout(WAIT, self.peer).await???;
        Ok(())
    }

    fn mutations(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|s| s.starts_with("POST"))
            .cloned()
            .collect()
    }
}

async fn request(socket: &mut UnixStream) -> anyhow::Result<(String, Value)> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        ensure!(bytes.len() < 8192);
        bytes.push(socket.read_u8().await?);
    }
    let head = String::from_utf8(bytes)?;
    let length = head
        .lines()
        .find_map(|l| {
            l.to_lowercase()
                .strip_prefix("content-length: ")
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "0".into())
        .parse::<usize>()?;
    ensure!(length < 8192);
    let mut body = vec![0; length];
    socket.read_exact(&mut body).await?;
    let request = head
        .lines()
        .next()
        .context("request line")?
        .strip_suffix(" HTTP/1.1")
        .context("HTTP version")?
        .to_owned();
    Ok((
        request,
        if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body)?
        },
    ))
}

#[tokio::test]
async fn docker_isolation_recreates_adapter_after_lost_update_and_kill_replies(
) -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let server = Server::new(&f.boot)?;
    let first = f.adapter(&server)?;
    let binding = f.bind(&first).await?;
    server.state.lock().unwrap().lose_update = true;
    ensure!(matches!(
        f.isolate(&first).await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    ensure!(server.mutations().len() == 1);
    let intent: Intent = first
        .inner
        .read(&format!("{}/intent", binding.boot_key()?))
        .await?
        .context("durable pre-effect intent")?;
    ensure!(intent.binding == binding);
    drop(first);
    let second = f.adapter(&server)?;
    server.state.lock().unwrap().lose_kill = true;
    ensure!(matches!(
        f.isolate(&second).await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    drop(second);
    let third = f.adapter(&server)?;
    let receipt = f.isolate(&third).await?;
    ensure!(receipt.boot == f.boot && receipt.evidence_digest != [0; 32]);
    let repeated = f.isolate(&third).await?;
    ensure!(receipt == repeated);
    ensure!(
        server
            .mutations()
            .iter()
            .filter(|v| v.contains("/kill?"))
            .count()
            == 1
    );
    let stored: Receipt = third
        .inner
        .read(&format!("{}/receipt", binding.boot_key()?))
        .await?
        .context("stored terminal evidence")?;
    ensure!(stored.terminal.finished == FINISH && stored.terminal.exit_code == 137);
    ensure!(server.state.lock().unwrap().inspect["HostConfig"]["AutoRemove"] == false);
    server.close().await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn docker_isolation_refuses_changed_identity_policy_and_missing_container(
) -> anyhow::Result<()> {
    for variant in 0..10 {
        let f = Fixture::new()?;
        let server = Server::new(&f.boot)?;
        let adapter = f.adapter(&server)?;
        f.bind(&adapter).await?;
        {
            let mut s = server.state.lock().unwrap();
            match variant {
                0 => s.engine = "replacement-engine".into(),
                1 => s.inspect["Id"] = json!("different-container"),
                2 => s.inspect["Config"]["Labels"]["io.cellar.fleet.owner"] = json!("other"),
                3 => s.inspect["State"]["StartedAt"] = json!(FINISH),
                4 => s.inspect["RestartCount"] = json!(1),
                5 => s.missing = true,
                6 => s.inspect["HostConfig"]["RestartPolicy"]["Name"] = json!("always"),
                7 => s.inspect["HostConfig"]["AutoRemove"] = json!(true),
                8 => s.inspect["HostConfig"]["PidMode"] = json!("host"),
                _ => s.inspect["Config"]["Labels"]["io.cellar.fleet.workers"] = json!("external"),
            }
        }
        ensure!(
            f.isolate(&adapter).await.is_err(),
            "variant {variant} granted proof"
        );
        ensure!(
            server.mutations().is_empty(),
            "variant {variant} mutated Docker"
        );
        server.close().await?;
        f.domain.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn docker_isolation_requires_immutable_boot_claim_and_operation() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let server = Server::new(&f.boot)?;
    let adapter = f.adapter(&server)?;
    ensure!(matches!(
        f.isolate(&adapter).await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    ensure!(server.state.lock().unwrap().requests.is_empty());
    let binding = f.bind(&adapter).await?;
    let mut replacement = binding.clone();
    replacement.boot.process = [44; 16];
    ensure!(matches!(
        adapter.inner.persist_binding(&replacement).await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    let key = format!("{}/intent", binding.boot_key()?);
    adapter
        .inner
        .persist(
            &key,
            &Intent {
                format: 1,
                operation: [10; 16],
                binding: binding.clone(),
            },
        )
        .await?;
    ensure!(matches!(
        f.isolate(&adapter).await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(server.mutations().is_empty());
    server.close().await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn docker_isolation_timeout_drops_connection_and_preserves_intent() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let server = Server::new(&f.boot)?;
    let adapter = f.adapter(&server)?;
    let binding = f.bind(&adapter).await?;
    server.state.lock().unwrap().hang_update = true;
    let result = adapter
        .run(
            Duration::from_millis(100),
            adapter.isolate_boot(ProposalId::new([9; 16])?, &f.boot),
        )
        .await;
    ensure!(matches!(result, Err(FleetCatalogError::Unconfirmed)));
    ensure!(adapter
        .inner
        .read::<Intent>(&format!("{}/intent", binding.boot_key()?))
        .await?
        .is_some());
    server.state.lock().unwrap().hang_update = false;
    ensure!(f.isolate(&adapter).await?.boot == f.boot);
    server.close().await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn docker_isolation_bounds_inspection_and_never_exposes_environment() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let server = Server::new(&f.boot)?;
    let adapter = f.adapter(&server)?;
    f.bind(&adapter).await?;
    server.state.lock().unwrap().oversized = true;
    let error = f.isolate(&adapter).await.unwrap_err();
    ensure!(matches!(error, FleetCatalogError::Unconfirmed));
    ensure!(!format!("{error:?}").contains("SECRET"));
    ensure!(server.mutations().is_empty());
    server.close().await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn docker_isolation_binding_refuses_restart_or_uncontained_workers() -> anyhow::Result<()> {
    ContainerId::new(ID)?;
    for variant in 0..5 {
        let f = Fixture::new()?;
        let server = Server::new(&f.boot)?;
        let adapter = f.adapter(&server)?;
        {
            let mut s = server.state.lock().unwrap();
            match variant {
                0 => s.inspect["HostConfig"]["RestartPolicy"]["Name"] = json!("always"),
                1 => s.inspect["HostConfig"]["Privileged"] = json!(true),
                2 => s.inspect["Config"]["Labels"]["io.cellar.fleet.boot-mode"] = json!("reusable"),
                3 => s.inspect["State"]["Paused"] = json!(true),
                _ => s.inspect["State"]["Running"] = json!(false),
            }
        }
        ensure!(f.bind(&adapter).await.is_err());
        ensure!(server.mutations().is_empty());
        server.close().await?;
        f.domain.shutdown().await;
    }
    Ok(())
}
