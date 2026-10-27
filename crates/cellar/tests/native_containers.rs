// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SDK control and ownership contracts run through production native cells.
//! A deterministic Engine API peer supplies external process outcomes.
#![allow(clippy::disallowed_methods)]
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Context as _, Result};
use cellar::{
    application::{
        containers::*, http, sockets, Cell, Context, HandlerFuture, Invocation, Output, Registry,
    },
    container::ContainerSpec,
    engine_api::RuntimeFetch,
    generation::Generation,
    runtime::RuntimeManager,
};
use http_body_util::Full;
use hyper::{
    body::{Bytes, Incoming},
    Request as HttpRequest, Response,
};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, watch},
};

#[path = "support/container_engine.rs"]
mod container_engine;
use container_engine::Engine;

#[derive(Default)]
struct Probe {
    hold: AtomicBool,
    container: Mutex<Option<Container>>,
    context: Mutex<Option<Context>>,
    exec: Mutex<Option<Exec>>,
}

struct Root(Arc<Probe>);

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let container = context.container()?;
            let path = url::Url::parse(&request.url)?.path().to_string();
            let value = match path.as_str() {
                "/start" => {
                    context
                        .storage()?
                        .put_json("before-container", &json!(true))?;
                    match container
                        .start(
                            &context,
                            StartOptions {
                                env: vec![("HELLO".into(), "world".into())],
                                labels: vec![("app".into(), "sandbox".into())],
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(run) => {
                            *self.0.container.lock().unwrap() = Some(container.clone());
                            *self.0.context.lock().unwrap() = Some(context.clone());
                            json!({"run":run})
                        }
                        Err(error) => json!({"error":error.to_string()}),
                    }
                }
                "/retained" => {
                    let retained = self.0.container.lock().unwrap().clone().unwrap();
                    json!(retained.running(&context)?)
                }
                "/foreign" => {
                    let retained = self.0.container.lock().unwrap().clone().unwrap();
                    json!(retained.running(&context).is_err())
                }
                "/files" => {
                    let path = FilePath::new("/tmp/cellar-?é.bin")?;
                    container
                        .write_file(&context, path.clone(), vec![0, 255, 42], 0o640)
                        .await?;
                    assert_eq!(
                        container.read_file(&context, path.clone(), 3).await?,
                        [0, 255, 42]
                    );
                    assert!(container
                        .read_file(&context, path.clone(), 2)
                        .await
                        .is_err());
                    let metadata = container.stat_file(&context, path).await?;
                    assert_eq!(
                        (
                            metadata.name.as_str(),
                            metadata.kind,
                            metadata.mode,
                            metadata.size
                        ),
                        ("cellar-?é.bin", FileKind::File, 0o640, 3)
                    );
                    let listing = container
                        .list_files(&context, FilePath::new("/tmp")?, 4096, 1)
                        .await?;
                    assert_eq!(listing.len(), 1);
                    assert!(container
                        .list_files(&context, FilePath::new("/tmp")?, 2, 1)
                        .await
                        .is_err());
                    json!(true)
                }
                "/terminal-refused" => {
                    let error = container
                        .exec(
                            &context,
                            ExecOptions {
                                cmd: vec!["echo-stdin".into()],
                                pty: Some(TerminalSize::new(80, 24)?),
                                ..Default::default()
                            },
                        )
                        .await
                        .err()
                        .context("old engine accepted a terminal")?;
                    anyhow::ensure!(format!("{error:#}").contains("1.42"));
                    json!(true)
                }
                "/terminal" => {
                    let process = container
                        .exec(
                            &context,
                            ExecOptions {
                                cmd: vec!["echo-stdin".into()],
                                pty: Some(TerminalSize::new(80, 24)?),
                                ..Default::default()
                            },
                        )
                        .await?;
                    let info = process.info(&context)?;
                    anyhow::ensure!(info.is_pty && info.pid.is_none());
                    anyhow::ensure!(process.resize(&context, 0, 24).await.is_err());
                    process.resize(&context, 132, 40).await?;
                    process.write(&context, [0, 255, 42]).await?;
                    process.close_stdin(&context).await?;
                    let output = process.output(&context, 1024).await?;
                    json!({"is_pty":info.is_pty,"pid":info.pid,"code":output.exit_code,"stdout":output.stdout,"stderr":output.stderr})
                }
                "/exec" => {
                    let process = container
                        .exec(
                            &context,
                            ExecOptions {
                                cmd: vec!["echo-stdin".into()],
                                ..Default::default()
                            },
                        )
                        .await?;
                    process.write(&context, b"hello exec").await?;
                    process.close_stdin(&context).await?;
                    let output = process.output(&context, 1024).await?;
                    json!({"code":output.exit_code,"stdout":String::from_utf8(output.stdout)?,"stderr":String::from_utf8(output.stderr)?})
                }
                "/limit" => {
                    let process = container
                        .exec(
                            &context,
                            ExecOptions {
                                cmd: vec!["echo-stdin".into()],
                                ..Default::default()
                            },
                        )
                        .await?;
                    process.write(&context, b"too much output").await?;
                    process.close_stdin(&context).await?;
                    json!(process.output(&context, 2).await.is_err())
                }
                "/retain-exec" => {
                    let process = container
                        .exec(
                            &context,
                            ExecOptions {
                                cmd: vec!["echo-stdin".into()],
                                ..Default::default()
                            },
                        )
                        .await?;
                    *self.0.exec.lock().unwrap() = Some(process);
                    json!(true)
                }
                "/retained-exec-output" => {
                    let process = self.0.exec.lock().unwrap().take().unwrap();
                    process.write(&context, b"surviving host").await?;
                    process.close_stdin(&context).await?;
                    let output = process.output(&context, 1024).await?;
                    json!({"code":output.exit_code,"stdout":String::from_utf8(output.stdout)?})
                }
                "/old-exec" => {
                    let process = self.0.exec.lock().unwrap().take().unwrap();
                    json!(process.kill(&context, 9).await.is_err())
                }
                "/http" => {
                    let port = context.var("HTTP_PORT").unwrap().parse()?;
                    let mut response = container
                        .port(&context, port)?
                        .fetch(
                            &context,
                            http::OutboundRequest::get("https://application.test/path?query=yes"),
                        )
                        .await?;
                    if let Some(stream) = response.stream.take() {
                        use futures_util::TryStreamExt;
                        response
                            .body
                            .extend(stream.try_concat().await.map_err(anyhow::Error::msg)?);
                    }
                    json!({"status":response.status, "body":String::from_utf8(response.body)?})
                }
                "/tcp" => {
                    let port = context.var("TCP_PORT").unwrap().parse()?;
                    let socket = container.port(&context, port)?.connect(
                        &context,
                        sockets::SocketOptions {
                            allow_half_open: true,
                            ..Default::default()
                        },
                    )?;
                    socket.opened().await?;
                    let writer = socket.writable.get_writer()?;
                    writer.write(&context, b"hello tcp".to_vec()).await?;
                    writer.close(&context).await?;
                    let reader = socket.readable.get_reader()?;
                    let mut bytes = Vec::new();
                    loop {
                        let chunk = reader.read(&context).await?;
                        if chunk.is_empty() {
                            break;
                        }
                        bytes.extend(chunk);
                    }
                    json!(String::from_utf8(bytes)?)
                }
                "/signal" => {
                    container.signal(&context, 1).await?;
                    json!(true)
                }
                "/timeout" => {
                    container
                        .set_inactivity_timeout(&context, Duration::from_secs(60))
                        .await?;
                    json!(true)
                }
                "/destroy" => {
                    container.destroy(&context).await?;
                    json!(true)
                }
                "/destroy-refused" => json!(container.destroy(&context).await.is_err()),
                "/running" => json!(container.running(&context)?),
                "/monitor" => json!(container.monitor(&context).await?),
                "/monitor-restart" => {
                    let previous = container.monitor(&context);
                    container.destroy(&context).await?;
                    container.start(&context, Default::default()).await?;
                    let code = tokio::time::timeout(Duration::from_secs(1), previous)
                        .await
                        .context("previous run's monitor was replaced by the new run")??;
                    container.destroy(&context).await?;
                    json!(code == 137)
                }
                "/monitor-empty" => json!(container.monitor(&context).await.is_err()),
                _ => return Err(anyhow!("unknown scenario {path}")),
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

async fn open(runtime: &RuntimeManager, scope: &str) -> Result<()> {
    runtime
        .restore_cell(
            scope,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(scope.into(), 1, true).await?;
    runtime.publish_cell(scope, 1)
}

async fn call(runtime: &RuntimeManager, scope: &str, path: &str) -> Result<Value> {
    let response = runtime
        .fetch_cell(
            scope.into(),
            None,
            RuntimeFetch {
                url: format!("http://cell{path}"),
                method: "GET".into(),
                headers: Vec::new(),
                body: Vec::new().into(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await?;
    assert_eq!(response.status, 200);
    Ok(serde_json::from_slice(&response.body)?)
}

#[test]
fn native_containers_preserve_ownership_proofs_exec_streams_and_ports() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
        tokio::time::timeout(Duration::from_secs(60), run()).await??;
        Ok(())
    })
}

async fn run() -> Result<()> {
    let directory = tempfile::Builder::new()
        .prefix("cellar-container-")
        .tempdir_in("/tmp")?;
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let peer = Arc::new(Engine {
        http_port: http.local_addr()?.port(),
        tcp_port: tcp.local_addr()?.port(),
        ..Default::default()
    });
    let http_task = tokio::spawn(async move {
        while let Ok((stream, _)) = http.accept().await {
            tokio::spawn(async move {
                let service =
                    hyper::service::service_fn(|request: HttpRequest<Incoming>| async move {
                        assert_eq!(
                            request.uri().path_and_query().unwrap().as_str(),
                            "/path?query=yes"
                        );
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(302)
                                .header("location", "https://unrelated.invalid")
                                .body(Full::new(Bytes::from_static(b"container response")))
                                .unwrap(),
                        )
                    });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    let tcp_task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = tcp.accept().await {
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).await.unwrap();
                stream.write_all(&bytes).await.unwrap();
                let _ = stream.shutdown().await;
            });
        }
    });
    let socket = directory.path().join("engine.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let engine = peer.clone();
    let engine_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let engine = engine.clone();
            tokio::spawn(async move {
                let service =
                    hyper::service::service_fn(move |request| engine.clone().answer(request));
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    std::env::set_var("DOCKER_HOST", format!("unix://{}", socket.display()));
    let probe = Arc::new(Probe::default());
    let (gate_tx, mut gates) = mpsc::unbounded_channel::<cellar::host_channels::GateReq>();
    let (held_tx, mut held) = mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gate_tx);
    let p = probe.clone();
    let gate_task = cellar::asyncrt::spawn(async move {
        while let Some(gate) = gates.recv().await {
            if gate.ticket.channel == cellar_logic::Channel::Container
                && p.hold.load(Ordering::SeqCst)
            {
                let _ = held_tx.send(gate);
            } else {
                let _ = gate.reply.send(Ok(()));
            }
        }
    });
    let p = probe.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Err(anyhow!("stateless unused")) }))
        .cell("Sandbox", move |_, _| Ok(Arc::new(Root(p.clone()))))?;
    let mut graph = fixture::graph("native-containers", "", vec!["Sandbox".into()]);
    graph.primary.options.vars = vec![
        ("HTTP_PORT".into(), peer.http_port.to_string()),
        ("TCP_PORT".into(), peer.tcp_port.to_string()),
    ];
    graph.primary.containers = vec![ContainerSpec {
        class_name: "Sandbox".into(),
        image: cellar::container::image_reference(&"a".repeat(64)),
        instance_type: Some("basic".into()),
        max_instances: None,
        runtime: Some("test-runtime".into()),
        exposed_ports: Vec::new(),
    }];
    graph.primary.fence_image = Some(cellar::container::image_reference(&"b".repeat(64)));
    use sha2::{Digest, Sha256};
    let image_bytes = b"fixture image tar";
    let archive = cellar::container::ImageArchive {
        image: graph.primary.containers[0].image.clone(),
        sha256: format!("{:x}", Sha256::digest(image_bytes)),
        bytes: image_bytes.len() as u64,
    };
    graph.primary.container_images = vec![archive.clone()];
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("images.sqlite"))?;
    bucket
        .put(&archive.key()?, b"corrupt archive".to_vec())
        .await?;
    let mut options = fixture::runtime_options(directory.path().join("cells"));
    options.bucket = Some(bucket.clone());
    let host = RuntimeManager::start(
        Generation::build_native(
            1,
            graph,
            fixture::generation_options(),
            HashMap::from([(
                "native-containers".into(),
                Arc::new(registry) as Arc<dyn cellar::application::Application>,
            )]),
        )?,
        options,
    )?;
    open(&host, "Sandbox:test").await?;
    let engine = cellar::container::engine_if_ready().unwrap();
    assert!(engine.ensure_image(&archive.image).await.is_err());
    assert!(
        !peer
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(_, path, _)| path.starts_with("/images/load")),
        "corrupt bucket bytes must not reach the engine"
    );
    bucket.put(&archive.key()?, image_bytes.to_vec()).await?;
    engine.ensure_image(&archive.image).await?;
    assert_eq!(
        peer.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, path, _)| path.starts_with("/images/load"))
            .count(),
        1
    );
    assert_eq!(call(&host, "Sandbox:test", "/running").await?, false);
    assert_eq!(call(&host, "Sandbox:test", "/monitor-empty").await?, true);
    probe.hold.store(true, Ordering::SeqCst);
    let copy = host.clone();
    let starting = tokio::spawn(async move { call(&copy, "Sandbox:test", "/start").await });
    let gate = held.recv().await.unwrap();
    assert_eq!(gate.ticket.channel, cellar_logic::Channel::Container);
    assert!(gate.ticket.position.is_some());
    assert_eq!(
        peer.app_creates(),
        0,
        "no container process before its source write is durable"
    );
    gate.reply
        .send(Err(cellar_logic::RequestError::NodeFenced))
        .unwrap();
    assert!(starting.await??["error"].as_str().is_some());
    assert_eq!(peer.app_creates(), 0);
    probe.hold.store(false, Ordering::SeqCst);
    assert_eq!(call(&host, "Sandbox:test", "/start").await?["run"], 1);
    assert_eq!(call(&host, "Sandbox:test", "/retained").await?, true);
    assert_eq!(call(&host, "Sandbox:test", "/files").await?, true);
    let context = probe.context.lock().unwrap().clone().unwrap();
    assert!(probe
        .container
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .running(&context)
        .is_err());
    open(&host, "Sandbox:other").await?;
    assert_eq!(call(&host, "Sandbox:other", "/foreign").await?, true);
    assert_eq!(
        call(&host, "Sandbox:test", "/exec").await?,
        json!({"code":7,"stdout":"hello exec","stderr":"stderr"})
    );
    peer.api_minor.store(41, Ordering::SeqCst);
    let allocated = peer.execs.lock().unwrap().len();
    assert_eq!(
        call(&host, "Sandbox:test", "/terminal-refused").await?,
        true
    );
    assert_eq!(
        peer.execs.lock().unwrap().len(),
        allocated,
        "unsupported terminal API allocated compute"
    );
    peer.api_minor.store(43, Ordering::SeqCst);
    assert_eq!(
        call(&host, "Sandbox:test", "/terminal").await?,
        json!({"is_pty":true,"pid":null,"code":7,"stdout":[0,255,42,115,116,100,101,114,114],"stderr":[]})
    );
    assert_eq!(call(&host, "Sandbox:test", "/limit").await?, true);
    assert_eq!(
        call(&host, "Sandbox:test", "/http").await?,
        json!({"status":302,"body":"container response"})
    );
    assert_eq!(call(&host, "Sandbox:test", "/tcp").await?, "hello tcp");
    call(&host, "Sandbox:test", "/signal").await?;
    call(&host, "Sandbox:test", "/timeout").await?;
    call(&host, "Sandbox:test", "/retain-exec").await?;
    assert_eq!(call(&host, "Sandbox:test", "/start").await?["run"], 2);
    assert_eq!(call(&host, "Sandbox:test", "/old-exec").await?, true);
    peer.refuse_remove.store(true, Ordering::SeqCst);
    assert_eq!(call(&host, "Sandbox:test", "/destroy-refused").await?, true);
    peer.refuse_remove.store(false, Ordering::SeqCst);
    let copy = host.clone();
    let monitor = tokio::spawn(async move { call(&copy, "Sandbox:test", "/monitor").await });
    call(&host, "Sandbox:test", "/destroy").await?;
    assert_eq!(monitor.await??, 137);
    assert_eq!(call(&host, "Sandbox:test", "/running").await?, false);
    host.swap_out_cell("Sandbox:test", 1).await?;
    host.start_cell("Sandbox:test".into(), 1, false).await?;
    host.publish_cell("Sandbox:test", 1)?;
    assert_eq!(
        call(&host, "Sandbox:test", "/foreign").await?,
        true,
        "same-epoch reactivation cannot borrow an old handle"
    );
    call(&host, "Sandbox:test", "/start").await?;
    let prior = engine.cell("Sandbox:test").unwrap();
    peer.refuse_remove.store(true, Ordering::SeqCst);
    assert!(engine
        .release("Sandbox:test", cellar::container::Release::Destroy)
        .await
        .is_err());
    assert!(
        Arc::ptr_eq(&prior, &engine.cell("Sandbox:test").unwrap()),
        "failed release must retain ownership bookkeeping"
    );
    let updated_image = cellar::container::image_reference(&"c".repeat(64));
    cellar::container::install_specs(
        vec![ContainerSpec {
            class_name: "Sandbox".into(),
            image: updated_image.clone(),
            instance_type: Some("standard-1".into()),
            max_instances: None,
            runtime: Some("updated-runtime".into()),
            exposed_ports: Vec::new(),
        }],
        Some(cellar::container::image_reference(&"b".repeat(64))),
        Vec::new(),
    );
    assert!(
        engine.attach("Sandbox:test", "Sandbox").await.is_err(),
        "a changed spec cannot be adopted before old container removal succeeds"
    );
    peer.refuse_remove.store(false, Ordering::SeqCst);
    let updated = engine.attach("Sandbox:test", "Sandbox").await?;
    assert!(!Arc::ptr_eq(&prior, &updated));
    assert_eq!(
        call(&host, "Sandbox:test", "/foreign").await?,
        true,
        "replaced container handle cannot regain authority"
    );
    call(&host, "Sandbox:test", "/start").await?;
    {
        let calls = peer.calls.lock().unwrap();
        let create = calls
            .iter()
            .find(|(_, path, _)| path.starts_with("/containers/create?name="))
            .unwrap();
        assert_eq!(create.2["HostConfig"]["Runtime"], "test-runtime");
        assert_eq!(create.2["Labels"]["cellar.user.app"], "sandbox");
        assert!(create.2["Env"]
            .as_array()
            .unwrap()
            .contains(&json!("HELLO=world")));
        let updated_create = calls
            .iter()
            .rev()
            .find(|(_, path, _)| path.starts_with("/containers/create?name="))
            .unwrap();
        assert_eq!(updated_create.2["Image"], updated_image);
        assert_eq!(updated_create.2["HostConfig"]["Runtime"], "updated-runtime");
    }
    assert_eq!(call(&host, "Sandbox:test", "/monitor-restart").await?, true);
    engine
        .release("Sandbox:test", cellar::container::Release::Destroy)
        .await?;
    peer.assert_immutable_effects()?;
    gate_task.abort();
    http_task.abort();
    tcp_task.abort();
    engine_task.abort();
    Ok(())
}

async fn embedded_container_host(
    domain: &cellar::asyncrt::ExecutionDomain,
    path: &std::path::Path,
    socket: &std::path::Path,
    image: &str,
    runtime: &str,
    probe: Arc<Probe>,
) -> Result<RuntimeManager> {
    domain
        .services()
        .configure_containers(cellar::container::EngineOptions {
            socket: socket.into(),
            namespace: None,
            runtime: Some(runtime.into()),
            dns: vec!["192.0.2.53".into()],
        })?;
    domain
        .run(async move {
            let (send, mut gates) = mpsc::unbounded_channel::<cellar::host_channels::GateReq>();
            cellar::host_channels::set_gate_tx(send);
            cellar::asyncrt::spawn(async move {
                while let Some(gate) = gates.recv().await {
                    let _ = gate.reply.send(Ok(()));
                }
            })
            .detach();
            let registry =
                Registry::new(|_, _| Box::pin(async { Err(anyhow!("stateless unused")) }))
                    .cell("Sandbox", move |_, _| Ok(Arc::new(Root(probe.clone()))))?;
            let mut graph = fixture::graph("same-container-app", "", vec!["Sandbox".into()]);
            graph.primary.containers = vec![ContainerSpec {
                class_name: "Sandbox".into(),
                image: image.into(),
                instance_type: Some("basic".into()),
                max_instances: None,
                runtime: None,
                exposed_ports: Vec::new(),
            }];
            graph.primary.fence_image = Some(cellar::container::image_reference(&"b".repeat(64)));
            let host = RuntimeManager::start(
                Generation::build_native(
                    1,
                    graph,
                    fixture::generation_options(),
                    HashMap::from([(
                        "same-container-app".into(),
                        Arc::new(registry) as Arc<dyn cellar::application::Application>,
                    )]),
                )?,
                fixture::runtime_options(path.into()),
            )?;
            open(&host, "Sandbox:same-business-id").await?;
            Ok::<_, anyhow::Error>(host)
        })
        .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_hosts_own_containers_execs_networks_and_restart_cleanup() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), owned_container_domains()).await?
}

async fn owned_container_domains() -> Result<()> {
    use cellar::asyncrt::ExecutionDomain;
    let directory = tempfile::Builder::new()
        .prefix("cellar-owned-containers-")
        .tempdir_in("/tmp")?;
    let socket = directory.path().join("engine.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let peer = Arc::new(Engine::default());
    let engine = peer.clone();
    let server = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let engine = engine.clone();
            tokio::spawn(async move {
                let service =
                    hyper::service::service_fn(move |request| engine.clone().answer(request));
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_probe = Arc::new(Probe::default());
    let second_probe = Arc::new(Probe::default());
    let first_path = directory.path().join("first");
    let second_path = directory.path().join("second");
    let first_image = cellar::container::image_reference(&"c".repeat(64));
    let second_image = cellar::container::image_reference(&"d".repeat(64));
    let first_host = embedded_container_host(
        &first,
        &first_path,
        &socket,
        &first_image,
        "first-runtime",
        first_probe.clone(),
    )
    .await?;
    assert_eq!(
        first
            .run(call(&first_host, "Sandbox:same-business-id", "/start"))
            .await??["run"],
        1
    );
    let second_host = embedded_container_host(
        &second,
        &second_path,
        &socket,
        &second_image,
        "second-runtime",
        second_probe.clone(),
    )
    .await?;
    assert_eq!(
        second
            .run(call(&second_host, "Sandbox:same-business-id", "/start"))
            .await??["run"],
        1
    );
    let first_engine = first
        .run(async { cellar::container::engine_if_ready().unwrap() })
        .await?;
    let second_engine = second
        .run(async { cellar::container::engine_if_ready().unwrap() })
        .await?;
    assert!(!Arc::ptr_eq(&first_engine, &second_engine));
    assert!(second
        .run(first_engine.ensure_image(&first_image))
        .await?
        .is_err());
    let second_cell = second_engine.cell("Sandbox:same-business-id").unwrap();
    assert!(first
        .run(first_engine.signal(&second_cell, 9))
        .await?
        .is_err());
    let original_second = second_probe.container.lock().unwrap().clone();
    *second_probe.container.lock().unwrap() = first_probe.container.lock().unwrap().clone();
    assert_eq!(
        second
            .run(call(&second_host, "Sandbox:same-business-id", "/foreign"))
            .await??,
        true
    );
    *second_probe.container.lock().unwrap() = original_second;
    first
        .run(call(
            &first_host,
            "Sandbox:same-business-id",
            "/retain-exec",
        ))
        .await??;
    second
        .run(call(
            &second_host,
            "Sandbox:same-business-id",
            "/retain-exec",
        ))
        .await??;
    let first_process = first
        .run(async { cellar::container::process(1).unwrap() })
        .await?;
    let second_process = second
        .run(async { cellar::container::process(1).unwrap() })
        .await?;
    assert!(
        !Arc::ptr_eq(&first_process, &second_process),
        "exec IDs are local to their owner"
    );
    assert!(second.run(first_process.kill(9)).await?.is_err());
    second
        .run(async {
            drop(first_probe.exec.lock().unwrap().take());
            assert!(
                Arc::ptr_eq(&second_process, &cellar::container::process(1).unwrap()),
                "dropping a foreign handle must not remove this host's colliding exec ID"
            );
        })
        .await?;
    assert!(
        first
            .run(async { cellar::container::process(1).is_none() })
            .await?
    );
    first
        .run(call(
            &first_host,
            "Sandbox:same-business-id",
            "/retain-exec",
        ))
        .await??;
    let retained = first
        .run(async { cellar::container::process(2).unwrap() })
        .await?;
    let (first_name, first_labels) = {
        let calls = peer.calls.lock().unwrap();
        let creates = calls
            .iter()
            .filter(|(_, path, _)| path.starts_with("/containers/create?name="))
            .collect::<Vec<_>>();
        assert_eq!(creates.len(), 2);
        assert_eq!(creates[0].2["Image"], first_image);
        assert_eq!(creates[1].2["Image"], second_image);
        assert_eq!(creates[0].2["HostConfig"]["Runtime"], "first-runtime");
        assert_eq!(creates[1].2["HostConfig"]["Runtime"], "second-runtime");
        assert_ne!(creates[0].1, creates[1].1);
        assert_ne!(
            creates[0].2["Labels"]["cellar.host"],
            creates[1].2["Labels"]["cellar.host"]
        );
        let fences = calls
            .iter()
            .filter(|(_, path, _)| path == "/containers/create?")
            .collect::<Vec<_>>();
        assert_eq!(fences.len(), 2);
        assert_ne!(fences[0].2["Env"], fences[1].2["Env"]);
        for fence in fences {
            assert!(fence.2["Env"][0]
                .as_str()
                .unwrap()
                .contains("table inet cellar_"));
        }
        (
            creates[0]
                .1
                .strip_prefix("/containers/create?name=")
                .unwrap()
                .to_string(),
            creates[0].2["Labels"].clone(),
        )
    };
    {
        let networks = peer.networks.lock().unwrap();
        assert_eq!(networks.len(), 4);
        let bridges = networks
            .values()
            .map(|network| {
                network["Options"]["com.docker.network.bridge.name"]
                    .as_str()
                    .unwrap()
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(bridges.len(), 4);
        assert!(bridges.iter().all(|name| name.len() <= 15));
    }
    first.close();
    // Repeating closure must not abort the cleanup it just scheduled.
    first.close();
    first.shutdown().await;
    assert_eq!(peer.containers.lock().unwrap().len(), 1);
    assert!(second.run(retained.wait()).await?.is_err());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if peer
                .execs
                .lock()
                .unwrap()
                .get("exec3")
                .unwrap()
                .load(Ordering::SeqCst)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        !peer
            .execs
            .lock()
            .unwrap()
            .get("exec2")
            .unwrap()
            .load(Ordering::SeqCst),
        "the sibling host's stdin remains open"
    );
    assert!(first_engine.cell("Sandbox:same-business-id").is_none());
    assert!(second_cell.running());
    // Simulate infrastructure left behind by a crashed owner. Stable state
    // identity reaps that owner's resources without touching its sibling.
    peer.labels
        .lock()
        .unwrap()
        .insert(first_name.clone(), first_labels);
    peer.containers
        .lock()
        .unwrap()
        .insert(first_name.clone(), watch::channel(None).0);
    let restarted = ExecutionDomain::new(tokio::runtime::Handle::current());
    restarted
        .services()
        .configure_containers(cellar::container::EngineOptions {
            socket: socket.clone(),
            namespace: None,
            runtime: None,
            dns: Vec::new(),
        })?;
    restarted
        .run(async {
            cellar::container::configure("test".into(), None, first_path)?;
            cellar::container::install_specs(
                Vec::new(),
                Some(cellar::container::image_reference(&"b".repeat(64))),
                Vec::new(),
            );
            cellar::container::engine().await?.ensure_fence().await
        })
        .await??;
    assert!(!peer.containers.lock().unwrap().contains_key(&first_name));
    assert_eq!(peer.containers.lock().unwrap().len(), 1);
    assert_eq!(
        peer.networks.lock().unwrap().len(),
        4,
        "restart reuses its own bridges"
    );
    restarted.shutdown().await;
    assert_eq!(
        second
            .run(call(
                &second_host,
                "Sandbox:same-business-id",
                "/retained-exec-output"
            ))
            .await??,
        json!({"code":7,"stdout":"surviving host"})
    );
    assert_eq!(
        second
            .run(call(&second_host, "Sandbox:same-business-id", "/retained"))
            .await??,
        true
    );
    second.shutdown().await;
    assert!(peer.containers.lock().unwrap().is_empty());
    server.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renamed_resource_and_foreign_name_reuse_preserve_run_identity() -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(30),
        immutable_resource_identity(IdentityScenario::NameReuse),
    )
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_name_adoption_preserves_owned_idle_cleanup() -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(30),
        immutable_resource_identity(IdentityScenario::FailedAdoptionExpiry),
    )
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successful_reactivation_cancels_only_its_previous_idle_cleanup() -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(30),
        immutable_resource_identity(IdentityScenario::Reactivation),
    )
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_inactivity_window_retires_the_old_timer() -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(30),
        immutable_resource_identity(IdentityScenario::ReplacementWindow),
    )
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reactivation_joins_idle_removal_already_admitted_by_the_engine() -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(30),
        immutable_resource_identity(IdentityScenario::AdmittedRemoval),
    )
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_release_joins_a_pending_idle_timer() -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(30),
        immutable_resource_identity(IdentityScenario::ExplicitRelease),
    )
    .await?
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum IdentityScenario {
    NameReuse,
    FailedAdoptionExpiry,
    Reactivation,
    ReplacementWindow,
    AdmittedRemoval,
    ExplicitRelease,
}

fn completed_identity_peer_connection(
    outcome: std::result::Result<std::result::Result<(), hyper::Error>, tokio::task::JoinError>,
    stopping: bool,
) -> Result<()> {
    match outcome {
        Ok(Ok(())) => Ok(()),
        Err(error) if stopping && error.is_cancelled() => Ok(()),
        Err(error) => Err(error).context("identity peer task failed"),
        Ok(Err(error)) => {
            // macOS reports ENOTCONN when HTTP shutdown follows the caller's
            // completed response and socket close. Every request/result is
            // still checked by the client; other protocol failures remain fatal.
            let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
            while let Some(current) = cause {
                if current
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.raw_os_error() == Some(libc::ENOTCONN))
                {
                    return Ok(());
                }
                cause = current.source();
            }
            Err(error).context("identity peer protocol failed")
        }
    }
}

async fn immutable_resource_identity(scenario: IdentityScenario) -> Result<()> {
    let directory = tempfile::tempdir_in("/tmp")?;
    let socket = directory.path().join("engine.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let peer = Arc::new(Engine::default());
    let serving = peer.clone();
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            cellar::asyncrt::select_biased! { "the identity peer stops admission before cancelling its connections";
                _ = &mut stopped => break,
                joined = connections.join_next(), if !connections.is_empty() => { completed_identity_peer_connection(joined.unwrap(), false)?; }
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    let engine = serving.clone();
                    connections.spawn(async move {
                        let service = hyper::service::service_fn(move |request| engine.clone().answer(request));
                        hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).with_upgrades().await
                    });
                }
            }
        }
        connections.abort_all();
        while let Some(joined) = connections.join_next().await {
            completed_identity_peer_connection(joined, true)?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let domain = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let docker = cellar::docker::Docker::new(&socket);
    let mut foreign = None;
    let result = async {
        let scope = "Sandbox:same-business-id";
        let probe = Arc::new(Probe::default());
        let host = embedded_container_host(
            &domain,
            &directory.path().join("cells"),
            &socket,
            &cellar::container::image_reference(&"c".repeat(64)),
            "runc",
            probe.clone(),
        )
        .await?;
        let started = domain.run(call(&host, scope, "/start")).await??;
        anyhow::ensure!(
            started["run"] == 1,
            "owned fixture failed to start: {started}"
        );
        let engine = domain
            .run(async { cellar::container::engine_if_ready().unwrap() })
            .await?;
        let cell = engine.cell("Sandbox:same-business-id").unwrap();
        let name = peer
            .containers
            .lock()
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone();
        let original = peer.resource_id(&name);
        if scenario == IdentityScenario::ExplicitRelease {
            cell.set_inactivity(Duration::from_secs(60));
            domain
                .run(engine.release(scope, cellar::container::Release::Keep))
                .await??;
            let retired = Arc::downgrade(&cell);
            domain
                .run(engine.release(scope, cellar::container::Release::Destroy))
                .await??;
            domain.run(host.swap_out_cell(scope, 1)).await??;
            probe.container.lock().unwrap().take();
            probe.context.lock().unwrap().take();
            drop(cell);
            tokio::time::timeout(Duration::from_secs(2), async {
                while retired.upgrade().is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .context("explicit release retained the removed cell until its idle deadline")?;
            anyhow::ensure!(!peer.containers.lock().unwrap().contains_key(&name));
            peer.assert_immutable_effects()?;
            return Ok(());
        }
        if scenario == IdentityScenario::AdmittedRemoval {
            let (release, waiting) = watch::channel(false);
            *peer.destroy_gate.lock().unwrap() = Some(waiting);
            cell.set_inactivity(Duration::ZERO);
            domain
                .run(engine.release(scope, cellar::container::Release::Keep))
                .await??;
            tokio::time::timeout(Duration::from_secs(2), async {
                while !peer.destroy_entered.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .context("idle removal did not enter the Engine")?;
            let mut adoption = Box::pin(domain.run(engine.attach(scope, "Sandbox")));
            anyhow::ensure!(
                tokio::time::timeout(Duration::from_millis(100), adoption.as_mut())
                    .await
                    .is_err(),
                "reactivation interrupted an entered idle removal"
            );
            release.send_replace(true);
            let adopted = tokio::time::timeout(Duration::from_secs(2), adoption).await???;
            anyhow::ensure!(
                !Arc::ptr_eq(&adopted, &cell)
                    && !adopted.running()
                    && engine
                        .cell(scope)
                        .is_some_and(|current| Arc::ptr_eq(&current, &adopted))
                    && !peer.containers.lock().unwrap().contains_key(&name),
                "reactivation reused the resource after its joined removal"
            );
            peer.assert_immutable_effects()?;
            return Ok(());
        }
        if matches!(
            scenario,
            IdentityScenario::Reactivation | IdentityScenario::ReplacementWindow
        ) {
            cell.set_inactivity(Duration::from_millis(500));
            domain
                .run(engine.release(scope, cellar::container::Release::Keep))
                .await??;
            if scenario == IdentityScenario::Reactivation {
                let reactivated = domain.run(engine.attach(scope, "Sandbox")).await??;
                anyhow::ensure!(
                    Arc::ptr_eq(&cell, &reactivated),
                    "reactivation changed the cell handle"
                );
            } else {
                cell.set_inactivity(Duration::from_secs(2));
                domain
                    .run(engine.release(scope, cellar::container::Release::Keep))
                    .await??;
            }
            // Observe beyond the old deadline; the successful reactivation
            // must leave no timer that can delete its live resource.
            tokio::time::sleep(Duration::from_secs(1)).await;
            anyhow::ensure!(
                engine
                    .cell(scope)
                    .is_some_and(|current| Arc::ptr_eq(&current, &cell))
                    && cell.running()
                    && peer.containers.lock().unwrap().contains_key(&name),
                "the previous idle timer removed a successfully reactivated container"
            );
            if scenario == IdentityScenario::Reactivation {
                domain
                    .run(engine.release(scope, cellar::container::Release::Keep))
                    .await??;
            }
            tokio::time::timeout(Duration::from_secs(2), async {
                while engine.cell(scope).is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .context("the current inactivity window did not remove the resource")?;
            anyhow::ensure!(!peer.containers.lock().unwrap().contains_key(&name));
            peer.assert_immutable_effects()?;
            return Ok(());
        }
        // Capture authority in this host, then poll the waiter after rename.
        let monitoring = domain
            .run(std::future::poll_fn(|_| {
                std::task::Poll::Ready(engine.monitor(&cell, 1))
            }))
            .await?;
        docker
            .expect(
                "POST",
                &format!("/containers/{original}/rename?name={name}-renamed"),
                None,
                "rename owned fixture resource",
            )
            .await?;
        let created = docker
            .expect(
                "POST",
                &format!("/containers/create?name={name}"),
                Some(json!({
                    "Image": "foreign",
                    "Labels": {"cellar.host": "foreign", "cellar.node": "foreign"}
                })),
                "create foreign fixture resource",
            )
            .await?
            .json()?;
        let replacement = created["Id"].as_str().context("replacement ID")?.to_owned();
        foreign = Some((name.clone(), replacement.clone()));
        anyhow::ensure!(original != replacement, "fixture reused an immutable ID");
        if scenario == IdentityScenario::FailedAdoptionExpiry {
            cell.set_inactivity(Duration::from_secs(1));
            domain
                .run(engine.release(scope, cellar::container::Release::Keep))
                .await??;
        }
        anyhow::ensure!(
            domain.run(engine.attach(scope, "Sandbox")).await?.is_err(),
            "name discovery must refuse foreign labels before adoption"
        );
        if scenario == IdentityScenario::FailedAdoptionExpiry {
            tokio::time::timeout(Duration::from_secs(2), async {
                while engine.cell(scope).is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .context("failed adoption disarmed the original idle cleanup")?;
            anyhow::ensure!(
                !peer
                    .containers
                    .lock()
                    .unwrap()
                    .contains_key(&format!("{name}-renamed")),
                "idle cleanup leaked its original immutable resource"
            );
            anyhow::ensure!(
                monitoring.await.map_err(anyhow::Error::msg)? == 137,
                "idle cleanup did not finish the original run"
            );
            peer.assert_immutable_effects()?;
            return Ok(());
        }
        anyhow::ensure!(domain.run(call(&host, scope, "/files")).await?? == true);
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("path", "/tmp/cellar-?é.bin")
            .finish();
        let untouched = docker
            .call(
                "HEAD",
                &format!("/containers/{replacement}/archive?{query}"),
                None,
            )
            .await?;
        anyhow::ensure!(
            untouched.status.as_u16() == 404,
            "owned file writes reached the replacement resource"
        );
        domain.run(call(&host, scope, "/exec")).await??;
        anyhow::ensure!(domain.run(call(&host, scope, "/signal")).await?? == true);
        anyhow::ensure!(domain.run(call(&host, scope, "/destroy")).await?? == true);
        let observed = tokio::time::timeout(Duration::from_secs(2), monitoring)
            .await?
            .map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            observed == 137,
            "the old run monitor observed the replacement resource"
        );
        anyhow::ensure!(
            peer.containers
                .lock()
                .unwrap()
                .get(&name)
                .is_some_and(|exit| exit.borrow().is_none()),
            "owned cleanup stopped the foreign replacement"
        );
        let refused = domain.run(call(&host, scope, "/start")).await??;
        anyhow::ensure!(
            refused["error"]
                .as_str()
                .is_some_and(|error| error.contains("ownership labels")),
            "restart must refuse an occupied foreign name"
        );
        anyhow::ensure!(peer
            .containers
            .lock()
            .unwrap()
            .get(&name)
            .is_some_and(|exit| exit.borrow().is_none()));
        peer.assert_immutable_effects()?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    domain.shutdown().await;
    let processes = tokio::time::timeout(Duration::from_secs(2), peer.join_processes()).await;
    let preserved = foreign.as_ref().is_none_or(|(name, id)| {
        peer.containers
            .lock()
            .unwrap()
            .get(name)
            .is_some_and(|exit| exit.borrow().is_none())
            && peer.resource_id(name) == *id
    });
    let cleanup = match foreign {
        Some((_, id)) => docker
            .expect(
                "DELETE",
                &format!("/containers/{id}?force=true"),
                None,
                "remove foreign test fixture",
            )
            .await
            .map(|_| ()),
        None => Ok(()),
    };
    let _ = stop.send(());
    let joined = server.await;
    result.with_context(|| format!("identity campaign server outcome={joined:?}, processes={processes:?}, cleanup={cleanup:?}"))?;
    anyhow::ensure!(
        preserved,
        "domain shutdown removed a foreign name replacement"
    );
    cleanup?;
    processes??;
    joined??;
    Ok(())
}
