// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! SDK control and ownership contracts run through production native cells.
//! A deterministic Engine API peer supplies external process outcomes.
#![allow(clippy::disallowed_methods)]
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::{
    application::{
        containers::*, http, sockets, Cell, Context, HandlerFuture, Invocation, Output, Registry,
    },
    container::ContainerSpec,
    engine_api::RuntimeFetch,
    generation::Generation,
    runtime::RuntimeManager,
};
use http_body_util::{BodyExt, Full};
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
                "/monitor-empty" => json!(container.monitor(&context).await.is_err()),
                _ => return Err(anyhow!("unknown scenario {path}")),
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

#[derive(Default)]
struct Engine {
    refuse_remove: AtomicBool,
    app_image_loaded: AtomicBool,
    containers: Mutex<HashMap<String, watch::Sender<Option<i64>>>>,
    execs: Mutex<HashMap<String, Arc<AtomicBool>>>,
    calls: Mutex<Vec<(String, String, Value)>>,
    http_port: u16,
    tcp_port: u16,
}
impl Engine {
    async fn answer(
        self: Arc<Self>,
        mut request: HttpRequest<Incoming>,
    ) -> Result<Response<Full<Bytes>>> {
        let path = request
            .uri()
            .path()
            .strip_prefix("/v1.43")
            .unwrap_or(request.uri().path())
            .to_string();
        let query = request.uri().query().unwrap_or("").to_string();
        let method = request.method().as_str().to_string();
        let body = request.body_mut().collect().await?.to_bytes();
        let body: Value = if path == "/images/load" {
            json!({"bytes":body.len()})
        } else if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body)?
        };
        self.calls
            .lock()
            .unwrap()
            .push((method.clone(), format!("{path}?{query}"), body.clone()));
        let mut status = 200;
        let value = if path == "/containers/json" {
            json!([])
        } else if path == "/images/load" {
            self.app_image_loaded.store(true, Ordering::SeqCst);
            json!({"stream":"Loaded image"})
        } else if path.starts_with("/images/") {
            if path
                == format!(
                    "/images/{}/json",
                    cellar::container::image_reference(&"a".repeat(64))
                )
                && !self.app_image_loaded.load(Ordering::SeqCst)
            {
                status = 404;
            }
            json!({"Config":{}})
        } else if path.starts_with("/networks/") && method == "GET" {
            status = 404;
            json!({})
        } else if path == "/containers/create" {
            if query.is_empty() {
                json!({"Id":"fence"})
            } else {
                let name = query.strip_prefix("name=").unwrap().to_string();
                self.containers
                    .lock()
                    .unwrap()
                    .insert(name.clone(), watch::channel(None).0);
                json!({"Id":name})
            }
        } else if path == "/containers/fence/wait" {
            json!({"StatusCode":0})
        } else if path.ends_with("/wait") {
            let name = path
                .trim_start_matches("/containers/")
                .trim_end_matches("/wait");
            let sender = self.containers.lock().unwrap().get(name).cloned().unwrap();
            let mut receiver = sender.subscribe();
            let code = loop {
                if let Some(code) = *receiver.borrow_and_update() {
                    break code;
                }
                receiver.changed().await?;
            };
            json!({"StatusCode":code})
        } else if path.starts_with("/containers/") && path.ends_with("/json") {
            let name = path
                .trim_start_matches("/containers/")
                .trim_end_matches("/json");
            let running = self
                .containers
                .lock()
                .unwrap()
                .get(name)
                .map(|sender| sender.borrow().is_none());
            match running {
                Some(running) => {
                    json!({"State":{"Running":running}, "NetworkSettings":{"IPAddress":"127.0.0.1","Ports":{
                        format!("{}/tcp",self.http_port):[{"HostPort":self.http_port.to_string()}],
                        format!("{}/tcp",self.tcp_port):[{"HostPort":self.tcp_port.to_string()}]
                    }}})
                }
                None => {
                    status = 404;
                    json!({})
                }
            }
        } else if path.starts_with("/containers/")
            && (method == "DELETE" || path.ends_with("/kill"))
        {
            let name = path
                .trim_start_matches("/containers/")
                .trim_end_matches("/kill");
            if method == "DELETE" || query.contains("SIGKILL") || query.ends_with("=9") {
                if let Some(sender) = self.containers.lock().unwrap().get(name) {
                    sender.send_replace(Some(137));
                }
            }
            if method == "DELETE" {
                if self.refuse_remove.load(Ordering::SeqCst) {
                    status = 500;
                } else {
                    self.containers.lock().unwrap().remove(name);
                }
            }
            json!({})
        } else if path.starts_with("/containers/") && path.ends_with("/exec") {
            let mut execs = self.execs.lock().unwrap();
            let id = format!("exec{}", execs.len() + 1);
            execs.insert(id.clone(), Arc::new(AtomicBool::new(false)));
            json!({"Id":id})
        } else if path.starts_with("/exec/") && path.ends_with("/json") {
            let id = path.trim_start_matches("/exec/").trim_end_matches("/json");
            let ended = self
                .execs
                .lock()
                .unwrap()
                .get(id)
                .unwrap()
                .load(Ordering::SeqCst);
            json!({"Pid":77,"Running":!ended,"ExitCode":7})
        } else if path.starts_with("/exec/") && path.ends_with("/start") && body["Detach"] == false
        {
            let id = path.trim_start_matches("/exec/").trim_end_matches("/start");
            let ended = self.execs.lock().unwrap().get(id).unwrap().clone();
            let upgrade = hyper::upgrade::on(&mut request);
            tokio::spawn(async move {
                let mut stream = TokioIo::new(upgrade.await.unwrap());
                let mut stdin = Vec::new();
                stream.read_to_end(&mut stdin).await.unwrap();
                for (channel, bytes) in [(1u8, stdin.as_slice()), (2, b"stderr".as_slice())] {
                    let mut header = [0u8; 8];
                    header[0] = channel;
                    header[4..].copy_from_slice(&(bytes.len() as u32).to_be_bytes());
                    if stream.write_all(&header).await.is_err()
                        || stream.write_all(bytes).await.is_err()
                    {
                        break;
                    }
                }
                ended.store(true, Ordering::SeqCst);
                let _ = stream.shutdown().await;
            });
            return Ok(Response::builder()
                .status(101)
                .header("connection", "Upgrade")
                .header("upgrade", "tcp")
                .body(Full::new(Bytes::new()))?);
        } else {
            json!({"Id":"network"})
        };
        Ok(Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(serde_json::to_vec(&value)?)))?)
    }
    fn app_creates(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, path, _)| path.starts_with("/containers/create?name="))
            .count()
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
    engine
        .release("Sandbox:test", cellar::container::Release::Destroy)
        .await?;
    gate_task.abort();
    http_task.abort();
    tcp_task.abort();
    engine_task.abort();
    Ok(())
}
