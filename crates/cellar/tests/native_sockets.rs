// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Socket APIs run through real native cells; proof verdicts are explicit adapters.
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::{
    application::{
        loader::{Outbound, WorkerCode},
        sockets::*,
        Cell, Context, HandlerFuture, Invocation, Output, Registry, Request,
    },
    engine_api::{HttpResponse, RuntimeFetch},
    generation::Generation,
    runtime::RuntimeManager,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, Notify},
};
const SCOPE: &str = "SocketCell:one";
#[derive(Debug, PartialEq)]
enum PeerEvent {
    Accepted,
    Bytes(Vec<u8>),
    Closed,
}
struct Probe {
    echo: String,
    half: String,
    hold: String,
    tls: String,
    hold_gate: AtomicBool,
    retained: Mutex<Option<Socket>>,
    old: Mutex<Option<Context>>,
    background: Notify,
    close_opening: Notify,
    pending_read: Notify,
    echo_accepts: AtomicUsize,
}
fn output(value: Value) -> Result<Output> {
    Ok(Output::Http(HttpResponse {
        status: 200,
        body: serde_json::to_vec(&value)?,
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    }))
}
async fn bytes(reader: &Reader, context: &Context) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = [0; 1];
    assert_eq!(reader.read_into(context, &mut []).await?, 0);
    loop {
        let size = reader.read_into(context, &mut buffer).await?;
        if size == 0 {
            break;
        }
        output.extend_from_slice(&buffer[..size]);
    }
    reader.closed().await?;
    Ok(output)
}
struct Root(Arc<Probe>);
impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("expected fetch"));
            };
            let path = url::Url::parse(&request.url)?.path().to_owned();
            let p = &self.0;
            match path.as_str() {
                "/validate" => output(json!([
                    "localhost",
                    "[",
                    "localhost:1/path",
                    "localhost:1?x",
                    ""
                ]
                .map(|address| SocketAddress::try_from(address).unwrap_err().to_string()))),
                "/echo" | "/gated-connect" | "/rejected-connect" | "/release" => {
                    if path.contains("connect") {
                        context
                            .storage()?
                            .put_json("before-connect", &json!(true))?;
                    }
                    let socket = context.connect(p.echo.clone(), SocketOptions::default())?;
                    if path == "/rejected-connect" {
                        let error = socket.opened().await.unwrap_err().to_string();
                        assert_eq!(socket.closed().await.unwrap_err().to_string(), error);
                        return output(json!({"refused":error.contains("refusing to send")}));
                    }
                    let info = socket.opened().await?;
                    let reader = socket.readable.get_reader()?;
                    let reader_exclusive = socket.readable.get_reader().is_err();
                    let reader = if path == "/release" {
                        let pending = reader.read(&context);
                        let queued = reader.read(&context);
                        reader.release_lock();
                        let next = socket.readable.get_reader()?;
                        drop(reader); // Must not release the new reader's lease.
                        assert!(socket.readable.get_reader().is_err());
                        assert!(pending.await.unwrap_err().to_string().contains("released"));
                        assert!(queued.await.unwrap_err().to_string().contains("released"));
                        next
                    } else {
                        reader
                    };
                    let first = socket.writable.get_writer()?;
                    let writer_exclusive = socket.writable.get_writer().is_err();
                    let a = first.write(&context, b"A".to_vec());
                    first.release_lock();
                    let writer = socket.writable.get_writer()?;
                    drop(first); // An accepted write survives releasing its writer.
                    assert!(socket.writable.get_writer().is_err());
                    let b = writer.write(&context, b"B".to_vec());
                    writer.close(&context).await?;
                    b.await?;
                    a.await?;
                    writer.closed().await?;
                    let first_read = reader.read(&context);
                    let second_read = reader.read(&context);
                    let second = second_read.await?;
                    let mut data = first_read.await?;
                    data.extend(second);
                    data.extend(bytes(&reader, &context).await?);
                    socket.closed().await?;
                    output(
                        json!({"bytes":data,"metadata":[info.remote_address.is_some(),info.local_address.is_some()],"exclusive":[reader_exclusive,writer_exclusive],"mode":"off","closed":true,"lateWriteRejected":writer.write(&context,vec![3]).await.is_err()}),
                    )
                }
                "/half" | "/default-half" => {
                    let half = path == "/half";
                    let socket = context.connect(
                        p.half.clone(),
                        SocketOptions {
                            allow_half_open: half,
                            ..SocketOptions::default()
                        },
                    )?;
                    let reader = socket.readable.get_reader()?;
                    let data = bytes(&reader, &context).await?;
                    let writer = socket.writable.get_writer()?;
                    let wrote = writer.write(&context, b"after".to_vec()).await.is_ok();
                    if half {
                        writer.close(&context).await?;
                        socket.close(&context).await?;
                    }
                    socket.closed().await?;
                    output(json!({"bytes":data,"halfOpen":half,"after":wrote}))
                }
                "/retain" | "/pending" | "/pending-tls" | "/close-opening" | "/background" => {
                    let socket = context.connect(
                        p.hold.clone(),
                        SocketOptions {
                            secure_transport: if path == "/pending-tls" || path == "/close-opening"
                            {
                                SecureTransport::On
                            } else {
                                SecureTransport::Off
                            },
                            ..SocketOptions::default()
                        },
                    )?;
                    *p.retained.lock().unwrap() = Some(socket.clone());
                    *p.old.lock().unwrap() = Some(context.clone());
                    if path == "/close-opening" {
                        p.close_opening.notified().await;
                        socket.close(&context).await?;
                        socket.closed().await?;
                        assert!(socket.opened().await.is_err());
                        return output(json!(true));
                    }
                    socket.opened().await?;
                    if path == "/pending" {
                        p.pending_read.notify_one();
                        socket.readable.get_reader()?.read(&context).await?;
                        return Err(anyhow!("pending read unexpectedly resumed"));
                    }
                    if path == "/background" {
                        let probe = self.0.clone();
                        let owner = context.clone();
                        context.wait_until(async move {
                            probe.background.notified().await;
                            let writer = socket.writable.get_writer()?;
                            writer.write(&owner, b"background".to_vec()).await?;
                            writer.close(&owner).await?;
                            socket.close(&owner).await?;
                            Ok(())
                        })?;
                    }
                    output(json!(true))
                }
                "/foreign" => {
                    let socket = p.retained.lock().unwrap().clone().unwrap();
                    let old = p.old.lock().unwrap().clone().unwrap();
                    assert!(socket.opened().await?.remote_address.is_some());
                    assert!(socket.close(&context).await.is_err());
                    assert!(socket.readable.get_reader()?.read(&context).await.is_err());
                    assert!(socket
                        .writable
                        .get_writer()?
                        .write(&context, vec![1])
                        .await
                        .is_err());
                    assert!(old
                        .connect(p.hold.clone(), SocketOptions::default())
                        .is_err());
                    output(json!(true))
                }
                "/gated-write" | "/rejected-write" | "/gated-close" | "/rejected-upgrade" => {
                    let socket = context.connect(
                        p.hold.clone(),
                        SocketOptions {
                            secure_transport: if path == "/rejected-upgrade" {
                                SecureTransport::StartTls
                            } else {
                                SecureTransport::Off
                            },
                            ..SocketOptions::default()
                        },
                    )?;
                    socket.opened().await?;
                    context.storage()?.put_json("before-output", &json!(path))?;
                    p.hold_gate.store(true, Ordering::SeqCst);
                    if path == "/rejected-upgrade" {
                        let tls = socket.start_tls(&context, Some("localhost".into()))?;
                        let error = tls.opened().await.unwrap_err().to_string();
                        assert!(error.contains("refusing to send"));
                        assert_eq!(tls.closed().await.unwrap_err().to_string(), error);
                        assert_eq!(socket.closed().await.unwrap_err().to_string(), error);
                        return output(json!({"refused":true}));
                    }
                    if path == "/gated-close" {
                        let refused = socket.close(&context).await.is_err();
                        socket.closed().await?;
                        return output(json!({"refused":refused}));
                    }
                    let writer = socket.writable.get_writer()?;
                    let first = writer.write(&context, b"secret".to_vec());
                    let second = writer.write(&context, b"second".to_vec());
                    if path == "/rejected-write" {
                        let first = first.await.unwrap_err().to_string();
                        assert_eq!(second.await.unwrap_err().to_string(), first);
                        assert_eq!(writer.closed().await.unwrap_err().to_string(), first);
                        assert_eq!(socket.closed().await.unwrap_err().to_string(), first);
                        return output(json!({"refused":true}));
                    }
                    second.await?;
                    first.await?;
                    writer.close(&context).await?;
                    socket.close(&context).await?;
                    output(json!(true))
                }
                "/tls-untrusted" | "/upgrade-untrusted" => {
                    let upgrade = path == "/upgrade-untrusted";
                    let socket = context.connect(
                        p.tls.clone(),
                        SocketOptions {
                            secure_transport: if upgrade {
                                SecureTransport::StartTls
                            } else {
                                SecureTransport::On
                            },
                            ..SocketOptions::default()
                        },
                    )?;
                    if upgrade {
                        socket.opened().await?;
                        let tls = socket.start_tls(&context, Some("localhost".into()))?;
                        let single = socket.start_tls(&context, None).is_err();
                        assert!(tls
                            .opened()
                            .await
                            .unwrap_err()
                            .to_string()
                            .contains("TLS handshake failed"));
                        let old_reader = socket.readable.get_reader()?;
                        let eof = old_reader.read(&context).await?.is_empty();
                        old_reader.closed().await?;
                        output(
                            json!({"mode":"on","singleUse":single,"oldWriteRejected":socket.writable.get_writer()?.write(&context,vec![1]).await.is_err(),"oldReadEof":eof,"newClosedRejected":tls.closed().await.is_err(),"oldClosedRejected":socket.closed().await.is_err()}),
                        )
                    } else {
                        let error = socket.opened().await.unwrap_err().to_string();
                        assert!(error.contains("TLS handshake failed"));
                        assert_eq!(socket.closed().await.unwrap_err().to_string(), error);
                        assert!(socket
                            .writable
                            .get_writer()?
                            .write(&context, vec![1])
                            .await
                            .is_err());
                        assert!(socket.readable.get_reader()?.read(&context).await.is_err());
                        output(json!({"untrustedRejected":true}))
                    }
                }
                "/deny" | "/broker" => {
                    let broker = path == "/broker";
                    let address = p.echo.clone();
                    let application = Registry::new(move |context, _| {
                        let address = address.clone();
                        Box::pin(async move {
                            let error = context
                                .connect(address, SocketOptions::default())
                                .err()
                                .unwrap()
                                .to_string();
                            assert!(
                                error.contains(if broker {
                                    "cannot broker connect"
                                } else {
                                    "not permitted to access the internet"
                                }),
                                "{error}"
                            );
                            output(json!(true))
                        })
                    });
                    let mut code = WorkerCode::new(Arc::new(application));
                    code.outbound = if broker {
                        Outbound::Broker(context.exported_entrypoint("default")?)
                    } else {
                        Outbound::Deny
                    };
                    let worker = context.loader("WORKERS")?.load(&context, code)?;
                    Ok(Output::Http(
                        worker
                            .entrypoint("default")
                            .fetch(
                                &context,
                                Request {
                                    url: "https://loaded/run".into(),
                                    method: "GET".into(),
                                    headers: Vec::new(),
                                    body: Vec::new().into(),
                                    entrypoint: None,
                                },
                            )
                            .await?,
                    ))
                }
                _ => Err(anyhow!("unknown path {path}")),
            }
        })
    }
}
fn request(path: &str) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("https://socket{path}"),
        method: "GET".into(),
        headers: Vec::new(),
        body: Vec::new().into(),
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    }
}
async fn call(runtime: &RuntimeManager, path: &str) -> Result<Value> {
    eprintln!("socket scenario {path}");
    let response = cellar::asyncrt::timeout(
        Duration::from_secs(10),
        runtime.fetch_cell(SCOPE.into(), None, request(path), None),
    )
    .await
    .map_err(|error| anyhow!("socket scenario {path}: {error}"))??;
    eprintln!("socket scenario {path} answered");
    assert_eq!(response.status, 200);
    Ok(serde_json::from_slice(&response.body)?)
}
async fn open(runtime: &RuntimeManager) -> Result<()> {
    runtime
        .restore_cell(
            SCOPE,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(SCOPE.into(), 1, true).await?;
    runtime.publish_cell(SCOPE, 1)
}
async fn next(events: &mut mpsc::UnboundedReceiver<PeerEvent>) -> PeerEvent {
    let event = events.recv().await.unwrap();
    match &event {
        PeerEvent::Bytes(bytes) => eprintln!("socket peer bytes({})", bytes.len()),
        event => eprintln!("socket peer {event:?}"),
    }
    event
}
async fn until_closed(events: &mut mpsc::UnboundedReceiver<PeerEvent>) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        match next(events).await {
            PeerEvent::Bytes(chunk) => bytes.extend(chunk),
            PeerEvent::Closed => return bytes,
            PeerEvent::Accepted => panic!("unexpected additional socket"),
        }
    }
}

// These peers run outside the cell execution boundary and are owned by the test.
#[allow(clippy::disallowed_methods)]
async fn run() -> Result<()> {
    let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let half = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let hold = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let tls = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let p = Arc::new(Probe {
        echo: echo.local_addr()?.to_string(),
        half: half.local_addr()?.to_string(),
        hold: hold.local_addr()?.to_string(),
        tls: format!("localhost:{}", tls.local_addr()?.port()),
        hold_gate: AtomicBool::new(false),
        retained: Mutex::new(None),
        old: Mutex::new(None),
        background: Notify::new(),
        close_opening: Notify::new(),
        pending_read: Notify::new(),
        echo_accepts: AtomicUsize::new(0),
    });
    let (half_send, mut half_events) = mpsc::unbounded_channel();
    let (hold_send, mut hold_events) = mpsc::unbounded_channel();
    let (tls_send, mut tls_events) = mpsc::unbounded_channel();
    let probe = p.clone();
    let echo_task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = echo.accept().await {
            probe.echo_accepts.fetch_add(1, Ordering::SeqCst);
            let mut bytes = Vec::new();
            stream.read_to_end(&mut bytes).await.unwrap();
            stream.write_all(&bytes).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    let half_task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = half.accept().await {
            stream.write_all(b"hello").await.unwrap();
            stream.shutdown().await.unwrap();
            let mut bytes = Vec::new();
            let _ = stream.read_to_end(&mut bytes).await;
            let _ = half_send.send(bytes);
        }
    });
    let hold_task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = hold.accept().await {
            let _ = hold_send.send(PeerEvent::Accepted);
            loop {
                let mut buffer = [0; 4096];
                match stream.read(&mut buffer).await {
                    Ok(0) | Err(_) => {
                        let _ = hold_send.send(PeerEvent::Closed);
                        break;
                    }
                    Ok(size) => {
                        let _ = hold_send.send(PeerEvent::Bytes(buffer[..size].to_vec()));
                    }
                }
            }
        }
    });
    let tls_task = tokio::spawn(async move {
        let certificate = rustls::pki_types::CertificateDer::from(
            include_bytes!("../host_tcp/fixtures/server.der").to_vec(),
        );
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../host_tcp/fixtures/key.der").to_vec(),
        );
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![certificate], key.into())
                .unwrap(),
        ));
        while let Ok((stream, _)) = tls.accept().await {
            assert!(
                acceptor.accept(stream).await.is_err(),
                "the SDK must reject an untrusted test certificate"
            );
            let _ = tls_send.send(());
        }
    });
    let (gate_send, mut gates) = mpsc::unbounded_channel::<cellar::host_channels::GateReq>();
    let (held_send, mut held) = mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gate_send);
    let probe = p.clone();
    let gate_task = cellar::asyncrt::spawn(async move {
        while let Some(gate) = gates.recv().await {
            if gate.ticket.channel == cellar_logic::Channel::Tcp
                && probe.hold_gate.load(Ordering::SeqCst)
            {
                let _ = held_send.send(gate);
            } else {
                let _ = gate.reply.send(Ok(()));
            }
        }
    });
    let probe = p.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Err(anyhow!("stateless unused")) }))
        .cell("SocketCell", move |_, _| Ok(Arc::new(Root(probe.clone()))))?;
    let mut graph = fixture::graph("native-sockets", "", vec!["SocketCell".into()]);
    graph.primary.loader_bindings = vec!["WORKERS".into()];
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        Generation::build_native(
            1,
            graph,
            fixture::generation_options(),
            HashMap::from([(
                "native-sockets".into(),
                Arc::new(registry) as Arc<dyn cellar::application::Application>,
            )]),
        )?,
        fixture::runtime_options(directory.path().join("cells")),
    )?;
    open(&runtime).await?;
    let mut expected = HashMap::new();
    for path in [
        "/validate",
        "/echo",
        "/release",
        "/half",
        "/default-half",
        "/tls-untrusted",
        "/upgrade-untrusted",
    ] {
        expected.insert(path, call(&runtime, path).await?);
        if path == "/half" {
            assert_eq!(half_events.recv().await.unwrap(), b"after");
        }
        if path == "/default-half" {
            assert!(half_events.recv().await.unwrap().is_empty());
        }
        if path.ends_with("untrusted") {
            tls_events.recv().await.unwrap();
        }
    }
    assert_eq!(expected["/echo"]["bytes"], json!([65, 66]));
    assert_eq!(expected["/release"], expected["/echo"]);
    assert_eq!(expected["/half"]["after"], true);
    assert_eq!(expected["/default-half"]["after"], false);
    let before = p.echo_accepts.load(Ordering::SeqCst);
    assert_eq!(call(&runtime, "/deny").await?, true);
    assert_eq!(call(&runtime, "/broker").await?, true);
    assert_eq!(p.echo_accepts.load(Ordering::SeqCst), before);
    for path in ["/gated-connect", "/rejected-connect"] {
        p.hold_gate.store(true, Ordering::SeqCst);
        let host = runtime.clone();
        let running = cellar::asyncrt::spawn(async move { call(&host, path).await });
        let gate = held.recv().await.unwrap();
        assert_eq!(gate.ticket.epoch, Some(1));
        assert!(gate.ticket.position.is_some());
        let before = p.echo_accepts.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(p.echo_accepts.load(Ordering::SeqCst), before);
        p.hold_gate.store(false, Ordering::SeqCst);
        let refused = path == "/rejected-connect";
        gate.reply
            .send(if refused {
                Err(cellar_logic::RequestError::DurabilityUnproven)
            } else {
                Ok(())
            })
            .unwrap();
        let report = running.await??;
        if refused {
            assert_eq!(report, json!({"refused":true}));
            assert_eq!(p.echo_accepts.load(Ordering::SeqCst), before);
        } else {
            assert_eq!(report, expected["/echo"]);
        }
    }
    for path in [
        "/gated-write",
        "/rejected-write",
        "/gated-close",
        "/rejected-upgrade",
    ] {
        let host = runtime.clone();
        let running = cellar::asyncrt::spawn(async move { call(&host, path).await });
        assert_eq!(next(&mut hold_events).await, PeerEvent::Accepted);
        let gate = held.recv().await.unwrap();
        assert_eq!(gate.ticket.epoch, Some(1));
        assert!(gate.ticket.position.is_some());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), hold_events.recv())
                .await
                .is_err()
        );
        p.hold_gate.store(false, Ordering::SeqCst);
        let refused = path != "/gated-write";
        gate.reply
            .send(if refused {
                Err(cellar_logic::RequestError::DurabilityUnproven)
            } else {
                Ok(())
            })
            .unwrap();
        let report = running.await??;
        assert_eq!(
            report,
            if refused {
                json!({"refused":true})
            } else {
                json!(true)
            }
        );
        let data = until_closed(&mut hold_events).await;
        assert_eq!(
            data,
            if refused {
                Vec::new()
            } else {
                b"secretsecond".to_vec()
            }
        );
        assert!(
            held.try_recv().is_err(),
            "a rejected queued write must not request another proof"
        );
    }
    assert_eq!(call(&runtime, "/retain").await?, true);
    assert_eq!(next(&mut hold_events).await, PeerEvent::Accepted);
    assert_eq!(next(&mut hold_events).await, PeerEvent::Closed);
    let retained = p.retained.lock().unwrap().clone().unwrap();
    retained.closed().await?;
    assert!(retained.opened().await?.remote_address.is_some());
    assert_eq!(call(&runtime, "/foreign").await?, true);
    for path in ["/pending", "/pending-tls"] {
        let request = request(path);
        let id = request.request_id.unwrap();
        let host = runtime.clone();
        let running = cellar::asyncrt::spawn(async move {
            host.fetch_cell(SCOPE.into(), None, request, None).await
        });
        assert_eq!(next(&mut hold_events).await, PeerEvent::Accepted);
        if path == "/pending-tls" {
            assert!(matches!(next(&mut hold_events).await, PeerEvent::Bytes(_)));
        } else {
            // Peer acceptance can precede client publication. This case cancels
            // an opened socket's pending read, rather than an in-flight dial.
            p.pending_read.notified().await;
        }
        runtime.abort_fetch(SCOPE, id);
        assert!(running.await?.is_err());
        until_closed(&mut hold_events).await;
        let retained = p.retained.lock().unwrap().clone().unwrap();
        let closed = retained.closed().await;
        if path == "/pending-tls" {
            assert!(closed.unwrap_err().to_string().contains("cancelled"));
        } else {
            closed?;
        }
    }
    let host = runtime.clone();
    let running = cellar::asyncrt::spawn(async move { call(&host, "/close-opening").await });
    assert_eq!(next(&mut hold_events).await, PeerEvent::Accepted);
    assert!(matches!(next(&mut hold_events).await, PeerEvent::Bytes(_)));
    p.close_opening.notify_one();
    assert_eq!(running.await??, true);
    until_closed(&mut hold_events).await;
    assert_eq!(call(&runtime, "/background").await?, true);
    assert_eq!(next(&mut hold_events).await, PeerEvent::Accepted);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), hold_events.recv())
            .await
            .is_err()
    );
    assert_eq!(call(&runtime, "/foreign").await?, true);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), hold_events.recv())
            .await
            .is_err()
    );
    p.background.notify_one();
    assert_eq!(until_closed(&mut hold_events).await, b"background");
    let retained = p.retained.lock().unwrap().clone().unwrap();
    retained.closed().await?;
    gate_task.abort();
    echo_task.abort();
    half_task.abort();
    hold_task.abort();
    tls_task.abort();
    Ok(())
}

#[test]
fn native_sockets_preserve_leases_half_close_proofs_policy_and_event_cleanup() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
        cellar::asyncrt::timeout(Duration::from_secs(60), run()).await??;
        Ok(())
    })
}
