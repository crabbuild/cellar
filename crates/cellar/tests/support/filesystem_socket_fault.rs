// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Supervisor-owned TCP cuts for a private, loopback S3 qualification server.
//! No object-store method or HTTP response is replaced by a successful adapter.

use anyhow::{ensure, Context as _, Result};
use serde_json::{json, Value};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

enum Command {
    Cut(oneshot::Sender<()>),
    Stop,
}

#[derive(Default)]
struct Metrics {
    accepted: AtomicUsize,
    forwarded: AtomicUsize,
    denied: AtomicUsize,
    cancelled: AtomicUsize,
    io_errors: AtomicUsize,
}

pub(super) struct Control {
    commands: mpsc::Sender<Command>,
    metrics: Arc<Metrics>,
}

impl Control {
    /// Acknowledged only after every admitted connection task has been joined.
    pub(super) async fn cut(&self) -> Result<()> {
        let (done, wait) = oneshot::channel();
        self.commands.send(Command::Cut(done)).await?;
        tokio::time::timeout(Duration::from_secs(5), wait).await??;
        Ok(())
    }

    pub(super) fn refused(&self) -> usize {
        self.metrics.denied.load(Ordering::Acquire)
    }
}

pub(super) struct Proxy {
    pub endpoint: String,
    pub control: Arc<Control>,
    task: Option<JoinHandle<Result<()>>>,
}

impl Proxy {
    pub(super) async fn start(endpoint: &str) -> Result<Self> {
        let url = reqwest::Url::parse(endpoint)?;
        let ip: IpAddr = url.host_str().context("missing S3 host")?.parse()?;
        ensure!(
            url.scheme() == "http"
                && ip.is_loopback()
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none(),
            "socket qualification requires a private loopback HTTP endpoint"
        );
        let upstream = SocketAddr::new(ip, url.port_or_known_default().context("missing S3 port")?);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (commands, receiver) = mpsc::channel(4);
        let metrics = Arc::new(Metrics::default());
        let task = tokio::spawn(serve(listener, upstream, receiver, metrics.clone()));
        Ok(Self {
            endpoint,
            control: Arc::new(Control { commands, metrics }),
            task: Some(task),
        })
    }

    pub(super) async fn shutdown(mut self) -> Result<Value> {
        self.control.commands.send(Command::Stop).await?;
        let task = self.task.as_mut().context("proxy already stopped")?;
        tokio::time::timeout(Duration::from_secs(5), task).await???;
        self.task.take();
        let metrics = &self.control.metrics;
        Ok(json!({
            "accepted_connections":metrics.accepted.load(Ordering::Acquire),
            "forwarded_connections":metrics.forwarded.load(Ordering::Acquire),
            "denied_connections":metrics.denied.load(Ordering::Acquire),
            "cancelled_connections":metrics.cancelled.load(Ordering::Acquire),
            "connection_io_errors":metrics.io_errors.load(Ordering::Acquire),
            "remaining_connections":0,"joined":true,
        }))
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn completed(
    result: std::result::Result<std::io::Result<()>, tokio::task::JoinError>,
    metrics: &Metrics,
) -> Result<()> {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            metrics.io_errors.fetch_add(1, Ordering::AcqRel);
        }
        Err(error) if error.is_cancelled() => {
            metrics.cancelled.fetch_add(1, Ordering::AcqRel);
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn drain(tasks: &mut JoinSet<std::io::Result<()>>, metrics: &Metrics) -> Result<()> {
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        completed(result, metrics)?;
    }
    Ok(())
}

async fn serve(
    listener: TcpListener,
    upstream: SocketAddr,
    mut commands: mpsc::Receiver<Command>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    let mut cut = false;
    loop {
        cellar::asyncrt::select_biased! { "cut and join sockets before accepting more connections";
            command = commands.recv() => match command {
                Some(Command::Cut(done)) => {
                    cut = true;
                    drain(&mut tasks, &metrics).await?;
                    let _ = done.send(());
                }
                Some(Command::Stop) | None => {
                    drain(&mut tasks, &metrics).await?;
                    return Ok(());
                }
            },
            Some(result) = tasks.join_next(), if !tasks.is_empty() => completed(result, &metrics)?,
            incoming = listener.accept() => {
                let (mut client, _) = incoming?;
                metrics.accepted.fetch_add(1, Ordering::AcqRel);
                if cut {
                    metrics.denied.fetch_add(1, Ordering::AcqRel);
                    drop(client);
                    continue;
                }
                ensure!(tasks.len() < 128, "socket qualification proxy exceeded its connection bound");
                let metrics = metrics.clone();
                tasks.spawn(async move {
                    let mut server = TcpStream::connect(upstream).await?;
                    metrics.forwarded.fetch_add(1, Ordering::AcqRel);
                    tokio::io::copy_bidirectional(&mut client, &mut server).await?;
                    Ok(())
                });
            }
        }
    }
}

#[tokio::test]
async fn cut_closes_existing_and_new_connections_and_joins_forwarders() -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = Proxy::start(&format!("http://{}", listener.local_addr()?)).await?;
    let client = TcpStream::connect(proxy.endpoint.trim_start_matches("http://")).await?;
    let (mut server, _) = listener.accept().await?;
    let mut client = client;
    client.write_all(b"seed").await?;
    let mut bytes = [0; 4];
    server.read_exact(&mut bytes).await?;
    ensure!(&bytes == b"seed");
    server.write_all(b"done").await?;
    client.read_exact(&mut bytes).await?;
    ensure!(&bytes == b"done");
    let result = async {
        proxy.control.cut().await?;
        ensure!(client.read(&mut bytes).await? == 0);
        ensure!(server.read(&mut bytes).await? == 0);
        let mut refused = TcpStream::connect(proxy.endpoint.trim_start_matches("http://")).await?;
        ensure!(refused.read(&mut bytes).await? == 0);
        ensure!(proxy.control.refused() == 1);
        Ok::<_, anyhow::Error>(())
    };
    let result = tokio::time::timeout(Duration::from_secs(5), result).await;
    let report = proxy.shutdown().await?;
    result??;
    ensure!(report["forwarded_connections"] == 1 && report["cancelled_connections"] == 1);
    ensure!(report["remaining_connections"] == 0 && report["joined"] == true);
    Ok(())
}
