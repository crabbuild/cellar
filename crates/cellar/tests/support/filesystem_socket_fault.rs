// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Supervisor-owned TCP cuts/stalls for a private, loopback S3 qualification server.
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
    sync::{mpsc, oneshot, watch, Notify},
    task::{JoinHandle, JoinSet},
};

enum Command {
    Cut(oneshot::Sender<()>),
    Stall(oneshot::Sender<()>),
    Stop,
}

#[derive(Default)]
struct Metrics {
    accepted: AtomicUsize,
    forwarded: AtomicUsize,
    denied: AtomicUsize,
    cancelled: AtomicUsize,
    io_errors: AtomicUsize,
    active: AtomicUsize,
    stalled: AtomicUsize,
    changed: Notify,
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

    /// Acknowledged after admitted forwarding has stopped. Sockets stay open;
    /// no further application bytes are read or written in either direction.
    pub(super) async fn stall(&self) -> Result<()> {
        let (done, wait) = oneshot::channel();
        self.commands.send(Command::Stall(done)).await?;
        tokio::time::timeout(Duration::from_secs(5), wait).await??;
        Ok(())
    }

    pub(super) fn stalled(&self) -> usize {
        self.metrics.stalled.load(Ordering::Acquire)
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
        let active = metrics.active.load(Ordering::Acquire);
        ensure!(active == 0, "proxy retained connection tasks after join");
        Ok(json!({
            "accepted_connections":metrics.accepted.load(Ordering::Acquire),
            "forwarded_connections":metrics.forwarded.load(Ordering::Acquire),
            "denied_connections":metrics.denied.load(Ordering::Acquire),
            "cancelled_connections":metrics.cancelled.load(Ordering::Acquire),
            "connection_io_errors":metrics.io_errors.load(Ordering::Acquire),
            "stalled_connections":metrics.stalled.load(Ordering::Acquire),
            "remaining_connections":active,"joined":true,
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

struct Active(Arc<Metrics>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_one();
    }
}

async fn park(
    client: TcpStream,
    server: Option<TcpStream>,
    metrics: &Metrics,
) -> std::io::Result<()> {
    metrics.stalled.fetch_add(1, Ordering::AcqRel);
    metrics.changed.notify_one();
    // Retain both sockets until the supervisor aborts and joins this task.
    // This deliberately neither reads bytes nor sends EOF/a synthetic response.
    std::future::pending::<()>().await;
    drop((client, server));
    Ok(())
}

async fn forward(
    mut client: TcpStream,
    upstream: SocketAddr,
    mut paused: watch::Receiver<bool>,
    metrics: &Metrics,
) -> std::io::Result<()> {
    let stalled = *paused.borrow();
    if stalled {
        return park(client, None, metrics).await;
    }
    let connected = cellar::asyncrt::select_biased! { "stop connection setup before acknowledging a stall";
        result = paused.changed() => { result.map_err(std::io::Error::other)?; None },
        result = TcpStream::connect(upstream) => Some(result?),
    };
    let Some(mut server) = connected else {
        return park(client, None, metrics).await;
    };
    metrics.forwarded.fetch_add(1, Ordering::AcqRel);
    let transferred = cellar::asyncrt::select_biased! { "drop forwarding buffers before acknowledging a stall";
        result = paused.changed() => { result.map_err(std::io::Error::other)?; None },
        result = tokio::io::copy_bidirectional(&mut client, &mut server) => Some(result),
    };
    match transferred {
        Some(result) => result.map(|_| ()),
        None => park(client, Some(server), metrics).await,
    }
}

async fn serve(
    listener: TcpListener,
    upstream: SocketAddr,
    mut commands: mpsc::Receiver<Command>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    let mut cut = false;
    let (pause, paused) = watch::channel(false);
    loop {
        cellar::asyncrt::select_biased! { "cut and join sockets before accepting more connections";
            command = commands.recv() => match command {
                Some(Command::Cut(done)) => {
                    cut = true;
                    drain(&mut tasks, &metrics).await?;
                    let _ = done.send(());
                }
                Some(Command::Stall(done)) => {
                    pause.send_replace(true);
                    loop {
                        let changed = metrics.changed.notified();
                        if metrics.active.load(Ordering::Acquire)
                            == metrics.stalled.load(Ordering::Acquire)
                        {
                            break;
                        }
                        changed.await;
                    }
                    let _ = done.send(());
                }
                Some(Command::Stop) | None => {
                    drain(&mut tasks, &metrics).await?;
                    return Ok(());
                }
            },
            Some(result) = tasks.join_next(), if !tasks.is_empty() => completed(result, &metrics)?,
            incoming = listener.accept() => {
                let (client, _) = incoming?;
                metrics.accepted.fetch_add(1, Ordering::AcqRel);
                if cut {
                    metrics.denied.fetch_add(1, Ordering::AcqRel);
                    drop(client);
                    continue;
                }
                ensure!(tasks.len() < 128, "socket qualification proxy exceeded its connection bound");
                let metrics = metrics.clone();
                let paused = paused.clone();
                // Count before spawn so a not-yet-polled task participates in
                // the stall acknowledgement and releases its slot on abort.
                metrics.active.fetch_add(1, Ordering::AcqRel);
                let active = Active(metrics.clone());
                tasks.spawn(async move {
                    let _active = active;
                    forward(client, upstream, paused, &metrics).await
                });
            }
        }
    }
}

#[tokio::test]
async fn stall_keeps_sockets_open_without_forwarding_and_joins_parked_tasks() -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = Proxy::start(&format!("http://{}", listener.local_addr()?)).await?;
    let result = async {
        let mut client = TcpStream::connect(proxy.endpoint.trim_start_matches("http://")).await?;
        let (mut server, _) = listener.accept().await?;
        let mut bytes = [0; 4];
        client.write_all(b"seed").await?;
        server.read_exact(&mut bytes).await?;
        ensure!(&bytes == b"seed");
        server.write_all(b"done").await?;
        client.read_exact(&mut bytes).await?;
        ensure!(&bytes == b"done");
        proxy.control.stall().await?;
        ensure!(proxy.control.stalled() == 1 && proxy.control.refused() == 0);
        client.write_all(b"held").await?;
        server.write_all(b"held").await?;
        ensure!(
            tokio::time::timeout(Duration::from_millis(100), client.read(&mut bytes))
                .await
                .is_err()
        );
        ensure!(
            tokio::time::timeout(Duration::from_millis(100), server.read(&mut bytes))
                .await
                .is_err()
        );
        let mut fresh = TcpStream::connect(proxy.endpoint.trim_start_matches("http://")).await?;
        fresh.write_all(b"held").await?;
        ensure!(
            tokio::time::timeout(Duration::from_millis(100), fresh.read(&mut bytes))
                .await
                .is_err()
        );
        ensure!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        ensure!(proxy.control.stalled() == 2 && proxy.control.refused() == 0);
        Ok::<_, anyhow::Error>((client, server, fresh))
    };
    let result = tokio::time::timeout(Duration::from_secs(5), result).await;
    let report = proxy.shutdown().await?;
    let _sockets = result??;
    ensure!(report["forwarded_connections"] == 1 && report["stalled_connections"] == 2);
    ensure!(report["cancelled_connections"] == 2 && report["remaining_connections"] == 0);
    ensure!(report["denied_connections"] == 0 && report["joined"] == true);
    Ok(())
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
