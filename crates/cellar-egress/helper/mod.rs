// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Dedicated Linux helper process. Its real kernel I/O and clock are outside
//! Cellar's injected host execution boundary. No detached tasks are spawned.

#![allow(clippy::disallowed_methods)]

mod dns;
mod listeners;
mod policy;

use crate::{
    control,
    firewall::Program,
    linux,
    transport::{self, Link, Stream},
    wire::{self, DirectRefusal, DirectReply, Open},
};
use anyhow::{ensure, Result};
use futures_util::{
    future::{poll_fn, BoxFuture},
    stream::{FuturesUnordered, StreamExt},
};
use listeners::{Listener, Packet};
use policy::Policy;
use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
    pin::pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::Semaphore,
};

pub const MAX_INTERCEPT_CONNECTIONS: usize = 64;
pub const MAX_DIRECT_CONNECTIONS: usize = 64;
pub const MAX_DNS_CONNECTIONS: usize = 64;

#[derive(Clone)]
pub struct Config {
    pub session: [u8; 32],
    pub internet: bool,
    pub https: bool,
    pub upstream: SocketAddr,
    pub http_port: u16,
    pub dns_port: u16,
    pub operation_timeout: Duration,
    pub install_timeout: Duration,
    /// Trusted image-local installer; never supplied by application code.
    pub nft_path: std::path::PathBuf,
}

impl Config {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.nft_path.is_absolute(),
            "firewall installer path must be absolute"
        );
        ensure!(self.upstream.port() != 0, "invalid upstream DNS port");
        ensure!(
            !self.operation_timeout.is_zero() && !self.install_timeout.is_zero(),
            "helper deadlines must be nonzero"
        );
        ensure!(
            !(self.upstream.ip().is_loopback()
                && [self.http_port, self.dns_port].contains(&self.upstream.port())),
            "upstream DNS points at the network helper"
        );
        Program::with_https(
            &[],
            self.http_port,
            self.dns_port,
            self.internet,
            self.https,
        )?;
        Ok(())
    }
}

enum Event {
    Driver(Result<()>),
    Control(Result<()>),
    Incoming(Option<Stream>),
    Listen(usize, Result<Packet>),
    Finished,
}

/// Run only in the dedicated helper's network namespace. A closed authenticated
/// Engine attach retires the transport and drops every owned data future/socket.
/// Entered firewall installation is joined before the helper returns.
pub async fn run<T>(io: T, config: Config) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    config.validate()?;
    let listeners = Listener::bind(&config).await?;
    let policy = Policy::new(config.clone());
    // The control prelude is readiness: no application may start before these
    // listeners and the initial offline/online policy actually exist.
    policy.install(Vec::new()).await?;
    let (link, mut incoming, driver) = transport::session(io, transport::Role::Container);
    let mut driver = pin!(driver.run());
    let controls = async {
        let stream = link.open().await?;
        let installer = policy.clone();
        control::serve(stream, config.session, move |_, targets| {
            let installer = installer.clone();
            async move { installer.install(targets).await }
        })
        .await
    };
    let mut controls = pin!(controls);
    let mut accepts: FuturesUnordered<_> = listeners
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, listener)| listener.next(index))
        .collect();
    let mut tasks: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
    let intercepts = Arc::new(Semaphore::new(MAX_INTERCEPT_CONNECTIONS));
    let directs = Arc::new(Semaphore::new(MAX_DIRECT_CONNECTIONS));
    let dns = Arc::new(Semaphore::new(MAX_DNS_CONNECTIONS));
    let mut events = 0;
    let reason = loop {
        let mut next = pin!(incoming.next());
        let event = poll_fn(|cx| {
            if let Poll::Ready(result) = driver.as_mut().poll(cx) {
                return Poll::Ready(Event::Driver(result));
            }
            if let Poll::Ready(result) = controls.as_mut().poll(cx) {
                return Poll::Ready(Event::Control(result));
            }
            if let Poll::Ready(Some(())) = tasks.poll_next_unpin(cx) {
                return Poll::Ready(Event::Finished);
            }
            if let Poll::Ready(stream) = next.as_mut().poll(cx) {
                return Poll::Ready(Event::Incoming(stream));
            }
            if let Poll::Ready(Some((index, packet))) = accepts.poll_next_unpin(cx) {
                return Poll::Ready(Event::Listen(index, packet));
            }
            Poll::Pending
        })
        .await;
        match event {
            Event::Driver(result) => break (true, false, result),
            Event::Control(result) => break (false, true, result),
            Event::Incoming(None) => {
                break (
                    false,
                    false,
                    Err(anyhow::anyhow!("egress stream receiver stopped")),
                )
            }
            Event::Incoming(Some(stream)) => {
                if let Ok(permit) = directs.clone().try_acquire_owned() {
                    let config = config.clone();
                    tasks.push(Box::pin(async move {
                        let _permit = permit;
                        let _ =
                            tokio::time::timeout(config.operation_timeout, direct(stream, config))
                                .await;
                    }));
                }
            }
            Event::Listen(index, packet) => {
                accepts.push(listeners[index].clone().next(index));
                let packet = match packet {
                    Ok(packet) => packet,
                    Err(error) => break (false, false, Err(error)),
                };
                let slots = if matches!(&packet, Packet::Http(_)) {
                    &intercepts
                } else {
                    &dns
                };
                if let Ok(permit) = slots.clone().try_acquire_owned() {
                    let policy = policy.clone();
                    let link = link.clone();
                    let timeout = config.operation_timeout;
                    tasks.push(Box::pin(async move {
                        let _permit = permit;
                        let operation = async move {
                            match packet {
                                Packet::Http(socket) => {
                                    intercept(socket, link, &policy.config).await
                                }
                                Packet::DnsTcp(socket) => dns::tcp(socket, policy).await,
                                Packet::DnsUdp {
                                    server,
                                    source,
                                    bytes,
                                } => dns::udp(server, source, bytes, policy).await,
                            }
                        };
                        let _ = tokio::time::timeout(timeout, operation).await;
                    }));
                }
            }
            Event::Finished => {}
        }
        events += 1;
        if events == 64 {
            events = 0;
            tokio::task::yield_now().await;
        }
    };
    link.close();
    drop(tasks);
    drop(accepts);
    // Only the control loop owns a child process. Its installer has a bounded
    // kill-and-wait path, then observes stream retirement on its next I/O poll.
    match (reason.0, reason.1) {
        (true, false) => {
            let _ = controls.await;
        }
        (false, true) => {
            let _ = driver.await;
        }
        (false, false) => {
            let _ = futures_util::future::join(driver, controls).await;
        }
        (true, true) => {}
    }
    reason.2
}

async fn intercept(mut socket: TcpStream, link: Link, config: &Config) -> Result<()> {
    let destination = linux::original_destination(&socket)?;
    ensure!(
        ![config.http_port, config.dns_port].contains(&destination.port()),
        "application dialled a helper listener directly"
    );
    let mut stream = link.open().await?;
    wire::write_open(&mut stream, &Open::Intercept { destination }).await?;
    tokio::io::copy_bidirectional(&mut socket, &mut stream).await?;
    Ok(())
}

async fn direct(mut stream: Stream, config: Config) -> Result<()> {
    let Open::Direct { host, port } = wire::read_open(&mut stream).await? else {
        anyhow::bail!("unexpected helper data stream");
    };
    if !config.internet {
        return refuse(&mut stream, DirectRefusal::InternetDisabled).await;
    }
    // Initial profile: fallback uses the kernel-observed destination. Never
    // resolve a guest-selected hostname through unmarked libc DNS here.
    let Ok(ip) = host.parse::<IpAddr>() else {
        return refuse(&mut stream, DirectRefusal::UnsupportedHost).await;
    };
    if ip.is_loopback() && [config.http_port, config.dns_port].contains(&port) {
        return refuse(&mut stream, DirectRefusal::ReservedDestination).await;
    }
    let mut socket = match linux::connect(SocketAddr::new(ip, port)).await {
        Ok(socket) => socket,
        Err(_) => return refuse(&mut stream, DirectRefusal::ConnectFailed).await,
    };
    wire::write_direct_reply(&mut stream, &DirectReply::Connected).await?;
    tokio::io::copy_bidirectional(&mut socket, &mut stream).await?;
    Ok(())
}

async fn refuse(stream: &mut Stream, reason: DirectRefusal) -> Result<()> {
    wire::write_direct_reply(stream, &DirectReply::Refused { reason }).await?;
    stream.shutdown().await?;
    Ok(())
}
