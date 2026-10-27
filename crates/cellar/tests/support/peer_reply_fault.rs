// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real peer tunnel proxy: hold signed submissions or capture complete replies
//! before controlled delay/loss. Packets and production proof stay unchanged.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context, Result};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch, Semaphore};

#[derive(Clone, Copy)]
enum Fault {
    Drop,
    Delay,
}

#[derive(Clone, Debug)]
pub(super) struct Exchange {
    pub method: String,
    pub scope: String,
    pub request: Vec<u8>,
    pub response: Vec<u8>,
    pub dropped: bool,
    pub delayed: bool,
}

#[derive(Clone, Debug)]
pub(super) struct HeldRequest {
    pub method: String,
    pub scope: String,
    pub body: Vec<u8>,
}

pub(super) struct Capture {
    armed: Mutex<Option<(&'static str, Fault)>>,
    held_methods: Mutex<Vec<&'static str>>,
    pub held_requests: Mutex<Vec<HeldRequest>>,
    pub request_entered: Semaphore,
    pub request_release: Semaphore,
    paused: watch::Sender<bool>,
    upstream_closing: AtomicBool,
    pub entered: Semaphore,
    pub release: Semaphore,
    pub blocked: Semaphore,
    pub released: Semaphore,
    pub exchanges: Mutex<Vec<Exchange>>,
    failure: Mutex<Option<String>>,
    caller_disconnects: AtomicUsize,
}

impl Default for Capture {
    fn default() -> Self {
        Self {
            armed: Mutex::new(None),
            held_methods: Mutex::new(Vec::new()),
            held_requests: Mutex::new(Vec::new()),
            request_entered: Semaphore::new(0),
            request_release: Semaphore::new(0),
            paused: watch::channel(false).0,
            upstream_closing: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            blocked: Semaphore::new(0),
            released: Semaphore::new(0),
            exchanges: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
            caller_disconnects: AtomicUsize::new(0),
        }
    }
}

impl Capture {
    /// Retain the first unexpected relay failure even if its owner unwinds
    /// before joining the proxy. Recording it never changes relay success.
    pub fn failure(&self) -> Option<String> {
        self.failure.lock().unwrap().clone()
    }

    pub fn caller_disconnects(&self) -> usize {
        self.caller_disconnects.load(Ordering::Acquire)
    }

    /// Hold exactly one original request for each method, before forwarding
    /// its unchanged signed packet. This is a submission barrier, not proof.
    pub fn hold_requests(&self, methods: &[&'static str]) {
        *self.held_methods.lock().unwrap() = methods.to_vec();
    }

    pub fn arm(&self, method: &'static str) {
        *self.armed.lock().unwrap() = Some((method, Fault::Drop));
    }

    /// Hold one complete append reply, then stop new append submissions until
    /// the test explicitly permits replay after its successor audit.
    pub fn delay_append(&self) {
        *self.armed.lock().unwrap() = Some(("__streamAppend", Fault::Delay));
    }

    pub fn resume_appends(&self) {
        self.paused.send_replace(false);
    }

    pub fn delay_ack(&self) {
        *self.armed.lock().unwrap() = Some(("__streamAck", Fault::Delay));
    }

    /// A fenced destination closes existing tunnels and can refuse new ones
    /// while cached callers discover its successor. Only controlled transport
    /// closure is normal; framing and other relay failures still fail the test.
    pub fn expect_upstream_close(&self) {
        self.upstream_closing.store(true, Ordering::Release);
    }
}

pub(super) struct Proxy {
    pub address: SocketAddr,
    pub capture: Arc<Capture>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl Proxy {
    pub async fn start(target: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let capture = Arc::new(Capture::default());
        let controls = capture.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                cellar::asyncrt::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let (socket, _) = accepted?;
                        let capture = controls.clone();
                        connections.spawn(relay(socket, target, capture));
                    },
                    Some(result) = connections.join_next(), if !connections.is_empty() => { result??; }
                }
            }
            while let Some(result) = connections.join_next().await {
                result??;
            }
            Ok(())
        });
        Ok(Self {
            address,
            capture,
            stop: Some(stop),
            task: Some(task),
        })
    }

    pub async fn wait_for_release(&mut self) -> Result<()> {
        cellar::asyncrt::select! {
            permit = self.capture.released.acquire() => {
                permit?.forget();
                Ok(())
            },
            result = self.task.as_mut().context("proxy already joined")? => {
                self.task.take();
                result.context("peer proxy task failed")?
                    .context("peer proxy relay failed")?;
                anyhow::bail!("peer proxy ended before held reply release")
            }
        }
    }

    pub async fn stop(mut self) -> Result<()> {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.task.as_mut().unwrap(),
        )
        .await???;
        Ok(())
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct Message {
    head: Vec<u8>,
    wire_body: Vec<u8>,
    body: Vec<u8>,
}

impl Message {
    fn request_path(&self) -> Option<&str> {
        let target = std::str::from_utf8(&self.head)
            .ok()?
            .lines()
            .next()?
            .split_whitespace()
            .nth(1)?;
        Some(target.split('?').next().unwrap_or(target))
    }

    fn header(&self, name: &str) -> Option<&str> {
        std::str::from_utf8(&self.head)
            .ok()?
            .lines()
            .skip(1)
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name).then(|| value.trim())
            })
    }

    async fn write(&self, socket: &mut TcpStream) -> Result<()> {
        socket.write_all(&self.head).await?;
        socket.write_all(&self.wire_body).await?;
        Ok(())
    }
}

async fn message(reader: &mut BufReader<TcpStream>, response: bool) -> Result<Option<Message>> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let read = reader.read_until(b'\n', &mut head).await?;
        if read == 0 && head.is_empty() {
            return Ok(None);
        }
        ensure!(
            read > 0 && head.len() <= 65536,
            "incomplete/oversized peer HTTP head"
        );
    }
    let mut result = Message {
        head,
        wire_body: Vec::new(),
        body: Vec::new(),
    };
    if result.header("transfer-encoding") == Some("chunked") {
        loop {
            let mut line = Vec::new();
            reader.read_until(b'\n', &mut line).await?;
            ensure!(
                line.ends_with(b"\r\n") && line.len() <= 128,
                "invalid chunk head"
            );
            let size = usize::from_str_radix(
                std::str::from_utf8(&line)?
                    .trim()
                    .split(';')
                    .next()
                    .unwrap(),
                16,
            )?;
            result.wire_body.extend_from_slice(&line);
            ensure!(
                result.body.len().saturating_add(size) <= 16 << 20,
                "oversized peer body"
            );
            if size == 0 {
                loop {
                    let mut trailer = Vec::new();
                    reader.read_until(b'\n', &mut trailer).await?;
                    ensure!(
                        trailer.ends_with(b"\r\n") && trailer.len() <= 65536,
                        "invalid chunk trailer"
                    );
                    ensure!(
                        result.wire_body.len().saturating_add(trailer.len()) <= (16 << 20) + 65536,
                        "oversized chunk framing"
                    );
                    result.wire_body.extend_from_slice(&trailer);
                    if trailer == b"\r\n" {
                        break;
                    }
                }
                break;
            }
            let mut chunk = vec![0; size + 2];
            reader.read_exact(&mut chunk).await?;
            ensure!(chunk.ends_with(b"\r\n"), "invalid chunk body");
            result.body.extend_from_slice(&chunk[..size]);
            result.wire_body.extend_from_slice(&chunk);
        }
    } else if let Some(length) = result.header("content-length") {
        let length: usize = length.parse()?;
        ensure!(length <= 16 << 20, "oversized peer body");
        result.body.resize(length, 0);
        reader.read_exact(&mut result.body).await?;
        result.wire_body = result.body.clone();
    } else {
        ensure!(
            !response
                || result.head.starts_with(b"HTTP/1.1 101 ")
                || result.head.starts_with(b"HTTP/1.1 204 ")
                || result.head.starts_with(b"HTTP/1.1 304 "),
            "unframed peer response"
        );
    }
    Ok(Some(result))
}

async fn relay(socket: TcpStream, target: SocketAddr, capture: Arc<Capture>) -> Result<()> {
    let result = relay_inner(socket, target, capture.clone()).await;
    let result = match result {
        Err(error)
            if capture.upstream_closing.load(Ordering::Acquire)
                && error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                    matches!(
                        error.kind(),
                        std::io::ErrorKind::BrokenPipe
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::NotConnected
                            | std::io::ErrorKind::ConnectionRefused
                    )
                }) =>
        {
            Ok(())
        }
        result => result,
    };
    if let Err(error) = &result {
        let mut failure = capture.failure.lock().unwrap();
        if failure.is_none() {
            let detail: String = format!("{error:#}").chars().take(4096).collect();
            eprintln!("peer proxy relay failed: {detail}");
            *failure = Some(detail);
        }
    }
    result
}

/// A caller can cancel after submission. Close only its relay, preserving the
/// listener for other peers. This records transport loss, never delivery or a
/// durability acknowledgement; the request and captured reply are not replayed.
async fn reply_to_caller(
    reply: &Message,
    caller: &mut TcpStream,
    capture: &Capture,
) -> Result<bool> {
    match reply.write(caller).await {
        Ok(()) => Ok(true),
        Err(error)
            if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::NotConnected
                )
            }) =>
        {
            capture.caller_disconnects.fetch_add(1, Ordering::Release);
            Ok(false)
        }
        Err(error) => Err(error).context("forward proxy reply to caller"),
    }
}

async fn relay_inner(socket: TcpStream, target: SocketAddr, capture: Arc<Capture>) -> Result<()> {
    let mut client = BufReader::new(socket);
    let upstream = TcpStream::connect(target)
        .await
        .context("connect proxy upstream")?;
    let mut owner = BufReader::new(upstream);
    loop {
        // A clean idle owner close must retire the caller's pooled tunnel.
        // Waiting only on the caller hides that EOF, then a later signed
        // request hits a dead socket and can take down unrelated relays.
        // Never replay it: the proxy cannot establish execution outcome.
        let request = cellar::asyncrt::select_biased! { "propagate idle upstream EOF before accepting another submission";
            available = owner.fill_buf() => {
                ensure!(available.context("observe idle proxy upstream")?.is_empty(), "unsolicited proxy upstream data");
                return Ok(());
            },
            request = caller_request(&mut client, &capture) => {
                let Some(request) = request.context("read proxy caller request")? else {
                    return Ok(());
                };
                request
            },
        };
        let hold = {
            let mut methods = capture.held_methods.lock().unwrap();
            request
                .header("x-cells-rpc-method")
                .and_then(|method| methods.iter().position(|candidate| *candidate == method))
                .map(|index| methods.remove(index))
        };
        if let Some(method) = hold {
            capture.held_requests.lock().unwrap().push(HeldRequest {
                method: method.into(),
                scope: request.header("x-cells-scope").unwrap_or_default().into(),
                body: request.body.clone(),
            });
            capture.request_entered.add_permits(1);
            capture.request_release.acquire().await?.forget();
        }
        let mut paused = capture.paused.subscribe();
        if request.header("x-cells-rpc-method") == Some("__streamAppend")
            && *paused.borrow_and_update()
        {
            capture.blocked.add_permits(1);
            while *paused.borrow_and_update() {
                paused.changed().await?;
            }
        }
        request
            .write(owner.get_mut())
            .await
            .context("forward proxy request upstream")?;
        let Some(reply) = message(&mut owner, true)
            .await
            .context("read proxy upstream reply")?
        else {
            return Ok(());
        };
        let method = request.header("x-cells-rpc-method");
        if matches!(
            method,
            Some("__streamAppend" | "__streamAck" | "__streamNext" | "__streamExtend")
        ) {
            let method = method.unwrap();
            if reply.head.starts_with(b"HTTP/1.1 503 ")
                && reply.header("x-cellar-event-error") == Some("Unavailable")
            {
                // A host admission refusal is a valid, unsubmitted outcome.
                // Forward it unchanged so production retry policy sees it;
                // it cannot consume a cut armed for a proven event reply.
                if !reply_to_caller(&reply, client.get_mut(), &capture).await? {
                    return Ok(());
                }
                continue;
            }
            ensure!(
                reply.head.starts_with(b"HTTP/1.1 200 "),
                "event RPC did not produce a successful HTTP reply"
            );
            let fault = {
                let mut armed = capture.armed.lock().unwrap();
                if armed.as_ref().is_some_and(|(target, _)| *target == method) {
                    armed.take().map(|(_, fault)| fault)
                } else {
                    None
                }
            };
            let dropped = matches!(fault, Some(Fault::Drop));
            let delayed = matches!(fault, Some(Fault::Delay));
            capture.exchanges.lock().unwrap().push(Exchange {
                method: method.into(),
                scope: request.header("x-cells-scope").unwrap_or_default().into(),
                request: request.body.clone(),
                response: reply.body.clone(),
                dropped,
                delayed,
            });
            if delayed {
                if method == "__streamAppend" {
                    capture.paused.send_replace(true);
                }
                capture.entered.add_permits(1);
                capture.release.acquire().await?.forget();
                let result = reply.write(client.get_mut()).await;
                capture.released.add_permits(1);
                // The old node may already have joined and closed this socket.
                // A late transport failure is expected; unrelated proxy errors
                // must still fail the test rather than disappear as cancellation.
                if let Err(failure) = result {
                    let closed = failure
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| {
                            matches!(
                                error.kind(),
                                std::io::ErrorKind::BrokenPipe
                                    | std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::ConnectionAborted
                                    | std::io::ErrorKind::NotConnected
                            )
                        });
                    if !closed {
                        return Err(failure);
                    }
                }
                return Ok(());
            }
            if dropped {
                capture.entered.add_permits(1);
                capture.release.acquire().await?.forget();
                client.get_mut().shutdown().await?;
                owner.get_mut().shutdown().await?;
                return Ok(());
            }
        }
        if !reply_to_caller(&reply, client.get_mut(), &capture).await? {
            return Ok(());
        }
        if reply.head.starts_with(b"HTTP/1.1 101 ")
            && request.request_path() != Some("/peer/tunnel")
        {
            // Only the outer tunnel carries inspectable HTTP after upgrade.
            // Log tails carry binary frames. Keep both readers so prefetched
            // bytes and half-close propagation survive the protocol switch.
            tokio::io::copy_bidirectional(&mut client, &mut owner)
                .await
                .context("relay upgraded proxy bytes")?;
            return Ok(());
        }
    }
}

/// A pooled caller can reset between requests, or cancel during upload. Its
/// socket owns only this relay; aborting the listener would also cancel other
/// original signed requests. Protocol/framing errors remain test failures.
async fn caller_request(
    client: &mut BufReader<TcpStream>,
    capture: &Capture,
) -> Result<Option<Message>> {
    match message(client, false).await {
        Err(error)
            if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::NotConnected
                )
            }) =>
        {
            capture.caller_disconnects.fetch_add(1, Ordering::Release);
            Ok(None)
        }
        result => result,
    }
}

#[tokio::test]
async fn caller_cancellation_preserves_listener_and_next_reply_cut() -> Result<()> {
    for refused in [false, true] {
        let upstream = TcpListener::bind("127.0.0.1:0").await?;
        let proxy = Proxy::start(upstream.local_addr()?).await?;
        proxy.capture.arm("__streamAppend");
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut caller = TcpStream::connect(proxy.address).await?;
            caller.write_all(b"POST /rpc HTTP/1.1\r\nx-cells-rpc-method: __streamNext\r\nContent-Length: 2\r\n\r\n[]").await?;
            let (owner, _) = upstream.accept().await?;
            let mut owner = BufReader::new(owner);
            ensure!(message(&mut owner, false).await?.is_some());
            // Reset after the complete request reached its destination, before
            // its reply. No upstream shutdown or synthetic I/O error is used.
            caller.set_zero_linger()?;
            drop(caller);
            let response = if refused {
                b"HTTP/1.1 503 Service Unavailable\r\nx-cellar-event-error: Unavailable\r\nContent-Length: 2\r\n\r\n{}".as_slice()
            } else {
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".as_slice()
            };
            owner.get_mut().write_all(response).await?;
            while proxy.capture.caller_disconnects.load(Ordering::Acquire) == 0
                && !proxy.task.as_ref().unwrap().is_finished()
            {
                tokio::task::yield_now().await;
            }
            ensure!(proxy.capture.failure().is_none(), "caller cancellation killed listener: {:?}", proxy.capture.failure());
            ensure!(proxy.capture.caller_disconnects.load(Ordering::Acquire) == 1);
            ensure!(!proxy.task.as_ref().unwrap().is_finished());
            drop(owner);

            let mut next = TcpStream::connect(proxy.address).await?;
            next.write_all(b"POST /rpc HTTP/1.1\r\nx-cells-rpc-method: __streamAppend\r\nContent-Length: 2\r\n\r\n[]").await?;
            let (owner, _) = upstream.accept().await?;
            let mut owner = BufReader::new(owner);
            ensure!(message(&mut owner, false).await?.is_some());
            owner.get_mut().write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").await?;
            proxy.capture.entered.acquire().await?.forget();
            {
                let exchanges = proxy.capture.exchanges.lock().unwrap();
                ensure!(exchanges.len() == if refused { 1 } else { 2 });
                let reply = exchanges.last().unwrap();
                ensure!(reply.method == "__streamAppend" && reply.dropped && reply.response == b"{}");
            }
            proxy.capture.release.add_permits(1);
            let mut bytes = Vec::new();
            next.read_to_end(&mut bytes).await?;
            ensure!(bytes.is_empty(), "cancellation consumed the next reply cut");
            Ok::<_, anyhow::Error>(())
        }).await.context("caller cancellation regression stalled").and_then(|result| result);
        let joined = proxy.stop().await;
        outcome.with_context(|| format!("proxy_join={joined:?}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test]
async fn idle_caller_reset_preserves_listener_and_other_relays() -> Result<()> {
    let upstream = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = Proxy::start(upstream.local_addr()?).await?;
    let capture = proxy.capture.clone();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut first = BufReader::new(TcpStream::connect(proxy.address).await?);
        first
            .get_mut()
            .write_all(b"GET /first HTTP/1.1\r\nHost: test\r\n\r\n")
            .await?;
        let (owner, _) = upstream.accept().await?;
        let mut owner = BufReader::new(owner);
        ensure!(message(&mut owner, false).await?.is_some());
        owner
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await?;
        ensure!(
            message(&mut first, true)
                .await?
                .context("first reply missing")?
                .body
                == b"ok"
        );

        // Keep a second relay active while the first pooled caller disappears.
        let mut second = BufReader::new(TcpStream::connect(proxy.address).await?);
        second
            .get_mut()
            .write_all(b"GET /second HTTP/1.1\r\nHost: test\r\n\r\n")
            .await?;
        let (other, _) = upstream.accept().await?;
        let mut other = BufReader::new(other);
        ensure!(message(&mut other, false).await?.is_some());
        first.get_mut().set_zero_linger()?;
        drop(first);
        // EOF at the original owner proves the proxy observed the reset.
        ensure!(message(&mut owner, false).await?.is_none());
        ensure!(
            capture.failure().is_none(),
            "idle caller reset killed proxy: {:?}",
            capture.failure()
        );
        ensure!(capture.caller_disconnects() == 1);
        ensure!(!proxy.task.as_ref().unwrap().is_finished());
        other
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nother")
            .await?;
        ensure!(
            message(&mut second, true)
                .await?
                .context("other relay lost its reply")?
                .body
                == b"other"
        );
        drop(second);
        drop(other);

        // The listener must also accept new work after the reset.
        let mut third = BufReader::new(TcpStream::connect(proxy.address).await?);
        third
            .get_mut()
            .write_all(b"GET /third HTTP/1.1\r\nHost: test\r\n\r\n")
            .await?;
        let (last, _) = upstream.accept().await?;
        let mut last = BufReader::new(last);
        ensure!(message(&mut last, false).await?.is_some());
        last.get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nlast")
            .await?;
        ensure!(
            message(&mut third, true)
                .await?
                .context("new relay lost its reply")?
                .body
                == b"last"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("idle caller reset regression stalled")
    .and_then(|result| result);
    let joined = proxy.stop().await;
    outcome.with_context(|| format!("proxy_join={joined:?}"))?;
    joined?;
    Ok(())
}

#[tokio::test]
async fn idle_destination_eof_closes_only_its_relay() -> Result<()> {
    let upstream = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = Proxy::start(upstream.local_addr()?).await?;
    let capture = proxy.capture.clone();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut first = BufReader::new(TcpStream::connect(proxy.address).await?);
        first
            .get_mut()
            .write_all(b"GET /first HTTP/1.1\r\nHost: test\r\n\r\n")
            .await?;
        let (owner, _) = upstream.accept().await?;
        let mut owner = BufReader::new(owner);
        ensure!(message(&mut owner, false).await?.is_some());
        owner
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await?;
        ensure!(
            message(&mut first, true)
                .await?
                .context("first reply missing")?
                .body
                == b"ok"
        );

        // Another original signed request stays live while this idle owner
        // closes. No shutdown cut, retry or replay is supplied by the proxy.
        let mut second = BufReader::new(TcpStream::connect(proxy.address).await?);
        second
            .get_mut()
            .write_all(b"GET /second HTTP/1.1\r\nHost: test\r\n\r\n")
            .await?;
        let (other, _) = upstream.accept().await?;
        let mut other = BufReader::new(other);
        ensure!(message(&mut other, false).await?.is_some());
        drop(owner);
        ensure!(
            message(&mut first, true).await?.is_none(),
            "idle owner EOF did not reach caller"
        );
        ensure!(
            capture.failure().is_none(),
            "idle owner EOF killed proxy: {:?}",
            capture.failure()
        );
        ensure!(!proxy.task.as_ref().unwrap().is_finished());
        other
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nother")
            .await?;
        ensure!(
            message(&mut second, true)
                .await?
                .context("other reply lost")?
                .body
                == b"other"
        );
        drop(second);
        drop(other);

        let mut third = BufReader::new(TcpStream::connect(proxy.address).await?);
        third
            .get_mut()
            .write_all(b"GET /third HTTP/1.1\r\nHost: test\r\n\r\n")
            .await?;
        let (last, _) = upstream.accept().await?;
        let mut last = BufReader::new(last);
        ensure!(message(&mut last, false).await?.is_some());
        last.get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nlast")
            .await?;
        ensure!(
            message(&mut third, true)
                .await?
                .context("new reply lost")?
                .body
                == b"last"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("idle owner EOF was hidden from caller")
    .and_then(|result| result);
    let joined = proxy.stop().await;
    outcome.with_context(|| format!("proxy_join={joined:?}"))?;
    joined?;
    Ok(())
}

#[tokio::test]
async fn truncated_caller_framing_still_fails_the_proxy() -> Result<()> {
    for request in [
        b"GET / HTTP/1.1\r\nHost: incomplete".as_slice(),
        b"POST / HTTP/1.1\r\nContent-Length: 2\r\n\r\nx".as_slice(),
    ] {
        let upstream = TcpListener::bind("127.0.0.1:0").await?;
        let proxy = Proxy::start(upstream.local_addr()?).await?;
        let capture = proxy.capture.clone();
        let mut caller = TcpStream::connect(proxy.address).await?;
        caller.write_all(request).await?;
        caller.shutdown().await?;
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            caller.read_to_end(&mut response),
        )
        .await??;
        ensure!(response.is_empty());
        let error = proxy
            .stop()
            .await
            .expect_err("truncated caller framing was treated as cancellation");
        ensure!(format!("{error:#}").contains("read proxy caller request"));
        ensure!(capture.failure().is_some());
        ensure!(capture.caller_disconnects() == 0);
    }
    Ok(())
}

#[tokio::test]
async fn event_refusal_preserves_listener_and_armed_proven_reply_cut() -> Result<()> {
    let upstream = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = Proxy::start(upstream.local_addr()?).await?;
    proxy.capture.arm("__streamNext");
    let owner = tokio::spawn(async move {
        for refused in [true, false] {
            let (socket, _) = upstream.accept().await?;
            let mut socket = BufReader::new(socket);
            let request = message(&mut socket, false)
                .await?
                .context("event request missing")?;
            ensure!(request.header("x-cells-rpc-method") == Some("__streamNext"));
            let response = if refused {
                b"HTTP/1.1 503 Service Unavailable\r\nx-cellar-event-error: Unavailable\r\nContent-Length: 9\r\n\r\nnot ready".as_slice()
            } else {
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".as_slice()
            };
            socket.get_mut().write_all(response).await?;
            socket.get_mut().shutdown().await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let request = b"POST /rpc HTTP/1.1\r\nx-cells-rpc-method: __streamNext\r\nContent-Length: 2\r\n\r\n[]";
        let mut first = BufReader::new(TcpStream::connect(proxy.address).await?);
        first.get_mut().write_all(request).await?;
        let refusal = message(&mut first, true)
            .await?
            .context("proxy swallowed the original refusal")?;
        ensure!(refusal.head.starts_with(b"HTTP/1.1 503 "));
        ensure!(refusal.header("x-cellar-event-error") == Some("Unavailable"));
        ensure!(refusal.body == b"not ready");
        ensure!(proxy.capture.exchanges.lock().unwrap().is_empty());
        ensure!(proxy.capture.entered.available_permits() == 0);
        drop(first);

        // A refusal is forwarded unchanged; only the later successful reply
        // consumes the explicitly armed fault and is recorded as proven.
        let mut second = TcpStream::connect(proxy.address).await?;
        second.write_all(request).await?;
        proxy.capture.entered.acquire().await?.forget();
        {
            let exchanges = proxy.capture.exchanges.lock().unwrap();
            ensure!(exchanges.len() == 1);
            ensure!(exchanges[0].method == "__streamNext");
            ensure!(exchanges[0].response == b"{}" && exchanges[0].dropped);
        }
        proxy.capture.release.add_permits(1);
        let mut bytes = Vec::new();
        second.read_to_end(&mut bytes).await?;
        ensure!(bytes.is_empty(), "armed reply loss was bypassed");
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("event refusal/capture exchange stalled")
    .and_then(|result| result);
    if outcome.is_err() {
        owner.abort();
    }
    let owner_join = owner.await;
    let proxy_join = proxy.stop().await;
    outcome.with_context(|| format!("owner_join={owner_join:?}; proxy_join={proxy_join:?}"))?;
    owner_join??;
    proxy_join?;
    Ok(())
}

#[tokio::test]
async fn untyped_event_failure_still_fails_the_proxy() -> Result<()> {
    let upstream = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = Proxy::start(upstream.local_addr()?).await?;
    let capture = proxy.capture.clone();
    let owner = tokio::spawn(async move {
        let (socket, _) = upstream.accept().await?;
        let mut socket = BufReader::new(socket);
        ensure!(message(&mut socket, false).await?.is_some());
        socket
            .get_mut()
            .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
            .await?;
        Ok::<_, anyhow::Error>(())
    });
    let mut caller = BufReader::new(TcpStream::connect(proxy.address).await?);
    caller.get_mut().write_all(b"POST /rpc HTTP/1.1\r\nx-cells-rpc-method: __streamNext\r\nContent-Length: 2\r\n\r\n[]").await?;
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        message(&mut caller, true),
    )
    .await?;
    owner.await??;
    let error = proxy
        .stop()
        .await
        .expect_err("untyped failure was treated as a host admission refusal");
    ensure!(error.to_string() == "event RPC did not produce a successful HTTP reply");
    ensure!(
        capture.failure().as_deref() == Some(error.to_string().as_str()),
        "proxy failure was lost before fixture diagnostics could read it"
    );
    ensure!(response?.is_none());
    Ok(())
}

#[tokio::test]
async fn log_upgrade_relays_binary_bytes_and_joins_both_directions() -> Result<()> {
    // Include newline, NUL and non-UTF-8 bytes: an HTTP parser cannot relay
    // either side of this upgraded protocol. Coalesce the server's 101 head
    // and first payload to exercise bytes already held by its BufReader.
    const CLIENT_BYTES: &[u8] = b"CLT1\0\xff\nrequest\r\n\0";
    const SERVER_BYTES: &[u8] = b"CLB1\0\xfe\nresponse\r\n\0";
    let upstream = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = Proxy::start(upstream.local_addr()?).await?;
    let exchange = async {
        let caller = async {
            let mut caller = TcpStream::connect(proxy.address).await?;
            caller.write_all(b"POST /peer/log/tail HTTP/1.1\r\nConnection: upgrade\r\nUpgrade: cellar-log-stream\r\nContent-Length: 0\r\n\r\n").await?;
            let mut caller = BufReader::new(caller);
            let response = message(&mut caller, true)
                .await?
                .context("missing upgrade")?;
            ensure!(response.head.starts_with(b"HTTP/1.1 101 "));
            caller.get_mut().write_all(CLIENT_BYTES).await?;
            caller.get_mut().shutdown().await?;
            let mut received = Vec::new();
            caller.read_to_end(&mut received).await?;
            ensure!(received == SERVER_BYTES, "binary response bytes changed");
            Ok::<_, anyhow::Error>(())
        };
        let owner = async {
            let (socket, _) = upstream.accept().await?;
            let mut owner = BufReader::new(socket);
            let request = message(&mut owner, false)
                .await?
                .context("missing request")?;
            ensure!(request.header("upgrade") == Some("cellar-log-stream"));
            let mut response = b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: cellar-log-stream\r\n\r\n".to_vec();
            response.extend_from_slice(SERVER_BYTES);
            owner.get_mut().write_all(&response).await?;
            let mut received = Vec::new();
            owner.read_to_end(&mut received).await?;
            ensure!(received == CLIENT_BYTES, "binary request bytes changed");
            owner.get_mut().shutdown().await?;
            Ok::<_, anyhow::Error>(())
        };
        let (caller, owner) = tokio::join!(caller, owner);
        caller?;
        owner?;
        Ok::<_, anyhow::Error>(())
    };
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), exchange).await;
    let joined = proxy.stop().await;
    result.context("binary log upgrade exchange stalled")??;
    joined?;
    Ok(())
}

#[tokio::test]
async fn bodyless_abort_response_keeps_next_message_framing_intact() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut writer = TcpStream::connect(listener.local_addr()?).await?;
    let (reader, _) = listener.accept().await?;
    writer.write_all(b"HTTP/1.1 204 No Content\r\nConnection: keep-alive\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await?;
    let mut reader = BufReader::new(reader);
    let abort = message(&mut reader, true).await?.unwrap();
    ensure!(abort.head.starts_with(b"HTTP/1.1 204 ") && abort.body.is_empty());
    let next = message(&mut reader, true).await?.unwrap();
    ensure!(next.head.starts_with(b"HTTP/1.1 200 ") && next.body == b"ok");
    Ok(())
}

#[tokio::test]
async fn destination_reset_requires_an_explicit_shutdown_cut() -> Result<()> {
    for closing in [false, true] {
        let upstream = TcpListener::bind("127.0.0.1:0").await?;
        let proxy = Proxy::start(upstream.local_addr()?).await?;
        let (close, closed) = oneshot::channel();
        let owner = tokio::spawn(async move {
            let (socket, _) = upstream.accept().await?;
            let mut socket = BufReader::new(socket);
            ensure!(message(&mut socket, false).await?.is_some());
            socket.get_mut().write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: cellar-log-stream\r\n\r\n").await?;
            closed.await?;
            // A real reset on an upgraded log stream, after the caller has
            // received its handshake. No synthetic relay error is supplied.
            socket.get_mut().set_zero_linger()?;
            Ok::<_, anyhow::Error>(())
        });
        let mut caller = BufReader::new(TcpStream::connect(proxy.address).await?);
        caller.get_mut().write_all(b"POST /peer/log/stream HTTP/1.1\r\nConnection: upgrade\r\nUpgrade: cellar-log-stream\r\nContent-Length: 0\r\n\r\n").await?;
        ensure!(message(&mut caller, true).await?.is_some());
        if closing {
            proxy.capture.expect_upstream_close();
        }
        close.send(()).unwrap();
        owner.await??;
        let joined = proxy.stop().await;
        if closing {
            joined.context("declared destination reset failed proxy join")?;
        } else {
            let error = joined.expect_err("undeclared destination reset was hidden");
            ensure!(
                error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::ConnectionReset),
                "wrong relay failure: {error:#}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn controlled_destination_closure_preserves_framing_failures() -> Result<()> {
    let upstream = TcpListener::bind("127.0.0.1:0").await?;
    let ingress = TcpListener::bind("127.0.0.1:0").await?;
    let mut caller = TcpStream::connect(ingress.local_addr()?).await?;
    let (socket, _) = ingress.accept().await?;
    let capture = Arc::new(Capture::default());
    capture.expect_upstream_close();
    let relay = tokio::spawn(relay(socket, upstream.local_addr()?, capture));
    let (owner, _) = upstream.accept().await?;
    caller
        .write_all(b"GET / HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
        .await?;
    let mut owner = BufReader::new(owner);
    ensure!(message(&mut owner, false).await?.is_some());
    owner
        .get_mut()
        .write_all(b"HTTP/1.1 200 OK\r\n\r\n")
        .await?;
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), relay)
        .await??
        .expect_err("controlled closure hid malformed framing");
    ensure!(
        error.to_string() == "read proxy upstream reply",
        "{error:#}"
    );
    ensure!(
        error.root_cause().to_string() == "unframed peer response",
        "{error:#}"
    );
    Ok(())
}

#[tokio::test]
async fn binary_upgrades_forward_prefetched_bytes_and_eof() -> Result<()> {
    const REQUEST: &[u8] = b"\x01\x00\xffclient";
    const PREFIX: &[u8] = b"\x02\x00\xfeserver";
    const SUFFIX: &[u8] = b"\x00\xffdone";
    let upstream = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = Proxy::start(upstream.local_addr()?).await?;
    let owner = tokio::spawn(async move {
        let (socket, _) = upstream.accept().await?;
        let mut socket = BufReader::new(socket);
        let request = message(&mut socket, false)
            .await?
            .context("missing upgrade")?;
        ensure!(request.head.starts_with(b"GET /peer/log/tail HTTP/1.1"));
        let mut reply = b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: test-tail\r\n\r\n".to_vec();
        reply.extend_from_slice(PREFIX);
        socket.get_mut().write_all(&reply).await?;
        let mut received = Vec::new();
        socket.read_to_end(&mut received).await?;
        ensure!(
            received == REQUEST,
            "binary request changed or lost at upgrade"
        );
        socket.get_mut().write_all(SUFFIX).await?;
        socket.get_mut().shutdown().await?;
        Ok::<_, anyhow::Error>(())
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut client = TcpStream::connect(proxy.address).await?;
        let mut request = b"GET /peer/log/tail HTTP/1.1\r\nHost: test\r\nConnection: upgrade\r\nUpgrade: test-tail\r\n\r\n".to_vec();
        request.extend_from_slice(REQUEST);
        client.write_all(&request).await?;
        client.shutdown().await?;
        let mut client = BufReader::new(client);
        let reply = message(&mut client, true).await?.context("missing handshake")?;
        ensure!(reply.head.starts_with(b"HTTP/1.1 101 "));
        let mut received = Vec::new();
        client.read_to_end(&mut received).await?;
        ensure!(received == [PREFIX, SUFFIX].concat(), "binary reply changed or lost at upgrade");
        owner.await??;
        proxy.stop().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}

#[tokio::test]
async fn outer_tunnel_keeps_http_fault_capture_after_upgrade() -> Result<()> {
    let upstream = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = Proxy::start(upstream.local_addr()?).await?;
    proxy.capture.arm("__streamNext");
    let owner = tokio::spawn(async move {
        let (socket, _) = upstream.accept().await?;
        let mut socket = BufReader::new(socket);
        let request = message(&mut socket, false)
            .await?
            .context("missing tunnel")?;
        ensure!(request.head.starts_with(b"GET /peer/tunnel HTTP/1.1"));
        socket.get_mut().write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: test-tunnel\r\n\r\n").await?;
        let request = message(&mut socket, false)
            .await?
            .context("missing inner RPC")?;
        ensure!(
            request.header("x-cells-rpc-method") == Some("__streamNext") && request.body == b"[]"
        );
        socket
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
            .await?;
        socket.get_mut().shutdown().await?;
        let mut remaining = Vec::new();
        socket.read_to_end(&mut remaining).await?;
        ensure!(remaining.is_empty());
        Ok::<_, anyhow::Error>(())
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut client = TcpStream::connect(proxy.address).await?;
        client.write_all(b"GET /peer/tunnel HTTP/1.1\r\nHost: test\r\nConnection: upgrade\r\nUpgrade: test-tunnel\r\n\r\nPOST /rpc HTTP/1.1\r\nHost: test\r\nx-cells-rpc-method: __streamNext\r\nx-cells-scope: stream-test\r\nContent-Length: 2\r\n\r\n[]").await?;
        client.shutdown().await?;
        let mut client = BufReader::new(client);
        let reply = message(&mut client, true).await?.context("missing handshake")?;
        ensure!(reply.head.starts_with(b"HTTP/1.1 101 "));
        proxy.capture.entered.acquire().await?.forget();
        {
            let exchanges = proxy.capture.exchanges.lock().unwrap();
            ensure!(exchanges.len() == 1 && exchanges[0].method == "__streamNext"
                && exchanges[0].request == b"[]" && exchanges[0].response == b"{}"
                && exchanges[0].dropped && !exchanges[0].delayed);
        }
        proxy.capture.release.add_permits(1);
        let mut remaining = Vec::new();
        client.read_to_end(&mut remaining).await?;
        ensure!(remaining.is_empty(), "faulted RPC bypassed capture after tunnel upgrade");
        owner.await??;
        proxy.stop().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}

#[tokio::test]
async fn log_upgrades_preserve_buffered_binary_bytes_and_half_closes() -> Result<()> {
    const EARLY: &[u8] = b"\x03\0\0\0{}\0client-before-upgrade";
    const REPLY: &[u8] = b"\x04\0\0\0CTS1\r\n\r\n\0";
    const LATE: &[u8] = b"\0ack-after-peer-half-close\r\n";
    for path in ["/peer/log/tail", "/peer/log/stream"] {
        let upstream = TcpListener::bind("127.0.0.1:0").await?;
        let proxy = Proxy::start(upstream.local_addr()?).await?;
        let server = tokio::spawn(async move {
            let (socket, _) = upstream.accept().await?;
            let mut owner = BufReader::new(socket);
            let request = message(&mut owner, false)
                .await?
                .context("upgrade request missing")?;
            ensure!(request.head.starts_with(format!("POST {path} ").as_bytes()));
            ensure!(request.header("upgrade") == Some("cellar-log-stream"));
            ensure!(request.body.is_empty());
            let head = b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: cellar-log-stream\r\n\r\n";
            owner
                .get_mut()
                .write_all(&[head.as_slice(), REPLY].concat())
                .await?;
            // The peer can finish its output before the caller finishes input.
            owner.get_mut().shutdown().await?;
            let mut received = vec![0; EARLY.len() + LATE.len()];
            owner.read_exact(&mut received).await?;
            ensure!(
                received == [EARLY, LATE].concat(),
                "upgrade changed caller bytes"
            );
            ensure!(
                owner.read(&mut [0]).await? == 0,
                "caller half-close was lost"
            );
            Ok::<_, anyhow::Error>(())
        });
        let mut caller = BufReader::new(TcpStream::connect(proxy.address).await?);
        let request = format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: upgrade\r\nUpgrade: cellar-log-stream\r\nContent-Length: 0\r\n\r\n");
        // Both directions may already have bytes in their HTTP read buffers
        // when the 101 transfers ownership to the binary protocol.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            caller
                .get_mut()
                .write_all(&[request.as_bytes(), EARLY].concat())
                .await?;
            let response = message(&mut caller, true)
                .await?
                .context("upgrade reply missing")?;
            ensure!(response.head.starts_with(b"HTTP/1.1 101 "));
            let mut received = vec![0; REPLY.len()];
            caller.read_exact(&mut received).await?;
            ensure!(received == REPLY, "upgrade changed peer bytes");
            ensure!(
                caller.read(&mut [0]).await? == 0,
                "peer half-close was lost"
            );
            caller.get_mut().write_all(LATE).await?;
            caller.get_mut().shutdown().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("binary log upgrade did not progress")
        .and_then(|result| result);
        drop(caller);
        if outcome.is_err() {
            server.abort();
        }
        let server_join = server.await;
        let proxy_join = proxy.stop().await;
        outcome.with_context(|| {
            format!("{path}: server_join={server_join:?}; proxy_join={proxy_join:?}")
        })?;
        server_join??;
        proxy_join?;
    }
    Ok(())
}
