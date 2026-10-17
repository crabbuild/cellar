//! Real peer tunnel proxy: hold signed submissions or capture complete replies
//! before controlled delay/loss. Packets and production proof stay unchanged.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context, Result};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
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
        }
    }
}

impl Capture {
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
    match result {
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
    }
}

async fn relay_inner(socket: TcpStream, target: SocketAddr, capture: Arc<Capture>) -> Result<()> {
    let mut client = BufReader::new(socket);
    let upstream = TcpStream::connect(target).await?;
    let mut owner = BufReader::new(upstream);
    while let Some(request) = message(&mut client, false).await? {
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
        request.write(owner.get_mut()).await?;
        let Some(reply) = message(&mut owner, true).await? else {
            break;
        };
        let method = request.header("x-cells-rpc-method");
        if matches!(
            method,
            Some("__streamAppend" | "__streamAck" | "__streamNext" | "__streamExtend")
        ) {
            let method = method.unwrap();
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
        reply.write(client.get_mut()).await?;
        if reply.head.starts_with(b"HTTP/1.1 101 ")
            && request.request_path() != Some("/peer/tunnel")
        {
            // Only the outer tunnel carries inspectable HTTP after upgrade.
            // Log tails carry binary frames. Keep both readers so prefetched
            // bytes and half-close propagation survive the protocol switch.
            tokio::io::copy_bidirectional(&mut client, &mut owner).await?;
            return Ok(());
        }
    }
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
    ensure!(error.to_string() == "unframed peer response", "{error:#}");
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
