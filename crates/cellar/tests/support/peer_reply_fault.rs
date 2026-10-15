//! Real peer tunnel proxy: capture a complete event RPC reply, then lose it.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
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

pub(super) struct Capture {
    armed: Mutex<Option<(&'static str, Fault)>>,
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

    /// A fenced destination can refuse new tunnel connections while cached
    /// callers discover its successor. Only this controlled refusal is normal.
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
    let mut client = BufReader::new(socket);
    let upstream = match TcpStream::connect(target).await {
        Ok(socket) => socket,
        Err(error)
            if capture.upstream_closing.load(Ordering::Acquire)
                && error.kind() == std::io::ErrorKind::ConnectionRefused =>
        {
            return Ok(())
        }
        Err(error) => return Err(error.into()),
    };
    let mut owner = BufReader::new(upstream);
    while let Some(request) = message(&mut client, false).await? {
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
        if matches!(method, Some("__streamAppend" | "__streamAck")) {
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
    }
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
