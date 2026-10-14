//! Real peer tunnel proxy: capture a complete append reply, then lose it.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Semaphore};

#[derive(Clone, Debug)]
pub(super) struct Exchange {
    pub scope: String,
    pub request: Vec<u8>,
    pub response: Vec<u8>,
    pub dropped: bool,
}

pub(super) struct Capture {
    pub armed: AtomicBool,
    pub entered: Semaphore,
    pub release: Semaphore,
    pub exchanges: Mutex<Vec<Exchange>>,
}

impl Default for Capture {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            exchanges: Mutex::new(Vec::new()),
        }
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
            !response || result.head.starts_with(b"HTTP/1.1 101 "),
            "unframed peer response"
        );
    }
    Ok(Some(result))
}

async fn relay(socket: TcpStream, target: SocketAddr, capture: Arc<Capture>) -> Result<()> {
    let mut client = BufReader::new(socket);
    let mut owner = BufReader::new(TcpStream::connect(target).await?);
    while let Some(request) = message(&mut client, false).await? {
        request.write(owner.get_mut()).await?;
        let Some(reply) = message(&mut owner, true).await? else {
            break;
        };
        if request.header("x-cells-rpc-method") == Some("__streamAppend") {
            ensure!(
                reply.head.starts_with(b"HTTP/1.1 200 "),
                "append did not produce a successful HTTP reply"
            );
            let dropped = capture.armed.swap(false, Ordering::AcqRel);
            capture.exchanges.lock().unwrap().push(Exchange {
                scope: request.header("x-cells-scope").unwrap_or_default().into(),
                request: request.body.clone(),
                response: reply.body.clone(),
                dropped,
            });
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
