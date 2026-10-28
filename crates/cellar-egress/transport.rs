// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A bounded multiplexer over the engine-authenticated byte connection.
//! No tasks are spawned here: the owner must drive and join `Driver::run`.

use anyhow::{anyhow, ensure, Result};
use futures_util::future::poll_fn;
use std::{
    future::Future,
    pin::{pin, Pin},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore},
};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

/// 64 intercepted connections, 64 direct fallback connections and one control
/// stream. The two directions share this connection-wide stream ceiling.
pub const MAX_STREAMS: usize = 129;
const PENDING_OPENS: usize = 8;

// Leave room for recently dropped streams while their reset messages are
// processed. Application-visible streams are independently capped below.
const WIRE_STREAMS: usize = MAX_STREAMS + PENDING_OPENS;
pub const MAX_RECEIVE_WINDOW: usize = WIRE_STREAMS * 256 * 1024;

pub struct Stream {
    inner: Compat<yamux::Stream>,
    _capacity: OwnedSemaphorePermit,
    closed: Arc<AtomicBool>,
}

fn revoked() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        "egress run is closed",
    )
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(revoked()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(revoked()));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(revoked()));
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(revoked()));
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Clone, Copy)]
pub enum Role {
    Host,
    Container,
}

struct Open {
    reply: oneshot::Sender<Result<Stream>>,
    capacity: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct Link {
    opens: mpsc::Sender<Open>,
    close: watch::Sender<bool>,
    capacity: Arc<Semaphore>,
    closed: Arc<AtomicBool>,
}

pub struct Incoming {
    receive: mpsc::Receiver<Stream>,
    closed: Arc<AtomicBool>,
}

struct RunGuard(Arc<AtomicBool>);

impl Drop for RunGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub struct Driver<T> {
    connection: yamux::Connection<Compat<T>>,
    opens: mpsc::Receiver<Open>,
    inbound: mpsc::Sender<Stream>,
    closed: watch::Receiver<bool>,
    capacity: Arc<Semaphore>,
    guard: RunGuard,
}

pub fn session<T>(io: T, role: Role) -> (Link, Incoming, Driver<T>)
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut config = yamux::Config::default();
    config
        .set_max_num_streams(WIRE_STREAMS)
        .set_max_connection_receive_window(Some(MAX_RECEIVE_WINDOW))
        .set_read_after_close(false)
        .set_split_send_size(16 * 1024);
    let mode = match role {
        Role::Host => yamux::Mode::Server,
        Role::Container => yamux::Mode::Client,
    };
    let (send, opens) = mpsc::channel(PENDING_OPENS);
    // Accepted streams count against Yamux's ceiling while queued. This queue
    // cannot block packet progress for streams already being served.
    let (inbound, receive) = mpsc::channel(MAX_STREAMS);
    let (close, closed) = watch::channel(false);
    let capacity = Arc::new(Semaphore::new(MAX_STREAMS));
    let revoked = Arc::new(AtomicBool::new(false));
    (
        Link {
            opens: send,
            close,
            capacity: capacity.clone(),
            closed: revoked.clone(),
        },
        Incoming {
            receive,
            closed: revoked.clone(),
        },
        Driver {
            connection: yamux::Connection::new(io.compat(), config, mode),
            opens,
            inbound,
            closed,
            capacity,
            guard: RunGuard(revoked),
        },
    )
}

impl Link {
    /// Overload refuses before a stream is opened; no unbounded producer queue.
    pub async fn open(&self) -> Result<Stream> {
        ensure!(!self.closed.load(Ordering::Acquire), "egress run is closed");
        let (reply, receive) = oneshot::channel();
        let capacity = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow!("egress stream limit"))?;
        self.opens
            .try_send(Open { reply, capacity })
            .map_err(|_| anyhow!("egress open queue is closed or full"))?;
        let stream = receive
            .await
            .map_err(|_| anyhow!("egress driver stopped"))??;
        ensure!(!self.closed.load(Ordering::Acquire), "egress run is closed");
        Ok(stream)
    }

    /// Revokes the whole run transport, including buffered data in live streams.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.close.send_replace(true);
    }
}

impl Incoming {
    pub async fn next(&mut self) -> Option<Stream> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        let stream = self.receive.recv().await?;
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        Some(stream)
    }
}

impl<T> Driver<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn run(self) -> Result<()> {
        let Self {
            mut connection,
            mut opens,
            inbound,
            mut closed,
            capacity,
            guard,
        } = self;
        let run_closed = guard.0.clone();
        let _guard = guard;
        ensure!(!*closed.borrow(), "egress transport is closed");
        let mut pending: Option<Open> = None;
        let mut retirement = pin!(closed.changed());
        poll_fn(move |cx| {
            // Retirement always wins over packet progress. This is a revocation
            // boundary, not a scheduler choice between independent operations.
            if retirement.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Ok(()));
            }
            poll_connection(
                &mut connection,
                &mut opens,
                &inbound,
                &mut pending,
                (&capacity, &run_closed),
                cx,
            )
        })
        .await
    }
}

fn poll_connection<T>(
    connection: &mut yamux::Connection<Compat<T>>,
    opens: &mut mpsc::Receiver<Open>,
    inbound: &mpsc::Sender<Stream>,
    pending: &mut Option<Open>,
    state: (&Arc<Semaphore>, &Arc<AtomicBool>),
    cx: &mut Context<'_>,
) -> Poll<Result<()>>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    for _ in 0..64 {
        let mut progress = false;
        if pending.is_none() {
            if let Poll::Ready(Some(open)) = opens.poll_recv(cx) {
                *pending = Some(open);
            }
        }
        if let Some(open) = pending.as_ref() {
            if open.reply.is_closed() {
                *pending = None;
                progress = true;
            } else if let Poll::Ready(stream) = connection.poll_new_outbound(cx) {
                let open = pending.take().expect("pending open was checked");
                match stream {
                    Ok(stream) => {
                        let _ = open.reply.send(Ok(Stream {
                            inner: stream.compat(),
                            _capacity: open.capacity,
                            closed: state.1.clone(),
                        }));
                    }
                    Err(error) => {
                        let _ = open.reply.send(Err(anyhow!("egress connection failed")));
                        return Poll::Ready(Err(error.into()));
                    }
                }
                progress = true;
            }
        }
        match connection.poll_next_inbound(cx) {
            Poll::Ready(Some(Ok(stream))) => {
                let Ok(permit) = state.0.clone().try_acquire_owned() else {
                    return Poll::Ready(Err(anyhow!("egress stream limit")));
                };
                if inbound
                    .try_send(Stream {
                        inner: stream.compat(),
                        _capacity: permit,
                        closed: state.1.clone(),
                    })
                    .is_err()
                {
                    return Poll::Ready(Err(anyhow!("egress stream receiver is closed or full")));
                }
                progress = true;
            }
            Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error.into())),
            Poll::Ready(None) => return Poll::Ready(Ok(())),
            Poll::Pending => {}
        }
        if !progress {
            return Poll::Pending;
        }
    }
    cx.waker().wake_by_ref();
    Poll::Pending
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
