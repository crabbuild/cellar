// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Count successful socket I/O below TLS, including handshakes and refused
//! traffic. Kernel-accepted bytes do not prove delivery; TCP/IP overhead and
//! retransmissions are outside the AsyncRead/AsyncWrite boundary.
use crate::raft::performance::{Counter, Registry};
use std::{
    io::{self, IoSlice},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Clone, Copy)]
pub(super) enum Lane {
    Control,
    Data,
    Query,
}

impl From<super::Lane> for Lane {
    fn from(lane: super::Lane) -> Self {
        match lane {
            super::Lane::Control => Self::Control,
            super::Lane::Data => Self::Data,
        }
    }
}

pub(super) struct Socket<S> {
    socket: S,
    performance: Arc<Registry>,
    read: Counter,
    written: Counter,
}

impl<S> Socket<S> {
    pub(super) fn new(socket: S, performance: Arc<Registry>, lane: Lane) -> Self {
        let (read, written) = match lane {
            Lane::Control => (Counter::ControlReadBytes, Counter::ControlWriteBytes),
            Lane::Data => (Counter::DataReadBytes, Counter::DataWriteBytes),
            Lane::Query => (Counter::QueryReadBytes, Counter::QueryWriteBytes),
        };
        Self {
            socket,
            performance,
            read,
            written,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Socket<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.socket).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.performance
                .increment(self.read, (buf.filled().len() - before) as u64);
        }
        result
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Socket<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.socket).poll_write(cx, buf);
        if let Poll::Ready(Ok(bytes)) = result {
            self.performance.increment(self.written, bytes as u64);
        }
        result
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.socket).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(bytes)) = result {
            self.performance.increment(self.written, bytes as u64);
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

impl super::State {
    pub(super) fn observed_socket(
        &self,
        socket: tokio::net::TcpStream,
        lane: Lane,
    ) -> Socket<tokio::net::TcpStream> {
        Socket::new(socket, self.performance.clone(), lane)
    }
}

#[cfg(test)]
#[path = "transport_metrics_tests.rs"]
mod tests;
