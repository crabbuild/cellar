// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Docker attach uses raw stdin writes and framed stdout/stderr reads. Decode
//! stdout incrementally without allocating a frame-sized buffer. Any diagnostic
//! or invalid frame poisons the connection; it cannot become protocol bytes.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub const MAX_FRAME_BYTES: u32 = 1024 * 1024;

pub struct DockerIo<T> {
    inner: T,
    header: [u8; 8],
    header_bytes: usize,
    remaining: u32,
    poisoned: bool,
    eof: bool,
}

impl<T> DockerIo<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            header: [0; 8],
            header_bytes: 0,
            remaining: 0,
            poisoned: false,
            eof: false,
        }
    }

    fn fail(&mut self, kind: io::ErrorKind, message: &'static str) -> Poll<io::Result<()>> {
        self.poisoned = true;
        Poll::Ready(Err(io::Error::new(kind, message)))
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for DockerIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.poisoned {
            return this.fail(
                io::ErrorKind::ConnectionAborted,
                "Docker attach stream is poisoned",
            );
        }
        if this.eof {
            return Poll::Ready(Ok(()));
        }
        // A daemon can emit empty frames. Keep a finite scheduling slice even
        // when the underlying stream can supply arbitrarily many immediately.
        for _ in 0..64 {
            if this.remaining == 0 {
                while this.header_bytes < this.header.len() {
                    let mut header = ReadBuf::new(&mut this.header[this.header_bytes..]);
                    match Pin::new(&mut this.inner).poll_read(cx, &mut header) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            this.poisoned = true;
                            return Poll::Ready(Err(error));
                        }
                        Poll::Ready(Ok(())) => {
                            let read = header.filled().len();
                            if read == 0 {
                                if this.header_bytes != 0 {
                                    return this.fail(
                                        io::ErrorKind::UnexpectedEof,
                                        "truncated Docker attach header",
                                    );
                                }
                                this.eof = true;
                                return Poll::Ready(Ok(()));
                            }
                            this.header_bytes += read;
                        }
                    }
                }
                if this.header[1..4] != [0, 0, 0] || !matches!(this.header[0], 1 | 2) {
                    return this.fail(io::ErrorKind::InvalidData, "invalid Docker attach header");
                }
                this.remaining =
                    u32::from_be_bytes(this.header[4..8].try_into().expect("fixed attach header"));
                this.header_bytes = 0;
                if this.remaining > MAX_FRAME_BYTES {
                    return this.fail(
                        io::ErrorKind::InvalidData,
                        "Docker attach frame exceeds limit",
                    );
                }
                if this.header[0] == 2 && this.remaining != 0 {
                    return this.fail(
                        io::ErrorKind::InvalidData,
                        "container network helper wrote stderr",
                    );
                }
                if this.remaining == 0 {
                    continue;
                }
            }
            let length = output.remaining().min(this.remaining as usize);
            let mut payload = ReadBuf::new(output.initialize_unfilled_to(length));
            match Pin::new(&mut this.inner).poll_read(cx, &mut payload) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    this.poisoned = true;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(())) => {
                    let read = payload.filled().len();
                    if read == 0 {
                        return this.fail(
                            io::ErrorKind::UnexpectedEof,
                            "truncated Docker attach payload",
                        );
                    }
                    this.remaining -= read as u32;
                    output.advance(read);
                    return Poll::Ready(Ok(()));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for DockerIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.poisoned {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Docker attach stream is poisoned",
            )));
        }
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.poisoned {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Docker attach stream is poisoned",
            )));
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.poisoned {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Docker attach stream is poisoned",
            )));
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
#[path = "docker_io_tests.rs"]
mod tests;
