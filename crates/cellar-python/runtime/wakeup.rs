//! Bounded, GIL-independent readiness for the interpreter's existing event loop.
// Only notification bytes cross this socket; SDK I/O stays in the native turn.
#![allow(clippy::disallowed_methods)]

use std::{
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    sync::atomic::{AtomicBool, Ordering},
};

pub(super) struct Wakeup {
    reader: UnixStream,
    writer: UnixStream,
    pending: AtomicBool,
    failed: AtomicBool,
}

impl Wakeup {
    pub(super) fn new() -> io::Result<Self> {
        let (reader, writer) = UnixStream::pair()?;
        reader.set_nonblocking(true)?;
        writer.set_nonblocking(true)?;
        Ok(Self {
            reader,
            writer,
            pending: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        })
    }

    pub(super) fn fd(&self) -> i32 {
        self.reader.as_raw_fd()
    }

    pub(super) fn notify(&self) {
        // Publish work BEFORE calling notify. One byte can cover an entire
        // burst, including while the interpreter is blocked or holds the GIL.
        if self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        loop {
            match (&self.writer).write(&[1]) {
                Ok(1) => return,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return,
                _ => {
                    self.failed.store(true, Ordering::Release);
                    return;
                }
            }
        }
    }

    pub(super) fn check(&self) -> io::Result<()> {
        if self.failed.load(Ordering::Acquire) {
            Err(io::Error::other("Python wakeup channel failed"))
        } else {
            Ok(())
        }
    }

    pub(super) fn drain(&self) -> io::Result<()> {
        self.check()?;
        let mut bytes = [0; 256];
        loop {
            match (&self.reader).read(&mut bytes) {
                Ok(0) => return Err(io::Error::other("Python wakeup channel closed")),
                Ok(_) => (),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            }
        }
        // Clear AFTER draining and BEFORE inspecting queues/lifetime flags.
        // A producer racing before this clear is covered by the next scan;
        // a producer racing after it leaves a new readable notification.
        self.pending.swap(false, Ordering::AcqRel);
        Ok(())
    }
}

#[cfg(test)]
#[path = "wakeup_tests.rs"]
mod tests;
