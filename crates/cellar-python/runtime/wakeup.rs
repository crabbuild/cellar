//! Bounded, GIL-independent readiness for the interpreter's existing event loop.
// Only notification bytes cross this socket; SDK I/O stays in the native turn.
#![allow(clippy::disallowed_methods)]

use std::{
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
};

pub(super) const INVALIDATED: u8 = 1;
pub(super) const RETIRED: u8 = 2;
pub(super) const ADMITTED: u8 = 4;

pub(super) struct Wakeup {
    reader: UnixStream,
    writer: UnixStream,
    pending: AtomicBool,
    lifecycle: Arc<AtomicU8>,
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
            lifecycle: Arc::new(AtomicU8::new(0)),
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

    pub(super) fn retire(&self) {
        // The caller revokes owner authority before publishing this hint.
        // Coalesce arbitrarily many retirements without allocating a queue or
        // scanning every Python object on ordinary admissions and replies.
        // RMW publication also chains concurrent producers' owner revocations
        // into the consumer's acquire, even when their notifications coalesce.
        self.lifecycle
            .fetch_or(INVALIDATED | RETIRED, Ordering::AcqRel);
        self.notify();
    }

    pub(super) fn invalidate(&self) {
        self.lifecycle.fetch_or(INVALIDATED, Ordering::AcqRel);
        self.notify();
    }

    pub(super) fn admit(&self) {
        self.lifecycle.fetch_or(ADMITTED, Ordering::AcqRel);
        self.notify();
    }

    pub(super) fn pump_needed(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) != 0
    }

    pub(super) fn lifecycle(&self) -> Arc<AtomicU8> {
        self.lifecycle.clone()
    }

    pub(super) fn take_lifecycle(&self) -> u8 {
        // Clear BEFORE consuming starts or scanning owners. Publication racing
        // with either pass leaves a hint even if its readiness byte coalesces.
        self.lifecycle.swap(0, Ordering::AcqRel)
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
