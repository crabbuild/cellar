// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Keep diagnostic I/O off lease and handler executors, without accepting
//! qualification reports whose diagnostic output was lost.
use anyhow::Result;
use std::io::{self, Write};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tracing_appender::non_blocking::{ErrorCounter, NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::fmt::writer::BoxMakeWriter;

const BUFFERED_LINES: usize = 8192;

pub(crate) struct Diagnostics {
    worker: WorkerGuard,
    dropped: ErrorCounter,
    progress: Arc<Progress>,
}

#[derive(Default)]
struct Progress {
    admitted: AtomicUsize,
    written: AtomicUsize,
    flushed: AtomicUsize,
    failed: AtomicBool,
}

#[derive(Clone)]
struct CountedWriter {
    inner: NonBlocking,
    progress: Arc<Progress>,
}

impl Write for CountedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(bytes)?;
        self.progress.admitted.fetch_add(count, Ordering::Release);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct CheckedOutput<W> {
    inner: W,
    progress: Arc<Progress>,
}

impl<W: Write> Write for CheckedOutput<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(bytes).inspect_err(|_| {
            self.progress.failed.store(true, Ordering::Release);
        })?;
        self.progress.written.fetch_add(count, Ordering::Release);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush().inspect_err(|_| {
            self.progress.failed.store(true, Ordering::Release);
        })?;
        self.progress.flushed.store(
            self.progress.written.load(Ordering::Acquire),
            Ordering::Release,
        );
        Ok(())
    }
}

fn output(sink: impl Write + Send + 'static, capacity: usize) -> (BoxMakeWriter, Diagnostics) {
    let progress = Arc::new(Progress::default());
    let (writer, worker) = NonBlockingBuilder::default()
        .buffered_lines_limit(capacity)
        // A saturated log sink must not become a lease/handler wait. A dropped
        // line invalidates qualification at the executable boundary instead.
        .lossy(true)
        .finish(CheckedOutput {
            inner: sink,
            progress: progress.clone(),
        });
    let dropped = writer.error_counter();
    let writer = CountedWriter {
        inner: writer,
        progress: progress.clone(),
    };
    (
        BoxMakeWriter::new(move || writer.clone()),
        Diagnostics {
            worker,
            dropped,
            progress,
        },
    )
}

pub(crate) fn initialize(filter: tracing_subscriber::EnvFilter) -> Diagnostics {
    let (writer, diagnostics) = output(std::io::stderr(), BUFFERED_LINES);
    crate::proof::initialize_diagnostics(filter, writer);
    diagnostics
}

impl Diagnostics {
    pub(crate) fn finish(self, result: Result<()>) -> Result<()> {
        // Called after owned runtimes have joined. Keep ordinary diagnostics
        // through final flush, then fail explicitly on bounded-queue loss or I/O.
        drop(self.worker);
        let dropped = self.dropped.dropped_lines();
        let failed = self.progress.failed.load(Ordering::Acquire);
        // WorkerGuard has a bounded flush wait. An exhausted flush wait is
        // incomplete evidence too, even if the queue never overflowed.
        let pending = self
            .progress
            .admitted
            .load(Ordering::Acquire)
            .saturating_sub(self.progress.flushed.load(Ordering::Acquire));
        if dropped == 0 && !failed && pending == 0 {
            return result;
        }
        let detail = format!("qualification diagnostics incomplete: dropped_lines={dropped}, output_failed={failed}, unflushed_bytes={pending}");
        match result {
            Ok(()) => Err(anyhow::anyhow!(detail)),
            Err(error) => Err(error.context(detail)),
        }
    }
}

#[cfg(test)]
#[path = "diagnostics_tests.rs"]
mod tests;
