// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::{
    io,
    sync::mpsc,
    time::{Duration, Instant},
};
use tracing::instrument::WithSubscriber;

struct HeldOutput {
    release: mpsc::Receiver<()>,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    finished: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for HeldOutput {
    fn drop(&mut self) {
        if let Some(finished) = self.finished.take() {
            let _ = finished.send(());
        }
    }
}

impl io::Write for HeldOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
            // A regression releases boundedly even when the timer's own
            // cancellation log strands the async worker.
            let _ = self.release.recv_timeout(Duration::from_secs(2));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct CancellationLog;

impl Drop for CancellationLog {
    fn drop(&mut self) {
        tracing::debug!(target: "store", event = "local_store_put_phase",
            phase = "caller_dropped", operation = 1, elapsed_us = 10_000,
            "development store write phase");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cancelled_store_diagnostics_do_not_delay_timeout_delivery() -> Result<()> {
    let (release, waiting) = mpsc::channel();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let (writer, diagnostics) = output(
        HeldOutput {
            release: waiting,
            entered: Some(entered),
            finished: None,
        },
        16,
    );
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(writer)
        .finish();
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let task = domain.spawn(
        async {
            let started = Instant::now();
            let result = cellar::asyncrt::timeout(Duration::from_millis(10), async {
                let _log = CancellationLog;
                std::future::pending::<()>().await;
            })
            .await;
            (result.is_err(), started.elapsed())
        }
        .with_subscriber(subscriber),
    )?;
    let (expired, elapsed) = task.await?;
    receive.await?;
    let _ = release.send(());
    domain.shutdown().await;
    diagnostics.finish(Ok(()))?;
    anyhow::ensure!(expired);
    anyhow::ensure!(
        elapsed < Duration::from_millis(200),
        "cancellation diagnostics delayed the 10 ms timeout by {elapsed:?}"
    );
    Ok(())
}

#[tokio::test]
async fn saturated_diagnostics_refuse_qualification_without_hiding_primary_failure() -> Result<()> {
    let (release, waiting) = mpsc::channel();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let (writer, diagnostics) = output(
        HeldOutput {
            release: waiting,
            entered: Some(entered),
            finished: None,
        },
        2,
    );
    let subscriber = tracing_subscriber::fmt().with_writer(writer).finish();
    async {
        tracing::warn!("hold output");
        receive.await?;
        for _ in 0..16 {
            tracing::warn!("bounded diagnostic burst");
        }
        Ok::<_, anyhow::Error>(())
    }
    .with_subscriber(subscriber)
    .await?;
    let _ = release.send(());
    let error = diagnostics
        .finish(Err(anyhow::anyhow!("primary run failure")))
        .unwrap_err();
    anyhow::ensure!(error.to_string().contains("diagnostics incomplete"));
    anyhow::ensure!(error.root_cause().to_string() == "primary run failure");
    Ok(())
}

#[test]
fn output_io_failure_refuses_qualification() {
    struct Broken;

    impl io::Write for Broken {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (writer, diagnostics) = output(Broken, 16);
    tracing::subscriber::with_default(
        tracing_subscriber::fmt().with_writer(writer).finish(),
        || {
            tracing::warn!("failed diagnostic output");
        },
    );
    let error = diagnostics.finish(Ok(())).unwrap_err();
    assert!(error.to_string().contains("output_failed=true"));
}

#[tokio::test]
async fn unfinished_output_refuses_qualification() -> Result<()> {
    let (release, waiting) = mpsc::channel();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let (finished, joined) = tokio::sync::oneshot::channel();
    let (writer, diagnostics) = output(
        HeldOutput {
            release: waiting,
            entered: Some(entered),
            finished: Some(finished),
        },
        16,
    );
    let subscriber = tracing_subscriber::fmt().with_writer(writer).finish();
    async {
        tracing::warn!("held final output");
        receive.await?;
        Ok::<_, anyhow::Error>(())
    }
    .with_subscriber(subscriber)
    .await?;
    let error = diagnostics.finish(Ok(())).unwrap_err();
    let _ = release.send(());
    joined.await?;
    assert!(error.to_string().contains("diagnostics incomplete"));
    assert!(!error.to_string().ends_with("unflushed_bytes=0"));
    Ok(())
}
