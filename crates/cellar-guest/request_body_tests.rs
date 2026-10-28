// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use futures_util::{stream, FutureExt};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[tokio::test]
async fn request_reads_pull_once_preserve_empty_chunks_and_source_failures() -> Result<()> {
    let reader = Reader::default();
    let pulls = Arc::new(AtomicUsize::new(0));
    let observed = pulls.clone();
    reader.install(Box::pin(
        stream::iter([
            Ok(vec![0, 255]),
            Ok(Vec::new()),
            Err("source failed".into()),
        ])
        .inspect(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        }),
    ))?;
    assert_eq!(pulls.load(Ordering::SeqCst), 0);
    assert_eq!(reader.read(2).await?, Some(vec![0, 255]));
    assert_eq!(pulls.load(Ordering::SeqCst), 1);
    assert_eq!(reader.read(2).await?, Some(Vec::new()));
    assert!(reader
        .read(2)
        .await
        .unwrap_err()
        .to_string()
        .contains("source failed"));
    assert!(reader.read(2).await.is_err());
    Ok(())
}

#[tokio::test]
async fn producer_chunks_split_into_bounded_frames_before_another_source_pull() -> Result<()> {
    let reader = Reader::default();
    let pulls = Arc::new(AtomicUsize::new(0));
    let observed = pulls.clone();
    let bytes = (0..1024 * 1024 + 17)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    reader.install(Box::pin(stream::iter([Ok(bytes.clone())]).inspect(
        move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        },
    )))?;
    let mut actual = Vec::new();
    while let Some(chunk) = reader.read(64 * 1024).await? {
        assert!(chunk.len() <= 64 * 1024);
        assert_eq!(pulls.load(Ordering::SeqCst), 1);
        actual.extend(chunk);
    }
    assert_eq!(actual, bytes);
    Ok(())
}

#[tokio::test]
async fn request_cancellation_joins_pending_source_and_refuses_concurrent_reads() -> Result<()> {
    struct Released(Arc<AtomicBool>);

    impl Drop for Released {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let reader = Reader::default();
    let released = Arc::new(AtomicBool::new(false));
    let guard = Released(released.clone());
    reader.install(Box::pin(stream::once(async move {
        let _guard = guard;
        std::future::pending::<Result<Vec<u8>, String>>().await
    })))?;
    let pending = reader.read(16);
    tokio::pin!(pending);
    assert!(pending.as_mut().now_or_never().is_none());
    assert!(reader.read(16).await.is_err());
    reader.cancel()?;
    assert!(pending.await.is_err());
    assert!(released.load(Ordering::SeqCst));
    assert!(reader.read(16).await.is_err());
    reader.cancel()?;
    Ok(())
}
