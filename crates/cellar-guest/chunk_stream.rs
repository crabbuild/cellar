// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A bridge frame does not have to match a producer's allocation boundary.

use anyhow::{ensure, Result};
use bytes::Bytes;
use cellar::engine_api::HttpChunkStream;
use futures_util::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

struct Chunks {
    source: Option<HttpChunkStream>,
    pending: Bytes,
    maximum: usize,
    owner: std::sync::Arc<cellar::host_services::HostServices>,
}

impl Stream for Chunks {
    type Item = std::result::Result<Vec<u8>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.source.is_none() && self.pending.is_empty() {
            return Poll::Ready(None);
        }
        if self.owner.is_closed() {
            self.pending = Bytes::new();
            self.source = None;
            return Poll::Ready(Some(Err(
                "response source execution domain is closed".into()
            )));
        }
        if self.pending.is_empty() {
            let Some(source) = self.source.as_mut() else {
                return Poll::Ready(None);
            };
            match source.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.source = None;
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(error))) => {
                    self.source = None;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(bytes))) => self.pending = Bytes::from(bytes),
            }
        }
        let count = self.pending.len().min(self.maximum);
        Poll::Ready(Some(Ok(self.pending.split_to(count).to_vec())))
    }
}

pub(crate) fn bounded(source: HttpChunkStream, maximum: usize) -> Result<HttpChunkStream> {
    ensure!(maximum > 0, "stream framing requires a positive limit");
    Ok(Box::pin(Chunks {
        source: Some(source),
        pending: Bytes::new(),
        maximum,
        owner: cellar::asyncrt::services(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    struct Source {
        polls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl Stream for Source {
        type Item = std::result::Result<Vec<u8>, String>;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let poll = self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Some(if poll == 0 {
                Ok((0..10).collect())
            } else {
                Err("source failed".into())
            }))
        }
    }

    impl Drop for Source {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn pending_bytes_do_not_pull_again_and_cancellation_drops_the_source() -> Result<()> {
        let origin = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let mut stream = origin
            .run(async {
                bounded(
                    Box::pin(Source {
                        polls: polls.clone(),
                        dropped: dropped.clone(),
                    }),
                    3,
                )
            })
            .await??;
        assert_eq!(stream.next().await.unwrap().unwrap(), vec![0, 1, 2]);
        assert_eq!(stream.next().await.unwrap().unwrap(), vec![3, 4, 5]);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        drop(stream);
        assert!(dropped.load(Ordering::SeqCst));
        origin.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn framing_preserves_exact_bytes_and_errors_without_successful_truncation() -> Result<()>
    {
        let origin = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let mut stream = origin
            .run(async {
                bounded(
                    Box::pin(Source {
                        polls,
                        dropped: dropped.clone(),
                    }),
                    3,
                )
            })
            .await??;
        let mut bytes = Vec::new();
        for length in [3, 3, 3, 1] {
            let chunk = stream.next().await.unwrap().map_err(anyhow::Error::msg)?;
            assert_eq!(chunk.len(), length);
            bytes.extend(chunk);
        }
        assert_eq!(bytes, (0..10).collect::<Vec<_>>());
        assert_eq!(stream.next().await.unwrap().unwrap_err(), "source failed");
        assert!(stream.next().await.is_none());
        assert!(dropped.load(Ordering::SeqCst));
        origin.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn buffered_frames_cannot_borrow_a_replacement_execution_domain() -> Result<()> {
        let origin = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
        let replacement = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
        let dropped = Arc::new(AtomicBool::new(false));
        let mut stream = origin
            .run(async {
                bounded(
                    Box::pin(Source {
                        polls: Arc::new(AtomicUsize::new(0)),
                        dropped: dropped.clone(),
                    }),
                    3,
                )
            })
            .await??;
        assert_eq!(stream.next().await.unwrap().unwrap(), vec![0, 1, 2]);
        origin.shutdown().await;
        let next = replacement.run(async { stream.next().await }).await?;
        assert!(
            next.unwrap().is_err(),
            "pending bytes escaped their closed origin"
        );
        assert!(dropped.load(Ordering::SeqCst));
        replacement.shutdown().await;
        Ok(())
    }
}
