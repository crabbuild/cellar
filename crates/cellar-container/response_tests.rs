// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn response(stream: HttpChunkStream) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: vec![("x-test".into(), "value".into())],
        body: vec![0, 255],
        stream: Some(stream),
        websocket: None,
        write_position: None,
        observed_position: None,
    }
}

#[test]
fn head_does_not_poll_stream_and_empty_chunk_is_not_eof() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(async {
            let polls = Arc::new(AtomicUsize::new(0));
            let probe = polls.clone();
            let chunks =
                futures_util::stream::iter([Ok(vec![]), Ok(vec![42, 0]), Ok(vec![255, 1])])
                    .inspect(move |_| {
                        probe.fetch_add(1, Ordering::SeqCst);
                    });
            let (head, reader) = Reader::open(response(Box::pin(chunks)), 2)?;
            assert_eq!(head["body"], "AP8=");
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            let reader = reader.unwrap();
            assert_eq!(reader.read(2).await?, json!({"done":false,"body":""}));
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert_eq!(reader.read(2).await?["body"], "KgA=");
            assert_eq!(reader.read(2).await?["body"], "/wE=");
            assert_eq!(reader.read(2).await?, json!({"done":true,"body":""}));
            // Total bytes exceed the per-frame limit, without whole-body collection.
            Ok(())
        })
}

#[test]
fn cancellation_interrupts_pending_read_and_drops_stream() -> Result<()> {
    struct Pending(Arc<AtomicUsize>);

    impl futures_util::Stream for Pending {
        type Item = std::result::Result<Vec<u8>, String>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Pending
        }
    }

    impl Drop for Pending {
        fn drop(&mut self) {
            self.0.store(usize::MAX, Ordering::SeqCst);
        }
    }
    tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(async {
            let polls = Arc::new(AtomicUsize::new(0));
            let (_, reader) = Reader::open(response(Box::pin(Pending(polls.clone()))), 2)?;
            let reader = reader.unwrap();
            let read = reader.read(2);
            futures_util::pin_mut!(read);
            assert!(futures_util::poll!(&mut read).is_pending());
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert!(reader.read(2).await.unwrap_err().is::<Validation>());
            reader.cancel();
            assert!(read.await.unwrap_err().to_string().contains("cancelled"));
            reader.cancel();
            assert_eq!(polls.load(Ordering::SeqCst), usize::MAX);
            assert!(reader.read(2).await.is_err());
            Ok(())
        })
}

#[test]
fn invalid_heads_and_oversized_chunks_refuse_without_rollback_claim() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(async {
            let chunks = futures_util::stream::iter([Ok(vec![0; 3])]);
            let (_, reader) = Reader::open(response(Box::pin(chunks)), 2)?;
            let error = reader.unwrap().read(2).await.unwrap_err();
            assert!(!error.is::<Validation>());
            let mut invalid = response(Box::pin(futures_util::stream::pending()));
            invalid.status = 101;
            assert!(Reader::open(invalid, 2).is_err());
            assert!(Reader::open(response(Box::pin(futures_util::stream::pending())), 1).is_err());
            Ok(())
        })
}
