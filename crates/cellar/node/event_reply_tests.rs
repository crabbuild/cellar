use super::*;
use http_body_util::{Full, StreamBody};
use hyper::body::{Frame, SizeHint};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

fn assert_ambiguous_protocol(failure: &anyhow::Error) {
    assert_eq!(
        failure.downcast_ref::<DeliveryError>().unwrap().code,
        ErrorCode::Ambiguous
    );
    assert!(failure
        .downcast_ref::<super::super::RemoteRpcTransportFailure>()
        .is_none());
    assert_eq!(
        super::super::classify_remote_attempt(failure),
        cellar_logic::routing::Attempt::Ambiguous
    );
}

#[tokio::test]
async fn exact_budget_and_unknown_length_chunks_are_accepted() -> Result<()> {
    assert_eq!(
        bounded(Full::new(Bytes::from_static(b"abcd")), 4).await?,
        "abcd"
    );
    let frames: Vec<Result<_, std::io::Error>> = vec![
        Ok(Frame::data(Bytes::from_static(b"ab"))),
        Ok(Frame::data(Bytes::from_static(b"cd"))),
    ];
    assert_eq!(
        bounded(StreamBody::new(futures_util::stream::iter(frames)), 4).await?,
        "abcd"
    );
    Ok(())
}

struct DeclaredBody {
    polls: Arc<AtomicUsize>,
    length: u64,
}

impl Body for DeclaredBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(None)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.length)
    }
}

#[tokio::test]
async fn oversized_declared_body_is_refused_without_polling() {
    let polls = Arc::new(AtomicUsize::new(0));
    let failure = bounded(
        DeclaredBody {
            polls: polls.clone(),
            length: 5,
        },
        4,
    )
    .await
    .unwrap_err();
    assert_ambiguous_protocol(&failure);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn chunk_overflow_stops_before_collecting_the_suffix() {
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let frames = [b"ab".as_slice(), b"cd", b"e", b"suffix"];
    let frames = futures_util::stream::iter(frames.into_iter().map(move |data| {
        observed.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(Frame::data(Bytes::from_static(data)))
    }));
    let failure = bounded(StreamBody::new(frames), 4).await.unwrap_err();
    assert_ambiguous_protocol(&failure);
    assert_eq!(polls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn real_body_transport_failure_retains_its_activation_tag() {
    let frames = vec![
        Ok(Frame::data(Bytes::from_static(b"prefix"))),
        Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "peer closed",
        )),
    ];
    let failure = receive(
        StreamBody::new(futures_util::stream::iter(frames)),
        true,
        None,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(
        failure.downcast_ref::<DeliveryError>().unwrap().code,
        ErrorCode::Ambiguous
    );
    assert!(failure
        .downcast_ref::<super::super::RemoteRpcTransportFailure>()
        .is_some());
    assert!(failure.downcast_ref::<std::io::Error>().is_some());
    assert_eq!(
        super::super::classify_remote_attempt(&failure),
        cellar_logic::routing::Attempt::Ambiguous
    );
}

#[tokio::test]
async fn cancellation_wins_ready_success_and_dropped_sender_without_polling_body() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            for send in [true, false] {
                let (sender, mut cancel) = oneshot::channel();
                if send {
                    sender.send(()).unwrap();
                } else {
                    drop(sender);
                }
                let polls = Arc::new(AtomicUsize::new(0));
                let observed = polls.clone();
                let frames = futures_util::stream::once(async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, std::io::Error>(Frame::data(Bytes::from_static(b"ready")))
                });
                let failure = receive(
                    StreamBody::new(Box::pin(frames)),
                    true,
                    Some(&mut cancel),
                    None,
                )
                .await
                .unwrap_err();
                assert_ambiguous_protocol(&failure);
                assert_eq!(polls.load(Ordering::SeqCst), 0);
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn receiver_enforces_the_shared_wire_ceiling_before_polling() {
    let polls = Arc::new(AtomicUsize::new(0));
    let failure = receive(
        DeclaredBody {
            polls: polls.clone(),
            length: MAX_RPC_REPLY_BYTES as u64 + 1,
        },
        true,
        None,
        None,
    )
    .await
    .unwrap_err();
    assert_ambiguous_protocol(&failure);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

struct PendingBody {
    polled: Option<oneshot::Sender<()>>,
    dropped: Arc<AtomicUsize>,
}

impl Body for PendingBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if let Some(sender) = self.polled.take() {
            let _ = sender.send(());
        }
        Poll::Pending
    }
}

impl Drop for PendingBody {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancellation_after_body_poll_drops_the_stalled_body() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let dropped = Arc::new(AtomicUsize::new(0));
            let (entered, entry) = oneshot::channel();
            let body = PendingBody {
                polled: Some(entered),
                dropped: dropped.clone(),
            };
            let (sender, mut cancel) = oneshot::channel();
            let receiving = receive(body, true, Some(&mut cancel), None);
            let cancelling = async {
                entry.await.unwrap();
                sender.send(()).unwrap();
            };
            let (result, ()) = futures_util::future::join(receiving, cancelling).await;
            assert_ambiguous_protocol(&result.unwrap_err());
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn dropping_the_receiving_future_releases_its_body() {
    use futures_util::FutureExt;
    let dropped = Arc::new(AtomicUsize::new(0));
    let body = PendingBody {
        polled: None,
        dropped: dropped.clone(),
    };
    assert!(receive(body, true, None, None).now_or_never().is_none());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn hyper_incoming_distinguishes_declared_overflow_from_truncated_transport() -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result=domain.run(async {
        for oversized in [true,false] {
            let (client,mut peer)=tokio::io::duplex(4096);
            let (mut sender,connection)=hyper::client::conn::http1::Builder::new()
                .handshake::<_,Full<Bytes>>(hyper_util::rt::TokioIo::new(client)).await?;
            let driver=crate::asyncrt::spawn(connection);
            let response=crate::asyncrt::spawn(async move {
                let mut bytes=Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    let mut byte=[0];peer.read_exact(&mut byte).await?;bytes.push(byte[0]);
                    anyhow::ensure!(bytes.len()<=4096,"request head exceeds fixture budget");
                }
                let length=if oversized {MAX_RPC_REPLY_BYTES+1}else{10};
                peer.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\nx").as_bytes()).await?;
                peer.shutdown().await?;
                Ok::<_,anyhow::Error>(())
            });
            let request=hyper::Request::builder().uri("/").body(Full::new(Bytes::new()))?;
            let response_head=sender.send_request(request).await?;
            let failure=receive(response_head.into_body(),true,None,None).await.unwrap_err();
            assert_eq!(failure.downcast_ref::<DeliveryError>().unwrap().code,ErrorCode::Ambiguous);
            assert_eq!(failure.downcast_ref::<super::super::RemoteRpcTransportFailure>().is_some(),!oversized);
            drop(sender);
            response.await??;
            let _=driver.await?;
        }
        Ok::<_,anyhow::Error>(())
    }).await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn exhausted_original_deadline_refuses_even_a_ready_body() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let polls = Arc::new(AtomicUsize::new(0));
            let failure = receive(
                DeclaredBody {
                    polls: polls.clone(),
                    length: 0,
                },
                true,
                None,
                Some(0),
            )
            .await
            .unwrap_err();
            assert_ambiguous_protocol(&failure);
            assert!(failure.to_string().contains("deadline"));
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn original_deadline_drops_a_stalled_body_without_network_retry_tag() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let dropped = Arc::new(AtomicUsize::new(0));
            let body = PendingBody {
                polled: None,
                dropped: dropped.clone(),
            };
            let deadline = crate::asyncrt::mono_ms() + 10;
            let result = crate::asyncrt::timeout(
                std::time::Duration::from_secs(2),
                receive(body, true, None, Some(deadline)),
            )
            .await?;
            let failure = result.unwrap_err();
            assert_ambiguous_protocol(&failure);
            assert!(failure.to_string().contains("deadline"));
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn cancellation_wins_a_tie_with_an_exhausted_deadline() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (sender, mut cancel) = oneshot::channel();
            sender.send(()).unwrap();
            let polls = Arc::new(AtomicUsize::new(0));
            let failure = receive(
                DeclaredBody {
                    polls: polls.clone(),
                    length: 0,
                },
                true,
                Some(&mut cancel),
                Some(0),
            )
            .await
            .unwrap_err();
            assert_ambiguous_protocol(&failure);
            assert!(format!("{failure:#}").contains("cancelled"));
            assert!(!format!("{failure:#}").contains("deadline"));
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn many_tiny_frames_are_coalesced_within_the_byte_budget() -> Result<()> {
    let frames = (0..4097).map(|_| Ok::<_, std::io::Error>(Frame::data(Bytes::from_static(b"x"))));
    let bytes = bounded(StreamBody::new(futures_util::stream::iter(frames)), 4097).await?;
    assert_eq!(bytes.len(), 4097);
    assert!(bytes.iter().all(|byte| *byte == b'x'));
    Ok(())
}

#[tokio::test]
async fn unused_trailers_are_not_part_of_the_rpc_result() -> Result<()> {
    let mut trailers = hyper::HeaderMap::new();
    trailers.insert(
        "x-untrusted",
        hyper::header::HeaderValue::from_static("discarded"),
    );
    let frames: Vec<Result<_, std::io::Error>> = vec![
        Ok(Frame::data(Bytes::from_static(b"data"))),
        Ok(Frame::trailers(trailers)),
    ];
    assert_eq!(
        bounded(StreamBody::new(futures_util::stream::iter(frames)), 4).await?,
        "data"
    );
    Ok(())
}

#[tokio::test]
async fn always_ready_empty_frames_cannot_starve_cancellation() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let polls = Arc::new(AtomicUsize::new(0));
            let observed = polls.clone();
            let (entered, entry) = oneshot::channel();
            let mut entered = Some(entered);
            let frames = futures_util::stream::poll_fn(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                if let Some(entered) = entered.take() {
                    let _ = entered.send(());
                }
                Poll::Ready(Some(Ok::<_, std::io::Error>(Frame::data(Bytes::new()))))
            });
            let (sender, mut cancel) = oneshot::channel();
            let collecting = receive(StreamBody::new(frames), true, Some(&mut cancel), None);
            let cancelling = async {
                entry.await.unwrap();
                sender.send(()).unwrap();
            };
            let (result, ()) = crate::asyncrt::timeout(
                std::time::Duration::from_secs(2),
                futures_util::future::join(collecting, cancelling),
            )
            .await?;
            assert_ambiguous_protocol(&result.unwrap_err());
            assert!(
                polls.load(Ordering::SeqCst) <= 128,
                "collector did not yield bounded work"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn very_large_captured_budget_still_accepts_a_ready_body() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let bytes = receive(
                Full::new(Bytes::from_static(b"ready")),
                true,
                None,
                Some(u64::MAX),
            )
            .await?;
            assert_eq!(bytes, "ready");
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}
