// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Receive ordered append replies independently of the lane's writer.
//! Keep this receive loop directly testable at its actual network boundary.
use super::LogStreamIo;
use tokio::io::{AsyncRead, ReadHalf};

pub(super) type Responses = tokio::sync::mpsc::Receiver<Vec<u8>>;
type Sender = tokio::sync::mpsc::Sender<Vec<u8>>;

pub(super) const CAPACITY: usize = super::SERVE_BATCH_FRAMES;

fn channel() -> (Sender, Responses) {
    tokio::sync::mpsc::channel(CAPACITY)
}

pub(super) fn spawn(
    read: ReadHalf<LogStreamIo>,
) -> anyhow::Result<(Responses, crate::asyncrt::TaskHandle<()>)> {
    let (send, receive) = channel();
    let reader = crate::asyncrt::try_spawn(pump(read, send))?;
    Ok((receive, reader))
}

async fn pump<R: AsyncRead + Unpin>(mut read: R, send: Sender) {
    loop {
        // Reserve before allocating/reading another frame. A full mailbox
        // backpressures the socket; receiver closure releases this wait too.
        let Ok(permit) = send.reserve().await else {
            return;
        };
        let frame = crate::asyncrt::select_biased! {
            "reader cancellation wins a tie with a frame from the retired connection";
            _ = send.closed() => return,
            frame = super::read_frame_limited(&mut read, super::CONTROL_REPLY_CAP) => frame,
        };
        match frame {
            Ok(Some(frame)) => {
                permit.send(frame);
            }
            Ok(None) | Err(_) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt as _;
    use std::pin::Pin;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    struct HeaderOnly {
        prefix: [u8; 4],
        offset: usize,
        payload_requested: Arc<AtomicUsize>,
    }

    impl AsyncRead for HeaderOnly {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.offset < self.prefix.len() {
                let end = (self.offset + buffer.remaining()).min(self.prefix.len());
                buffer.put_slice(&self.prefix[self.offset..end]);
                self.offset = end;
                Poll::Ready(Ok(()))
            } else {
                self.payload_requested
                    .fetch_add(buffer.remaining(), Ordering::SeqCst);
                Poll::Ready(Err(std::io::Error::other(
                    "payload read after oversized metadata header",
                )))
            }
        }
    }

    #[tokio::test]
    async fn metadata_header_refuses_before_requesting_its_payload() -> anyhow::Result<()> {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let requested = Arc::new(AtomicUsize::new(0));
        let probe = requested.clone();
        let result = domain
            .run(async move {
                let (send, receive) = channel();
                pump(
                    HeaderOnly {
                        prefix: ((64_u32 << 10) + 1).to_le_bytes(),
                        offset: 0,
                        payload_requested: probe,
                    },
                    send,
                )
                .await;
                assert!(receive.is_closed());
            })
            .await;
        domain.shutdown().await;
        result?;
        assert_eq!(
            requested.load(Ordering::SeqCst),
            0,
            "ordered metadata reader requested an oversized payload"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reply_burst_backpressures_and_joins_when_receiver_closes() -> anyhow::Result<()> {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain
            .run(async {
                let body = br#"{"ok":true,"end":1,"epoch":1}"#;
                let mut bytes = Vec::new();
                for _ in 0..256 {
                    bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(body);
                }
                let (send, receive) = channel();
                let mut reader = Box::pin(pump(bytes.as_slice(), send));
                let mut finished = false;
                for _ in 0..256 {
                    finished = reader.as_mut().now_or_never().is_some();
                    if finished || receive.len() > 64 {
                        break;
                    }
                    // Give normal cooperative budgets a fresh poll without
                    // consuming queued replies or changing any clock/deadline.
                    let mut yielded = false;
                    futures_util::future::poll_fn(|context| {
                        if std::mem::replace(&mut yielded, true) {
                            Poll::Ready(())
                        } else {
                            context.waker().wake_by_ref();
                            Poll::Pending
                        }
                    })
                    .await;
                }
                let queued = receive.len();
                drop(receive);
                if !finished {
                    reader.await;
                }
                (queued, finished)
            })
            .await;
        domain.shutdown().await;
        let (queued, finished) = result?;
        assert!(
            queued <= 64,
            "ordered reply queue grew past 64 frames: {queued}"
        );
        assert!(
            !finished,
            "reply reader consumed the burst without backpressure"
        );
        Ok(())
    }

    #[tokio::test]
    async fn valid_metadata_preserves_fifo_across_backpressure_and_joins_at_eof(
    ) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt as _;
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain
            .run(crate::asyncrt::timeout(
                std::time::Duration::from_secs(5),
                async {
                    let (mut peer, io) = tokio::io::duplex(4096);
                    let (read, write) = tokio::io::split(Box::new(io) as LogStreamIo);
                    let (mut receive, reader) = spawn(read)?;
                    let writer = crate::asyncrt::try_spawn(async move {
                        for end in 0..80_u64 {
                            let mut body = serde_json::to_vec(&super::super::AppendResp {
                                ok: true,
                                end,
                                epoch: Some(1),
                            })?;
                            if end == 0 {
                                body.resize(super::super::CONTROL_REPLY_CAP, b' ');
                            }
                            super::super::write_frame(&mut peer, &body).await?;
                        }
                        peer.shutdown().await?;
                        Ok::<_, anyhow::Error>(())
                    })?;
                    for end in 0..80 {
                        let body = receive
                            .recv()
                            .await
                            .ok_or_else(|| anyhow::anyhow!("early reply EOF"))?;
                        let reply: super::super::AppendResp = serde_json::from_slice(&body)?;
                        anyhow::ensure!(reply.ok && reply.end == end && reply.epoch == Some(1));
                        if end == 0 {
                            anyhow::ensure!(body.len() == super::super::CONTROL_REPLY_CAP);
                        }
                    }
                    anyhow::ensure!(receive.recv().await.is_none());
                    writer.await??;
                    reader.await?;
                    drop(write);
                    Ok::<_, anyhow::Error>(())
                },
            ))
            .await;
        domain.shutdown().await;
        result???;
        Ok(())
    }

    #[tokio::test]
    async fn retirement_joins_a_spawned_reader_with_a_full_mailbox() -> anyhow::Result<()> {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain
            .run(crate::asyncrt::timeout(
                std::time::Duration::from_secs(5),
                async {
                    let (mut peer, io) = tokio::io::duplex(4096);
                    let (read, write) = tokio::io::split(Box::new(io) as LogStreamIo);
                    let (receive, reader) = spawn(read)?;
                    let writer = crate::asyncrt::try_spawn(async move {
                        for _ in 0..256 {
                            super::super::write_frame(&mut peer, &[1_u8; 128]).await?;
                        }
                        Ok::<_, anyhow::Error>(())
                    })?;
                    while receive.len() < 64 {
                        tokio::task::yield_now().await;
                    }
                    anyhow::ensure!(receive.len() == 64 && !receive.is_closed());
                    drop(receive);
                    drop(write);
                    // Normal reader completion, rather than domain cancellation,
                    // proves retirement releases its blocked queue reservation.
                    reader.await?;
                    let _ = writer.await?;
                    Ok::<_, anyhow::Error>(())
                },
            ))
            .await;
        domain.shutdown().await;
        result???;
        Ok(())
    }

    #[tokio::test]
    async fn data_frames_keep_their_larger_limit() -> anyhow::Result<()> {
        let body = vec![7_u8; (64 << 10) + 1];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&body);
        assert_eq!(
            super::super::read_frame(&mut bytes.as_slice()).await?,
            Some(body)
        );
        Ok(())
    }

    #[tokio::test]
    async fn closure_in_the_current_poll_refuses_reader_admission_without_panicking() {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let observed = Arc::new(AtomicUsize::new(0));
        let verdict = observed.clone();
        let _ = domain
            .run(async {
                let (_client, server) = tokio::io::duplex(64);
                let (read, write) = tokio::io::split(Box::new(server) as LogStreamIo);
                domain.close();
                let admitted =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| spawn(read)));
                let state = match admitted {
                    Ok(Err(error)) if error.is::<crate::asyncrt::DomainClosed>() => 1,
                    Err(_) => 2,
                    _ => 3,
                };
                verdict.store(state, Ordering::SeqCst);
                drop(write);
            })
            .await;
        domain.shutdown().await;
        assert_eq!(
            observed.load(Ordering::SeqCst),
            1,
            "closed reply-reader admission must return DomainClosed without panic or a new task"
        );
    }

    struct OneStream(std::sync::Mutex<Option<LogStreamIo>>);

    impl super::super::LogTransport for OneStream {
        fn post<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: &'a str,
            _: Vec<u8>,
            _: Option<std::time::Duration>,
        ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>>
        {
            Box::pin(async { anyhow::bail!("unexpected HTTP fallback") })
        }

        fn open_stream<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: &'a str,
        ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<LogStreamIo>> + Send + 'a>>
        {
            let io = self.0.lock().unwrap().take();
            Box::pin(async move { io.ok_or_else(|| anyhow::anyhow!("unexpected reconnect")) })
        }
    }

    #[tokio::test]
    async fn a_large_lane_window_drains_replies_while_submission_is_backpressured(
    ) -> anyhow::Result<()> {
        use super::super::{
            AppendReq, AppendResp, Entry, HedgeMode, LaneJob, LaneTuning, LogTransport, Member,
        };
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let requests = Arc::new(AtomicUsize::new(0));
        let answered = Arc::new(AtomicUsize::new(0));
        let settled = Arc::new(AtomicUsize::new(0));
        let observations = (requests.clone(), answered.clone(), settled.clone());
        let result = domain
            .run(crate::asyncrt::timeout(
                std::time::Duration::from_secs(5),
                async move {
                    // Small duplex buffers force actual bidirectional backpressure;
                    // no timers, sleeps, forged credit or larger queue make it pass.
                    let (client, mut peer) = tokio::io::duplex(128);
                    let transport: Arc<dyn LogTransport> =
                        Arc::new(OneStream(std::sync::Mutex::new(Some(Box::new(client)))));
                    let peer_task = crate::asyncrt::try_spawn(async move {
                        for sequence in 1..=128 {
                            let body = super::super::read_frame(&mut peer)
                                .await?
                                .ok_or_else(|| anyhow::anyhow!("early request EOF"))?;
                            let request = super::super::decode_append(&body)?;
                            anyhow::ensure!(
                                request.entries.len() == 1 && request.entries[0].seq == sequence
                            );
                            requests.fetch_add(1, Ordering::SeqCst);
                            let reply = serde_json::to_vec(&AppendResp {
                                ok: true,
                                end: sequence,
                                epoch: Some(1),
                            })?;
                            super::super::write_frame(&mut peer, &reply).await?;
                            answered.fetch_add(1, Ordering::SeqCst);
                        }
                        anyhow::ensure!(super::super::read_frame(&mut peer).await?.is_none());
                        Ok::<_, anyhow::Error>(())
                    })?;
                    let (send, receive) = tokio::sync::mpsc::unbounded_channel();
                    let mut replies = Vec::new();
                    for sequence in 1..=128 {
                        let (resp, response) = tokio::sync::oneshot::channel();
                        send.send(LaneJob {
                            req: Arc::new(AppendReq {
                                leader: "leader/00000000000000000000000000000001/generation".into(),
                                epoch: 1,
                                truncate_to: 0,
                                entries: vec![Entry {
                                    seq: sequence,
                                    cell: "cell".into(),
                                    cell_epoch: 1,
                                    txid: sequence,
                                    bytes: vec![1; 1024],
                                }],
                            }),
                            enqueued: crate::asyncrt::mono_ms(),
                            bytes: 1024,
                            resp,
                        })?;
                        replies.push(response);
                    }
                    let lane = crate::asyncrt::try_spawn(super::super::member_lane(
                        Member {
                            node: "follower".into(),
                            addr: "unused".into(),
                        },
                        transport,
                        Arc::new(cellar_logic::log_evict::EvictionPolicy::default()),
                        Arc::new(std::sync::Mutex::new(
                            cellar_logic::log_evict::FollowerHealth::default(),
                        )),
                        LaneTuning {
                            hedge: HedgeMode::Fixed(0),
                            stream: None,
                            stream_transport: true,
                        },
                        receive,
                        crate::ltx_repl::StopToken::new(),
                    ))?;
                    for (index, reply) in replies.into_iter().enumerate() {
                        anyhow::ensure!(
                            reply.await? == Some(("follower".into(), index as u64 + 1))
                        );
                        settled.fetch_add(1, Ordering::SeqCst);
                    }
                    drop(send);
                    lane.await?;
                    peer_task.await??;
                    Ok::<_, anyhow::Error>(())
                },
            ))
            .await;
        domain.shutdown().await;
        let result = match result {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(error.into()),
            Err(error) => Err(error.into()),
        };
        result.map_err(|error| {
            anyhow::anyhow!(
                "{error:#}; requests={} replies={} settled={}",
                observations.0.load(Ordering::SeqCst),
                observations.1.load(Ordering::SeqCst),
                observations.2.load(Ordering::SeqCst)
            )
        })
    }
}
