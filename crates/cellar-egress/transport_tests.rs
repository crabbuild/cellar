// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]

use super::*;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinHandle,
};

struct Pair {
    host: Link,
    host_in: Incoming,
    container: Link,
    container_in: Incoming,
    tasks: Vec<JoinHandle<Result<()>>>,
    abrupt_peer_close: bool,
}

impl Pair {
    fn new() -> Self {
        let (left, right) = tokio::io::duplex(1024);
        let (host, host_in, driver) = session(left, Role::Host);
        let host_task = tokio::spawn(driver.run());
        let (container, container_in, driver) = session(right, Role::Container);
        Self {
            host,
            host_in,
            container,
            container_in,
            tasks: vec![host_task, tokio::spawn(driver.run())],
            abrupt_peer_close: false,
        }
    }

    async fn stop(&mut self) -> Result<()> {
        self.host.close();
        self.container.close();
        for mut task in self.tasks.drain(..) {
            match tokio::time::timeout(Duration::from_secs(2), &mut task).await {
                Ok(joined) => {
                    if let Err(error) = joined? {
                        // This test deliberately drops a peer with a partially
                        // transmitted Yamux frame. Require that exact I/O fault,
                        // rather than suppressing arbitrary driver errors.
                        ensure!(
                            self.abrupt_peer_close
                                && error.chain().any(|cause| cause
                                    .downcast_ref::<std::io::Error>()
                                    .is_some_and(
                                        |error| error.kind() == std::io::ErrorKind::UnexpectedEof
                                    )),
                            "unexpected driver failure: {error:#}"
                        );
                    }
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    anyhow::bail!("egress driver did not join");
                }
            }
        }
        Ok(())
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[tokio::test]
async fn independent_streams_preserve_binary_and_half_close_in_both_directions() -> Result<()> {
    let mut pair = Pair::new();
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let mut intercept = pair.container.open().await?;
        intercept.write_all(&[0, 255, 42]).await?;
        intercept.shutdown().await?;
        let mut native = pair
            .host_in
            .next()
            .await
            .ok_or_else(|| anyhow!("missing intercept"))?;
        let mut bytes = Vec::new();
        native.read_to_end(&mut bytes).await?;
        ensure!(bytes == [0, 255, 42]);
        native.write_all(b"response").await?;
        native.shutdown().await?;
        bytes.clear();
        intercept.read_to_end(&mut bytes).await?;
        ensure!(bytes == b"response");

        let mut direct = pair.host.open().await?;
        direct.write_all(b"direct").await?;
        let mut peer = pair
            .container_in
            .next()
            .await
            .ok_or_else(|| anyhow!("missing direct stream"))?;
        let mut data = [0; 6];
        peer.read_exact(&mut data).await?;
        ensure!(&data == b"direct");
        Ok::<_, anyhow::Error>(())
    })
    .await;
    pair.stop().await?;
    result??;
    Ok(())
}

#[tokio::test]
async fn stalled_data_does_not_block_control_and_close_wakes_a_blocked_writer() -> Result<()> {
    let mut pair = Pair::new();
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let mut blocked = pair.container.open().await?;
        blocked.write_all(b"head").await?;
        let _held = pair
            .host_in
            .next()
            .await
            .ok_or_else(|| anyhow!("missing held stream"))?;
        let data = vec![7; 2 * 1024 * 1024];
        let writing = blocked.write_all(&data);
        tokio::pin!(writing);
        ensure!(
            std::future::poll_fn(|cx| Poll::Ready(writing.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let mut control = pair.host.open().await?;
        control.write_all(b"control").await?;
        let mut peer = pair
            .container_in
            .next()
            .await
            .ok_or_else(|| anyhow!("missing control"))?;
        let mut data = [0; 7];
        peer.read_exact(&mut data).await?;
        ensure!(&data == b"control");
        pair.abrupt_peer_close = true;
        pair.container.close();
        ensure!(
            writing.await.is_err(),
            "closed run acknowledged a blocked write"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await;
    pair.stop().await?;
    result??;
    Ok(())
}

#[tokio::test]
async fn open_queue_is_bounded_and_abandoned_opens_release_their_reservation() -> Result<()> {
    let (io, _peer) = tokio::io::duplex(1024);
    let (link, _inbound, driver) = session(io, Role::Host);
    let mut waiting = Vec::new();
    for _ in 0..PENDING_OPENS {
        let mut open = Box::pin(link.open());
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(open.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        waiting.push(open);
    }
    assert!(link.open().await.is_err());
    assert_eq!(
        link.capacity.available_permits(),
        MAX_STREAMS - PENDING_OPENS
    );
    drop(waiting);
    // No driver has been polled: the queue still owns cancelled requests.
    drop(driver);
    assert_eq!(link.capacity.available_permits(), MAX_STREAMS);
    assert!(link.open().await.is_err());
    Ok(())
}

#[tokio::test]
async fn run_retirement_rejects_reads_even_when_the_peer_still_holds_a_stream() -> Result<()> {
    let mut pair = Pair::new();
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let mut source = pair.container.open().await?;
        source.write_all(&[1]).await?;
        let mut retained = pair
            .host_in
            .next()
            .await
            .ok_or_else(|| anyhow!("missing retained stream"))?;
        let mut byte = [0];
        retained.read_exact(&mut byte).await?;
        pair.host.close();
        pair.container.close();
        tokio::task::yield_now().await;
        assert!(retained.read_exact(&mut byte).await.is_err());
        assert!(source.write_all(&[2]).await.is_err());
        Ok::<_, anyhow::Error>(())
    })
    .await;
    pair.stop().await?;
    result??;
    Ok(())
}

#[tokio::test]
async fn close_revokes_buffered_bytes_synchronously_before_the_driver_runs() -> Result<()> {
    let mut pair = Pair::new();
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let mut source = pair.container.open().await?;
        source.write_all(&[1, 2, 3]).await?;
        let mut retained = pair
            .host_in
            .next()
            .await
            .ok_or_else(|| anyhow!("missing stream"))?;
        let mut first = [0];
        retained.read_exact(&mut first).await?;
        pair.host.close();
        let mut tail = [0; 2];
        ensure!(
            retained.read_exact(&mut tail).await.is_err(),
            "retired run released buffered bytes"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await;
    pair.stop().await?;
    result??;
    Ok(())
}

#[tokio::test]
async fn stream_limit_refuses_without_closing_existing_traffic_and_released_slots_reopen(
) -> Result<()> {
    let mut pair = Pair::new();
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let mut streams = Vec::new();
        for _ in 0..MAX_STREAMS {
            let mut source = pair.host.open().await?;
            source.write_all(b"x").await?;
            let mut peer = pair
                .container_in
                .next()
                .await
                .ok_or_else(|| anyhow!("missing stream"))?;
            let mut byte = [0];
            peer.read_exact(&mut byte).await?;
            ensure!(&byte == b"x");
            streams.push((source, peer));
        }
        ensure!(
            pair.host.open().await.is_err(),
            "stream ceiling was not enforced"
        );
        streams[0].1.write_all(b"alive").await?;
        let mut bytes = [0; 5];
        streams[0].0.read_exact(&mut bytes).await?;
        ensure!(
            &bytes == b"alive",
            "overload interrupted an existing stream"
        );
        drop(streams.pop());
        let mut fresh = pair.host.open().await?;
        fresh.write_all(b"fresh").await?;
        let mut peer = pair
            .container_in
            .next()
            .await
            .ok_or_else(|| anyhow!("missing replacement stream"))?;
        peer.read_exact(&mut bytes).await?;
        ensure!(&bytes == b"fresh");
        Ok::<_, anyhow::Error>(())
    })
    .await;
    pair.stop().await?;
    result??;
    Ok(())
}
