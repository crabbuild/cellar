// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Expired peer streams cannot consume the next command's replication attempt.
use super::*;

pub(crate) async fn peer_connection_idle(rooted: bool, control: bool) -> Result<()> {
    let mut h = Harness::new(rooted, Arc::new(object_store::memory::InMemory::new()), 1).await?;
    let baseline = h.fixture.transports[1..]
        .iter()
        .map(|transport| transport.usage().tasks)
        .collect::<Vec<_>>();
    let result = async {
        let first = h
            .command(
                1,
                b"before-idle",
                Durability::Quorum,
                Duration::from_secs(15),
            )
            .await?
            .response()
            .await?;
        assert_eq!(first.durability(), Durability::Quorum);
        let index = first
            .local_receipt()
            .context("first receipt")?
            .position
            .index;
        drop(first);
        controller::applied(&h.fixture, h.route.history, index).await?;
        if control {
            heartbeat(&h).await?;
        }
        // The fixture has no automatic Raft ticks. Observe actual receiver task
        // exit at the existing transport I/O bound; no request window is active.
        for (transport, baseline) in h.fixture.transports[1..].iter().zip(&baseline) {
            assert!(
                transport.usage().tasks > *baseline,
                "peer stream was not opened"
            );
        }
        controller::wait_for(async || {
            Ok(h.fixture.transports[1..]
                .iter()
                .zip(&baseline)
                .all(|(transport, baseline)| transport.usage().tasks == *baseline))
        })
        .await?;
        if control {
            heartbeat(&h).await?;
        }
        let response = h
            .command(
                2,
                b"after-idle",
                Durability::Quorum,
                Duration::from_secs(15),
            )
            .await?
            .response()
            .await?;
        assert_eq!(response.durability(), Durability::Quorum);
        let receipt = response.local_receipt().context("after-idle receipt")?;
        assert_eq!(receipt.receipt.result(), b"after-idle");
        assert_eq!(receipt.version.sequence.get(), 3);
        assert_eq!(h.calls.load(Ordering::SeqCst), 2);
        drop(response);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    // Join the original producer and all network owners before reporting a
    // negative control. Its failed command is never retried with a new window.
    if result.is_err() {
        h.fixture.cluster._root.disable_cleanup(true);
        eprintln!(
            "retained peer-idle fixture {}",
            h.fixture.cluster._root.path().display()
        );
    }
    h.close().await?;
    result
}

async fn heartbeat(h: &Harness) -> Result<()> {
    let status = h.fixture.runtimes[0].status(h.route.history).await?;
    for to in [2, 3] {
        let mut message = Message {
            from: 1,
            to,
            term: status.term.get(),
            commit: status.committed.get(),
            ..Default::default()
        };
        message.set_msg_type(MessageType::MsgHeartbeat);
        h.fixture.transports[0]
            .submit_physical(&h.route.history, message)?
            .accepted()
            .await?;
    }
    Ok(())
}
