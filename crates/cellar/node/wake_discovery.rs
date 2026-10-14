//! Resource activation precedes admitting the persisted cold-wake backlog.
use super::{AppHandle, Bucket, BucketOwnership, Message};
use crate::application::event_stream::readiness::Readiness;
use std::{future::Future, sync::Arc};

async fn after_activation<F: Future>(
    readiness: Option<&Readiness>,
    discovery: F,
) -> anyhow::Result<F::Output> {
    if let Some(readiness) = readiness {
        readiness.wait().await?;
    }
    Ok(discovery.await)
}

pub(super) fn start(
    app: &AppHandle,
    bucket: Bucket,
    ownership: Arc<BucketOwnership>,
    node: &str,
    tick_ms: u64,
) -> anyhow::Result<()> {
    let readiness = app
        .runtime
        .as_ref()
        .map(|runtime| runtime.generation().event_readiness.clone());
    let actor = app.tx.clone();
    let node = node.to_string();
    crate::asyncrt::try_spawn(async move {
        // Ownership and predecessor recovery already completed. Event resource
        // RPCs still need actor permits; admitting every cold source first can
        // strand those RPCs behind owner-log recovery until their deadline.
        let result = after_activation(readiness.as_deref(), async move {
            let boot_ms = super::now_ms();
            let live = crate::dead_node_gc::live_nodes(&bucket, boot_ms).await;
            let due = crate::wake::due_scan(&bucket, boot_ms as i64).await;
            for (cell, entry_ms) in crate::wake::elected_due(&ownership, &node, &live, due).await {
                let _ = actor.send(Message::WakeHint {
                    cell,
                    entry_ms,
                    scope: cellar_logic::WakeHintScope::Fleet,
                });
            }
            crate::wake::run_periodic(bucket, ownership, node, actor, tick_ms).await;
        })
        .await;
        if let Err(error) = result {
            tracing::debug!(%error, "wake discovery stopped before resource activation");
        }
    })?
    .detach();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn cold_discovery_waits_for_activation_without_consuming_work() -> anyhow::Result<()> {
        let readiness = Readiness::new(false);
        let calls = AtomicUsize::new(0);
        let mut discovery = Box::pin(after_activation(Some(&readiness), async {
            calls.fetch_add(1, Ordering::SeqCst);
            7
        }));
        assert!(discovery.as_mut().now_or_never().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        readiness.finish(Ok(()));
        assert_eq!(discovery.await?, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn failed_or_cancelled_activation_never_starts_discovery() -> anyhow::Result<()> {
        let readiness = Readiness::new(false);
        let calls = AtomicUsize::new(0);
        let mut discovery = Box::pin(after_activation(Some(&readiness), async {
            calls.fetch_add(1, Ordering::SeqCst);
        }));
        assert!(discovery.as_mut().now_or_never().is_none());
        readiness.finish(Err("configuration refused".into()));
        assert!(discovery.await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        readiness.begin();
        let mut discovery = Box::pin(after_activation(Some(&readiness), async {
            calls.fetch_add(1, Ordering::SeqCst);
        }));
        assert!(discovery.as_mut().now_or_never().is_none());
        drop(discovery);
        readiness.finish(Ok(()));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_without_event_activation_starts_discovery_immediately() -> anyhow::Result<()> {
        assert_eq!(after_activation(None, async { 7 }).await?, 7);
        assert_eq!(
            after_activation(Some(&Readiness::new(true)), async { 9 }).await?,
            9
        );
        Ok(())
    }
}
