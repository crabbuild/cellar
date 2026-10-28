// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, retryable collection after the permanent admission barrier.
use super::*;

pub(super) async fn maintain(
    app: AppHandle,
    ownership: Option<Arc<BucketOwnership>>,
) -> anyhow::Result<()> {
    let Some(runtime) = &app.runtime else {
        return Ok(());
    };
    let Some(record) = runtime.retirements.next().await? else {
        return Ok(());
    };
    let scope = record.evidence.scope();
    // Eviction joins admitted requests and the replica writer. It refuses an
    // active event; the durable queue survives that refusal and any host loss.
    app.evict(scope.to_string())
        .await
        .map_err(|error| anyhow::anyhow!("erasure awaits cell eviction: {error:?}"))?;
    anyhow::ensure!(
        runtime.published_epoch(scope).is_none(),
        "erasure runtime remains resident"
    );
    if let Some(replication) = runtime.replication() {
        replication.ltx().ensure_erasure_idle(scope)?;
    }
    let directory = runtime.data_dir.join(scope);
    let filesystem = crate::asyncrt::fs();
    if !crate::asyncrt::try_blocking(move || filesystem.remove_dir_batch(&directory, 100))?
        .await??
    {
        return Ok(());
    }
    // Returning nodes may remove their own inactive caches even while the
    // terminal cell's last owner is live. Shared deletion waits for that owner.
    if let Some(ownership) = &ownership {
        if let Some(owner) = ownership.read_owner(scope).await? {
            if let Some(node) = owner.node.as_deref() {
                let local =
                    node == ownership.node() && owner.lineage.as_deref() == ownership.lineage();
                if !local
                    && ownership.read_node_lease(node).await?.is_some_and(|lease| {
                        lease.lineage == owner.lineage && lease.expires_ms > now_ms()
                    })
                {
                    return Ok(());
                }
            }
        }
    }
    if let Some(replication) = runtime.replication() {
        if !replication.ltx().erase_retired_batch(scope).await? {
            return Ok(());
        }
    }
    runtime.retirements.complete(&record).await
}
