// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, retryable collection after the permanent admission barrier.
use super::*;
use crate::cell_runtime::retirement::{Candidate, Record};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::sync::{atomic::AtomicBool, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Per-node limits for terminal sandbox data collection. Provider retirement
/// remains durable cell work; these limits apply after its erasure barrier.
#[derive(Clone, Debug)]
pub struct SandboxErasurePolicy {
    /// Maximum simultaneous erasure attempts on this node, including entered
    /// local filesystem work whose async caller has timed out. Default: 8.
    pub concurrency: usize,
    /// How often free slots discover more queued work. Default: 250 ms.
    pub scan_period: std::time::Duration,
    /// Deadline for one discovery or erasure attempt. Durable requests survive
    /// timeouts and are retried on a later pass. Default: 20 seconds.
    pub attempt_timeout: std::time::Duration,
}

impl Default for SandboxErasurePolicy {
    fn default() -> Self {
        Self {
            concurrency: 8,
            scan_period: std::time::Duration::from_millis(250),
            attempt_timeout: std::time::Duration::from_secs(20),
        }
    }
}

impl SandboxErasurePolicy {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=64).contains(&self.concurrency),
            "sandbox erasure concurrency must be 1..=64"
        );
        for duration in [self.scan_period, self.attempt_timeout] {
            anyhow::ensure!(
                !duration.is_zero() && duration <= std::time::Duration::from_secs(60),
                "sandbox erasure intervals must be positive and at most 60 seconds"
            );
        }
        Ok(())
    }
}

pub(super) struct Collector {
    policy: SandboxErasurePolicy,
    permits: Arc<Semaphore>,
    scanning: AtomicBool,
    active: Mutex<std::collections::HashSet<String>>,
    round: std::sync::atomic::AtomicUsize,
}

struct ScanLease(Arc<Collector>);

impl Drop for ScanLease {
    fn drop(&mut self) {
        self.0.scanning.store(false, Ordering::Release);
    }
}

struct JobLease {
    collector: Arc<Collector>,
    scope: String,
    _permit: OwnedSemaphorePermit,
}

impl Drop for JobLease {
    fn drop(&mut self) {
        self.collector.active.lock().unwrap().remove(&self.scope);
    }
}

impl Collector {
    pub(super) fn new(policy: SandboxErasurePolicy) -> anyhow::Result<Arc<Self>> {
        policy.validate()?;
        Ok(Arc::new(Self {
            permits: Arc::new(Semaphore::new(policy.concurrency)),
            policy,
            scanning: AtomicBool::new(false),
            active: Default::default(),
            round: Default::default(),
        }))
    }

    /// Returned work is joined with the driver's ordinary dispatches. Freed
    /// slots can be reused while a prior batch still has slow attempts running.
    pub(super) fn schedule(
        self: &Arc<Self>,
        app: AppHandle,
        ownership: Option<Arc<BucketOwnership>>,
    ) -> Option<futures_util::future::BoxFuture<'static, ()>> {
        let runtime = app.runtime.clone()?;
        if self.scanning.swap(true, Ordering::AcqRel) {
            return None;
        }
        let scan = ScanLease(self.clone());
        let mut permits = Vec::new();
        while let Ok(permit) = self.permits.clone().try_acquire_owned() {
            permits.push(permit);
        }
        if permits.is_empty() {
            return None;
        }
        let collector = self.clone();
        Some(Box::pin(async move {
            let bound = permits.len();
            let audit = if bound == 1 {
                usize::from(collector.round.fetch_add(1, Ordering::Relaxed) % 4 == 3)
            } else {
                (bound / 4).max(1)
            };
            let selected = crate::asyncrt::timeout(collector.policy.attempt_timeout, async {
                let mut selected = if bound > audit {
                    runtime
                        .retirements
                        .discover(false, bound - audit, crate::asyncrt::mono_ms())
                        .await?
                } else {
                    Vec::new()
                };
                let available = bound - selected.len();
                if available > 0 {
                    selected.extend(
                        runtime
                            .retirements
                            .discover(true, available, crate::asyncrt::mono_ms())
                            .await?,
                    );
                }
                Ok::<_, anyhow::Error>(selected)
            })
            .await;
            drop(scan);
            let selected = match selected {
                Ok(Ok(selected)) => selected,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "sandbox erasure discovery remains queued");
                    return;
                }
                Err(_) => {
                    tracing::warn!("sandbox erasure discovery timed out; retry retained");
                    return;
                }
            };
            let mut work = FuturesUnordered::new();
            for (candidate, permit) in selected.into_iter().zip(permits) {
                if !collector
                    .active
                    .lock()
                    .unwrap()
                    .insert(candidate.scope.clone())
                {
                    continue;
                }
                let lease = Arc::new(JobLease {
                    collector: collector.clone(),
                    scope: candidate.scope.clone(),
                    _permit: permit,
                });
                let app = app.clone();
                let ownership = ownership.clone();
                let timeout = collector.policy.attempt_timeout;
                work.push(async move {
                    let result = crate::asyncrt::timeout(
                        timeout,
                        collect(&app, ownership.as_deref(), &candidate, lease.clone()),
                    )
                    .await;
                    match result {
                        Ok(Ok(())) => {}
                        result => tracing::debug!(
                            scope = %candidate.scope, ?result,
                            "sandbox erasure attempt remains queued"
                        ),
                    }
                });
            }
            while work.next().await.is_some() {}
        }))
    }
}

async fn collect(
    app: &AppHandle,
    ownership: Option<&BucketOwnership>,
    candidate: &Candidate,
    lease: Arc<JobLease>,
) -> anyhow::Result<()> {
    let runtime = app.runtime.as_ref().expect("collector requires a runtime");
    let Some(record) = runtime.retirements.load(candidate).await? else {
        return Ok(());
    };
    maintain(app, ownership, &record, lease).await
}

async fn maintain(
    app: &AppHandle,
    ownership: Option<&BucketOwnership>,
    record: &Record,
    lease: Arc<JobLease>,
) -> anyhow::Result<()> {
    let runtime = app.runtime.as_ref().expect("collector requires a runtime");
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
    let entered = lease.clone();
    if !crate::asyncrt::try_blocking(move || {
        let _entered = entered;
        filesystem.remove_dir_batch(&directory, 100)
    })?
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
    runtime.retirements.complete(record).await
}
